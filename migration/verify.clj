;; E-futon1a-to-futon1b-migration-pipeline — S5: Verification (rewritten
;; 2026-07-10, F6: the original checksum layer hashed full transformed source
;; docs against a 2-column projection of DIFFERENT rows — decorative — and the
;; source-count interlock read a manifest only the superseded census export
;; path writes, printing PASS on 0=0).
;;
;; Three layers, ALL feeding the verdict:
;;   1. COUNTS: per-population doc counts from the export files actually
;;      ingested vs futon1b table counts. Absent files are SKIP, not PASS.
;;   2. DOC CHECKSUM: a deterministic stride-sample of ids per table; the
;;      SAME doc fetched from the node and compared key-by-key against the
;;      transformed source doc (re-applying the recorded rescue stage for
;;      docs the ingest rescue ladder stringified). Temporal values are
;;      normalized to ISO instants on both sides (XTDB 2 returns
;;      ZonedDateTime where the source had #inst Dates).
;;   3. SAMPLED PARITY: futon1a census counts vs futon1b per-type queries —
;;      only meaningful once hyperedges are ingested; SKIP when absent.
;;
;; Run: clojure -J-Xmx2g -M:node -m migration.verify --input-dir <dir>
;;      --store-dir <dir> [--base-url URL] [--sample N]
(ns migration.verify
  (:require [clojure.edn :as edn]
            [migration.export :as ex]
            [migration.transform :as xf]
            [migration.ingest :as ingest]
            [xtdb.node :as xtn]
            [xtdb.api :as xt])
  (:import [java.io File]
           [java.net URL HttpURLConnection])
  (:gen-class))

;; ---------------------------------------------------------------------------
;; HTTP client (minimal, same as export.clj).
;; ---------------------------------------------------------------------------

(defn http-get
  ([^String url-str] (http-get url-str 60000))
  ([^String url-str timeout-ms]
   (let [conn ^HttpURLConnection (.. (URL. url-str) openConnection)]
     (.setRequestMethod conn "GET")
     (.setConnectTimeout conn 30000)
     (.setReadTimeout conn timeout-ms)
     (.setRequestProperty conn "Accept" "application/edn")
     (let [code (.getResponseCode conn)]
       (if (= 200 code)
         (slurp (.getInputStream conn))
         (let [err (try (slurp (.getErrorStream conn)) (catch Exception _ ""))]
           (throw (ex-info (str "HTTP " code " for " url-str)
                           {:url url-str :code code :body err}))))))))

(defn parse-edn [s]
  (when (and s (seq s)) (edn/read-string s)))

;; ---------------------------------------------------------------------------
;; Canonicalization. Temporals unify to ISO instants so a source-side
;; java.util.Date and the node's ZonedDateTime read-back compare equal.
;; ---------------------------------------------------------------------------

(declare stable-str)

(def ^:private canonical-key-cmpr
  (fn [a b] (compare (stable-str a) (stable-str b))))

(defn canonicalize
  "Canonicalize a value into a form with deterministic pr-str."
  [v]
  (cond
    ;; nil-valued entries are dropped: XTDB 2 stores nil struct fields as
    ;; absent, so nil == absent is the storage semantics (verified on
    ;; :entity/props read-back, 2026-07-10). Nils inside vectors are kept —
    ;; element positions matter.
    (map? v)
    (into (sorted-map-by canonical-key-cmpr)
          (keep (fn [[k vv]] (when (some? vv) [k (canonicalize vv)])))
          v)

    (set? v)
    (vec (sort-by stable-str (map canonicalize v)))

    (sequential? v)
    (mapv canonicalize v)

    (instance? java.util.Date v)
    {:__type :instant :iso (str (.toInstant ^java.util.Date v))}

    (instance? java.time.Instant v)
    {:__type :instant :iso (str v)}

    (instance? java.time.ZonedDateTime v)
    {:__type :instant :iso (str (.toInstant ^java.time.ZonedDateTime v))}

    (instance? java.time.OffsetDateTime v)
    {:__type :instant :iso (str (.toInstant ^java.time.OffsetDateTime v))}

    (instance? java.time.temporal.TemporalAccessor v)
    {:__type :temporal :class (.getName (class v)) :iso (str v)}

    (instance? java.util.UUID v)
    {:__type :uuid :value (str v)}

    (or (nil? v) (string? v) (keyword? v) (symbol? v)
        (number? v) (boolean? v) (char? v))
    v

    :else
    {:__type :object :class (.getName (class v)) :str (str v)}))

(defn stable-str
  "Deterministic pr-str for any EDN-ish value."
  [v]
  (pr-str (canonicalize v)))

;; ---------------------------------------------------------------------------
;; Node-side helpers.
;; ---------------------------------------------------------------------------

(defn count-table
  [node table]
  (try
    (count (xt/q node (list 'from table ['xt/id])))
    (catch Exception _ 0)))

(defn fetch-doc
  "Fetch the doc for id from a table, projecting exactly the columns named by
  ks (keywords). `[*]` binds nothing in XTDB 2.0.0, so we project the source
  doc's own keys — absent columns bind null, which is what the comparison
  wants. Returns nil if the doc is absent."
  [node table id ks]
  (let [cols (into ['xt/id]
                   (comp (remove #{:xt/id})
                         (map #(symbol (namespace %) (name %))))
                   ks)]
    (first (xt/q node (list '-> (list 'from table (vec cols))
                            (list 'where (list '= 'xt/id id)))))))

(defn await-indexed
  "Poll tables until total count stabilises (>0, unchanged across two polls)
  or timeout. A freshly-reopened node replays its log asynchronously."
  [node tables timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop [prev -1 stable 0]
      (let [total (reduce + (map #(count-table node %) tables))]
        (cond
          (and (pos? total) (= total prev) (>= stable 1))
          (do (println (format "  indexer caught up: %d docs indexed" total)) total)

          (> (System/currentTimeMillis) deadline)
          (do (println (format "  WARN: await-indexed timed out at %d docs" total)) total)

          :else
          (do (Thread/sleep 500)
              (recur total (if (= total prev) (inc stable) 0))))))))

;; ---------------------------------------------------------------------------
;; Layer 1: counts from the export files actually ingested.
;; ---------------------------------------------------------------------------

(defn source-populations
  "Read each export file and derive expected per-table counts.
  Returns {:present {table n} :absent [file ...]} — heavy read; -Xmx2g for a
  full export set."
  [input-dir]
  (let [file-of {:evidence "evidence.edn"
                 :hyperedges "hyperedges.edn"
                 :type-catalog "type-catalog.edn"
                 :graph "graph-snapshot.edn"}
        present (atom {}) absent (atom [])]
    (doseq [[label fname] file-of]
      (let [path (str input-dir "/" fname)]
        (if (.exists (File. path))
          (let [docs (ex/read-edn-file path)]
            (if (= label :graph)
              (swap! present assoc
                     :entities (count (filter :entity/id docs))
                     :relations (count (filter :relation/id docs)))
              (swap! present assoc label (count docs))))
          (swap! absent conj fname))))
    {:present @present :absent @absent}))

;; ---------------------------------------------------------------------------
;; Layer 2: per-doc checksum on a deterministic stride sample.
;; ---------------------------------------------------------------------------

(defn- rescue-source
  "Re-apply the recorded rescue stage to a transformed source doc so the
  comparison matches what ingest actually stored."
  [doc stage]
  (case stage
    :rescued-1 (xf/stringify-risky-nils doc nil)
    :rescued-2 (-> doc (xf/stringify-risky-nils nil) (xf/stringify-deep-colls nil))
    doc))

(defn compare-doc
  "Compare a transformed(+rescued) source doc against the node's copy.
  Returns nil when clean, else {:id .. :missing? true} or {:id .. :keys [..]}."
  [node table src-doc rescue-stage]
  (let [src (rescue-source src-doc rescue-stage)
        id (:xt/id src)
        dest (fetch-doc node table id (keys src))]
    (if-not dest
      {:id id :missing? true}
      (let [bad (vec (for [[k v] src
                           :when (and (some? v)
                                      (not= (stable-str v) (stable-str (get dest k))))]
                       k))]
        (when (seq bad) {:id id :keys bad})))))

(defn checksum-sample
  "Stride-sample n docs from source-docs, compare each against the node.
  rescue-stage-of: id -> :rescued-1 | :rescued-2 | nil."
  [node table source-docs n rescue-stage-of]
  (let [docs (vec source-docs)
        total (count docs)
        stride (max 1 (quot total (max 1 n)))
        sample (->> (range 0 total stride) (take n) (map docs))
        results (keep (fn [src]
                        (let [tx (xf/transform-doc src)]
                          (compare-doc node table tx (rescue-stage-of (:xt/id tx)))))
                      sample)]
    {:sampled (min n (count (range 0 total stride)))
     :mismatched (vec results)}))

;; ---------------------------------------------------------------------------
;; Layer 3: sampled census parity (hyperedges only, when present).
;; ---------------------------------------------------------------------------

(defn get-census-count
  [base-url type-str]
  (let [resp (parse-edn (http-get (str base-url "/api/alpha/census?type=" type-str)))]
    (:count resp 0)))

(defn parity-samples
  [node base-url]
  (for [[type-str label] [["code/v05/calls" "hx-by-type-calls"]
                          ["edge/renamed-to" "hx-by-type-renamed-to"]
                          ["mission-scope/nesting" "hx-by-type-nesting"]]
        :let [type-kw (keyword type-str)]]
    (let [source (try (get-census-count base-url type-str) (catch Exception _ -1))
          dest (count (xt/q node (list '->
                                       (list 'from :hyperedges ['xt/id 'hx/type])
                                       (list 'where (list '= 'hx/type type-kw)))))]
      {:label label :source source :dest dest :pass? (= source dest)})))

;; ---------------------------------------------------------------------------
;; Main verification.
;; ---------------------------------------------------------------------------

(def ^:private table->file
  {:evidence "evidence.edn"
   :hyperedges "hyperedges.edn"
   :type-catalog "type-catalog.edn"
   :entities "graph-snapshot.edn"
   :relations "graph-snapshot.edn"})

(defn- load-rescue-stages
  "id -> rescue stage, from ingest-summary.edn (if present)."
  [input-dir]
  (let [path (str input-dir "/ingest-summary.edn")]
    (if (.exists (File. path))
      (let [summary (edn/read-string (slurp path))
            results (vals (:results summary))]
        (merge (into {} (for [id (mapcat :rescued-1-ids results)] [id :rescued-1]))
               (into {} (for [id (mapcat :rescued-2-ids results)] [id :rescued-2]))))
      {})))

(defn verify
  [input-dir base-url store-dir sample-n]
  (println "=== Verification (S5) ===")
  (println "Input dir:" input-dir "| Store dir:" store-dir "| Sample:" sample-n)
  (when-not store-dir
    (throw (ex-info "verify requires --store-dir (the persistent store ingest wrote)"
                    {:hint "run ingest with --store-dir DIR first"})))
  (with-open [node (xtn/start-node (ingest/node-cfg store-dir))]
    (println "--- Awaiting indexer catch-up on reopen ---")
    (await-indexed node [:hyperedges :entities :evidence :relations
                         :type-catalog :docs :lab-sessions :misc] 120000)

    (let [;; Layer 1: counts.
          _ (println "--- Layer 1: population counts (source files vs tables) ---")
          {:keys [present absent]} (source-populations input-dir)
          count-results
          (vec (for [[table expected] (sort-by str present)]
                 (let [got (count-table node table)
                       pass? (= expected got)]
                   (println (format "  %-14s source=%-7d dest=%-7d %s"
                                    table expected got (if pass? "PASS" "FAIL")))
                   pass?)))
          _ (doseq [f absent] (println (format "  %-14s SKIP (%s absent)" "-" f)))

          ;; Layer 2: per-doc checksum.
          _ (println "--- Layer 2: per-doc checksum (stride sample) ---")
          rescue-stage-of (load-rescue-stages input-dir)
          checksum-results
          (vec (for [[table fname] table->file
                     :let [path (str input-dir "/" fname)]
                     :when (.exists (File. path))]
                 (let [docs (cond->> (ex/read-edn-file path)
                              (= table :entities) (filter :entity/id)
                              (= table :relations) (filter :relation/id))
                       {:keys [sampled mismatched]}
                       (checksum-sample node table docs sample-n rescue-stage-of)]
                   (println (format "  %-14s sampled=%-4d mismatched=%-3d %s"
                                    table sampled (count mismatched)
                                    (if (empty? mismatched) "PASS" "FAIL")))
                   (doseq [m (take 5 mismatched)]
                     (println (format "    %s %s" (:id m)
                                      (if (:missing? m) "MISSING FROM NODE"
                                          (str "keys " (pr-str (:keys m)))))))
                   (empty? mismatched))))

          ;; Layer 3: parity (hyperedges only).
          hx-count (count-table node :hyperedges)
          _ (println "--- Layer 3: sampled census parity ---")
          parity (if (pos? hx-count)
                   (doall (parity-samples node base-url))
                   (do (println "  SKIP (no hyperedges ingested yet)") nil))
          _ (doseq [{:keys [label source dest pass?]} parity]
              (println (format "  %-30s source=%-8d dest=%-8d %s"
                               label source dest (if pass? "PASS" "FAIL"))))

          all-pass? (and (every? true? count-results)
                         (every? true? checksum-results)
                         (every? :pass? (or parity [])))]
      (println)
      (println (format "=== Verification %s ===%s"
                       (if all-pass? "PASS" "FAIL")
                       (if (seq absent)
                         (format " (partial: %s absent)" (pr-str absent)) "")))
      {:counts-pass? (every? true? count-results)
       :checksum-pass? (every? true? checksum-results)
       :parity parity
       :absent absent
       :all-pass? all-pass?})))

;; ---------------------------------------------------------------------------
;; CLI.
;; ---------------------------------------------------------------------------

(defn- parse-args [args]
  (loop [args args opts {:input-dir "migration-export"
                         :base-url "http://localhost:7071"
                         :store-dir nil
                         :sample 100}]
    (if-not (seq args)
      opts
      (case (first args)
        "--input-dir" (recur (nnext args) (assoc opts :input-dir (second args)))
        "--base-url"  (recur (nnext args) (assoc opts :base-url (second args)))
        "--store-dir" (recur (nnext args) (assoc opts :store-dir (second args)))
        "--sample"    (recur (nnext args) (assoc opts :sample
                                                 (Long/parseLong (second args))))
        "--help" (do (println "Usage: clojure -J-Xmx2g -M:node -m migration.verify"
                              "[--input-dir DIR] [--base-url URL] --store-dir DIR"
                              "[--sample N]")
                     (System/exit 0))
        (throw (ex-info (str "Unknown arg: " (first args)) {:args args}))))))

(defn -main [& args]
  (let [{:keys [input-dir base-url store-dir sample]} (parse-args args)]
    (verify input-dir base-url store-dir sample)
    (shutdown-agents)))
