;; E-futon1a-to-futon1b-migration-pipeline — S3: Batched ingest into futon1b.
;;
;; Ingests transformed docs into an XTDB 2 in-process node via put-docs.
;; Batched to stay within the -Xmx1g -XX:MaxDirectMemorySize=1g heap cap.
;;
;; The batch size defaults to 200 (the proven pattern from migrate_futon1.clj).
;; Each batch is a single transaction (put-docs accepts multiple docs per tx).
;;
;; Progress reporting: prints batch number, docs ingested, elapsed time,
;; and approximate docs/sec after each batch.
;;
;; Run: clojure -M:node -m migration.ingest --input-dir <dir> [--batch N]
;;      [--xmx-mb M] [--tables TABLE1,TABLE2,...]
(ns migration.ingest
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [migration.transform :as xf]
            [xtdb.node :as xtn]
            [xtdb.api :as xt])
  (:import [java.io PushbackReader File])
  (:gen-class))

;; ---------------------------------------------------------------------------
;; EDN file reading (streaming where possible).
;; ---------------------------------------------------------------------------

(defn read-edn-file
  "Read an EDN file. Returns the parsed data."
  [^String path]
  (with-open [r (PushbackReader. (io/reader path))]
    (edn/read r)))

;; ---------------------------------------------------------------------------
;; Node configuration.
;;
;; FINDING #4 (claude-16 review, 2026-07-05): the original ingest used a bare
;; `(xtn/start-node)` — an in-MEMORY node. A full migration into an in-memory
;; node discards all data on JVM exit and cannot be re-opened by verify.clj.
;; For a real migration we open a PERSISTENT on-disk node. A persistent XTDB 2
;; node needs BOTH a local transaction log and local object storage (each
;; defaults to in-memory independently). Verified round-trip (close → reopen →
;; read-back PASS) before adoption.
;; ---------------------------------------------------------------------------

(defn node-cfg
  "XTDB 2 config for a persistent on-disk node rooted at store-dir.
  Pass nil for an in-memory node (slice/testing only)."
  [store-dir]
  (if store-dir
    {:log     [:local {:path (str store-dir "/log")}]
     :storage [:local {:path (str store-dir "/storage")}]}
    {}))

;; ---------------------------------------------------------------------------
;; Table routing: assign each doc to a table based on its type.
;;
;; XTDB 2 uses table names (the first arg to put-docs) to organize docs.
;; We route by doc-type, matching the slice's table structure:
;;   :hyperedges, :entities, :evidence, :relations, :type-catalog, :docs
;; ---------------------------------------------------------------------------

(defn doc-table
  "Determine the XTDB 2 table for a doc based on its classification."
  [doc]
  (let [doc-type (xf/classify-doc doc)]
    (case doc-type
      :hyperedge     :hyperedges
      :evidence      :evidence
      :entity        :entities
      :relation      :relations
      :docbook       :docs
      :lab-session   :lab-sessions
      :type-catalog  :type-catalog
      :unknown       :misc)))

;; ---------------------------------------------------------------------------
;; Batched ingest.
;; ---------------------------------------------------------------------------

(defn ingest-batch!
  "Ingest a batch of docs into the node. Returns the tx result."
  [node table docs]
  (let [ops (vec (for [d docs] [:put-docs table d]))
        tx-result (xt/execute-tx node ops)]
    tx-result))

;; ---------------------------------------------------------------------------
;; Rescue ladder (2026-07-10): Arrow's column typing is stateful, so a doc can
;; fail put-docs depending on what preceded it ("Unknown type: NULL" on the
;; cursor-telemetry evidence docs). When a batch fails we retry per-doc; a doc
;; that still fails is re-tried through the rescue transforms (stage 1: nil-
;; carrying values stringified; stage 2: all deep colls stringified). Every
;; rescue is shape-logged; a doc that survives no stage is recorded by id.
;; ---------------------------------------------------------------------------

(defn put-doc-with-rescue!
  "Put one doc, escalating through rescue transforms on failure.
  Returns :ok, :rescued-1, :rescued-2, or {:error msg :id xt-id}."
  ([node table doc shape-log]
   (put-doc-with-rescue! node table doc shape-log nil))
  ([node table doc shape-log valid-from]
   (let [table-spec (cond-> {:into table}
                      valid-from (assoc :valid-from valid-from))]
     (letfn [(try-put [d] (try (xt/execute-tx node [[:put-docs table-spec d]]) true
                               (catch Exception _ false)))]
       (cond
         (try-put doc) :ok
         (try-put (xf/stringify-risky-nils doc shape-log)) :rescued-1
         (try-put (xf/stringify-deep-colls
                    (xf/stringify-risky-nils doc nil) shape-log)) :rescued-2
         :else {:error "unrescuable — failed all rescue stages"
                :id (:xt/id doc)})))))

(defn rescue-batch!
  "Per-doc retry of a failed batch. Returns {:ok n :rescued-1 n :rescued-2 n
  :rescued-1-ids [..] :rescued-2-ids [..] :failed [{:error .. :id ..} ...]}.
  Per-stage ids let verify.clj re-apply the same rescue to the source side
  before comparing."
  [node table docs shape-log]
  (reduce (fn [acc doc]
            (let [res (put-doc-with-rescue! node table doc shape-log)]
              (case res
                :ok        (update acc :ok (fnil inc 0))
                :rescued-1 (-> acc (update :rescued-1 (fnil inc 0))
                               (update :rescued-1-ids conj (:xt/id doc)))
                :rescued-2 (-> acc (update :rescued-2 (fnil inc 0))
                               (update :rescued-2-ids conj (:xt/id doc)))
                (update acc :failed conj res))))
          {:failed [] :rescued-1-ids [] :rescued-2-ids []}
          docs))

(defn ingest-docs-batched
  "Ingest a collection of docs in batches, with progress reporting.
  Returns {:ingested n :batches n :errors [...]}.

  Docs within a batch go to the same table. If a batch contains mixed doc types,
  they are grouped by table first."
  ([node docs]
   (ingest-docs-batched node docs 200 nil))
  ([node docs batch-size]
   (ingest-docs-batched node docs batch-size nil))
  ([node docs batch-size shape-log]
   (let [docs (vec docs)
         total (count docs)
         start (System/currentTimeMillis)
         ;; Group docs by table, preserving order within each table.
         grouped (->> docs
                      (group-by doc-table))
         ;; Flatten back into [table doc] pairs for ordered batching.
         table-doc-pairs (mapcat (fn [[table group]]
                                   (map (fn [d] [table d]) group))
                                 grouped)]
     (println (format "  Ingesting %d docs across %d tables, batch=%d"
                      total (count grouped) batch-size))
     (loop [pairs (partition-all batch-size table-doc-pairs)
            batch-num 1
            ingested 0
            errors []
            rescues {:rescued-1-ids [] :rescued-2-ids []}]
       (if-not (seq pairs)
         (let [elapsed (- (System/currentTimeMillis) start)
               rate (if (pos? elapsed) (/ (* 1000.0 ingested) elapsed) 0)]
           (println (format "  Done: %d docs in %d batches, %.1fs (%.0f docs/sec)"
                            ingested (dec batch-num) (/ elapsed 1000.0) rate))
           {:ingested ingested :batches (dec batch-num) :errors errors
            :rescued-1-ids (:rescued-1-ids rescues)
            :rescued-2-ids (:rescued-2-ids rescues)})
         (let [batch (first pairs)
               ;; Group within-batch by table (a batch may span tables).
               by-table (group-by first batch)
               ;; `recur` cannot cross a `try` boundary, so the try RETURNS an
               ;; outcome (:ok or {:error ...}) and we recur below on it.
               outcome (try
                         (doseq [[table pairs] by-table]
                           (ingest-batch! node table (map second pairs)))
                         ;; VERIFIED PUT (2026-07-10): XTDB 2.0.0 batch puts
                         ;; can drop rows SILENTLY — observed ~13/200 evidence
                         ;; docs per batch absent after a no-error execute-tx,
                         ;; while the same doc lands fine alone. Point-query
                         ;; every id; re-put misses per-doc (rescue ladder),
                         ;; and only then call the batch :ok.
                         (let [absent (for [[table doc] batch
                                            :let [id (:xt/id doc)]
                                            :when (empty?
                                                   (xt/q node
                                                         (list '-> (list 'from table '[xt/id])
                                                               (list 'where (list '= 'xt/id id)))))]
                                        [table doc])]
                           (if (empty? absent)
                             :ok
                             (let [repaired
                                   (reduce (fn [acc [table doc]]
                                             (let [r (put-doc-with-rescue! node table doc shape-log)
                                                   present? (seq (xt/q node
                                                                       (list '-> (list 'from table '[xt/id])
                                                                             (list 'where (list '= 'xt/id (:xt/id doc))))))]
                                               (if present?
                                                 (update acc :repaired inc)
                                                 (update acc :lost conj {:id (:xt/id doc) :r r}))))
                                           {:repaired 0 :lost []}
                                           absent)]
                               (println (format "    batch %d: %d silently-dropped, %d repaired, %d LOST"
                                                batch-num (count absent) (:repaired repaired)
                                                (count (:lost repaired))))
                               (if (empty? (:lost repaired))
                                 :ok
                                 {:error "verified-put: docs lost after repair"
                                  :lost (:lost repaired)}))))
                         (catch Exception e
                           {:error (.getMessage e)
                            :sample-id (:xt/id (second (first batch)))}))
               ;; On batch failure, walk the batch per-doc through the rescue
               ;; ladder instead of dropping all its docs.
               rescue (when (not= :ok outcome)
                        (println (format "    batch %d failed (%s) — per-doc rescue..."
                                         batch-num (:error outcome)))
                        (reduce (fn [acc [table doc]]
                                  (merge-with (fn [a b] (if (vector? a) (into a b) (+ a b)))
                                              acc
                                              (rescue-batch! node table [doc] shape-log)))
                                {:failed [] :rescued-1-ids [] :rescued-2-ids []}
                                batch))]
           (if (= :ok outcome)
             (let [new-ingested (+ ingested (count batch))
                   elapsed (- (System/currentTimeMillis) start)
                   rate (if (pos? elapsed) (/ (* 1000.0 new-ingested) elapsed) 0)]
               (when (or (= 0 (mod batch-num 10)) (= 1 batch-num))
                 (println (format "    batch %d: %d/%d docs (%.0f docs/sec)"
                                  batch-num new-ingested total rate)))
               (recur (rest pairs) (inc batch-num) new-ingested errors rescues))
             (let [saved (+ (:ok rescue 0) (:rescued-1 rescue 0) (:rescued-2 rescue 0))]
               (println (format "    rescue batch %d: ok=%d rescued-1=%d rescued-2=%d failed=%d"
                                batch-num (:ok rescue 0) (:rescued-1 rescue 0)
                                (:rescued-2 rescue 0) (count (:failed rescue))))
               (recur (rest pairs) (inc batch-num) (+ ingested saved)
                      (if (seq (:failed rescue))
                        (conj errors {:batch batch-num :error (:error outcome)
                                      :unrescued (:failed rescue)})
                        errors)
                      (merge-with into rescues
                                  (select-keys rescue [:rescued-1-ids :rescued-2-ids])))))))))))

;; ---------------------------------------------------------------------------
;; File-based ingest: read each population file, transform, ingest.
;; ---------------------------------------------------------------------------

(defn ingest-file
  "Read, transform, and ingest docs from an EDN file.
  Returns the ingest result map."
  ([node ^String path]
   (ingest-file node path 200 nil))
  ([node ^String path batch-size shape-log]
   (if (.exists (File. path))
     (let [docs (read-edn-file path)]
       (println (format "[ingest] %s: %d docs" path (count docs)))
       (let [transformed (xf/transform-docs docs shape-log)]
         (ingest-docs-batched node transformed batch-size shape-log)))
     (do
       (println (format "[ingest] %s: NOT FOUND, skipping" path))
       {:ingested 0 :skipped true}))))

;; ---------------------------------------------------------------------------
;; Post-ingest count verification.
;; ---------------------------------------------------------------------------

(defn count-by-table
  "Count docs in each table after ingest."
  [node]
  (into {}
        (for [table [:hyperedges :entities :evidence :relations
                     :type-catalog :docs :lab-sessions :misc]]
          (let [cnt (try
                      (count (xt/q node (list 'from table ['xt/id])))
                      (catch Exception _ 0))]
            [table cnt]))))

;; ---------------------------------------------------------------------------
;; CLI.
;; ---------------------------------------------------------------------------

(defn- parse-args [args]
  (loop [args args opts {:input-dir "migration-export"
                         :batch 200
                         :tables "all"
                         :store-dir nil}]
    (if-not (seq args)
      opts
      (case (first args)
        "--input-dir" (recur (nnext args) (assoc opts :input-dir (second args)))
        "--batch"     (recur (nnext args) (assoc opts :batch
                                                  (Long/parseLong (second args))))
        "--tables"    (recur (nnext args) (assoc opts :tables (second args)))
        "--store-dir" (recur (nnext args) (assoc opts :store-dir (second args)))
        "--help" (do (println "Usage: clojure -M:node -m migration.ingest"
                              "[--input-dir DIR] [--batch N] [--tables T1,T2,...]"
                              "[--store-dir DIR]")
                     (println "  --store-dir DIR  persistent on-disk store"
                              "(omit = in-memory, discarded on exit)")
                     (System/exit 0))
        (throw (ex-info (str "Unknown arg: " (first args)) {:args args}))))))

(def file-tables
  "Map from file name to population label."
  [["hyperedges.edn" :hyperedges]
   ["evidence.edn" :evidence]
   ["graph-snapshot.edn" :graph]
   ["type-catalog.edn" :type-catalog]])

(defn -main [& args]
  (let [{:keys [input-dir batch tables store-dir]} (parse-args args)
        shape-log (xf/make-shape-log)
        files (if (= "all" tables)
                file-tables
                (let [wanted (set (map keyword (str/split tables #",")))]
                  (filter #(contains? wanted (second %)) file-tables)))]
    (println "=== Batched ingest from" input-dir "===")
    (println "Batch size:" batch)
    (println "Store:" (if store-dir (str "persistent on-disk @ " store-dir)
                                     "IN-MEMORY (discarded on exit — testing only)"))
    (println)
    (with-open [node (xtn/start-node (node-cfg store-dir))]
      (let [results (into {}
                          (for [[file label] files]
                            (let [path (str input-dir "/" file)
                                  res (ingest-file node path batch shape-log)]
                              [label res])))
            counts (count-by-table node)]
        (println)
        (println "=== Ingest complete ===")
        (println "Table counts:")
        (doseq [[table cnt] (sort-by str counts)]
          (when (pos? cnt)
            (println (format "  %-15s %d" table cnt))))
        (println)
        (println "Shape log entries:" (count @shape-log))
        (when (seq @shape-log)
          (println "First 5 unknown shapes:")
          (doseq [entry (take 5 @shape-log)]
            (println (format "  %s: key=%s reason=%s"
                             (:doc-type entry)
                             (:key entry)
                             (:reason entry)))))
        ;; Write ingest summary.
        (let [summary {:results results
                       :table-counts counts
                       :shape-log-entries @shape-log}]
          (spit (str input-dir "/ingest-summary.edn") (pr-str summary))
          (println "Wrote" (str input-dir "/ingest-summary.edn")))))
    (shutdown-agents)))
