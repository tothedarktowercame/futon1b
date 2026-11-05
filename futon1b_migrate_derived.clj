(ns futon1b-migrate-derived
  "Resumable, paced evidence-to-derived migration, one declared family at a time."
  (:refer-clojure :exclude [run!])
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [futon1b-derived :as derived]
            [futon1b-xt :as fxt]
            [xtdb.api :as xt])
  (:import [java.security MessageDigest]
           [java.time Instant]
           [java.nio.file Files StandardCopyOption]))

(def default-root "/home/joe/code/storage/derived-migration")
(def default-batch-size 1000)
(def default-pause-ms 2000)

(defn- canonical [x]
  (cond (map? x) (into (sorted-map-by #(compare (pr-str %1) (pr-str %2)))
                       (map (fn [[k v]] [k (canonical v)])) x)
        (set? x) (vec (sort-by pr-str (map canonical x)))
        (sequential? x) (mapv canonical x)
        :else x))

(defn sha256 [x]
  (let [bytes (.digest (MessageDigest/getInstance "SHA-256")
                       (.getBytes (pr-str (canonical x)) "UTF-8"))]
    (apply str (map #(format "%02x" (bit-and 0xff %)) bytes))))

(defn sha256-text [s]
  (let [bytes (.digest (MessageDigest/getInstance "SHA-256")
                       (.getBytes (str s) "UTF-8"))]
    (apply str (map #(format "%02x" (bit-and 0xff %)) bytes))))

(defn- act-ids [row]
  (let [body (:evidence/body row)
        ids (concat (when-let [x (:act-id body)] [x])
                    (when-let [x (:source-act-id body)] [x])
                    (:source-act-ids body)
                    (when-let [x (get-in body [:artifact :act-id])] [x]))]
    (if (seq ids) (vec (sort (distinct (map str ids))))
        [(str (:evidence/id row))])))

(defn artifact-activation-transform
  "The body the f75fb0fff writer would have stored: no :text and no
  :text-truncated?. Rows written before f75fb0fff already carry :text-sha256 of
  the FULL text, computed before truncation, so it is kept as written; hashing
  the stored :text would hash a 64 KiB prefix for truncated rows. A row with
  no hash gets one only when its text was complete; a truncated row without a
  hash keeps :text-truncated? so the gap stays visible."
  [row]
  (let [body (:evidence/body row)
        text (:text body)
        truncated? (true? (:text-truncated? body))
        body (cond-> (dissoc body :text :text-truncated?)
               (and text (not (:text-sha256 body)) (not truncated?))
               (assoc :text-sha256 (sha256-text text))
               (and (not (:text-sha256 body)) truncated?)
               (assoc :text-truncated? true))]
    {:derived/id (str "derived:artifact-activation:" (:evidence/id row))
     :derived/family :artifact-activation
     :derived/writer "futon3c/artifact-activation"
     :derived/writer-version "f75fb0fff"
     :derived/source-act-ids (act-ids row)
     :derived/subject (or (:evidence/subject row)
                          {:ref/type :evidence :ref/id (:evidence/id row)})
     :derived/at (:evidence/at row)
     :derived/body body}))

(def artifact-activation-spec
  {:family :artifact-activation
   :writer "futon3c/artifact-activation"
   :writer-version "f75fb0fff"
   :select {:author "futon3c/artifact-activation"
            :type :artifact/weak-activation
            :tags [:artifact-retrieval :weak-activation]}
   :transform artifact-activation-transform})

(def family-specs {:artifact-activation artifact-activation-spec})

(def ^:dynamic *pressure-fn*
  (fn []
    (if-let [f (requiring-resolve 'futon1b-server/expensive-read-snapshot)]
      (f) {:permits/available 4 :permits/waiters 0})))
(def ^:dynamic *sleep-fn* #(Thread/sleep %))

(defn- pressure? []
  (let [p (*pressure-fn*)]
    (or (zero? (long (or (:permits/available p) 0)))
        (pos? (long (or (:permits/waiters p) 0)))
        (pos? (long (or (get-in p [:request-workers 0 :queue-depth]) 0))))))

(defn- page-keys
  "The next LIMIT [xt/id evidence/at] pairs of SPEC's family after CURSOR.
  Narrow on purpose: the same where/order-by/limit over `[*]` took 103 s per
  200-row page on the live store (2026-11-05, evidence ~600k rows), because it
  carries every matching body through the sort; this projection took 7.4 s
  for 200 and 7.3 s for 1,000."
  [node spec cursor limit]
  (let [{:keys [author type]} (:select spec)
        [after-at after-id] (or cursor ["" ""])]
    (vec (fxt/safe-q
          node (fxt/pq '[p-author p-type p-at p-id p-limit]
                       '(-> (from :evidence [xt/id evidence/at evidence/author evidence/type])
                            (where (and (= evidence/author p-author)
                                        (= evidence/type p-type)
                                        (or (> evidence/at p-at)
                                            (and (= evidence/at p-at)
                                                 (> xt/id p-id)))))
                            (order-by evidence/at xt/id)
                            (limit p-limit))
                       author type after-at after-id limit)))))

(defn- hydrate
  "Full evidence rows for KEYS, in KEYS order, by one `_id IN` statement.
  `_id IN` is a table scan at any size (fxt/equality-hydrate-max-ids), so one
  statement per page: 15 s for 200 ids, 25 s for 1,000 live, against 0.28 s
  per id by point read."
  [node keys]
  (if (empty? keys) []
      (let [ids (mapv :xt/id keys)
            sql (str "SELECT * FROM evidence WHERE _id IN ("
                     (str/join "," (repeat (count ids) "?")) ")")
            by-id (into {} (map (juxt :xt/id identity))
                        (fxt/safe-q node (into [sql] ids)))
            missing (remove by-id ids)]
        (when (seq missing)
          (throw (ex-info "Evidence row vanished between page and hydrate"
                          {:reason :hydrate-missing :ids (vec missing)})))
        (mapv by-id ids))))

(defn- page-source [node spec cursor limit]
  (hydrate node (page-keys node spec cursor limit)))

(defn- family-rows [node family]
  (fxt/safe-q node (fxt/pq '[p-family]
                            '(-> (from :derived [*])
                                 (where (= derived/family p-family)))
                            family)))

(defn- prepared [payload]
  (derived/build-derived-doc payload))

(defn- derived-by-ids [node ids]
  (if (empty? ids) {}
      (let [sql (str "SELECT * FROM derived WHERE _id IN ("
                     (str/join "," (repeat (count ids) "?")) ")")]
        (into {} (map (juxt :derived/id identity)
                      (fxt/safe-q node (into [sql] ids)))))))

(defn- write-batch! [node payloads]
  (let [built (mapv prepared payloads)
        ids (mapv #(get-in % [:doc :derived/id]) built)
        before (derived-by-ids node ids)
        conflicts (keep (fn [{:keys [doc]}]
                          (when-let [stored (before (:derived/id doc))]
                            (when-not (= (dissoc doc :xt/id) (dissoc stored :xt/id))
                              (:derived/id doc)))) built)
        new (filterv #(nil? (before (get-in % [:doc :derived/id]))) built)]
    (when (seq conflicts)
      (throw (ex-info "Existing derived row differs from migration output"
                      {:reason :derived-conflict :ids (vec conflicts)})))
    (when (seq new)
      (xt/execute-tx
       node
       (into []
             (mapcat (fn [{:keys [doc links valid-from]}]
                       (into [[:put-docs {:into :derived :valid-from valid-from} doc]]
                             (map (fn [link]
                                    [:put-docs {:into :derived-source-links
                                                :valid-from valid-from} link]))
                             links)))
             new)))
    (let [after (derived-by-ids node ids)
          mismatches (keep (fn [{:keys [doc]}]
                             (when-not (= (dissoc doc :xt/id)
                                          (some-> (after (:derived/id doc))
                                                  (dissoc :xt/id)))
                               (:derived/id doc))) built)]
      (when (seq mismatches)
        (throw (ex-info "Derived migration readback mismatch"
                        {:reason :readback-mismatch :ids (vec mismatches)})))
      {:written (count new) :existing (- (count built) (count new))})))

(defn- atomic-spit! [file value]
  (let [tmp (io/file (str file ".new"))]
    (spit tmp (pr-str value))
    (Files/move (.toPath tmp) (.toPath (io/file file))
                (into-array StandardCopyOption
                            [StandardCopyOption/REPLACE_EXISTING
                             StandardCopyOption/ATOMIC_MOVE]))))

(defn- append-lines! [file rows]
  (with-open [w (io/writer file :append true)]
    (doseq [row rows] (.write w (str (pr-str row) "\n")))))

(defn- read-state [dir]
  (let [f (io/file dir "state.edn")]
    (when (.exists f) (edn/read-string (slurp f)))))

(defn- manifest-lines [dir]
  (let [f (io/file dir "manifest.edn")]
    (if (.exists f)
      (with-open [r (io/reader f)] (doall (map edn/read-string (line-seq r))))
      [])))

(defn run!
  [node family {:keys [root run-id batch-size pause-ms dry-run? max-batches]
                :or {root default-root batch-size default-batch-size
                     pause-ms default-pause-ms}}]
  (let [spec (or (family-specs family)
                 (throw (ex-info "Unknown derived family" {:family family})))
        run-id (or run-id (str (Instant/now)))
        dir (io/file root (name family) run-id)
        _ (.mkdirs dir)
        state (read-state dir)
        started (System/nanoTime)]
    (loop [cursor (:cursor state) batches (long (or (:batches state) 0))
           seen 0 written 0 existing 0 bytes 0 timings []]
      (if-let [stop (cond (and max-batches (>= batches max-batches)) :interrupted
                          (pressure?) :paused-pressure)]
        {:status stop
         :family family :run-id run-id :batches batches :seen seen
         :written written :existing existing :bytes bytes :batch-ms timings}
        (let [rows (page-source node spec cursor batch-size)]
          (if (empty? rows)
            (let [report {:status :complete :family family :run-id run-id
                          :batches batches :seen seen :written written
                          :existing existing :bytes bytes :batch-ms timings
                          :duration-ms (/ (- (System/nanoTime) started) 1e6)}]
              (when-not dry-run? (atomic-spit! (io/file dir "complete.edn") report))
              report)
            (let [payloads (mapv (:transform spec) rows)
                  entries (mapv (fn [row payload]
                                  {:evidence-id (:evidence/id row)
                                   :derived-id (:derived/id payload)
                                   :source-body-sha256 (sha256 (:evidence/body row))
                                   :derived-body-sha256 (sha256 (:derived/body payload))})
                                rows payloads)
                  t0 (System/nanoTime)
                  result (if dry-run? {:written 0 :existing 0}
                             (write-batch! node payloads))
                  elapsed (/ (- (System/nanoTime) t0) 1e6)
                  last-row (peek rows)
                  next-cursor [(:evidence/at last-row) (:evidence/id last-row)]
                  next-batches (inc batches)]
              (when-not dry-run?
                (append-lines! (io/file dir "manifest.edn") entries)
                (append-lines! (io/file dir "batches.edn")
                               [{:batch next-batches :cursor next-cursor
                                 :count (count rows) :elapsed-ms elapsed}])
                (atomic-spit! (io/file dir "state.edn")
                              {:cursor next-cursor :batches next-batches}))
              (when (pos? pause-ms) (*sleep-fn* pause-ms))
              (recur next-cursor next-batches (+ seen (count rows))
                     (+ written (:written result)) (+ existing (:existing result))
                     (+ bytes (reduce + (map #(count (.getBytes (pr-str (:evidence/body %)) "UTF-8")) rows)))
                     (conj timings elapsed)))))))))

(defn verify!
  [node family {:keys [root run-id] :or {root default-root}}]
  (let [spec (family-specs family)
        dir (io/file root (name family) run-id)
        ;; A crash after the manifest append but before state.edn is written
        ;; replays that batch on resume, so one derived id can appear twice.
        manifest (vals (into {} (map (juxt :derived-id identity))
                             (manifest-lines dir)))
        ;; Stream the source: keep a count and the 20 rows with the lowest
        ;; id hash, never the whole family (artifact-activation is 270 MB).
        [source-count sample]
        (loop [cursor nil n 0 sample (sorted-map)]
          (let [page (page-keys node spec cursor 5000)]
            (if (empty? page) [n (hydrate node (vals sample))]
                (let [x (peek page)
                      sample (reduce (fn [m k]
                                       (let [m (assoc m (sha256 (:xt/id k)) k)]
                                         (if (> (count m) 20) (dissoc m (key (last m))) m)))
                                     sample page)]
                  (recur [(:evidence/at x) (:xt/id x)] (+ n (count page)) sample)))))
        rows (family-rows node family)
        by-id (into {} (map (juxt :derived/id identity) rows))
        bad (keep (fn [{:keys [derived-id derived-body-sha256]}]
                    (when-not (= derived-body-sha256
                                 (some-> (by-id derived-id) :derived/body sha256))
                      derived-id)) manifest)
        sample-bad (keep (fn [row]
                           (let [payload ((:transform spec) row)
                                 expected (-> (derived/build-derived-doc payload)
                                              :doc (dissoc :xt/id))]
                             (when-not (= expected
                                          (some-> (by-id (:derived/id payload))
                                                  (dissoc :xt/id)))
                               (:evidence/id row)))) sample)
        ok? (and (= source-count (count manifest) (count rows))
                 (empty? bad) (empty? sample-bad))
        receipt {:ok ok? :family family :source-count source-count
                 :manifest-count (count manifest) :derived-count (count rows)
                 :bad-ids (vec bad) :sample-bad (vec sample-bad)}]
    (atomic-spit! (io/file dir "verification.edn") receipt)
    receipt))
