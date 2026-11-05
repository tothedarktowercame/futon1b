(ns futon1b-evidence-erase
  "Reviewed evidence erasure after an exact, manifested copy to :derived."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [futon1b-derived :as derived]
            [futon1b-text :as text]
            [futon1b-xt :as fxt]
            [xtdb.api :as xt])
  (:import [java.io File FileOutputStream OutputStreamWriter]
           [java.nio.channels FileChannel]
           [java.nio.file Files StandardCopyOption StandardOpenOption]
           [java.security MessageDigest]
           [java.time Instant]
           [java.util UUID]))

(def erase-max 500)
(defonce ^:private receipt-lock (Object.))

(defn- canonical [x]
  (cond
    (map? x) (into (sorted-map-by #(compare (pr-str %1) (pr-str %2)))
                   (map (fn [[k v]] [k (canonical v)])) x)
    (set? x) (vec (sort-by pr-str (map canonical x)))
    (sequential? x) (mapv canonical x)
    :else x))

(defn body-sha256 [body]
  (let [bytes (.getBytes (pr-str (canonical body)) "UTF-8")
        digest (.digest (MessageDigest/getInstance "SHA-256") bytes)]
    (apply str (map #(format "%02x" (bit-and 0xff %)) digest))))

(defn- refuse! [kind data]
  (throw (ex-info "Evidence erase refused" (assoc data :kind kind))))

(defn- manifest-files [run-dir]
  (let [dir (io/file run-dir)]
    (when-not (.isDirectory dir)
      (refuse! :migration-run-dir-unavailable {:migration-run-dir (str run-dir)}))
    (->> (.listFiles dir)
         (filter #(.isFile ^File %))
         (filter #(re-find #"(?i)manifest.*\.edn$" (.getName ^File %)))
         (sort-by #(.getName ^File %)))))

(defn- manifest-values [file]
  (with-open [reader (io/reader file)]
    (vec
     (mapcat
      (fn [[line-number line]]
        (if (str/blank? line)
          []
          (let [value (try (edn/read-string line)
                           (catch Throwable error
                             (refuse! :invalid-migration-manifest-line
                                      {:path (.getPath ^File file)
                                       :line line-number
                                       :error (.getMessage error)})))]
            (cond
              (and (map? value) (vector? (:entries value))) (:entries value)
              (vector? value) value
              (map? value) [value]
              :else (refuse! :invalid-migration-manifest-line
                             {:path (.getPath ^File file) :line line-number
                              :value value})))))
      (map-indexed (fn [i line] [(inc i) line]) (line-seq reader))))))

(defn- source-id [entry]
  (or (:source-id entry) (:evidence-id entry) (:evidence/id entry)
      (:source/evidence-id entry)))

(defn- derived-id [entry]
  (or (:derived-id entry) (:derived/id entry)))

(defn- source-body-hash [entry]
  (or (:source-body-sha256 entry) (:evidence-body-sha256 entry)
      (:body-sha256 entry) (:source/body-sha256 entry)))

(defn- derived-body-hash [entry]
  (or (:derived-body-sha256 entry) (:derived/body-sha256 entry)))

(defn- manifest-by-source [run-dir]
  (let [files (vec (manifest-files run-dir))]
    (when (empty? files)
      (refuse! :migration-manifest-unavailable
               {:migration-run-dir (str run-dir)}))
    (reduce
     (fn [result entry]
       (let [id (source-id entry)]
         (if-not (string? id)
           (refuse! :invalid-migration-manifest-entry {:entry entry})
           (if-let [prior (get result id)]
             (if (= prior entry) result
                 (refuse! :duplicate-migration-manifest-entry {:id id}))
             (assoc result id entry)))))
     {} (mapcat manifest-values files))))

(defn- current-evidence [node ids]
  (let [docs (fxt/hydrate-by-ids node :evidence ids)
        by-id (into {} (map (juxt (comp str :xt/id) identity)) docs)]
    (when-let [missing (seq (remove by-id ids))]
      (refuse! :evidence-not-current {:ids (vec missing)}))
    by-id))

(defn- validate-copy! [node ids family run-dir]
  (let [entries (manifest-by-source run-dir)
        evidence (current-evidence node ids)]
    (mapv
     (fn [id]
       (let [entry (get entries id)
             did (some-> entry derived-id str)
             copied (when did (derived/fetch-by-id node did))
             source-hash (get evidence id)
             actual-source-hash (body-sha256 (:evidence/body source-hash))
             actual-derived-hash (when copied (body-sha256 (:derived/body copied)))]
         (when-not entry
           (refuse! :migration-manifest-entry-missing {:id id}))
         (when-not copied
           (refuse! :derived-copy-missing {:id id :derived/id did}))
         (when-not (= family (:derived/family copied))
           (refuse! :derived-family-mismatch
                    {:id id :expected family :actual (:derived/family copied)}))
         (when-not (= (source-body-hash entry) actual-source-hash)
           (refuse! :source-body-hash-mismatch
                    {:id id :expected (source-body-hash entry)
                     :actual actual-source-hash}))
         (when-not (= (derived-body-hash entry) actual-derived-hash)
           (refuse! :derived-body-hash-mismatch
                    {:id id :derived/id did :expected (derived-body-hash entry)
                     :actual actual-derived-hash}))
         {:id id :derived/id did
          :source-body-sha256 actual-source-hash
          :derived-body-sha256 actual-derived-hash
          :document (get evidence id)}))
     ids)))

(defn- next-receipt-file [run-dir]
  (let [dir (io/file run-dir)
        used (into #{}
                   (keep #(some->> (.getName ^File %)
                                   (re-matches #"erase-(\d+)\.edn") second
                                   Long/parseLong))
                   (.listFiles dir))
        n (first (remove used (range 1 Long/MAX_VALUE)))]
    (io/file dir (str "erase-" n ".edn"))))

(defn- durable-write! [file value]
  (let [tmp (io/file (.getParentFile ^File file)
                     (str "." (.getName ^File file) "." (UUID/randomUUID) ".tmp"))]
    (with-open [stream (FileOutputStream. tmp)
                writer (OutputStreamWriter. stream "UTF-8")]
      (.write writer (str (pr-str value) "\n"))
      (.flush writer)
      (.sync (.getFD stream)))
    (Files/move (.toPath tmp) (.toPath ^File file)
                (into-array StandardCopyOption
                            [StandardCopyOption/ATOMIC_MOVE
                             StandardCopyOption/REPLACE_EXISTING]))
    ;; Persist the directory entry as well as the bytes. The receipt must
    ;; survive a crash at the immediately following destructive transaction.
    (with-open [directory (FileChannel/open (.toPath (.getParentFile ^File file))
                                             (into-array StandardOpenOption
                                                         [StandardOpenOption/READ]))]
      (.force directory true))
    (.getCanonicalPath ^File file)))

(defn- remaining-history
  "Ids among IDS with any row left in evidence, across all system time and
  all valid time, by one statement. Per id this was two queries at ~0.55 s
  each live (2026-11-05), 9 min for a 500-id batch; `_id IN` is one scan."
  [node ids]
  (set (map (comp str :xt/id)
            (fxt/timed-q node (into [(str "SELECT _id FROM evidence"
                                          " FOR ALL SYSTEM_TIME FOR ALL VALID_TIME"
                                          " WHERE _id IN ("
                                          (str/join "," (repeat (count ids) "?")) ")")]
                                    ids)))))

(defn- verify-store-erased! [node ids]
  (let [remaining (remaining-history node ids)]
    (when (seq remaining)
      (throw (ex-info "Evidence history remains after erase"
                      {:kind :evidence-erase-incomplete :remaining (vec (sort remaining))})))
    true))

(defn erase-evidence!
  "Erase at most 500 manifested evidence documents after verifying their
   exact derived copies. With :dry-run true, perform validation only and write
   nothing. A non-dry run durably records full current documents before the
   single XTDB erase transaction, clears all sidecar rows, and records outcome."
  [node {:keys [ids family migration-run-dir reason dry-run]}]
  (let [ids (mapv str ids)]
    (when (or (empty? ids) (not= (count ids) (count (distinct ids))))
      (refuse! :invalid-evidence-erase-ids {:ids ids}))
    (when (> (count ids) erase-max)
      (refuse! :evidence-erase-batch-too-large {:max erase-max :count (count ids)}))
    (when-not (keyword? family)
      (refuse! :invalid-evidence-erase-family {:family family}))
    (when-not (and (string? reason) (not (str/blank? reason)))
      (refuse! :invalid-evidence-erase-reason {:reason reason}))
    (let [copies (validate-copy! node ids family migration-run-dir)
          report {:ids ids :family family :reason reason :count (count ids)
                  :copies (mapv #(dissoc % :document) copies)}]
      (if dry-run
        (assoc report :dry-run true :status :validated)
        (locking receipt-lock
          (let [receipt-file (next-receipt-file migration-run-dir)
                receipt {:schema :futon1b/evidence-erase-receipt-v1
                         :status :prepared
                         :prepared-at (str (Instant/now))
                         :ids ids :family family :reason reason
                         :rows copies}
                receipt-path (durable-write! receipt-file receipt)]
            (try
              (xt/execute-tx node (mapv (fn [id] [:erase-docs :evidence id]) ids))
              (verify-store-erased! node ids)
              (let [sidecar (text/erase-evidence-sidecar! ids)
                    outcome {:status :erased :at (str (Instant/now))
                             :xtdb {:erased (count ids)} :sidecar sidecar}]
                (durable-write! receipt-file (assoc receipt :outcome outcome))
                (assoc report :status :erased :receipt receipt-path
                       :outcome outcome))
              (catch Throwable error
                (durable-write! receipt-file
                                (assoc receipt :outcome
                                       {:status :failed :at (str (Instant/now))
                                        :error (.getMessage error)
                                        :data (ex-data error)}))
                (throw error)))))))))
