(ns test-evidence-erase
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is run-tests]]
            [futon1b-derived :as derived]
            [futon1b-evidence-erase :as erase]
            [futon1b-migrate-derived :as migrate]
            [futon1b-text :as text]
            [futon1b-xt :as fxt]
            [xtdb.api :as xt]
            [xtdb.node :as xtn])
  (:import [java.nio.file Files]
           [java.time Instant]))

(defn- temp-dir []
  (.toFile (Files/createTempDirectory
            "evidence-erase-" (make-array java.nio.file.attribute.FileAttribute 0))))

(defn- evidence-doc [id marker]
  {:xt/id id :evidence/id id :evidence/type :artifact-activation
   :evidence/claim-type :observation :evidence/author "artifact-activation"
   :evidence/session-id "erase-test" :evidence/at "2026-11-05T20:00:00Z"
   :evidence/tags [:artifact-activation :derived]
   :evidence/body {:marker marker :text (str "erasable " marker)}})

(defn- copy! [node source family]
  (let [did (str "derived:" (name family) ":" (:evidence/id source))
        body (:evidence/body source)]
    (derived/write-derived!
     node {:derived/id did :derived/family family :derived/writer "erase-test"
           :derived/writer-version "v1" :derived/source-act-ids [(:evidence/id source)]
           :derived/subject {:ref/type :evidence :ref/id (:evidence/id source)}
           :derived/at (:evidence/at source) :derived/body body})
    {:source-id (:evidence/id source) :derived-id did
     :source-body-sha256 (erase/body-sha256 body)
     :derived-body-sha256 (erase/body-sha256 body)}))

(defn- write-manifest! [dir entries]
  (spit (io/file dir "manifest-1.edn")
        (apply str (map #(str (pr-str %) "\n") entries))))

(defn- all-system-rows [node id]
  (fxt/timed-q node ["SELECT _id FROM evidence FOR ALL SYSTEM_TIME WHERE _id = ?" id]))

(deftest manifested-copy-erases-history-sidecars-and-stays-erased
  (with-open [node (xtn/start-node)]
    (let [dir (temp-dir)
          gone-a (evidence-doc "erase:a" "needle-a")
          gone-b (evidence-doc "erase:b" "needle-b")
          kept (evidence-doc "erase:kept" "needle-kept")
          ids (mapv :evidence/id [gone-a gone-b])]
      (xt/execute-tx node (mapv (fn [doc] [:put-docs :evidence doc])
                                [gone-a gone-b kept]))
      (text/init! {:path (str (io/file dir "sidecar.db"))})
      (text/catch-up! node)
      (text/backfill-docs! node)
      (write-manifest! dir [(copy! node gone-a :artifact-activation)
                            (copy! node gone-b :artifact-activation)])
      (let [saw-prepared? (atom false)
            execute xt/execute-tx
            result
            (with-redefs [xt/execute-tx
                          (fn [n ops & options]
                            (when (every? #(= :erase-docs (first %)) ops)
                              (let [receipts (filter #(re-matches #"erase-\d+\.edn"
                                                                  (.getName ^java.io.File %))
                                                     (.listFiles dir))]
                                (reset! saw-prepared?
                                        (= :prepared (:status (edn/read-string
                                                               (slurp (first receipts))))))))
                            (apply execute n ops options))]
              (erase/erase-evidence!
               node {:ids ids :family :artifact-activation
                     :migration-run-dir (.getPath dir) :reason "reviewed test family"}))
            receipt (edn/read-string (slurp (:receipt result)))]
        (is @saw-prepared? "the recoverable receipt exists before :erase-docs")
        (is (= :erased (:status result)))
        (is (= :erased (get-in receipt [:outcome :status])))
        (is (= ids (:ids receipt)))
        (is (= [gone-a gone-b] (mapv :document (:rows receipt))))
        (is (every? empty? (map #(all-system-rows node %) ids)))
        (is (some? (first (fxt/hydrate-by-ids node :evidence [(:evidence/id kept)]))))
        (is (every? zero? (vals (text/evidence-sidecar-counts ids))))
        (is (every? pos? (vals (text/evidence-sidecar-counts [(:evidence/id kept)]))))
        (is (empty? (:results (text/search node {:q "needle-a" :scope :all :limit 10}))))
        (text/catch-up! node)
        (text/reconcile! node)
        (is (every? zero? (vals (text/evidence-sidecar-counts ids))))
        (is (some? (derived/fetch-by-id node (get-in receipt [:rows 0 :derived/id]))))))))

(deftest missing-copy-and-hash-drift-refuse-the-whole-call
  (with-open [node (xtn/start-node)]
    (let [dir (temp-dir)
          a (evidence-doc "refuse:a" "a")
          b (evidence-doc "refuse:b" "b")]
      (xt/execute-tx node [[:put-docs :evidence a] [:put-docs :evidence b]])
      (text/init! {:path (str (io/file dir "sidecar.db"))})
      (text/catch-up! node)
      (let [entry (copy! node a :artifact-activation)]
        (write-manifest! dir [entry
                              {:source-id (:evidence/id b)
                               :derived-id "derived:artifact-activation:missing"
                               :source-body-sha256
                               (erase/body-sha256 (:evidence/body b))
                               :derived-body-sha256
                               (erase/body-sha256 (:evidence/body b))}])
        (is (= :derived-copy-missing
               (try
                 (erase/erase-evidence!
                  node {:ids [(:evidence/id a) (:evidence/id b)]
                        :family :artifact-activation :migration-run-dir (.getPath dir)
                        :reason "missing copy must refuse"})
                 nil
                 (catch clojure.lang.ExceptionInfo error (:kind (ex-data error))))))
        (write-manifest! dir [(assoc entry :source-body-sha256 (apply str (repeat 64 "0")))])
        (is (= :source-body-hash-mismatch
               (try
                 (erase/erase-evidence!
                  node {:ids [(:evidence/id a) (:evidence/id b)]
                        :family :artifact-activation :migration-run-dir (.getPath dir)
                        :reason "must refuse atomically"})
                 nil
                 (catch clojure.lang.ExceptionInfo error (:kind (ex-data error))))))
        (is (= #{(:evidence/id a) (:evidence/id b)}
               (set (map (comp str :xt/id)
                         (fxt/hydrate-by-ids node :evidence
                                             [(:evidence/id a) (:evidence/id b)])))))
        (is (empty? (filter #(re-matches #"erase-\d+\.edn" (.getName ^java.io.File %))
                            (.listFiles dir))))))))

(deftest dry-run-validates-without-writing
  (with-open [node (xtn/start-node)]
    (let [dir (temp-dir)
          doc (evidence-doc "dry:a" "dry")]
      (xt/execute-tx node [[:put-docs :evidence doc]])
      (text/init! {:path (str (io/file dir "sidecar.db"))})
      (write-manifest! dir [(copy! node doc :artifact-activation)])
      (is (= :validated
             (:status (erase/erase-evidence!
                       node {:ids [(:evidence/id doc)] :family :artifact-activation
                             :migration-run-dir (.getPath dir) :reason "count first"
                             :dry-run true}))))
      (is (some? (first (fxt/hydrate-by-ids node :evidence [(:evidence/id doc)]))))
      (is (empty? (filter #(re-matches #"erase-\d+\.edn" (.getName ^java.io.File %))
                          (.listFiles dir))))
      (is (= :evidence-erase-batch-too-large
             (try
               (erase/erase-evidence!
                node {:ids (mapv #(str "too-many:" %) (range 501))
                      :family :artifact-activation :migration-run-dir (.getPath dir)
                      :reason "must be reviewed in chunks" :dry-run true})
               nil
               (catch clojure.lang.ExceptionInfo error (:kind (ex-data error)))))))))

(deftest history-check-sees-every-version-the-erase-must-remove
  ;; The bad case the post-erase check exists for: a row whose only version is
  ;; in past valid time, and a row replaced by a later version. A check that
  ;; reads only the current row would miss both.
  (with-open [node (xtn/start-node)]
    (let [remaining #'erase/remaining-history
          past (evidence-doc "hist:past" "p")
          twice (evidence-doc "hist:twice" "t1")]
      (xt/execute-tx node [[:put-docs {:into :evidence
                                       :valid-from (Instant/parse "2026-01-01T00:00:00Z")
                                       :valid-to (Instant/parse "2026-02-01T00:00:00Z")}
                            past]])
      (xt/execute-tx node [[:put-docs :evidence twice]])
      (xt/execute-tx node [[:put-docs :evidence (assoc-in twice [:evidence/body :marker] "t2")]])
      (is (= #{"hist:past" "hist:twice"} (remaining node ["hist:past" "hist:twice" "hist:none"])))
      (xt/execute-tx node [[:erase-docs :evidence "hist:past"] [:erase-docs :evidence "hist:twice"]])
      (is (empty? (remaining node ["hist:past" "hist:twice"]))))))

(deftest migration-manifest-drives-the-erase
  ;; The real producer of the manifest, not a hand-written one: a field-name
  ;; or hash drift between C and D refuses here rather than on the live store.
  (with-open [node (xtn/start-node)]
    (let [dir (temp-dir)
          rows (mapv (fn [n]
                       (let [id (format "aa-%03d" n)]
                         {:xt/id id :evidence/id id :evidence/type :artifact/weak-activation
                          :evidence/claim-type :observation
                          :evidence/author "futon3c/artifact-activation"
                          :evidence/at (str (Instant/ofEpochMilli (+ 1793900000000 n)))
                          :evidence/tags [:artifact-retrieval :weak-activation]
                          :evidence/body {:artifact {:kind :invoke :id (str "i-" n)}
                                          :text (str "prompt " n) :text-truncated? false
                                          :text-sha256 "x" :hits []}}))
                     (range 12))]
      (xt/execute-tx node (mapv (fn [d] [:put-docs :evidence d]) rows))
      (text/init! {:path (str (io/file dir "sidecar.db"))})
      (text/catch-up! node)
      (binding [migrate/*pressure-fn* (constantly {:permits/available 4 :permits/waiters 0})
                migrate/*sleep-fn* (constantly nil)]
        (is (= :complete (:status (migrate/run! node :artifact-activation
                                                {:root (.getPath dir) :run-id "r"
                                                 :batch-size 5 :pause-ms 0})))))
      (let [run-dir (io/file dir "artifact-activation" "r")
            ids (mapv :evidence/id rows)
            result (erase/erase-evidence!
                    node {:ids ids :family :artifact-activation
                          :migration-run-dir (.getPath run-dir) :reason "integration"})]
        (is (= :erased (:status result)))
        (is (empty? (#'erase/remaining-history node ids)))
        (is (every? zero? (vals (text/evidence-sidecar-counts ids))))
        (is (= 12 (count (fxt/safe-q node '(from :derived [xt/id])))))))))

(defn -main [& _]
  (let [result (run-tests 'test-evidence-erase)]
    (shutdown-agents)
    (System/exit (if (zero? (+ (:fail result) (:error result))) 0 1))))
