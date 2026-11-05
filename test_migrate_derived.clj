(ns test-migrate-derived
  (:require [clojure.test :refer [deftest is run-tests testing]]
            [futon1b-migrate-derived :as migrate]
            [futon1b-xt :as fxt]
            [xtdb.api :as xt]
            [xtdb.node :as xtn])
  (:import [java.nio.file Files]
           [java.time Instant]))

(defn- full-text [n] (str "copied prompt " n " DISTINCTIVE-TEXT"))

(defn- activation
  "Rows as the pre-f75fb0fff writer stored them: :text-sha256 of the FULL text,
  and every 7th row's :text cut to a prefix with :text-truncated? true. Rows
  from 1150 on carry no hash, as the oldest rows may not."
  [n]
  (let [id (format "activation-%04d" n)
        at (str (Instant/ofEpochMilli (+ 1793900000000 n)))
        truncated? (zero? (mod n 7))
        text (cond-> (full-text n) truncated? (subs 0 10))]
    (cond->
     {:xt/id id :evidence/id id :evidence/type :artifact/weak-activation
     :evidence/claim-type :observation
     :evidence/author "futon3c/artifact-activation" :evidence/at at
     :evidence/subject {:ref/type :invoke :ref/id (str "invoke-" n)}
     :evidence/tags [:artifact-retrieval :weak-activation]
     :evidence/body {:artifact {:kind :invoke :id (str "invoke-" n)}
                     :text text :text-truncated? truncated?
                     :retrieval {:model "test"} :hits [] :cited [] :weak []}}
      (< n 1150) (assoc-in [:evidence/body :text-sha256]
                           (migrate/sha256-text (full-text n)))))) 

(defn- unrelated [n]
  {:xt/id (str "other-" n) :evidence/id (str "other-" n)
   :evidence/type :operator-turn :evidence/claim-type :observation
   :evidence/author "joe" :evidence/at (str (Instant/ofEpochMilli (+ 1793901000000 n)))
   :evidence/tags [] :evidence/body {:text "keep me"}})

(defn- put-docs! [node docs]
  (doseq [chunk (partition-all 500 docs)]
    (xt/execute-tx node (mapv (fn [doc] [:put-docs :evidence doc]) chunk))))

(defn- table-count [node table]
  (count (fxt/safe-q node (list 'from table ['xt/id]))))

(deftest artifact-family-migration-is-resumable-idempotent-and-verifiable
  (with-open [node (xtn/start-node)]
    (let [source (mapv activation (range 1200))
          others (mapv unrelated (range 50))
          root (.toString (Files/createTempDirectory
                           "derived-migration-test"
                           (make-array java.nio.file.attribute.FileAttribute 0)))
          opts {:root root :run-id "run-1" :batch-size 200 :pause-ms 0}]
      (put-docs! node (into source others))
      (binding [migrate/*pressure-fn* (constantly {:permits/available 4
                                                    :permits/waiters 0})
                migrate/*sleep-fn* (constantly nil)]
        (testing "two batches can stop and the retained cursor resumes"
          (let [first-run (migrate/run! node :artifact-activation
                                        (assoc opts :max-batches 2))]
            (is (= :interrupted (:status first-run)))
            (is (= 400 (table-count node :derived))))
          (let [resumed (migrate/run! node :artifact-activation opts)]
            (println "[derived-migration-test] resumed batch ms" (:batch-ms resumed))
            (is (= :complete (:status resumed)))
            (is (= 1200 (table-count node :derived)))
            (is (= 1200 (table-count node :derived-source-links)))))

        (testing "text is replaced and unrelated evidence remains untouched"
          (let [rows (fxt/safe-q node '(from :derived [*]))]
            (is (every? #(nil? (get-in % [:derived/body :text])) rows))
            (let [by-n (into {} (map (fn [r] [(parse-long (subs (:derived/id r) 39)) r])) rows)
                  sha (fn [n] (get-in (by-n n) [:derived/body :text-sha256]))]
              (testing "a stored full-text hash is kept, never recomputed from a prefix"
                (is (= (migrate/sha256-text (full-text 7)) (sha 7)))
                (is (every? #(= (migrate/sha256-text (full-text %)) (sha %))
                            (range 1150))))
              (testing "a complete text without a hash gets the writer's hash"
                (is (= (migrate/sha256-text (full-text 1151)) (sha 1151))))
              (testing "a truncated text without a hash gets none, and says so"
                (is (nil? (sha 1155)))
                (is (true? (get-in (by-n 1155) [:derived/body :text-truncated?]))))
              (is (not-any? #(contains? (:derived/body (by-n %)) :text-truncated?)
                            (range 1150))))
            (is (= 1250 (table-count node :evidence)))
            (is (= 50 (count (fxt/safe-q
                              node '(-> (from :evidence [evidence/author])
                                        (where (= evidence/author "joe")))))))))

        (testing "rerun is a no-op and verification passes"
          ;; A crash between the manifest append and state.edn replays a batch.
          (spit (str root "/artifact-activation/run-1/manifest.edn")
                (first (line-seq (java.io.BufferedReader.
                                  (java.io.FileReader.
                                   (str root "/artifact-activation/run-1/manifest.edn")))))
                :append true)
          (spit (str root "/artifact-activation/run-1/manifest.edn") "\n" :append true)
          (let [again (migrate/run! node :artifact-activation
                                    (assoc opts :run-id "rerun"))
                receipt (migrate/verify! node :artifact-activation
                                         {:root root :run-id "run-1"})]
            (is (= :complete (:status again)))
            (is (zero? (:written again)))
            (is (= 1200 (:existing again)))
            (is (:ok receipt))
            (is (= 1200 (:manifest-count receipt)))))

        (testing "dry-run changes neither table"
          (let [before [(table-count node :evidence) (table-count node :derived)]
                result (migrate/run! node :artifact-activation
                                     {:root root :run-id "dry" :batch-size 200
                                      :pause-ms 0 :dry-run? true})]
            (is (= :complete (:status result)))
            (is (= before [(table-count node :evidence) (table-count node :derived)]))))

        (testing "tampering is detected"
          (let [id "derived:artifact-activation:activation-0000"
                row (first (fxt/safe-q
                            node (fxt/pq '[p-id]
                                         '(-> (from :derived [*])
                                              (where (= xt/id p-id))) id)))]
            (xt/execute-tx node [[:put-docs {:into :derived
                                             :valid-from (Instant/parse "2026-11-05T22:00:00Z")}
                                   (assoc row :derived/body {:tampered true})]])
            (is (false? (:ok (migrate/verify! node :artifact-activation
                                              {:root root :run-id "run-1"}))))))))))

(defn -main [& _]
  (let [result (run-tests 'test-migrate-derived)]
    (shutdown-agents)
    (System/exit (if (zero? (+ (:fail result) (:error result))) 0 1))))
