(ns test-arxana-batch-reads
  (:require [clojure.test :refer [deftest is run-tests]]
            [futon1b-arxana :as a] [futon1b-arxana-index :as idx]
            [xtdb.api :as xt] [xtdb.node :as node]))
(deftest batch-reads
  (with-open [n (node/start-node {})]
    (let [file (java.io.File/createTempFile "batch-index" ".db") index (idx/open-index (.getPath file))
          at "2026-08-27T12:00:00.000Z"
          record (fn [i] {:id (str "arxana:clusion:sha256:test-" i) :type-revision [:invented/link 1]
                          :event-time at :asserted-content {:tag [:x :y]}
                          :endpoints [{:role :item :kind :clusion :target {:clusion/id (str "item-" i)}}]})
          records (mapv record (range 201))]
      (doseq [r records]
        (xt/execute-tx n (into [[:put-docs {:into :arxana_clusions :valid-from (java.time.Instant/parse at)}
                               {:xt/id (:id r) :record/data (pr-str r) :clusion/type :invented/link :clusion/type-version 1}]]
                              (map (fn [row] [:put-docs {:into :arxana_clusion_fields :valid-from (java.time.Instant/parse at)} row]) (a/clusion-field-rows r)))))
      (let [r (a/records-by-ids n {:record-ids (conj (mapv :id records) "absent") :valid-time "2026-09-01T00:00:00.000Z"})]
        (is (= 202 (count (:results r))))
        (is (= records (mapv :record (filter #(= :ok (:status %)) (:results r)))))
        (is (= :missing (:status (last (:results r))))))
      (is (every? #(= :missing (:status %)) (:results (a/records-by-ids n {:record-ids (mapv :id records) :valid-time "2026-08-01T00:00:00.000Z"}))))
      (is (= [] (:results (a/records-by-ids n {:record-ids []}))))
      (is (thrown? clojure.lang.ExceptionInfo (a/records-by-ids n {:record-ids (vec (repeat 1001 "x"))})))
      (is (= 201 (:total (a/count-by-field n {:type-revision [:invented/link 1] :field :tag :value :x}))))
      (is (= 0 (:total (a/count-by-field n {:type-revision [:invented/link 1] :field :tag :value :z}))))
      (doseq [i (range 201) j (range 2)]
        (xt/execute-tx n [[:put-docs :arxana_evidence
                          {:xt/id (str "att-" i "-" j) :record/data (pr-str {:id (str "att-" i "-" j) :subject-clusion-id (str "item-" i) :event-time at})}]]))
      (idx/rebuild! index n)
      (doseq [kind [:attestations-by-subjects :role-reverse-many]]
        (let [ids (conj (mapv #(str "item-" %) (range 201)) "absent")
              q {:query/type kind :subject-ids ids :endpoint-ids ids :edge-type [:invented/link 1] :endpoint-role :item :limit 1}
              r (idx/many-sets index n q)]
          (is (= :ok (:status r))) (is (= 202 (count (:results r))))
          (is (= 0 (:total (last (:results r)))))
          (let [counts (idx/many-sets index n (assoc q :limit 0))]
            (is (= (:total (first (:results r))) (:total (first (:results counts))))))
          (is (thrown? clojure.lang.ExceptionInfo (idx/many-sets index n (assoc q :limit 201))))
          (is (= (if (= kind :attestations-by-subjects) 2 1) (:total (first (:results r)))))
          (is (= (= kind :attestations-by-subjects) (:truncated? (first (:results r)))))
          (is (= [] (:results (idx/many-sets index n (assoc q :subject-ids [] :endpoint-ids [])))))))
      (xt/execute-tx n [[:put-docs :arxana_index_state {:xt/id "source" :source/sequence 10}]])
      (doseq [kind [:attestations-by-subjects :role-reverse-many]]
        (is (= :stale-index (:status (idx/many-sets index n {:query/type kind :subject-ids ["item-0"]
                                                            :endpoint-ids ["item-0"] :edge-type [:invented/link 1] :endpoint-role :item})))) ))))
(defn -main [& _]
  (let [r (run-tests 'test-arxana-batch-reads)] (shutdown-agents)
    (when (pos? (+ (:fail r) (:error r))) (System/exit 1))))
