(ns test-evidence-harness
  (:require [clojure.test :refer [deftest is run-tests]]
            [futon1b-evidence :as evidence]
            [test-evidence-origin :as origin]
            [xtdb.node :as xtn]))

(deftest ^:slow storage-contract
  (with-open [node (xtn/start-node)]
    (doseq [[id h] [["wm" {:kind :war-machine :basis :producer-context :execution-id "job:1"}]
                    ["none" {:kind :none :basis :producer-context :source-ref "session:plain"}]
                    ["unknown" {:kind :unknown :basis :producer-context :reason "no-context"}]
                    ["absent" nil]]]
      (let [entry (cond-> (assoc origin/entry :id id :session-id "harness-test")
                    h (assoc :harness h))]
        (is (= 201 (first (evidence/write-evidence! node entry))))
        (let [stored (evidence/public-doc (evidence/fetch-by-id node id))]
          (is (= origin/stamp (:evidence/origin stored)))
          (is (= h (:evidence/harness stored)))
          (is (= (some? h) (contains? stored :evidence/harness))))))
    (let [page (evidence/query-evidence node {"session-id" "harness-test" "limit" "100"})]
      (is (= 4 (count (:entries page))))
      (is (= 3 (count (filter #(contains? % :evidence/harness) (:entries page))))))
    (doseq [[h reason] [[{:kind :war-machine :basis :producer-context} :missing-execution-id]
                        [{:kind :unknown :basis :producer-context} :missing-harness-reason]
                        [{:kind :zai :basis :producer-context} :zai-harness-not-deployed]
                        [{:kind :none :basis :producer-context :extra true} :unexpected-harness-key]
                        [{:kind :robot :basis :producer-context} :unknown-harness-kind]
                        [{:kind :none :basis :inferred} :invalid-harness-basis]
                        [{:kind :none :basis :producer-context :execution-id 9} :invalid-harness-string]
                        [nil :invalid-harness-map]]]
      (let [[status body] (evidence/write-evidence! node (assoc origin/entry :id "refused" :harness h))]
        (is (= 400 status))
        (is (= :invalid-harness (:error/code body)))
        (is (= reason (:reason body))))
      (is (nil? (evidence/fetch-by-id node "refused"))))))

(deftest wire-normalization
  (let [h {:kind :none :basis :producer-context}]
    (is (= h (get-in (evidence/build-evidence-doc
                      (assoc origin/entry :harness {"kind" "none" "basis" "producer-context"}))
                     [:doc :evidence/harness])))
    (is (= h (get-in (evidence/build-evidence-doc
                      (assoc origin/entry :harness nil :evidence/harness h))
                     [:doc :evidence/harness])))))
(defn -main [& _]
  (let [r (run-tests 'test-evidence-harness 'test-evidence-origin)]
    (shutdown-agents)
    (System/exit (if (zero? (+ (:fail r) (:error r))) 0 1))))
