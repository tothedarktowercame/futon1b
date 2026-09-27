(ns test-evidence-origin
  (:require [clojure.test :refer [deftest is run-tests]]
            [futon1b-evidence :as evidence]
            [xtdb.node :as xtn]))
(def stamp {:kind :harness :actor "parked-resume" :writer "p6o-test"
            :attributed-author "joe" :authorization :unknown
            :recorded-at "2026-09-27T00:00:00Z" :basis :write-time})
(def entry {:id "p6o-origin" :type :coordination :claim-type :step :author "joe"
            :body {:event "p6o-test"} :origin stamp})
(deftest origin-contract
  (is (= "grant:123" (get-in (evidence/build-evidence-doc (assoc-in entry [:origin :authorization] "grant:123"))
                            [:doc :evidence/origin :authorization])))
  (is (not (contains? (:doc (evidence/build-evidence-doc (dissoc entry :origin))) :evidence/origin)))
  (doseq [bad [nil (assoc stamp :kind :robot) (assoc stamp :extra true)
               (assoc stamp :basis :inferred) (assoc stamp :recorded-at "junk")
               (assoc stamp :attributed-author "someone-else") (dissoc stamp :writer)]]
    (is (= :invalid-origin (get-in (evidence/build-evidence-doc (assoc entry :origin bad)) [:invalid :error/code])))))
(deftest ^:slow roundtrip
  (with-open [node (xtn/start-node)]
    (is (= 201 (first (evidence/write-evidence! node entry))))
    (is (= stamp (:evidence/origin (evidence/fetch-by-id node "p6o-origin"))))
    (is (= "joe" (:evidence/author (evidence/fetch-by-id node "p6o-origin"))))
    (let [[status body] (evidence/write-evidence! node (assoc entry :origin (assoc stamp :kind :robot)))]
      (is (= 400 status)) (is (= :invalid-origin (:error/code body))))))
(defn -main [& _]
  (let [r (run-tests 'test-evidence-origin)]
    (shutdown-agents)
    (System/exit (if (zero? (+ (:fail r) (:error r))) 0 1))))
