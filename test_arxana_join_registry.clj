(ns test-arxana-join-registry
  (:require [clojure.test :refer [deftest is run-tests]]
            [futon1b-arxana :as a]
            [xtdb.node :as xtn]))

(defn edge [id type roles]
  {:id (str "arxana:clusion:sha256:invented-" id) :type-revision type
   :asserted-content {}
   :endpoints (mapv (fn [[role kind target]]
                      {:role role :kind kind :target {(case kind :entity :entity/id :clusion/id) target}})
                    roles)})
(defn put! [node r]
  (a/conditional-command! node {:protocol/version "arxana.v1" :message/id (:id r)
                                :operation :clusion/assert :body {:record r}}))

(deftest contract
  (let [snapshot {[:pattern/proposal 1] #{:wrong :pattern}}
        error (try (a/checked-join-shapes snapshot) nil
                   (catch clojure.lang.ExceptionInfo e (ex-data e)))]
    (is (= :arxana/join-shape-conflict (:error error)))
    (is (= [:pattern/proposal 1] (:type-revision error))))
  (is (= #{:evidence} (get a/edge-join-shapes [:hou/focus 1])))
  (is (thrown? Exception (a/checked-join-shapes {[:invalid 1] #{:only-one}}))))

(deftest catalogue-and-entity-joins
  (with-open [node (xtn/start-node)]
    (doseq [r [(edge "checked" [:design/checked-by 1] [[:choice :clusion "choice"] [:check :clusion "check"]])
               (edge "shaped" [:design/shaped-by 1] [[:choice :clusion "choice"] [:ruling :clusion "ruling"]])
               (edge "serves" [:workplan/serves 2] [[:item :entity "item"] [:benefit-line :clusion "benefit"]])
               (edge "serves2" [:workplan/serves 2] [[:item :entity "item"] [:benefit-line :clusion "benefit2"]])]]
      (put! node r))
    (let [result (a/edge-join node {:patterns [{:edge-type [:design/checked-by 1] :bind {:choice "c" :check "k"}}
                                              {:edge-type [:design/shaped-by 1] :bind {:choice "c" :ruling "r"}}]})]
      (is (= [{:c "choice" :k "check" :r "ruling"}] (mapv #(dissoc % :edges) (:rows result)))))
    (let [result (a/edge-join node {:patterns [{:edge-type [:workplan/serves 2] :bind {:item "i" :benefit-line "b"}}
                                              {:edge-type [:workplan/serves 2] :bind {:item "i" :benefit-line "b2"}}]
                                  :where {"b" "benefit" "b2" "benefit2"}})]
      (is (= [{:i "item" :b "benefit" :b2 "benefit2"}] (mapv #(dissoc % :edges) (:rows result)))))
    (is (thrown? Exception
                 (a/edge-join node {:patterns [{:edge-type [:invented/unknown 1] :bind {:a "x"}}
                                               {:edge-type [:design/checked-by 1] :bind {:choice "x"}}]})))))

(defn -main [& _]
  (let [r (run-tests 'test-arxana-join-registry)]
    (shutdown-agents)
    (when (pos? (+ (:fail r) (:error r))) (System/exit 1))))
