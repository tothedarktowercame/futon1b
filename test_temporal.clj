(ns test-temporal
  "Phase 3 bitemporal memory projection tests.

   Run: clojure -M:node -m test-temporal"
  (:require [clojure.test :refer [deftest is run-tests testing]]
            [futon1b-graph :as graph]
            [futon1b-server :as server]
            [xtdb.node :as xtn])
  (:import [java.time Instant]))

(def ^:dynamic *node* nil)

(defn- state-at
  [endpoint valid-as-of]
  (get-in
   (graph/hyperedges-query
    *node*
    (cond-> {:end endpoint :type :memory/assert :limit 10}
      valid-as-of (assoc :valid-as-of valid-as-of)))
   [:hyperedges 0 :hx/props :state]))

(deftest current-and-as-of-memory-projections-disagree
  (let [t1 (.minusSeconds (Instant/now) 120)
        t2 (.minusSeconds (Instant/now) 60)
        base {:hx/id "hx-temporal-memory"
              :hx/type :memory/assert
              :hx/endpoints ["e-temporal" "pattern/temporal"]
              :hx/props {:roles {:entry "e-temporal"}
                         :state :current}}]
    (is (:ok (server/upsert-hyperedge!
              *node* (assoc base :hx/valid-time (str t1)))))
    (is (:ok (server/upsert-hyperedge!
              *node* (-> base
                         (assoc :hx/valid-time (str t2))
                         (assoc-in [:hx/props :state] :superseded)))))
    (is (= :current (state-at "pattern/temporal" (.plusSeconds t1 1))))
    (is (= :superseded (state-at "pattern/temporal" nil)))))

(deftest end-valid-time-removes-current-but-preserves-history
  (let [t1 (.minusSeconds (Instant/now) 120)
        t2 (.minusSeconds (Instant/now) 60)
        edge {:hx/id "hx-temporal-retract"
              :hx/type :memory/assert
              :hx/endpoints ["e-retract" "pattern/retract"]
              :hx/props {:state :current}}]
    (is (:ok (server/upsert-hyperedge!
              *node* (assoc edge :hx/valid-time (str t1)))))
    (is (:ok (server/upsert-hyperedge!
              *node* (assoc edge :hx/op "retract"
                            :hx/valid-time (str t2)))))
    (is (nil? (state-at "pattern/retract" nil)))
    (is (= :current (state-at "pattern/retract" (.plusSeconds t1 1))))))

(deftest temporal-input-is-strict
  (testing "bad temporal directives fail instead of degrading to current time"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"invalid temporal instant"
         (#'server/parse-instant "not-an-instant")))))

(deftest evidence-append-does-not-invalidate-current-projection
  (let [endpoint "pattern/projection-generation-evidence"
        before (graph/memory-projection-components
                *node* {:endpoints [endpoint] :limit 3})]
    (is (= :ok
           (graph/put-verified!
            *node* :evidence
            {:xt/id "e-unrelated-projection-generation"
             :evidence/id "e-unrelated-projection-generation"
             :evidence/type :observation
             :evidence/claim-type :observation
             :evidence/author "projection-generation-test"
             :evidence/at "2026-07-26T12:00:00Z"
             :evidence/body {:note "unrelated evidence append"}
             :evidence/tags [:test]})))
    (with-redefs [graph/initialize-memory-projection!
                  (fn [_]
                    (throw (ex-info "unexpected projection rebuild" {})))]
      (let [after (graph/memory-projection-components
                   *node* {:endpoints [endpoint] :limit 3})]
        (is (= (get-in before [:temporal-basis :projection-revision])
               (get-in after [:temporal-basis :projection-revision])))
        (is (= (get-in before [:temporal-basis :projection-generation])
               (get-in after [:temporal-basis :projection-generation])))))))

(deftest memory-edge-write-advances-projection-generation
  (let [endpoint "pattern/projection-generation-memory"
        memory-id "e-projection-generation-memory"
        edge-id "hx-projection-generation-memory"
        before (graph/memory-projection-components
                *node* {:endpoints [endpoint] :limit 3})]
    (is (= :ok
           (graph/put-verified!
            *node* :evidence
            {:xt/id memory-id
             :evidence/id memory-id
             :evidence/type :memory
             :evidence/claim-type :observation
             :evidence/author "projection-generation-test"
             :evidence/session-id "projection-generation-session"
             :evidence/at "2026-07-26T12:01:00Z"
             :evidence/body {:hook "Projection generation hook"}
             :evidence/tags [:memory]})))
    (is (:ok
         (server/upsert-hyperedge!
          *node*
          {:hx/id edge-id
           :hx/type :memory/assert
           :hx/endpoints [memory-id endpoint]
           :hx/props {:domain :mathematics
                      :state :current
                      :attachment-status :reviewed
                      :roles {:entry memory-id
                              :patterns [endpoint]}}})))
    (let [result
          (graph/memory-projection-components
           *node* {:endpoints [endpoint] :limit 3})]
      (is (= [memory-id]
             (mapv #(get-in % [:entry :evidence/id])
                   (get-in result [:groups 0 :components]))))
      (is (= (inc (get-in before
                          [:temporal-basis :projection-generation]))
             (get-in result
                     [:temporal-basis :projection-generation]))))))

(deftest historical-projection-bypasses-current-index
  (with-redefs [graph/initialize-memory-projection!
                (fn [_]
                  (throw (ex-info "historical projection touched current index"
                                  {})))]
    (is (= :as-of
           (get-in
            (graph/memory-projection-components
             *node*
             {:endpoints ["pattern/historical-generation-bypass"]
              :limit 3
              :valid-as-of (.minusSeconds (Instant/now) 30)})
            [:temporal-basis :mode])))))

(defn -main [& _]
  (with-open [node (xtn/start-node)]
    (binding [*node* node]
      (let [{:keys [fail error]} (run-tests 'test-temporal)]
        (shutdown-agents)
        (System/exit (if (zero? (+ fail error)) 0 1))))))
