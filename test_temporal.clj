(ns test-temporal
  "Phase 3 bitemporal memory projection tests.

   Run: clojure -M:node -m test-temporal"
  (:require [clojure.test :refer [deftest is run-tests testing]]
            [futon1b-graph :as graph]
            [futon1b-server :as server]
            [futon1b-xt :as fxt]
            [xtdb.api :as xt]
            [xtdb.node :as xtn])
  (:import [java.time Instant]))

(def ^:dynamic *node* nil)

(deftest hyperedge-payload-normalization
  (testing "plain string ends retain their existing document and stable id"
    (is (= {:xt/id "hx:test/edge:a.b"
            :hx/id "hx:test/edge:a.b"
            :hx/type :test/edge
            :hx/endpoints ["a" "b"]
            :hx/ends [{:entity-id "a"} {:entity-id "b"}]}
           (server/build-hyperedge-doc
            {:hx/type :test/edge :hx/endpoints ["a" "b"]}))))
  (testing "map ends contribute entity ids to identity and retain only roles"
    (is (= {:xt/id "hx:test/edge:entity/a.entity/b"
            :hx/id "hx:test/edge:entity/a.entity/b"
            :hx/type :test/edge
            :hx/endpoints ["entity/a" "entity/b"]
            :hx/ends [{:entity-id "entity/a" :role :source}
                      {:entity-id "entity/b" :role :target}]}
           (server/build-hyperedge-doc
            {:hx/type :test/edge
             :hx/endpoints [{:entity-id "entity/a" :role :source :ignored true}
                            {:entity-id "entity/b" :role "target"}]}))))
  (testing "a map end without a string entity id is a layer-4 rejection"
    (let [failure (try
                    (server/build-hyperedge-doc
                     {:hx/type :test/edge :hx/endpoints [{:role :target}]})
                    (catch clojure.lang.ExceptionInfo e e))]
      (is (= {:layer 4
              :reason :invalid-hyperedge-end
              :context {:end {:role :target}
                        :required :entity-id
                        :expected :string}}
             (:error (ex-data failure)))))))

(defn- state-at
  [endpoint valid-as-of]
  (get-in
   (graph/hyperedges-query
    *node*
    (cond-> {:end endpoint :type :memory/assert :limit 10}
      valid-as-of (assoc :valid-as-of valid-as-of)))
   [:hyperedges 0 :hx/props :state]))

(deftest projection-gate-waits-for-a-real-indexing-backlog
  (let [payload (apply str (repeat 1000 "x"))
        docs (mapv (fn [i]
                     {:xt/id (str "quiescence-backlog-" i)
                      :payload payload})
                   (range 20000))
        submit (future
                 (xt/submit-tx *node* [(into [:put-docs :docs] docs)]))
        observed-backlog?
        (loop [tries 0]
          (let [{:keys [latest-submitted-byte-offset
                        latest-processed-byte-offset]}
                (graph/restart-readiness-status *node*)]
            (cond
              (> latest-submitted-byte-offset latest-processed-byte-offset)
              true

              (or (realized? submit) (>= tries 5000))
              false

              :else
              (do (Thread/sleep 1) (recur (inc tries))))))]
    (is observed-backlog? "the test must exercise a real submitted/processed gap")
    (let [result (graph/wait-for-indexing-quiescence! *node* 30000 1)]
      (is (= "bytes" (:unit result)))
      (is (<= (:latest-submitted-byte-offset result)
              (:latest-processed-byte-offset result))))
    @submit))

(deftest projection-gate-times-out-loudly
  (with-redefs [graph/restart-readiness-status
                (constantly {:unit "bytes"
                             :latest-submitted-byte-offset 200
                             :latest-processed-byte-offset 100})]
    (let [failure (try
                    (graph/wait-for-indexing-quiescence! ::node 0 1)
                    nil
                    (catch clojure.lang.ExceptionInfo e e))]
      (is (= :indexing-quiescence-timeout
             (get-in (ex-data failure) [:error :reason])))
      (is (= "bytes" (get-in (ex-data failure)
                              [:error :context :unit]))))))

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

(deftest moving-watermark-does-not-rebuild-projection
  ;; 2026-09-29 outage: evidence appends moved the global watermark inside
  ;; every point refresh, and each memory write fell back to a full rebuild
  ;; (80-250 s x 5 attempts) under the node lock.
  (let [endpoint "pattern/projection-watermark-moves"
        memory-id "e-projection-watermark-moves"
        edge-id "hx-projection-watermark-moves"
        ticks (atom 0)]
    (graph/memory-projection-components *node* {:endpoints [endpoint] :limit 3})
    (is (= :ok
           (graph/put-verified!
            *node* :evidence
            {:xt/id memory-id
             :evidence/id memory-id
             :evidence/type :memory
             :evidence/claim-type :observation
             :evidence/author "projection-watermark-test"
             :evidence/session-id "projection-watermark-session"
             :evidence/at "2026-09-29T00:00:00Z"
             :evidence/body {:hook "Watermark moves hook"}
             :evidence/tags [:memory]})))
    (with-redefs-fn
      {#'graph/node-watermark (fn [_] {:tick (swap! ticks inc)})
       #'graph/initialize-memory-projection!
       (fn [_] (throw (ex-info "unexpected projection rebuild" {})))}
      (fn []
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
        (is (= [memory-id]
               (mapv #(get-in % [:entry :evidence/id])
                     (get-in (graph/memory-projection-components
                              *node* {:endpoints [endpoint] :limit 3})
                             [:groups 0 :components]))))))))

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

(defn- fake-hydrated-memory
  [edge-id]
  {:hyperedge-id edge-id
   :hx/type :memory/assert
   :hx/endpoints [(str "e-" edge-id) "pattern/versioned"]
   :hx/props {:roles {:entry (str "e-" edge-id)}}
   :memory-id (str "e-" edge-id)
   :evidence/id (str "e-" edge-id)
   :evidence/type :memory
   :evidence/claim-type :observation
   :evidence/author "temporal-test"
   :evidence/session-id "temporal-test"
   :evidence/body {:hook "Version-aware projection test"}})

(deftest historical-version-rows-count-once
  (let [captured (atom nil)
        version-rows
        [{:xt/id "hx-versioned-a" :matched-endpoint "pattern/versioned"}
         {:xt/id "hx-versioned-a" :matched-endpoint "pattern/versioned"}
         {:xt/id "hx-versioned-a" :matched-endpoint "pattern/versioned"}]]
    (with-redefs-fn
      {#'fxt/safe-q
       (fn [_ query]
         (reset! captured query)
         version-rows)
       #'graph/hydrate-memory-components
       (fn [_ edge-ids _]
         (mapv fake-hydrated-memory edge-ids))}
      (fn []
        (let [result
              (graph/memory-projection-components
               ::versioned-node
               {:endpoints ["pattern/versioned"]
                :limit 1
                :system-as-of (Instant/now)})]
          (is (= ["hx-versioned-a"]
                 (mapv :hyperedge-id
                       (get-in result [:groups 0 :components]))))
          (is (= 1 (get-in result [:audit :selected-row-count])))
          ;; safe-q receives the parameterised vector `[(fn params body) & args]`
          ;; (fxt/pq): the endpoints ride as args, the shape is in `body`.
          (let [[[_ params body] & args] @captured]
            (is (= '[p-ep0] params))
            (is (= ["pattern/versioned"] (vec args)))
            (is (= '(aggregate xt/id matched-endpoint)
                   (nth body 5)))
            (is (= '(limit 2) (last body)))))))))

(deftest system-time-versions-of-one-memory-count-once
  (let [endpoint "pattern/system-version-history"
        memory-id "e-system-version-history"
        edge-id "hx-system-version-history"]
    (is (= :ok
           (graph/put-verified!
            *node* :evidence
            {:xt/id memory-id
             :evidence/id memory-id
             :evidence/type :memory
             :evidence/claim-type :observation
             :evidence/author "temporal-test"
             :evidence/session-id "temporal-test"
             :evidence/at "2026-07-31T12:00:00Z"
             :evidence/body {:hook "System-time version history"}
             :evidence/tags [:memory :test]})))
    (doseq [state [:first :second :third]]
      (is (:ok
           (server/upsert-hyperedge!
            *node*
            {:hx/id edge-id
             :hx/type :memory/assert
             :hx/endpoints [memory-id endpoint]
             :hx/props {:state state
                        :attachment-status :reviewed
                        :roles {:entry memory-id
                                :patterns [endpoint]}}}))))
    (let [result
          (graph/memory-projection-components
           *node*
           {:endpoints [endpoint]
            :limit 1
            :system-as-of (Instant/now)})]
      (is (= [edge-id]
             (mapv :hyperedge-id
                   (get-in result [:groups 0 :components]))))
      (is (= 1 (get-in result [:audit :distinct-edge-count]))))))

(deftest out-of-range-system-time-projection-is-empty
  (let [result
        (graph/memory-projection-components
         *node*
         {:endpoints ["pattern/versioned"]
          :limit 1
          :system-as-of (Instant/parse "2020-01-01T00:00:00Z")})]
    (is (= :as-of (get-in result [:temporal-basis :mode])))
    (is (empty? (get-in result [:groups 0 :components])))))

(deftest distinct-projection-results-still-trip-bound
  (let [selection-rows
        [{:xt/id "hx-distinct-a" :matched-endpoint "pattern/versioned"}
         {:xt/id "hx-distinct-b" :matched-endpoint "pattern/versioned"}]
        error
        (with-redefs [fxt/safe-q (fn [_ _] selection-rows)]
          (try
            (graph/memory-projection-components
             ::bounded-node
             {:endpoints ["pattern/versioned"]
              :limit 1
              :system-as-of (Instant/now)})
            nil
            (catch clojure.lang.ExceptionInfo e
              e)))]
    (is (= :memory-projection-result-bound-exceeded
           (get-in (ex-data error) [:error :reason])))
    (is (= {:endpoint-count 1
            :per-endpoint-limit 1
            :maximum 1}
           (get-in (ex-data error) [:error :context])))))

(defn- put-test-memory!
  [endpoint suffix]
  (let [memory-id (str "e-current-limit-" suffix)
        edge-id (str "hx-current-limit-" suffix)]
    (is (= :ok
           (graph/put-verified!
            *node* :evidence
            {:xt/id memory-id
             :evidence/id memory-id
             :evidence/type :memory
             :evidence/claim-type :observation
             :evidence/author "temporal-test"
             :evidence/session-id "temporal-test"
             :evidence/at "2026-07-31T12:00:00Z"
             :evidence/body {:hook "Current projection limit test"}
             :evidence/tags [:memory :test]})))
    (is (:ok
         (server/upsert-hyperedge!
          *node*
          {:hx/id edge-id
           :hx/type :memory/assert
           :hx/endpoints [memory-id endpoint]
           :hx/props {:attachment-status :reviewed
                      :roles {:entry memory-id
                              :patterns [endpoint]}}})))))

(deftest current-projection-limit-behaviour-is-unchanged
  (let [endpoint "pattern/current-limit"]
    (doseq [suffix ["a" "b" "c"]]
      (put-test-memory! endpoint suffix))
    (let [result
          (graph/memory-projection-components
           *node* {:endpoints [endpoint] :limit 2})]
      (is (= :current (get-in result [:temporal-basis :mode])))
      (is (= 2 (count (get-in result [:groups 0 :components]))))
      (is (= 2 (get-in result [:groups 0 :audit :selected-count]))))))

(deftest typed-entity-limit-is-pushed-into-xtdb
  (let [captured (atom [])
        returned [{:xt/id "entity-a"
                   :entity/id "entity-a"
                   :entity/name "A"
                   :entity/type :generation-test}
                  {:xt/id "entity-b"
                   :entity/id "entity-b"
                   :entity/name "B"
                   :entity/type :generation-test}]
        expected (mapv #(dissoc % :xt/id) returned)]
    (with-redefs [fxt/safe-q (fn [_ form]
                               (swap! captured conj form)
                               returned)]
      (is (= {:entities expected :count 2 :next-cursor "entity-b"}
             (graph/entities-query
              ::capturing-node {:type :generation-test :limit 2}))))
    ;; Parameterised (fxt/pq): the limit is pushed down as `(limit p-limit)`
    ;; with the value riding as a query arg, so the compiled plan is reused.
    (let [[[_ params body] & args] (first @captured)]
      (is (= '[p-type p-limit] params))
      (is (= [:generation-test 2] (vec args)))
      (is (= (list 'order-by
                   {:val 'xt/id :dir :asc})
             (nth body 3)))
      (is (= '(limit p-limit) (last body)))))
  (let [docs [{:xt/id "entity-limit-c"
               :entity/id "entity-limit-c"
               :entity/name "C"
               :entity/type :entity-limit-test}
              {:xt/id "entity-limit-a"
               :entity/id "entity-limit-a"
               :entity/name "A"
               :entity/type :entity-limit-test}
              {:xt/id "entity-limit-b"
               :entity/id "entity-limit-b"
               :entity/name "B"
               :entity/type :entity-limit-test}]
        expected
        (->> docs
             (sort-by #(str (or (:entity/id %) (:xt/id %))))
             (take 2)
             (mapv #(dissoc % :xt/id)))]
    (doseq [doc docs]
      (is (= :ok (graph/put-verified! *node* :entities doc))))
    (let [result (graph/entities-query
                  *node* {:type :entity-limit-test :limit 2})]
      (is (= expected (:entities result)))
      (is (= 3 (:count result)))
      (is (= "entity-limit-b" (:next-cursor result))))))

(deftest entity-total-is-opt-out-and-its-absence-is-typed
  ;; The count is a second full scan of the type; no caller outside this file
  ;; reads it (TN-entities-speedups-2026-09-26.md §3). The flag makes it
  ;; skippable without letting "not asked for" read as "none".
  (let [returned [{:xt/id "total-flag-a"
                   :entity/id "total-flag-a"
                   :entity/name "A"
                   :entity/type :total-flag-test}
                  {:xt/id "total-flag-b"
                   :entity/id "total-flag-b"
                   :entity/name "B"
                   :entity/type :total-flag-test}]
        expected (mapv #(dissoc % :xt/id) returned)
        capturing (fn [seen]
                    (fn [_node form]
                      (swap! seen conj form)
                      returned))
        ;; The count statement is the parameterised read with no `order-by`
        ;; and no `p-limit`; the window carries both, the hydrate is SQL.
        count-form? (fn [form]
                      (and (not (string? (first form)))
                           (let [[_fn params body] (first form)]
                             (and (= '[p-type] params)
                                  (not-any? #(and (seq? %) (= 'order-by (first %)))
                                            body)))))]
    (testing "absent :include-total? runs the count, as every caller sees today"
      (let [seen (atom [])
            result (graph/entities-query ::capturing-node
                                         {:type :total-flag-test :limit 2}
                                         (capturing seen))]
        (is (= {:entities expected :count 2 :next-cursor "total-flag-b"} result))
        ;; Count the COUNT statements, not the statements: how many the
        ;; hydrate takes is `fxt/hydrate-by-ids`' business and changes with
        ;; its id threshold.
        (is (= 1 (count (filter count-form? @seen))))
        (is (seq @seen))))
    (testing ":include-total? false issues no count statement"
      (let [seen (atom [])
            result (graph/entities-query ::capturing-node
                                         {:type :total-flag-test :limit 2
                                          :include-total? false}
                                         (capturing seen))]
        (is (empty? (filter count-form? @seen)))
        (is (seq @seen))
        (is (= {:absent :not-requested} (:count result)))
        (is (= expected (:entities result)))
        (is (= "total-flag-b" (:next-cursor result)))))
    (testing "the absence is typed, so it cannot be read as a value"
      (let [result (graph/entities-query ::capturing-node
                                         {:type :total-flag-test :limit 2
                                          :include-total? false}
                                         (capturing (atom [])))]
        (is (some? (:count result)))
        (is (not (number? (:count result))))
        (is (contains? result :count))))))

(deftest unordered-entity-window-drops-the-cursor
  ;; `order-by` is the whole cost of the window (5.7 s against 0.13 s at
  ;; limit 1, TN-entities-speedups-2026-09-26.md §2) and it is also what
  ;; :next-cursor resumes, so dropping one drops the other.
  (let [returned [{:xt/id "unordered-a"
                   :entity/id "unordered-a"
                   :entity/name "A"
                   :entity/type :unordered-flag-test}
                  {:xt/id "unordered-b"
                   :entity/id "unordered-b"
                   :entity/name "B"
                   :entity/type :unordered-flag-test}]
        capturing (fn [seen]
                    (fn [_node form]
                      (swap! seen conj form)
                      returned))
        window-body (fn [seen]
                      (let [[fn-form] (first @seen)
                            [_fn _params body] fn-form]
                        body))
        ordered-by? (fn [body]
                      (boolean (some #(and (seq? %) (= 'order-by (first %))) body)))]
    (testing "the ordered window is today's: order-by, then limit"
      (let [seen (atom [])]
        (graph/entities-query ::capturing-node
                              {:type :unordered-flag-test :limit 2}
                              (capturing seen))
        (is (ordered-by? (window-body seen)))
        (is (= '(limit p-limit) (last (window-body seen))))))
    (testing ":ordered? false issues no order-by and keeps the limit"
      (let [seen (atom [])
            result (graph/entities-query ::capturing-node
                                         {:type :unordered-flag-test :limit 2
                                          :ordered? false}
                                         (capturing seen))]
        (is (not (ordered-by? (window-body seen))))
        (is (= '(limit p-limit) (last (window-body seen))))
        (is (not (contains? result :next-cursor)))))
    (testing "`after` on an unordered read is refused before any statement runs"
      (is (nil? (graph/entities-query-refusal {:after "some-id"})))
      (is (nil? (graph/entities-query-refusal {:ordered? false})))
      (let [refusal (graph/entities-query-refusal {:after "some-id" :ordered? false})]
        (is (= {:layer 4 :reason :unordered-page-has-no-cursor}
               (select-keys (:error (ex-data refusal)) [:layer :reason]))))
      (let [seen (atom [])
            thrown (try (graph/entities-query ::capturing-node
                                              {:type :unordered-flag-test :limit 2
                                               :after "some-id" :ordered? false}
                                              (capturing seen))
                        (catch clojure.lang.ExceptionInfo e e))]
        (is (= :unordered-page-has-no-cursor
               (get-in (ex-data thrown) [:error :reason])))
        (is (empty? @seen)))))
  (let [docs (mapv (fn [i] {:id (format "unordered-row-%02d" i)
                            :name (format "Unordered %02d" i)
                            :type "unordered/window"})
                   (range 6))]
    (graph/write-entities-batch! *node* {:entities docs})
    (testing "above the type's size the unordered read returns the same rows"
      (let [ordered (graph/entities-query *node* {:type :unordered/window :limit 50})
            unordered (graph/entities-query *node* {:type :unordered/window :limit 50
                                                    :ordered? false})]
        (is (= 6 (count (:entities unordered))))
        (is (= (set (map :entity/id (:entities ordered)))
               (set (map :entity/id (:entities unordered)))))))
    (testing "a full window carries a cursor only when it is ordered"
      (let [ordered (graph/entities-query *node* {:type :unordered/window :limit 6})
            unordered (graph/entities-query *node* {:type :unordered/window :limit 6
                                                    :ordered? false})]
        (is (= "unordered-row-05" (:next-cursor ordered)))
        (is (not (contains? unordered :next-cursor)))))))

(deftest hydrate-by-ids-splits-at-the-measured-crossover
  ;; `_id IN` is a table scan at every size and `_id = ?` uses the id index
  ;; (TN-entities-speedups-2026-09-26.md §1), so small hydrations go by
  ;; equality. Both branches must keep the caller's id order and drop misses.
  (let [doc (fn [id] {:xt/id id :entity/id id :entity/name (str "doc-" id)})
        ;; Rows come back REVERSED, so a result in id order can only come from
        ;; the caller's ids, not from the order the store answered in.
        capturing (fn [seen present]
                    (fn [_node stmt]
                      (swap! seen conj stmt)
                      (vec (reverse (keep present (rest stmt))))))
        present-all (fn [ids] (into {} (map (juxt identity doc)) ids))]
    (testing "at or below the threshold: one equality statement per id"
      (let [ids (mapv #(str "eq-" %) (range 3))
            seen (atom [])
            out (fxt/hydrate-by-ids ::node :entities ids
                                    (capturing seen (present-all ids)))]
        (is (= 3 (count @seen)))
        (is (every? #(re-find #"WHERE _id = \?$" (first %)) @seen))
        (is (every? #(= 2 (count %)) @seen))
        (is (= ids (mapv :xt/id out)))))
    (testing "exactly at the threshold is still equality"
      (let [ids (mapv #(format "eq-%03d" %) (range fxt/equality-hydrate-max-ids))
            seen (atom [])]
        (fxt/hydrate-by-ids ::node :entities ids (capturing seen (present-all ids)))
        (is (= fxt/equality-hydrate-max-ids (count @seen)))
        (is (every? #(re-find #"WHERE _id = \?$" (first %)) @seen))))
    (testing "above the threshold: one chunked IN statement, id order kept"
      (let [ids (mapv #(format "in-%03d" %) (range (inc fxt/equality-hydrate-max-ids)))
            seen (atom [])
            out (fxt/hydrate-by-ids ::node :entities ids
                                    (capturing seen (present-all ids)))]
        (is (= 1 (count @seen)))
        (is (re-find #"WHERE _id IN \(" (first (first @seen))))
        (is (= (count ids) (count (re-seq #"\?" (first (first @seen))))))
        (is (= ids (mapv :xt/id out)))))
    (testing "missing ids are dropped, in both branches"
      (let [ids (mapv #(format "miss-%03d" %) (range 4))
            here (present-all [(first ids) (last ids)])
            out (fxt/hydrate-by-ids ::node :entities ids (capturing (atom []) here))]
        (is (= [(first ids) (last ids)] (mapv :xt/id out))))
      (let [ids (mapv #(format "miss-%03d" %) (range (inc fxt/equality-hydrate-max-ids)))
            here (present-all [(first ids) (last ids)])
            out (fxt/hydrate-by-ids ::node :entities ids (capturing (atom []) here))]
        (is (= [(first ids) (last ids)] (mapv :xt/id out)))))
    (testing "no ids, no statement"
      (let [seen (atom [])]
        (is (= [] (fxt/hydrate-by-ids ::node :entities [] (capturing seen {}))))
        (is (empty? @seen))))))

(deftest entity-batch-deduplicates-repeated-names
  (let [entity {:name "batch-local-duplicate" :type "batch/local-duplicate"}
        first-result (graph/write-entities-batch!
                      *node* {:entities [entity entity entity]})
        first-ids (mapv :id (:entities first-result))
        second-result (graph/write-entities-batch!
                       *node* {:entities [entity entity entity]})
        second-ids (mapv :id (:entities second-result))
        stored (graph/entities-query
                *node* {:type :batch/local-duplicate :limit 10})]
    (is (= 1 (count (distinct first-ids))))
    (is (= first-ids second-ids))
    (is (= 1 (:count stored)))
    (is (= (first first-ids) (get-in stored [:entities 0 :entity/id]))))
  (let [explicit {:id "batch-explicit-id"
                  :name "batch-explicit-name"
                  :type "batch/explicit"}
        implicit (dissoc explicit :id)
        result (graph/write-entities-batch!
                *node* {:entities [explicit implicit]})]
    (is (= ["batch-explicit-id" "batch-explicit-id"]
           (mapv :id (:entities result))))))

(deftest entity-pagination-enumerates-more-than-server-max-window
  (let [n 5001
        entities (mapv (fn [i]
                         {:id (format "paged-%05d" i)
                          :name (format "Paged %05d" i)
                          :type "pagination/over-server-max"})
                       (range n))]
    (graph/write-entities-batch! *node* {:entities entities})
    (let [page-1 (graph/entities-query
                  *node* {:type :pagination/over-server-max :limit 5000})
          page-2 (graph/entities-query
                  *node* {:type :pagination/over-server-max :limit 5000
                          :after (:next-cursor page-1)})
          rows (concat (:entities page-1) (:entities page-2))]
      (is (= n (:count page-1)))
      (is (= n (:count page-2)))
      (is (= 5000 (count (:entities page-1))))
      (is (= 1 (count (:entities page-2))))
      (is (= n (count rows)))
      (is (= n (count (distinct (map :entity/id rows))))))))

(defn -main [& _]
  (with-open [node (xtn/start-node)]
    (binding [*node* node]
      (let [{:keys [fail error]} (run-tests 'test-temporal)]
        (shutdown-agents)
        (System/exit (if (zero? (+ fail error)) 0 1))))))
