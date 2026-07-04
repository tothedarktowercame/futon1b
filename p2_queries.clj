;; M-futon1b-port P2 — query-site translations with runtime assertions.
;;
;; Each translated site from futon1a's ~44 xtdb/q locations is rendered as
;; an XTQL pipeline, run against the ingested slice, and asserted against
;; an oracle count computed independently in Clojure from the same slice
;; data. This is the seed-as-oracle pattern generalized to real data.
;;
;; Run: cd /home/joe/code/futon1b && clojure -M:node -e '(load-file "p2_queries.clj")'
;;
;; HAZARD CATALOG (see NOTES.md §Hazard catalog):
;;   H1: naive unnest → silent unfiltered results. Fix: pipeline unnest+where.
;;   H2: Clojure-isms in XTQL where → runtime error. Fix: XTQL predicates only.
;;   H3: from silently ignores extra positional forms → silent unfiltered.
;;       Fix: where must be threaded: (-> (from ...) (where ...)).
;; Every query below uses the threaded pipeline form exclusively.
(require '[xtdb.node :as xtn] '[xtdb.api :as xt]
         '[clojure.edn :as edn] '[clojure.java.io :as io])
(import '[java.io PushbackReader])

(defn read-edn [f]
  (with-open [r (PushbackReader. (io/reader f))]
    (edn/read r)))

(def slice (read-edn "seed/substrate-slice.edn"))
(def hyperedges (:hyperedges slice))
(def entities (:entities slice))

;; ---------------------------------------------------------------------------
;; Oracle helpers — compute expected results in plain Clojure from the slice.
;; ---------------------------------------------------------------------------
(defn oracle-by-type [type-kw]
  (filter #(= type-kw (:hx/type %)) hyperedges))

(defn oracle-endpoint-members [endpoint-id]
  (filter #(some #{endpoint-id} (:hx/endpoints %)) hyperedges))

(defn oracle-entity-by-type [type-kw]
  (filter #(= type-kw (:entity/type %)) entities))

(defn oracle-distinct-endpoints []
  (into #{} (mapcat :hx/endpoints hyperedges)))

;; Pick representative samples from the real data for assertions.
(def sample-type (first (keys (group-by :hx/type hyperedges))))
(def sample-endpoint (-> hyperedges first :hx/endpoints first))
(def sample-entity-type :pattern/library)
(def all-hx-types (sort (keys (group-by :hx/type hyperedges))))

;; ---------------------------------------------------------------------------
;; P2 assertions — each is one translated query site + oracle check.
;; ---------------------------------------------------------------------------

(defn ingest! [node table docs]
  (xt/execute-tx node (vec (for [d docs] [:put-docs table d]))))

;; Tx-op form assertions (Q2 ruling: 3 total — put, delete, evict).
(defn tx-op-assertions [node]
  (println)
  (println "=== tx-op form assertions (Q2 ruling: 3 total) ===")

  ;; T1 — put-docs: write + read-back.
  (xt/execute-tx node [[:put-docs :hyperedges
                         {:xt/id "tx-test-1" :hx/type :test/put
                          :hx/endpoints ["a" "b"]}]])
  (let [doc (first (xt/q node '(-> (from :hyperedges [xt/id hx/type])
                                   (where (= xt/id "tx-test-1")))))]
    (println "T1 put-docs read-back:" (nil? doc) "->"
             (if (= :test/put (:hx/type doc)) "PASS" "FAIL")))

  ;; T2 — delete-docs: write, delete, read-back-gone.
  (xt/execute-tx node [[:put-docs :hyperedges
                         {:xt/id "tx-test-2" :hx/type :test/delete
                          :hx/endpoints ["c" "d"]}]])
  (xt/execute-tx node [[:delete-docs :hyperedges "tx-test-2"]])
  (let [doc (first (xt/q node '(-> (from :hyperedges [xt/id])
                                   (where (= xt/id "tx-test-2")))))]
    (println "T2 delete-docs gone:" (nil? doc) "->"
             (if (nil? doc) "PASS" "FAIL")))

  ;; T3 — erase-docs: write, erase, read-back-gone.
  ;; XTDB 2 uses `erase-docs` (not `evict-docs` — that tx-op doesn't exist
  ;; in 2.x). Erase removes the doc entirely (all history), which is the
  ;; semantic equivalent of futon1a's `:xtdb.api/evict`.
  (xt/execute-tx node [[:put-docs :hyperedges
                         {:xt/id "tx-test-3" :hx/type :test/erase
                          :hx/endpoints ["e" "f"]}]])
  (xt/execute-tx node [[:erase-docs :hyperedges "tx-test-3"]])
  (let [doc (first (xt/q node '(-> (from :hyperedges [xt/id])
                                   (where (= xt/id "tx-test-3")))))]
    (println "T3 erase-docs gone:" (nil? doc) "->"
             (if (nil? doc) "PASS" "FAIL"))))

;; ---------------------------------------------------------------------------
;; Main: ingest, then run all P2 assertions.
;; ---------------------------------------------------------------------------
(with-open [node (xtn/start-node)]
  (ingest! node :hyperedges hyperedges)
  (ingest! node :entities   entities)

  ;; ========================================================================
  ;; FAMILY A — endpoint membership (the core idiom, ~34 sites in futon1a)
  ;; Datalog original: {:find [(pull e [*])] :in [eid]
  ;;                    :where [[e :hx/endpoints eid]]}
  ;; XTQL translation: (-> (from :hyperedges [xt/id hx/endpoints])
  ;;                        (unnest {:ep hx/endpoints})
  ;;                        (where (= ep <eid>))
  ;;                        (return xt/id))
  ;; ========================================================================
  (println)
  (println "=== Family A: endpoint membership ===")

  ;; A1 — single endpoint membership against oracle.
  (let [expected (set (map :xt/id (oracle-endpoint-members sample-endpoint)))
        got (set (map :xt/id
                      (xt/q node (list '->
                                       (list 'from :hyperedges ['xt/id 'hx/endpoints])
                                       (list 'unnest '{:ep hx/endpoints})
                                       (list 'where (list '= 'ep sample-endpoint))
                                       (list 'return 'xt/id)))))]
    (println "A1 membership(" sample-endpoint "): expected" (count expected)
             "got" (count got)
             (if (= expected got) "PASS" "FAIL")))

  ;; A2 — membership against a second endpoint (different cardinality).
  (let [ep2 (-> hyperedges second :hx/endpoints first)
        expected (set (map :xt/id (oracle-endpoint-members ep2)))
        got (set (map :xt/id
                      (xt/q node (list '->
                                       (list 'from :hyperedges ['xt/id 'hx/endpoints])
                                       (list 'unnest '{:ep hx/endpoints})
                                       (list 'where (list '= 'ep ep2))
                                       (list 'return 'xt/id)))))]
    (println "A2 membership(" ep2 "): expected" (count expected)
             "got" (count got)
             (if (= expected got) "PASS" "FAIL")))

  ;; A3 — membership on a nonexistent endpoint returns empty.
  (let [got (xt/q node '(-> (from :hyperedges [xt/id hx/endpoints])
                            (unnest {:ep hx/endpoints})
                            (where (= ep "DOES-NOT-EXIST-999"))
                            (return xt/id)))]
    (println "A3 membership(none): expected 0 got" (count got)
             (if (empty? got) "PASS" "FAIL")))

  ;; ========================================================================
  ;; FAMILY B — by-type enumeration + count (the census/population idiom)
  ;; Datalog original: {:find [e] :in [t] :where [[e :hx/type t]]}
  ;;                    {:find [(count e)] :in [t] :where [[e :hx/type t]]}
  ;; XTQL: (-> (from :hyperedges [xt/id hx/type]) (where (= hx/type <kw>)))
  ;; ========================================================================
  (println)
  (println "=== Family B: by-type enumeration + count ===")

  ;; B1 — enumerate one type, compare to oracle.
  (let [type-kw sample-type
        expected (count (oracle-by-type type-kw))
        got (count (xt/q node (list '->
                                    (list 'from :hyperedges ['xt/id 'hx/type])
                                    (list 'where (list '= 'hx/type type-kw)))))]
    (println "B1 by-type(" type-kw "): expected" expected "got" got
             (if (= expected got) "PASS" "FAIL")))

  ;; B2 — enumerate ALL types, compare per-type to oracle.
  (let [results (for [type-kw all-hx-types]
                  (let [expected (count (oracle-by-type type-kw))
                        got (count (xt/q node (list '->
                                                    (list 'from :hyperedges ['xt/id 'hx/type])
                                                    (list 'where (list '= 'hx/type type-kw)))))]
                    [type-kw expected got (= expected got)]))]
    (doseq [[type-kw expected got pass?] results]
      (println "B2 by-type(" type-kw "): expected" expected "got" got
               (if pass? "PASS" "FAIL")))
    (println "B2 all-types:" (if (every? #(nth % 3) results) "ALL PASS" "SOME FAIL")))

  ;; B3 — by-type with limit (futon1a's hyperedges-latest pattern).
  (let [type-kw sample-type
        lim 5
        got (count (xt/q node (list '->
                                    (list 'from :hyperedges ['xt/id 'hx/type])
                                    (list 'where (list '= 'hx/type type-kw))
                                    (list 'limit lim))))]
    (println "B3 by-type+limit(" type-kw "limit" lim "): got" got
             (if (= lim got) "PASS" "FAIL")))

  ;; ========================================================================
  ;; FAMILY C — entity by-type (the entities-latest idiom)
  ;; Datalog: {:find [(pull e [*])] :in [t] :where [[e :entity/type t]]}
  ;; XTQL: (-> (from :entities [xt/id entity/type]) (where (= entity/type <kw>)))
  ;; ========================================================================
  (println)
  (println "=== Family C: entity by-type ===")

  ;; C1 — entity by-type count against oracle.
  (let [type-kw sample-entity-type
        expected (count (oracle-entity-by-type type-kw))
        got (count (xt/q node (list '->
                                    (list 'from :entities ['xt/id 'entity/type])
                                    (list 'where (list '= 'entity/type type-kw)))))]
    (println "C1 entity-by-type(" type-kw "): expected" expected "got" got
             (if (= expected got) "PASS" "FAIL")))

  ;; ========================================================================
  ;; FAMILY D — entity attribute lookup (fetch-entity by name)
  ;; Datalog: {:find [(pull e [:entity/id])] :in [name]
  ;;           :where [[e :entity/name name]]}
  ;; XTQL: (-> (from :entities [xt/id entity/name]) (where (= entity/name <s>)))
  ;; ========================================================================
  (println)
  (println "=== Family D: entity attribute lookup ===")

  ;; D1 — lookup by entity/name.
  (let [sample-name (:entity/name (first entities))
        got (map :xt/id (xt/q node (list '->
                                         (list 'from :entities ['xt/id 'entity/name])
                                         (list 'where (list '= 'entity/name sample-name)))))
        expected-names (set (map :xt/id
                                 (filter #(= sample-name (:entity/name %)) entities)))]
    (println "D1 entity-by-name(" (subs (str sample-name) 0 (min 40 (count (str sample-name))))
             "): expected" (count expected-names) "got" (count got)
             (if (= expected-names (set got)) "PASS" "FAIL")))

  ;; D2 — lookup by entity/external-id (pick an entity that actually has one).
  (let [sample-ent (first (filter :entity/external-id entities))
        sample-eid (:entity/external-id sample-ent)
        got (set (map :xt/id
                      (xt/q node (list '->
                                       (list 'from :entities ['xt/id 'entity/external-id])
                                       (list 'where (list '= 'entity/external-id sample-eid))))))
        expected (set (map :xt/id
                           (filter #(= sample-eid (:entity/external-id %)) entities)))]
    (println "D2 entity-by-external-id: expected" (count expected) "got" (count got)
             (if (= expected got) "PASS" "FAIL")))

  ;; ========================================================================
  ;; FAMILY E — distinct endpoint enumeration (the A4 pattern from P1,
  ;; now formalized as a query site)
  ;; XTQL: (-> (from :hyperedges [hx/endpoints]) (unnest {:ep hx/endpoints})
  ;;            (return ep))
  ;; ========================================================================
  (println)
  (println "=== Family E: distinct endpoint enumeration ===")

  ;; E1 — distinct endpoints via unnest, compared to Clojure oracle.
  (let [got (count (set (map :ep
                             (xt/q node '(-> (from :hyperedges [hx/endpoints])
                                             (unnest {:ep hx/endpoints})
                                             (return ep))))))
        expected (count (oracle-distinct-endpoints))]
    (println "E1 distinct-endpoints: expected" expected "got" got
             (if (= expected got) "PASS" "FAIL")))

  ;; ========================================================================
  ;; Tx-op form assertions (Q2 ruling: 3 total).
  ;; ========================================================================
  (tx-op-assertions node)

  (println)
  (println "=== P2 query translations complete ==="))
