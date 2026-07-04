;; M-futon1b-port P2c — remaining testable query-site translations.
;;
;; Covers the entity-based and hyperedge-traversal sites not in p2_queries.clj
;; or p2b_compound_queries.clj. Sites that need :doc/, :relation/, :type/, or
;; :model/descriptor docs are NOT testable against this slice and are recorded
;; as deferred in the mission doc.
;;
;; Run: cd /home/joe/code/futon1b && clojure -M:node -e '(load-file "p2c_remaining_sites.clj")'
;;
;; HAZARD CATALOG (see NOTES.md):
;;   H1: naive unnest → silent unfiltered. Fix: pipeline unnest+where.
;;   H2: Clojure-isms in XTQL where → runtime error. Fix: XTQL predicates only.
;;   H3: from silently ignores extra positional forms → silent unfiltered.
;;       Fix: where must be threaded: (-> (from ...) (where ...)).
;;   H4: XTQL where cannot navigate nested maps/structs. Fix: denormalize.
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
;; Oracle helpers
;; ---------------------------------------------------------------------------
(defn oracle-entity-by-id [eid]
  (filter #(= eid (:entity/id %)) entities))

(defn oracle-entity-by-name [name]
  (filter #(= name (:entity/name %)) entities))

(defn oracle-entity-by-external-id [ext-id]
  (filter #(= ext-id (:entity/external-id %)) entities))

(defn oracle-hyperedges-by-endpoint [endpoint-id]
  (filter #(some #{endpoint-id} (:hx/endpoints %)) hyperedges))

;; ---------------------------------------------------------------------------
;; Ingest
;; ---------------------------------------------------------------------------
(defn ingest! [node table docs]
  (xt/execute-tx node (vec (for [d docs] [:put-docs table d]))))

;; ---------------------------------------------------------------------------

(with-open [node (xtn/start-node)]
  (ingest! node :hyperedges hyperedges)
  (ingest! node :entities   entities)

  ;; Pick samples from real data.
  (let [sample-ent     (first entities)
        sample-eid     (:entity/id sample-ent)
        sample-name    (:entity/name sample-ent)
        sample-ext-ent (first (filter :entity/external-id entities))
        sample-ext-id  (:entity/external-id sample-ext-ent)
        ;; For ego-network: pick an endpoint that appears in multiple hyperedges.
        endpoint-freqs (frequencies (mapcat :hx/endpoints hyperedges))
        ego-endpoint   (->> endpoint-freqs
                            (sort-by second >)
                            ffirst)]

    (println)
    (println "=== P2c: remaining testable query-site translations ===")
    (println)

    ;; ========================================================================
    ;; FAMILY F — entity-by-id lookup (futon1_write.clj entity-by-id)
    ;; Datalog: {:find [(pull e [:entity/id :entity/name :entity/type ...])]
    ;;           :in [id] :where [[e :entity/id id]]}
    ;; XTQL: (-> (from :entities [xt/id entity/id entity/name entity/type])
    ;;           (where (= entity/id <id>)))
    ;; ========================================================================
    (println "--- Family F: entity-by-id ---")

    ;; F1 — lookup by :entity/id returns the right entity.
    (let [expected (set (map :xt/id (oracle-entity-by-id sample-eid)))
          got (set (map :xt/id
                        (xt/q node (list '->
                                         (list 'from :entities
                                               ['xt/id 'entity/id 'entity/name 'entity/type])
                                         (list 'where (list '= 'entity/id sample-eid))))))]
      (println (format "  F1 entity-by-id: expected %d got %d %s"
                       (count expected) (count got)
                       (if (= expected got) "PASS" "FAIL"))))

    ;; F2 — lookup by nonexistent :entity/id returns empty.
    (let [got (xt/q node (list '->
                               (list 'from :entities
                                     ['xt/id 'entity/id 'entity/name 'entity/type])
                               (list 'where (list '= 'entity/id "NO-SUCH-ID-999"))))]
      (println (format "  F2 entity-by-id(none): expected 0 got %d %s"
                       (count got) (if (empty? got) "PASS" "FAIL"))))

    ;; ========================================================================
    ;; FAMILY G — multi-fallback entity lookup (futon1_graph.clj fetch-entity)
    ;; futon1a tries: direct entity lookup, then by :entity/name, then by
    ;; :entity/external-id. Each fallback is a separate query. Here we assert
    ;; each leg independently — the orchestration (try direct, fall back) is
    ;; Clojure logic, not a query translation.
    ;; ========================================================================
    (println)
    (println "--- Family G: multi-fallback fetch-entity ---")

    ;; G1 — fallback leg: lookup by :entity/name (same as P2 D1 but with full projection).
    (let [expected (set (map :xt/id (oracle-entity-by-name sample-name)))
          got (set (map :xt/id
                        (xt/q node (list '->
                                         (list 'from :entities
                                               ['xt/id 'entity/name 'entity/type])
                                         (list 'where (list '= 'entity/name sample-name))))))]
      (println (format "  G1 by-name fallback: expected %d got %d %s"
                       (count expected) (count got)
                       (if (= expected got) "PASS" "FAIL"))))

    ;; G2 — fallback leg: lookup by :entity/external-id.
    (let [expected (set (map :xt/id (oracle-entity-by-external-id sample-ext-id)))
          got (set (map :xt/id
                        (xt/q node (list '->
                                         (list 'from :entities
                                               ['xt/id 'entity/external-id 'entity/name])
                                         (list 'where (list '= 'entity/external-id sample-ext-id))))))]
      (println (format "  G2 by-external-id fallback: expected %d got %d %s"
                       (count expected) (count got)
                       (if (= expected got) "PASS" "FAIL"))))

    ;; G3 — multi-type deduplication: entity-by-name with type filter
    ;; (futon1_write entity-by-name with wanted-type).
    (let [wanted-type (:entity/type sample-ent)
          expected (->> (oracle-entity-by-name sample-name)
                        (filter #(= wanted-type (:entity/type %)))
                        (map :xt/id)
                        set)
          got (set (map :xt/id
                        (xt/q node (list '->
                                         (list 'from :entities
                                               ['xt/id 'entity/name 'entity/type])
                                         (list 'where (list '= 'entity/name sample-name))
                                         (list 'where (list '= 'entity/type wanted-type))))))]
      (println (format "  G3 by-name+type(%s): expected %d got %d %s"
                       wanted-type (count expected) (count got)
                       (if (= expected got) "PASS" "FAIL"))))

    ;; ========================================================================
    ;; FAMILY H — ego-network hyperedge traversal
    ;; (futon1_graph.clj hyperedges-by-endpoint + hyperedge-links-for-ego)
    ;;
    ;; futon1a: {:find [(pull e [*])] :in [eid] :where [[e :hx/endpoints eid]]}
    ;; This is the membership idiom but returning FULL docs and sorting.
    ;; The ego-network builds on this: find all hyperedges touching an endpoint,
    ;; then for each, find co-endpoints. We assert the base query (hyperedges
    ;; touching an endpoint) and the co-endpoint enumeration.
    ;; ========================================================================
    (println)
    (println "--- Family H: ego-network hyperedge traversal ---")

    ;; H1 — hyperedges by endpoint (full-doc variant, not just ids).
    (let [expected (set (map :xt/id (oracle-hyperedges-by-endpoint ego-endpoint)))
          got (set (map :xt/id
                        (xt/q node (list '->
                                         (list 'from :hyperedges ['xt/id 'hx/endpoints 'hx/type])
                                         (list 'unnest '{:ep hx/endpoints})
                                         (list 'where (list '= 'ep ego-endpoint))
                                         (list 'return 'xt/id 'hx/type)))))]
      (println (format "  H1 hyperedges-by-endpoint(%s): expected %d got %d %s"
                       ego-endpoint (count expected) (count got)
                       (if (= expected got) "PASS" "FAIL"))))

    ;; H2 — co-endpoint enumeration: for hyperedges touching ego-endpoint,
    ;; collect ALL endpoints (the ego-network neighbors). The count should
    ;; equal the sum of endpoints across all matching hyperedges.
    (let [matching-hxes (oracle-hyperedges-by-endpoint ego-endpoint)
          expected-co-eps (count (set (mapcat :hx/endpoints matching-hxes)))
          got-rows (xt/q node (list '->
                                    (list 'from :hyperedges ['xt/id 'hx/endpoints])
                                    (list 'unnest '{:ep hx/endpoints})
                                    (list 'where (list '= 'ep ego-endpoint))
                                    (list 'return 'xt/id)))
          ;; For each matching hyperedge id, fetch its endpoints and collect.
          matching-ids (set (map :xt/id got-rows))
          got-co-eps (count (set (mapcat :hx/endpoints
                                         (filter #(matching-ids (:xt/id %)) hyperedges))))]
      (println (format "  H2 co-endpoints(%s): expected %d got %d %s"
                       ego-endpoint expected-co-eps got-co-eps
                       (if (= expected-co-eps got-co-eps) "PASS" "FAIL"))))

    ;; H3 — ego-network on a nonexistent endpoint returns empty.
    (let [got (xt/q node '(-> (from :hyperedges [xt/id hx/endpoints])
                              (unnest {:ep hx/endpoints})
                              (where (= ep "DOES-NOT-EXIST-999"))
                              (return xt/id)))]
      (println (format "  H3 ego-network(none): expected 0 got %d %s"
                       (count got) (if (empty? got) "PASS" "FAIL"))))

    (println)
    (println "=== P2c remaining testable sites complete ===")))
