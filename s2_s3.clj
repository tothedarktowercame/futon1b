;; E-futon1b-foothold S2+S3 — synthetic seed + the three query translations.
;; Run: cd /home/joe/code/futon1b && clojure -M:node -e '(load-file "s2_s3.clj")'
;; Assertions compare XTQL results against counts computed in plain Clojure
;; from the same seed data — the seed is the oracle.
(require '[xtdb.node :as xtn] '[xtdb.api :as xt])

(def endpoint-pool (mapv #(str "e-" %) (range 1 31)))
(def hx-types [:code/calls :code/mentions :mission/depends :evidence/links :social/tags])

(def hyperedges
  (vec (for [i (range 100)]
         {:xt/id i
          :hx/type (nth hx-types (mod i 5))
          :hx/endpoints (vec (distinct
                              (cond-> [(nth endpoint-pool (mod i 30))
                                       (nth endpoint-pool (mod (* 2 i) 30))]
                                (even? i) (conj (nth endpoint-pool (mod (* 3 i) 30))))))
          :hx/at (format "2026-07-04T%02d:00:00Z" (mod i 24))})))

(def evidence
  (vec (for [i (range 30)]
         {:xt/id (+ 1000 i)
          :evidence/type :observation
          :evidence/author (str "agent-" (mod i 3))
          :evidence/at (format "2026-07-04T%02d:30:00Z" (mod i 24))
          :evidence/tags (nth [[:psr] [:pur] [:par] [:proof-path] [:psr :proof-path]]
                              (mod i 5))})))

;; Oracle counts, computed from the seed itself.
(def expected-e7    (count (filter #(some #{"e-7"} (:hx/endpoints %)) hyperedges)))
(def expected-calls (count (filter #(= :code/calls (:hx/type %)) hyperedges)))
(def expected-pp    (count (filter #(some #{:proof-path} (:evidence/tags %)) evidence)))

(with-open [node (xtn/start-node)]
  (xt/execute-tx node (vec (for [h hyperedges] [:put-docs :hyperedges h])))
  (xt/execute-tx node (vec (for [e evidence] [:put-docs :evidence e])))

  ;; (a) endpoint membership — Datalog original: [?h :hx/endpoints "e-7"]
  (let [qa (xt/q node '(-> (from :hyperedges [xt/id hx/type hx/endpoints])
                           (unnest {:ep hx/endpoints})
                           (where (= ep "e-7"))
                           (return xt/id)))
        a-n (count (set (map :xt/id qa)))
        ;; (b) by-type with limit — Datalog: [?h :hx/type :code/calls] + :limit
        qb-all (xt/q node '(-> (from :hyperedges [xt/id hx/type])
                               (where (= hx/type :code/calls))))
        qb-lim (xt/q node '(-> (from :hyperedges [xt/id hx/type])
                               (where (= hx/type :code/calls))
                               (limit 10)))
        ;; (c) evidence by tag — Datalog: [?e :evidence/tags :proof-path]
        qc (xt/q node '(-> (from :evidence [xt/id evidence/tags])
                           (unnest {:tag evidence/tags})
                           (where (= tag :proof-path))
                           (return xt/id)))
        c-n (count (set (map :xt/id qc)))]
    (println "S3a endpoint-membership: expected" expected-e7 "got" a-n
             (if (= expected-e7 a-n) "PASS" "FAIL"))
    (println "S3b by-type+limit: expected-total" expected-calls
             "got-total" (count qb-all) "got-limited" (count qb-lim)
             (if (and (= expected-calls (count qb-all)) (<= (count qb-lim) 10))
               "PASS" "FAIL"))
    (println "S3c tag-membership: expected" expected-pp "got" c-n
             (if (= expected-pp c-n) "PASS" "FAIL"))))
