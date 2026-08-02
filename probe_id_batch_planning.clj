;; Which shape does XTDB plan as index lookups when fetching N docs by primary key?
;;
;; WHY: on the LIVE store (34,480-doc `code/v05/var`), a single `(= xt/id x)`
;; costs ~50ms but ONE 50-clause `(or (= xt/id a) …)` costs ~40s — the
;; disjunction is not index-backed. A 6,000-doc fixture showed the OPPOSITE
;; (5000 rows batched in 15.5s), so fixture size matters. This probe seeds a
;; fixture sized to the live type and compares three shapes:
;;
;;   A  N sequential point lookups          (what we shipped back to)
;;   B  one N-clause XTQL `or`              (the regression)
;;   C  one SQL `WHERE _id IN (?…)`         (the open question for JUXT)
;;
;; If C is fast where B is slow, batched hydration is viable via the SQL surface
;; and XTQL is missing a predicate that its own SQL surface has. If B and C are
;; both slow, per-doc lookups are simply correct and the finding is that XTDB
;; will not batch primary-key fetches at all.
;;
;;   clojure -M:node -m probe-id-batch-planning [n-docs] [n-ids]
(ns probe-id-batch-planning
  (:require [futon1b-xt :as fxt]
            [xtdb.api :as xt]
            [xtdb.node :as xtn]))

(def ^:private hx-type "code/v05/var")

;; Ids shaped like the live store's, which run 60-70 chars — length drives
;; generated-expression size, so a short synthetic id understates shape B.
(defn- mk-id [i]
  (format "hx:%s:futon0-d/agent-nick/agent-nick--agent-buffers-%06d" hx-type i))

(defn- ms [f]
  (let [t0 (System/currentTimeMillis) r (f)]
    [(- (System/currentTimeMillis) t0) r]))

(defn -main [& args]
  (let [n-docs (Integer/parseInt (or (first args) "40000"))
        n-ids (Integer/parseInt (or (second args) "50"))]
    (with-open [node (xtn/start-node {})]
      (println (format "seeding %d docs…" n-docs))
      (let [[t _] (ms (fn []
                        (doseq [batch (partition-all 1000 (range n-docs))]
                          (xt/execute-tx
                           node
                           (mapv (fn [i]
                                   [:put-docs :hyperedges
                                    {:xt/id (mk-id i) :hx/id (mk-id i)
                                     :hx/type (keyword hx-type)
                                     :hx/endpoints [(format "e/%06d" i)]
                                     :hx/props {:repo "futon0-d" :i i}
                                     :prop/repo "futon0-d"}])
                                 batch)))))]
        (println (format "  seeded in %.1fs" (/ t 1000.0))))

      (let [ids (mapv mk-id (range 0 (* n-ids 7) 7))]   ; scattered, not contiguous
        (println (format "\nfetching %d docs by primary key, three shapes:\n" n-ids))

        ;; A — N sequential point lookups
        (let [[t rows] (ms (fn [] (->> ids
                                       (map #(fxt/q1 node (list '-> (list 'from :hyperedges '[*])
                                                                (list 'where (list '= 'xt/id %)))))
                                       (keep identity) count)))]
          (println (format "  A  %-34s %6dms  rows=%d  (%.1f ms/row)"
                           "N point lookups" t rows (/ (double t) (max 1 n-ids)))))

        ;; B — one N-clause XTQL disjunction
        (let [[t rows] (ms (fn [] (try
                                    (count (fxt/safe-q node (list '-> (list 'from :hyperedges '[*])
                                                                  (list 'where (cons 'or (map #(list '= 'xt/id %) ids))))))
                                    (catch Throwable e (str "THREW: " (.getMessage e))))))]
          (println (format "  B  %-34s %6dms  rows=%s" "one N-clause XTQL `or`" t rows)))

        ;; C — one SQL IN
        (let [sql (str "SELECT * FROM hyperedges WHERE _id IN ("
                       (clojure.string/join "," (repeat (count ids) "?")) ")")
              [t rows] (ms (fn [] (try (count (xt/q node (into [sql] ids)))
                                       (catch Throwable e (str "THREW: " (.getMessage e))))))]
          (println (format "  C  %-34s %6dms  rows=%s" "one SQL `_id IN (?…)`" t rows)))

        (println "\n(A is what futon1b ships; B is the reverted regression; C is the open question.)"))))
  (shutdown-agents))
