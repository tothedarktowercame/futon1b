;; Bisect the window size at which 2b887d3's single-query hydration stops
;; returning rows. The variadic `(or (= xt/id …) …)` has one clause PER ROW, so
;; the question is where XTDB 2.1 stops honouring it — and whether it fails
;; LOUD (throws) or QUIET (returns []). Quiet is the dangerous one.
(ns scale-probe-bisect
  (:require [futon1b-graph :as graph]
            [futon1b-xt :as fxt]
            [xtdb.node :as xtn]
            [xtdb.api :as xt]))

(def ^:private hx-type "probe/scale-edge")

(defn -main [& args]
  (let [n (Integer/parseInt (or (first args) "6000"))]
    (with-open [node (xtn/start-node {})]
      (doseq [batch (partition-all 500 (range n))]
        (xt/execute-tx
         node
         (mapv (fn [i]
                 (let [id (format "hx:%s:%06d" hx-type i)]
                   [:put-docs :hyperedges
                    {:xt/id id :hx/id id :hx/type (keyword hx-type)
                     :hx/endpoints [(format "e/%06d" i)]
                     :hx/props {:repo "probe-d" :i i}
                     :prop/repo "probe-d"}]))
               batch)))
      (println "seeded; census ="
               (:count (graph/census node {:type hx-type})))
      (println "\n limit | rows | ms   | outcome")
      (println "-------+------+------+--------")
      (doseq [lim [50 100 250 500 1000 2000 3000 4000 5000]]
        (graph/invalidate-hyperedge-query-cache!)
        (let [t0 (System/currentTimeMillis)
              r (try (graph/hyperedges-query node {:type hx-type :limit lim
                                                   :include-total? false})
                     (catch Throwable t {:threw (or (.getMessage t)
                                                    (str (class t)))}))
              ms (- (System/currentTimeMillis) t0)]
          (println (format "%6d | %4s | %4d | %s"
                           lim
                           (if (:threw r) "-" (count (:hyperedges r)))
                           ms
                           (cond (:threw r) (str "THREW: " (:threw r))
                                 (= lim (count (:hyperedges r))) "ok"
                                 :else "SHORT/EMPTY — silent")))))
      ;; Is the projection or the hydration losing them?
      (println "\nprojection-only sanity at 5000:")
      (let [sel (fxt/safe-q node (list '-> (list 'from :hyperedges
                                                 '[xt/id hx/type prop/timestamp
                                                   prop/repo prop/source-file])
                                       (list 'where (list '= 'hx/type
                                                          (keyword hx-type)))
                                       (list 'order-by {:val 'xt/id :dir :asc})
                                       (list 'limit 5000)))]
        (println "  projected ids:" (count sel))
        (let [ids (mapv :xt/id (take 5000 sel))
              hy (try (fxt/safe-q node (list '-> (list 'from :hyperedges '[*])
                                             (list 'where
                                                   (cons 'or (map #(list '= 'xt/id %) ids)))))
                      (catch Throwable t [:threw (.getMessage t)]))]
          (println "  hydrated via variadic or:"
                   (if (and (vector? hy) (= :threw (first hy)))
                     (str "THREW: " (second hy))
                     (count hy)))))))
  (shutdown-agents))
