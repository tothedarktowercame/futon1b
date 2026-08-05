;; First query-ladder probe over the mark7z corpus on XTDB 2.2.0-rc0
;; (M-xtdb-22x-benchmarking). Rungs, in the mission's ladder order:
;;   R1 point lookup by _id           (was flat 0.15-0.18s on 2.1.0 live)
;;   R2 scalar filter (kind = claim)  (the #3663 workload)
;;   R3 text scan (LIKE)              (the #5637 gap, measured honestly as a scan)
;;   R4 graph traversal (edge -> premise node join)
;;   R5 XTQL id-set membership        (the 08-02 §1 finding — re-verified on rc0)
;; Each timed cold-ish (fresh process) with 3 repeats to show cache effect.
;;
;;   clojure -M:node -m query-probe --store <dir>
(ns query-probe
  (:require [xtdb.node :as xtn]
            [xtdb.api :as xt]))

(defn open-store [dir]
  (xtn/start-node {:log     [:local {:path (str dir "/log")}]
                   :storage [:local {:path (str dir "/storage")}]}))

(defn- timed [f]
  (let [t0 (System/nanoTime) r (f) ms (/ (- (System/nanoTime) t0) 1e6)]
    [ms r]))

(defn- rung [node label q & args]
  (try
    (let [runs (vec (for [_ (range 3)]
                      (let [[ms r] (timed #(count (xt/q node (if args (into [q] args) q))))]
                        {:ms ms :rows r})))]
      (println (format "  %-42s %6.1f / %6.1f / %6.1f ms  (%d rows)"
                       label (:ms (runs 0)) (:ms (runs 1)) (:ms (runs 2))
                       (:rows (peek runs)))))
    (catch Throwable t
      (println (format "  %-42s ERR: %s" label
                       (subs (str (.getMessage t)) 0
                             (min 140 (count (str (.getMessage t))))))))))

(defn -main [& args]
  (let [opts (apply hash-map args)
        sdir (get opts "--store")]
    (assert sdir "need --store <dir>")
    (with-open [node (open-store sdir)]
      (let [row0 (first (xt/q node "SELECT _id FROM iatc_nodes LIMIT 1"))
            some-id (or (:_id row0) (:xt/id row0) (first (vals row0)))]
        (println "query-probe on" sdir "— sample node id:" some-id)
        (rung node "R1 point lookup by _id"
              "SELECT _id, kind FROM iatc_nodes WHERE _id = ?" some-id)
        (rung node "R2 scalar: kind = 'claim'"
              "SELECT _id FROM iatc_nodes WHERE kind = 'claim'")
        (rung node "R3 text LIKE '%fibration%'"
              "SELECT _id FROM iatc_nodes WHERE \"text\" LIKE '%fibration%'")
        (rung node "R4 join: edges -> premise nodes"
              (str "SELECT e._id, n.kind FROM iatc_edges e, iatc_nodes n "
                   "WHERE n._id = e.premise_refs[1]"))
        ;; R5: the 08-02 §1 XTQL finding, re-verified on 2.2.0-rc0
        (println "  R5 XTQL membership forms (08-02 findings re-check):")
        (doseq [[label form]
                [["(in _id [...])"
                  (list '-> (list 'from :iatc_nodes '[xt/id])
                        (list 'where (list 'in 'xt/id [some-id "nope-1" "nope-2"])))]
                 ["(or (= _id ..) ..)"
                  (list '-> (list 'from :iatc_nodes '[xt/id])
                        (list 'where (list 'or (list '= 'xt/id some-id)
                                           (list '= 'xt/id "nope-1"))))]]]
          (let [r (try (let [[ms n] (timed #(count (xt/q node form)))]
                         (format "OK %d rows, %.1f ms" n ms))
                       (catch Throwable t
                         (str "ERR: " (subs (str (.getMessage t)) 0
                                            (min 120 (count (str (.getMessage t))))))))]
            (println (format "    %-24s %s" label r))))
        (println "  R5-SQL id IN (...):")
        (rung node "SQL _id IN (?,?,?)"
              "SELECT _id FROM iatc_nodes WHERE _id IN (?, ?, ?)" some-id "nope-1" "nope-2")))))
