;; Does XTQL 2.1.0 offer ANY set-membership predicate over a literal list?
;; This decides whether the variadic-`or` workaround (which blows the JVM
;; method-size limit at ~250 clauses) is forced or merely chosen. Run:
;;   clojure -M:node -m probe-in-predicate
(ns probe-in-predicate
  (:require [futon1b-xt :as fxt]
            [xtdb.node :as xtn]
            [xtdb.api :as xt]))

(defn- try-form [node label form]
  (let [r (try {:ok (count (xt/q node form))}
               (catch Throwable t {:err (or (.getMessage t) (str (class t)))}))]
    (println (format "  %-38s %s" label
                     (if (:ok r) (str "OK, " (:ok r) " rows")
                         (str "ERR: " (subs (str (:err r)) 0 (min 150 (count (str (:err r)))))))))))

(defn -main [& _]
  (with-open [node (xtn/start-node {})]
    (xt/execute-tx node (mapv (fn [i] [:put-docs :hyperedges
                                       {:xt/id (str "hx:" i) :hx/type :probe/t}])
                              (range 10)))
    (let [ids ["hx:1" "hx:2" "hx:3"]]
      (println "XTQL set-membership over a literal list, xtdb 2.1.0:")
      (try-form node "(or (= xt/id ..) ..)  [current]"
                (list '-> (list 'from :hyperedges '[xt/id])
                      (list 'where (cons 'or (map #(list '= 'xt/id %) ids)))))
      (try-form node "(in xt/id [..])"
                (list '-> (list 'from :hyperedges '[xt/id])
                      (list 'where (list 'in 'xt/id ids))))
      (try-form node "(contains? #{..} xt/id)"
                (list '-> (list 'from :hyperedges '[xt/id])
                      (list 'where (list 'contains? (set ids) 'xt/id))))
      (try-form node "(= xt/id (any ..))"
                (list '-> (list 'from :hyperedges '[xt/id])
                      (list 'where (list '= 'xt/id (list 'any ids)))))
      ;; SQL surface, for comparison with the XTQL surface
      (println "\nSQL surface:")
      (let [r (try {:ok (count (xt/q node ["SELECT _id FROM hyperedges WHERE _id IN (?, ?, ?)"
                                           "hx:1" "hx:2" "hx:3"]))}
                   (catch Throwable t {:err (.getMessage t)}))]
        (println (format "  %-38s %s" "SELECT .. WHERE _id IN (?,?,?)"
                         (if (:ok r) (str "OK, " (:ok r) " rows")
                             (str "ERR: " (subs (str (:err r)) 0 (min 150 (count (str (:err r)))))))))))
    (println))
  (shutdown-agents))
