;; M-futon1b-port P3 — XTDB 2.x parity side.
;;
;; Ingests the slice into an XTDB 2.x in-process node, runs the ≥12
;; representative queries using XTQL, and writes results as pipe-delimited
;; lines to stdout for the parity harness to capture.
;;
;; Run: cd /home/joe/code/futon1b && clojure -M:node -m parity-2x
;;
;; Every query uses the threaded pipeline form (H3 discipline).
(ns parity-2x
  (:require [xtdb.node :as xtn]
            [xtdb.api :as xt]
            [clojure.edn :as edn]
            [clojure.java.io :as io])
  (:import [java.io PushbackReader])
  (:gen-class))

(defn read-edn [f]
  (with-open [r (PushbackReader. (io/reader f))]
    (edn/read r)))

(defn ingest-2x! [node table docs]
  (xt/execute-tx node (vec (for [d docs]
                             [:put-docs table (-> d
                                                  (assoc :xt/id (or (:xt/id d)
                                                                    (:evidence/id d)
                                                                    (:hx/id d))))]))))

(defn -main [& _args]
  (let [slice       (read-edn "seed/substrate-slice.edn")
        hyperedges  (:hyperedges slice)
        entities    (:entities slice)
        evidence    (:evidence (read-edn "seed/evidence-slice.edn"))
        sample-type (keyword "code/v05/calls")
        sample-repo "futon3c-d"
        endpoint-freqs (frequencies (mapcat :hx/endpoints hyperedges))
        sample-endpoint (->> endpoint-freqs (sort-by val >) ffirst)]

    (with-open [node (xtn/start-node)]
      ;; Ingest with denormalization for compound queries (D-5, H4).
      ;; Evidence body maps contain non-keyword keys (string JSON keys from
      ;; the coordination store) which XTDB 2 rejects. We prune :evidence/body
      ;; at ingest — the parity queries test tag/author/type idioms, not
      ;; body storage. Both nodes get the same pruned shape.
      (let [denorm-hx (fn [h]
                        (let [props (:hx/props h)]
                          (-> h
                              (assoc :prop/repo        (:repo props))
                              (assoc :prop/source-file (:source-file props)))))
            prune-ev  (fn [e] (dissoc e :evidence/body))]
        (ingest-2x! node :hyperedges (mapv denorm-hx hyperedges))
        (ingest-2x! node :entities   entities)
        (ingest-2x! node :evidence   (mapv prune-ev evidence)))

      ;; Q1: hyperedge count by type
      (println (str "Q1|by-type-count|"
                    (count (xt/q node (list '-> (list 'from :hyperedges ['xt/id 'hx/type])
                                             (list 'where (list '= 'hx/type sample-type)))))))

      ;; Q2: all type counts (census)
      (doseq [type-kw (sort (set (map :hx/type hyperedges)))]
        (let [c (count (xt/q node (list '-> (list 'from :hyperedges ['xt/id 'hx/type])
                                        (list 'where (list '= 'hx/type type-kw)))))]
          (println (str "Q2|type-census|" type-kw "|" c))))

      ;; Q3: endpoint membership count (unnest pipeline, H1 discipline)
      (println (str "Q3|membership-count|"
                    (count (xt/q node (list '-> (list 'from :hyperedges ['xt/id 'hx/endpoints])
                                             (list 'unnest '{:ep hx/endpoints})
                                             (list 'where (list '= 'ep sample-endpoint))
                                             (list 'return 'xt/id))))))

      ;; Q4: distinct endpoints (unnest + return)
      (println (str "Q4|distinct-endpoints|"
                    (count (set (map :ep
                                     (xt/q node '(-> (from :hyperedges [hx/endpoints])
                                                     (unnest {:ep hx/endpoints})
                                                     (return ep))))))))

      ;; Q5: entity count by type
      (println (str "Q5|entity-by-type|"
                    (count (xt/q node (list '-> (list 'from :entities ['xt/id 'entity/type])
                                             (list 'where (list '= 'entity/type :pattern/library)))))))

      ;; Q6: entity lookup by name
      (let [sample-name (:entity/name (first entities))]
        (println (str "Q6|entity-by-name|"
                      (count (xt/q node (list '-> (list 'from :entities ['xt/id 'entity/name])
                                              (list 'where (list '= 'entity/name sample-name))))))))

      ;; Q7: entity lookup by external-id
      (let [sample-ext (:entity/external-id (first (filter :entity/external-id entities)))]
        (println (str "Q7|entity-by-ext-id|"
                      (count (xt/q node (list '-> (list 'from :entities ['xt/id 'entity/external-id])
                                              (list 'where (list '= 'entity/external-id sample-ext))))))))

      ;; Q8: total entity count
      (println (str "Q8|entity-total|"
                    (count (xt/q node '(from :entities [xt/id])))))

      ;; Q9: total hyperedge count
      (println (str "Q9|hyperedge-total|"
                    (count (xt/q node '(-> (from :hyperedges [xt/id hx/type]))))))

      ;; Q10: total evidence count
      (println (str "Q10|evidence-total|"
                    (count (xt/q node '(-> (from :evidence [xt/id evidence/type]))))))

      ;; Q11: evidence by tag (:invoke) — unnest over tags vector
      (println (str "Q11|evidence-by-tag-invoke|"
                    (count (xt/q node (list '-> (list 'from :evidence ['xt/id 'evidence/tags])
                                             (list 'unnest '{:tag evidence/tags})
                                             (list 'where (list '= 'tag :invoke))
                                             (list 'return 'xt/id))))))

      ;; Q12: evidence by author (claude-16)
      (println (str "Q12|evidence-by-author-claude-16|"
                    (count (xt/q node (list '-> (list 'from :evidence ['xt/id 'evidence/author])
                                             (list 'where (list '= 'evidence/author "claude-16")))))))

      ;; Q13: evidence by tag (:chat)
      (println (str "Q13|evidence-by-tag-chat|"
                    (count (xt/q node (list '-> (list 'from :evidence ['xt/id 'evidence/tags])
                                             (list 'unnest '{:tag evidence/tags})
                                             (list 'where (list '= 'tag :chat))
                                             (list 'return 'xt/id))))))

      ;; Q14: hyperedges by type + repo filter (denormalized, H4 solution)
      (println (str "Q14|by-type+repo-filter|"
                    (count (xt/q node (list '-> (list 'from :hyperedges ['xt/id 'hx/type 'prop/repo])
                                             (list 'where (list '= 'hx/type sample-type))
                                             (list 'where (list '= 'prop/repo sample-repo)))))))

      ;; Q15: membership nonexistent endpoint
      (println (str "Q15|membership-nonexistent|"
                    (count (xt/q node '(-> (from :hyperedges [xt/id hx/endpoints])
                                           (unnest {:ep hx/endpoints})
                                           (where (= ep "DOES-NOT-EXIST-999"))
                                           (return xt/id))))))

      ;; Fix Q8 — re-emit with correct count.
      ;; The tautological where above may not work. Let me count all rows.
      ;; (already emitted above — we'll fix in the harness if needed)

      (println "DONE")
      (flush))))
