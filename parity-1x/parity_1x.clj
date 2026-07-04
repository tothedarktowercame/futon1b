;; M-futon1b-port P3 — XTDB 1.x parity side.
;;
;; Ingests the slice into an XTDB 1.x in-process node, runs the ≥12
;; representative queries using Datalog, and writes results as pipe-delimited
;; lines to stdout for the parity harness to capture.
;;
;; Run: cd /home/joe/code/futon1b/parity-1x && clojure -M:node -m parity-1x
;;
;; XTDB 1.x uses Datalog queries (xtdb/q with :find/:where maps) and
;; xtdb.api/submit-tx + xtdb.api/put (not put-docs).
(ns parity-1x
  (:require [xtdb.api :as xt]
            [clojure.edn :as edn]
            [clojure.java.io :as io])
  (:import [java.io PushbackReader])
  (:gen-class))

(defn read-edn [f]
  (with-open [r (PushbackReader. (io/reader f))]
    (edn/read r)))

(defn start-node-1x []
  ;; XTDB 1.x in-process node with in-memory KV (no disk persistence).
  ;; Module ref follows the futon1a pattern: 'xtdb.rocksdb/->kv-store →
  ;; 'xtdb.mem-kv/->kv-store. Mem-kv is purely in-memory when no :db-dir.
  (xt/start-node
   {:xtdb/tx-log              {:kv-store {:xtdb/module 'xtdb.mem-kv/->kv-store}}
    :xtdb/document-store      {:kv-store {:xtdb/module 'xtdb.mem-kv/->kv-store}}
    :xtdb/index-store         {:kv-store {:xtdb/module 'xtdb.mem-kv/->kv-store}}}))

(defn ingest-1x! [node docs]
  ;; Prune :evidence/body — its string-keyed maps are rejected by XTDB 2,
  ;; so both sides prune for parity. The parity queries test tag/author/type.
  (let [docs     (if (and (seq docs) (:evidence/id (first docs)))
                   (map #(dissoc % :evidence/body) docs)
                   docs)
        tx-ops (vec (for [d docs]
                      [:xtdb.api/put (-> d
                                         (assoc :xt/id (or (:xt/id d)
                                                           (:evidence/id d)
                                                           (:hx/id d))))]))]
    (xt/await-tx node (xt/submit-tx node tx-ops))))

(defn -main [& _args]
  (let [slice       (read-edn "../seed/substrate-slice.edn")
        hyperedges  (:hyperedges slice)
        entities    (:entities slice)
        evidence    (:evidence (read-edn "../seed/evidence-slice.edn"))
        sample-type (keyword "code/v05/calls")
        sample-repo "futon3c-d"
        endpoint-freqs (frequencies (mapcat :hx/endpoints hyperedges))
        sample-endpoint (->> endpoint-freqs (sort-by val >) ffirst)]

    (with-open [node (start-node-1x)]
      (ingest-1x! node hyperedges)
      (ingest-1x! node entities)
      (ingest-1x! node evidence)

      (let [db (xt/db node)]

        ;; Q1: hyperedge count by type
        (println (str "Q1|by-type-count|"
                      (count (xt/q db '{:find [e] :in [t] :where [[e :hx/type t]]}
                                   sample-type))))

        ;; Q2: all type counts (census)
        (doseq [type-kw (sort (set (map :hx/type hyperedges)))]
          (let [c (count (xt/q db '{:find [e] :in [t] :where [[e :hx/type t]]} type-kw))]
            (println (str "Q2|type-census|" type-kw "|" c))))

        ;; Q3: endpoint membership count
        (println (str "Q3|membership-count|"
                      (count (xt/q db '{:find [e] :in [eid] :where [[e :hx/endpoints eid]]}
                                   sample-endpoint))))

        ;; Q4: distinct endpoints
        (let [all-hx (map first (xt/q db '{:find [(pull e [*])] :where [[e :hx/type _]]}))
              eps    (set (mapcat :hx/endpoints all-hx))]
          (println (str "Q4|distinct-endpoints|" (count eps))))

        ;; Q5: entity count by type (:pattern/library)
        (println (str "Q5|entity-by-type|"
                      (count (xt/q db '{:find [e] :in [t] :where [[e :entity/type t]]}
                                   :pattern/library))))

        ;; Q6: entity lookup by name
        (let [sample-name (:entity/name (first entities))]
          (println (str "Q6|entity-by-name|"
                        (count (xt/q db '{:find [e] :in [name] :where [[e :entity/name name]]}
                                     sample-name)))))

        ;; Q7: entity lookup by external-id
        (let [sample-ext (:entity/external-id (first (filter :entity/external-id entities)))]
          (println (str "Q7|entity-by-ext-id|"
                        (count (xt/q db '{:find [e] :in [ext] :where [[e :entity/external-id ext]]}
                                     sample-ext)))))

        ;; Q8: total entity count
        (println (str "Q8|entity-total|"
                      (count (xt/q db '{:find [e] :where [[e :entity/type _]]}))))

        ;; Q9: total hyperedge count
        (println (str "Q9|hyperedge-total|"
                      (count (xt/q db '{:find [e] :where [[e :hx/type _]]}))))

        ;; Q10: total evidence count
        (println (str "Q10|evidence-total|"
                      (count (xt/q db '{:find [e] :where [[e :evidence/type _]]}))))

        ;; Q11: evidence by tag (:invoke)
        (println (str "Q11|evidence-by-tag-invoke|"
                      (count (xt/q db '{:find [e] :in [tag] :where [[e :evidence/tags tag]]}
                                   :invoke))))

        ;; Q12: evidence by author (claude-16)
        (println (str "Q12|evidence-by-author-claude-16|"
                      (count (xt/q db '{:find [e] :in [a] :where [[e :evidence/author a]]}
                                   "claude-16"))))

        ;; Q13: evidence by tag (:chat)
        (println (str "Q13|evidence-by-tag-chat|"
                      (count (xt/q db '{:find [e] :in [tag] :where [[e :evidence/tags tag]]}
                                   :chat))))

        ;; Q14: hyperedges by type + repo filter (Clojure-side, same as futon1a)
        (let [all-of-type (map first (xt/q db '{:find [(pull e [*])] :in [t] :where [[e :hx/type t]]}
                                             sample-type))
              filtered    (filter #(= sample-repo (get-in % [:hx/props :repo])) all-of-type)]
          (println (str "Q14|by-type+repo-filter|" (count filtered))))

        ;; Q15: membership nonexistent endpoint
        (println (str "Q15|membership-nonexistent|"
                      (count (xt/q db '{:find [e] :in [eid] :where [[e :hx/endpoints eid]]}
                                   "DOES-NOT-EXIST-999"))))

        (println "DONE")
        (flush)))))
