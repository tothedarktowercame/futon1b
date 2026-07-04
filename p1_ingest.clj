;; M-futon1b-port P1 — ingest the real substrate-2 slice into a throwaway
;; XTDB 2 in-process node and assert against the export manifest.
;;
;; Run: cd /home/joe/code/futon1b && clojure -M:node -e '(load-file "p1_ingest.clj")'
;;
;; The manifest (substrate-slice-manifest.edn) is the oracle — its counts
;; were computed at export time by claude-16 from the same :7071 store.
;; This script re-derives the same counts from what actually landed in the
;; futon1b node and asserts equality. If any assertion fails, that is a
;; data-loss or translation bug, not a test failure to route around.
(require '[xtdb.node :as xtn] '[xtdb.api :as xt]
         '[clojure.edn :as edn] '[clojure.java.io :as io])
(import '[java.io PushbackReader])

(defn read-edn [f]
  (with-open [r (PushbackReader. (io/reader f))]
    (edn/read r)))

;; ---------------------------------------------------------------------------
;; Load the slice and manifest.
;; ---------------------------------------------------------------------------
(def slice    (read-edn "seed/substrate-slice.edn"))
(def manifest (read-edn "seed/substrate-slice-manifest.edn"))

(def hyperedges (:hyperedges slice))
(def entities   (:entities slice))

;; ---------------------------------------------------------------------------
;; Oracle counts from the manifest.
;; ---------------------------------------------------------------------------
(def manifest-hx-total   (:hyperedge-count manifest))
(def manifest-hx-by-type (:by-type manifest))
(def manifest-ent-total  (:entity-count manifest))
(def manifest-endpoints  (:distinct-endpoints-in-slice manifest))

;; ---------------------------------------------------------------------------
;; Ingest into a throwaway in-process node.
;; XTDB 2 put-docs takes the doc map directly; :xt/id is the primary key.
;; Hyperedges go into :hyperedges, entities into :entities — separate tables
;; matching the slice structure. No schema declaration needed (foothold S2).
;; ---------------------------------------------------------------------------
(defn ingest! [node table docs]
  (xt/execute-tx node (vec (for [d docs] [:put-docs table d]))))

(with-open [node (xtn/start-node)]
  (ingest! node :hyperedges hyperedges)
  (ingest! node :entities   entities)

  ;; -------------------------------------------------------------------------
  ;; Assertions against the manifest (the P1 gate).
  ;; -------------------------------------------------------------------------

  ;; A1 — total hyperedge count.
  (let [got (count (xt/q node '(from :hyperedges [xt/id])))]
    (println "A1 hyperedge-total: expected" manifest-hx-total "got" got
             (if (= manifest-hx-total got) "PASS" "FAIL")))

  ;; A2 — hyperedge count per :hx/type (all 10 type families).
  ;; `where` must be a threaded pipeline stage: (-> (from ...) (where ...)),
  ;; NOT an inline clause inside `from`. Inline extras after the column vector
  ;; are silently ignored — a silent-wrong-result datum (the foothold's hazard
  ;; class, now encountered on real data).
  (doseq [[type-kw expected] (sort-by str manifest-hx-by-type)]
    (let [got (count (xt/q node (list '->
                                      (list 'from :hyperedges ['xt/id 'hx/type])
                                      (list 'where (list '= 'hx/type type-kw)))))]
      (println "A2 type" type-kw ": expected" expected "got" got
               (if (= expected got) "PASS" "FAIL"))))

  ;; A3 — total entity count.
  (let [got (count (xt/q node '(from :entities [xt/id])))]
    (println "A3 entity-total: expected" manifest-ent-total "got" got
             (if (= manifest-ent-total got) "PASS" "FAIL")))

  ;; A4 — distinct endpoint strings across all hyperedges.
  ;; This uses the unnest idiom proven in the foothold (S3a) — the same
  ;; pipeline P2/P3 will use for membership queries, now on real data.
  (let [all-eps (xt/q node '(-> (from :hyperedges [hx/endpoints])
                                (unnest {:ep hx/endpoints})
                                (return ep)))
        distinct-eps (count (set (map :ep all-eps)))]
    (println "A4 distinct-endpoints: expected" manifest-endpoints "got" distinct-eps
             (if (= manifest-endpoints distinct-eps) "PASS" "FAIL")))

  ;; A5 — spot-check: every hyperedge that landed has a non-empty :hx/endpoints
  ;; vector. A silent data loss (e.g. put-docs dropping the multivalued attr)
  ;; would show zero-count endpoint vectors. Checked Clojure-side since XTQL
  ;; does not have Clojure's `empty?` (the foothold's lesson: XTQL is not
  ;; Clojure, even when it looks like it).
  (let [all-hx (xt/q node '(from :hyperedges [xt/id hx/endpoints]))
        no-eps (count (filter #(empty? (:hx/endpoints %)) all-hx))]
    (println "A5 hyperedges-with-empty-endpoints:" no-eps
             (if (zero? no-eps) "PASS" "FAIL")))

  (println)
  (println "=== P1 ingest assertions complete ==="))
