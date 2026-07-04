;; M-futon1b-port P2b — compound hyperedge query translations with runtime assertions.
;;
;; Translates futon1a's `hyperedges-by-type` compound idiom (by-type + :hx/props
;; filtering) to XTDB 2 XTQL. Each assertion queries XTQL against the ingested
;; slice and compares to an oracle computed independently in plain Clojure.
;;
;; Run: cd /home/joe/code/futon1b && clojure -M:node -e '(load-file "p2b_compound_queries.clj")'
;;
;; HAZARD CATALOG (extends NOTES.md):
;;   H1: naive unnest → silent unfiltered results. Fix: pipeline unnest+where.
;;   H2: Clojure-isms in XTQL where → runtime error. Fix: XTQL predicates only.
;;   H3: from silently ignores extra positional forms → silent unfiltered.
;;       Fix: where must be threaded: (-> (from ...) (where ...)).
;;   H4 (NEW, found in this script): XTQL where CANNOT navigate nested maps.
;;       get, get-in, and .get all fail ("not applicable to types struct").
;;       The {parent [child]} nested bind spec also fails on qualified keywords
;;       ("Attribute in bind spec must be keyword"). Fix: denormalize nested
;;       :hx/props fields into top-level :prop/<key> columns at ingest time.
;;       This is strictly better than futon1a's approach — futon1a itself does
;;       props filtering in Clojure (prop-matches? with get-in on materialized
;;       entities), never in Datalog. Denormalization pushes the filter INTO
;;       the query layer where it belongs.
;;
;; Every query below uses the threaded pipeline form exclusively (H3 discipline).
(require '[xtdb.node :as xtn] '[xtdb.api :as xt]
         '[clojure.edn :as edn] '[clojure.java.io :as io])
(import '[java.io PushbackReader])

(defn read-edn [f]
  (with-open [r (PushbackReader. (io/reader f))]
    (edn/read r)))

(def slice (read-edn "seed/substrate-slice.edn"))
(def hyperedges (:hyperedges slice))

;; ---------------------------------------------------------------------------
;; Oracle helpers — compute expected results in plain Clojure from the slice.
;; ---------------------------------------------------------------------------

(defn oracle-by-type [type-kw]
  (filter #(= type-kw (:hx/type %)) hyperedges))

(defn oracle-by-type-and-prop [type-kw prop-kw prop-val]
  (filter #(and (= type-kw (:hx/type %))
                (= prop-val (get-in % [:hx/props prop-kw])))
          hyperedges))

(defn oracle-census-by-prop [type-kw prop-kw]
  (->> (oracle-by-type type-kw)
       (map #(get-in % [:hx/props prop-kw]))
       (group-by identity)
       (map (fn [[k vs]] [k (count vs)]))
       (into {})))

;; ---------------------------------------------------------------------------
;; Ingest with denormalization.
;;
;; XTQL cannot navigate nested :hx/props maps in `where` (H4). We denormalize
;; the props we need for filtering into top-level :prop/<key> columns at ingest
;; time. The original :hx/props map is preserved unchanged alongside them.
;; This mirrors futon1a's data model (nested props map) while enabling XTQL
;; compound queries that futon1a could only do via Clojure-side post-filtering.
;; ---------------------------------------------------------------------------

(defn denorm-hx
  "Add top-level :prop/<key> columns for each :hx/props entry, preserving the
   original :hx/props map."
  [h]
  (let [props (:hx/props h)]
    (-> h
        (assoc :prop/repo        (:repo props))
        (assoc :prop/source-file (:source-file props))
        (assoc :prop/phase       (:phase props)))))

(defn ingest! [node table docs]
  (xt/execute-tx node (vec (for [d docs] [:put-docs table d]))))

;; ---------------------------------------------------------------------------

(with-open [node (xtn/start-node)]
  (ingest! node :hyperedges (mapv denorm-hx hyperedges))

  ;; Pick a type family and props from the real data for assertions.
  ;; Using keyword constructor to avoid clj-kondo false-positive on the
  ;; double-slash literal :code/v05/calls (valid in Clojure, flagged by kondo).
  (let [test-type (keyword "code/v05/calls")        ; 150 hyperedges, 13 repos
        test-repo "futon3c-d"                       ; 49 of the 150 calls
        test-repo-absent "NO-SUCH-REPO-999"         ; guaranteed absent
        test-source-file
        "/home/joe/code/futon3c/src/futon3c/agency/federation.clj"] ; 2 of 150 calls

  (println)
  (println "=== P2b: compound hyperedge query translations ===")
  (println "Type family:" test-type "| repo:" test-repo)
  (println "Denormalized :hx/props → :prop/repo, :prop/source-file, :prop/phase")
  (println)

  ;; ========================================================================
  ;; ASSERTION 1 — Compound: by-type + repo filter.
  ;; futon1a original: Datalog query for :hx/type, then Clojure prop-matches?
  ;; on :hx/props :repo. XTQL translation: two threaded where stages, one for
  ;; type and one for the denormalized :prop/repo.
  ;; ========================================================================
  (println "--- Assertion 1: by-type + repo filter ---")
  (let [expected (count (oracle-by-type-and-prop test-type :repo test-repo))
        got (count (xt/q node (list '->
                                    (list 'from :hyperedges ['xt/id 'hx/type 'prop/repo])
                                    (list 'where (list '= 'hx/type test-type))
                                    (list 'where (list '= 'prop/repo test-repo)))))]
    (println (format "  by-type(%s) + repo(%s): expected %d got %d %s"
                     test-type test-repo expected got
                     (if (= expected got) "PASS" "FAIL"))))

  ;; ========================================================================
  ;; ASSERTION 2 — Compound: by-type + source-file filter.
  ;; Same pattern, filtering on :prop/source-file instead of :prop/repo.
  ;; ========================================================================
  (println)
  (println "--- Assertion 2: by-type + source-file filter ---")
  (let [expected (count (oracle-by-type-and-prop test-type :source-file test-source-file))
        got (count (xt/q node (list '->
                                    (list 'from :hyperedges ['xt/id 'hx/type 'prop/source-file])
                                    (list 'where (list '= 'hx/type test-type))
                                    (list 'where (list '= 'prop/source-file test-source-file)))))]
    (println (format "  by-type(%s) + source-file: expected %d got %d %s"
                     test-type expected got
                     (if (= expected got) "PASS" "FAIL"))))

  ;; ========================================================================
  ;; ASSERTION 3 — Compound: by-type + repo + limit.
  ;; futon1a original: sort eids, lazy pull, filter, then (take limit).
  ;; XTQL: two where stages + (limit N). Assert count = min(oracle, limit).
  ;; ========================================================================
  (println)
  (println "--- Assertion 3: by-type + repo + limit ---")
  (let [oracle-matches (count (oracle-by-type-and-prop test-type :repo test-repo))
        lim 10
        expected (min oracle-matches lim)
        got (count (xt/q node (list '->
                                    (list 'from :hyperedges ['xt/id 'hx/type 'prop/repo])
                                    (list 'where (list '= 'hx/type test-type))
                                    (list 'where (list '= 'prop/repo test-repo))
                                    (list 'limit lim))))]
    (println (format "  by-type(%s) + repo(%s) + limit(%d): expected %d (min(%d,%d)) got %d %s"
                     test-type test-repo lim expected oracle-matches lim got
                     (if (= expected got) "PASS" "FAIL"))))

  ;; ========================================================================
  ;; ASSERTION 4 — Compound: by-type + repo filter returning empty.
  ;; futon1a original: the cond->> filter would simply return no docs.
  ;; XTQL: same compound query with a repo that doesn't exist. Assert 0.
  ;; ========================================================================
  (println)
  (println "--- Assertion 4: by-type + repo filter (nonexistent → empty) ---")
  (let [expected (count (oracle-by-type-and-prop test-type :repo test-repo-absent))
        got (count (xt/q node (list '->
                                    (list 'from :hyperedges ['xt/id 'hx/type 'prop/repo])
                                    (list 'where (list '= 'hx/type test-type))
                                    (list 'where (list '= 'prop/repo test-repo-absent)))))]
    (println (format "  by-type(%s) + repo(%s): expected %d got %d %s"
                     test-type test-repo-absent expected got
                     (if (= expected got) "PASS" "FAIL"))))

  ;; ========================================================================
  ;; ASSERTION 5 — Props-based grouping (census-with-filter).
  ;; For one type family, group by :hx/props :repo and count per repo.
  ;; futon1a has no direct Datalog equivalent for this (it always fetched
  ;; then counted in Clojure). The XTQL approach: fetch all rows of the type
  ;; with their :prop/repo, then group-by in Clojure (the census is a
  ;; presentation concern, not a query concern). Each group count is
  ;; asserted against the oracle independently.
  ;; ========================================================================
  (println)
  (println "--- Assertion 5: census — by-type grouped by :prop/repo ---")
  (let [oracle-census (oracle-census-by-prop test-type :repo)
        rows (xt/q node (list '->
                              (list 'from :hyperedges ['xt/id 'hx/type 'prop/repo])
                              (list 'where (list '= 'hx/type test-type))))
        got-census (->> rows
                        (map :prop/repo)
                        (group-by identity)
                        (map (fn [[k vs]] [k (count vs)]))
                        (into {}))
        ;; Compare: every oracle repo appears with the right count,
        ;; and no extra repos appear.
        all-repos (set (concat (keys oracle-census) (keys got-census)))
        pass? (every? (fn [repo]
                        (= (get oracle-census repo 0)
                           (get got-census repo 0)))
                      all-repos)]
    (println (format "  type %s: %d repos in oracle, %d in XTQL"
                     test-type (count oracle-census) (count got-census)))
    (println "  per-repo breakdown (oracle → XTQL):")
    (doseq [repo (sort (keys oracle-census))]
      (println (format "    %-20s oracle=%-4d xtql=%-4d %s"
                       repo
                       (get oracle-census repo)
                       (get got-census repo)
                       (if (= (get oracle-census repo)
                              (get got-census repo 0))
                         "✓" "✗"))))
    (println (format "  census match: %s" (if pass? "PASS" "FAIL"))))

  (println)
  (println "=== P2b compound query translations complete ===")))
