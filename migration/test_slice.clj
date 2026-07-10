;; E-futon1a-to-futon1b-migration-pipeline — S2/S3 test against seed slice.
;;
;; Proves the transform + ingest pipeline against the known-good seed slice
;; (the same data P1/P2/P3 proved against). This is the medium-chunk gate
;; before the full run.
;;
;; Run: cd /home/joe/code/futon1b && clojure -M:node -e '(load-file "migration/test_slice.clj")'
(require '[xtdb.node :as xtn]
         '[xtdb.api :as xt]
         '[clojure.edn :as edn]
         '[clojure.java.io :as io])
(require '[migration.transform :as xf])
(import '[java.io PushbackReader])

(defn read-edn [f]
  (with-open [r (PushbackReader. (io/reader f))]
    (edn/read r)))

;; Load seed slices.
(def substrate-slice (read-edn "seed/substrate-slice.edn"))
(def evidence-slice (read-edn "seed/evidence-slice.edn"))

(def hyperedges (:hyperedges substrate-slice))
(def entities (:entities substrate-slice))
(def evidence (:evidence evidence-slice))

;; Transform all docs.
(def shape-log (xf/make-shape-log))
(def tx-hyperedges (xf/transform-docs hyperedges shape-log))
(def tx-entities (xf/transform-docs entities shape-log))
(def tx-evidence (xf/transform-docs evidence shape-log))

;; --- Transform assertions ---

(println "=== S2: Transform assertions ===")

;; T1: hyperedge count preserved.
(println "T1 hx-count-preserved:"
         (if (= (count hyperedges) (count tx-hyperedges)) "PASS" "FAIL")
         (count hyperedges) "→" (count tx-hyperedges))

;; T2: evidence count preserved.
(println "T2 evidence-count-preserved:"
         (if (= (count evidence) (count tx-evidence)) "PASS" "FAIL")
         (count evidence) "→" (count tx-evidence))

;; T3: all evidence bodies are XTDB-2-safe (string or keyword-keyed map).
(let [safe (count (filter #(or (string? (:evidence/body %))
                                (and (map? (:evidence/body %))
                                     (not (xf/string-keyed-map? (:evidence/body %)))))
                          tx-evidence))]
  (println "T3 evidence-body-safe:"
           (if (= (count tx-evidence) safe) "PASS" "FAIL")
           safe "/" (count tx-evidence) "bodies are XTDB-2-safe"))

;; T4: hyperedges have denormalized :prop/repo.
(let [with-prop-repo (count (filter :prop/repo tx-hyperedges))
      orig-with-props (count (filter #(get-in % [:hx/props :repo]) hyperedges))]
  (println "T4 hx-prop-repo-denormalized:"
           (if (= orig-with-props with-prop-repo) "PASS" "FAIL")
           with-prop-repo "/" orig-with-props "have :prop/repo"))

;; T5: original :hx/props preserved.
;; Check first hyperedge specifically.
(println "T5 hx-props-preserved:"
         (if (= (:hx/props (first hyperedges))
                (:hx/props (first tx-hyperedges)))
           "PASS" "FAIL"))

;; T6: all docs have :xt/id.
(let [all-docs (concat tx-hyperedges tx-entities tx-evidence)
      with-id (count (filter :xt/id all-docs))]
  (println "T6 all-docs-have-xt-id:"
           (if (= (count all-docs) with-id) "PASS" "FAIL")
           with-id "/" (count all-docs)))

;; T7: shape log — should have NO unknown types for slice data.
(println "T7 no-unknown-shapes:"
         (if (empty? @shape-log) "PASS" "FAIL")
         "(shape-log-entries:" (count @shape-log) ")")

(println)

;; --- Ingest assertions ---

(println "=== S3: Ingest assertions (transformed → futon1b node) ===")

(defn ingest! [node table docs]
  (xt/execute-tx node (vec (for [d docs] [:put-docs table d]))))

(with-open [node (xtn/start-node)]
  (ingest! node :hyperedges tx-hyperedges)
  (ingest! node :entities tx-entities)
  (ingest! node :evidence tx-evidence)

  ;; A1: hyperedge count matches.
  (let [got (count (xt/q node '(from :hyperedges [xt/id])))]
    (println "A1 hx-count:" (count tx-hyperedges) "→" got
             (if (= (count tx-hyperedges) got) "PASS" "FAIL")))

  ;; A2: entity count matches.
  (let [got (count (xt/q node '(from :entities [xt/id])))]
    (println "A2 entity-count:" (count tx-entities) "→" got
             (if (= (count tx-entities) got) "PASS" "FAIL")))

  ;; A3: evidence count matches.
  (let [got (count (xt/q node '(from :evidence [xt/id])))]
    (println "A3 evidence-count:" (count tx-evidence) "→" got
             (if (= (count tx-evidence) got) "PASS" "FAIL")))

  ;; A4: query by denormalized prop/repo works (H4 solution).
  (let [sample-repo (-> hyperedges first :hx/props :repo)
        got (count (xt/q node (list '-> (list 'from :hyperedges ['xt/id 'prop/repo])
                                    (list 'where (list '= 'prop/repo sample-repo)))))]
    (println "A4 query-by-prop-repo:" sample-repo "got" got
             (if (pos? got) "PASS" "FAIL")))

  ;; A5: evidence by tag works (unnest idiom).
  (let [got (count (xt/q node '(-> (from :evidence [xt/id evidence/tags])
                                   (unnest {:tag evidence/tags})
                                   (where (= tag :invoke))
                                   (return xt/id))))]
    (println "A5 evidence-by-tag-invoke: got" got
             (if (pos? got) "PASS" "FAIL")))

  (println)
  (println "=== S2/S3 slice test complete ==="))
