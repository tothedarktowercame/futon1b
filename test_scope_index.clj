(ns test-scope-index
  "Throwaway-sidecar tests for the P5 scope index (futon1b-scopeindex):
   per-kind counts vs an independent count (with `/`-kinds present and the
   passage's :source :kind NOT counted), per-paper transactional re-index,
   watermark skip + forced idempotence, Q7 closed-interval boundaries, and
   the prototype's bad case (a parser that drops `/` kinds fails the
   per-kind check).

  Run: clojure -M:node -m test-scope-index"
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [futon1b-text :as text]
            [futon1b-scopeindex :as scope])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [java.time Instant]))

(def ^:private unqualified {:builder-fn rs/as-unqualified-maps})

(defn- temp-dir []
  (-> (Files/createTempDirectory
       "futon1b-scopeindex-test-"
       (make-array FileAttribute 0))
      .toFile
      .getAbsolutePath))

(defn- check! [label value]
  (println (format "  %-66s %s" label (if value "PASS" "FAIL")))
  (assert value label))

(defn- ds [] @(var-get (ns-resolve 'futon1b-text '!ds)))

;; --- Fixture ---------------------------------------------------------------
;; Two papers, three expo files. Kinds include `/`-containing kinds. Every
;; passage carries :source {:kind :expository}, which must NOT be counted.

(def fixture-files
  {"p1_0001.edn"
   "{:paper/id \"p1\" :passage/id \"p1:pass-0001:L10-20\"
     :source {:lines [10 20] :kind :expository}
     :provenance {:generator \"expository-json/v1\" :model \"fixture\"}
     :scopes [{:id :s1 :kind :connection :source {:lines [10 20]}}
              {:id :s2 :kind :universal-property/characterizes
               :source {:lines [12 14]}}]}"
   "p1_0002.edn"
   "{:paper/id \"p1\" :passage/id \"p1:pass-0002:L30-40\"
     :source {:lines [30 40] :kind :expository}
     :provenance {:generator \"expository-json/v1\" :model \"fixture\"}
     :scopes [{:id :s1 :kind :rationale/telos :source {:lines [31 35]}}]}"
   "p2_0001.edn"
   "{:paper/id \"p2\" :passage/id \"p2:pass-0001:L1-5\"
     :source {:lines [1 5] :kind :expository}
     :provenance {:generator \"expository-json/v2\" :model \"fixture\"}
     :scopes [{:id :s1 :kind :connection :source {:lines [1 3]}}
              {:id :s2 :kind :connection :source {:lines [2 5]}}
              {:id :s3 :kind :obstruction :source {:lines [4 4]}}]}"})

;; Independent expected per-kind count, straight from the fixture literals
;; above (NOT derived by running the parser under test).
(def expected-kinds
  {"connection" 3
   "universal-property/characterizes" 1
   "rationale/telos" 1
   "obstruction" 1})

;; --- P6a fixture: marks (JSON, char offsets) and graphs (EDN) ---------------

(defn- paper-text [n]
  (apply str (map #(format "line %02d of the paper text\n" %)
                  (range 1 (inc n)))))

(defn- line-starts [text]
  ;; 1-based: line-starts[l] = char offset of line l's first char.
  (vec (into [nil 0]
             (keep-indexed (fn [i c] (when (= c \newline) (inc i))) text))))

(defn- char-span
  "Exclusive-end char offsets covering lines [l0 l1] of text."
  [text l0 l1]
  (let [starts (line-starts text)
        c0 (nth starts l0)
        l1-start (nth starts l1)]
    [c0 (str/index-of text "\n" l1-start)]))

(def p1-text (paper-text 40))
(def p2-text (paper-text 5))

(def p1-marks
  (let [[a0 a1] (char-span p1-text 12 14)
        [b0 b1] (char-span p1-text 14 14)]
    [{"start" a0 "end" a1 "layer" "dp" "kind" "let-binder" "tip" "binds X"}
     {"start" b0 "end" b1 "layer" "dp" "kind" "definiendum"
      "term-index" 0 "tip" "definiendum #0"}]))

(def p2-marks
  (let [[c0 c1] (char-span p2-text 1 3)]
    [{"start" c0 "end" c1 "layer" "dp" "kind" "math" "tip" "p2 mark"}]))

(defn- marks-json [paper text marks]
  ;; Fable paper ids carry the trailing "-dp" layer tag.
  (json/generate-string {"paper" (str paper "-dp")
                         "text" text
                         "marks" marks}))

(def fixture-graph-files
  {"p1__p0.edn"
   "{:paper/id \"p1\" :passage/id \"p1:pass-0001:L10-20\"
     :source {:lines [10 20] :kind :proof}
     :nodes [{:id :n1 :kind :claim :source {:lines [10 11]}}
             {:id :n2 :kind :object :source {:lines [14 14]}}
             {:id :n3 :kind :claim :source {:lines [31 35]}}]}"
   ;; Semcheck REPORT, not a graph: must NOT be indexed (it would throw:
   ;; no :paper/id).
   "p1__p0.rung2.edn"
   "{:schema :futon6.iatc-semcheck.v1 :check-summary {}}"
   "p2__p0.edn"
   "{:paper/id \"p2\" :passage/id \"p2:pass-0001:L1-5\"
     :source {:lines [1 5] :kind :proof}
     :nodes [{:id :n1 :kind :claim :source {:lines [1 2]}}]}"})

(defn- write-fixture! [dir]
  (let [expo (io/file dir "artifacts/expo")
        marks (io/file dir "artifacts/marks")
        graphs (io/file dir "artifacts/graphs")]
    (.mkdirs expo) (.mkdirs marks) (.mkdirs graphs)
    (doseq [[name body] fixture-files]
      (spit (io/file expo name) body))
    (spit (io/file marks "fable-p1-dp-emacs.json")
          (marks-json "p1" p1-text p1-marks))
    (spit (io/file marks "fable-p2-dp-emacs.json")
          (marks-json "p2" p2-text p2-marks))
    (doseq [[name body] fixture-graph-files]
      (spit (io/file graphs name) body))
    {:expo expo :marks marks :graphs graphs}))

(defn- q [sql & params]
  (jdbc/execute! (ds) (into [sql] params) unqualified))

(defn -main []
  (let [dir (temp-dir)
        run-dir (io/file dir "mark-test-run")
        dirs (write-fixture! run-dir)
        expo (:expo dirs)
        marks-dir (:marks dirs)
        graphs-dir (:graphs dirs)]
    (text/init! {:path (str dir "/throwaway.db")})
    (scope/init!)

    (println "== P5 scope sidecar tests ==")

    ;; --- index the fixture run -------------------------------------------
    ;; 3 expo + 2 marks + 2 graph files (the .rung2.edn report is excluded).
    (let [r (scope/index-run! (ds) (.getPath run-dir))]
      (check! "index-run! files" (= 7 (:files r)))
      (check! "index-run! rows" (= 6 (:rows r)))
      (check! "index-run! mark rows" (= 3 (:mark-rows r)))
      (check! "index-run! gnode rows" (= 4 (:gnode-rows r)))
      (check! "index-run! not skipped" (false? (:skipped r)))
      (check! "run name from dir" (= "mark-test-run" (:run r))))

    ;; --- per-kind counts vs independent count -----------------------------
    (let [counts (scope/kind-counts (ds) {:run "mark-test-run"})]
      (check! "per-kind counts equal independent fixture count"
              (= expected-kinds counts))
      (check! "`/`-containing kinds present (prototype's regex dropped these)"
              (and (= 1 (get counts "universal-property/characterizes"))
                   (= 1 (get counts "rationale/telos"))))
      (check! "passage :source :kind :expository NOT counted as a scope"
              (nil? (get counts "expository"))))

    ;; --- bad case: a parser that drops `/` kinds ---------------------------
    ;; Simulating the prototype's regex bug: drop every kind containing "/".
    ;; The per-kind check MUST fail against this parser's counts — that is
    ;; how the prototype shipped 772 rows instead of 857.
    (let [buggy (into {} (remove (fn [[k _]] (str/includes? k "/")))
                      (scope/kind-counts (ds) {:run "mark-test-run"}))]
      (check! "bad case: /-dropping parser FAILS the per-kind check"
              (not= expected-kinds buggy)))

    ;; --- file coordinates (I1 re-parse) -------------------------------------
    (let [rows (scope/scopes-of-paper (ds) "p1")]
      (check! "Q6 row count for p1" (= 3 (count rows)))
      (check! "Q6 rows ordered by line"
              (= [[10 20] [12 14] [31 35]] (mapv (juxt :l0 :l1) rows)))
      (check! "rows carry their source file (I1 re-parse coordinate)"
              (= #{"p1_0001.edn" "p1_0002.edn"} (set (map :file rows)))))

    ;; --- Q7 closed-interval boundaries ---------------------------------------
    ;; Scope s2 of p1 is [12 14]; convention: CLOSED, touching overlaps,
    ;; matching render_scope_margin.py's ink plates (a scope paints every
    ;; line in [l0,l1]).
    (let [overlap (fn [a b]
                    (mapv (juxt :l0 :l1)
                          (scope/scopes-overlapping (ds) "p1" a b)))]
      (check! "Q7 [14 99] touches [12 14] -> included (closed)"
              (some #{[12 14]} (overlap 14 99)))
      (check! "Q7 [15 99] clears [12 14] -> excluded"
              (not-any? #{[12 14]} (overlap 15 99)))
      (check! "Q7 [0 12] touches [12 14] from below -> included"
              (some #{[12 14]} (overlap 0 12)))
      (check! "Q7 [0 11] clears [12 14] from below -> excluded"
              (not-any? #{[12 14]} (overlap 0 11)))
      (check! "Q7 [13 13] strictly inside [12 14] -> included"
              (some #{[12 14]} (overlap 13 13))))

    ;; --- Q8 across papers ------------------------------------------------------
    (let [rows (scope/scopes-of-kind (ds) "connection")]
      (check! "Q8 connection across papers" (= #{"p1" "p2"} (set (map :paper rows))))
      (check! "Q8 row count" (= 3 (count rows))))

    ;; --- P6a: mark table ----------------------------------------------------
    (let [rows (scope/marks-overlapping (ds) "p1" 1 40)]
      (check! "P6a marks of p1" (= 2 (count rows)))
      (check! "P6a char offsets -> closed line spans"
              (= [[12 14] [14 14]] (mapv (juxt :l0 :l1) rows)))
      (check! "P6a rows carry verbatim char offsets (I1 file coordinate)"
              (= (char-span p1-text 12 14)
                 ((juxt :c0 :c1) (first rows))))
      (check! "P6a payload round-trips to the source mark"
              (= (first p1-marks)
                 (json/parse-string (:payload (first rows)))))
      (check! "P6a -dp layer tag stripped from paper id"
              (= #{"p1"} (set (map :paper rows)))))
    (check! "P6a .rung2.edn semcheck report NOT indexed"
            (= 0 (:n (first (q "SELECT count(*) AS n FROM gnode
                                 WHERE file LIKE '%rung2%'")))))
    (check! "P6a gnode spans verbatim"
            (= [[10 11] [14 14] [31 35]]
               (mapv (juxt :l0 :l1)
                     (q "SELECT l0, l1 FROM gnode WHERE paper = 'p1'
                         ORDER BY node_id"))))

    ;; --- P6a: Q9 node-join ---------------------------------------------------
    ;; n2 of p1 has span [14 14]. Closed-interval overlap must include the
    ;; TOUCHING mark [12 14] and the touching scope [12 14], plus the
    ;; exactly-covering mark [14 14] and the enclosing scope [10 20].
    (let [j (scope/node-join (ds) "mark-test-run" "p1" "n2")]
      (check! "Q9 node returned with parsed EDN"
              (= :n2 (get-in j [:node :node-data :id])))
      (check! "Q9 span" (= [14 14] (:span j)))
      (check! "Q9 marks exact, boundaries included"
              (= #{"let-binder" "definiendum"}
                 (set (map :kind (:marks j)))))
      (check! "Q9 mark-data parsed from payload"
              (= "binds X" (get-in (first (:marks j)) [:mark-data "tip"])))
      (check! "Q9 scopes exact, boundaries included"
              (= #{"connection" "universal-property/characterizes"}
                 (set (map :kind (:scopes j))))))
    (let [j (scope/node-join (ds) "mark-test-run" "p1" "n3")]
      (check! "Q9 n3 span [31 35] -> one scope, no marks"
              (and (= ["rationale/telos"] (mapv :kind (:scopes j)))
                   (empty? (:marks j)))))
    (check! "Q9 unknown node -> nil"
            (nil? (scope/node-join (ds) "mark-test-run" "p1" "n99")))

    ;; --- P6a bad case: a HALF-OPEN join drops touching rows ------------------
    ;; If the join were implemented half-open (l0 < b AND l1 > a), span
    ;; [14 14] would match NEITHER the touching mark [12 14] NOR the
    ;; exactly-covering [14 14] — the exact-membership checks above fail.
    (let [half-open (q "SELECT kind FROM mark
                        WHERE paper = 'p1' AND l0 < 14 AND l1 > 14")]
      (check! "bad case: half-open join FAILS the Q9 membership check"
              (not= #{"let-binder" "definiendum"}
                    (set (map :kind half-open)))))

    ;; --- watermark skip ----------------------------------------------------------
    (let [r (scope/index-run! (ds) (.getPath run-dir))]
      (check! "unchanged run skipped via watermark" (true? (:skipped r))))
    (let [before (q "SELECT count(*) AS n FROM scope")
          r (scope/index-run! (ds) (.getPath run-dir) {:force true})]
      (check! "forced re-index runs" (false? (:skipped r)))
      (check! "forced re-index idempotent"
              (= (:n (first before))
                 (:n (first (q "SELECT count(*) AS n FROM scope"))))))

    ;; --- edit one paper's file: exactly that paper's rows change ----------------
    (let [p2-before (q "SELECT * FROM scope WHERE paper = 'p2' ORDER BY scope_id")]
      (spit (io/file expo "p1_0002.edn")
            "{:paper/id \"p1\" :passage/id \"p1:pass-0002:L30-40\"
              :source {:lines [30 40] :kind :expository}
              :provenance {:generator \"expository-json/v1\" :model \"fixture\"}
              :scopes [{:id :s1 :kind :rationale/telos :source {:lines [31 35]}}
                       {:id :s2 :kind :open-problem/status
                        :source {:lines [36 38]}}]}")
      ;; dir mtime does not notice an in-place edit; set the file mtime ahead.
      (Files/setLastModifiedTime (.toPath (io/file expo "p1_0002.edn"))
                                 (java.nio.file.attribute.FileTime/from
                                  (Instant/parse "2030-01-01T00:00:00Z")))
      (let [r (scope/index-run! (ds) (.getPath run-dir))]
        (check! "edited run re-indexed (watermark noticed the edit)"
                (false? (:skipped r)))
        (check! "edit added exactly one row (p1: 3 -> 4)"
                (= 4 (:n (first (q "SELECT count(*) AS n FROM scope
                                    WHERE paper = 'p1'")))))
        (check! "new kind indexed" (= 1 (get (scope/kind-counts (ds))
                                             "open-problem/status")))
        (check! "p2 rows untouched by p1's re-index"
                (= (mapv #(select-keys % [:passage :scope_id :kind :l0 :l1])
                         p2-before)
                   (mapv #(select-keys % [:passage :scope_id :kind :l0 :l1])
                         (q "SELECT * FROM scope WHERE paper = 'p2'
                             ORDER BY scope_id"))))))

    ;; --- P6a: touching one paper's marks file changes exactly that paper ----
    (let [p1-all (fn [] {:scope (q "SELECT * FROM scope WHERE paper = 'p1'
                                    ORDER BY passage, scope_id")
                         :mark (q "SELECT * FROM mark WHERE paper = 'p1'
                                   ORDER BY mark_ix")
                         :gnode (q "SELECT * FROM gnode WHERE paper = 'p1'
                                    ORDER BY node_id")})
          p1-before (p1-all)
          p2-scope-before (q "SELECT * FROM scope WHERE paper = 'p2'
                              ORDER BY scope_id")
          p2-gnode-before (q "SELECT * FROM gnode WHERE paper = 'p2'")
          [d0 d1] (char-span p2-text 4 5)]
      (spit (io/file marks-dir "fable-p2-dp-emacs.json")
            (marks-json "p2" p2-text
                        (conj p2-marks
                              {"start" d0 "end" d1 "layer" "dp"
                               "kind" "concept" "tip" "added"})))
      (Files/setLastModifiedTime
       (.toPath (io/file marks-dir "fable-p2-dp-emacs.json"))
       (java.nio.file.attribute.FileTime/from
        ;; later than the 2030 mtime the p1_0002.edn edit above recorded
        (Instant/parse "2031-01-01T00:00:00Z")))
      (let [r (scope/index-run! (ds) (.getPath run-dir))]
        (check! "P6a marks-dir edit noticed by watermark" (false? (:skipped r)))
        (check! "P6a p2 marks 1 -> 2"
                (= 2 (:n (first (q "SELECT count(*) AS n FROM mark
                                     WHERE paper = 'p2'")))))
        (check! "P6a new mark's span derived"
                (= [4 5] ((juxt :l0 :l1)
                          (first (q "SELECT l0, l1 FROM mark
                                     WHERE paper = 'p2' AND kind = 'concept'")))))
        (check! "P6a p1 rows in ALL tables untouched by p2's re-index"
                (= p1-before (p1-all)))
        (check! "P6a p2 scope/gnode rows untouched by p2's marks-only edit"
                (and (= p2-scope-before
                        (q "SELECT * FROM scope WHERE paper = 'p2'
                            ORDER BY scope_id"))
                     (= p2-gnode-before
                        (q "SELECT * FROM gnode WHERE paper = 'p2'"))))))

    ;; --- removed paper's rows are dropped ---------------------------------------
    (.delete (io/file expo "p2_0001.edn"))
    (scope/index-run! (ds) (.getPath run-dir) {:force true})
    (check! "scope rows gone when a paper's last expo file is removed"
            (= 0 (:n (first (q "SELECT count(*) AS n FROM scope
                                  WHERE paper = 'p2'")))))
    (check! "mark/gnode rows stay while the paper still has marks/graphs files"
            (= [2 1] [(:n (first (q "SELECT count(*) AS n FROM mark
                                       WHERE paper = 'p2'")))
                      (:n (first (q "SELECT count(*) AS n FROM gnode
                                       WHERE paper = 'p2'")))]))
    (.delete (io/file marks-dir "fable-p2-dp-emacs.json"))
    (.delete (io/file graphs-dir "p2__p0.edn"))
    (scope/index-run! (ds) (.getPath run-dir) {:force true})
    (check! "paper with no remaining files anywhere is dropped from all tables"
            (every? zero?
                    (map (fn [t]
                           (:n (first (q (str "SELECT count(*) AS n FROM " t
                                              " WHERE paper = 'p2'")))))
                         ["scope" "mark" "gnode"])))

    (println "ALL PASS")
    (System/exit 0)))
