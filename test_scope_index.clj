(ns test-scope-index
  "Throwaway-sidecar tests for the P5 scope index (futon1b-scopeindex):
   per-kind counts vs an independent count (with `/`-kinds present and the
   passage's :source :kind NOT counted), per-paper transactional re-index,
   watermark skip + forced idempotence, Q7 closed-interval boundaries, and
   the prototype's bad case (a parser that drops `/` kinds fails the
   per-kind check).

  Run: clojure -M:node -m test-scope-index"
  (:require [clojure.java.io :as io]
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

(defn- write-fixture! [dir]
  (let [expo (io/file dir "artifacts/expo")]
    (.mkdirs expo)
    (doseq [[name body] fixture-files]
      (spit (io/file expo name) body))
    expo))

(defn- q [sql & params]
  (jdbc/execute! (ds) (into [sql] params) unqualified))

(defn -main []
  (let [dir (temp-dir)
        run-dir (io/file dir "mark-test-run")
        expo (write-fixture! run-dir)]
    (text/init! {:path (str dir "/throwaway.db")})
    (scope/init!)

    (println "== P5 scope sidecar tests ==")

    ;; --- index the fixture run -------------------------------------------
    (let [r (scope/index-run! (ds) (.getPath run-dir))]
      (check! "index-run! files" (= 3 (:files r)))
      (check! "index-run! rows" (= 6 (:rows r)))
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

    ;; --- removed paper's rows are dropped ---------------------------------------
    (.delete (io/file expo "p2_0001.edn"))
    (scope/index-run! (ds) (.getPath run-dir) {:force true})
    (check! "paper with no remaining files is dropped"
            (= 0 (:n (first (q "SELECT count(*) AS n FROM scope
                                  WHERE paper = 'p2'")))))

    (println "ALL PASS")
    (System/exit 0)))
