(ns futon1b-scopeindex
  "P5 scope sidecar index (DESIGN-hyperedge-scope-sidecar-2026-09-26 §2–§4,
  packet §7 item 5). Indexes a superpod run directory's
  artifacts/expo/*.edn files into a `scope` table in the SAME SQLite file
  family as futon1b-text / futon1b-hxindex (shared datasource).

  Unlike the hx_* tables this index is FILE-derived, not XTDB-derived: the
  run directory is the source of truth and the rebuild oracle. Scopes are
  deliberately NOT ingested into XTDB (design §2, option (b)).

  Maintenance contract (design §4):
  - I1 re-check for a scope row is re-parsing the cited file, so every row
    carries its source file name (the one column added to design §3's DDL,
    for exactly this reason).
  - I2 watermark is per run: (run, dir-mtime, file-count) in scope_meta.
    dir-mtime is strengthened to max(expo-dir mtime, newest file mtime):
    a plain directory mtime does not change when a file is edited in
    place, and the P5 acceptance requires an edit to one paper's file to
    be noticed.
  - Re-run replacement of one paper is DELETE WHERE run=? AND paper=? +
    inserts in ONE SQLite transaction, so readers never see a
    half-replaced paper (design §4).

  Write serialization: an equivalent of futon1b-hxindex's P3e write lock +
  bounded SQLITE_BUSY retry (the two namespaces share the file but not the
  lock object; the retry absorbs cross-namespace and cross-process writers
  the same way hxindex's retry absorbs futon1b-text's FTS writers).

  Overlap convention for Q7 (scopes-overlapping): CLOSED intervals,
  l0 <= :b AND l1 >= :a — touching intervals count as overlapping. This
  matches render_scope_margin.py's ink-plate semantics, where a scope
  paints every line in [l0,l1] and any shared line mixes the ink.

  No HTTP route in this packet (P6 or later); no server edit.

  P6a adds the `mark` and `gnode` tables (design §7 item 6) for Q9:
  - mark: S1 marks from artifacts/marks/*.json. The fable paper id carries
    a trailing \"-dp\" layer tag (\"0708.1921-dp\"); it is stripped to join
    to :paper/id. Mark spans are CHARACTER offsets into the file's text;
    the table stores them verbatim (c0/c1, the I1 file coordinate) and
    derives the 1-based CLOSED line span [l0 l1] from the text's newline
    positions. payload is the mark re-serialized as canonical JSON.
  - gnode: proof-graph nodes from artifacts/graphs/<paper>__pN.edn. The
    base .edn files are canonical: .rung2.edn files are semcheck REPORTS
    (schema :futon6.iatc-semcheck.v1) that reference the base file and
    carry no node spans of their own, so they are not indexed. Nodes DO
    carry :source :lines, so no derivation is needed.
  - index-run! walks all three dirs; the watermark covers all three; the
    per-paper one-transaction replace covers scope+mark+gnode together.
  - node-join answers Q9: node -> span -> marks + scopes overlapping the
    span on the same CLOSED-interval convention as Q7.

  No HTTP route in P6a either; no server edit."
  (:require [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [futon1b-text :as text]))

(def ^:private unqualified {:builder-fn rs/as-unqualified-maps})

(defonce !stats (atom {:last-index-run nil}))

;; ---------------------------------------------------------------------------
;; Write lock + bounded SQLITE_BUSY retry (equivalent of hxindex P3e; see ns
;; docstring for why it is a separate lock object).
;; ---------------------------------------------------------------------------

(defonce ^:private !write-lock (Object.))

(def ^:dynamic *busy-retry*
  {:attempts 6 :base-sleep-ms 250})

(defn- busy-error? [t]
  (and (instance? java.sql.SQLException t)
       (let [e ^java.sql.SQLException t
             code (.getErrorCode e)
             msg (str (.getMessage e))]
         (or (= 5 code) (= 517 code)
             (str/includes? msg "SQLITE_BUSY")
             (str/includes? msg "database is locked")))))

(defn- with-busy-retry* [f]
  (let [{:keys [attempts base-sleep-ms]
         :or {attempts 6 base-sleep-ms 250}} *busy-retry*]
    (loop [n 1 sleep-ms (long base-sleep-ms)]
      (let [r (try {:ok (f)} (catch Throwable t {:err t}))]
        (if-let [t (:err r)]
          (if (and (busy-error? t) (< n (long attempts)))
            (do (Thread/sleep sleep-ms)
                (recur (inc n) (min 8000 (* 2 sleep-ms))))
            (throw t))
          (:ok r))))))

(defn- write-locked* [f]
  (with-busy-retry* (fn [] (locking !write-lock (f)))))

(defmacro ^:private ^{:clj-kondo/lint-as 'clojure.core/with-open} with-write-tx
  [[tx ds] & body]
  `(write-locked*
    (fn [] (jdbc/with-transaction [~tx ~ds] ~@body))))

;; ---------------------------------------------------------------------------
;; Schema (design §3, plus `file` for the I1 re-parse, plus scope_meta).
;; ---------------------------------------------------------------------------

(def ^:private ddl
  ["CREATE TABLE IF NOT EXISTS scope (
      run       TEXT NOT NULL,
      paper     TEXT NOT NULL,
      passage   TEXT NOT NULL,
      scope_id  TEXT NOT NULL,
      kind      TEXT NOT NULL,
      l0 INTEGER NOT NULL, l1 INTEGER NOT NULL,
      generator TEXT, model TEXT,
      file TEXT NOT NULL)"            ;; I1: the file to re-parse
   "CREATE INDEX IF NOT EXISTS scope_paper_lines ON scope(paper, l0, l1)"
   "CREATE INDEX IF NOT EXISTS scope_kind        ON scope(kind, paper)"
   "CREATE INDEX IF NOT EXISTS scope_run_paper   ON scope(run, paper)"
   "CREATE TABLE IF NOT EXISTS scope_meta (
      run TEXT PRIMARY KEY,
      dir_mtime INTEGER NOT NULL,
      file_count INTEGER NOT NULL)"
   ;; P6a: S1 marks. c0/c1 are the verbatim char offsets (I1 file
   ;; coordinate); l0/l1 are the derived 1-based CLOSED line span.
   "CREATE TABLE IF NOT EXISTS mark (
      run     TEXT NOT NULL,
      paper   TEXT NOT NULL,
      mark_ix INTEGER NOT NULL,
      kind    TEXT NOT NULL,
      layer   TEXT,
      l0 INTEGER NOT NULL, l1 INTEGER NOT NULL,
      c0 INTEGER NOT NULL, c1 INTEGER NOT NULL,
      payload TEXT NOT NULL,
      file TEXT NOT NULL)"
   "CREATE INDEX IF NOT EXISTS mark_paper_lines ON mark(paper, l0, l1)"
   "CREATE INDEX IF NOT EXISTS mark_run_paper   ON mark(run, paper)"
   ;; P6a: proof-graph nodes (canonical base .edn files only, never
   ;; .rung2.edn semcheck reports). payload is the node's EDN verbatim.
   "CREATE TABLE IF NOT EXISTS gnode (
      run     TEXT NOT NULL,
      paper   TEXT NOT NULL,
      passage TEXT NOT NULL,
      node_id TEXT NOT NULL,
      kind    TEXT,
      l0 INTEGER NOT NULL, l1 INTEGER NOT NULL,
      payload TEXT NOT NULL,
      file TEXT NOT NULL)"
   "CREATE INDEX IF NOT EXISTS gnode_paper_lines    ON gnode(paper, l0, l1)"
   "CREATE INDEX IF NOT EXISTS gnode_run_paper_node ON gnode(run, paper, node_id)"])

(defn- ds* [] @text/!ds)

(defn init!
  "Create the scope tables on the shared sidecar datasource. Idempotent.
   Requires futon1b-text/init! to have run (it owns the file), unless a
   datasource is passed explicitly."
  ([] (init! {:ds (ds*)}))
  ([{:keys [ds]}]
   (when-not ds
     (throw (ex-info "scopeindex: no sidecar datasource (text/init! first)" {})))
   (doseq [stmt ddl] (jdbc/execute! ds [stmt]))
   {:ok true}))

;; ---------------------------------------------------------------------------
;; Parsing. EDN reader, NOT regex: the prototype's regex dropped every kind
;; containing `/` (772 rows instead of 857) — kinds such as
;; :universal-property/characterizes. Kind/scope-id are stored as their full
;; keyword text without the leading colon (subs 1, not `name`, which would
;; truncate at the `/`). The passage's own :source :kind (:expository) is
;; NOT a scope and is never read here.
;; ---------------------------------------------------------------------------

(defn- kw->str [k]
  (when (keyword? k)
    (subs (str k) 1)))

(defn parse-expo-file
  "Parse one expo EDN file into scope rows (maps with :paper :passage
   :scope-id :kind :l0 :l1 :generator :model). Throws on a malformed file —
   the acceptance per-kind counts must match the files exactly, so a
   silently skipped scope is a bug, not robustness."
  [file]
  (let [doc (edn/read-string (slurp file))
        paper (:paper/id doc)
        passage (:passage/id doc)
        {:keys [generator model]} (:provenance doc)
        scopes (:scopes doc)]
    (when-not (and (string? paper) (string? passage) (sequential? scopes))
      (throw (ex-info (str "scopeindex: malformed expo file " file)
                      {:file (str file)})))
    (mapv (fn [s]
            (let [kind (kw->str (:kind s))
                  [l0 l1] (get-in s [:source :lines])]
              (when-not (and kind (integer? l0) (integer? l1))
                (throw (ex-info (str "scopeindex: scope without kind/lines in "
                                     file)
                                {:file (str file) :scope s})))
              {:paper paper
               :passage passage
               :scope-id (kw->str (:id s))
               :kind kind
               :l0 (long l0) :l1 (long l1)
               :generator generator :model model}))
          scopes)))

;; ---------------------------------------------------------------------------
;; P6a parsing: marks (JSON) and graph nodes (EDN).
;; ---------------------------------------------------------------------------

(defn- newline-index
  "Sorted long-array of the text's newline offsets, for char-offset ->
   1-based line conversion."
  [^String text]
  (long-array (keep-indexed (fn [i c] (when (= c \newline) (long i))) text)))

(defn- line-of
  "1-based line of char offset pos: (newlines strictly before pos) + 1."
  [^longs nl ^long pos]
  (let [i (java.util.Arrays/binarySearch nl pos)]
    (long (inc (if (neg? i) (dec (- i)) i)))))

(defn parse-marks-file
  "Parse one artifacts/marks/*.json file into mark rows. Paper id: the
   fable 'paper' field carries a trailing \"-dp\" layer tag which is
   stripped to join to expo/graph :paper/id. Line spans are derived from
   the file's own text (closed interval; l1 is the line of the last
   marked char). Throws on malformed entries, same contract as
   parse-expo-file."
  [file]
  (let [doc (json/parse-string (slurp file))
        raw-paper (get doc "paper")
        text ^String (get doc "text")
        marks (get doc "marks")]
    (when-not (and (string? raw-paper) (string? text) (sequential? marks))
      (throw (ex-info (str "scopeindex: malformed marks file " file)
                      {:file (str file)})))
    (let [paper (if (str/ends-with? raw-paper "-dp")
                  (subs raw-paper 0 (- (count raw-paper) 3))
                  raw-paper)
          nl (newline-index text)
          n (count text)]
      (mapv (fn [ix m]
              (let [c0 (long (get m "start"))
                    c1 (long (get m "end"))
                    kind (get m "kind")]
                (when-not (and (string? kind)
                               (<= 0 c0) (<= c0 c1) (<= c1 n))
                  (throw (ex-info (str "scopeindex: mark without kind or with "
                                       "bad offsets in " file)
                                  {:file (str file) :mark m})))
                {:paper paper
                 :mark-ix (long ix)
                 :kind kind
                 :layer (get m "layer")
                 :l0 (line-of nl c0)
                 :l1 (line-of nl (max c0 (dec c1)))
                 :c0 c0 :c1 c1
                 :payload (json/generate-string m)}))
            (range) marks))))

(defn parse-graph-file
  "Parse one artifacts/graphs/<paper>__pN.edn base graph file into node
   rows. Nodes carry :source :lines, stored verbatim; payload is the
   node's EDN verbatim. Throws on a node without id/lines."
  [file]
  (let [doc (edn/read-string (slurp file))
        paper (:paper/id doc)
        passage (:passage/id doc)
        nodes (:nodes doc)]
    (when-not (and (string? paper) (string? passage) (sequential? nodes))
      (throw (ex-info (str "scopeindex: malformed graph file " file)
                      {:file (str file)})))
    (mapv (fn [nd]
            (let [node-id (kw->str (:id nd))
                  [l0 l1] (get-in nd [:source :lines])]
              (when-not (and node-id (integer? l0) (integer? l1))
                (throw (ex-info (str "scopeindex: graph node without id/lines "
                                     "in " file)
                                {:file (str file) :node nd})))
              {:paper paper
               :passage passage
               :node-id node-id
               :kind (kw->str (:kind nd))
               :l0 (long l0) :l1 (long l1)
               :payload (pr-str nd)}))
          nodes)))

;; ---------------------------------------------------------------------------
;; Watermark (I2): (run, dir-mtime, file-count) per run, where dir-mtime is
;; max(dir mtimes, newest file mtime) — see ns docstring. P6a: the
;; watermark covers all three artifact dirs (expo, marks, graphs).
;; ---------------------------------------------------------------------------

(defn- artifacts-dir-of
  "Resolve the artifacts dir: run-dir/artifacts if present, run-dir itself
   if it directly holds expo/, else run-dir's parent (a leaf such as
   artifacts/expo was passed). The run name is the artifacts dir's parent
   name (or run-dir's own name when artifacts/ was found inside it)."
  [run-dir]
  (let [f (io/file run-dir)]
    (cond
      (.isDirectory (io/file f "artifacts")) [(io/file f "artifacts")
                                              (.getName f)]
      (.isDirectory (io/file f "expo"))      [f (.getName (.getParentFile f))]
      :else [(.getParentFile f)
             (.getName (.getParentFile (.getParentFile f)))])))

(defn- subdir-files
  "Top-level files of artifacts/<sub> matching pred, sorted by name. An
   absent subdir is an empty set, not an error: a run may legitimately
   have expo but (yet) no marks or graphs. Top-level only: .attempts/
   holds per-attempt retries (plus .response.json), not the run's
   finalized reading."
  [artifacts-dir sub pred]
  (let [d (io/file artifacts-dir sub)]
    (if-not (.isDirectory d)
      {:dir nil :files []}
      {:dir d
       :files (->> (.listFiles d)
                   (filter #(.isFile ^java.io.File %))
                   (filter #(pred (.getName ^java.io.File %)))
                   (sort-by #(.getName ^java.io.File %))
                   vec)})))

(defn- expo-files [artifacts-dir]
  (subdir-files artifacts-dir "expo" #(str/ends-with? % ".edn")))

(defn- marks-files [artifacts-dir]
  (subdir-files artifacts-dir "marks" #(str/ends-with? % ".json")))

(defn- graph-files [artifacts-dir]
  ;; Base <paper>__pN.edn only: .rung2.edn files are semcheck reports
  ;; referencing the base file, not graphs (see ns docstring).
  (subdir-files artifacts-dir "graphs"
                #(and (str/ends-with? % ".edn")
                      (not (str/includes? % ".rung2")))))

(defn- watermark [dir-entries]
  (let [dirs (keep :dir dir-entries)
        files (mapcat :files dir-entries)]
    {:dir-mtime (reduce max 0
                        (concat (map #(.lastModified ^java.io.File %) dirs)
                                (map #(.lastModified ^java.io.File %) files)))
     :file-count (count files)}))

(defn- stored-watermark [ds run]
  (jdbc/execute-one! ds ["SELECT dir_mtime, file_count FROM scope_meta
                          WHERE run = ?" run] unqualified))

(defn- record-watermark! [ds run wm]
  (jdbc/execute! ds ["INSERT INTO scope_meta(run, dir_mtime, file_count)
                      VALUES (?,?,?)
                      ON CONFLICT(run) DO UPDATE SET
                        dir_mtime=excluded.dir_mtime,
                        file_count=excluded.file_count"
                     run (:dir-mtime wm) (:file-count wm)]))

;; ---------------------------------------------------------------------------
;; index-run!
;; ---------------------------------------------------------------------------

(defn- insert-scope-rows! [tx run rows]
  (doseq [[file srows] rows
          s srows]
    (jdbc/execute! tx ["INSERT INTO scope(run, paper, passage, scope_id,
                          kind, l0, l1, generator, model, file)
                        VALUES (?,?,?,?,?,?,?,?,?,?)"
                       run (:paper s) (:passage s) (:scope-id s)
                       (:kind s) (:l0 s) (:l1 s)
                       (:generator s) (:model s) file])))

(defn- insert-mark-rows! [tx run rows]
  (doseq [[file mrows] rows
          m mrows]
    (jdbc/execute! tx ["INSERT INTO mark(run, paper, mark_ix, kind, layer,
                          l0, l1, c0, c1, payload, file)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?)"
                       run (:paper m) (:mark-ix m) (:kind m) (:layer m)
                       (:l0 m) (:l1 m) (:c0 m) (:c1 m)
                       (:payload m) file])))

(defn- insert-gnode-rows! [tx run rows]
  (doseq [[file nrows] rows
          nd nrows]
    (jdbc/execute! tx ["INSERT INTO gnode(run, paper, passage, node_id, kind,
                          l0, l1, payload, file)
                        VALUES (?,?,?,?,?,?,?,?,?)"
                       run (:paper nd) (:passage nd) (:node-id nd)
                       (:kind nd) (:l0 nd) (:l1 nd)
                       (:payload nd) file])))

(defn index-run!
  "Index the run at run-dir (a directory containing artifacts/, the
   artifacts dir itself, or one of its leaves): expo files into scope,
   marks JSON into mark, base graph files into gnode. Each paper's rows
   in ALL THREE tables are replaced in ONE SQLite transaction, so readers
   never see a half-replaced paper; papers no longer present anywhere in
   the run are dropped from all three tables. Records the per-run
   watermark (covering all three dirs); skips when unchanged unless
   {:force true}."
  ([run-dir] (index-run! (ds*) run-dir nil))
  ([ds run-dir] (index-run! ds run-dir nil))
  ([ds run-dir {:keys [force]}]
   (let [t0 (System/currentTimeMillis)
         [artifacts-dir run] (artifacts-dir-of run-dir)
         expo (expo-files artifacts-dir)
         marks (marks-files artifacts-dir)
         graphs (graph-files artifacts-dir)
         entries [expo marks graphs]
         wm (watermark entries)
         stored (stored-watermark ds run)
         nfiles (reduce + (map (comp count :files) entries))]
     (if (and (not force)
              (= (:dir-mtime wm) (:dir_mtime stored))
              (= (:file-count wm) (:file_count stored)))
       (let [r {:run run :skipped true :files nfiles :rows 0
                :elapsed-ms (- (System/currentTimeMillis) t0)}]
         (swap! !stats assoc :last-index-run r)
         r)
       (let [parse (fn [parser files]
                     (group-by (comp :paper first val)
                               (into {}
                                     (map (fn [f] [(.getName ^java.io.File f)
                                                   (parser f)]))
                                     files)))
             scopes-by-paper (parse parse-expo-file (:files expo))
             marks-by-paper (parse parse-marks-file (:files marks))
             gnodes-by-paper (parse parse-graph-file (:files graphs))
             present (into (set (keys scopes-by-paper))
                           (concat (keys marks-by-paper)
                                   (keys gnodes-by-paper)))
             count-rows (fn [m] (reduce + (map (comp count val)
                                               (apply concat (vals m)))))
             scope-rows (count-rows scopes-by-paper)
             mark-rows (count-rows marks-by-paper)
             gnode-rows (count-rows gnodes-by-paper)]
         ;; Drop rows of papers no longer present anywhere in the run.
         (with-write-tx [tx ds]
           (doseq [table ["scope" "mark" "gnode"]
                   p (map :paper
                          (jdbc/execute! tx
                                         [(str "SELECT DISTINCT paper FROM "
                                               table " WHERE run = ?") run]
                                         unqualified))
                   :when (not (present p))]
             (jdbc/execute! tx [(str "DELETE FROM " table
                                     " WHERE run = ? AND paper = ?")
                                run p])))
         ;; Replace each paper in one transaction covering all three tables.
         (doseq [paper present]
           (with-write-tx [tx ds]
             (doseq [table ["scope" "mark" "gnode"]]
               (jdbc/execute! tx [(str "DELETE FROM " table
                                       " WHERE run = ? AND paper = ?")
                                  run paper]))
             (insert-scope-rows! tx run (get scopes-by-paper paper))
             (insert-mark-rows! tx run (get marks-by-paper paper))
             (insert-gnode-rows! tx run (get gnodes-by-paper paper))))
         (write-locked* (fn [] (record-watermark! ds run wm)))
         (let [r {:run run :skipped false :files nfiles
                  :rows scope-rows
                  :mark-rows mark-rows
                  :gnode-rows gnode-rows
                  :papers (count present)
                  :elapsed-ms (- (System/currentTimeMillis) t0)}]
           (swap! !stats assoc :last-index-run r)
           r))))))

;; ---------------------------------------------------------------------------
;; Reads (Q6–Q8). Rows carry their file coordinates for the I1 re-parse.
;; ---------------------------------------------------------------------------

(defn scopes-of-paper
  "Q6: all scopes of one paper (any run unless :run given), by line."
  ([ds paper] (scopes-of-paper ds paper nil))
  ([ds paper {:keys [run]}]
   (jdbc/execute! ds (into [(str "SELECT run, paper, passage, scope_id, kind,
                                   l0, l1, generator, model, file
                                  FROM scope WHERE paper = ?"
                                 (when run " AND run = ?")
                                 " ORDER BY l0, l1, scope_id")
                            paper]
                           (when run [run]))
                  unqualified)))

(defn scopes-overlapping
  "Q7: scopes of one paper overlapping the CLOSED line interval [a b] —
   l0 <= b AND l1 >= a; touching intervals count (see ns docstring)."
  ([ds paper a b] (scopes-overlapping ds paper a b nil))
  ([ds paper a b {:keys [run]}]
   (jdbc/execute! ds (into [(str "SELECT run, paper, passage, scope_id, kind,
                                   l0, l1, generator, model, file
                                  FROM scope
                                  WHERE paper = ? AND l0 <= ? AND l1 >= ?"
                                 (when run " AND run = ?")
                                 " ORDER BY l0, l1, scope_id")
                            paper (long b) (long a)]
                           (when run [run]))
                  unqualified)))

(defn scopes-of-kind
  "Q8: all scopes of one kind, across papers and runs."
  ([ds kind] (scopes-of-kind ds kind nil))
  ([ds kind {:keys [run]}]
   (jdbc/execute! ds (into [(str "SELECT run, paper, passage, scope_id, kind,
                                   l0, l1, generator, model, file
                                  FROM scope WHERE kind = ?"
                                 (when run " AND run = ?")
                                 " ORDER BY paper, l0, l1")
                            kind]
                           (when run [run]))
                  unqualified)))

(defn kind-counts
  "Per-kind scope counts, optionally restricted to one run. The acceptance
   check compares this against an independent count of the source files."
  ([ds] (kind-counts ds nil))
  ([ds {:keys [run]}]
   (into {}
         (map (juxt :kind :n))
         (jdbc/execute! ds (into [(str "SELECT kind, count(*) AS n FROM scope"
                                       (when run " WHERE run = ?")
                                       " GROUP BY kind ORDER BY n DESC")]
                                 (when run [run]))
                        unqualified))))

;; ---------------------------------------------------------------------------
;; P6a reads: marks and the Q9 node join.
;; ---------------------------------------------------------------------------

(defn marks-overlapping
  "Marks of one paper overlapping the CLOSED line interval [a b], same
   convention as scopes-overlapping. Rows carry c0/c1 (the verbatim char
   offsets) and payload (the mark's JSON) for the I1 re-check."
  ([ds paper a b] (marks-overlapping ds paper a b nil))
  ([ds paper a b {:keys [run]}]
   (jdbc/execute! ds (into [(str "SELECT run, paper, mark_ix, kind, layer,
                                    l0, l1, c0, c1, payload, file
                                  FROM mark
                                  WHERE paper = ? AND l0 <= ? AND l1 >= ?"
                                 (when run " AND run = ?")
                                 " ORDER BY l0, l1, mark_ix")
                            paper (long b) (long a)]
                           (when run [run]))
                  unqualified)))

(defn node-join
  "Q9: graph node -> its line span -> S1 marks and S4 scopes overlapping
   that span (closed intervals, same convention as Q7). Returns
   {:node <row with :node-data parsed EDN> :span [l0 l1]
    :marks [...] :scopes [...]}, or nil when (run, paper, node-id) is not
   indexed. Mark rows carry :mark-data (parsed payload JSON). Node ids
   are unique only within a passage: when the paper has several passages
   with the same node-id, pass {:passage p} to disambiguate (without it
   the ambiguity throws rather than answering for an arbitrary node)."
  ([ds run paper node-id] (node-join ds run paper node-id nil))
  ([ds run paper node-id {:keys [passage]}]
   (when-let [node (let [rows (jdbc/execute! ds (into [(str "SELECT run, paper,
                                                               passage, node_id,
                                                               kind, l0, l1,
                                                               payload, file
                                                          FROM gnode
                                                          WHERE run = ?
                                                            AND paper = ?
                                                            AND node_id = ?"
                                                         (when passage
                                                           " AND passage = ?"))
                                                       run paper node-id]
                                                      (when passage [passage]))
                                             unqualified)]
                     (cond
                       (empty? rows) nil
                       (= 1 (count rows)) (first rows)
                       :else (throw (ex-info (str "scopeindex: node-id " node-id
                                                  " is ambiguous in " paper
                                                  "; pass {:passage p}")
                                             {:passages (mapv :passage rows)}))))]
     (let [a (:l0 node) b (:l1 node)
           marks (marks-overlapping ds paper a b {:run run})
           scopes (scopes-overlapping ds paper a b {:run run})]
       {:node (assoc node :node-data (edn/read-string (:payload node)))
        :span [a b]
        :marks (mapv #(assoc % :mark-data (json/parse-string (:payload %)))
                     marks)
        :scopes scopes}))))
