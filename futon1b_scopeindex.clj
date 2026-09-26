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

  No HTTP route in this packet (P6 or later); no server edit.\""
  (:require [clojure.edn :as edn]
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
      file_count INTEGER NOT NULL)"])

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
;; Watermark (I2): (run, dir-mtime, file-count) per run, where dir-mtime is
;; max(expo-dir mtime, newest file mtime) — see ns docstring.
;; ---------------------------------------------------------------------------

(defn- expo-dir-of [run-dir]
  (let [f (io/file run-dir)]
    (if (.isDirectory (io/file f "artifacts/expo"))
      (io/file f "artifacts/expo")
      f)))                       ; allow passing the expo dir directly

(defn- expo-files [expo-dir]
  ;; Top-level .edn files only: .attempts/ holds per-attempt retries of the
  ;; same passages (plus .response.json), not the run's finalized reading.
  (->> (.listFiles expo-dir)
       (filter #(.isFile ^java.io.File %))
       (filter #(str/ends-with? (.getName ^java.io.File %) ".edn"))
       (sort-by #(.getName ^java.io.File %))
       vec))

(defn- watermark [expo-dir files]
  {:dir-mtime (reduce max (.lastModified expo-dir)
                      (map #(.lastModified ^java.io.File %) files))
   :file-count (count files)})

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

(defn index-run!
  "Index every expo file of the run at run-dir (a directory containing
   artifacts/expo/, or the expo dir itself) into the scope table. Replaces
   each paper's rows in ONE SQLite transaction; drops rows of papers no
   longer present in the run; records the per-run watermark. Skips the run
   when the watermark is unchanged unless {:force true}."
  ([run-dir] (index-run! (ds*) run-dir nil))
  ([ds run-dir] (index-run! ds run-dir nil))
  ([ds run-dir {:keys [force]}]
   (let [t0 (System/currentTimeMillis)
         expo-dir (expo-dir-of run-dir)
         run (if (= expo-dir (io/file run-dir))
               (.getName (.getParentFile expo-dir)) ; passed the expo dir
               (.getName (io/file run-dir)))
         files (expo-files expo-dir)
         wm (watermark expo-dir files)
         stored (stored-watermark ds run)]
     (if (and (not force)
              (= (:dir-mtime wm) (:dir_mtime stored))
              (= (:file-count wm) (:file_count stored)))
       (let [r {:run run :skipped true :files (count files) :rows 0
                :elapsed-ms (- (System/currentTimeMillis) t0)}]
         (swap! !stats assoc :last-index-run r)
         r)
       (let [by-file (into {}
                           (map (fn [f] [(.getName ^java.io.File f)
                                         (parse-expo-file f)]))
                           files)
             by-paper (group-by (comp :paper first val) by-file)
             present (set (keys by-paper))
             rows (reduce + (map (comp count val) by-file))]
         ;; Drop rows of papers no longer present in the run.
         (with-write-tx [tx ds]
           (doseq [p (map :paper
                          (jdbc/execute! tx ["SELECT DISTINCT paper FROM scope
                                              WHERE run = ?" run] unqualified))
                   :when (not (present p))]
             (jdbc/execute! tx ["DELETE FROM scope WHERE run = ? AND paper = ?"
                                run p])))
         ;; Replace each paper in one transaction.
         (doseq [[paper file-rows] (group-by (fn [[_ rows]] (:paper (first rows)))
                                             by-file)]
           (with-write-tx [tx ds]
             (jdbc/execute! tx ["DELETE FROM scope WHERE run = ? AND paper = ?"
                                run paper])
             (doseq [[file srows] file-rows
                     s srows]
               (jdbc/execute! tx ["INSERT INTO scope(run, paper, passage,
                                     scope_id, kind, l0, l1, generator,
                                     model, file)
                                   VALUES (?,?,?,?,?,?,?,?,?,?)"
                                  run (:paper s) (:passage s) (:scope-id s)
                                  (:kind s) (:l0 s) (:l1 s)
                                  (:generator s) (:model s) file]))))
         (write-locked* (fn [] (record-watermark! ds run wm)))
         (let [r {:run run :skipped false :files (count files) :rows rows
                  :papers (count by-paper)
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
