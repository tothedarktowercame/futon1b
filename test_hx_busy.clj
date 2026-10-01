(ns test-hx-busy
  "P3e reproduction: sidecar write hooks vs SQLITE_BUSY (throwaway db, no XTDB
   node needed — the hooks touch only the sidecar datasource).

   Mechanism evidence:
   - T1 (cause a): a pure-writer transaction contending with a writer that
     holds the lock past busy_timeout fails with plain [SQLITE_BUSY] only
     AFTER the timeout — exactly the live error recorded in hx-stats.
   - T2 (cause b, control): a deferred WAL transaction that READS then writes
     after another writer committed fails IMMEDIATELY (busy_timeout does not
     wait on a snapshot upgrade) — a different failure shape, and unreachable
     by the sidecar's write transactions (none reads inside its tx).
   Fix behavior:
   - T3: a hook fired while an EXTERNAL writer (raw jdbc — models the FTS
     writer or an operator's script, which don't take the hx lock) holds the
     lock past busy_timeout still lands its rows and is NOT counted as a
     failure (the retry absorbs it). FAILS against the pre-fix hxindex.
   - T4: a hook whose busy outlasts the retry IS counted (P2 gate intact).

   Run: clojure -M:node -m test-hx-busy
   Bad case: git show c9ede34:futon1b_hxindex.clj > badcase/futon1b_hxindex.clj
             clojure -Sdeps '{:paths [\"badcase\" \".\"]}' -M:node -m test-hx-busy"
  (:require [clojure.string :as str]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [futon1b-text :as text]
            [futon1b-hxindex :as hx])
  (:import [java.nio.file Files]
           [java.util.concurrent CountDownLatch TimeUnit]))

(def ^:private unqualified {:builder-fn rs/as-unqualified-maps})

(defn- temp-dir []
  (-> (Files/createTempDirectory
       "futon1b-hx-busy-test-"
       (make-array java.nio.file.attribute.FileAttribute 0))
      .toFile
      .getAbsolutePath))

(defn- check! [label value]
  (println (format "  %-66s %s" label (if value "PASS" "FAIL")))
  (assert value label))

(defn- wait-for [timeout-ms pred]
  (let [deadline (+ (System/currentTimeMillis) (long timeout-ms))]
    (loop []
      (cond (pred) true
            (> (System/currentTimeMillis) deadline) false
            :else (do (Thread/sleep 50) (recur))))))

(def ^:private !ds-atom (var-get (ns-resolve 'futon1b-text '!ds)))
(defn- ds [] @!ds-atom)

(defn- rows-for [id]
  (->> (jdbc/execute! (ds) ["SELECT pos, endpoint FROM hx_edge
                             WHERE hx_id = ? ORDER BY pos" id]
                      unqualified)
       (mapv (juxt :pos :endpoint))))

(defn- hx-doc [id type endpoints]
  {:xt/id id :hx/id id :hx/type type :hx/endpoints (vec endpoints)})

(defn- hold-write-lock!
  "Start a thread that takes the db write lock (raw jdbc — deliberately NOT
   through any hxindex fn, modeling an external writer) and holds it for
   HOLD-MS before committing. Returns a promise delivered once the lock is
   held. JOIN the returned thread before starting another writer."
  [wds hold-ms]
  (let [held (promise)
        t (doto (Thread.
                 ^Runnable
                 (fn []
                   (jdbc/with-transaction [tx wds]
                     (jdbc/execute! tx ["INSERT INTO busy_probe(k, v) VALUES ('w', '1')
                                         ON CONFLICT(k) DO UPDATE SET v=excluded.v"])
                     (deliver held true)
                     (Thread/sleep (long hold-ms)))))
            (.setDaemon true)
            (.start))]
    @held
    t))

(defn -main [& _]
  (let [dir (temp-dir)
        file (str dir "/fts5-evidence.db")]
    (text/init! {:path file})
    ;; Rebind the shared datasource with a SHORT busy_timeout so the test
    ;; doesn't spend 10 s per contention; the same spec text/init! uses
    ;; otherwise. Hooks read the atom per call, so this is what they get.
    (reset! !ds-atom
            (jdbc/get-datasource {:dbtype "sqlite" :dbname file
                                  :busy_timeout 500}))
    (hx/init!)
    (jdbc/execute! (ds) ["CREATE TABLE IF NOT EXISTS busy_probe(
                           k TEXT PRIMARY KEY, v TEXT)"])
    (check! "sidecar is WAL with the short busy_timeout for the test"
            (and (= "wal" (:journal_mode (first (jdbc/execute! (ds) ["PRAGMA journal_mode"]
                                                               unqualified))))
                 (= 500 (:timeout (first (jdbc/execute! (ds) ["PRAGMA busy_timeout"]
                                                         unqualified))))))

    ;; ---- T1 (cause a): pure writer vs long-held write lock ----------------
    (println "T1: pure-writer tx vs a writer holding the lock past busy_timeout")
    (let [holder (hold-write-lock! (ds) 2500)
          t0 (System/currentTimeMillis)
          err (try
                (jdbc/with-transaction [tx (ds)]
                  (jdbc/execute! tx ["INSERT INTO busy_probe(k, v) VALUES ('t1', '1')"]))
                nil
                (catch java.sql.SQLException e e))
          elapsed (- (System/currentTimeMillis) t0)]
      (println (str "    error after " elapsed " ms: "
                    (some-> err .getMessage)))
      (check! "T1: contended pure writer fails with [SQLITE_BUSY]"
              (and err (str/includes? (str (.getMessage err)) "SQLITE_BUSY")))
      (check! "T1: but only after busy_timeout expired (>= 400 ms, not instant)"
              (>= elapsed 400))
      (.join ^Thread holder 10000))

    ;; ---- T2 (cause b, control): deferred read→write upgrade ---------------
    (println "T2: deferred WAL tx that reads then writes (snapshot upgrade)")
    (with-open [c1 (jdbc/get-connection (ds))]
      (.setAutoCommit c1 false)
      (jdbc/execute! c1 ["SELECT count(*) AS n FROM busy_probe"] unqualified)
      ;; another writer commits, invalidating c1's read snapshot
      (jdbc/execute! (ds) ["INSERT INTO busy_probe(k, v) VALUES ('t2', '1')"])
      (let [t0 (System/currentTimeMillis)
            err (try
                  (jdbc/execute! c1 ["INSERT INTO busy_probe(k, v) VALUES ('t2b', '1')"])
                  nil
                  (catch java.sql.SQLException e e))
            elapsed (- (System/currentTimeMillis) t0)]
        (println (str "    error after " elapsed " ms: "
                      (some-> err .getMessage)))
        (check! "T2: read→write upgrade fails"
                (some? err))
        (check! "T2: and fails IMMEDIATELY (< 400 ms) — busy_timeout cannot wait on it"
                (< elapsed 400))))

    ;; ---- T3 (fix): hook survives an external writer past busy_timeout -----
    (println "T3: on-put! hook vs a 2500 ms external lock holder")
    (let [before (:hook-failures @hx/!stats)
          holder (hold-write-lock! (ds) 2500)]
      (hx/on-put! (hx-doc "hx:busy:1" :probe/busy ["x" "y"]))
      (check! "T3: hook rows landed despite the long external writer"
              (wait-for 8000 #(= [[0 "x"] [1 "y"]] (rows-for "hx:busy:1"))))
      (check! "T3: no hook failure was counted"
              (= before (:hook-failures @hx/!stats)))
      (.join ^Thread holder 10000))

    ;; ---- T4: a genuine failure (busy outlasts the retry) is counted -------
    (println "T4: hook busy outlasting the retry is still counted")
    (let [latch (CountDownLatch. 1)
          held (promise)
          _holder (doto (Thread.
                        ^Runnable
                        (fn []
                          (jdbc/with-transaction [tx (ds)]
                            (jdbc/execute! tx ["INSERT INTO busy_probe(k, v) VALUES ('t4', '1')
                                                ON CONFLICT(k) DO UPDATE SET v=excluded.v"])
                            (deliver held true)
                            (.await latch 60 TimeUnit/SECONDS))))
                   (.setDaemon true)
                   (.start))
          _ @held
          before (:hook-failures @hx/!stats)
          retry-var (ns-resolve 'futon1b-hxindex '*busy-retry*)
          run (fn []
                (hx/on-put! (hx-doc "hx:busy:2" :probe/busy ["z"]))
                (wait-for 20000
                          #(= (inc before) (:hook-failures @hx/!stats))))
          counted (if retry-var
                    (with-redefs-fn {retry-var {:attempts 2 :base-sleep-ms 50}}
                      run)
                    (run))]
      (.countDown latch)
      (check! "T4: exhausted retry increments :hook-failures" counted)
      (check! "T4: last-hook-error records the op and id"
              (let [{:keys [op id]} (:last-hook-error @hx/!stats)]
                (and (= "upsert" op) (= "hx:busy:2" id)))))

    (println "test-hx-busy: all checks passed")))
