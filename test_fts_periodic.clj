(ns test-fts-periodic
  "Throwaway-node tests for the FTS5 repair loop and attributable index
  failures (added 2026-08-03 after 70 silent, unattributable index misses
  that only a hand-run catch-up recovered).

  Run: clojure -M:node -m test-fts-periodic"
  (:require [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [futon1b-text :as text]
            [xtdb.api :as xt]
            [xtdb.node :as xtn])
  (:import [java.nio.file Files]))

(defn- temp-dir []
  (-> (Files/createTempDirectory
       "futon1b-periodic-test-"
       (make-array java.nio.file.attribute.FileAttribute 0))
      .toFile
      .getAbsolutePath))

(defn- check! [label value]
  (println (format "  %-62s %s" label (if value "PASS" "FAIL")))
  (assert value label))

(defn- priv [sym] (ns-resolve 'futon1b-text sym))

(defn- in-index? [id]
  (let [ds @(var-get (priv '!ds))]
    (pos? (long (:n (jdbc/execute-one!
                     ds ["SELECT count(*) AS n FROM ev_fts WHERE id = ?" id]
                     {:builder-fn rs/as-unqualified-maps}))))))

(defn- wait-for
  "Poll PRED up to TIMEOUT-MS. Returns true as soon as it holds."
  [timeout-ms pred]
  (let [deadline (+ (System/currentTimeMillis) (long timeout-ms))]
    (loop []
      (cond (pred) true
            (> (System/currentTimeMillis) deadline) false
            :else (do (Thread/sleep 100) (recur))))))

(defn- doc [i at]
  {:xt/id (str "periodic-" i)
   :evidence/id (str "periodic-" i)
   :evidence/type :note
   :evidence/author "joe"
   :evidence/session-id "periodic-test"
   :evidence/at at
   :evidence/body (str "common periodic body " i)})

(defn -main [& _]
  (let [dir (temp-dir)
        seed (mapv #(doc % (format "2026-07-31T00:%02d:00Z" %)) (range 5))]
    (with-open [node (xtn/start-node)]
      (xt/execute-tx node (mapv (fn [d] [:put-docs :evidence d]) seed))
      (text/init! {:path (str dir "/fts5-evidence.db")})
      (text/catch-up! node)
      (check! "seed docs indexed by the initial catch-up" (in-index? "periodic-0"))

      ;; ---- the repair loop actually repairs -----------------------------
      ;; A doc written to XTDB whose live index never happened is exactly the
      ;; on-append! failure case: present in the store, absent from the index,
      ;; and (by design) the checkpoint was not advanced past it.
      (let [missed (doc 99 "2026-07-31T01:00:00Z")]
        (xt/execute-tx node [[:put-docs :evidence missed]])
        (check! "a doc whose live index failed is absent from the index"
                (not (in-index? "periodic-99")))
        (text/start-periodic-catch-up! node :interval-ms 500 :page 200)
        (check! "stats reports the repair loop as running"
                (true? (:periodic? (text/stats))))
        (check! "periodic catch-up re-indexes the missed doc without a restart"
                (wait-for 15000 #(in-index? "periodic-99")))
        (text/stop-periodic-catch-up!)
        (check! "stats reports the loop stopped"
                (false? (:periodic? (text/stats)))))

      ;; ---- the loop is opt-outable --------------------------------------
      (let [r (text/start-periodic-catch-up! node :interval-ms 0)]
        (check! "interval-ms <= 0 disables the loop"
                (and (false? (:periodic r)) (= :disabled (:reason r))))
        (check! "disabled means nothing was scheduled"
                (false? (:periodic? (text/stats)))))

      ;; ---- single-flight -------------------------------------------------
      ;; Two concurrent builds only lengthen sqlite's write-lock hold, which
      ;; amplifies the on-append! failures a catch-up exists to repair.
      (let [running (priv '!catch-up-running?)]
        (reset! (var-get running) true)
        (try
          (check! "a concurrent catch-up skips instead of piling on"
                  (= {:skipped :already-running} (text/catch-up! node)))
          (finally (reset! (var-get running) false)))
        (check! "the guard clears, so a later catch-up still runs"
                (contains? (text/catch-up! node) :indexed)))

      ;; ---- failures are attributable, not just counted --------------------
      (let [ds-atom (var-get (priv '!ds))
            good-ds @ds-atom
            stats-var (var-get (priv '!stats))
            ;; read the stats ATOM here, not text/stats: that does a
            ;; count(*) against the datasource this block deliberately breaks
            before (:errors @stats-var)]
        (swap! stats-var dissoc :last-error)
        (reset! ds-atom
                (jdbc/get-datasource
                 {:dbtype "sqlite"
                  :dbname "/nonexistent-futon1b-dir/does-not-exist.db"}))
        (try
          (text/on-append! (doc 404 "2026-07-31T02:00:00Z"))
          (check! "a failed live index records WHICH doc failed"
                  (wait-for 5000 #(= "periodic-404" (:id (:last-error @stats-var)))))
          (check! "the failure carries a diagnosable reason"
                  (seq (str (:error (:last-error @stats-var)))))
          (check! "the error counter still advances"
                  (> (long (:errors @stats-var)) (long before)))
          (finally (reset! ds-atom good-ds))))))
  (println "FTS PERIODIC: ALL PASS")
  (shutdown-agents))
