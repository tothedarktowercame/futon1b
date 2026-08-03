(ns futon1b-text
  "D1 — the FTS5 text sidecar, embedded in the store JVM (M-text-sidecar).

   Chalk-note semantics demonstrated at the application layer: the FTS5
   index is a CANDIDATE PRE-FILTER; membership is decided by re-checking
   every candidate against XTDB (fetch by id + structured filters). The
   index can therefore be stale or over-broad without ever being wrong —
   it only costs re-check work.

   Scope (POC boundary, mission §IDENTIFY): evidence table only; token
   AND/OR composition; one analysis chain (unicode61, NO stemming — keeps
   the scan+re-check oracle exact); BM25 ranking (the zaif retrieve arm
   consumes scores, P4). Prefix/phrase/relevance-beyond-BM25 deferred.

   Sync contract (P3): index refresh rides the append path (safe because
   evidence is append-only); on boot, catch-up scans evidence with
   :at >= last indexed :at (overlap deduped by delete+insert upsert).
   Staleness bound: one in-flight append (the future in on-append!).

   The sqlite file lives beside the XTDB store (<store-dir>/fts5-evidence.db)
   and is DERIVED data: deleting it and rebuilding is always safe."
  (:require [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [clojure.string :as str]
            [futon1b-xt :as fxt])
  (:import [java.util.concurrent Executors ScheduledExecutorService
            ThreadFactory TimeUnit]))

(def ^:private unqualified {:builder-fn rs/as-unqualified-maps})

(defonce !ds (atom nil))
(defonce !stats (atom {:indexed 0 :errors 0 :last-at nil}))

(def hydration-width 4)
(def max-df-terms 32)
(def max-offset 10000)

;; Periodic repair loop. on-append! is fire-and-forget and deliberately does
;; NOT advance the checkpoint, so a failed live index is repaired only by a
;; catch-up — and until 2026-08-03 the only triggers were a restart or a
;; manual POST. Measured that day: 70 silent, unattributable index misses,
;; recovered in 30s once a catch-up was finally run by hand.
(def default-catch-up-interval-ms
  (or (when-let [s (System/getenv "FUTON1B_FTS_CATCHUP_MS")]
        (try (Long/parseLong s) (catch Exception _ nil)))
      300000))

;; Small page on the repair pass. index-batch! wraps a whole page in ONE write
;; transaction, and that transaction is what contends with on-append! for
;; sqlite's single writer (busy_timeout 10s) — a boot-sized 1000-doc page can
;; outlast the timeout and manufacture the very failures this loop repairs.
(def periodic-page 200)

;; ---------------------------------------------------------------------------
;; Schema + init.
;; ---------------------------------------------------------------------------

(def ^:private ddl
  ;; WAL first: default rollback-journal mode makes WRITERS BLOCK READERS —
  ;; a stats count() returned SQLITE_BUSY mid-build (found live 2026-07-11).
  ;; WAL is a persistent db property; readers then never block on the build.
  ["PRAGMA journal_mode=WAL"
   "CREATE VIRTUAL TABLE IF NOT EXISTS ev_fts USING fts5(
      id UNINDEXED, author UNINDEXED, at UNINDEXED, session UNINDEXED,
      body, tokenize='unicode61')"
   "CREATE TABLE IF NOT EXISTS fts_meta (k TEXT PRIMARY KEY, v TEXT)"])

(defn- meta-get [ds k]
  (:fts_meta/v (first (jdbc/execute! ds ["SELECT v FROM fts_meta WHERE k = ?" k]))))

(defn- meta-set! [ds k v]
  (jdbc/execute! ds ["INSERT INTO fts_meta(k,v) VALUES(?,?)
                      ON CONFLICT(k) DO UPDATE SET v=excluded.v" k (str v)]))

(defn init!
  "Open (or create) the sidecar db beside the store. Idempotent."
  [{:keys [store-dir path]}]
  (let [file (or path (str store-dir "/fts5-evidence.db"))
        ;; busy_timeout: the on-append! future and the batch build contend
        ;; for sqlite's single writer; wait instead of throwing SQLITE_BUSY.
        ds (jdbc/get-datasource {:dbtype "sqlite" :dbname file
                                 :busy_timeout 10000})]
    (doseq [stmt ddl] (jdbc/execute! ds [stmt]))
    (reset! !ds ds)
    (swap! !stats assoc :last-at (meta-get ds "last-at"))
    {:ok true :path file :last-at (meta-get ds "last-at")}))

;; ---------------------------------------------------------------------------
;; Text extraction + indexing.
;; ---------------------------------------------------------------------------

(defn- body-text
  "Render the text-bearing content of an evidence doc. Body is the v0
   text field (strings as-is, structures pr-str'd — turn text, handoff
   records, and tool transcripts all live there)."
  [doc]
  (let [b (:evidence/body doc)]
    (if (string? b) b (pr-str b))))

(defn- index-batch!
  "Upsert docs into the index (delete+insert: FTS5 has no PK). One tx."
  [ds docs]
  (jdbc/with-transaction [tx ds]
    (doseq [d docs]
      (let [id (str (:xt/id d))]
        (jdbc/execute! tx ["DELETE FROM ev_fts WHERE id = ?" id])
        (jdbc/execute! tx ["INSERT INTO ev_fts(id, author, at, session, body)
                            VALUES (?,?,?,?,?)"
                           id
                           (str (:evidence/author d))
                           (str (:evidence/at d))
                           (some-> (:evidence/session-id d) str)
                           (body-text d)]))))
  (count docs))

(def ^:private scan-cols
  '[xt/id evidence/at evidence/author evidence/session-id evidence/body])

(defn- scan-after
  "One keyset page strictly after (at, id), oldest first. The compound
   key makes progress guaranteed even through :at tie plateaus (bulk
   imports share few :at values — thousands of docs per tie; pagination
   on :at alone spun there, found live on the first full build)."
  [node [after-at after-id] page]
  (fxt/safe-q node
              (list '-> (list 'from :evidence scan-cols)
                    (list 'where
                          (list 'or
                                (list '> 'evidence/at (str after-at))
                                (list 'and
                                      (list '= 'evidence/at (str after-at))
                                      (list '> 'xt/id (str after-id)))))
                    '(order-by evidence/at xt/id)
                    (list 'limit page))))

(defonce ^:private !catch-up-running? (atom false))

(defn catch-up!
  "Index everything strictly after the last indexed (at, id) checkpoint.
   With no checkpoint this is the full deterministic rebuild. Returns
   {:indexed n :last-at s}. Batched keyset pagination — never holds more
   than `page` bodies (the 259MB read-edn-file lesson).

   Single-flight: a concurrent caller gets {:skipped :already-running}
   instead of piling a second scan onto sqlite's single writer. Two builds
   at once only lengthen the write-lock hold and so amplify the on-append!
   failures a catch-up exists to repair."
  [node & {:keys [page] :or {page 1000}}]
  (if-not (compare-and-set! !catch-up-running? false true)
    {:skipped :already-running}
    (try
      (let [ds @!ds]
        (loop [after [(or (meta-get ds "last-at") "") (or (meta-get ds "last-id") "")]
               total 0]
          (let [docs (scan-after node after page)]
            (if (empty? docs)
              (do (swap! !stats assoc :indexed total)
                  {:indexed total :last-at (meta-get ds "last-at")})
              (let [n (index-batch! ds docs)
                    lst (last docs)
                    hi [(str (:evidence/at lst)) (str (:xt/id lst))]]
                (meta-set! ds "last-at" (first hi))
                (meta-set! ds "last-id" (second hi))
                (swap! !stats assoc :last-at (first hi))
                (recur hi (+ total n)))))))
      (finally (reset! !catch-up-running? false)))))

(defn on-append!
  "Write-path hook: index one freshly-written doc. Fire-and-forget —
   an index failure must never affect the verified put. Deliberately does
   NOT advance the (at, id) checkpoint: catch-up! owns it — a live append
   moving the checkpoint past territory an interrupted build never scanned
   would turn a restart into silent skips. The cost is bounded re-indexing
   (upsert dedupes) on the next catch-up."
  [xdoc]
  (when-let [ds @!ds]
    (future
      (try
        (index-batch! ds [xdoc])
        (catch Throwable t
          ;; Attributable, not just counted. A bare counter tells you THAT
          ;; n documents fell out of the index and never WHICH — so the gap
          ;; is undiagnosable and, since :ready stays true, invisible.
          (let [id (str (:xt/id xdoc))
                at (str (:evidence/at xdoc))
                msg (str (.getSimpleName (class t)) ": " (.getMessage t))]
            (swap! !stats (fn [s]
                            (-> s
                                (update :errors inc)
                                (assoc :last-error {:id id :at at :error msg}))))
            (println (str "[fts] index failed id=" id " at=" at " — " msg
                          " (recoverable: checkpoint not advanced; the next"
                          " catch-up re-indexes it)"))
            (flush)))))))

(defonce ^:private !scheduler (atom nil))

(defn- daemon-factory []
  (reify ThreadFactory
    (newThread [_ r]
      (doto (Thread. ^Runnable r "fts-periodic-catch-up")
        (.setDaemon true)))))

(defn start-periodic-catch-up!
  "Run catch-up! every INTERVAL-MS so a failed live append is repaired
   without waiting for a restart. Daemon thread; idempotent (a second call
   while one is scheduled is a no-op). INTERVAL-MS <= 0 disables the loop.

   scheduleWithFixedDelay, not AtFixedRate: a build slower than the interval
   must not stack another on top of it."
  [node & {:keys [interval-ms page]
           :or {interval-ms default-catch-up-interval-ms page periodic-page}}]
  (cond
    (not (pos? (long interval-ms))) {:ok true :periodic false :reason :disabled}
    (some? @!scheduler) {:ok true :periodic true :reason :already-running}
    :else
    (let [sched (Executors/newSingleThreadScheduledExecutor (daemon-factory))]
      (.scheduleWithFixedDelay
       sched
       ^Runnable
       (fn []
         (try
           (let [{:keys [indexed skipped]} (catch-up! node :page page)]
             ;; Quiet in the steady state — log only real repair work, so a
             ;; line here always means something had fallen out of the index.
             (when (and (nil? skipped) (pos? (long (or indexed 0))))
               (println (str "[fts] periodic catch-up re-indexed " indexed " doc(s)"))
               (flush)))
           (catch InterruptedException _
             ;; stop-periodic-catch-up! interrupting an in-flight scan is an
             ;; intentional shutdown, not a failure — don't cry wolf.
             (.interrupt (Thread/currentThread)))
           (catch Throwable t
             (println (str "[fts] periodic catch-up failed: "
                           (.getSimpleName (class t)) ": " (.getMessage t)))
             (flush))))
       (long interval-ms) (long interval-ms) TimeUnit/MILLISECONDS)
      (reset! !scheduler sched)
      {:ok true :periodic true :interval-ms interval-ms :page page})))

(defn stop-periodic-catch-up!
  "Cancel the repair loop (test/teardown use)."
  []
  (when-let [^ScheduledExecutorService s @!scheduler]
    (.shutdownNow s)
    (reset! !scheduler nil)
    {:ok true :periodic false}))

;; ---------------------------------------------------------------------------
;; Search: FTS5 candidates -> XTDB re-check.
;; ---------------------------------------------------------------------------

(defn- match-string
  "Sanitize a user query into FTS5 MATCH syntax. Tokens are double-quoted
   (no syntax injection); bare AND/OR pass through as operators; default
   conjunction is AND."
  [q]
  (let [toks (remove str/blank? (str/split (str q) #"\s+"))]
    (->> toks
         (map (fn [t]
                (if (contains? #{"AND" "OR"} t)
                  t
                  (str "\"" (str/replace t "\"" "\"\"") "\""))))
         (str/join " "))))

(defn- candidates
  "Ranked candidate ids from FTS5. Over-fetches so the re-check can drop
   stale/filtered rows without starving k."
  [ds {:keys [q author session-id since before limit offset]}]
  (let [k (or limit 10)
        offset (or offset 0)
        overfetch (max 50 (* 4 k))
        clauses (cond-> ["ev_fts MATCH ?"]
                  author (conj "author = ?")
                  session-id (conj "session = ?")
                  since (conj "at >= ?")
                  before (conj "at < ?"))
        params (cond-> [(match-string q)]
                 author (conj (str author))
                 session-id (conj (str session-id))
                 since (conj (str since))
                 before (conj (str before)))
        sql (str "SELECT id, bm25(ev_fts) AS score FROM ev_fts WHERE "
                 (str/join " AND " clauses)
                 " ORDER BY bm25(ev_fts) LIMIT ? OFFSET ?")]
    (jdbc/execute! ds (into [sql] (conj params overfetch offset)) unqualified)))

(def ^:private recheck-cols
  '[xt/id evidence/id evidence/at evidence/author evidence/session-id
    evidence/type evidence/ephemeral?])

(defn- fetch-doc
  [node id cols]
  (first (fxt/safe-q node (list '-> (list 'from :evidence cols)
                                (list 'where (list '= 'xt/id id))))))

(defn- fetch-wave
  "Fetch one candidate wave with four queries in flight. `safe-q` owns the
  process-wide four-permit budget, so concurrent HTTP requests cannot
  multiply this width."
  [node ids cols]
  (->> ids
       (partition-all hydration-width)
       (mapcat (fn [batch]
                 (->> batch
                      (mapv #(future (fetch-doc node % cols)))
                      (mapv deref))))
       vec))

(defn- passes-recheck?
  [doc {:keys [author session-id since before include-ephemeral]}]
  (and (or (nil? author) (= (str author) (str (:evidence/author doc))))
       (or (nil? session-id) (= (str session-id) (str (:evidence/session-id doc))))
       (or (nil? since) (>= (compare (str (:evidence/at doc)) (str since)) 0))
       (or (nil? before) (neg? (compare (str (:evidence/at doc)) (str before))))
       ;; contract semantics: param absent = no filtering
       (or (not (false? include-ephemeral))
           (not (true? (:evidence/ephemeral? doc))))))

(defn- recheck-candidates
  "Re-check candidates in waves of k and stop once k survive. This preserves
  the old short-circuit while replacing serial point reads with bounded
  concurrency. `cols` is what each surviving doc is fetched with — the narrow
  re-check projection when the caller does not want bodies, `[*]` when it does."
  [node cands k params cols]
  (loop [remaining cands
         survivors []]
    (if (or (>= (count survivors) k) (empty? remaining))
      (vec (take k survivors))
      (let [wave (vec (take k remaining))
            docs (fetch-wave node (mapv :id wave) cols)
            accepted (into []
                           (keep (fn [[cand doc]]
                                   (when (and doc (passes-recheck? doc params))
                                     {:score (:score cand) :doc doc})))
                           (map vector wave docs))]
        (recur (drop k remaining) (into survivors accepted))))))

(defn document-frequencies
  "Index-only document frequencies for sanitized terms. Never reads XTDB."
  [terms]
  (when (> (count terms) max-df-terms)
    (throw (IllegalArgumentException.
            (str "at most " max-df-terms " df terms are allowed"))))
  (let [ds @!ds
        terms (vec (distinct terms))
        frequencies
        (into {}
              (map (fn [term]
                     [term
                      (:n (jdbc/execute-one!
                           ds
                           ["SELECT count(*) AS n FROM ev_fts WHERE ev_fts MATCH ?"
                            (match-string term)]
                           unqualified))]))
              terms)
        indexed (:n (jdbc/execute-one! ds
                                      ["SELECT count(*) AS n FROM ev_fts"]
                                      unqualified))]
    {:df frequencies :indexed indexed}))

(defn search
  "Free-text search: FTS5 pre-filter + bounded-concurrency XTDB re-check.
   A candidate survives only if the doc exists in the store AND still
   passes the structured filters (author/session/since/before/ephemeral)
   read from the STORE's copy, not the index's. Returns
   {:results [{:score f :entry doc} ...] :count n :checked n :index-as-of s}."
  [node {:keys [limit hydrate] :as params}]
  (let [ds @!ds
        k (or limit 10)
        hydrate? (not (false? hydrate))
        cands (candidates ds params)
        ;; The re-check reads author/session/at/ephemeral?, ALL of which a full
        ;; doc already carries. So when the caller wants bodies, fetch `[*]`
        ;; ONCE in the re-check wave: a separate projection pass would double
        ;; the XTDB round trips on the default (hydrated) path — every existing
        ;; caller — for no gain. The narrow projection is a win only when the
        ;; bodies are then thrown away, i.e. under hydrate=false.
        survivors (recheck-candidates node cands k params
                                      (if hydrate? '[*] recheck-cols))
        results
        (if hydrate?
          (mapv (fn [{:keys [score doc]}]
                  {:score score :entry (dissoc doc :xt/id)})
                survivors)
          (mapv (fn [{:keys [score doc]}]
                  {:score score
                   :evidence/id (:evidence/id doc)
                   :evidence/at (:evidence/at doc)
                   :evidence/author (:evidence/author doc)
                   :evidence/type (:evidence/type doc)})
                survivors))]
    {:results results
     :count (count results)
     :checked (count cands)
     :index-as-of (meta-get ds "last-at")}))

(defn stats []
  (let [ds @!ds
        rows (when ds
               (:n (jdbc/execute-one! ds ["SELECT count(*) AS n FROM ev_fts"]
                                      unqualified)))]
    (assoc @!stats :rows rows :ready (some? ds)
           :periodic? (some? @!scheduler)
           :catch-up-running? @!catch-up-running?)))
