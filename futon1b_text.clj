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
            [clojure.edn :as edn]
            [clojure.string :as str]
            [futon1b-xt :as fxt]
            [xtdb.api :as xt]
            [xtdb.serde :as serde])
  (:import [java.time Instant]
           [java.time.temporal ChronoUnit]
           [java.util.concurrent Executors LinkedBlockingQueue
            ScheduledExecutorService ThreadFactory TimeUnit]
           [java.util.concurrent.atomic AtomicLong]))

(def ^:private unqualified {:builder-fn rs/as-unqualified-maps})

(defonce !ds (atom nil))
(defonce !started-at (str (java.time.Instant/now)))

(defonce !stats (atom {:indexed 0 :errors 0 :last-at nil
                       :recheck-rejections 0}))

(def hydration-width 4)
(def max-df-terms 32)
(def max-offset 10000)

(def projection
  [{:store-field :evidence/body
    :index-home :ev_fts/body
    :indexed-as :fts5-unicode61}
   {:store-field :evidence/author
    :index-home [:ev_fts/author :ev_attr/author]
    :indexed-as :btree}
   {:store-field :evidence/at
    :index-home [:ev_fts/at :ev_attr/at]
    :indexed-as :btree}
   {:store-field :evidence/session-id
    :index-home [:ev_fts/session :ev_attr/session]
    :indexed-as :btree}
   {:store-field :evidence/type
    :index-home :ev_attr/type
    :indexed-as [:btree :with-at]}
   {:store-field :evidence/claim-type
    :index-home :ev_attr/claim_type
    :indexed-as [:btree :with-at]}
   {:store-field :evidence/tags
    :index-home :ev_tags/tag-id
    :indexed-as :junction-primary-key}
   {:store-field :evidence/subject
    :index-home [:ev_attr/subject_type :ev_attr/subject_id]
    :indexed-as :composite-btree}
   {:store-field :evidence/pattern-id
    :index-home :ev_attr/pattern_id
    :indexed-as :btree}
   {:store-field :evidence/ephemeral?
    :index-home :ev_attr/ephemeral
    :indexed-as :recheck-assist}
   {:store-field :evidence/conjecture?
    :index-home :ev_attr/conjecture
    :indexed-as :recheck-assist}])

(def residual
  {:channels [:calls :email :speech]
   :history [:before-basis-capture]
   :undeclared-fields [:evidence/in-reply-to :evidence/fork-of :evidence/id]
   :undeclared-tables [:hyperedges :entities]})

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
   "CREATE TABLE IF NOT EXISTS fts_meta (k TEXT PRIMARY KEY, v TEXT)"
   "CREATE TABLE IF NOT EXISTS ev_attr (
  id TEXT PRIMARY KEY, type TEXT, claim_type TEXT, author TEXT, at TEXT,
  session TEXT, subject_type TEXT, subject_id TEXT, pattern_id TEXT,
  ephemeral INTEGER, conjecture INTEGER);"
   "CREATE INDEX IF NOT EXISTS ev_attr_claim_at ON ev_attr(claim_type, at);"
   "CREATE INDEX IF NOT EXISTS ev_attr_type_at  ON ev_attr(type, at);"
   "CREATE INDEX IF NOT EXISTS ev_attr_auth_at  ON ev_attr(author, at);"
   "CREATE INDEX IF NOT EXISTS ev_attr_subject  ON ev_attr(subject_type, subject_id);"
   "CREATE INDEX IF NOT EXISTS ev_attr_pattern  ON ev_attr(pattern_id);"
   ;; Index-served evidence reads (futon1b-evidence index-window): newest
   ;; first within a session, and newest first overall.
   "CREATE INDEX IF NOT EXISTS ev_attr_session_at ON ev_attr(session, at);"
   "CREATE INDEX IF NOT EXISTS ev_attr_at ON ev_attr(at, id);"
;; ev_fts rows by evidence id. FTS5 cannot index its `id` column, so
   ;; `DELETE FROM ev_fts WHERE id = ?` scanned the whole table (0.6 s and
   ;; up at 332k rows, 2026-09-28) inside the write lock, for every indexed
   ;; doc. See ensure-fts-map!.
   "CREATE TABLE IF NOT EXISTS ev_fts_map (id TEXT NOT NULL, fts_rowid INTEGER NOT NULL,
      PRIMARY KEY (id, fts_rowid)) WITHOUT ROWID;"
   ;; The stored evidence row as XTDB returns it (SELECT *), transit-encoded.
   ;; See the doc cache section.
   "CREATE TABLE IF NOT EXISTS ev_doc (id TEXT PRIMARY KEY, doc BLOB NOT NULL);"
   "CREATE TABLE IF NOT EXISTS ev_tags (
  id TEXT, tag TEXT, PRIMARY KEY (tag, id)) WITHOUT ROWID;"
   "CREATE INDEX IF NOT EXISTS ev_tags_id ON ev_tags(id);"
   ;; fts_meta gains keys: "basis-tx" (pr-str of :latest-completed-txs),
   ;;                      "basis-captured-at"
   ])

(defn- meta-get [ds k]
  (:fts_meta/v (first (jdbc/execute! ds ["SELECT v FROM fts_meta WHERE k = ?" k]))))

(defn- meta-set! [ds k v]
  (jdbc/execute! ds ["INSERT INTO fts_meta(k,v) VALUES(?,?)
                      ON CONFLICT(k) DO UPDATE SET v=excluded.v" k (str v)]))

(defn ensure-fts-map!
  "Fill ev_fts_map from ev_fts once (one scan), recorded in fts_meta. Until
  it is filled, index-batch! deletes from ev_fts by scanning, as before."
  [ds]
  (when-not (= "complete" (meta-get ds "fts-map"))
    (jdbc/with-transaction [tx ds]
      (jdbc/execute! tx ["INSERT OR IGNORE INTO ev_fts_map(id, fts_rowid)
                          SELECT id, rowid FROM ev_fts"])
      (meta-set! tx "fts-map" "complete"))))

(defn init!
  "Open (or create) the sidecar db beside the store. Idempotent."
  [{:keys [store-dir path]}]
  (let [file (or path (str store-dir "/fts5-evidence.db"))
        ;; busy_timeout: the on-append! future and the batch build contend
        ;; for sqlite's single writer; wait instead of throwing SQLITE_BUSY.
        ds (jdbc/get-datasource {:dbtype "sqlite" :dbname file
                                 :busy_timeout 10000})]
    (doseq [stmt ddl] (jdbc/execute! ds [stmt]))
    (ensure-fts-map! ds)
    (reset! !ds ds)
    (swap! !stats assoc :last-at (meta-get ds "last-at"))
    {:ok true :path file :last-at (meta-get ds "last-at")}))

;; ---------------------------------------------------------------------------
;; One writer at a time on the sidecar file.
;;
;; futon1b-hxindex and this namespace write the same SQLite file, and SQLite
;; has one write lock. Each used to hold its own in-process lock, so an hx
;; write (synchronous on the hyperedge path since c9c7fe6, ~5k/hour) and an
;; evidence index write waited on each other only through busy_timeout, and
;; the evidence side gave up: 983 SQLITE_BUSY failures in a day (2026-09-25).
;; Both now serialize on sidecar-write-lock; the busy retry is left for
;; writers outside this JVM.
;; ---------------------------------------------------------------------------

(defonce sidecar-write-lock (Object.))

(defn busy-error?
  "True for SQLITE_BUSY (5) and its extended codes (e.g. BUSY_SNAPSHOT 517);
   matched on the message too because driver/version differences decide
   whether the extended code reaches SQLException.getErrorCode."
  [t]
  (and (instance? java.sql.SQLException t)
       (let [e ^java.sql.SQLException t
             code (.getErrorCode e)
             msg (str (.getMessage e))]
         (or (= 5 code) (= 517 code)
             (str/includes? msg "SQLITE_BUSY")
             (str/includes? msg "database is locked")))))

(defn- with-sidecar-write*
  "Run F holding sidecar-write-lock. SQLITE_BUSY is retried with doubling
   backoff, ATTEMPTS times in all, or until F succeeds when ATTEMPTS is nil.
   The sleep happens outside the lock. Any other error is thrown."
  [attempts f]
  (loop [n 1 sleep-ms 250]
    (let [r (try {:ok (locking sidecar-write-lock (f))}
                 (catch Throwable t {:err t}))]
      (if-let [t (:err r)]
        (if (and (busy-error? t) (or (nil? attempts) (< n (long attempts))))
          (do (Thread/sleep sleep-ms)
              (recur (inc n) (min 8000 (* 2 sleep-ms))))
          (throw t))
        (:ok r)))))

(def ^:private catch-up-busy-attempts 6)

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
  "Upsert docs into all declared index tables. One transaction per batch."
  [ds docs]
  (jdbc/with-transaction [tx ds]
    (doseq [d docs]
      (let [id (str (:xt/id d))
            subject (:evidence/subject d)]
        (if (= "complete" (meta-get tx "fts-map"))
          (do (jdbc/execute! tx ["DELETE FROM ev_fts WHERE rowid IN
                                    (SELECT fts_rowid FROM ev_fts_map WHERE id = ?)" id])
              (jdbc/execute! tx ["DELETE FROM ev_fts_map WHERE id = ?" id]))
          (jdbc/execute! tx ["DELETE FROM ev_fts WHERE id = ?" id]))
        (jdbc/execute! tx ["DELETE FROM ev_attr WHERE id = ?" id])
        (jdbc/execute! tx ["DELETE FROM ev_tags WHERE id = ?" id])
        (jdbc/execute! tx ["INSERT INTO ev_fts(id, author, at, session, body)
                            VALUES (?,?,?,?,?)"
                           id
                           (str (:evidence/author d))
                           (str (:evidence/at d))
                           (some-> (:evidence/session-id d) str)
                           (body-text d)])
        (jdbc/execute! tx ["INSERT INTO ev_fts_map(id, fts_rowid) VALUES (?, last_insert_rowid())" id])
        (jdbc/execute! tx ["INSERT INTO ev_attr(
                              id, type, claim_type, author, at, session,
                              subject_type, subject_id, pattern_id,
                              ephemeral, conjecture)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?)"
                           id
                           (str (:evidence/type d))
                           (str (:evidence/claim-type d))
                           (str (:evidence/author d))
                           (str (:evidence/at d))
                           (str (:evidence/session-id d))
                           (str (:ref/type subject))
                           (str (:ref/id subject))
                           (str (:evidence/pattern-id d))
                           (if (:evidence/ephemeral? d) 1 0)
                           (if (:evidence/conjecture? d) 1 0)])
        (doseq [tag (:evidence/tags d)]
          (jdbc/execute! tx ["INSERT INTO ev_tags(id, tag) VALUES (?,?)"
                             id (str tag)])))))
  (count docs))

(def ^:private scan-cols
  '[xt/id evidence/at evidence/author evidence/session-id evidence/body
    evidence/type evidence/claim-type evidence/tags evidence/subject
    evidence/pattern-id evidence/ephemeral? evidence/conjecture?])

(defn- scan-after
  "One keyset page strictly after (at, id), oldest first. The compound
   key makes progress guaranteed even through :at tie plateaus (bulk
   imports share few :at values — thousands of docs per tie; pagination
   on :at alone spun there, found live on the first full build)."
  [node [after-at after-id] page]
  (fxt/safe-q node
              (fxt/pq '[p-at p-id p-page]
                      (list '-> (list 'from :evidence scan-cols)
                            '(where (or (> evidence/at p-at)
                                        (and (= evidence/at p-at)
                                             (> xt/id p-id))))
                            '(order-by evidence/at xt/id)
                            '(limit p-page))
                      (str after-at) (str after-id) page)))

(defonce ^:private !catch-up-running? (atom false))

(defn- tx-id-map
  "Reduce XTDB's per-db completed transaction records to plain numeric ids.
   The full status value remains stored in basis-tx; this normalized companion
   is EDN-readable without XTDB tagged-literal readers and exists only for
   numeric staleness calculation."
  [status]
  (into {}
        (map (fn [[db txs]]
               [(str db)
                (->> (tree-seq coll? seq txs)
                     (keep #(when (associative? %) (get % :tx-id)))
                     (filter number?)
                     (reduce max -1))]))
        (:latest-completed-txs status)))

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
      (let [ds @!ds
            basis-tx (select-keys
                      (xt/status node)
                      [:latest-completed-txs :latest-submitted-msg-ids
                       :latest-processed-msg-ids])
            basis-tx-ids (tx-id-map basis-tx)
            basis-captured-at (str (Instant/now))]
        (loop [after [(or (meta-get ds "last-at") "") (or (meta-get ds "last-id") "")]
               total 0]
          (let [docs (scan-after node after page)]
            (if (empty? docs)
              ;; Drain: the pre-scan basis is now proven covered (contract
              ;; C6). The checkpoint is NOT written here — it advances per
              ;; completed page below, which is what keeps a long rebuild
              ;; resumable and its progress observable in stats. The two
              ;; claims differ: the checkpoint's ("everything <= (at,id) is
              ;; indexed") is true after every page; the basis's ("reflects
              ;; the store as of these coordinates") only on a full drain.
              (do (with-sidecar-write*
                    catch-up-busy-attempts
                    #(jdbc/with-transaction [tx ds]
                       (meta-set! tx "basis-tx" (pr-str basis-tx))
                       (meta-set! tx "basis-tx-ids" (pr-str basis-tx-ids))
                       (meta-set! tx "basis-captured-at" basis-captured-at)))
                  (swap! !stats assoc :indexed total)
                  {:indexed total :last-at (meta-get ds "last-at")})
              (let [n (with-sidecar-write* catch-up-busy-attempts
                        #(index-batch! ds docs))
                    lst (last docs)
                    hi [(str (:evidence/at lst)) (str (:xt/id lst))]]
                (with-sidecar-write* catch-up-busy-attempts
                  #(do (meta-set! ds "last-at" (first hi))
                       (meta-set! ds "last-id" (second hi))))
                (swap! !stats assoc :last-at (first hi))
                (recur hi (+ total n)))))))
      (finally (reset! !catch-up-running? false)))))

;; ---------------------------------------------------------------------------
;; Doc cache: the stored row, so index-served reads need not touch XTDB.
;;
;; Reading `evidence$body` is what makes evidence reads cost what they do: XTDB
;; keeps it as one union of every body shape ever written (a 3,595-char type,
;; 2026-09-27), so a point read with the body costs 80-160 ms against ~20 ms
;; without, and 590 docs by `_id IN` cost 8.8 s. ev_doc holds each row exactly
;; as `SELECT * FROM evidence` returns it (transit round-trips it exactly), read
;; back from the store after commit by its own thread, so a slow read-back
;; never holds up ev_attr. Evidence is append-only (a duplicate id is a 409),
;; so a cached row does not go stale. A missing row is only a cache miss: the
;; caller reads it from the store.
;; ---------------------------------------------------------------------------

(defonce ^:private !node
  ;; The store node, for read-backs. Set by reconcile!.
  (atom nil))

(defonce ^:private doc-queue (LinkedBlockingQueue.))
(defonce ^:private !doc-filler (atom nil))

(def ^:private doc-fill-batch
  "Read-backs per round: at or below fxt/equality-hydrate-max-ids, so each
  round is point reads rather than one full-table `IN` read."
  fxt/equality-hydrate-max-ids)

(defn encode-doc
  "Transit-encode a complete store document for a SQLite sidecar body cache."
  ^bytes [doc]
  (serde/write-transit doc :json))

(defn decode-doc
  "Decode a complete store document written by encode-doc."
  [^bytes b]
  (serde/read-transit b :json))

(defn- put-docs!
  "Upsert stored rows into ev_doc."
  [ds docs]
  (when (seq docs)
    (with-sidecar-write*
      nil
      #(jdbc/with-transaction [tx ds]
         (doseq [d docs]
           (jdbc/execute! tx ["INSERT INTO ev_doc(id, doc) VALUES (?,?)
                               ON CONFLICT(id) DO UPDATE SET doc=excluded.doc"
                              (str (:xt/id d)) (encode-doc d)]))))))

(defn- doc-filler-loop []
  (try
    (loop []
      (let [batch (java.util.ArrayList.)]
        (.add batch (.take doc-queue))
        (.drainTo doc-queue batch (dec doc-fill-batch))
        (try
          (when-let [node @!node]
            (put-docs! @!ds (fxt/hydrate-by-ids node :evidence (vec batch))))
          (catch InterruptedException e (throw e))
          (catch Throwable t
            ;; A miss, not a loss: reads fall back to the store for these ids.
            (println (str "[fts] doc cache read-back failed for " (.size batch)
                          " id(s): " (.getSimpleName (class t)) ": " (.getMessage t)))
            (flush)))
        (recur)))
    (catch InterruptedException _ nil)))

(defn- queue-doc-fill! [id]
  (locking !doc-filler
    (when-not (some-> ^Thread @!doc-filler .isAlive)
      (reset! !doc-filler (doto (Thread. ^Runnable doc-filler-loop "fts-doc-filler")
                            (.setDaemon true)
                            (.start)))))
  (.put doc-queue (str id)))

(defn cached-docs
  "id -> stored row for the IDS ev_doc holds; absent ids are cache misses."
  [ids]
  (let [ds @!ds]
    (if (and ds (seq ids))
      (into {}
            (mapcat (fn [chunk]
                      (map (fn [r] [(:id r) (decode-doc (:doc r))])
                           (jdbc/execute! ds (into [(str "SELECT id, doc FROM ev_doc WHERE id IN ("
                                                         (str/join "," (repeat (count chunk) "?"))
                                                         ")")]
                                                   chunk)
                                          unqualified))))
            (partition-all 500 (map str ids)))
      {})))

(defn backfill-docs!
  "Fill ev_doc for every store row it lacks, in one streaming store read,
  flushing every 500 rows. Safe to repeat.

  DO NOT RUN THIS AGAINST THE LIVE STORE. XTDB builds the whole result before
  the stream yields a row: on 2026-09-28 00:07 it drove the serving heap to
  4,092 of 4,096 MB, held a query permit for seven minutes, and every Agency
  turn failed with \"futon1b read did not obtain evidence\" until the query
  was cancelled through xtdb.pgwire. It is fine on test-sized stores; the live
  backfill needs bounded windows."
  [node]
  (let [ds @!ds
        have (into #{} (map :id) (jdbc/execute! ds ["SELECT id FROM ev_doc"] unqualified))
        flush! (fn [buf] (put-docs! ds buf) [])
        [buf n] (fxt/timed-reduce-q
                 node ["SELECT * FROM evidence"]
                 (fn [[buf n] row]
                   (if (contains? have (str (:xt/id row)))
                     [buf n]
                     (let [buf (conj buf row)]
                       (if (>= (count buf) 500)
                         [(flush! buf) (inc n)]
                         [buf (inc n)]))))
                 [[] 0]
                 1800)]
    (flush! buf)
    {:filled n :already (count have)}))

;; ---------------------------------------------------------------------------
;; Live appends: one queue, one writer (2026-09-27).
;;
;; on-append! used to index each doc in its own future and give up after one
;; busy_timeout. A doc lost that way was repaired only if its :at was above
;; the catch-up checkpoint, so reads could never rely on the index alone and
;; every tag read also scanned the store's last 15 minutes directly (~2 s:
;; XTDB reads the whole evidence table for any predicate). Now every append is
;; queued for a single writer thread that retries SQLITE_BUSY until it
;; succeeds. An append that fails for any other reason is kept in !failed-ids
;; and retried by the periodic tick. `complete?` says when reads may use the
;; index alone.
;; ---------------------------------------------------------------------------

(defonce ^:private append-queue (LinkedBlockingQueue.))
(defonce ^:private enqueued-seq (AtomicLong. 0))
(defonce ^:private indexed-seq (AtomicLong. 0))
(defonce ^:private !writer (atom nil))

(defonce ^:private !failed-ids
  ;; id -> {:at :error} for appends the writer could not index; while any
  ;; remain, `complete?` is false.
  (atom {}))

(defonce ^:private !reconciled-at
  ;; Set by reconcile!: from then on every store doc is indexed, queued, or
  ;; in !failed-ids. nil after a boot or reload until reconcile! runs.
  (atom nil))

(def ^:private append-batch-max 200)

(defn- record-failure! [xdoc ^Throwable t]
  ;; Attributable, not just counted. A bare counter tells you THAT n
  ;; documents fell out of the index and never WHICH.
  (let [id (str (:xt/id xdoc))
        at (str (:evidence/at xdoc))
        msg (str (.getSimpleName (class t)) ": " (.getMessage t))]
    (swap! !failed-ids assoc id {:at at :error msg})
    (swap! !stats (fn [s]
                    (-> s
                        (update :errors inc)
                        (assoc :last-error {:id id :at at :error msg}))))
    (println (str "[fts] index failed id=" id " at=" at " — " msg
                  " (held in failed-ids; the periodic tick retries it)"))
    (flush)))

(defn- index-appends!
  "Index one drained batch, retrying SQLITE_BUSY without limit. Any other
   failure is isolated to the docs that cause it."
  [docs]
  (let [ds @!ds]
    (try
      (with-sidecar-write* nil #(index-batch! ds docs))
      (catch InterruptedException e (throw e))
      (catch Throwable t
        (if (= 1 (count docs))
          (record-failure! (first docs) t)
          (doseq [d docs]
            (try (with-sidecar-write* nil #(index-batch! ds [d]))
                 (catch InterruptedException e (throw e))
                 (catch Throwable t (record-failure! d t)))))))))

(defn- writer-loop []
  (try
    (loop []
      (let [batch (java.util.ArrayList.)]
        (.add batch (.take append-queue))
        (.drainTo append-queue batch (dec append-batch-max))
        (try
          (index-appends! (mapv second batch))
          (doseq [[_ d] batch] (queue-doc-fill! (:xt/id d)))
          (catch InterruptedException e (throw e))
          (catch Throwable t
            (doseq [[_ d] batch] (record-failure! d t))))
        (.set indexed-seq (long (first (.get batch (dec (.size batch))))))
        (recur)))
    (catch InterruptedException _ nil)))

(defn- ensure-writer! []
  (locking !writer
    (when-not (some-> ^Thread @!writer .isAlive)
      (reset! !writer (doto (Thread. ^Runnable writer-loop "fts-append-writer")
                        (.setDaemon true)
                        (.start))))))

(defn on-append!
  "Write-path hook: queue one freshly-written doc for the sidecar writer.
   Never blocks or fails the verified put. Deliberately does NOT advance the
   (at, id) checkpoint: catch-up! owns it — a live append moving the
   checkpoint past territory an interrupted build never scanned would turn a
   restart into silent skips."
  [xdoc]
  (when @!ds
    (ensure-writer!)
    ;; Sequence numbers must enter the queue in order: the writer publishes
    ;; the last one of each batch as indexed-seq.
    (locking append-queue
      (.put append-queue [(.incrementAndGet enqueued-seq) xdoc]))))

(def ^:private complete-wait-ms 2000)

(defn complete?
  "True when the sidecar holds every evidence doc acknowledged before this
   call: the writer has indexed every append queued so far (waiting up to
   WAIT-MS for it), nothing is held in !failed-ids, and reconcile! has run.
   Reads that get false keep scanning the store."
  ([] (complete? complete-wait-ms))
  ([wait-ms]
   (boolean
    (and @!ds
         @!reconciled-at
         (let [target (.get enqueued-seq)
               deadline (+ (System/currentTimeMillis) (long wait-ms))]
           (loop []
             (cond
               (>= (.get indexed-seq) target) true
               (> (System/currentTimeMillis) deadline) false
               :else (do (Thread/sleep 5) (recur)))))
         ;; After the wait: a failure in the awaited batch lands here first.
         (empty? @!failed-ids)))))

(defn retry-failed!
  "Re-index the docs held in !failed-ids from the store. A doc the store no
   longer holds needs no index row and is released too."
  [node]
  (let [ids (vec (keys @!failed-ids))]
    (doseq [chunk (partition-all 500 ids)]
      (let [docs (fxt/hydrate-by-ids node :evidence chunk)]
        (when (seq docs)
          (with-sidecar-write* catch-up-busy-attempts #(index-batch! @!ds docs)))
        (swap! !failed-ids #(apply dissoc % chunk))))
    {:retried (count ids)}))

(defn reconcile!
  "Index every store evidence doc the sidecar lacks, then mark it complete.
   catch-up! only scans above its checkpoint, so an append lost below it was
   never repaired; comparing the two id sets is what makes `complete?` true.
   Docs committed after the store read arrive through on-append!."
  [node]
  (reset! !node node)
  (let [ds @!ds
        store-ids (into [] (map #(str (:xt/id %)))
                        (fxt/timed-q node ["SELECT _id FROM evidence"] 300))
        index-ids (into #{} (map :id)
                        (jdbc/execute! ds ["SELECT id FROM ev_attr"] unqualified))
        missing (vec (remove index-ids store-ids))]
    (doseq [chunk (partition-all 500 missing)]
      (let [docs (fxt/hydrate-by-ids node :evidence chunk)]
        (with-sidecar-write* catch-up-busy-attempts #(index-batch! ds docs))))
    (retry-failed! node)
    (reset! !reconciled-at (str (Instant/now)))
    {:store (count store-ids) :missing (count missing)
     :reconciled-at @!reconciled-at}))

(declare attr-clauses index-enum-values)

(defn attr-candidates
  "Candidate {:id :at} rows for an evidence list read, with every filter of Q
   the index holds applied in SQL (attr-clauses, plus include-ephemeral=false),
   strictly below CURSOR [at id] when given, newest first, at most LIMIT
   (nil = all).

   Candidates only (contract C1): the caller re-checks each against the store.
   Complete only while `complete?` holds. nil when no sidecar is attached."
  [q cursor limit]
  (when-let [ds @!ds]
    (let [[first-tag & more-tags] (:tags q)
          {:keys [clauses params]} (attr-clauses (assoc q :tags more-tags))
          [c-at c-id] cursor
          clauses (cond-> clauses
                    (false? (:include-ephemeral q)) (conj "a.ephemeral = 0")
                    cursor (conj "(a.at < ? OR (a.at = ? AND a.id < ?))"))
          params (cond-> params
                   cursor (into [(str c-at) (str c-at) (str c-id)]))
          ;; The first tag is a join, not an EXISTS, so SQLite may drive the
          ;; read from ev_tags' (tag, id) key when the tag is the rarer filter.
          ;; DISTINCT: a tag stored in both of its enum spellings joins twice.
          sql (str "SELECT DISTINCT a.id, a.at FROM ev_attr a"
                   (when first-tag
                     " JOIN ev_tags t0 ON t0.id = a.id AND t0.tag IN (?,?)")
                   (when (seq clauses)
                     (str " WHERE " (str/join " AND " clauses)))
                   " ORDER BY a.at DESC, a.id DESC"
                   (when limit " LIMIT ?"))]
      (jdbc/execute! ds (cond-> (-> [sql]
                                    (into (when first-tag (index-enum-values first-tag)))
                                    (into params))
                          limit (conj limit))
                     unqualified))))

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
  [node & {:keys [interval-ms page min-free-permits]
           :or {interval-ms default-catch-up-interval-ms page periodic-page
                min-free-permits 2}}]
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
           (if (< (fxt/query-permits-available) (long min-free-permits))
             ;; Defer to interactive reads (2026-09-30: this scan and the hx
             ;; catch-up held two of the four query permits while agents'
             ;; evidence appends got 504s). The next tick runs it.
             (swap! !stats update :periodic-deferred (fnil inc 0))
           (do
           (when (seq @!failed-ids)
             (println (str "[fts] periodic retry of failed appends: "
                           (pr-str (retry-failed! node))))
             (flush))
           (let [{:keys [indexed skipped]} (catch-up! node :page page)]
             ;; Quiet in the steady state — log only real repair work, so a
             ;; line here always means something had fallen out of the index.
             (when (and (nil? skipped) (pos? (long (or indexed 0))))
               (println (str "[fts] periodic catch-up re-indexed " indexed " doc(s)"))
               (flush)))))
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

(defn- index-enum-values
  "Both verbatim representations a typed store value may have in the derived
   index. Writes deliberately preserve strings vs keywords; candidates must
   over-approximate that distinction and let the store-side re-check decide."
  [v]
  (when (some? v)
    (let [raw (str/replace-first (str v) #"^:" "")]
      [raw (str ":" raw)])))

(defn- attr-clauses
  "Attribute predicates over the `a` (ev_attr) alias, shared by candidate
   search and scoped document frequencies so the two cannot drift. Excludes
   the content MATCH, which each caller supplies itself."
  [{:keys [author session-id since before type claim-type tags
           subject-type subject-id pattern-id]}]
  {:clauses (cond-> []
              author (conj "a.author = ?")
              session-id (conj "a.session = ?")
              since (conj "a.at >= ?")
              before (conj "a.at < ?")
              type (conj "a.type IN (?,?)")
              claim-type (conj "a.claim_type IN (?,?)")
              subject-type (conj "a.subject_type IN (?,?)")
              subject-id (conj "a.subject_id = ?")
              pattern-id (conj "a.pattern_id IN (?,?)")
              (seq tags)
              (into (repeat
                     (count tags)
                     "EXISTS (SELECT 1 FROM ev_tags t
                              WHERE t.id = a.id AND t.tag IN (?,?))")))
   :params (cond-> []
             author (conj (str author))
             session-id (conj (str session-id))
             since (conj (str since))
             before (conj (str before))
             type (into (index-enum-values type))
             claim-type (into (index-enum-values claim-type))
             subject-type (into (index-enum-values subject-type))
             subject-id (conj (str subject-id))
             pattern-id (into (index-enum-values pattern-id))
             (seq tags) (into (mapcat index-enum-values tags)))})

(defn- candidates
  "Candidate ids from one composable SQL statement. Content queries rank by
   BM25; attribute-only queries use deterministic newest-first ordering."
  [ds {:keys [q limit offset] :as opts}]
  (let [k (or limit 10)
        offset (or offset 0)
        overfetch (max 50 (* 4 k))
        content? (not (str/blank? (str q)))
        from (if content?
               "ev_fts f JOIN ev_attr a USING (id)"
               "ev_attr a")
        {attr-c :clauses attr-p :params} (attr-clauses opts)
        clauses (cond-> []
                  content? (conj "ev_fts MATCH ?")
                  :always (into attr-c))
        params (cond-> []
                 content? (conj (match-string q))
                 :always (into attr-p))
        sql (str "SELECT a.id, "
                 (if content? "bm25(ev_fts)" "NULL")
                 " AS score FROM " from
                 (when (seq clauses)
                   (str " WHERE " (str/join " AND " clauses)))
                 (if content?
                   " ORDER BY bm25(ev_fts)"
                   " ORDER BY a.at DESC, a.id DESC")
                 " LIMIT ? OFFSET ?")]
    (jdbc/execute! ds (into [sql] (conj params overfetch offset)) unqualified)))

(defn tag-candidates
  "Candidate [id at] rows for the evidence list/count path: ids whose indexed
   tags include every one of TAGS, with at in [SINCE, BELOW-AT), strictly below
   CURSOR [at id] when given, newest first, at most LIMIT (nil = all).

   Candidates only (contract C1): the caller re-checks each against the store.
   Returns nil when no sidecar is attached, so the caller keeps its store scan.
   BELOW-AT is required: the caller scans the store itself from there up,
   because this index can miss documents newer than its checkpoint."
  [{:keys [tags since below-at cursor limit]}]
  (when-let [ds @!ds]
    (let [[first-tag & more-tags] tags
          [c-at c-id] cursor
          clauses (cond-> ["t.tag IN (?,?)" "a.at < ?"]
                    since (conj "a.at >= ?")
                    cursor (conj "(a.at < ? OR (a.at = ? AND a.id < ?))")
                    (seq more-tags)
                    (into (repeat (count more-tags)
                                  "EXISTS (SELECT 1 FROM ev_tags t2
                                           WHERE t2.id = a.id AND t2.tag IN (?,?))")))
          params (cond-> (into (index-enum-values first-tag) [(str below-at)])
                   since (conj (str since))
                   cursor (into [(str c-at) (str c-at) (str c-id)])
                   (seq more-tags) (into (mapcat index-enum-values more-tags)))
          sql (str "SELECT a.id, a.at FROM ev_tags t JOIN ev_attr a ON a.id = t.id"
                   " WHERE " (str/join " AND " clauses)
                   " ORDER BY a.at DESC, a.id DESC"
                   (when limit " LIMIT ?"))]
      (jdbc/execute! ds (into [sql] (cond-> params limit (conj limit)))
                     unqualified))))

(defn checkpoint-at
  "The sidecar's catch-up checkpoint :at (everything at or before it has been
   offered to the index), or nil when no sidecar is attached or none is set."
  []
  (when-let [ds @!ds]
    (let [v (meta-get ds "last-at")]
      (when-not (str/blank? v) v))))

(def ^:private recheck-cols
  '[xt/id evidence/id evidence/at evidence/author evidence/session-id
    evidence/body evidence/type evidence/claim-type evidence/tags
    evidence/subject evidence/pattern-id evidence/ephemeral?])

(defn- fetch-doc
  [node id cols]
  (first (fxt/safe-q node (fxt/pq '[p-id]
                                  (list '-> (list 'from :evidence cols)
                                        '(where (= xt/id p-id)))
                                  id))))

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

(defn- enum-value [v]
  (some-> (if (keyword? v) (subs (str v) 1) (str v))
          (str/replace-first #"^:" "")))

(defn- content-tokens [s]
  (->> (re-seq #"[\p{L}\p{N}]+" (str/lower-case (str s)))
       set))

(defn- content-match?
  "Certify the content predicate against the store's body text. Token
   semantics approximate FTS5 unicode61 minus diacritic folding, so a
   diacritic-insensitive index match can be dropped here — a bounded
   recall edge, never a wrong answer (C1 direction)."
  [doc q]
  (if (str/blank? (str q))
    true
    (let [body-tokens (content-tokens (body-text doc))
          groups (partition-by #{"OR"} (str/split (str q) #"\s+"))
          conjunctions (->> groups
                            (remove #(= ["OR"] %))
                            (map #(remove #{"AND"} %)))]
      (boolean
       (some (fn [terms]
               (every? body-tokens (mapcat content-tokens terms)))
             conjunctions)))))

(defn- tag-match? [doc tags]
  (let [stored (set (map enum-value (:evidence/tags doc)))]
    (every? stored (map enum-value tags))))

(defn- passes-recheck?
  [doc {:keys [q author session-id since before type claim-type tags
               subject-type subject-id pattern-id include-ephemeral]}]
  (let [subject (:evidence/subject doc)]
    (and (content-match? doc q)
       (or (nil? author) (= (str author) (str (:evidence/author doc))))
       (or (nil? session-id) (= (str session-id) (str (:evidence/session-id doc))))
       (or (nil? since) (>= (compare (str (:evidence/at doc)) (str since)) 0))
       (or (nil? before) (neg? (compare (str (:evidence/at doc)) (str before))))
       (or (nil? type) (= (enum-value type) (enum-value (:evidence/type doc))))
       (or (nil? claim-type)
           (= (enum-value claim-type) (enum-value (:evidence/claim-type doc))))
       (or (empty? tags) (tag-match? doc tags))
       (or (nil? subject-type)
           (= (enum-value subject-type) (enum-value (:ref/type subject))))
       (or (nil? subject-id) (= (str subject-id) (str (:ref/id subject))))
       (or (nil? pattern-id)
           (= (enum-value pattern-id) (enum-value (:evidence/pattern-id doc))))
       ;; contract semantics: param absent = no filtering
       (or (not (false? include-ephemeral))
           (not (true? (:evidence/ephemeral? doc)))))))

(defn- recheck-candidates
  "Re-check candidates in waves of k and stop once k survive. This preserves
  the old short-circuit while replacing serial point reads with bounded
  concurrency. `cols` is what each surviving doc is fetched with — the narrow
  re-check projection when the caller does not want bodies, `[*]` when it does."
  [node cands k params cols]
  (loop [remaining cands
         survivors []
         checked 0]
    (if (or (>= (count survivors) k) (empty? remaining))
      {:survivors (vec survivors) :checked checked}
      (let [wave (vec (take k remaining))
            docs (fetch-wave node (mapv :id wave) cols)
            needed (- k (count survivors))
            {:keys [accepted wave-checked]}
            (loop [pairs (seq (map vector wave docs))
                   accepted []
                   wave-checked 0]
              (if (or (empty? pairs) (>= (count accepted) needed))
                {:accepted accepted :wave-checked wave-checked}
                (let [[[cand doc] & more] pairs]
                  (recur more
                         (cond-> accepted
                           (and doc (passes-recheck? doc params))
                           (conj {:score (:score cand) :doc doc}))
                         (inc wave-checked)))))]
        (swap! !stats update :recheck-rejections (fnil + 0)
               (- wave-checked (count accepted)))
        (recur (drop k remaining)
               (into survivors accepted)
               (+ checked wave-checked))))))

(defn- hydrate-survivors
  "Hydrate certified ids through the same bounded SQL IN shape used by the
   evidence list path. SQLite proposes ids; this query still reads XTDB."
  [node survivors]
  (if-not (seq survivors)
    []
    (let [ids (mapv #(get-in % [:doc :xt/id]) survivors)
          placeholders (str/join "," (repeat (count ids) "?"))
          sql (str "SELECT * FROM evidence WHERE _id IN (" placeholders ")")
          docs (fxt/safe-q node (into [sql] ids))
          by-id (into {} (map (juxt :xt/id identity)) docs)]
      (into []
            (keep (fn [{:keys [doc] :as survivor}]
                    (when-let [full-doc (get by-id (:xt/id doc))]
                      (assoc survivor :doc full-doc))))
            survivors))))

(defn document-frequencies
  "Index-only document frequencies for sanitized terms. Never reads XTDB.

   With `filters`, df is computed WITHIN that population and `:indexed` is the
   size of that same population -- both must move together or the ratio is
   meaningless. The response states `:population` so a caller can tell a scoped
   answer from an unscoped one; a df of 15 against 149,766 and a df of 15
   against 770 are different claims, and until 2026-08-19 the endpoint silently
   ignored filters and always returned the former.

   Filter keys are the search ones: :author :session-id :since :before :type
   :claim-type :tags :subject-type :subject-id :pattern-id."
  ([terms] (document-frequencies terms nil))
  ([terms filters]
   (when (> (count terms) max-df-terms)
     (throw (IllegalArgumentException.
             (str "at most " max-df-terms " df terms are allowed"))))
   (let [ds @!ds
         terms (vec (distinct terms))
         {:keys [clauses params]} (attr-clauses (or filters {}))
         scoped? (seq clauses)
         from (if scoped? "ev_fts f JOIN ev_attr a USING (id)" "ev_fts")
         where (str/join " AND " (cons "ev_fts MATCH ?" clauses))
         sql (str "SELECT count(*) AS n FROM " from " WHERE " where)
         frequencies
         (into {}
               (map (fn [term]
                      [term
                       (:n (jdbc/execute-one!
                            ds (into [sql (match-string term)] params)
                            unqualified))]))
               terms)
         ;; the denominator must be the SAME population the numerators were
         ;; counted over, or a percentile band computed from it is nonsense
         indexed (:n (jdbc/execute-one!
                      ds (if scoped?
                           (into [(str "SELECT count(*) AS n FROM ev_attr a WHERE "
                                       (str/join " AND " clauses))]
                                 params)
                           ["SELECT count(*) AS n FROM ev_fts"])
                      unqualified))]
     {:df frequencies
      :indexed indexed
      :population (if scoped? :filtered :whole-index-unfiltered)
      :filters (when scoped? (into {} (remove (comp nil? val) (or filters {}))))})))

(defn search
  "Unified content/attribute candidates + bounded-concurrency XTDB re-check.
   A candidate survives only if the doc exists in the store AND still
   passes every requested predicate read from the STORE's copy, not the
   index's."
  [node {:keys [limit hydrate] :as params}]
  (let [ds @!ds
        k (or limit 10)
        hydrate? (not (false? hydrate))
        cands (candidates ds params)
        ;; Two phases, deliberately: the re-check wave fetches recheck-cols
        ;; (which must include body — content certification reads the STORE's
        ;; text, not the index's), then hydrate-survivors fetches full docs
        ;; for the ≤k survivors in ONE SQL IN — the only affordable id-set
        ;; shape (TN 2026-08-02 §1). The survivor bodies are read twice on
        ;; the hydrated path; bounded by k, and it keeps the wave projection
        ;; independent of what callers want back.
        {:keys [survivors checked]}
        (recheck-candidates node cands k params recheck-cols)
        survivors (if hydrate?
                    (hydrate-survivors node survivors)
                    survivors)
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
     ;; Identities in rank order, OUTSIDE the payload text. A hydrated
     ;; response cannot be lexically scraped: evidence bodies legitimately
     ;; contain ':score ' and ':evidence/id' themselves, so splitting on
     ;; those over-segments (measured: a limit=10 query yielded 310 ':score '
     ;; boundaries) and the miscount is silent. Consumers that want ids
     ;; should read this, not regex the body.
     ;; Both shapes: hydrated results nest the id under :entry, bare ones
     ;; carry it top-level. The first version read only the top level, so it
     ;; returned a full-length vector of nils in the hydrated mode -- the only
     ;; mode this field exists for. It was "verified" by comparing it against
     ;; (mapv :evidence/id results), which is its own implementation and
     ;; therefore agrees with itself in both modes, nils included.
     :ids (mapv #(or (:evidence/id %) (get-in % [:entry :evidence/id])) results)
     :count (count results)
     :checked checked
     :index-as-of (meta-get ds "last-at")
     :index-basis {:checkpoint [(meta-get ds "last-at")
                                (meta-get ds "last-id")]
                   :basis-tx (meta-get ds "basis-tx")
                   :basis-captured-at (meta-get ds "basis-captured-at")}}))

(defn- read-meta-edn [ds k]
  (when-let [s (and ds (meta-get ds k))]
    (try (edn/read-string s) (catch Throwable _ nil))))

(defn- staleness [node ds captured-at]
  (let [basis-ids (read-meta-edn ds "basis-tx-ids")
        live-ids (when node (tx-id-map (xt/status node)))
        tx-lag (when (and basis-ids live-ids)
                 (->> (into #{} (concat (keys basis-ids) (keys live-ids)))
                      (map #(max 0 (- (long (get live-ids % -1))
                                      (long (get basis-ids % -1)))))
                      (reduce max 0)))
        age-ms (when captured-at
                 (try
                   (max 0 (.between ChronoUnit/MILLIS
                                    (Instant/parse captured-at) (Instant/now)))
                   (catch Throwable _ nil)))]
    {:tx-lag tx-lag :age-ms age-ms}))

(defn stats
  "Index health. Read the WINDOW on each counter before comparing any two.

   Three fields were misread together in the field (2026-08-19), so they are
   labelled inline:

     :indexed  total from the LAST catch-up run only, overwritten each run --
               not a lifetime figure;
     :errors   CUMULATIVE on-append! failures since process start, never reset;
     :ready    the sidecar datasource is attached. NOT a coverage claim.

   :indexed and :errors have different windows and different denominators, so
   their ratio means nothing. An on-append! failure does not drop a write: the
   store already holds the document, the checkpoint is deliberately not
   advanced, and the next catch-up re-indexes it. Measured 2026-08-19 on Zone:
   all 125 logged failures were present in ev_fts AND ev_attr afterwards, with
   index and store level at 150,428.

   The coverage claim is :basis (C2/C6) -- present means a scan drained."
  ([] (stats nil))
  ([node]
   (let [ds @!ds
         rows (when ds
                (:n (jdbc/execute-one! ds ["SELECT count(*) AS n FROM ev_fts"]
                                       unqualified)))
         captured-at (when ds (meta-get ds "basis-captured-at"))
         basis {:tx (when ds (meta-get ds "basis-tx"))
                :captured-at captured-at}]
     (assoc @!stats :rows rows :ready (some? ds)
            ;; Windows, inline: these were read as a ratio in the field
            ;; and are not comparable. See the docstring.
            :ready-means :sidecar-attached-not-coverage
            :indexed-window :last-catch-up-run-only
            :errors-window :cumulative-since-process-start
            :process-started-at !started-at
            :coverage-claim-is :basis
            :basis basis
            :staleness (staleness node ds captured-at)
            :projection projection
            :residual residual
            :periodic? (some? @!scheduler)
            :appends {:enqueued (.get enqueued-seq) :indexed (.get indexed-seq)
                      :failed (count @!failed-ids)
                      :reconciled-at @!reconciled-at
                      :doc-fill-queued (.size doc-queue)}
            :catch-up-running? @!catch-up-running?))))
