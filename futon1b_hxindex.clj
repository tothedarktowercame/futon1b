(ns futon1b-hxindex
  "P1 hyperedge sidecar index (DESIGN-hyperedge-scope-sidecar-2026-09-26 §3/§4,
  P0 measurements by kimi-7). Fills the index only; no read is served from it
  yet (that is P2).

  Tables live in the SAME SQLite file family as futon1b-text (the datasource
  is shared): hx_edge(hx_id, type, pos, endpoint) with the two indexes the
  PROTO-neo4j review showed make the two-hop query seek (endpoint, pos, hx_id)
  and (hx_id, pos), plus (type, endpoint) for Q2/Q3. hx_meta(k,v) holds the
  system-time checkpoint. hx_id is kept as text — no interning yet.

  Sync contract (mirrors the evidence sidecar):
  - a post-time hook (on-put!/on-delete!) upserts/deletes rows after the XTDB
    tx is verified, is fire-and-forget, never fails the request, and NEVER
    advances the checkpoint;
  - catch-up! owns the checkpoint: keyset-paged `FOR ALL SYSTEM_TIME` queries
    on _system_from (upserts) and _system_to (tombstones — P0 measured that
    deletes are invisible to _system_from). Every changed id is re-read from
    the store and upserted/deleted, so a superseded version and a real
    tombstone both repair correctly. A skipped hook is repaired by the next
    catch-up, at boot and then periodically (FUTON1B_HX_CATCHUP_MS, default
    15 min — each catch-up query scans the whole hyperedges table per the P0
    EXPLAIN, so this is not a sub-minute poll; the caller takes an
    expensive-read permit, and the run is skipped when xt.txs shows no new
    transaction).

  The index is DERIVED data: rebuild! from empty is always safe."
  (:require [clojure.string :as str]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [futon1b-text :as text]
            [futon1b-xt :as fxt])
  (:import [java.time Instant OffsetDateTime ZoneOffset]
           [java.util.concurrent Executors ScheduledExecutorService
            ThreadFactory TimeUnit]))

(def ^:private unqualified {:builder-fn rs/as-unqualified-maps})

(defonce !stats (atom {:hook-failures 0
                       :last-hook-error nil
                       :last-catch-up nil}))

(def default-catch-up-interval-ms
  (or (when-let [s (System/getenv "FUTON1B_HX_CATCHUP_MS")]
        (try (Long/parseLong s) (catch Exception _ nil)))
      900000))

;; ---------------------------------------------------------------------------
;; P3e: SQLITE_BUSY hardening.
;;
;; The live hook failures (hx-stats, 2026-09-26, after the 10:57 restart)
;; were plain [SQLITE_BUSY] — the PRIMARY result code 5, i.e. the busy
;; handler waited the full busy_timeout (PRAGMA busy_timeout = 10000,
;; verified live) and the write lock was STILL held: another writer held
;; the lock longer than 10 s (cause a of the requisition). It is not the
;; WAL deferred read→write upgrade (cause b): that fails IMMEDIATELY with
;; SQLITE_BUSY_SNAPSHOT — busy_timeout never applies because no wait can
;; help — and none of the sidecar's write transactions can even hit it,
;; because none of them reads inside the transaction (reproduced in
;; test-hx-busy: the read→write case errors in < 100 ms, the contended
;; pure-writer case errors only after busy_timeout expires).
;;
;; Two layers, chosen from that evidence:
;; - one in-process lock serializes EVERY hxindex write transaction, so the
;;   fire-and-forget hooks never contend with catch-up!/fill!/backfill!
;;   running in this JVM (the measured failures coincided with an init!/
;;   backfill run in the serving JVM);
;; - a bounded retry on SQLITE_BUSY absorbs writers this lock cannot reach:
;;   an operator's script can hold the lock from another process entirely.
;;   (futon1b-text's FTS writes shared the file but not this lock until
;;   2026-09-27; the lock is now text/sidecar-write-lock.)
;;
;; Failures that outlast the retry still land in hook-failure! and keep
;; reads-usable? honest — the P2 gate is unchanged.
;; ---------------------------------------------------------------------------

;; The sidecar file's single lock, shared with futon1b-text's evidence
;; writes (2026-09-27): two in-process locks on one SQLite file only moved
;; the contention into busy_timeout.
(def ^:private !write-lock text/sidecar-write-lock)

(def ^:dynamic *busy-retry*
  "Bounded retry policy for SQLITE_BUSY on sidecar writes (P3e). Each
   attempt itself waits up to the connection's busy_timeout (10 s live), so
   6 attempts cover ~1 minute of external lock-holding before giving up.
   Rebindable for tests."
  {:attempts 6 :base-sleep-ms 250})

(defn- busy-error?
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

(defn- with-busy-retry*
  "Run F, retrying on SQLITE_BUSY with doubling backoff per *busy-retry*.
   Any other error — and a BUSY that outlasts :attempts — is thrown."
  [f]
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

(defn- write-locked*
  "Run F holding the in-process hx write lock, with the busy retry OUTSIDE
   the lock (a failed attempt releases the monitor before sleeping, so it
   never starves the writer this JVM is legitimately waiting on)."
  [f]
  (with-busy-retry* (fn [] (locking !write-lock (f)))))

(defmacro ^:private ^{:clj-kondo/lint-as 'clojure.core/with-open} with-write-tx
  "jdbc/with-transaction under write-locked*: serialized against every other
   hxindex writer in this JVM and retried on SQLITE_BUSY."
  [[tx ds] & body]
  `(write-locked*
    (fn [] (jdbc/with-transaction [~tx ~ds] ~@body))))

;; ---------------------------------------------------------------------------
;; Schema. Shares the futon1b-text datasource (same SQLite file family).
;; ---------------------------------------------------------------------------

(def ^:private ddl
  ["CREATE TABLE IF NOT EXISTS hx_edge (
      hx_id TEXT NOT NULL, type TEXT NOT NULL,
      pos INTEGER NOT NULL, endpoint TEXT NOT NULL)"
   ;; (type, endpoint): Q2 type+endpoint, Q3 endpoint prefix range.
   "CREATE INDEX IF NOT EXISTS hx_type_end ON hx_edge(type, endpoint)"
   ;; The two the PROTO-neo4j review showed turn the two-hop query from
   ;; not-finishing into 2.7 ms:
   "CREATE INDEX IF NOT EXISTS hx_end_pos_id ON hx_edge(endpoint, pos, hx_id)"
   "CREATE INDEX IF NOT EXISTS hx_id_pos ON hx_edge(hx_id, pos)"
   ;; (type, hx_id): P3 type-only keyset reads (type-candidates/type-count) —
   ;; (type, endpoint) cannot deliver hx_id order for a whole type.
   "CREATE INDEX IF NOT EXISTS hx_type_id ON hx_edge(type, hx_id)"
   ;; P3c: one row per hyperedge, present even with ZERO endpoints (hx_edge
   ;; has one row per endpoint, so a zero-endpoint hyperedge is invisible to
   ;; it — type-candidates/type-count and census read from hx_node instead).
   "CREATE TABLE IF NOT EXISTS hx_node (
      hx_id TEXT PRIMARY KEY, type TEXT NOT NULL)"
   "CREATE INDEX IF NOT EXISTS hx_node_type_id ON hx_node(type, hx_id)"
   "CREATE TABLE IF NOT EXISTS hx_meta (k TEXT PRIMARY KEY, v TEXT)"])

(defn- ds* [] @text/!ds)

(defn- meta-get [ds k]
  (:v (first (jdbc/execute! ds ["SELECT v FROM hx_meta WHERE k = ?" k]
                            unqualified))))

(defn- meta-set! [ds k v]
  (write-locked*
   (fn []
     (jdbc/execute! ds ["INSERT INTO hx_meta(k,v) VALUES(?,?)
                         ON CONFLICT(k) DO UPDATE SET v=excluded.v" k (str v)]))))

(defn init!
  "Create the hyperedge tables on the shared sidecar datasource.
   Idempotent. Requires futon1b-text/init! to have run (it owns the file)."
  ([] (init! {:ds (ds*)}))
  ([{:keys [ds]}]
   (when-not ds
     (throw (ex-info "hxindex: no sidecar datasource (text/init! first)" {})))
   (doseq [stmt ddl] (jdbc/execute! ds [stmt]))
   {:ok true}))

;; ---------------------------------------------------------------------------
;; Row upsert/delete.
;; ---------------------------------------------------------------------------

(defn index-doc!
  "Delete any hx_edge rows for the doc's hx_id, insert one row per endpoint
   position, and upsert the hx_node row (one per hyperedge, even with zero
   endpoints — P3c). One SQLite transaction, so readers never see a
   half-replaced hyperedge (positions may change between versions)."
  [ds doc]
  (let [id (str (:xt/id doc))
        t (str (:hx/type doc))
        endpoints (:hx/endpoints doc)]
    (with-write-tx [tx ds]
      (jdbc/execute! tx ["DELETE FROM hx_edge WHERE hx_id = ?" id])
      (doseq [[pos end] (map-indexed vector endpoints)]
        (jdbc/execute! tx ["INSERT INTO hx_edge(hx_id, type, pos, endpoint)
                            VALUES (?,?,?,?)" id t pos (str end)]))
      (jdbc/execute! tx ["INSERT INTO hx_node(hx_id, type) VALUES (?,?)
                          ON CONFLICT(hx_id) DO UPDATE SET type=excluded.type"
                         id t])))
  (count (:hx/endpoints doc)))

(defn index-docs!
  "index-doc! for DOCS in one SQLite transaction (the hyperedge batch write)."
  [ds docs]
  (with-write-tx [tx ds]
    (doseq [doc docs]
      (let [id (str (:xt/id doc))
            t (str (:hx/type doc))]
        (jdbc/execute! tx ["DELETE FROM hx_edge WHERE hx_id = ?" id])
        (doseq [[pos end] (map-indexed vector (:hx/endpoints doc))]
          (jdbc/execute! tx ["INSERT INTO hx_edge(hx_id, type, pos, endpoint)
                              VALUES (?,?,?,?)" id t pos (str end)]))
        (jdbc/execute! tx ["INSERT INTO hx_node(hx_id, type) VALUES (?,?)
                            ON CONFLICT(hx_id) DO UPDATE SET type=excluded.type"
                           id t]))))
  (count docs))

(defn delete-id! [ds id]
  (with-write-tx [tx ds]
    (jdbc/execute! tx ["DELETE FROM hx_edge WHERE hx_id = ?" (str id)])
    (jdbc/execute! tx ["DELETE FROM hx_node WHERE hx_id = ?" (str id)])))

;; ---------------------------------------------------------------------------
;; Write-path hooks. Fire-and-forget; never fail the request; never advance
;; the checkpoint (catch-up! owns it — see futon1b-text/on-append! for why).
;; ---------------------------------------------------------------------------

(defn- hook-failure! [op id t]
  (let [msg (str (.getSimpleName (class t)) ": " (.getMessage t))]
    (swap! !stats (fn [s]
                    (-> s
                        (update :hook-failures inc)
                        (assoc :last-hook-error {:op op :id id :error msg}))))
    (println (str "[hxindex] " op " failed id=" id " — " msg
                  " (recoverable: checkpoint not advanced; the next"
                  " catch-up repairs it)"))
    (flush)))

(defn on-put!
  "Hook after a verified hyperedge put: upsert the doc's rows."
  [doc]
  (when-let [ds (ds*)]
    (future
      (try
        (index-doc! ds doc)
        (catch Throwable t
          (hook-failure! "upsert" (str (:xt/id doc)) t))))))

(defn on-delete!
  "Hook after a verified hyperedge retraction: drop the id's rows."
  [id]
  (when-let [ds (ds*)]
    (future
      (try
        (delete-id! ds id)
        (catch Throwable t
          (hook-failure! "delete" (str id) t))))))

;; ---------------------------------------------------------------------------
;; Catch-up over system time (P0: nothing seeks on system time, every query
;; below scans the hyperedges table — hence the 15-minute default cadence and
;; the expensive-read permit taken by the caller).
;; ---------------------------------------------------------------------------

(def ^:private epoch (Instant/parse "1970-01-01T00:00:00Z"))

(defn- ->odt [t]
  (cond
    (instance? OffsetDateTime t) t
    (instance? Instant t) (.atOffset ^Instant t ZoneOffset/UTC)
    (string? t) (try
                  (->odt (Instant/parse t))
                  (catch java.time.format.DateTimeParseException _
                    ;; str of an OffsetDateTime carries the "[UTC]" zone id
                    (.toOffsetDateTime (java.time.ZonedDateTime/parse t))))
    :else (throw (ex-info (str "hxindex: cannot coerce to OffsetDateTime: " (pr-str t))
                          {:value t}))))

(defn- row-id [row] (or (:xt/id row) (:_id row)))
(defn- row-marker [row] (or (:marker row) (:xt/system-from row) (:xt/system-to row)))

(defn- changed-page
  "One keyset page of hyperedge ids whose system-time column (`_system_from`
   or `_system_to`) advanced past (TS, ID). P0 measured: both columns scan,
   cost is roughly constant in T."
  [node col [ts id] page]
  (fxt/timed-q
   node
   [(str "SELECT _id, " col " AS marker
          FROM hyperedges FOR ALL SYSTEM_TIME
          WHERE " col " > ? OR (" col " = ? AND _id > ?)
          ORDER BY " col ", _id
          LIMIT ?")
    (->odt ts) (->odt ts) (str id) (long page)]))

(defn- repair-ids!
  "Re-read each changed id's CURRENT row (or absence) and upsert/delete the
   sidecar. This is what makes both query legs converge: a superseded
   version (from the _system_from leg) re-upserts, a tombstone deletes."
  [ds node ids]
  (let [ids (vec (distinct ids))
        ;; the two columns the index needs, not SELECT * (hydrate-by-ids):
        ;; 2.5 s against 4.8 s for 200 live ids (claude-12, 2026-09-26)
        present (into {}
                      (comp (mapcat (fn [chunk]
                                      (fxt/timed-q
                                       node
                                       (into [(str "SELECT _id, hx$type AS t, hx$endpoints AS ends
                                                    FROM hyperedges WHERE _id IN ("
                                                   (str/join "," (repeat (count chunk) "?")) ")")]
                                             chunk))))
                            (map (fn [r] [(str (row-id r))
                                          {:hx/type (or (:t r) (:hx/type r))
                                           :hx/endpoints (or (:ends r) (:hx/endpoints r))}])))
                      (partition-all 500 ids))]
    (with-write-tx [tx ds]
      (doseq [id ids]
        (if-let [doc (get present id)]
          (do (jdbc/execute! tx ["DELETE FROM hx_edge WHERE hx_id = ?" id])
              (doseq [[pos end] (map-indexed vector (:hx/endpoints doc))]
                (jdbc/execute! tx ["INSERT INTO hx_edge(hx_id, type, pos, endpoint)
                                    VALUES (?,?,?,?)"
                                   id (str (:hx/type doc)) pos (str end)]))
              (jdbc/execute! tx ["INSERT INTO hx_node(hx_id, type) VALUES (?,?)
                                  ON CONFLICT(hx_id) DO UPDATE SET type=excluded.type"
                                 id (str (:hx/type doc))]))
          (do (jdbc/execute! tx ["DELETE FROM hx_edge WHERE hx_id = ?" id])
              (jdbc/execute! tx ["DELETE FROM hx_node WHERE hx_id = ?" id])))))
    (count ids)))

(defn- latest-tx-id
  "Latest committed transaction the node knows (xt.txs, P0 §4a). Used only
   as a cheap no-work guard; never as the checkpoint."
  [node]
  (:latest (first (fxt/timed-q node ["SELECT MAX(_id) AS latest FROM xt.txs"]))))

(defn- current-page
  "One page of CURRENT hyperedges keyed on _id, with the two columns the index
   needs, so the fill needs no hydration round-trip."
  [node after page]
  (fxt/timed-q
   node
   ["SELECT _id, hx$type AS t, hx$endpoints AS ends FROM hyperedges
     WHERE _id > ? ORDER BY _id LIMIT ?"
    (str after) (long page)]))

(defn- fill!
  "Fill an EMPTY index from the current rows (review of 106fa1d, claude-12).
   Walking the system-time legs from the epoch costs one whole-table scan per
   page over every version ever written (P0: each page scans), i.e. ~1000
   scans at 1000/page for the live store; this walks current rows only, in
   large _id pages, and skips the tombstone leg (nothing to delete in an empty
   index). The checkpoint is the (system time, id) of the store's newest version,
   read BEFORE the fill (one scan): every write committed after it has a later system time and
   is seen by the next catch-up's legs. A clock margin instead would make the
   next upsert leg re-read recent versions and repair deletes it cannot see,
   hiding a missing tombstone leg (found by the bad-case test)."
  [ds node tx-id page]
  (let [;; MAX, not ORDER BY ... DESC LIMIT 1: the descending sort over
        ;; FOR ALL SYSTEM_TIME faults on the live store (XTDB 2.1.0,
        ;; "index: 784264, length: 8 (expected: range(0, 7440))", 2026-09-26)
        ts (:m (first (fxt/timed-q node ["SELECT MAX(_system_from) AS m
                                          FROM hyperedges FOR ALL SYSTEM_TIME"])))
        top (when ts
              {:marker ts
               ;; one transaction's rows; MAX is unsupported on string ids
               :xt/id (row-id (last (fxt/timed-q
                                     node ["SELECT _id FROM hyperedges
                                            FOR ALL SYSTEM_TIME WHERE _system_from = ?
                                            ORDER BY _id"
                                           ts])))})
        n (loop [after "" n 0]
            (let [rows (current-page node after page)]
              (if (empty? rows)
                n
                (do (with-write-tx [tx ds]
                      (doseq [r rows
                              :let [id (str (row-id r))]]
                        (jdbc/execute! tx ["DELETE FROM hx_edge WHERE hx_id = ?" id])
                        (doseq [[pos end] (map-indexed vector (or (:ends r) (:hx/endpoints r)))]
                          (jdbc/execute! tx ["INSERT INTO hx_edge(hx_id, type, pos, endpoint)
                                              VALUES (?,?,?,?)"
                                             id (str (or (:t r) (:hx/type r))) pos (str end)]))
                        (jdbc/execute! tx ["INSERT INTO hx_node(hx_id, type) VALUES (?,?)
                                            ON CONFLICT(hx_id) DO UPDATE SET type=excluded.type"
                                           id (str (or (:t r) (:hx/type r)))])))
                    (recur (str (row-id (last rows))) (+ n (count rows)))))))]
    ;; (ts, id) of the newest version, same keyset as the legs: re-reading it
    ;; would let the upsert leg repair a delete of that doc (see docstring).
    (meta-set! ds "checkpoint-ts" (if top (str (row-marker top)) (str epoch)))
    (meta-set! ds "checkpoint-id" (if top (str (row-id top)) ""))
    (meta-set! ds "last-tx-id" (str tx-id))
    n))

(defonce ^:private !catch-up-running? (atom false))

(defn backfill-hx-node!
  "P3c: populate hx_node for a sidecar built before the table existed. Every
   hyperedge with at least one endpoint is already distinct in hx_edge, so
   those rows are copied LOCALLY (no XTDB round-trip); zero-endpoint
   hyperedges are invisible to hx_edge and are paged out of the store
   (measured 2026-09-28: 0 live, but the census contract must count them).
   Idempotent (ON CONFLICT DO NOTHING); recorded in hx_meta under
   hx-node-backfill so it runs once. catch-up! calls this before anything
   else when the marker is absent — the live server's periodic catch-up
   therefore backfills on its first run after the reload, no restart and no
   manual step."
  [ds node & {:keys [page] :or {page 20000}}]
  (let [started (System/currentTimeMillis)
        copied (:next.jdbc/update-count
                (first (write-locked*
                        (fn []
                          (jdbc/execute! ds ["INSERT INTO hx_node(hx_id, type)
                                              SELECT hx_id, type FROM hx_edge
                                              GROUP BY hx_id
                                              ON CONFLICT(hx_id) DO NOTHING"])))))
        zero (loop [after "" n 0]
               (let [rows (fxt/timed-q
                           node
                           ["SELECT _id, hx$type AS t FROM hyperedges
                             WHERE (hx$endpoints IS NULL
                                    OR cardinality(hx$endpoints) = 0)
                             AND _id > ? ORDER BY _id LIMIT ?"
                            (str after) (long page)])]
                 (if (empty? rows)
                   n
                   (do (with-write-tx [tx ds]
                         (doseq [r rows]
                           (jdbc/execute! tx ["INSERT INTO hx_node(hx_id, type)
                                               VALUES (?,?)
                                               ON CONFLICT(hx_id) DO NOTHING"
                                              (str (row-id r))
                                              (str (or (:t r) (:hx/type r)))])))
                       (recur (str (row-id (last rows))) (+ n (count rows)))))))]
    (meta-set! ds "hx-node-backfill" (str (Instant/now)))
    {:copied-from-hx-edge copied :zero-endpoint zero
     :elapsed-ms (- (System/currentTimeMillis) started)}))

(defn node-index-ready?
  "P3c gate: hx_node (the one-row-per-hyperedge table the census and
   type-only reads count from) has been backfilled. False on a sidecar
   written before P3c until the first catch-up! after the reload."
  []
  (boolean (when-let [ds (ds*)] (meta-get ds "hx-node-backfill"))))

(defn catch-up!
  "Repair the index for everything changed since the checkpoint, then advance
   it. Keyset-paged on (system-time, _id) for both the upsert leg
   (_system_from) and the tombstone leg (_system_to); :tombstones? false
   exists ONLY for the bad-case test. Skipped as {:skipped :no-new-tx} when
   xt.txs shows no transaction newer than the last run (pass :force true to
   override, e.g. from rebuild!). Single-flight like the evidence catch-up."
  [node & {:keys [page fill-page force tombstones?]
           :or {page 1000 fill-page 20000 force false tombstones? true}}]
  (if-not (compare-and-set! !catch-up-running? false true)
    {:skipped :already-running}
    (try
      (let [ds (ds*)
            _ (when-not ds (throw (ex-info "hxindex: no datasource" {})))
            started (System/currentTimeMillis)
            ;; P3c: a sidecar written before hx_node existed gets the table
            ;; backfilled here, BEFORE the no-new-tx skip — the backfill must
            ;; not wait for a store write.
            node-backfill (when (nil? (meta-get ds "hx-node-backfill"))
                            (backfill-hx-node! ds node))
            tx-id (latest-tx-id node)
            prior-tx (some-> (meta-get ds "last-tx-id") Long/parseLong)]
        (cond
          (and (not force) (some? prior-tx) (= prior-tx tx-id))
          (cond-> {:skipped :no-new-tx :tx-id tx-id}
            node-backfill (assoc :hx-node-backfill node-backfill))

          (nil? (meta-get ds "checkpoint-ts"))
          (let [n (fill! ds node tx-id fill-page)
                elapsed (- (System/currentTimeMillis) started)]
            (meta-set! ds "hook-failures-at-catch-up" (:hook-failures @!stats))
            (swap! !stats assoc :last-catch-up
                   {:at (str (Instant/now)) :elapsed-ms elapsed :changed n :fill true})
            (cond-> {:filled n :elapsed-ms elapsed :tx-id tx-id}
              node-backfill (assoc :hx-node-backfill node-backfill)))

          :else
          (let [checkpoint [(or (meta-get ds "checkpoint-ts") (str epoch))
                            (or (meta-get ds "checkpoint-id") "")]
                upserted (loop [after checkpoint changed 0]
                           (let [rows (changed-page node "_system_from" after page)]
                             (if (empty? rows)
                               changed
                               (do (repair-ids! ds node (map row-id rows))
                                   (let [lst (last rows)
                                         hi [(str (row-marker lst)) (str (row-id lst))]]
                                     (meta-set! ds "checkpoint-ts" (first hi))
                                     (meta-set! ds "checkpoint-id" (second hi))
                                     (recur hi (+ changed (count rows)))))))) ;; ids superseded by an upsert are re-upserted by repair-ids!;
                ;; the tombstone leg exists for DELETEs, which _system_from
                ;; never sees (P0 §3).
                deleted (if-not tombstones?
                          0
                          (loop [after checkpoint changed 0]
                            (let [rows (changed-page node "_system_to" after page)]
                              (if (empty? rows)
                                changed
                                (let [lst (last rows)]
                                  (repair-ids! ds node (map row-id rows))
                                  (recur [(str (row-marker lst)) (str (row-id lst))]
                                         (+ changed (count rows))))))))]
            (meta-set! ds "last-tx-id" (str tx-id))
            (meta-set! ds "hook-failures-at-catch-up" (:hook-failures @!stats))
            (let [elapsed (- (System/currentTimeMillis) started)
                  res (cond-> {:changed (+ upserted deleted)
                               :upsert-leg upserted :tombstone-leg deleted
                               :elapsed-ms elapsed :tx-id tx-id}
                        node-backfill (assoc :hx-node-backfill node-backfill))]
              (swap! !stats assoc :last-catch-up
                     {:at (str (Instant/now)) :elapsed-ms elapsed
                      :changed (:changed res)})
              res))))
      (finally (reset! !catch-up-running? false)))))

(defn rebuild!
  "Full rebuild from an empty index. The oracle is the store itself: the
   checkpoint is rewound to the epoch and catch-up re-reads every id's
   current row, so only current content lands in the index."
  [node & {:keys [page] :or {page 1000}}]
  (let [ds (ds*)
        started (System/currentTimeMillis)]
    (with-write-tx [tx ds]
      (jdbc/execute! tx ["DELETE FROM hx_edge"])
      (jdbc/execute! tx ["DELETE FROM hx_node"])
      (jdbc/execute! tx ["DELETE FROM hx_meta WHERE k IN ('checkpoint-ts','checkpoint-id','last-tx-id','hx-node-backfill')"]))
    (assoc (catch-up! node :page page :force true)
           :total-elapsed-ms (- (System/currentTimeMillis) started))))

;; ---------------------------------------------------------------------------
;; Periodic repair loop (boot + every FUTON1B_HX_CATCHUP_MS, default 15 min).
;; ---------------------------------------------------------------------------

(defonce ^:private !scheduler (atom nil))

(defn- daemon-factory []
  (reify ThreadFactory
    (newThread [_ r]
      (doto (Thread. ^Runnable r "hx-periodic-catch-up")
        (.setDaemon true)))))

(defn start-periodic-catch-up!
  "Run catch-up! every INTERVAL-MS (default FUTON1B_HX_CATCHUP_MS / 15 min).
   CATCH-UP-FN overrides the run (the server wraps it in an expensive-read
   permit); it defaults to (catch-up! node). Daemon, idempotent,
   fixed-delay; <= 0 disables."
  [node & {:keys [interval-ms page catch-up-fn skip-retry-ms]
           :or {interval-ms default-catch-up-interval-ms page 1000
                skip-retry-ms 60000}}]
  (cond
    (not (pos? (long interval-ms))) {:ok true :periodic false :reason :disabled}
    (some? @!scheduler) {:ok true :periodic true :reason :already-running}
    :else
    (let [run (or catch-up-fn #(catch-up! node :page page))
          sched (Executors/newSingleThreadScheduledExecutor (daemon-factory))]
      (.scheduleWithFixedDelay
       sched
       ^Runnable
       (fn []
         (try
           ;; A skip (the server's wrapper found no free expensive-read
           ;; permit within 3 s) used to leave no trace, and 11:45-13:00
           ;; 2026-09-26 recorded no catch-up at all. Record every outcome
           ;; and retry a skipped run each minute, up to 10 times, rather
           ;; than waiting a whole interval.
           (let [{:keys [changed skipped]}
                 (loop [tries 1]
                   (let [r (run)]
                     (swap! !stats assoc :last-periodic
                            {:at (str (Instant/now)) :tries tries
                             :result (select-keys r [:changed :skipped :elapsed-ms])})
                     (if (and (:skipped r) (< tries 10))
                       (do (Thread/sleep (long skip-retry-ms)) (recur (inc tries)))
                       r)))]
             (when (and (nil? skipped) (pos? (long (or changed 0))))
               (println (str "[hxindex] periodic catch-up repaired " changed " id(s)"))
               (flush)))
           (catch InterruptedException _
             (.interrupt (Thread/currentThread)))
           (catch Throwable t
             (println (str "[hxindex] periodic catch-up failed: "
                           (.getSimpleName (class t)) ": " (.getMessage t)))
             (flush))))
       (long interval-ms) (long interval-ms) TimeUnit/MILLISECONDS)
      (reset! !scheduler sched)
      {:ok true :periodic true :interval-ms interval-ms})))

(defn stop-periodic-catch-up! []
  (when-let [^ScheduledExecutorService s @!scheduler]
    (.shutdownNow s)
    (reset! !scheduler nil)
    {:ok true :periodic false}))

;; ---------------------------------------------------------------------------
;; P2 read side: serve type+endpoint reads from the index (DESIGN §7). The
;; graph read path re-checks every candidate against XTDB before returning
;; it; this namespace only gates and supplies candidate ids.
;; ---------------------------------------------------------------------------

;; FUTON1B_HX_READS, default on. An atom (not a def from the env) so a test
;; or an operator in a REPL can flip it without a restart.
(defonce !reads-enabled
  (atom (not= "false" (System/getenv "FUTON1B_HX_READS"))))

(defn reads-usable?
  "P2 gate (DESIGN §7): the index may serve a read when reads are enabled, a
   checkpoint exists (the fill has run) and NO write hook has failed since
   the last catch-up. A failed hook means a hyperedge written after the
   checkpoint may be missing from the candidates — which the XTDB re-check
   cannot see (it only drops stale positives) — so the read must fall back
   to the scan path."
  []
  (let [ds (ds*)]
    (boolean
     (and @!reads-enabled
          ds
          (meta-get ds "checkpoint-ts")
          (let [stored (meta-get ds "hook-failures-at-catch-up")]
            (or (nil? stored)
                (= (try (Long/parseLong stored) (catch Exception _ -1))
                   (long (:hook-failures @!stats)))))))))

(defn checkpoint
  "The catch-up watermark the served candidates are complete up to, for the
   :hx-index response annotation."
  []
  (when-let [ds (ds*)]
    {:ts (meta-get ds "checkpoint-ts")
     :id (meta-get ds "checkpoint-id")}))

(defn type-end-candidates
  "Ordered distinct hx_ids indexed under TYPE (stored as `(str keyword)`,
   colon included) at ANY of ENDPOINTS, keyset-paged on hx_id > AFTER
   (\"\" pages from the start), at most FETCH ids. One seek per endpoint on
   the (type, endpoint) index, merged and re-sorted in memory — the same
   per-target fetch + merge the XTDB scan path does."
  [{:keys [type endpoints after fetch]}]
  (when-let [ds (ds*)]
    (let [t (str type)
          per-target
          (fn [e]
            (jdbc/execute! ds
                           ["SELECT DISTINCT hx_id FROM hx_edge
                             WHERE type = ? AND endpoint = ? AND hx_id > ?
                             ORDER BY hx_id LIMIT ?"
                            t (str e) (str (or after "")) (long fetch)]
                           unqualified))]
      (->> endpoints
           (mapcat per-target)
           (map :hx_id)
           (distinct)
           (sort)
           (take (long fetch))
           (vec)))))

(defn prefix-successor
  "P3d: the strict upper bound of the string range covering PREFIX: PREFIX
   with its last code point incremented (\"ab\" → \"ac\", \"dir:x→\" →
   \"dir:x↓\", U+2192 → U+2193). A range [PREFIX, (prefix-successor PREFIX)) contains exactly
   the strings equal to PREFIX or beginning with it — including non-ASCII
   endpoints, which a naive (str PREFIX \"z\") bound would wrongly exclude
   (any suffix char above \\z, e.g. → U+2192). Callers refuse the empty
   prefix before reaching here."
  [^String prefix]
  (let [len (.length prefix)
        cp (.codePointBefore prefix len)
        head (subs prefix 0 (- len (Character/charCount cp)))
        upper (String. (int-array [(inc cp)]) 0 1)]
    (str head upper)))

(defn type-end-prefix-candidates
  "P3d: ordered distinct hx_ids indexed under TYPE (stored as `(str keyword)`,
   colon included) at ANY endpoint equal to or beginning with PREFIX,
   keyset-paged on hx_id > AFTER (\"\" pages from the start), at most FETCH
   ids. One ordered range scan over the (type, endpoint) index; DISTINCT
   collapses rows for hyperedges with several matching endpoints, so a page
   may hold fewer than FETCH rows — the caller re-pulls like P2b/P3."
  [{:keys [type prefix after fetch]}]
  (when-let [ds (ds*)]
    (->> (jdbc/execute! ds
                        ["SELECT DISTINCT hx_id FROM hx_edge
                          WHERE type = ? AND endpoint >= ? AND endpoint < ?
                          AND hx_id > ?
                          ORDER BY hx_id LIMIT ?"
                         (str type) prefix (prefix-successor prefix)
                         (str (or after "")) (long fetch)]
                        unqualified)
         (map :hx_id)
         (vec))))

(defn type-end-prefix-count
  "P3d include-total: hyperedges indexed under TYPE at any endpoint equal to
   or beginning with PREFIX — count(DISTINCT hx_id) over the prefix range.
   Exact only up to the checkpoint plus hook repairs, like type-count."
  [type prefix]
  (when-let [ds (ds*)]
    (:count (first (jdbc/execute! ds
                                  ["SELECT count(DISTINCT hx_id) AS count
                                    FROM hx_edge
                                    WHERE type = ? AND endpoint >= ? AND endpoint < ?"
                                   (str type) prefix (prefix-successor prefix)]
                                  unqualified)))))

(defn type-candidates
  "P3: ordered distinct hx_ids indexed under TYPE (stored as `(str keyword)`,
   colon included), keyset-paged on hx_id > AFTER (\"\" pages from the start),
   at most FETCH ids — one ordered scan over the (type, hx_id) index with
   adjacent-duplicate dedup. The cursor contract is the scan path's: AFTER is
   the last xt/id of the previous window, compared as TEXT, the same
   lexicographic order XTDB's `order-by xt/id` applies to string ids.
   P3c: reads hx_node (one row per hyperedge), so zero-endpoint hyperedges
   ARE candidates — they are real members of the type."
  [{:keys [type after fetch]}]
  (when-let [ds (ds*)]
    (->> (jdbc/execute! ds
                        ["SELECT hx_id FROM hx_node
                          WHERE type = ? AND hx_id > ?
                          ORDER BY hx_id LIMIT ?"
                         (str type) (str (or after "")) (long fetch)]
                        unqualified)
         (map :hx_id)
         (vec))))

(defn type-count
  "P3 include-total / P3c census: hyperedges indexed under TYPE, read from
   hx_node (one row per hyperedge), so zero-endpoint hyperedges count.
   Matches XTDB's type count when the index is caught up; exact only up to
   the checkpoint plus hook repairs — a hyperedge deleted unhooked since the
   last catch-up still counts until the next catch-up removes it."
  [type]
  (when-let [ds (ds*)]
    (:count (first (jdbc/execute! ds
                                  ["SELECT count(*) AS count
                                    FROM hx_node WHERE type = ?"
                                   (str type)]
                                  unqualified)))))

;; ---------------------------------------------------------------------------
;; P4: oracle — the sidecar checked against XTDB itself (DESIGN §7 item 4).
;; ---------------------------------------------------------------------------

(defn- xtdb-type-counts*
  "The census scan branch's own query, called DIRECTLY (graph/census now
   answers from the sidecar and would make the oracle compare the index
   with itself)."
  [node]
  (into {}
        (map (fn [row] [(str (:type row)) (long (:n row))]))
        (fxt/timed-q node ["SELECT hx$type AS type, count(*) AS n
                            FROM hyperedges GROUP BY hx$type"])))

(defn- sidecar-type-counts* [ds]
  (into {}
        (map (fn [row] [(:type row) (long (:n row))]))
        (jdbc/execute! ds ["SELECT type, count(*) AS n FROM hx_node GROUP BY type"]
                       unqualified)))

(defn- count-diffs
  "Types whose hx_node count differs from the XTDB count, over the union of
   both key sets (a type present on only one side is a mismatch)."
  [sidecar xtdb]
  (vec
   (keep (fn [t]
           (let [s (long (get sidecar t 0))
                 x (long (get xtdb t 0))]
             (when (not= s x)
               {:type t :sidecar s :xtdb x})))
         (into (sorted-set) (concat (keys sidecar) (keys xtdb))))))

(defn- endpoint-sample-check
  "Compare hx_edge rows (ORDER BY pos) with the XTDB document's
   hx$endpoints for a sample of hx_node ids per type. A sidecar id with no
   XTDB doc (xt nil) or an XTDB doc with no sidecar rows is a mismatch.
   Returns {:sampled n :mismatches [...]}."
  [ds node sample-per-type]
  (let [types (map :type (jdbc/execute! ds ["SELECT DISTINCT type FROM hx_node ORDER BY type"]
                                        unqualified))
        samples (into {}
                      (map (fn [t]
                             [t (mapv :hx_id
                                      (jdbc/execute! ds ["SELECT hx_id FROM hx_node
                                                          WHERE type = ? ORDER BY hx_id LIMIT ?"
                                                         t (long sample-per-type)]
                                                     unqualified))]))
                      types)]
    {:sampled (reduce + 0 (map (comp count val) samples))
     :mismatches
     (vec
      (for [[t ids] samples
            id ids
            :let [side (mapv :endpoint
                             (jdbc/execute! ds ["SELECT endpoint FROM hx_edge
                                                 WHERE hx_id = ? ORDER BY pos" id]
                                            unqualified))
                  row (first (fxt/timed-q node ["SELECT hx$endpoints AS ends
                                                 FROM hyperedges WHERE _id = ?" id]))
                  xt (when row (mapv str (:ends row)))]
            :when (not= side xt)]
        {:type t :id id :sidecar side :xtdb xt}))}))

(defn oracle
  "P4 (DESIGN §7 item 4): compare the sidecar against XTDB itself. Per type,
   the hx_node count vs the census scan query (called directly, NOT via
   graph/census, which now answers from the sidecar); then, for a sample of
   ids per type, the ordered hx_edge endpoints vs the document's
   hx/endpoints. Read-only with respect to the INDEX unless :recheck is on.

   Counts can legitimately differ when a write lands between the two reads.
   Handling: when the first comparison shows a difference, run ONE catch-up!
   (:force, so a no-new-tx skip cannot mask it) and compare again — a
   difference that drains is reported under :raced with its before values,
   one that survives is a real :mismatch. Damage done behind the index's
   back (a row deleted or planted directly) survives catch-up, which only
   re-reads ids changed since the checkpoint, so it is reported, not
   repaired. Pass :recheck false for a strictly read-only run (any
   difference is then reported as a mismatch without the drain attempt).

   Records the result in hx-stats under :last-oracle."
  [node & {:keys [sample-per-type recheck]
           :or {sample-per-type 25 recheck true}}]
  (let [started (System/currentTimeMillis)
        ds (ds*)
        _ (when-not ds (throw (ex-info "hxindex: no sidecar datasource" {})))
        compare-counts (fn [] (count-diffs (sidecar-type-counts* ds)
                                           (xtdb-type-counts* node)))
        first-pass (compare-counts)
        {:keys [mismatches raced]}
        (if (and recheck (seq first-pass))
          (do (catch-up! node :force true)
              (let [second-pass (compare-counts)
                    still (set (map :type second-pass))]
                {:mismatches (mapv #(assoc % :rechecked true) second-pass)
                 :raced (vec (remove #(still (:type %)) first-pass))}))
          {:mismatches (if recheck first-pass
                           (mapv #(assoc % :rechecked false) first-pass))
           :raced []})
        end-check (endpoint-sample-check ds node sample-per-type)
        result {:types-checked (count (into #{} (concat (keys (sidecar-type-counts* ds))
                                                        (keys (xtdb-type-counts* node)))))
                :mismatches mismatches
                :raced raced
                :sampled-ids (:sampled end-check)
                :endpoint-mismatches (:mismatches end-check)
                :checkpoint (checkpoint)
                :at (str (Instant/now))
                :elapsed-ms (- (System/currentTimeMillis) started)}]
    (swap! !stats assoc :last-oracle result)
    result))

;; ---------------------------------------------------------------------------
;; Stats.
;; ---------------------------------------------------------------------------

(defn hx-stats
  "Index health for the hyperedge sidecar: row count per type, checkpoint,
   last catch-up time + duration, cumulative hook failures."
  []
  (let [ds (ds*)
        per-type (when ds
                   (jdbc/execute! ds ["SELECT n.type, n.hyperedges,
                                       COALESCE(e.rows, 0) AS rows
                                       FROM (SELECT type, count(*) AS hyperedges
                                             FROM hx_node GROUP BY type) n
                                       LEFT JOIN (SELECT type, count(*) AS rows
                                                  FROM hx_edge GROUP BY type) e
                                       ON e.type = n.type
                                       ORDER BY n.hyperedges DESC"]
                                  unqualified))]
    (assoc @!stats
           :ready (some? ds)
           :per-type per-type
           :hx-node-hyperedges (when ds
                                 (:n (first (jdbc/execute! ds ["SELECT count(*) AS n FROM hx_node"]
                                                           unqualified))))
           :hx-node-backfilled (node-index-ready?)
           :checkpoint (when ds {:ts (meta-get ds "checkpoint-ts")
                                 :id (meta-get ds "checkpoint-id")
                                 :last-tx-id (meta-get ds "last-tx-id")})
           :hook-failures-window :cumulative-since-process-start
           :periodic? (some? @!scheduler)
           :catch-up-running? @!catch-up-running?)))
