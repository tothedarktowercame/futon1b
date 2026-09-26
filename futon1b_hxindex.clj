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
   "CREATE TABLE IF NOT EXISTS hx_meta (k TEXT PRIMARY KEY, v TEXT)"])

(defn- ds* [] @text/!ds)

(defn- meta-get [ds k]
  (:v (first (jdbc/execute! ds ["SELECT v FROM hx_meta WHERE k = ?" k]
                            unqualified))))

(defn- meta-set! [ds k v]
  (jdbc/execute! ds ["INSERT INTO hx_meta(k,v) VALUES(?,?)
                      ON CONFLICT(k) DO UPDATE SET v=excluded.v" k (str v)]))

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
  "Delete any rows for the doc's hx_id, then insert one row per endpoint
   position. One SQLite transaction, so readers never see a half-replaced
   hyperedge (positions may change between versions)."
  [ds doc]
  (let [id (str (:xt/id doc))
        t (str (:hx/type doc))
        endpoints (:hx/endpoints doc)]
    (jdbc/with-transaction [tx ds]
      (jdbc/execute! tx ["DELETE FROM hx_edge WHERE hx_id = ?" id])
      (doseq [[pos end] (map-indexed vector endpoints)]
        (jdbc/execute! tx ["INSERT INTO hx_edge(hx_id, type, pos, endpoint)
                            VALUES (?,?,?,?)" id t pos (str end)]))))
  (count (:hx/endpoints doc)))

(defn delete-id! [ds id]
  (jdbc/execute! ds ["DELETE FROM hx_edge WHERE hx_id = ?" (str id)]))

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
    (jdbc/with-transaction [tx ds]
      (doseq [id ids]
        (if-let [doc (get present id)]
          (do (jdbc/execute! tx ["DELETE FROM hx_edge WHERE hx_id = ?" id])
              (doseq [[pos end] (map-indexed vector (:hx/endpoints doc))]
                (jdbc/execute! tx ["INSERT INTO hx_edge(hx_id, type, pos, endpoint)
                                    VALUES (?,?,?,?)"
                                   id (str (:hx/type doc)) pos (str end)])))
          (jdbc/execute! tx ["DELETE FROM hx_edge WHERE hx_id = ?" id]))))
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
                (do (jdbc/with-transaction [tx ds]
                      (doseq [r rows
                              :let [id (str (row-id r))]]
                        (jdbc/execute! tx ["DELETE FROM hx_edge WHERE hx_id = ?" id])
                        (doseq [[pos end] (map-indexed vector (or (:ends r) (:hx/endpoints r)))]
                          (jdbc/execute! tx ["INSERT INTO hx_edge(hx_id, type, pos, endpoint)
                                              VALUES (?,?,?,?)"
                                             id (str (or (:t r) (:hx/type r))) pos (str end)]))))
                    (recur (str (row-id (last rows))) (+ n (count rows)))))))]
    ;; (ts, id) of the newest version, same keyset as the legs: re-reading it
    ;; would let the upsert leg repair a delete of that doc (see docstring).
    (meta-set! ds "checkpoint-ts" (if top (str (row-marker top)) (str epoch)))
    (meta-set! ds "checkpoint-id" (if top (str (row-id top)) ""))
    (meta-set! ds "last-tx-id" (str tx-id))
    n))

(defonce ^:private !catch-up-running? (atom false))

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
            tx-id (latest-tx-id node)
            prior-tx (some-> (meta-get ds "last-tx-id") Long/parseLong)]
        (cond
          (and (not force) (some? prior-tx) (= prior-tx tx-id))
          {:skipped :no-new-tx :tx-id tx-id}

          (nil? (meta-get ds "checkpoint-ts"))
          (let [n (fill! ds node tx-id fill-page)
                elapsed (- (System/currentTimeMillis) started)]
            (meta-set! ds "hook-failures-at-catch-up" (:hook-failures @!stats))
            (swap! !stats assoc :last-catch-up
                   {:at (str (Instant/now)) :elapsed-ms elapsed :changed n :fill true})
            {:filled n :elapsed-ms elapsed :tx-id tx-id})

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
                  res {:changed (+ upserted deleted)
                       :upsert-leg upserted :tombstone-leg deleted
                       :elapsed-ms elapsed :tx-id tx-id}]
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
    (jdbc/execute! ds ["DELETE FROM hx_edge"])
    (jdbc/execute! ds ["DELETE FROM hx_meta WHERE k IN ('checkpoint-ts','checkpoint-id','last-tx-id')"])
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
  [node & {:keys [interval-ms page catch-up-fn]
           :or {interval-ms default-catch-up-interval-ms page 1000}}]
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
           (let [{:keys [changed skipped]} (run)]
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

;; ---------------------------------------------------------------------------
;; Stats.
;; ---------------------------------------------------------------------------

(defn hx-stats
  "Index health for the hyperedge sidecar: row count per type, checkpoint,
   last catch-up time + duration, cumulative hook failures."
  []
  (let [ds (ds*)
        per-type (when ds
                   (jdbc/execute! ds ["SELECT type, count(*) AS rows,
                                       count(DISTINCT hx_id) AS hyperedges
                                       FROM hx_edge GROUP BY type
                                       ORDER BY rows DESC"]
                                  unqualified))]
    (assoc @!stats
           :ready (some? ds)
           :per-type per-type
           :checkpoint (when ds {:ts (meta-get ds "checkpoint-ts")
                                 :id (meta-get ds "checkpoint-id")
                                 :last-tx-id (meta-get ds "last-tx-id")})
           :hook-failures-window :cumulative-since-process-start
           :periodic? (some? @!scheduler)
           :catch-up-running? @!catch-up-running?)))
