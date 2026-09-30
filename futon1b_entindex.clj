(ns futon1b-entindex
  "Current-entity SQLite sidecar (M-entities-sidecar P1).

  This namespace maintains but does not serve an entity index. Verified
  writes call on-put!/on-delete! and wait for the returned future. Hook
  failures never fail the store write and never advance the checkpoint.
  catch-up! repairs missed writes from both system-time legs after an
  operator has explicitly run fill!. Each store page is evaluated through
  :with-page-permit, allowing the server to acquire and release one expensive
  read permit per bounded page. fill! is deliberately never called by init!,
  catch-up!, or the periodic loop.

  ent_node.doc is exactly the hydrated :entities document, encoded with
  futon1b-text/encode-doc. The index is derived and may be rebuilt."
  (:require [clojure.string :as str]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [futon1b-hxindex :as hx]
            [futon1b-text :as text]
            [futon1b-xt :as fxt])
  (:import [java.time Instant OffsetDateTime ZoneOffset]
           [java.util.concurrent Executors ThreadFactory TimeUnit]))

(def ^:private unqualified {:builder-fn rs/as-unqualified-maps})
(def ^:private epoch (Instant/parse "1970-01-01T00:00:00Z"))

(defonce !stats (atom {:hook-failures 0 :last-hook-error nil :last-catch-up nil}))
(defonce !reads-enabled
  (atom (not= "false" (System/getenv "FUTON1B_ENT_READS"))))

(defonce ^:private !with-page-permit
  ;; Offline callers have no HTTP admission pool. The serving JVM installs
  ;; its one-page permit wrapper during sidecar initialization.
  (atom (fn [f] (f))))

(defn set-page-permit!
  "Install the serving JVM's expensive-read permit wrapper."
  [f]
  (reset! !with-page-permit f)
  nil)

(def default-catch-up-interval-ms
  (or (some-> (System/getenv "FUTON1B_ENT_CATCHUP_MS")
              (as-> s (try (Long/parseLong s) (catch Exception _ nil))))
      900000))

(def ^:private ddl
  ["CREATE TABLE IF NOT EXISTS ent_node (
      id TEXT PRIMARY KEY, type TEXT NOT NULL, doc BLOB NOT NULL)"
   "CREATE INDEX IF NOT EXISTS ent_type_id ON ent_node(type, id)"
   "CREATE TABLE IF NOT EXISTS ent_meta (k TEXT PRIMARY KEY, v TEXT)"])

(defn- ds* [] @text/!ds)

(defn- write-locked* [f]
  ;; Reuse the hyperedge sidecar's bounded SQLITE_BUSY policy and the shared
  ;; futon1b-text write lock. Visibility of this helper is widened in P1;
  ;; its hyperedge behaviour is unchanged.
  (hx/write-locked* f))

(defmacro ^:private ^{:clj-kondo/lint-as 'clojure.core/with-open} with-write-tx
  [[tx ds] & body]
  `(write-locked* (fn [] (jdbc/with-transaction [~tx ~ds] ~@body))))

(defn- meta-get [ds k]
  (:v (first (jdbc/execute! ds ["SELECT v FROM ent_meta WHERE k = ?" k]
                            unqualified))))

(defn- meta-set! [ds k v]
  (write-locked*
   #(jdbc/execute! ds ["INSERT INTO ent_meta(k,v) VALUES(?,?)
                        ON CONFLICT(k) DO UPDATE SET v=excluded.v" k (str v)])))

(defn init!
  "Create entity sidecar tables. Does not read XTDB or fill the index."
  ([] (init! {:ds (ds*)}))
  ([{:keys [ds]}]
   (when-not ds
     (throw (ex-info "entindex: no sidecar datasource (text/init! first)" {})))
   (doseq [stmt ddl] (jdbc/execute! ds [stmt]))
   {:ok true}))

(defn index-doc! [ds doc]
  (let [id (str (:xt/id doc))
        t (str (:entity/type doc))]
    ;; A typeless entity is stored under type "" here exactly as repair-ids!
    ;; and fill! store it; throwing would count a hook failure and close the
    ;; gate until the next catch-up for a write that is not an error.
    (when (str/blank? id)
      (throw (ex-info "entindex: entity requires xt/id" {:doc doc})))
    (write-locked*
     #(jdbc/execute! ds ["INSERT INTO ent_node(id,type,doc) VALUES(?,?,?)
                          ON CONFLICT(id) DO UPDATE SET
                          type=excluded.type, doc=excluded.doc"
                         id t (text/encode-doc doc)])))
  doc)

(defn delete-id! [ds id]
  (write-locked* #(jdbc/execute! ds ["DELETE FROM ent_node WHERE id = ?" (str id)])))

(defn- hook-failure! [op id t]
  (let [msg (str (.getSimpleName (class t)) ": " (.getMessage t))]
    (swap! !stats (fn [s]
                    (-> s
                        (update :hook-failures inc)
                        (assoc :last-hook-error {:op op :id (str id) :error msg}))))
    (println (str "[entindex] " op " failed id=" id " — " msg
                  " (recoverable: checkpoint not advanced; catch-up repairs it)"))
    (flush)))

(defn on-put!
  "After a verified entity put, asynchronously upsert its complete document.
  The returned future catches and counts failures; callers may dereference it
  so a successful response observes the row without inheriting hook failure."
  [doc]
  (when-let [ds (ds*)]
    (future
      (try (index-doc! ds doc)
           (catch Throwable t (hook-failure! :upsert (:xt/id doc) t))))))

(defn on-delete!
  "After a verified entity delete, asynchronously remove its sidecar row."
  [id]
  (when-let [ds (ds*)]
    (future
      (try (delete-id! ds id)
           (catch Throwable t (hook-failure! :delete id t))))))

(defn await-hook!
  "Wait for a hook when a sidecar is attached. Hook futures do not throw."
  [hook]
  (when hook @hook)
  nil)

(defn- ->odt [t]
  (cond
    (instance? OffsetDateTime t) t
    (instance? Instant t) (.atOffset ^Instant t ZoneOffset/UTC)
    (string? t) (try (->odt (Instant/parse t))
                     (catch java.time.format.DateTimeParseException _
                       (.toOffsetDateTime (java.time.ZonedDateTime/parse t))))
    :else (throw (ex-info "entindex: invalid system time" {:value t}))))

(defn- row-id [row] (or (:xt/id row) (:_id row)))
(defn- row-marker [row] (or (:marker row) (:xt/system-from row) (:xt/system-to row)))
(defn- permit-call [with-page-permit f] (with-page-permit f))

(defn- changed-page [node col [ts id] page with-page-permit]
  (permit-call
   with-page-permit
   #(fxt/timed-q
     node
     [(str "SELECT _id, " col " AS marker
            FROM entities FOR ALL SYSTEM_TIME
            WHERE " col " > ? OR (" col " = ? AND _id > ?)
            ORDER BY " col ", _id LIMIT ?")
      (->odt ts) (->odt ts) (str id) (long page)])))

(defn- latest-tx-id [node with-page-permit]
  (:latest (first (permit-call with-page-permit
                               #(fxt/timed-q node ["SELECT MAX(_id) AS latest FROM xt.txs"])))))

(defn- repair-ids! [ds node ids with-page-permit]
  (let [ids (vec (distinct (map str ids)))
        docs (permit-call with-page-permit #(fxt/hydrate-by-ids node :entities ids))
        present (into {} (map (juxt (comp str :xt/id) identity)) docs)]
    (with-write-tx [tx ds]
      (doseq [id ids]
        (if-let [doc (get present id)]
          (jdbc/execute! tx ["INSERT INTO ent_node(id,type,doc) VALUES(?,?,?)
                              ON CONFLICT(id) DO UPDATE SET
                              type=excluded.type, doc=excluded.doc"
                             id (str (:entity/type doc)) (text/encode-doc doc)])
          (jdbc/execute! tx ["DELETE FROM ent_node WHERE id = ?" id]))))
  (count ids)))

(defn- newest-version
  "(system time, id) of the store's newest entity version, or nil when empty."
  [node with-page-permit]
  (permit-call
   with-page-permit
   #(let [ts (:m (first (fxt/timed-q node ["SELECT MAX(_system_from) AS m
                                           FROM entities FOR ALL SYSTEM_TIME"])))]
      (when ts
        {:marker ts
         :xt/id (row-id (last (fxt/timed-q
                               node ["SELECT _id FROM entities FOR ALL SYSTEM_TIME
                                      WHERE _system_from = ? ORDER BY _id" ts])))}))))

(defonce !fill-progress (atom nil))
(defonce ^:private !fill-stop? (atom false))

(defn stop-fill!
  "Ask a running fill! to stop after its current chunk. The index stays
  unfilled (no checkpoint), so fill! can be run again."
  []
  (reset! !fill-stop? true)
  {:ok true :stopping (some? @!fill-progress)})

(defn fill!
  "Fill a never-filled entity index. An explicit operator action.

  Reads every current id once on narrow columns, then hydrates them in
  CHUNK-sized `_id IN` reads, sleeping PAUSE-MS between chunks so other
  readers get a permit. Each read goes through WITH-PAGE-PERMIT (direct
  execution for offline tests). Measured live 2026-09-30 under load: the
  56,499 ids in 14.8 s, and 500 ids hydrated in 38.8 s. An `IN` read costs
  about the same at any N (TN-entities-speedups-2026-09-26 §1), so chunks
  are large. P1's `SELECT * ... ORDER BY _id LIMIT 1000` pages were
  cancelled at the 60 s deadline: the ordering sorts every body in the
  table for each page.

  The index may already hold rows: the write hooks run from the moment the
  namespace is loaded, before anyone fills. A chunk read before a later hook
  write can overwrite that newer row, so the fill leaves the read gate CLOSED;
  the first catch-up! replays every version after the pre-fill checkpoint and
  is what opens it. Progress is in !fill-progress (and ent-stats)."
  [node & {:keys [chunk pause-ms with-page-permit]
           :or {chunk 2000 pause-ms 2000 with-page-permit @!with-page-permit}}]
  (when-not (<= 1 (long chunk) 2000)
    (throw (ex-info "entindex: fill chunk must be 1..2000" {:chunk chunk})))
  (let [ds (or (ds*) (throw (ex-info "entindex: no datasource" {})))
        filled-at (meta-get ds "checkpoint-ts")]
    (when filled-at
      (throw (ex-info "entindex: already filled; use catch-up! (or rebuild from empty)"
                      {:checkpoint-ts filled-at})))
    (reset! !fill-stop? false)
    (try
      (let [started (System/currentTimeMillis)
            tx-id (latest-tx-id node with-page-permit)
            top (newest-version node with-page-permit)
            ids (permit-call with-page-permit
                             #(mapv (comp str row-id)
                                    (fxt/timed-q node ["SELECT _id FROM entities"] 120)))
            chunks (partition-all chunk ids)
            _ (reset! !fill-progress {:ids (count ids) :chunks (count chunks)
                                      :done 0 :written 0 :started-at (str (Instant/now))})
            n (loop [[c & more] chunks n 0 i 0]
                (cond
                  (nil? c) n
                  @!fill-stop? (throw (ex-info "entindex: fill stopped by stop-fill!"
                                               {:entindex/error :fill-stopped :written n}))
                  :else
                  (let [docs (permit-call with-page-permit
                                          #(fxt/hydrate-by-ids node :entities (vec c)))]
                    (with-write-tx [tx ds]
                      (doseq [doc docs]
                        (jdbc/execute! tx ["INSERT INTO ent_node(id,type,doc) VALUES(?,?,?)
                                           ON CONFLICT(id) DO UPDATE SET
                                           type=excluded.type, doc=excluded.doc"
                                           (str (row-id doc)) (str (:entity/type doc))
                                           (text/encode-doc doc)])))
                    (swap! !fill-progress assoc :done (inc i) :written (+ n (count docs))
                           :elapsed-ms (- (System/currentTimeMillis) started))
                    (when (and (seq more) (pos? (long pause-ms)))
                      (Thread/sleep (long pause-ms)))
                    (recur more (+ n (count docs)) (inc i)))))]
        (meta-set! ds "checkpoint-ts" (if top (str (row-marker top)) (str epoch)))
        (meta-set! ds "checkpoint-id" (if top (str (row-id top)) ""))
        (meta-set! ds "last-tx-id" (str tx-id))
        ;; Matches no failure count, so reads-usable? stays false until the
        ;; first catch-up! records a real one (see the docstring).
        (meta-set! ds "hook-failures-at-catch-up" -1)
        (swap! !fill-progress assoc :finished-at (str (Instant/now)))
        {:filled n :tx-id tx-id :gate :closed-until-first-catch-up
         :elapsed-ms (- (System/currentTimeMillis) started)})
      (finally (reset! !fill-stop? false)))))

(defonce ^:private !catch-up-running? (atom false))
(declare !scheduler)

(defn catch-up!
  "Repair an explicitly filled index from _system_from and _system_to.
  :tombstones? false exists only to demonstrate the delete bad case."
  [node & {:keys [page force tombstones? with-page-permit]
           :or {page 1000 force false tombstones? true
                with-page-permit @!with-page-permit}}]
  (if-not (compare-and-set! !catch-up-running? false true)
    {:skipped :already-running}
    (try
      (let [ds (or (ds*) (throw (ex-info "entindex: no datasource" {})))
            started (System/currentTimeMillis)]
        (if-not (meta-get ds "checkpoint-ts")
          {:skipped :not-filled}
          (let [failures-at-start (:hook-failures @!stats)
                tx-id (latest-tx-id node with-page-permit)
                prior (some-> (meta-get ds "last-tx-id") Long/parseLong)]
            (if (and (not force) (some? prior) (= prior tx-id))
              {:skipped :no-new-tx :tx-id tx-id}
              (let [checkpoint [(meta-get ds "checkpoint-ts")
                                (or (meta-get ds "checkpoint-id") "")]
                    ;; The next checkpoint is read BEFORE the legs run. Every
                    ;; version at or before it is seen by the legs below;
                    ;; anything committed later has a later system time and
                    ;; is seen by the next run. Read after the legs (as P1
                    ;; first did), a version committed in between landed
                    ;; behind the checkpoint unrepaired.
                    newest (newest-version node with-page-permit)
                    leg (fn [col]
                          (loop [after checkpoint n 0]
                            (let [rows (changed-page node col after page with-page-permit)]
                              (if (empty? rows)
                                n
                                (let [last-row (last rows)]
                                  (repair-ids! ds node (map row-id rows) with-page-permit)
                                  (recur [(str (row-marker last-row))
                                          (str (row-id last-row))]
                                         (+ n (count rows))))))))
                    upserts (leg "_system_from")
                    ;; Both legs begin at the old checkpoint. Advance only
                    ;; after both complete, so a failed tombstone leg cannot
                    ;; hide deletes behind an upsert watermark.
                    tombstones (if tombstones? (leg "_system_to") 0)]
                (when newest
                  (meta-set! ds "checkpoint-ts" (str (row-marker newest)))
                  (meta-set! ds "checkpoint-id" (str (row-id newest))))
                (meta-set! ds "last-tx-id" tx-id)
                ;; Failures counted before this run began; one that happened
                ;; during it is not known to be repaired and keeps the gate shut.
                (meta-set! ds "hook-failures-at-catch-up" failures-at-start)
                (let [res {:changed (+ upserts tombstones)
                           :upsert-leg upserts :tombstone-leg tombstones
                           :tx-id tx-id :elapsed-ms (- (System/currentTimeMillis) started)}]
                  (swap! !stats assoc :last-catch-up
                         (assoc (select-keys res [:changed :elapsed-ms])
                                :at (str (Instant/now))))
                  res))))))
      (catch clojure.lang.ExceptionInfo e
        (if (= :expensive-read-busy (:entindex/error (ex-data e)))
          {:skipped :expensive-read-busy}
          (throw e)))
      (finally (reset! !catch-up-running? false)))))

(defn checkpoint []
  (when-let [ds (ds*)]
    {:ts (meta-get ds "checkpoint-ts") :id (meta-get ds "checkpoint-id")
     :last-tx-id (meta-get ds "last-tx-id")}))

(defn reads-usable? []
  (let [ds (ds*)]
    (boolean
     (and @!reads-enabled ds (meta-get ds "checkpoint-ts")
          (= (long (:hook-failures @!stats))
             (try (Long/parseLong (or (meta-get ds "hook-failures-at-catch-up") "-1"))
                  (catch Exception _ -1)))))))

(defn ent-stats []
  (let [ds (ds*)]
    (assoc @!stats
           :ready (some? ds)
           :rows (when ds (:n (first (jdbc/execute! ds ["SELECT count(*) AS n FROM ent_node"]
                                                        unqualified))))
           :per-type (when ds (jdbc/execute! ds ["SELECT type, count(*) AS entities
                                                  FROM ent_node GROUP BY type ORDER BY entities DESC"]
                                             unqualified))
           :checkpoint (checkpoint)
           :fill @!fill-progress
           :periodic? (boolean @!scheduler)
           :catch-up-running? @!catch-up-running?)))

(defonce ^:private !scheduler (atom nil))

(defn- daemon-factory []
  (reify ThreadFactory
    (newThread [_ r]
      (doto (Thread. ^Runnable r "ent-periodic-catch-up") (.setDaemon true)))))

(defn start-periodic-catch-up!
  [node & {:keys [interval-ms catch-up-fn]
           :or {interval-ms default-catch-up-interval-ms}}]
  (cond
    (not (pos? (long interval-ms))) {:ok true :periodic false :reason :disabled}
    @!scheduler {:ok true :periodic true :reason :already-running}
    :else
    (let [run (or catch-up-fn #(catch-up! node))
          scheduler (Executors/newSingleThreadScheduledExecutor (daemon-factory))]
      (.scheduleWithFixedDelay scheduler ^Runnable
                               #(try (run)
                                     (catch Throwable t
                                       (swap! !stats assoc :last-periodic-error
                                              (str (.getMessage t)))))
                               (long interval-ms) (long interval-ms) TimeUnit/MILLISECONDS)
      (reset! !scheduler scheduler)
      {:ok true :periodic true :interval-ms interval-ms})))

(defn stop-periodic-catch-up! []
  (when-let [scheduler @!scheduler]
    (.shutdownNow scheduler)
    (reset! !scheduler nil))
  {:ok true :periodic false})
