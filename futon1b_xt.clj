;; futon1b-xt — shared XTDB 2 query helper.
;;
;; XTDB 2.1.0 errors with "Not all variables in expression are in scope"
;; when a query references a column on a table NO doc has ever been written
;; to (the table doesn't exist yet, so the column can't bind). On a fresh
;; operational-first store every first-touch read hits this — including the
;; hyperedge no-op guard's read-before-first-ever-write and /health. Treat
;; exactly that error as an empty result; everything else propagates.
(ns futon1b-xt
  (:require [clojure.string :as str]
            [xtdb.api :as xt]
            [next.jdbc :as jdbc]
            [xtdb.next.jdbc :as xt-jdbc])
  (:import [java.sql Connection]
           [java.util.concurrent Executors Semaphore ThreadFactory]
           [xtdb.api DataSource]))

(def ^:private query-width 4)

(defonce ^:private query-permits
  ;; One process-wide budget for pgwire queries. Per-request futures otherwise
  ;; multiply the HTTP worker count (four requests x four hydrations = sixteen
  ;; concurrent XTDB queries), precisely the convoy that caused the 2026-07-22
  ;; brown-out. Fair acquisition keeps point reads and writes from starving.
  (Semaphore. query-width true))

(def ^:private cached-plan-retries
  "A pgwire cached plan is invalidated when the result type of its table
  changes underneath it. XTDB materializes hyperedge props as `prop$*`
  COLUMNS, so ingesting documents with previously unseen prop keys widens the
  table and invalidates plans — continuously, while a backlog is indexing.
  Re-executing re-prepares, so this is retryable rather than fatal; it was
  fatal here only because we rethrew it. Found 2026-08-13, when it blocked
  every boot after ~9.5k new pattern/clause records were written."
  5)

(defn- run-guarded
  "Run THUNK under the process-wide query budget with the two known-benign
  failure mappings (unbound-column → [], invalidated cached plan → retry)."
  [thunk]
  (.acquire query-permits)
  (try
    (loop [attempt 1]
      (let [r (try
                (thunk)
                (catch Exception e
                  (let [msg (str (.getMessage e))]
                    (cond
                      (re-find #"(?i)not all variables in expression are in scope|table not found"
                               msg)
                      []

                      (and (re-find #"(?i)cached plan must not change result type" msg)
                           (< attempt cached-plan-retries))
                      ::retry-cached-plan

                      :else (throw e)))))]
        (if (identical? r ::retry-cached-plan)
          (do (Thread/sleep (* 200 attempt))
              (recur (inc attempt)))
          r)))
    (finally
      (.release query-permits))))

(defn safe-q
  [node form]
  (run-guarded #(xt/q node form)))

;; ---------------------------------------------------------------------------
;; Deadlined reads (E-futon1b-gc-wedge, 2026-08-23).
;;
;; `xt/q` hands next.jdbc a fixed option map with no :timeout, and the pgwire
;; socket read it blocks in does not honour thread interruption — two evidence
;; page reads sat in `Net.poll` for four days holding both expensive-read
;; permits. A Clojure future around `xt/q` cannot fix that. `timed-q` opens
;; the node's own JDBC connection and sets two deadlines on it:
;;   - `setNetworkTimeout`: the socket itself gives up (the real guarantee —
;;     the blocked read throws, `finally` blocks run, permits return);
;;   - `setQueryTimeout`: pgjdbc's best-effort cancel. Measured 2026-08-23: XTDB
;;     2.1.0 pgwire does NOT act on it, so the effective deadline is
;;     timeout + 5s (the network margin). Once the client socket drops, the
;;     server-side scan stops within ~3s (probed: CPU → 0, no operator threads).
;; Everything else mirrors xtdb.api/plan-q (BEGIN READ ONLY with the node's
;; await-token, ROLLBACK, the same row builder and key-fn).
;; ---------------------------------------------------------------------------

(def default-query-timeout-s
  "Server-side deadline for one expensive read. 60s initially (see the
  excursion); the network timeout sits 5s above it so the cancel gets a chance
  to land before the socket is abandoned."
  60)

(defonce ^:private network-timeout-executor
  (Executors/newSingleThreadExecutor
   (reify ThreadFactory
     (newThread [_ r]
       (doto (Thread. r "futon1b-jdbc-network-timeout")
         (.setDaemon true))))))

(defn- xtql->sql
  "Same envelope xtdb.api uses (private there): one `?` per `fn` parameter."
  [xtql]
  (let [n-params (or (when (seq? xtql)
                       (let [[op params] xtql]
                         (when (and (or (= 'fn op) (= 'fn* op)) (vector? params))
                           (count params))))
                     0)]
    (format "XTQL ($$ %s $$ %s)" (pr-str xtql)
            (apply str (repeat n-params ", ?")))))

(defn timeout-error?
  "True when E is (or wraps) a JDBC deadline expiry from `timed-q`."
  [^Throwable e]
  (loop [e e]
    (cond
      (nil? e) false
      (= ::timeout (:futon1b/error (ex-data e))) true
      (instance? java.net.SocketTimeoutException e) true
      (re-find #"(?i)canceling statement due to user request|query timeout|socket.?timeout|read timed out"
               (str (.getMessage e)))
      true
      :else (recur (.getCause e)))))

(defn timed-q
  "Run QUERY+ARGS (an XTQL form, a `[(fn [..] ..) args*]` vector, or
  `[sql args*]`) against NODE with a hard deadline of TIMEOUT-S seconds.
  Returns a vector of maps like `xt/q`. On expiry the connection is torn down
  and an ex-info with `{:futon1b/error ::timeout}` is thrown."
  ([node query+args] (timed-q node query+args default-query-timeout-s))
  ([^DataSource node query+args timeout-s]
   (let [[query args] (if (vector? query+args)
                        [(first query+args) (vec (rest query+args))]
                        [query+args []])
         sql (cond (string? query) query
                   (seq? query) (xtql->sql query)
                   :else (throw (ex-info "Unknown query type" {:query query})))
         run (fn []
               (let [^Connection conn (.build (.createConnectionBuilder node))]
                 (try
                   (.setNetworkTimeout conn network-timeout-executor
                                       (int (* 1000 (+ timeout-s 5))))
                   (if-let [token (.getAwaitToken node)]
                     (jdbc/execute! conn ["BEGIN READ ONLY WITH (AWAIT_TOKEN = ?)" token])
                     (jdbc/execute! conn ["BEGIN READ ONLY"]))
                   (try
                     (into [] (map #(into {} %))
                           (jdbc/plan conn (into [sql] args)
                                      {:builder-fn xt-jdbc/builder-fn
                                       ::xt-jdbc/key-fn :kebab-case-keyword
                                       :timeout (long timeout-s)}))
                     (catch Exception e
                       (if (timeout-error? e)
                         (throw (ex-info (format "query exceeded %ds deadline" timeout-s)
                                         {:futon1b/error ::timeout
                                          :timeout-s timeout-s
                                          :sql sql}
                                         e))
                         (throw e)))
                     (finally
                       ;; A timed-out socket will not take a ROLLBACK; closing
                       ;; the connection below discards the transaction anyway.
                       (try (jdbc/execute! conn ["ROLLBACK"]) (catch Exception _))))
                   (finally
                     (try (.close conn) (catch Exception _))))))]
     (run-guarded run))))

(defn q1 [node form]
  (first (safe-q node form)))

(defn pq
  "Parameterised XTQL: `(pq '[p-id] body id)` → `[(fn [p-id] body) id]`, the
  vector form `xt/q`/`safe-q`/`timed-q` all accept.

  USE THIS instead of splicing values into the form. XTDB 2.1.0 compiles each
  distinct query expression — literals included — to JVM classes via `eval`
  under a fresh DynamicClassLoader (xtdb.expression/emit-projection and
  friends, LRU-memoised on the expression). Inlined ids/limits therefore make
  every request a cache miss; on 2026-08-23 the :7073 JVM reached 160k live
  classloaders / 1.9 GB metaspace that way. With parameters the compiled plan
  is keyed on query SHAPE and the class count stays bounded."
  [params body & args]
  (into [(list 'fn params body)] args))

(def ^:private hydrate-chunk-size
  "Ids per `_id IN (?…)` statement. pgjdbc caps bind parameters at 32767; 500
  keeps each statement small and lets the permit budget interleave readers."
  500)

(defn hydrate-by-ids
  "Full documents (`SELECT *`) for IDS from TABLE, in the order of IDS, via
  chunked SQL `_id IN (?, …)` through QUERY-FN (default `timed-q`). Missing ids
  are dropped.

  Why this exists (2026-08-23, /entities ~13 s/call): a wide projection driven
  by a non-key predicate — `(-> (from :entities [*]) (where (= entity/type t)))`
  — reads every row's nested `entity/props` across the whole 49k-row table,
  12.7 s for a 1,351-row type even without order-by, 15 s with one. The same
  rows by `_id IN (1351 ids)` take ~1 s because the IID index selects the rows
  before the wide columns are materialised. So typed reads select an ordered
  window of `[xt/id entity/type]` (~0.7 s) and hydrate here. 50 point lookups
  cost 4.4 s, so per-id hydration is not the answer either; IN is."
  ([node table ids] (hydrate-by-ids node table ids timed-q))
  ([node table ids query-fn]
   (let [ids (vec ids)
         table-name (name table)
         by-id (into {}
                     (comp (mapcat (fn [chunk]
                                     (query-fn node
                                               (into [(str "SELECT * FROM " table-name
                                                           " WHERE _id IN ("
                                                           (str/join ", " (repeat (count chunk) "?"))
                                                           ")")]
                                                     chunk))))
                           (map (fn [doc] [(:xt/id doc) doc])))
                     (partition-all hydrate-chunk-size ids))]
     (into [] (keep by-id) ids))))

(defn present? [node table id]
  (seq (safe-q node (pq '[p-id]
                        (list '-> (list 'from table '[xt/id])
                              '(where (= xt/id p-id)))
                        id))))
