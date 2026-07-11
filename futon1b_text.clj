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
            [futon1b-xt :as fxt]))

(def ^:private unqualified {:builder-fn rs/as-unqualified-maps})

(defonce !ds (atom nil))
(defonce !stats (atom {:indexed 0 :errors 0 :last-at nil}))

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

(defn catch-up!
  "Index everything strictly after the last indexed (at, id) checkpoint.
   With no checkpoint this is the full deterministic rebuild. Returns
   {:indexed n :last-at s}. Batched keyset pagination — never holds more
   than `page` bodies (the 259MB read-edn-file lesson)."
  [node & {:keys [page] :or {page 1000}}]
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
            (recur hi (+ total n))))))))

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
        (catch Throwable _
          (swap! !stats update :errors inc))))))

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
  [ds {:keys [q author session-id since before limit]}]
  (let [k (or limit 10)
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
                 " ORDER BY bm25(ev_fts) LIMIT " overfetch)]
    (jdbc/execute! ds (into [sql] params) unqualified)))

(defn- fetch-doc [node id]
  (first (fxt/safe-q node (list '-> '(from :evidence [*])
                                (list 'where (list '= 'xt/id id))))))

(defn search
  "Free-text search: FTS5 pre-filter + per-candidate XTDB re-check.
   A candidate survives only if the doc exists in the store AND still
   passes the structured filters (author/session/since/before/ephemeral)
   read from the STORE's copy, not the index's. Returns
   {:results [{:score f :entry doc} ...] :count n :checked n :index-as-of s}."
  [node {:keys [author session-id since before include-ephemeral limit] :as params}]
  (let [ds @!ds
        k (or limit 10)
        cands (candidates ds params)
        checked
        (into []
              (comp
               (map (fn [{:keys [id score]}]
                      (when-let [doc (fetch-doc node id)]
                        {:score score :doc doc})))
               (filter some?)
               (filter (fn [{:keys [doc]}]
                         (and (or (nil? author) (= (str author) (str (:evidence/author doc))))
                              (or (nil? session-id) (= (str session-id) (str (:evidence/session-id doc))))
                              (or (nil? since) (>= (compare (str (:evidence/at doc)) (str since)) 0))
                              (or (nil? before) (neg? (compare (str (:evidence/at doc)) (str before))))
                              ;; contract semantics: param absent = no filtering
                              (or (not (false? include-ephemeral))
                                  (not (true? (:evidence/ephemeral? doc)))))))
               (take k))
              cands)]
    {:results (mapv (fn [{:keys [score doc]}]
                      {:score score :entry (dissoc doc :xt/id)})
                    checked)
     :count (count checked)
     :checked (count cands)
     :index-as-of (meta-get ds "last-at")}))

(defn stats []
  (let [ds @!ds
        rows (when ds
               (:n (jdbc/execute-one! ds ["SELECT count(*) AS n FROM ev_fts"]
                                      unqualified)))]
    (assoc @!stats :rows rows :ready (some? ds))))
