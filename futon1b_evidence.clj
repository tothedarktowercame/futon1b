;; futon1b-evidence — the A1 slice of the operational switchover
;; (E-futon1b-operational-switchover): the evidence routes per
;; API-CONTRACT.md §3, on XTDB 2 with the F4 rescue ladder.
;;
;; Contract deviations, all deliberate and documented in API-CONTRACT.md:
;; - adds `before` + `include-ephemeral` query params and GET /count
;;   (the futon3c EvidenceBackend protocol needs them; futon1a ignores
;;   them). Absent include-ephemeral = futon1a behavior (no filtering).
;; - success envelope carries :rescue (F4 rescue stage) instead of
;;   :tx-id/:path/id — no proof-path machinery in v1.
(ns futon1b-evidence
  (:require [clojure.string :as str]
            [migration.transform :as xf]
            [migration.ingest :as ingest]
            [futon1b-xt :as fxt]
            [futon1b-text :as text]))

;; ---------------------------------------------------------------------------
;; Payload → doc.
;; ---------------------------------------------------------------------------

(defn- field
  "Contract: each field accepted with or without the evidence/ namespace,
  namespaced wins (routes.clj:761-831)."
  [payload k]
  (let [nk (keyword "evidence" (name k))]
    (if (contains? payload nk) (get payload nk) (get payload k))))

(defn- normalize-type [t]
  (cond (keyword? t) t
        (and (string? t) (str/starts-with? t ":")) (keyword (subs t 1))
        (string? t) (keyword t)
        :else nil))

(defn build-evidence-doc
  "Validate + default the evidence payload. Returns {:doc m} or
  {:invalid <plain-400-body>} (the three required-field 400s are plain
  {:error <string>}, not the layered envelope — contract §3)."
  [payload]
  (let [etype (normalize-type (field payload :type))
        claim-type (normalize-type (field payload :claim-type))
        author (field payload :author)]
    (cond
      (nil? etype) {:invalid {:error "evidence/type required"}}
      (nil? claim-type) {:invalid {:error "evidence/claim-type required"}}
      (or (nil? author) (str/blank? (str author)))
      {:invalid {:error "evidence/author required"}}

      :else
      (let [id (or (field payload :id) (str (random-uuid)))
            at (or (field payload :at) (str (java.time.Instant/now)))
            doc (cond-> {:xt/id id
                         :evidence/id id
                         :evidence/type etype
                         :evidence/claim-type claim-type
                         :evidence/author (str author)
                         :evidence/at at
                         :evidence/body (or (field payload :body) {})
                         :evidence/tags (vec (or (field payload :tags) []))}
                  (field payload :subject)
                  (assoc :evidence/subject (field payload :subject))
                  (field payload :pattern-id)
                  (assoc :evidence/pattern-id (field payload :pattern-id))
                  (field payload :session-id)
                  (assoc :evidence/session-id (field payload :session-id))
                  (field payload :in-reply-to)
                  (assoc :evidence/in-reply-to (field payload :in-reply-to))
                  (field payload :fork-of)
                  (assoc :evidence/fork-of (field payload :fork-of))
                  (some? (field payload :conjecture?))
                  (assoc :evidence/conjecture? (boolean (field payload :conjecture?)))
                  (some? (field payload :ephemeral?))
                  (assoc :evidence/ephemeral? (boolean (field payload :ephemeral?))))]
        {:doc doc}))))

;; ---------------------------------------------------------------------------
;; Point reads.
;; ---------------------------------------------------------------------------

(defn fetch-by-id [node id]
  (first (fxt/safe-q node (fxt/pq '[p-id]
                                  '(-> (from :evidence [*])
                                       (where (= xt/id p-id)))
                                  id))))

(defn evidence-exists? [node id]
  (seq (fxt/safe-q node (fxt/pq '[p-id]
                                '(-> (from :evidence [xt/id])
                                     (where (= xt/id p-id)))
                                id))))

(defn public-doc [doc]
  (dissoc doc :xt/id))

(def ^:private default-page-size 100)
(def ^:private max-page-size 1000)
(def ^:private scan-page-size 1000)

#_{:clj-kondo/ignore [:unused-private-var]}
(defn- hydrate-projected-pointwise
  "Historical N+1 hydrator retained only as an equality oracle in A1 tests."
  [node projected]
  (keep #(fetch-by-id node (:xt/id %)) projected))

(defn- hydrate-projected
  "Hydrate a bounded projected page in one parameterised SQL membership query.

  XTDB 2.1 has no working XTQL list-membership predicate. A variadic XTQL `or`
  also compiles into an oversized JVM method at realistic page sizes. The SQL
  surface supports `IN` natively, so the selected ids remain parameters rather
  than generated query code. Restore projected order after hydration because
  SQL `IN` does not define row order. The HTTP limit caps this at 1,000 ids."
  [node projected]
  (if-not (seq projected)
    []
    (let [ids (mapv :xt/id projected)
          placeholders (str/join "," (repeat (count ids) "?"))
          sql (str "SELECT * FROM evidence WHERE _id IN (" placeholders ")")
          docs (fxt/timed-q node (into [sql] ids))
          by-id (into {} (map (juxt :xt/id identity)) docs)]
      (into [] (keep #(get by-id (:xt/id %))) projected))))

;; ---------------------------------------------------------------------------
;; Write path: append-only, duplicate-id 409, transform + rescue + verify.
;; ---------------------------------------------------------------------------

(defonce !shape-log (xf/make-shape-log))

(defn prepare-evidence-write
  "Build and validate an evidence write without mutating the node. Returns a
  transformed :doc or the exact :status/:body refusal used by write-evidence!."
  [node payload]
  (let [{:keys [doc invalid]} (build-evidence-doc payload)
        ;; Reshapes are recorded except the by-design stringification of the
        ;; JSON-keyed body; a string-keyed :evidence/subject leaves a record.
        prepared-doc (when doc (xf/transform-doc doc !shape-log
                                                 {:log-stringify? true
                                                  :log-except #{[:evidence/body]}}))]
    (cond
      invalid {:status 400 :body invalid}

      (and (:evidence/in-reply-to doc)
           (not (evidence-exists? node (:evidence/in-reply-to doc))))
      {:status 409
       :doc prepared-doc
       :body {:error :reply-not-found
              :evidence/id (:xt/id doc)
              :in-reply-to (:evidence/in-reply-to doc)}}

      (and (:evidence/fork-of doc)
           (not (evidence-exists? node (:evidence/fork-of doc))))
      {:status 409
       :doc prepared-doc
       :body {:error :fork-not-found
              :evidence/id (:xt/id doc)
              :fork-of (:evidence/fork-of doc)}}

      (evidence-exists? node (:xt/id doc))
      {:status 409
       :doc prepared-doc
       :body {:error "duplicate evidence id" :evidence/id (:xt/id doc)}}

      :else {:doc prepared-doc})))

(defn rescue-evidence-doc!
  "Apply the evidence writer's existing rescue ladder to a prepared doc."
  [node doc]
  (ingest/put-doc-with-rescue! node :evidence doc !shape-log))

(defn write-evidence!
  "Returns [status body]. 201 on success (contract envelope), 400 on
  missing required fields, 409 on duplicate id, 500 if the doc is absent
  after the rescue ladder (verified put, as the hyperedge path)."
  [node payload]
  (let [{:keys [status body doc]} (prepare-evidence-write node payload)]
    (if status
      [status body]
      (let [res (rescue-evidence-doc! node doc)]
        (if (evidence-exists? node (:xt/id doc))
          (do
            ;; D1 sidecar refresh rides the append path (M-text-sidecar P3);
            ;; fire-and-forget — never affects the verified put.
            (text/on-append! doc)
            [201 (cond-> {:ok true
                          :evidence/id (:evidence/id doc)
                          :entry (public-doc doc)}
                   (keyword? res) (assoc :rescue res))])
          [500 {:ok false :evidence/id (:evidence/id doc)
                :error "verified put: doc absent after rescue ladder"}])))))

;; ---------------------------------------------------------------------------
;; Query path. Exact-match params push down to XTQL where; the rest
;; post-filter in Clojure (contract §3 semantics, lexicographic since).
;; ---------------------------------------------------------------------------

(def ^:private filter-cols
  "Projection sufficient for every post-filter + sort AND every pushdown
  where-column — a where var absent from the projection is unbound, which
  safe-q maps to an empty result (silent zero; bit the /count tests).
  Fetching [*] across the corpus died at 94k docs (>60s; 2026-07-11)."
  '[xt/id evidence/id evidence/at evidence/type evidence/claim-type evidence/author
    evidence/session-id evidence/fork-of
    evidence/ephemeral? evidence/tags evidence/subject evidence/pattern-id])

(def ^:private filter-param-specs
  "Pushdown filters in a FIXED order, each a [query-key param-sym clause-fn].
  The set of present keys selects the query shape; the values ride as
  parameters, so the compiled plan text is stable across requests."
  [[:type 'p-type #(list '= 'evidence/type %)]
   [:claim-type 'p-claim-type #(list '= 'evidence/claim-type %)]
   [:author 'p-author #(list '= 'evidence/author %)]
   [:session-id 'p-session-id #(list '= 'evidence/session-id %)]
   [:fork-of 'p-fork-of #(list '= 'evidence/fork-of %)]
   [:since 'p-since #(list '>= 'evidence/at %)]
   [:before 'p-before #(list '< 'evidence/at %)]])

(defn- pushdown-params
  "[param-syms args where-clauses] for the pushdown filters present in Q, in
  the fixed `filter-param-specs` order."
  [q]
  (let [present (filter (fn [[k]] (some? (get q k))) filter-param-specs)]
    [(mapv second present)
     (mapv (fn [[k]] (get q k)) present)
     (mapv (fn [[_ sym f]] (f sym)) present)]))

(defn- fetch-filtered
  "Evidence docs with type/claim-type/author/session-id AND since/before
  (lexicographic string compare — the contract's own semantics) pushed
  down to XTQL as parameters. cols = '[*] for full docs, filter-cols for
  cheap scans. Deadlined (E-futon1b-gc-wedge)."
  [node q cols]
  (let [[params args clauses] (pushdown-params q)
        body (if (seq clauses)
               (list '-> (list 'from :evidence cols) (cons 'where clauses))
               (list 'from :evidence cols))]
    (fxt/timed-q node (into [(list 'fn params body)] args))))

(def ^:private scalar-filter-cols
  "filter-cols minus the nested `evidence/subject` union. Projecting the
  union on every keyset page was the most expensive part of the page shape
  (E-futon1b-gc-wedge); it is only needed when subject post-filtering is."
  (vec (remove #{'evidence/subject} filter-cols)))

(defn- page-query
  "Compact cursor-bounded projection, without a database sort or limit.
  Values remain parameters so each filter/cursor shape shares a compiled plan.
  Global top-K selection happens while reducing these rows, before pagination."
  [q cursor cols]
  (let [[f-params f-args f-clauses] (pushdown-params q)
        params (into f-params (if cursor '[p-cursor-at p-cursor-id] []))
        args (into f-args (if cursor (vec cursor) []))
        clauses (cond-> f-clauses
                  cursor (conj (list 'or
                                     (list '< 'evidence/at 'p-cursor-at)
                                     (list 'and
                                           (list '= 'evidence/at 'p-cursor-at)
                                           (list '< 'xt/id 'p-cursor-id)))))
        source (list 'from :evidence cols)
        body (if (seq clauses)
               (list '-> source (cons 'where clauses))
               source)]
    (into [(list 'fn params body)] args)))

(defn- row-cursor [row]
  [(str (:evidence/at row)) (str (:xt/id row))])

(defn- fetch-newest-projected-page
  "Select global newest K identities with O(K) retained compact rows.

  XTDB 2.1.0's external descending sort reverses comparator argument indices
  across different relations, corrupting order once it spills (>102400 rows).
  Stream the entire cursor-bounded projection under the existing JDBC deadline
  instead: retain the largest K keys and only then return them descending.
  There is no database LIMIT and no sorting of an already truncated page.
  Bodies are still hydrated only after the bounded window is selected."
  [node q cursor page-size cols]
  (let [newest (fxt/timed-reduce-q
                node (page-query q cursor cols)
                (fn [rows row]
                  (let [rows (conj rows row)]
                    (if (> (count rows) page-size)
                      (disj rows (first rows))
                      rows)))
                (sorted-set-by #(compare (row-cursor %1) (row-cursor %2))))]
    (vec (rseq newest))))

(declare apply-post-filters)

(defn- requires-post-filtering?
  [{:keys [tags subject-type subject-id pattern-id include-ephemeral]}]
  (or (seq tags) subject-type subject-id pattern-id
      (false? include-ephemeral)))

(def ^:private max-scanned-rows-per-request
  "Whole-request ceiling on projected rows scanned for post-filtered reads.
  Per-page limits bounded each query but not the loop around them: a sparse
  post-filter (rare tag, subject) walked the corpus inside one HTTP request.
  On exhaustion the response carries what matched plus a cursor and
  `:incomplete true`; the caller continues from the cursor."
  20000)

;; ---------------------------------------------------------------------------
;; Tag reads through the text sidecar's tag index.
;;
;; Tags are not a pushdown column, so a tag read walked every projected row in
;; its window and kept the few that matched: 15-20 s per tags=test-registry
;; read over futon3c's default 48 h window, 99 s unwindowed, for 3,124 matches
;; among ~300k rows (2026-09-25, live). The sidecar already keeps ev_tags.
;; Below the tail start it proposes candidate ids; every candidate is re-read
;; from the store and re-checked against the same filters (contract C1). From
;; the tail start up, the store is still scanned directly.
;; ---------------------------------------------------------------------------

(def ^:private tag-tail-margin-ms
  "How far below the sidecar checkpoint the store is still scanned directly.
  on-append! fails under sqlite's single writer (SQLITE_BUSY: 983 in one day,
  live) and catch-up! only repairs from its checkpoint up. :evidence/at is
  stamped by the writer before the commit lands, so a doc whose own on-append!
  failed can commit just below a checkpoint that already passed its :at. The
  margin has to exceed that stamp-to-commit latency."
  (* 15 60 1000))

(defn- tag-tail-start
  "The :at from which tag reads scan the store directly, or nil when there is
  no sidecar checkpoint (the caller then keeps the full store scan)."
  []
  (when-let [ck (text/checkpoint-at)]
    (try (str (.minusMillis (java.time.Instant/parse ck) tag-tail-margin-ms))
         (catch Exception _ nil))))

(defn- sql-col [sym]
  ;; `evidence/claim-type` -> "evidence$claim_type"; quoted for the `?` of
  ;; ephemeral?. A misspelt column reads as NULL rather than failing, which
  ;; would make its filter silently vacuous, so the recheck test covers each.
  (str "\"" (namespace sym) "$" (str/replace (name sym) "-" "_") "\""))

(defn- projected-by-ids
  "The store's projected rows for IDS (SQL IN, as hydrate-projected)."
  [node ids cols]
  (if-not (seq ids)
    []
    (let [sql (str "SELECT _id, "
                   (str/join ", " (map sql-col (remove #{'xt/id} cols)))
                   " FROM evidence WHERE _id IN ("
                   (str/join "," (repeat (count ids) "?")) ")")]
      (fxt/timed-q node (into [sql] ids)))))

(defn- pushdown-match?
  "The Clojure image of `filter-param-specs` for rows that did not come
  through the XTQL where clause. since/before are in apply-post-filters."
  [row q]
  (every? (fn [[k field]]
            (let [want (get q k)]
              (or (nil? want) (= want (get row field)))))
          [[:type :evidence/type]
           [:claim-type :evidence/claim-type]
           [:author :evidence/author]
           [:session-id :evidence/session-id]
           [:fork-of :evidence/fork-of]]))

(defn- recheck-projected
  "Store rows for candidate ids that still satisfy every filter in Q, in
  candidate order. A candidate the store no longer holds is dropped."
  [node cands q cols]
  (let [rows (projected-by-ids node (mapv :id cands) cols)
        by-id (into {} (map (juxt #(str (:xt/id %)) identity)) rows)
        ordered (keep #(get by-id (str (:id %))) cands)]
    (filter #(pushdown-match? % q) (apply-post-filters ordered q))))

(def ^:private tag-recheck-wave 1000)

(defn- recheck-wave
  "Candidates fetched per re-check round when NEEDED rows are still wanted:
  twice the need (most candidates survive), within [100, tag-recheck-wave]."
  [needed]
  (min tag-recheck-wave (max 100 (* 2 needed))))

(declare scan-window)

(defn- tail-query
  "Q restricted to the part of the store tag reads scan directly."
  [q tail-start]
  (assoc q :since (if (and (:since q) (pos? (compare (:since q) tail-start)))
                    (:since q)
                    tail-start)))

(defn- subject-cols [q]
  (if (or (:subject-type q) (:subject-id q)) filter-cols scalar-filter-cols))

(defn- tag-window
  "bounded-window for a tag query, or nil when the sidecar cannot serve it.
  Tail rows (at >= tail start) come from the store scan and are all newer
  than any candidate, so the page is tail matches then candidate survivors."
  [node q limit initial-cursor]
  (when-let [tail-start (tag-tail-start)]
    (let [tail (scan-window node (tail-query q tail-start) limit initial-cursor)
          want (- limit (count (:entries tail)))
          cols (subject-cols q)]
      (if (or (:incomplete tail) (<= want 0))
        tail
        (loop [cursor initial-cursor
               selected []
               checked 0]
          (let [remaining (- want (count selected))
                wave (recheck-wave remaining)
                cands (text/tag-candidates {:tags (:tags q) :since (:since q)
                                            :below-at tail-start :cursor cursor
                                            :limit wave})
                selected' (into selected
                                (take remaining (recheck-projected node cands q cols)))
                checked' (+ checked (count cands))]
            (if (or (>= (count selected') want) (< (count cands) wave))
              (let [full? (>= (count selected') want)]
                ;; full? implies selected' is non-empty, since want > 0 here.
                {:entries (into (:entries tail)
                                (map public-doc (hydrate-projected node selected')))
                 :next-cursor (when full? (row-cursor (peek selected')))
                 :scanned (+ (:scanned tail) checked')})
              (let [lst (peek cands)]
                (recur [(:at lst) (:id lst)] selected' checked')))))))))

(defn- tag-count
  "count-evidence for a tag query via the sidecar, or nil when it cannot."
  [node q]
  (when-let [tail-start (tag-tail-start)]
    (let [tail-q (tail-query q tail-start)
          tail-n (count (apply-post-filters (fetch-filtered node tail-q filter-cols) tail-q))
          cols (subject-cols q)
          cands (text/tag-candidates {:tags (:tags q) :since (:since q)
                                      :below-at tail-start})]
      (+ tail-n
         (reduce + (map #(count (recheck-projected node % q cols))
                        (partition-all tag-recheck-wave cands)))))))

(defn- scan-window
  "Return a cursor page of at most LIMIT exact matches, newest first.

  Do not ask XTDB to order full evidence documents. On the full store that
  query built an Arrow order-by/coalescing result over the matching corpus,
  drove the server through MemoryHigh, and occupied every HTTP worker long
  enough for even /health to time out. Instead, scan the compact projection
  already used by /count, apply the authoritative filters, retain only the
  newest LIMIT identities, and hydrate that bounded window with point reads.
  This keeps the expensive full-document cardinality at LIMIT while preserving
  exact API ordering and filter semantics.

  The scan is bounded per request by `max-scanned-rows-per-request`; the
  result map carries :scanned (projected rows read) and, when the ceiling
  stopped the scan early, :incomplete true with :next-cursor set."
  [node q limit initial-cursor]
  (let [post-filter? (requires-post-filtering? q)
        cols (if (or (:subject-type q) (:subject-id q))
               filter-cols
               scalar-filter-cols)]
    (loop [cursor initial-cursor
           selected []
           scanned 0]
      (let [page-size (if post-filter?
                        scan-page-size
                        (max 1 (- limit (count selected))))
            page (vec (fetch-newest-projected-page node q cursor page-size cols))
            scanned' (+ scanned (count page))
            matches (vec (apply-post-filters page q))
            selected' (into selected matches)]
        (cond
          (>= (count selected') limit)
          (let [window (vec (take limit selected'))
                next-cursor (some-> window peek row-cursor)]
            {:entries (mapv public-doc (hydrate-projected node window))
             ;; A full page may end exactly at EOF. Returning its cursor is safe:
             ;; the next request proves exhaustion with an empty page.
             :next-cursor next-cursor
             :scanned scanned'})

          (< (count page) page-size)
          {:entries (mapv public-doc (hydrate-projected node selected'))
           :next-cursor nil
           :scanned scanned'}

          :else
          (let [next-cursor (row-cursor (peek page))]
            (when (= cursor next-cursor)
              (throw (ex-info "Evidence keyset scan made no progress"
                              {:cursor cursor :limit limit})))
            (if (>= scanned' max-scanned-rows-per-request)
              {:entries (mapv public-doc (hydrate-projected node selected'))
               :next-cursor next-cursor
               :scanned scanned'
               :incomplete true}
              (recur next-cursor selected' scanned'))))))))

;; tag-window falls back to scan-window when there is no sidecar checkpoint.
(defn- bounded-window
  [node q limit initial-cursor]
  (or (when (seq (:tags q)) (tag-window node q limit initial-cursor))
      (scan-window node q limit initial-cursor)))

(defn- name-of [x]
  (cond (keyword? x) (name x)
        (symbol? x) (name x)
        :else (str x)))

(defn- tag-match? [doc tags]
  (let [stored (set (map name-of (:evidence/tags doc)))]
    (every? stored tags)))

(defn- subject-match? [doc subject-type subject-id]
  (let [subj (:evidence/subject doc)
        ref-type (or (get subj :ref/type) (get subj "ref/type"))
        ref-id (or (get subj :ref/id) (get subj "ref/id"))]
    (and (or (nil? subject-type) (= (name-of ref-type) (name subject-type)))
         (or (nil? subject-id) (= (str ref-id) subject-id)))))

(defn- apply-post-filters
  [docs {:keys [since before tags subject-type subject-id pattern-id
                include-ephemeral]}]
  (cond->> docs
    ;; include-ephemeral absent (nil) = futon1a behavior: no filtering.
    (false? include-ephemeral)
    (remove #(true? (:evidence/ephemeral? %)))

    since  (filter #(>= (compare (str (:evidence/at %)) since) 0))
    before (filter #(neg? (compare (str (:evidence/at %)) before)))
    (seq tags) (filter #(tag-match? % tags))
    (or subject-type subject-id) (filter #(subject-match? % subject-type subject-id))
    pattern-id (filter #(= pattern-id (normalize-type (:evidence/pattern-id %))))))

(defn- parse-query-params
  "String HTTP params (contract §3) → typed filter map."
  [p]
  (cond-> {}
    (p "type") (assoc :type (normalize-type (p "type")))
    (p "claim-type") (assoc :claim-type (normalize-type (p "claim-type")))
    (p "author") (assoc :author (p "author"))
    (p "session-id") (assoc :session-id (p "session-id"))
    (p "fork-of") (assoc :fork-of (p "fork-of"))
    (p "subject-type") (assoc :subject-type (normalize-type (p "subject-type")))
    (p "subject-id") (assoc :subject-id (p "subject-id"))
    (p "pattern-id") (assoc :pattern-id (normalize-type (p "pattern-id")))
    (p "since") (assoc :since (p "since"))
    (p "before") (assoc :before (p "before"))
    (and (p "cursor-at") (p "cursor-id"))
    (assoc :cursor [(p "cursor-at") (p "cursor-id")])
    (p "tags") (assoc :tags (remove str/blank? (str/split (p "tags") #",")))
    (p "include-ephemeral")
    (assoc :include-ephemeral (= "true" (str/lower-case (p "include-ephemeral"))))
    (p "limit") (assoc :limit (try (Long/parseLong (p "limit"))
                                   (catch Exception _ nil)))))

(defn query-evidence-response
  "Validate and serve one bounded evidence page as [HTTP-STATUS BODY].

  Missing limit uses DEFAULT-PAGE-SIZE. Invalid, non-positive, and oversized
  limits are rejected rather than selecting an unbounded realization path.
  Continue with the returned :next-cursor map as cursor-at/cursor-id."
  [node http-params]
  (let [raw-limit (http-params "limit")
        parsed-limit (if raw-limit
                       (try (Long/parseLong raw-limit)
                            (catch Exception _ ::invalid))
                       default-page-size)]
    (if (or (= ::invalid parsed-limit)
            (not (pos-int? parsed-limit))
            (> parsed-limit max-page-size))
      [400 {:ok false
            :error "limit must be an integer between 1 and 1000"
            :limit/max max-page-size}]
      (let [q (parse-query-params http-params)
            {:keys [entries next-cursor scanned incomplete]}
            (bounded-window node q parsed-limit (:cursor q))]
        [200 (cond-> {:entries entries
                      :count (count entries)
                      :limit parsed-limit
                      :scanned scanned}
               next-cursor
               (assoc :next-cursor {:at (first next-cursor)
                                    :id (second next-cursor)})
               ;; Scan ceiling hit before LIMIT matches: fewer entries than
               ;; requested does NOT mean end of corpus — continue from cursor.
               incomplete
               (assoc :incomplete true
                      :scan/max max-scanned-rows-per-request))]))))

(defn query-evidence
  "Compatibility helper for in-process callers; returns the validated body."
  [node http-params]
  (second (query-evidence-response node http-params)))

(defn count-evidence
  "GET /api/alpha/evidence/count → {:count n} (same filters, no limit).
  Projected scan — never materializes full docs."
  [node http-params]
  (let [q (dissoc (parse-query-params http-params) :limit)]
    {:count (or (when (seq (:tags q)) (tag-count node q))
                (count (apply-post-filters (fetch-filtered node q filter-cols) q)))}))

;; ---------------------------------------------------------------------------
;; Sessions + chain.
;; ---------------------------------------------------------------------------

(defn sessions
  "GET /api/alpha/evidence/sessions — contract §3 envelope. since restricts
  the ENTRIES considered; :types/:authors stringify keywords WITH the colon
  ((str kw)), unlike the entry stream."
  [node http-params]
  (let [since (http-params "since")
        author (http-params "author")
        limit (some-> (http-params "limit")
                      (as-> s (try (Long/parseLong s) (catch Exception _ nil))))
        docs (cond->> (fxt/safe-q node '(from :evidence [xt/id evidence/session-id evidence/at
                                                          evidence/type evidence/author]))
               author (filter #(= author (:evidence/author %)))
               since (filter #(>= (compare (str (:evidence/at %)) since) 0)))
        sessioned (filter :evidence/session-id docs)
        by-session (group-by :evidence/session-id sessioned)
        rows (->> by-session
                  (map (fn [[sid entries]]
                         (let [ats (sort (map #(str (:evidence/at %)) entries))]
                           {:session-id sid
                            :count (count entries)
                            :types (->> entries (map :evidence/type) distinct
                                        (map str) sort vec)
                            :authors (->> entries (map :evidence/author) distinct
                                          (map str) sort vec)
                            :first-at (first ats)
                            :latest-at (last ats)})))
                  (sort-by :latest-at #(compare %2 %1)))
        limited (if (and (int? limit) (pos? limit)) (take limit rows) rows)]
    {:window-since since
     :author-filter author
     :total-sessions (count rows)
     :total-entries (count sessioned)
     :sessions (vec limited)}))

(defn chain
  "GET /api/alpha/evidence/{id}/chain — follow :evidence/in-reply-to to the
  root, cycle-safe. {:chain [<root> ... <id>]} (oldest first; empty if id
  unknown)."
  [node id]
  (loop [cur-id id, acc (), seen #{}]
    (if (or (nil? cur-id) (seen cur-id))
      {:chain (vec acc)}
      (if-let [doc (fetch-by-id node cur-id)]
        (recur (:evidence/in-reply-to doc)
               (cons (public-doc doc) acc)
               (conj seen cur-id))
        {:chain (vec acc)}))))
