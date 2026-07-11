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
  (first (fxt/safe-q node (list '-> '(from :evidence [*])
                          (list 'where (list '= 'xt/id id))))))

(defn- exists? [node id]
  (seq (fxt/safe-q node (list '-> '(from :evidence [xt/id])
                        (list 'where (list '= 'xt/id id))))))

(defn- public-doc [doc]
  (dissoc doc :xt/id))

;; ---------------------------------------------------------------------------
;; Write path: append-only, duplicate-id 409, transform + rescue + verify.
;; ---------------------------------------------------------------------------

(defonce !shape-log (xf/make-shape-log))

(defn write-evidence!
  "Returns [status body]. 201 on success (contract envelope), 400 on
  missing required fields, 409 on duplicate id, 500 if the doc is absent
  after the rescue ladder (verified put, as the hyperedge path)."
  [node payload]
  (let [{:keys [doc invalid]} (build-evidence-doc payload)]
    (cond
      invalid [400 invalid]

      (exists? node (:xt/id doc))
      [409 {:error "duplicate evidence id" :evidence/id (:xt/id doc)}]

      :else
      (let [xdoc (xf/transform-doc doc)
            res (ingest/put-doc-with-rescue! node :evidence xdoc !shape-log)]
        (if (exists? node (:xt/id xdoc))
          (do
            ;; D1 sidecar refresh rides the append path (M-text-sidecar P3);
            ;; fire-and-forget — never affects the verified put.
            (text/on-append! xdoc)
            [201 (cond-> {:ok true
                          :evidence/id (:evidence/id xdoc)
                          :entry (public-doc xdoc)}
                   (keyword? res) (assoc :rescue res))])
          [500 {:ok false :evidence/id (:evidence/id xdoc)
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
  '[xt/id evidence/at evidence/type evidence/claim-type evidence/author
    evidence/session-id evidence/fork-of
    evidence/ephemeral? evidence/tags evidence/subject])

(defn- fetch-filtered
  "Evidence docs with type/claim-type/author/session-id AND since/before
  (lexicographic string compare — the contract's own semantics) pushed
  down to XTQL. cols = '[*] for full docs, filter-cols for cheap scans."
  [node {:keys [type claim-type author session-id fork-of since before]} cols]
  (let [clauses (cond-> []
                  type (conj (list '= 'evidence/type type))
                  claim-type (conj (list '= 'evidence/claim-type claim-type))
                  author (conj (list '= 'evidence/author author))
                  session-id (conj (list '= 'evidence/session-id session-id))
                  fork-of (conj (list '= 'evidence/fork-of fork-of))
                  since (conj (list '>= 'evidence/at since))
                  before (conj (list '< 'evidence/at before)))]
    (if (seq clauses)
      (fxt/safe-q node (list '-> (list 'from :evidence cols)
                       (cons 'where clauses)))
      (fxt/safe-q node (list 'from :evidence cols)))))

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
  [docs {:keys [since before tags subject-type subject-id include-ephemeral]}]
  (cond->> docs
    ;; include-ephemeral absent (nil) = futon1a behavior: no filtering.
    (false? include-ephemeral)
    (remove #(true? (:evidence/ephemeral? %)))

    since  (filter #(>= (compare (str (:evidence/at %)) since) 0))
    before (filter #(neg? (compare (str (:evidence/at %)) before)))
    (seq tags) (filter #(tag-match? % tags))
    (or subject-type subject-id) (filter #(subject-match? % subject-type subject-id))))

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
    (p "since") (assoc :since (p "since"))
    (p "before") (assoc :before (p "before"))
    (p "tags") (assoc :tags (remove str/blank? (str/split (p "tags") #",")))
    (p "include-ephemeral")
    (assoc :include-ephemeral (= "true" (str/lower-case (p "include-ephemeral"))))
    (p "limit") (assoc :limit (try (Long/parseLong (p "limit"))
                                   (catch Exception _ nil)))))

(defn query-evidence
  "GET /api/alpha/evidence → {:entries [...] :count n} (newest first).
  With a limit: phase 1 scans projected columns to find the window, phase 2
  re-fetches [*] bounded by the window's oldest :at — full docs are only
  materialized for ~limit rows, not the corpus."
  [node http-params]
  (let [q (parse-query-params http-params)
        limit (:limit q)]
    (if (and (int? limit) (pos? limit))
      (let [slim (-> (fetch-filtered node q filter-cols)
                     (apply-post-filters q))
            window (->> slim
                        (sort-by #(str (:evidence/at %)) #(compare %2 %1))
                        (take limit))]
        (if (empty? window)
          {:entries [] :count 0}
          (let [cutoff (str (:evidence/at (last window)))
                q2 (update q :since (fn [s] (if (and s (pos? (compare s cutoff))) s cutoff)))
                docs (-> (fetch-filtered node q2 '[*])
                         (apply-post-filters q2))
                sorted (sort-by #(str (:evidence/at %)) #(compare %2 %1) docs)
                entries (mapv public-doc (take limit sorted))]
            {:entries entries :count (count entries)})))
      (let [docs (-> (fetch-filtered node q '[*])
                     (apply-post-filters q))
            sorted (sort-by #(str (:evidence/at %)) #(compare %2 %1) docs)
            entries (mapv public-doc sorted)]
        {:entries entries :count (count entries)}))))

(defn count-evidence
  "GET /api/alpha/evidence/count → {:count n} (same filters, no limit).
  Projected scan — never materializes full docs."
  [node http-params]
  (let [q (dissoc (parse-query-params http-params) :limit)]
    {:count (count (apply-post-filters (fetch-filtered node q filter-cols) q))}))

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
