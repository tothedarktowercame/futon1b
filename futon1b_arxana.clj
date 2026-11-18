;; futon1b-arxana — M-jvm-arxana INSTANTIATE increment 2: the narrow
;; authenticated conditional command route (D3 write contract, D7-DR1/DR4,
;; V3 S1). One store-side XTDB transaction atomically:
;;
;;   1. ASSERTs the expected head (or its absence) — XTDB 2.1 SQL ASSERT,
;;      the semantics V3 S1 demonstrated: the loser of a two-writer race
;;      aborts the WHOLE transaction, journal row included;
;;   2. ASSERTs the message id has no stored outcome (idempotency key);
;;   3. writes the immutable content-addressed artifact revision;
;;   4. moves the head;
;;   5. appends the journal event;
;;   6. appends the durable outcome.
;;
;; Duplicate message ids return the original outcome without reapplying
;; (crash/republish: the outcome and journal are already durable in the same
;; transaction as the apply, so recovery is a READ, never a re-apply).
;; Stale refusal returns the current head, the submitted base and the D3
;; recovery choices; nothing is written, so there is no partial mutation.
;;
;; This is the ONLY route that may write arxana/* records; the generic write
;; routes refuse that namespace (futon1b-gates/gate-reserved-namespace!).
;; No sequencer, no CRDT: atomicity stays in the store (D7 A2, O-D3a).
(ns futon1b-arxana
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [futon1b-gates :as gates]
            [futon1b-hxindex :as hx]
            [futon1b-xt :as fxt]
            [xtdb.api :as xt])
  (:import [java.nio.charset StandardCharsets]
           [java.security MessageDigest]
           [java.time Instant]))

(def protocol-version "arxana.v1")

(def service-principal
  "The authenticated Arxana service principal for the conditional route.
  Deployment-configurable; the route refuses any other (or absent) principal."
  (let [env (System/getenv "FUTON1B_ARXANA_PRINCIPAL")]
    (if (str/blank? env) "arxana-service" (str/trim env))))

(defn authenticate!
  "The conditional route accepts only the configured Arxana service
  principal (D7: the route authenticates the service principal; generic
  writers never reach arxana/* state)."
  [principal]
  (when-not (and (string? principal)
                 (not (str/blank? principal))
                 (= principal service-principal))
    (throw (gates/layered-error 3 :arxana-principal-refused
                                {:category :forbidden
                                 :message "conditional arxana route requires the x-arxana-principal of the Arxana service"})))
  principal)

;; ---------------------------------------------------------------------------
;; Reads.
;; ---------------------------------------------------------------------------

(defn outcome-by-message
  "The durable outcome for MESSAGE-ID, or nil. This read IS the
  crash/republish recovery path: an applied command's outcome committed in
  the same transaction as the apply, so a retry returns it and never
  reapplies."
  [node message-id]
  (fxt/q1 node (fxt/pq '[p-id]
                       '(-> (from :arxana_outcomes [*])
                            (where (= xt/id p-id)))
                       message-id)))

(defn current-head
  "The current head revision id for TARGET-ID, or nil when absent."
  [node target-id]
  (some-> (fxt/q1 node (fxt/pq '[p-id]
                               '(-> (from :arxana_heads [xt/id head/revision-id])
                                    (where (= xt/id p-id)))
                               target-id))
          :head/revision-id))

(defn revision-by-id
  "The immutable revision row, including canonical bytes, or nil."
  [node revision-id]
  (fxt/q1 node (fxt/pq '[p-id]
                       '(-> (from :arxana_revisions [*])
                            (where (= xt/id p-id)))
                       revision-id)))

(defn journal-entries
  "All journal rows for TARGET-ID (test/introspection aid)."
  [node target-id]
  (fxt/safe-q node (fxt/pq '[p-target]
                           '(-> (from :arxana_journal [*])
                                (where (= journal/target-id p-target)))
                           target-id)))

(defn clusion-by-id [node record-id]
  (some-> (fxt/q1 node (fxt/pq '[p-id]
                               '(-> (from :arxana_clusions [xt/id record/data])
                                    (where (= xt/id p-id)))
                               record-id))
          :record/data edn/read-string))
(defn evidence-by-id [node record-id]
  (some-> (fxt/q1 node (fxt/pq '[p-id]
                               '(-> (from :arxana_evidence [xt/id record/data])
                                    (where (= xt/id p-id)))
                               record-id))
          :record/data edn/read-string))
(defn assessment-by-id [node record-id]
  (some-> (fxt/q1 node (fxt/pq '[p-id]
                               '(-> (from :arxana_assessments [xt/id record/data])
                                    (where (= xt/id p-id)))
                               record-id))
          :record/data edn/read-string))
(defn attempt-by-id [node record-id]
  (some-> (fxt/q1 node (fxt/pq '[p-id]
                               '(-> (from :arxana_attempts [xt/id record/data])
                                    (where (= xt/id p-id)))
                               record-id))
          :record/data edn/read-string))
(defn external-reference-by-id [node record-id]
  (some-> (fxt/q1 node (fxt/pq '[p-id]
                               '(-> (from :arxana_external_refs [xt/id record/data])
                                    (where (= xt/id p-id)))
                               record-id))
          :record/data edn/read-string))
(defn mark-occurrence-by-id [node record-id]
  (some-> (fxt/q1 node (fxt/pq '[p-id]
                               '(-> (from :arxana_mark_occurrences [xt/id record/data])
                                    (where (= xt/id p-id)))
                               record-id))
          :record/data edn/read-string))

(defn- parse-record-instant [value reason context]
  (or (try
        (when (and (string? value)
                   (re-matches #"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z" value))
          (Instant/parse value))
        (catch java.time.format.DateTimeParseException _ nil))
      (throw (gates/layered-error 4 reason (assoc context :category :invalid)))))

(defn validate-valid-time
  "Parse a query valid-time with the same fixed-width grammar as event-time.
  Read times need not be past; the future restriction applies only to writes."
  [value]
  (parse-record-instant value :arxana-invalid-valid-time {:valid-time value}))

(defn record-by-id
  "Reserved record read, optionally at valid-time (ISO milliseconds).
  Searches only the fixed record-kind tables. No time means current reads."
  ([node record-id]
   (or (clusion-by-id node record-id)
       (evidence-by-id node record-id)
       (assessment-by-id node record-id)
       (attempt-by-id node record-id)
       (external-reference-by-id node record-id)
       (mark-occurrence-by-id node record-id)))
  ([node record-id valid-time]
   (let [instant (validate-valid-time valid-time)]
     (some (fn [table]
             (some-> (fxt/q1 node
                             (fxt/pq '[p-id p-at]
                                     (list '->
                                           (list 'from table {:bind '[xt/id record/data]
                                                              :for-valid-time '(at p-at)})
                                           '(where (= xt/id p-id)))
                                     record-id instant))
                     :record/data edn/read-string))
           [:arxana_clusions :arxana_evidence :arxana_assessments
            :arxana_attempts :arxana_external_refs :arxana_mark_occurrences]))))

(defn- stored-record [node table record-id]
  (case table
    :arxana_clusions (clusion-by-id node record-id)
    :arxana_evidence (evidence-by-id node record-id)
    :arxana_assessments (assessment-by-id node record-id)
    :arxana_attempts (attempt-by-id node record-id)
    :arxana_external_refs (external-reference-by-id node record-id)
    :arxana_mark_occurrences (mark-occurrence-by-id node record-id)))

(defn evidence-for [node clusion-id]
  (mapv (comp edn/read-string :record/data)
        (fxt/safe-q node (fxt/pq '[p-id]
                                '(-> (from :arxana_evidence [record/data subject/clusion-id])
                                     (where (= subject/clusion-id p-id)))
                                clusion-id))))

(defn assessments-for [node clusion-id]
  (mapv (comp edn/read-string :record/data)
        (fxt/safe-q node (fxt/pq '[p-id]
                                '(-> (from :arxana_assessments [record/data subject/clusion-id])
                                     (where (= subject/clusion-id p-id)))
                                clusion-id))))

;; Immutability is checked by comparing stored record data as text, so the
;; text must not depend on how an equal map was built: small Clojure maps
;; print in insertion order. Sort map keys at every level before printing.
(defn canonical-record-data
  "Deterministic EDN text for RECORD: equal values give equal strings."
  [record]
  (pr-str (walk/postwalk
           (fn [x]
             (if (map? x)
               (into (sorted-map-by (fn [a b] (compare (pr-str a) (pr-str b)))) x)
               x))
           record)))

(defn source-sequence
  "O(1) authoritative freshness marker for the C3 sidecar. It advances in
  the same transaction as every indexed record."
  [node]
  (or (some-> (fxt/q1 node
                       '(-> (from :arxana_index_state [xt/id source/sequence])
                            (where (= xt/id "source"))))
              :source/sequence)
      0))

;; ---------------------------------------------------------------------------
;; Flat derived columns (M-repl-circus; Joe, 2026-11-12: "I think we should
;; sort out the Arxana storage. I like the analogy with missions").
;;
;; Each record's type and role endpoints are merged into the written doc as
;; plain columns beside record/data, so XTQL can push filters down — the same
;; precedent as the hyperedge prop/* columns (H4, build-hyperedge-doc). The
;; columns are DERIVED from the record at write time: they never enter
;; record/data, so the record id (a digest of record/data content) is
;; unchanged, and the immutability ASSERT (which compares record$data only)
;; is untouched. A re-put of an existing record re-derives the same columns
;; deterministically, so the ASSERT's "same id, same record$data" arm still
;; holds and the re-put is a no-op.
;;
;; Multi-valued roles: none in futon3c type-registry registry-v1 (a clusion's
;; arity equals its declared role count; each role carries exactly one
;; endpoint). If a future type revision admits several endpoints on one
;; role, the column value becomes a vector of strings (endpoint-column-id
;; handles a sequential target); no current role is multi-valued.
;; ---------------------------------------------------------------------------

(defn- target-id-string
  "The plain string id of one endpoint target: an entity id, a clusion id,
  an artifact revision id, or an external reference id. Anything richer is
  printed (it cannot be indexed usefully as a scalar)."
  [target]
  (str (or (:entity/id target)
           (:ref/id target)
           (:artifact/revision-id target)
           (:clusion/id target)
           (pr-str target))))

(defn- endpoint-column-id
  [endpoint]
  (let [target (:target endpoint)]
    (if (sequential? target)
      (mapv target-id-string target)
      (target-id-string target))))

(defn flat-columns
  "The derived plain columns for [OPERATION RECORD], merged into the written
  doc. Pure. Scalar keyword/string/number values only; nils are omitted.
  Operations not listed get no columns."
  [operation record]
  (let [clean (fn [m] (into {} (filter (fn [[_ v]] (some? v))) m))]
    (case operation
      :clusion/assert
      (let [[type-key type-version] (:type-revision record)]
        (into {:clusion/type type-key
               :clusion/type-version type-version}
              (map (fn [endpoint]
                     [(keyword "role" (name (:role endpoint)))
                      (endpoint-column-id endpoint)]))
              (:endpoints record)))
      :evidence/append
      (clean {:attest/basis (:basis record)
              :attest/standing (:standing record)
              :attest/coverage (:coverage record)
              :attest/producer (some-> (:producer record) str)
              :attest/method (some-> (get-in record [:method :id]) str)
              :attest/kind (or (:attest/kind record) (:kind record))})
      :assessment/append
      (clean {:assess/verdict (:verdict record)
              :assess/actor (some-> (:actor record) str)})
      {})))

;; ---------------------------------------------------------------------------
;; Reads over the flat columns (not wired to a route yet).
;; ---------------------------------------------------------------------------

(defn clusions-by-type-and-role
  "Clusions of TYPE whose ROLE endpoint is ENDPOINT-ID, bounded at LIMIT.
  The role name becomes a column name, which XTQL cannot parameterise; role
  names are few and closed (the type registry), so each role compiles one
  plan, like one pq shape per role."
  [node type role endpoint-id limit]
  (let [role-col (symbol "role" (name role))]
    (fxt/timed-q node
                 (fxt/pq '[p-type p-endpoint p-limit]
                         (list '->
                               (list 'from :arxana_clusions
                                     (vector 'xt/id 'clusion/type role-col))
                               (list 'where
                                     (list '= 'clusion/type 'p-type)
                                     (list '= role-col 'p-endpoint))
                               (list 'limit 'p-limit))
                         type endpoint-id limit))))

(def edge-join-shapes
  "Closed Circus clusion vocabulary admitted by `edge-join`.  The route never
  accepts column names or XTQL from its caller; a type revision selects its
  exact role set here."
  {[:pattern/proposal 1] #{:turn :pattern}
   [:pattern/occurrence 1] #{:turn :pattern}
   [:memory/recall 1] #{:turn :memory}
   [:memory/use 1] #{:turn :memory}
   [:memory/burial 1] #{:turn :memory}
   [:memory/repetition 1] #{:turn :memory}
   [:interpretation 1] #{:source :intent :target}
   [:workplan/mention 2] #{:item :evidence}
   [:hou/focus 1] #{:evidence}
   [:hou/focus-relation 1] #{:focus :item}
   [:needs 1] #{:needing :supplier}
   [:supersedes 1] #{:successor :predecessor}})

(def ^:private edge-join-keys #{:query/type :patterns :where :limit})
(def ^:private logic-var-pattern #"[A-Za-z][A-Za-z0-9_-]{0,63}")

(defn- edge-join-error [reason context]
  (throw (gates/layered-error 4 reason context)))

(defn- logic-var-name [x]
  (let [s (cond (symbol? x) (name x)
                (string? x) x
                :else nil)]
    (when-not (and s (re-matches logic-var-pattern s))
      (edge-join-error :edge-join-invalid-variable {:provided (pr-str x)}))
    s))

(defn- normalize-edge-join
  [{:keys [patterns where limit] :as request}]
  (let [unknown (seq (remove edge-join-keys (keys request)))
        limit (if (nil? limit) 100 limit)]
    (when unknown
      (edge-join-error :edge-join-unknown-field {:fields (vec unknown)}))
    (when-not (and (vector? patterns) (<= 2 (count patterns) 4))
      (edge-join-error :edge-join-pattern-count
                       {:minimum 2 :maximum 4 :provided (if (sequential? patterns)
                                                         (count patterns) patterns)}))
    (when-not (and (integer? limit) (<= 1 limit 500))
      (edge-join-error :edge-join-invalid-limit
                       {:minimum 1 :maximum 500 :provided limit}))
    (let [normalized
          (mapv
           (fn [i pattern]
             (when-not (and (map? pattern) (= #{:edge-type :bind} (set (keys pattern))))
               (edge-join-error :edge-join-invalid-pattern
                                {:pattern i :fields (when (map? pattern) (vec (keys pattern)))}))
             (let [edge-type (:edge-type pattern)
                   roles (get edge-join-shapes edge-type)
                   bind (:bind pattern)]
               (when-not roles
                 (edge-join-error :edge-join-unknown-edge-type
                                  {:pattern i :edge-type edge-type}))
               (when-not (and (map? bind) (seq bind))
                 (edge-join-error :edge-join-invalid-bind {:pattern i}))
               (doseq [role (keys bind)]
                 (when-not (contains? roles role)
                   (edge-join-error :edge-join-unknown-role
                                    {:pattern i :edge-type edge-type :role role
                                     :allowed (vec (sort roles))})))
               {:edge-type edge-type
                :bind (into {} (map (fn [[role var]] [role (logic-var-name var)])) bind)}))
           (range) patterns)
          vars (set (mapcat (comp vals :bind) normalized))
          where (or where {})]
      (when-not (map? where)
        (edge-join-error :edge-join-invalid-where {:provided (pr-str where)}))
      (let [where (into {}
                        (map (fn [[var value]]
                               (let [var (logic-var-name var)]
                                 (when-not (contains? vars var)
                                   (edge-join-error :edge-join-unknown-variable
                                                    {:variable var}))
                                 (when-not (string? value)
                                   (edge-join-error :edge-join-invalid-constant
                                                    {:variable var :provided (pr-str value)}))
                                 [var value])))
                        where)]
        {:patterns normalized :where where :limit limit}))))

(defn- compile-edge-join
  "Compile validated patterns into one parameterised XTQL `unify`.  Caller
  variable spellings are canonicalised to v0, v1, ... so arbitrary names do
  not create an unbounded XTDB compiled-plan cache."
  [{:keys [patterns where limit]}]
  (let [user-vars (vec (distinct (mapcat (comp vals :bind) patterns)))
        canonical (zipmap user-vars (map #(symbol (str "v" %)) (range)))
        edge-vars (mapv #(symbol (str "edge" %)) (range (count patterns)))
        type-params (mapv #(symbol (str "type" %)) (range (count patterns)))
        version-params (mapv #(symbol (str "version" %)) (range (count patterns)))
        where-vars (vec (sort (keys where)))
        where-params (mapv #(symbol (str "constant" %)) (range (count where-vars)))
        limit-param 'result-limit
        froms (mapv
               (fn [i {:keys [bind]}]
                 (list 'from :arxana_clusions
                       (vec (concat [{:xt/id (nth edge-vars i)}
                                     {:clusion/type (nth type-params i)}
                                     {:clusion/type-version (nth version-params i)}]
                                    (map (fn [[role var]]
                                           {(keyword "role" (name role)) (get canonical var)})
                                         (sort-by (comp name key) bind))))))
               (range) patterns)
        wheres (mapv (fn [var param]
                       (list 'where (list '= (get canonical var) param)))
                     where-vars where-params)
        body (list '->
                   (apply list 'unify (concat froms wheres))
                   (apply list 'return (concat (vals canonical) edge-vars))
                   (list 'limit limit-param))
        params (vec (concat type-params version-params where-params [limit-param]))
        args (vec (concat (map (comp first :edge-type) patterns)
                          (map (comp second :edge-type) patterns)
                          (map where where-vars)
                          [(inc limit)]))]
    {:query (apply fxt/pq params body args)
     :canonical canonical :edge-vars edge-vars :limit limit}))

(defn edge-join
  "Run a bounded 2..4-pattern equality join over flat Arxana clusion columns.
  Shared caller variable names are equality joins.  Returns normalized keyword
  bindings plus the contributing clusion ids; no submitted query text is ever
  evaluated."
  [node request]
  (let [{:keys [query canonical edge-vars limit]} (compile-edge-join
                                                   (normalize-edge-join request))
        started (System/nanoTime)
        rows+ (fxt/timed-q node query)
        ms (long (/ (- (System/nanoTime) started) 1000000))
        truncated? (> (count rows+) limit)
        rows (mapv (fn [row]
                     (assoc (into {} (map (fn [[user-var query-var]]
                                           [(keyword user-var) (get row (keyword (name query-var)))])
                                         canonical))
                            :edges (mapv #(get row (keyword (name %))) edge-vars)))
                   (take limit rows+))]
    {:status :ok :rows rows :row-count (count rows)
     :truncated? truncated? :limit limit :ms ms}))

(defn newest-evidence
  "The newest attestation on SUBJECT-ID, optionally restricted to KIND
  (the attest/kind column; nil means any). Newest by valid time (record
  event-time when supplied, otherwise ingestion time), tie-broken by id."
  ([node subject-id] (newest-evidence node subject-id nil))
  ([node subject-id kind]
   (let [where-clause (if (some? kind)
                        (list 'where
                              (list '= 'subject/clusion-id 'p-subject)
                              (list '= 'attest/kind 'p-kind))
                        (list 'where
                              (list '= 'subject/clusion-id 'p-subject)))]
     (first
      (fxt/timed-q node
                   (fxt/pq '[p-subject p-kind]
                           (list '->
                                 (list 'from :arxana_evidence
                                       '[xt/id subject/clusion-id attest/kind
                                         attest/basis attest/standing
                                         xt/valid-from])
                                 where-clause
                                 (list 'order-by
                                       {:val 'xt/valid-from :dir :desc}
                                       {:val 'xt/id :dir :desc})
                                 (list 'limit 1))
                           subject-id kind))))))

;; ---------------------------------------------------------------------------
;; Envelope + command validation (D3: unknown fields rejected in v1).
;; ---------------------------------------------------------------------------

(defn- sha256-hex [^String s]
  (let [digest (.digest (MessageDigest/getInstance "SHA-256")
                        (.getBytes s StandardCharsets/UTF_8))]
    (apply str (map #(format "%02x" %) digest))))

(def ^:private envelope-keys
  #{:protocol/version :message/id :operation :principal/id :sent-at :body})

(def ^:private body-keys
  #{:target/id :expected/revision-id :command})
(def ^:private append-body-keys #{:record})

(def ^:private command-keys
  #{:revision/canonical-bytes :revision/media-type :entity-records :hyperedge-records})

(defn- absent-marker? [v]
  (or (nil? v) (= :absent v) (= ":absent" v) (= "absent" v)))

(defn- check-no-unknown-keys! [where allowed m]
  (doseq [k (keys m)]
    (when-not (contains? allowed k)
      (throw (gates/layered-error 4 :arxana-unknown-field
                                  {:category :invalid :where where :field k
                                   :allowed allowed})))))

(defn- arxana-entity-doc
  "Build one live-envelope entity doc for an arxana/* record. Ids stay inside
  the reserved identity space so the generic retract gate (gates/arxana-id?)
  also refuses them."
  [record]
  (when-not (map? record)
    (throw (gates/layered-error 4 :arxana-invalid-record {:record record})))
  (let [type (:type record)
        name' (:name record)]
    (when-not (gates/arxana-namespaced? type)
      (throw (gates/layered-error 4 :arxana-invalid-record-type
                                  {:category :invalid :type type
                                   :message "conditional route writes arxana/* records only"})))
    (when (str/blank? (str name'))
      (throw (gates/layered-error 4 :missing-required {:required [:name]})))
    (let [id (:id record)]
      (when (and (some? id) (not (gates/arxana-id? (str id))))
        (throw (gates/layered-error 4 :arxana-invalid-record-id
                                    {:category :invalid :id id
                                     :message "arxana entity ids must live in the arxana: identity space"})))
      (let [id (or (some-> id str) (str "arxana:" (random-uuid)))]
        (cond-> {:xt/id id :entity/id id :entity/name name'
                 :entity/type (keyword (str/replace (str type) #"^:" ""))}
          (:external-id record) (assoc :entity/external-id (:external-id record))
          (:source record) (assoc :entity/source (:source record))
          (map? (:props record)) (assoc :entity/props (:props record)))))))

(defn- arxana-hyperedge-doc
  "Build one live-envelope hyperedge doc (same stable-id rule as the generic
  route: hx:<type>:<sorted-endpoints>); type must be arxana/*."
  [record]
  (when-not (map? record)
    (throw (gates/layered-error 4 :arxana-invalid-record {:record record})))
  (let [hx-type (:hx/type record)
        endpoints (:hx/endpoints record)]
    (when-not (gates/arxana-namespaced? hx-type)
      (throw (gates/layered-error 4 :arxana-invalid-record-type
                                  {:category :invalid :hx/type hx-type
                                   :message "conditional route writes arxana/* records only"})))
    (when-not (and (sequential? endpoints) (seq endpoints))
      (throw (gates/layered-error 4 :missing-required {:required [:hx/endpoints]})))
    (let [type-name (str/replace (str hx-type) #"^:" "")
          endpoints (mapv str endpoints)
          id (or (some-> (:hx/id record) str)
                 (str "hx:" type-name ":" (str/join "." (sort endpoints))))]
      (cond-> {:xt/id id :hx/id id
               :hx/type (keyword type-name)
               :hx/endpoints endpoints
               :hx/ends (mapv (fn [e] {:entity-id e}) endpoints)}
        (map? (:hx/props record)) (assoc :hx/props (:hx/props record))))))

(defn- validate-revision-envelope
  "Validate the D3 v1 envelope and the revision/ingest body. Returns
  {:message-id :operation :target-id :expected (nil = :absent) :bytes
   :media-type :entity-docs :hyperedge-docs}."
  [payload]
  (when-not (map? payload)
    (throw (gates/layered-error 4 :arxana-invalid-envelope {:category :invalid})))
  (check-no-unknown-keys! :envelope envelope-keys payload)
  (when-not (= protocol-version (:protocol/version payload))
    (throw (gates/layered-error 4 :arxana-unsupported-version
                                {:category :unsupported
                                 :required protocol-version
                                 :got (:protocol/version payload)})))
  (let [message-id (:message/id payload)
        operation (:operation payload)
        body (:body payload)]
    (when-not (and (string? message-id) (not (str/blank? message-id)))
      (throw (gates/layered-error 4 :missing-required {:required [:message/id]})))
    (when-not (map? body)
      (throw (gates/layered-error 4 :missing-required {:required [:body]})))
    (check-no-unknown-keys! :body body-keys body)
    (let [target-id (:target/id body)]
      (when-not (and (string? target-id) (not (str/blank? target-id)))
        (throw (gates/layered-error 4 :missing-required {:required [:target/id]})))
      (when-not (contains? body :expected/revision-id)
        (throw (gates/layered-error 4 :missing-required {:required [:expected/revision-id]})))
      (let [expected (:expected/revision-id body)
            command (:command body)]
        (when-not (or (absent-marker? expected)
                      (and (string? expected) (not (str/blank? expected))))
          (throw (gates/layered-error 4 :arxana-invalid-expected-revision
                                      {:category :invalid :got expected
                                       :expected "revision id string or :absent"})))
        (when-not (map? command)
          (throw (gates/layered-error 4 :missing-required {:required [:command]})))
        (check-no-unknown-keys! :command command-keys command)
        (let [bytes (:revision/canonical-bytes command)]
          (when-not (and (string? bytes) (not (str/blank? bytes)))
            (throw (gates/layered-error 4 :missing-required
                                        {:required [:revision/canonical-bytes]})))
          {:message-id message-id
           :operation operation
           :target-id target-id
           :expected (when-not (absent-marker? expected) expected)
           :bytes bytes
           :media-type (or (:revision/media-type command) "application/octet-stream")
           :entity-docs (mapv arxana-entity-doc (:entity-records command))
           :hyperedge-docs (mapv arxana-hyperedge-doc (:hyperedge-records command))})))))

(def ^:private append-spec
  {:clusion/assert {:prefix "arxana:clusion:sha256:" :table :arxana_clusions}
   :evidence/append {:prefix "arxana:attestation:sha256:" :table :arxana_evidence}
   :assessment/append {:prefix "arxana:record:sha256:" :table :arxana_assessments}
   :attempt/append {:prefix "arxana:record:sha256:" :table :arxana_attempts}
   :external-reference/assert {:prefix "arxana:record:sha256:" :table :arxana_external_refs}
   :mark-occurrence/assert {:prefix "arxana:record:sha256:" :table :arxana_mark_occurrences}})

(defn- validate-append-envelope [payload]
  (let [operation (:operation payload)
        body (:body payload)
        spec (get append-spec operation)]
    (when-not (map? body)
      (throw (gates/layered-error 4 :missing-required {:required [:body]})))
    (check-no-unknown-keys! :body append-body-keys body)
    (let [record (:record body)
          id (:id record)]
      (when-not (map? record)
        (throw (gates/layered-error 4 :arxana-invalid-record {:category :invalid})))
      (when-not (and (string? id) (str/starts-with? id (:prefix spec)))
        (throw (gates/layered-error 4 :arxana-invalid-record-id
                                    {:category :invalid :id id :operation operation})))
      (when (and (#{:evidence/append :assessment/append} operation)
                 (not (string? (:subject-clusion-id record))))
        (throw (gates/layered-error 4 :missing-required
                                    {:required [:subject-clusion-id]})))
      {:message-id (:message/id payload) :operation operation
       :record record :record-id id :table (:table spec)
       :subject-id (or (:subject-clusion-id record)
                       (when (= operation :attempt/append) (:subject/id record)))})))

(def batch-limit 500)

(defn- at-record [index id f]
  (try (f)
       (catch clojure.lang.ExceptionInfo e
         (throw (ex-info (.getMessage e)
                         (update-in (ex-data e) [:error :context]
                                    merge {:record/index index :record/id id}) e)))))

(defn- validate-batch [payload]
  (let [body (:body payload) rows (:records body)]
    (when-not (map? body)
      (throw (gates/layered-error 4 :arxana-invalid-batch {:category :invalid})))
    (check-no-unknown-keys! :body #{:records} body)
    (when-not (and (vector? rows) (<= 1 (count rows) batch-limit))
      (throw (gates/layered-error 4 :arxana-batch-size
                                  {:category :invalid :limit batch-limit})))
    {:message-id (:message/id payload) :operation :records/append
     :records
     (mapv (fn [i row]
             (at-record i (get-in row [:record :id])
                        #(do
                           (when-not (map? row)
                             (throw (gates/layered-error 4 :arxana-invalid-record {:category :invalid})))
                           (check-no-unknown-keys! :batch-record #{:operation :record} row)
                           (when-not (contains? append-spec (:operation row))
                             (throw (gates/layered-error 4 :arxana-unsupported-operation
                                                         {:category :invalid})))
                           (validate-append-envelope
                            {:operation (:operation row) :body {:record (:record row)}}))))
           (range) rows)}))

(defn- validate-envelope [payload]
  (when-not (map? payload)
    (throw (gates/layered-error 4 :arxana-invalid-envelope {:category :invalid})))
  (check-no-unknown-keys! :envelope envelope-keys payload)
  (when-not (= protocol-version (:protocol/version payload))
    (throw (gates/layered-error 4 :arxana-unsupported-version
                                {:category :unsupported :required protocol-version
                                 :got (:protocol/version payload)})))
  (when-not (and (string? (:message/id payload))
                 (not (str/blank? (:message/id payload))))
    (throw (gates/layered-error 4 :missing-required {:required [:message/id]})))
  (cond
    (= :records/append (:operation payload)) (validate-batch payload)
    (= :revision/ingest (:operation payload)) (validate-revision-envelope payload)
    (append-spec (:operation payload)) (validate-append-envelope payload)
    :else (throw (gates/layered-error 4 :arxana-unsupported-operation
                                     {:category :unsupported
                                      :operation (:operation payload)
                                      :supported (vec (cons :revision/ingest (keys append-spec)))}))))

;; ---------------------------------------------------------------------------
;; The conditional transaction (V3 S1 semantics).
;; ---------------------------------------------------------------------------

(defn- stale-refusal
  "D3 stale refusal: current head, submitted base, recoverable command
  reference and safe choices; never a silent retry against a new base.
  L1 (409): the precondition is no longer current."
  [node message-id {:keys [target-id expected]}]
  (throw
   (gates/layered-error
    1 :arxana-stale-head
    {:category :stale
     :code :arxana/stale-head
     :message-id message-id
     :target/id target-id
     :current-head (current-head node target-id)
     :submitted-base (or expected :absent)
     :recoverable {:journal-ref [:arxana_journal message-id]
                   :choices [:refresh :rebase-and-resubmit :export-patch]}})))


(defn- index-committed-hyperedges!
  "Index hyperedge docs an Arxana command has just committed, as every other
   hyperedge write path does (hx/on-put! journals a failure for repair), and
   drop cached type listings. Before 2026-11-14 these puts never reached the
   hyperedge sidecar; no client sent :hyperedge-records then, so nothing was
   missing (0 of 3,733 outcomes), but the next one would have been invisible
   to index-served reads. Never throws: the command is already committed."
  [docs]
  (doseq [d docs]
    (try
      (when-let [f (hx/on-put! d)] @f)
      (when-let [inv (requiring-resolve 'futon1b-graph/invalidate-hyperedge-query-cache!)]
        (inv (:hx/type d)))
      (catch Throwable _ nil))))

(defn- conditional-revision-command!
  "Execute one authenticated conditional command. Returns the D3 response
  envelope; a duplicate message id returns the stored original outcome with
  :duplicate? true."
  [node cmd]
  (let [{:keys [message-id operation]} cmd]
    (if-let [outcome (outcome-by-message node message-id)]
      ;; Idempotent retry: return the original outcome, never reapply.
      (assoc (edn/read-string (:outcome/response outcome)) :duplicate? true)
      (let [digest (sha256-hex (:bytes cmd))
            revision-id (str "arxana:revision:sha256:" digest)
            committed-at (str (Instant/now))
            response (cond-> {:protocol/version protocol-version
                              :message/id message-id
                              :operation operation
                              :ok {:target/id (:target-id cmd)
                                   :revision/id revision-id
                                   :head/revision-id revision-id
                                   :revision/digest digest}}
                       (seq (:entity-docs cmd)) (assoc-in [:ok :entity/ids]
                                                          (mapv :xt/id (:entity-docs cmd)))
                       (seq (:hyperedge-docs cmd)) (assoc-in [:ok :hyperedge/ids]
                                                             (mapv :xt/id (:hyperedge-docs cmd))))
            head-assert (if (nil? (:expected cmd))
                          [:sql "ASSERT NOT EXISTS (SELECT 1 FROM arxana_heads WHERE _id = ?)"
                           [(:target-id cmd)]]
                          ;; XTDB 2 SQL column naming: :head/revision-id is
                          ;; head$revision_id (namespace '$', '-' → '_').
                          [:sql "ASSERT EXISTS (SELECT 1 FROM arxana_heads WHERE _id = ? AND head$revision_id = ?)"
                           [(:target-id cmd) (:expected cmd)]])
            ops (into [head-assert
                       [:sql "ASSERT NOT EXISTS (SELECT 1 FROM arxana_outcomes WHERE _id = ?)"
                        [message-id]]
                       [:put-docs :arxana_revisions
                        {:xt/id revision-id
                         :revision/target-id (:target-id cmd)
                         :revision/digest digest
                         :revision/algorithm :sha256
                         :revision/media-type (:media-type cmd)
                         :revision/canonical-bytes (:bytes cmd)
                         :revision/committed-at committed-at}]
                       [:put-docs :arxana_heads
                        {:xt/id (:target-id cmd)
                         :head/revision-id revision-id
                         :head/message-id message-id}]
                       [:put-docs :arxana_journal
                        {:xt/id message-id
                         :journal/message-id message-id
                         :journal/target-id (:target-id cmd)
                         :journal/expected-revision (or (:expected cmd) ":absent")
                         :journal/new-revision revision-id
                         :journal/committed-at committed-at}]
                       [:put-docs :arxana_outcomes
                        {:xt/id message-id
                         :outcome/result :committed
                         :outcome/target-id (:target-id cmd)
                         :outcome/response (pr-str response)}]]
                      (concat (map (fn [d] [:put-docs :entities d]) (:entity-docs cmd))
                              (map (fn [d] [:put-docs :hyperedges d]) (:hyperedge-docs cmd))))]
        (try
          ;; `execute-tx` blocks until this node has indexed the transaction;
          ;; a normal return is therefore the durable commit receipt.  Do not
          ;; immediately re-query through `fxt/safe-q` here: on a table's
          ;; first appearance its missing-table compatibility mapping can
          ;; transiently return [], turning a committed command into a false
          ;; 503 even though the head/outcome become visible to the caller.
          (xt/execute-tx node ops {:metadata {:arxana/message-id message-id
                                              :arxana/target-id (:target-id cmd)}})
          (index-committed-hyperedges! (:hyperedge-docs cmd))
          (catch Throwable t
            ;; V3 S1: an ASSERT failure aborts the whole transaction. Two
            ;; causes reach here: the head precondition (stale) or the
            ;; outcome precondition (a concurrent duplicate that won). A
            ;; stored outcome means the latter; otherwise stale — IF the
            ;; failure was an assertion. Anything else is a real fault.
            (if (re-find #"(?i)assert" (str (.getMessage t)))
              (if-let [outcome (outcome-by-message node message-id)]
                (assoc (edn/read-string (:outcome/response outcome)) :duplicate? true)
                (stale-refusal node message-id cmd))
              (throw t))))
        response))))

(defn- record-put-target
  "An act's event-time is its XTDB valid-from, not its ingestion time.
  Undated records retain the ordinary put-docs target. Future acts are refused
  before any transaction, since current-time immutable/index reads must see
  every published record. The journal and index events keep ingestion time."
  [table record]
  (if-not (contains? record :event-time)
    table
    (let [value (:event-time record)
          instant (parse-record-instant value :arxana-invalid-event-time
                                        {:record/id (:id record) :event-time value})]
      (when (.isAfter ^Instant instant (Instant/now))
        (throw (gates/layered-error 4 :arxana-future-event-time
                                    {:category :invalid :record/id (:id record)
                                     :event-time value})))
      {:into table :valid-from instant})))

(def record-kind-tables
  "Closed reserved-record vocabulary; callers never supply table names."
  {:clusion :arxana_clusions :external-reference :arxana_external_refs
   :attestation :arxana_evidence :assessment :arxana_assessments
   :attempt :arxana_attempts :mark-occurrence :arxana_mark_occurrences})

(defn- record-fields [kind record]
  (if (= kind :clusion)
    (:asserted-content record)
    ;; Top-level values remain searchable whole. Also expose one map level
    ;; through lossless paths, e.g. [:method :check/id], not ambiguous dotted
    ;; keywords. Deeper maps remain whole canonical EDN values.
    (into (dissoc record :id :transport)
          (for [[parent value] (dissoc record :id :transport)
                :when (map? value) [child v] value]
            [[parent child] v]))))

(defn record-field-rows
  "Uniform reserved-record projection in the historical arxana_clusion_fields
  table. Clusions use asserted-content; other kinds use top-level fields (except
  id/transport) plus one map level as two-element field paths. Scalar vectors
  expand; maps/nested values are canonical EDN, not element-searchable.
  Numeric elements additionally retain their numeric type in :field/number."
  [kind record]
  (let [[type version] (:type-revision record)
        scalar? #(or (keyword? %) (string? %) (number? %) (boolean? %))]
    (vec
     (for [[field value] (sort-by (comp pr-str key) (record-fields kind record))
           element (distinct (if (and (vector? value) (every? scalar? value)) value [value]))]
       (cond-> {:xt/id (sha256-hex (canonical-record-data
                                  (if (= kind :clusion) [(:id record) field element]
                                      [kind (:id record) field element])))
                :record/id (:id record) :record/kind kind :record/table (record-kind-tables kind)
                :field/name field :field/value (canonical-record-data element)}
         (= kind :clusion) (assoc :clusion/type type :clusion/type-version version)
         (number? element) (assoc :field/number element))))))

(defn clusion-field-rows [record] (record-field-rows :clusion record))

(defn- field-put-ops [operation record target]
  (when-let [table (:table (append-spec operation))]
    (let [kind (some (fn [[kind t]] (when (= table t) kind)) record-kind-tables)
          rows (record-field-rows kind record)
          target (if (map? target) (assoc target :into :arxana_clusion_fields)
                     :arxana_clusion_fields)]
      (when (seq rows) [(into [:put-docs target] rows)]))))

(defn- query-coordinate [{:keys [type-revision limit valid-time record-kind]
                          :or {record-kind :clusion}}]
  (when-not (contains? record-kind-tables record-kind)
    (throw (gates/layered-error 4 :arxana-invalid-record-kind {:category :invalid :record-kind record-kind})))
  (when (and (= record-kind :clusion)
             (not (and (vector? type-revision) (= 2 (count type-revision))
                       (keyword? (first type-revision)) (pos-int? (second type-revision)))))
    (throw (gates/layered-error 4 :arxana-invalid-type-revision {:category :invalid :type-revision type-revision})))
  (when (and (not= record-kind :clusion) type-revision)
    (throw (gates/layered-error 4 :arxana-type-not-applicable {:category :invalid})))
  (when-not (or (nil? limit) (integer? limit))
    (throw (gates/layered-error 4 :arxana-invalid-limit {:category :invalid :limit limit})))
  {:type (first type-revision) :version (second type-revision) :kind record-kind
   :limit (min 200 (max 1 (or limit 50)))
   :at (if valid-time (validate-valid-time valid-time) (Instant/now))})

(defn- field-coordinate? [f]
  (or (keyword? f) (and (vector? f) (= 2 (count f)) (every? keyword? f))))

(defn- range-bounds [request]
  (doseq [key [:lower :upper]]
    (let [bound (get request key)]
      (when-not (and (map? bound) (number? (:value bound))
                     (boolean? (:inclusive? bound))
                     (Double/isFinite (double (:value bound))))
        (throw (gates/layered-error 4 :arxana-invalid-range
                                    {:category :invalid :bound key :value bound})))))
  (when (> (get-in request [:lower :value]) (get-in request [:upper :value]))
    (throw (gates/layered-error 4 :arxana-invalid-range {:category :invalid :reason :reversed})))
  request)

(defn- query-records [node request mode]
  (let [{:keys [type version limit at kind]} (query-coordinate request)
        field? (not= mode :type)
        range? (= mode :range)
        _ (when (and field? (not (field-coordinate? (:field request))))
            (throw (gates/layered-error 4 :arxana-invalid-field {:category :invalid})))
        _ (when range? (range-bounds request))
        source (source-sequence node)
        typed? (= kind :clusion)
        base (list 'from (record-kind-tables kind)
                   {:bind (cond-> '[{:xt/id rid} {:record/data data}]
                            typed? (into '[{:clusion/type p-type} {:clusion/type-version p-version}]))
                    :for-valid-time '(at p-at)})
        projection (list 'from :arxana_clusion_fields
                         {:bind (cond-> '[{:record/id rid} {:record/kind p-kind} {:field/name p-field}]
                                  range? (conj '{:field/number number})
                                  (not range?) (conj '{:field/value p-value}))
                          :for-valid-time '(at p-at)})
        conditions (when range?
                     [(list 'where
                            (list (if (get-in request [:lower :inclusive?]) '>= '>) 'number 'p-lower)
                            (list (if (get-in request [:upper :inclusive?]) '<= '<) 'number 'p-upper))])
        start (if field? (apply list 'unify (concat [base projection] conditions)) base)
        ;; A numeric vector may match several rows: deduplicate before limit.
        body (list '-> start '(aggregate rid data) '(order-by rid) '(limit p-limit))
        params (cond-> '[p-at p-limit]
                 typed? (into '[p-type p-version])
                 field? (into '[p-kind p-field])
                 (= mode :equal) (conj 'p-value)
                 range? (into '[p-lower p-upper]))
        args (cond-> [at (inc limit)]
               typed? (into [type version])
               field? (into [kind (:field request)])
               (= mode :equal) (conj (canonical-record-data (:value request)))
               range? (into [(get-in request [:lower :value]) (get-in request [:upper :value])]))
        rows (fxt/timed-q node (apply fxt/pq params body args))
        after (source-sequence node)]
    (if (not= source after)
      {:status :stale-index :source/watermark after :index/watermark source}
      {:status :ok :records (mapv #(edn/read-string (:data %)) (take limit rows))
       :limit limit :truncated? (> (count rows) limit) :watermark source :valid-time (str at)})))

(defn records-by-type [node request]
  (query-records node (assoc request :record-kind :clusion) :type))
(defn records-by-field [node request] (query-records node request :equal))
(defn records-by-field-range [node request] (query-records node request :range))

(defn type-census
  "XTQL counts at one valid/system time, bounded by type buckets, not records.
  Non-clusion kinds with zero records have a zero bucket."
  [node request]
  (let [{:keys [limit at]} (query-coordinate (assoc request :record-kind :attestation :type-revision nil))
        system-at (Instant/now)
        rows (mapcat
              (fn [[kind table]]
                (let [clusion? (= kind :clusion)
                      body (list '->
                                 (list 'from table
                                       {:bind (if clusion? '[clusion/type clusion/type-version] '[xt/id])
                                        :for-valid-time '(at p-at) :for-system-time '(at p-system)})
                                 (if clusion? '(aggregate clusion/type clusion/type-version {:n (row-count)})
                                     '(aggregate {:n (row-count)}))
                                 (if clusion? '(order-by clusion/type clusion/type-version) '(order-by n))
                                 '(limit p-limit))
                      buckets (fxt/timed-q node (fxt/pq '[p-at p-system p-limit] body at system-at (inc limit)))]
                  (if clusion?
                    (map (fn [r] {:record-kind kind :type-revision [(:clusion/type r) (:clusion/type-version r)]
                                  :count (:n r)}) buckets)
                    [{:record-kind kind :count (or (:n (first buckets)) 0)}])))
              (sort-by key record-kind-tables))
        ordered (sort-by (juxt :record-kind (comp pr-str :type-revision)) rows)]
    {:status :ok :counts (vec (take limit ordered)) :limit limit :truncated? (> (count ordered) limit)
     :valid-time (str at) :system-time (str system-at) :watermark (source-sequence node)}))

(defn backfill-record-fields!
  "Explicit maintenance across all reserved kinds and valid-time intervals.
  Upgrades prior projection rows in place; no authoritative record is rewritten.
  A repeated completed backfill executes no transaction."
  [node]
  (let [existing (set (fxt/timed-q node
                                  '(from :arxana_clusion_fields
                                         {:bind [* xt/valid-from xt/valid-to] :for-valid-time :all-time})))
        desired (for [[kind table] (sort-by key record-kind-tables)
                      r (fxt/timed-q node (list 'from table
                                               {:bind '[record/data xt/valid-from xt/valid-to]
                                                :for-valid-time :all-time}))
                      row (record-field-rows kind (edn/read-string (:record/data r)))]
                  (merge row (select-keys r [:xt/valid-from :xt/valid-to])))
        missing (remove existing desired)
        ops (mapv (fn [row]
                    [:put-docs (cond-> {:into :arxana_clusion_fields
                                        :valid-from (.toInstant ^java.time.ZonedDateTime (:xt/valid-from row))}
                                 (:xt/valid-to row)
                                 (assoc :valid-to (.toInstant ^java.time.ZonedDateTime (:xt/valid-to row))))
                     (dissoc row :xt/valid-from :xt/valid-to)]) missing)]
    (when (seq ops) (xt/execute-tx node ops))
    {:status :ok :rows-written (count ops)}))

(defn backfill-clusion-fields!
  "Compatibility entry point; now backfills every reserved record kind."
  [node] (backfill-record-fields! node))

(defn- conditional-append-command! [node {:keys [message-id operation record
                                                  record-id table subject-id]}]
  (if-let [outcome (outcome-by-message node message-id)]
    (assoc (edn/read-string (:outcome/response outcome)) :duplicate? true)
    (let [put-target (record-put-target table record)
          existing (stored-record node table record-id)
          _ (when (and existing (not= existing record))
              (throw (gates/layered-error 4 :arxana-immutable-record-conflict
                                          {:category :conflict :record/id record-id})))
          _ (when (and subject-id (nil? (clusion-by-id node subject-id)))
              (throw (gates/layered-error 4 :arxana-unknown-clusion
                                          {:category :missing
                                           :subject-clusion-id subject-id})))
          response {:protocol/version protocol-version :message/id message-id
                    :operation operation
                    :ok {:record/id record-id :subject-clusion-id subject-id
                         :created? (nil? existing)}}
          doc (cond-> (merge {:xt/id record-id :record/data (canonical-record-data record)
                              :record/operation operation}
                             (flat-columns operation record))
                subject-id (assoc :subject/clusion-id subject-id))
          table-name (name table)
          immutable-assert (format (str "ASSERT (NOT EXISTS (SELECT 1 FROM %s WHERE _id = ?) "
                                         "OR EXISTS (SELECT 1 FROM %s WHERE _id = ? AND record$data = ?))")
                                   table-name table-name)
          indexed? (#{:clusion/assert :evidence/append :assessment/append
                       :attempt/append} operation)]
      (loop [attempt 0]
        (let [counter (when indexed? (source-sequence node))
              next-counter (some-> counter inc)
              counter-assert (when indexed?
                               (if (zero? counter)
                                 [:sql "ASSERT NOT EXISTS (SELECT 1 FROM arxana_index_state WHERE _id='source')"]
                                 [:sql "ASSERT EXISTS (SELECT 1 FROM arxana_index_state WHERE _id='source' AND source$sequence=?)"
                                  [counter]]))
              ops (vec
                   (concat
                    [[:sql "ASSERT NOT EXISTS (SELECT 1 FROM arxana_outcomes WHERE _id = ?)"
                      [message-id]]
                     [:sql immutable-assert [record-id record-id (canonical-record-data record)]]]
                    (when subject-id
                      [[:sql "ASSERT EXISTS (SELECT 1 FROM arxana_clusions WHERE _id = ?)"
                        [subject-id]]])
                    (when indexed? [counter-assert])
                    [[:put-docs put-target doc]]
                    (field-put-ops operation record put-target)
                    (when indexed?
                      [[:put-docs :arxana_index_events
                        {:xt/id next-counter :source/sequence next-counter
                         :record/id record-id :record/table table}]
                       [:put-docs :arxana_index_state
                        {:xt/id "source" :source/sequence next-counter}]])
                    [[:put-docs :arxana_outcomes
                      {:xt/id message-id :outcome/result :committed
                       :outcome/response (pr-str response)}]]))
              result (try
                       (xt/execute-tx node ops {:metadata {:arxana/message-id message-id
                                                           :arxana/record-id record-id}})
                       {:committed response}
                       (catch Throwable t {:thrown t}))]
          (if-let [t (:thrown result)]
            (if (re-find #"(?i)assert" (str (.getMessage ^Throwable t)))
              (if-let [outcome (outcome-by-message node message-id)]
                (assoc (edn/read-string (:outcome/response outcome)) :duplicate? true)
                (if (and indexed? (< attempt 32))
                  (recur (inc attempt))
                  (throw (gates/layered-error 4 :arxana-append-precondition
                                              {:category :conflict
                                               :record/id record-id
                                               :subject-clusion-id subject-id}))))
              (throw t))
            (:committed result)))))))

(defn- batch-plan [node records counter]
  ;; Validate against persisted content plus earlier batch entries. SQL
  ;; assertions repeat these checks inside the transaction to close races.
  (reduce
   (fn [{:keys [seen sequence] :as plan} [i {:keys [record record-id table subject-id operation]}]]
     (at-record
      i record-id
      (fn []
        (let [put-target (record-put-target table record)
              existing (or (get seen [table record-id]) (stored-record node table record-id))
              _ (when (and existing (not= existing record))
                  (throw (gates/layered-error 4 :arxana-immutable-record-conflict
                                              {:category :conflict})))
              _ (when (and subject-id
                           (not (or (get seen [:arxana_clusions subject-id])
                                    (clusion-by-id node subject-id))))
                  (throw (gates/layered-error 4 :arxana-unknown-clusion
                                              {:category :missing})))
              ;; An identical existing record is a success with nothing to
              ;; write: no new document version, no index event (claude-13,
              ;; so idempotent re-runs of a backfill do not grow history).
              indexed? (and (nil? existing)
                            (#{:clusion/assert :evidence/append :assessment/append :attempt/append} operation))
              n (if indexed? (inc sequence) sequence)
              doc (cond-> (merge {:xt/id record-id :record/data (canonical-record-data record)
                                  :record/operation operation} (flat-columns operation record))
                    subject-id (assoc :subject/clusion-id subject-id))
              assertion (format (str "ASSERT (NOT EXISTS (SELECT 1 FROM %s WHERE _id = ?) "
                                     "OR EXISTS (SELECT 1 FROM %s WHERE _id = ? AND record$data = ?))")
                                (name table) (name table))]
          (-> plan
              (assoc :sequence n)
              (assoc-in [:seen [table record-id]] record)
              (update :results conj {:record/id record-id :subject-clusion-id subject-id
                                     :created? (nil? existing)})
              (update :ops into
                      (concat [[:sql assertion [record-id record-id (canonical-record-data record)]]]
                              (when subject-id
                                [[:sql "ASSERT EXISTS (SELECT 1 FROM arxana_clusions WHERE _id = ?)" [subject-id]]])
                              (when (nil? existing)
                                (concat [[:put-docs put-target doc]]
                                        (field-put-ops operation record put-target)))
                              (when indexed?
                                [[:put-docs :arxana_index_events
                                  {:xt/id n :source/sequence n :record/id record-id :record/table table}]]))))))))
   {:seen {} :sequence counter :results [] :ops []} (map-indexed vector records)))

(defn- conditional-batch-command! [node {:keys [message-id operation records]}]
  (loop [attempt 0]
    (if-let [outcome (outcome-by-message node message-id)]
      (assoc (edn/read-string (:outcome/response outcome)) :duplicate? true)
      (let [counter (source-sequence node)
            plan (batch-plan node records counter)
            response {:protocol/version protocol-version :message/id message-id
                      :operation operation :ok {:records (:results plan)}}
            ops (vec (concat
                      [[:sql "ASSERT NOT EXISTS (SELECT 1 FROM arxana_outcomes WHERE _id = ?)" [message-id]]
                       (if (zero? counter)
                         [:sql "ASSERT NOT EXISTS (SELECT 1 FROM arxana_index_state WHERE _id='source')"]
                         [:sql "ASSERT EXISTS (SELECT 1 FROM arxana_index_state WHERE _id='source' AND source$sequence=?)" [counter]])]
                      (:ops plan)
                      (when (> (:sequence plan) counter)
                        [[:put-docs :arxana_index_state {:xt/id "source" :source/sequence (:sequence plan)}]])
                      [[:put-docs :arxana_journal
                        {:xt/id message-id :journal/message-id message-id :journal/operation operation
                         :journal/record-ids (mapv :record-id records)
                         :journal/committed-at (str (Instant/now))}]
                       [:put-docs :arxana_outcomes {:xt/id message-id :outcome/result :committed
                                                   :outcome/response (pr-str response)}]]))
            error (try (xt/execute-tx node ops {:metadata {:arxana/message-id message-id}}) nil
                       (catch Throwable t t))]
        (if error
          (if (and (< attempt 32) (re-find #"(?i)assert" (str (.getMessage ^Throwable error))))
            ;; Rebuild the plan: an immutable conflict now names its index;
            ;; a racing identical message returns its committed outcome.
            (recur (inc attempt))
            (throw error))
          response)))))

(defn conditional-command!
  "Execute one authenticated, validated reserved command. Revision ingestion
  uses head CAS; Formal Arxana records use immutable append transactions."
  [node payload]
  (let [cmd (validate-envelope payload)]
    (case (:operation cmd)
      :revision/ingest (conditional-revision-command! node cmd)
      :records/append (conditional-batch-command! node cmd)
      (conditional-append-command! node cmd))))
