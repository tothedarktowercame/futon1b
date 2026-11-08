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
            [futon1b-gates :as gates]
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

(defn- stored-record [node table record-id]
  (case table
    :arxana_clusions (clusion-by-id node record-id)
    :arxana_evidence (evidence-by-id node record-id)
    :arxana_assessments (assessment-by-id node record-id)))

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
   :assessment/append {:prefix "arxana:record:sha256:" :table :arxana_assessments}})

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
      (when (and (not= operation :clusion/assert)
                 (not (string? (:subject-clusion-id record))))
        (throw (gates/layered-error 4 :missing-required
                                    {:required [:subject-clusion-id]})))
      {:message-id (:message/id payload) :operation operation
       :record record :record-id id :table (:table spec)
       :subject-id (:subject-clusion-id record)})))

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

(defn- conditional-append-command! [node {:keys [message-id operation record
                                                  record-id table subject-id]}]
  (if-let [outcome (outcome-by-message node message-id)]
    (assoc (edn/read-string (:outcome/response outcome)) :duplicate? true)
    (let [existing (stored-record node table record-id)
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
          doc (cond-> {:xt/id record-id :record/data (pr-str record)
                       :record/operation operation}
                subject-id (assoc :subject/clusion-id subject-id))
          table-name (name table)
          immutable-assert (format (str "ASSERT (NOT EXISTS (SELECT 1 FROM %s WHERE _id = ?) "
                                         "OR EXISTS (SELECT 1 FROM %s WHERE _id = ? AND record$data = ?))")
                                   table-name table-name)
          indexed? (#{:clusion/assert :evidence/append} operation)]
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
                     [:sql immutable-assert [record-id record-id (pr-str record)]]]
                    (when subject-id
                      [[:sql "ASSERT EXISTS (SELECT 1 FROM arxana_clusions WHERE _id = ?)"
                        [subject-id]]])
                    (when indexed? [counter-assert])
                    [[:put-docs table doc]]
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

(defn conditional-command!
  "Execute one authenticated, validated reserved command. Revision ingestion
  uses head CAS; Formal Arxana records use immutable append transactions."
  [node payload]
  (let [cmd (validate-envelope payload)]
    (if (= :revision/ingest (:operation cmd))
      (conditional-revision-command! node cmd)
      (conditional-append-command! node cmd))))
