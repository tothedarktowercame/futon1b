;; futon1b-graph — the A3/A4/A5 slices of E-futon1b-operational-switchover:
;; entities + relations (writes with the full gate stack), hyperedge reads,
;; census, and the type registry, per API-CONTRACT.md §4-§8.
;;
;; Deviations (deliberate, mirrored in API-CONTRACT/README):
;; - success envelopes carry :rescue instead of :tx-id/:path/id (no proof
;;   paths in v1);
;; - relation public :type = :relation/type (futon1a derives it from
;;   provenance :note; no live-path caller reads it);
;; - hyperedge :hx/type is NOT auto-registered as a type doc — matching
;;   futon1a's actual types-from-doc source (entity/relation/intent only),
;;   not the folklore;
;; - entities/latest guards its limit parse (futon1a 500s on a bad value).
(ns futon1b-graph
  (:require [clojure.string :as str]
            [futon1b-gates :as gates]
            [migration.transform :as xf]
            [migration.ingest :as ingest]
            [futon1b-xt :as fxt]
            [xtdb.api :as xt]))

;; ---------------------------------------------------------------------------
;; Shared write plumbing: transform + rescue ladder + verified read-back.
;; ---------------------------------------------------------------------------

(defonce !shape-log (xf/make-shape-log))


(defn put-verified!
  "Transform, put through the rescue ladder, verify by read-back.
  Returns the rescue stage keyword (:ok/:rescued-1/:rescued-2) or throws
  the L0-shaped error (503) if the doc is absent after all stages."
  [node table doc]
  (let [xdoc (xf/transform-doc doc)
        res (ingest/put-doc-with-rescue! node table xdoc !shape-log)]
    (if (fxt/present? node table (:xt/id xdoc))
      (if (keyword? res) res :ok)
      (throw (gates/layered-error 0 :postcommit-missing-entities
                                  {:xt/id (:xt/id xdoc) :table table})))))

;; ---------------------------------------------------------------------------
;; Type registry (A5) — futon1a.model.type-registry ported.
;; ---------------------------------------------------------------------------

(defn- type-id->xt-id [kind type-id]
  (str "type|" (name kind) "|" (if (keyword? type-id) (str type-id) (str type-id))))

(defn- infer-parent [type-id]
  (when (keyword? type-id)
    (when-let [ns' (namespace type-id)]
      (keyword ns'))))

(defn type-doc [{:keys [type-id kind parent aliases]}]
  {:xt/id (type-id->xt-id kind type-id)
   :type/id type-id
   :type/kind kind
   :type/parent (or parent (infer-parent type-id))
   :type/aliases (vec (distinct (filter keyword? (or aliases []))))})

(defn register-types!
  "Idempotent type registration for the types a write introduces (futon1a
  tx-ops-for-docs, minus the every-write re-put: we skip types already
  present so live writes don't accrete identical doc versions)."
  [node kind-type-pairs]
  (doseq [{:keys [kind type-id]} kind-type-pairs
          :when (keyword? type-id)
          td [(type-doc {:type-id type-id :kind kind})
              (when-let [p (infer-parent type-id)]
                (type-doc {:type-id p :kind kind}))]
          :when (and td (not (fxt/present? node :type-catalog (:xt/id td))))]
    (put-verified! node :type-catalog td)))

(defn list-types [node]
  (let [docs (filter :type/kind (fxt/safe-q node '(from :type-catalog [*])))]
    ;; NB unlike the other reads, /types keeps :xt/id (contract §8: pulled [*]).
    {:types (->> docs
                 (sort-by (fn [m] (str (name (:type/kind m)) "|" (str (:type/id m)))))
                 vec)}))

(defn- normalize-type [t]
  (cond (keyword? t) t
        (and (string? t) (str/starts-with? t ":")) (keyword (subs t 1))
        (string? t) (keyword t)
        :else nil))

(defn- elapsed-ms
  [started-ns]
  (/ (double (- (System/nanoTime) started-ns)) 1000000.0))

(defn types-mutate!
  "POST /types/parent and /types/merge (body-only penholder — contract §8).
  op = :parent | :merge."
  [node op payload]
  (gates/authorize! (some-> (:penholder payload) str str/trim not-empty))
  (let [type-id (normalize-type (:type/id payload))
        kind (normalize-type (:type/kind payload))]
    (when-not (and type-id kind)
      (throw (gates/layered-error 4 :missing-required
                                  {:required [:type/id :type/kind]})))
    (when (and (= op :merge) (not (sequential? (:type/aliases payload))))
      (throw (gates/layered-error 4 :invalid-type-aliases
                                  {:got (:type/aliases payload)})))
    (let [existing (fxt/q1 node (fxt/pq '[p-id]
                                        '(-> (from :type-catalog [*])
                                             (where (= xt/id p-id)))
                                        (type-id->xt-id kind type-id)))
          base (or existing (type-doc {:type-id type-id :kind kind}))
          doc (case op
                :parent (assoc base :type/parent (normalize-type (:type/parent payload)))
                :merge (assoc base :type/aliases
                              (vec (distinct (map normalize-type (:type/aliases payload))))))]
      {:ok true :rescue (put-verified! node :type-catalog doc)})))

;; ---------------------------------------------------------------------------
;; Entities (A3) — §5. Ensure-by-name minting + full gate stack.
;; ---------------------------------------------------------------------------

(defn- entities-by-name [node name']
  (fxt/safe-q node (fxt/pq '[p-name]
                           '(-> (from :entities [*])
                                (where (= entity/name p-name)))
                           name')))

(defn fetch-entity
  "futon1a f1g/fetch-entity: :xt/id → :entity/name → :entity/external-id,
  deterministic smallest-id pick on duplicates."
  [node id]
  (or (fxt/q1 node (fxt/pq '[p-id]
                           '(-> (from :entities [*])
                                (where (= xt/id p-id)))
                           id))
      (->> (entities-by-name node id)
           (sort-by #(str (:entity/id %)))
           first)
      (->> (fxt/safe-q node (fxt/pq '[p-id]
                                    '(-> (from :entities [*])
                                         (where (= entity/external-id p-id)))
                                    id))
           (sort-by #(str (:entity/id %)))
           first)))

(defn public-entity
  "futon1a normalize-entity: the compat public shape."
  [doc]
  (cond-> {:id (or (:entity/id doc) (:xt/id doc))
           :name (:entity/name doc)
           :type (:entity/type doc)
           :external-id (:entity/external-id doc)
           :source (:entity/source doc)}
    (:entity/props doc) (assoc :props (:entity/props doc))
    (:media/sha256 doc) (assoc :media/sha256 (:media/sha256 doc))))

(def ^:private retractable-tables
  "Tables that POST /api/alpha/documents/retract will delete from.

  `:relations` added 2026-08-13. It was an oversight, not a policy: the
  deletion body is already table-generic (`[:delete-docs table id]`), the
  post-commit read-back check is generic, and the `:hyperedges`-only follow-up
  work (query-cache invalidation, memory-projection refresh) is guarded by an
  explicit table test, so relations skip it harmlessly.

  Why it mattered: since the watcher began writing patterns as entities, one
  pattern is 15 documents — 1 pattern entity, 7 clause entities, and 7
  `:pattern/has-*` relations. Without `:relations` here, deleting or re-filing
  a pattern could remove 8 of the 15 and strand 7 relations pointing at
  documents that no longer exist. codex-3 correctly refused to implement that
  partial cleanup. See zone.hyperreal.enterprises/2026-08-13-relations-retraction.html

  Cascade (the store inferring which relations belong to a retracted entity)
  was considered and deferred: it would put pattern semantics in the substrate,
  where the caller already knows them. Callers name every document they want
  removed and get atomicity plus read-back verification for free."
  #{:entities :hyperedges :relations})

(declare invalidate-hyperedge-query-cache!
         refresh-memory-projection-component!)

(defn with-memory-projection-mutation
  "Serialize a store mutation and its projection refresh for one node."
  [node f]
  (locking node (f)))

(defn retract-documents!
  "Atomically retract entity/hyperedge documents after validating the whole
  request. Deletes are idempotent; every requested id is read back after the
  transaction so XTDB's silent-drop failure mode cannot report success."
  [node payload]
  (let [requested (:documents payload)]
    (when-not (and (sequential? requested) (seq requested) (every? map? requested))
      (throw (gates/layered-error 4 :invalid-document-retraction
                                  {:expected :non-empty-seq-of-maps})))
    (let [documents
          (->> requested
               (mapv (fn [{:keys [table id]}]
                       (let [table (cond
                                     (keyword? table) table
                                     (string? table) (keyword table)
                                     :else nil)]
                         (when-not (and (contains? retractable-tables table)
                                        (string? id) (not (str/blank? id)))
                           (throw (gates/layered-error
                                   4 :invalid-document-retraction
                                   {:allowed-tables retractable-tables
                                    :document {:table table :id id}})))
                         {:table table :id id})))
               distinct
               vec)]
      (with-memory-projection-mutation
        node
        (fn []
          (xt/execute-tx node
                         (mapv (fn [{:keys [table id]}]
                                 [:delete-docs table id])
                               documents))
          (let [remaining (filterv (fn [{:keys [table id]}]
                                     (fxt/present? node table id))
                                   documents)]
            (when (seq remaining)
              (throw (gates/layered-error 0 :postcommit-retraction-failed
                                          {:remaining remaining}))))
          (when (some #(= :hyperedges (:table %)) documents)
            (invalidate-hyperedge-query-cache!))
          (doseq [{:keys [table id]} documents
                  :when (= :hyperedges table)]
            (refresh-memory-projection-component! node id))
          {:ok true :count (count documents) :documents documents})))))

(defn entity-by-external
  "GET /api/alpha/entity?source=…&external-id=… (contract §5): both params
  required (L4 400); multiple matches → L1 409 :external-id-ambiguous."
  [node {:keys [source external-id]}]
  (when (or (str/blank? (str source)) (str/blank? (str external-id)))
    (throw (gates/layered-error 4 :missing-required
                                {:required [:source :external-id]})))
  (let [matches (fxt/safe-q node (fxt/pq '[p-source p-external-id]
                                         '(-> (from :entities [*])
                                              (where (= entity/source p-source)
                                                     (= entity/external-id p-external-id)))
                                         source external-id))]
    (cond
      (empty? matches)
      [404 {:error {:reason :not-found
                    :identity {:source source :external-id external-id}}}]
      (> (count matches) 1)
      (throw (gates/layered-error 1 :external-id-ambiguous
                                  {:candidates (mapv :entity/id matches)}))
      :else [200 {:entity (first matches)}])))

(defn- ensure-entity-id
  "Ensure semantics (futon1_write.clj:34-60): requested :id → existing by
  :entity/name (prefer matching type, then smallest id) → fresh UUID."
  [node {:keys [id name' type]} batch-candidates]
  (or id
      (let [candidates (concat (entities-by-name node name')
                               (filter #(= name' (:entity/name %)) batch-candidates))
            preferred (or (seq (filter #(= type (:entity/type %)) candidates))
                          (seq candidates))]
        (some->> preferred (sort-by #(str (:entity/id %))) first :entity/id))
      (str (random-uuid))))

(defn- build-entity
  "Validate and build one entity without writing it."
  ([node payload] (build-entity node payload []))
  ([node payload batch-candidates]
   (let [name' (:name payload)
         type (normalize-type (:type payload))]
     (when (or (str/blank? (str name')) (nil? type))
       (throw (gates/layered-error 4 :missing-required
                                   {:required [:name :type]
                                    :got (select-keys payload [:name :type])})))
     (let [id (ensure-entity-id node {:id (:id payload) :name' name' :type type}
                                batch-candidates)
           doc (cond-> {:xt/id id :entity/id id :entity/name name' :entity/type type}
                 (:external-id payload) (assoc :entity/external-id (:external-id payload))
                 (:source payload) (assoc :entity/source (:source payload))
                 (map? (:props payload)) (assoc :entity/props (:props payload)))
           gate-res (gates/gate-entity-id! doc)]
       {:doc doc
        :type type
        :queued? (:queued? gate-res)
        :public (public-entity doc)}))))

(defn write-entity!
  "POST /api/alpha/entity — the route where every gate fires (contract §5)."
  [node payload]
  (let [{:keys [doc type queued? public]} (build-entity node payload)
        rescue (put-verified! node :entities doc)]
      (register-types! node [{:kind :entity :type-id type}])
      (cond-> {:profile "default"
               :entity public
               :rescue rescue}
        queued? (assoc :queued? true))))

(defn write-entities-batch!
  "POST /api/alpha/entities/batch (contract §5 batch variant).
  {:entities [<per-item shape of write-entity!> ...]}. Every item is built and
  gate-validated before the first entity write, so an invalid item makes the
  batch all-or-nothing. The entity docs commit in one execute-tx. XTDB 2 batch
  puts can drop rows silently, so every doc is read back; each absent doc runs
  through the per-doc rescue ladder and an unrescuable absence throws the L0
  error. Type registration runs after verified entity persistence, once for
  every distinct entity type, and may use its own verified transactions.
  Envelope carries :rescue instead of :tx-id/:path-id. A transport/commit or
  post-commit rescue failure is not an atomic rollback guarantee: callers must
  treat an error after validation as indeterminate and read back by entity id."
  [node payload]
  (when-not (contains? payload :entities)
    (throw (gates/layered-error 4 :missing-required {:required [:entities]})))
  (let [entities (:entities payload)]
    (when-not (and (sequential? entities) (seq entities) (every? map? entities))
      (throw (gates/layered-error 4 :invalid-entities-batch
                                  {:expected :non-empty-seq-of-maps
                                   :got (if (sequential? entities)
                                          (mapv (comp str type) entities)
                                          (str (type entities)))})))
    (let [built (reduce (fn [acc entity]
                          (conj acc (build-entity node entity (mapv :doc acc))))
                        [] entities)
          docs (mapv (comp xf/transform-doc :doc) built)]
      (try (xt/execute-tx node (mapv (fn [d] [:put-docs :entities d]) docs))
           (catch Exception _ nil))
      (let [rescue
            (into {}
                  (keep (fn [d]
                          (when-not (fxt/present? node :entities (:xt/id d))
                            (let [res (ingest/put-doc-with-rescue!
                                       node :entities d !shape-log)]
                              (if (fxt/present? node :entities (:xt/id d))
                                [(:xt/id d) (if (keyword? res) res :ok)]
                                (throw (gates/layered-error
                                        0 :postcommit-missing-entities
                                        {:xt/id (:xt/id d) :table :entities})))))))
                  docs)]
        (register-types! node (mapv (fn [t] {:kind :entity :type-id t})
                                    (distinct (map :type built))))
        (cond-> {:profile "default"
                 :count (count built)
                 :entities (mapv :public built)}
          (some :queued? built) (assoc :queued? true)
          (seq rescue) (assoc :rescue rescue))))))

(defn entities-latest
  "GET /api/alpha/entities/latest — generic branch + the pattern/library
  sigil annotation (contract §5).

  Until 2026-08-23 the sigil join was a MEMBERSHIP filter: a pattern without
  a `:pattern/has-sigil` relation was not returned at all. After the 08-14
  re-ingest rewrote pattern ids to slugs the UUID-keyed relations matched
  nothing and Zone served `{:entities []}` with HTTP 200 — 1,372 rows present,
  0 returned, no error a consumer could see. Sigil presence is now an
  ATTRIBUTE (`:sigiled? true`) and the envelope carries `:sigil-join` so an
  empty library and a broken join are distinguishable."
  ([node opts]
   (entities-latest node opts fxt/safe-q))
  ([node {:keys [type limit]} query-fn]
   (let [t (normalize-type type)
         n (long (max 1 (or limit 1)))
         all (query-fn node (fxt/pq '[p-type]
                                    '(-> (from :entities [*])
                                         (where (= entity/type p-type)))
                                    t))
         library? (= t :pattern/library)
         sigil-src-ids (when library?
                         (->> (query-fn node '(-> (from :relations [relation/type relation/src])
                                                  (where (= relation/type :pattern/has-sigil))))
                              (map :relation/src) set))
         sigiled? (fn [d] (contains? sigil-src-ids (:entity/id d)))
         matched (when library? (count (filter sigiled? all)))
         docs (->> all
                   (group-by :entity/name)
                   (map (fn [[_ ds]] (first (sort-by #(str (:entity/id %)) ds))))
                   (sort-by #(str (:entity/name %)))
                   (take n)
                   (mapv (fn [d] (cond-> (public-entity d)
                                   library? (assoc :sigiled? (sigiled? d))))))]
     (when (and library? (seq all) (zero? matched))
       (println (format "[futon1b-sigil-join] BROKEN: %d pattern/library rows, %d has-sigil relation srcs, 0 matched"
                        (count all) (count sigil-src-ids))))
     (cond-> {:profile "default"
              :type (if t (subs (str t) 1) (str type))
              :entities docs}
       library? (assoc :sigil-join {:patterns (count all)
                                    :relation-srcs (count sigil-src-ids)
                                    :matched matched})))))

(defn entities-query
  "Backend-neutral typed entity read. Returns raw entity documents so callers
  can inspect domain fields written before the HTTP cutover as well as the
  equivalent fields carried in :entity/props by post-cutover writes. :count is
  the true type total; :next-cursor resumes the stable xt/id ordering."
  ([node opts]
   (entities-query node opts fxt/safe-q))
  ([node {:keys [type limit after]} query-fn]
   (let [t (normalize-type type)
         limited? (and (int? limit) (pos? limit))
         ;; Values ride as parameters (see fxt/pq); the form varies only by
         ;; which clauses are present, so the compiled plan is reused.
         params (cond-> '[p-type]
                  after (conj 'p-after)
                  limited? (conj 'p-limit))
         args (cond-> [t]
                after (conj after)
                limited? (conj limit))
         clauses (cond-> ['(= entity/type p-type)]
                   after (conj '(> xt/id p-after)))
         query-tail (cond-> [(cons 'where clauses)
                             '(order-by {:val xt/id :dir :asc})]
                      limited? (conj '(limit p-limit)))
         docs (query-fn node
                        (apply fxt/pq params
                               (cons '-> (cons '(from :entities [*]) query-tail))
                               args))
         total (count (query-fn node
                                (fxt/pq '[p-type]
                                        '(-> (from :entities [xt/id entity/type])
                                             (where (= entity/type p-type)))
                                        t)))
         window (vec docs)
         next-cursor (when (and (int? limit) (pos? limit)
                                (= limit (count window)))
                       (:xt/id (peek window)))]
     (cond-> {:entities (mapv #(dissoc % :xt/id) window)
              :count total}
       next-cursor (assoc :next-cursor next-cursor)))))

;; ---------------------------------------------------------------------------
;; Relations (A3) — §6. Stable rel| ids, both key spellings.
;; ---------------------------------------------------------------------------

(defn- uuid-shaped? [s]
  (and (string? s)
       (re-matches #"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}" s)))

(defn- resolve-rel-endpoint
  "src/dst: entity name | :entity/id | raw UUID string | map with
  :id/:entity/id/:name (futon1_write.clj:110-122). Unresolvable → the
  contract's raw-500 wart."
  [node x]
  (or (cond
        (map? x) (or (some->> (or (:id x) (:entity/id x)) (str))
                     (some->> (:name x) (resolve-rel-endpoint node)))
        (string? x)
        (or (when (fxt/present? node :entities x) x)
            (some->> (entities-by-name node x)
                     (sort-by #(str (:entity/id %))) first :entity/id)
            (when (uuid-shaped? x) x))
        :else nil)
      (throw (ex-info "relation requires resolvable src/dst and type" {:got x}))))

(defn- stable-relation-id [src-id type-kw dst-id prov]
  (str "rel|" src-id "|" (if (keyword? type-kw) (subs (str type-kw) 1) (str type-kw))
       "|" dst-id "|" (str (or (:note prov) "")) "|" (str (or (:order prov) ""))))

(defn write-relation!
  "POST /api/alpha/relation (contract §6)."
  [node payload]
  (let [type (normalize-type (:type payload))
        src (:src payload)
        dst (:dst payload)]
    (when (or (nil? type) (nil? src) (nil? dst))
      (throw (gates/layered-error 4 :missing-required
                                  {:required [:type :src :dst]})))
    (let [prov (or (:provenance payload)
                   (when (map? (:props payload))
                     {:note (:label (:props payload)) :props (:props payload)}))
          src-id (resolve-rel-endpoint node src)
          dst-id (resolve-rel-endpoint node dst)
          rel-id (or (:id payload) (stable-relation-id src-id type dst-id prov))
          doc (cond-> {:xt/id rel-id :relation/id rel-id :relation/type type
                       :relation/src src-id :relation/dst dst-id
                       :relation/from src-id :relation/to dst-id}
                prov (assoc :relation/provenance prov))
          rescue (put-verified! node :relations doc)]
      (register-types! node [{:kind :relation :type-id type}])
      {:profile "default"
       :relation {:id rel-id :type type :relation/type type
                  :src-id src-id :dst-id dst-id :provenance prov}
       :rescue rescue})))

(defn write-relations-batch!
  "POST /api/alpha/relations/batch (contract §6 batch variant).
  {:relations [<per-item shape of write-relation!> ...]}. All validation and
  endpoint resolution run before the first write, and §6 batch semantics
  require every resolved endpoint to already exist in :entities (L2
  :missing-endpoint) — stricter than the single route's uuid pass-through.
  The whole batch commits in one execute-tx; XTDB 2 batch puts can drop rows
  silently (E-futon1a-to-futon1b 2026-07-10), so every doc is read back and
  an absent one escalates through the per-doc rescue ladder, throwing the
  L0 error if still absent. Envelope carries :rescue instead of
  :tx-id/:path/id, per this port's deviations."
  [node payload]
  (when-not (contains? payload :relations)
    (throw (gates/layered-error 4 :missing-required {:required [:relations]})))
  (let [rels (:relations payload)]
    (when-not (and (sequential? rels) (seq rels) (every? map? rels))
      (throw (gates/layered-error 4 :invalid-relations-batch
                                  {:expected :non-empty-seq-of-maps
                                   :got (if (sequential? rels)
                                          (mapv (comp str type) rels)
                                          (str (type rels)))})))
    (let [built
          (mapv
           (fn [r]
             (when (or (nil? (:type r)) (nil? (:src r)) (nil? (:dst r)))
               (throw (gates/layered-error 4 :missing-required
                                           {:required [:type :src :dst]
                                            :got (select-keys r [:type :src :dst])})))
             (let [type (normalize-type (:type r))
                   prov (or (:provenance r)
                            (when (map? (:props r))
                              {:note (:label (:props r)) :props (:props r)}))
                   src-id (resolve-rel-endpoint node (:src r))
                   dst-id (resolve-rel-endpoint node (:dst r))
                   rel-id (or (:id r) (stable-relation-id src-id type dst-id prov))]
               {:doc (cond-> {:xt/id rel-id :relation/id rel-id :relation/type type
                              :relation/src src-id :relation/dst dst-id
                              :relation/from src-id :relation/to dst-id}
                       prov (assoc :relation/provenance prov))
                :public {:id rel-id :type type :relation/type type
                         :src-id src-id :dst-id dst-id :provenance prov}}))
           rels)
          missing (->> built
                       (mapcat (fn [{:keys [doc]}]
                                 [(:relation/from doc) (:relation/to doc)]))
                       distinct
                       (remove #(fxt/present? node :entities %))
                       vec)]
      (when (seq missing)
        (throw (gates/layered-error 2 :missing-endpoint {:missing missing})))
      (let [docs (mapv (comp xf/transform-doc :doc) built)]
        (try (xt/execute-tx node (mapv (fn [d] [:put-docs :relations d]) docs))
             (catch Exception _ nil))
        (let [rescue
              (into {}
                    (keep (fn [d]
                            (when-not (fxt/present? node :relations (:xt/id d))
                              (let [res (ingest/put-doc-with-rescue!
                                         node :relations d !shape-log)]
                                (if (fxt/present? node :relations (:xt/id d))
                                  [(:xt/id d) (if (keyword? res) res :ok)]
                                  (throw (gates/layered-error
                                          0 :postcommit-missing-entities
                                          {:xt/id (:xt/id d) :table :relations})))))))
                    docs)]
          (register-types! node (mapv (fn [t] {:kind :relation :type-id t})
                                      (distinct (map (comp :relation/type :doc) built))))
          (cond-> {:profile "default"
                   :count (count built)
                   :relations (mapv :public built)}
            (seq rescue) (assoc :rescue rescue)))))))

(defn relations-query
  "Typed relation read used by substrate consumers. Filters are conjunctive;
  the response preserves both legacy from/to and src/dst spellings."
  [node {:keys [type types from to limit hydrate?]}]
  (let [types (or (seq types) (when type [type]))
        query-for (fn [relation-type]
                    (let [specs (cond-> []
                                  relation-type
                                  (conj ['p-type '(= relation/type p-type)
                                         (normalize-type relation-type)])
                                  from (conj ['p-from '(= relation/from p-from) from])
                                  to (conj ['p-to '(= relation/to p-to) to]))
                          clauses (mapv second specs)
                          form (cond-> (list '-> '(from :relations [xt/id relation/id relation/type
                                                                  relation/from relation/to
                                                                  relation/src relation/dst
                                                                  relation/provenance]))
                                 (seq clauses) (concat [(cons 'where clauses)]))]
                      (apply fxt/pq (mapv first specs) form (mapv #(nth % 2) specs))))
        docs (if (seq types)
               (mapcat #(fxt/safe-q node (query-for %)) types)
               (fxt/safe-q node (query-for nil)))
        docs (sort-by #(str (or (:relation/id %) (:xt/id %))) docs)
        docs (if (and (int? limit) (pos? limit)) (take limit docs) docs)
        result {:relations (mapv #(dissoc % :xt/id) docs)
                :count (count docs)}]
    (if-not hydrate?
      result
      (let [ids (into #{} (mapcat #(keep % [:relation/from :relation/to])) docs)
            entities (->> (fxt/safe-q node '(from :entities [*]))
                          (filter #(contains? ids (or (:entity/id %) (:xt/id %))))
                          (mapv #(dissoc % :xt/id)))]
        (assoc result :entities entities)))))

(defn- entity-ids-of-type
  [node type]
  (into #{} (map :xt/id)
        (fxt/safe-q node
                    (fxt/pq '[p-type]
                            '(-> (from :entities [xt/id entity/type])
                                 (where (= entity/type p-type)))
                            (normalize-type type)))))

(defn- entity-type-inhabited?
  [node type]
  (boolean
   (seq (fxt/safe-q node
                    (fxt/pq '[p-type]
                            '(-> (from :entities [xt/id entity/type])
                                 (where (= entity/type p-type))
                                 (limit 1))
                            (normalize-type type))))))

(defn- hyperedge-type-inhabited?
  [node type endpoint-types]
  (let [docs (fxt/safe-q node
                         (fxt/pq '[p-type]
                                 '(-> (from :hyperedges [xt/id hx/type hx/endpoints])
                                      (where (= hx/type p-type)))
                                 (normalize-type type)))
        required (mapv #(entity-ids-of-type node %) endpoint-types)]
    (boolean
     (some (fn [doc]
             (let [endpoints (set (:hx/endpoints doc))]
               (every? #(some endpoints %) required)))
           docs))))

(defn inhabitation
  "Evaluate semantic graph bindings without exposing an XTDB query language.
  Results retain input order and contain only authoritative existence claims."
  [node bindings]
  (mapv (fn [{:keys [kind type endpoint-types] :as binding}]
          (assoc binding :inhabited?
                 (case kind
                   :entity (entity-type-inhabited? node type)
                   :hyperedge (hyperedge-type-inhabited? node type endpoint-types)
                   false)))
        bindings))

;; ---------------------------------------------------------------------------
;; Hyperedge reads (A4) — §4.
;; ---------------------------------------------------------------------------

(declare hyperedge-from)

(defn hyperedge-by-id
  "GET /api/alpha/hyperedge/{id} (id = URL-decoded URI tail)."
  ([node id] (hyperedge-by-id node id {}))
  ([node id temporal]
   (when-let [doc (fxt/q1 node (fxt/pq '[p-id]
                                       (list '->
                                             (hyperedge-from '[*] temporal)
                                             '(where (= xt/id p-id)))
                                       id))]
     (when (:hx/id doc) (dissoc doc :xt/id)))))

(def ^:private hyperedge-window-cols
  '[xt/id hx/type prop/timestamp prop/repo prop/source-file])

(defn- temporal-filter
  [instant]
  (when instant (list 'at instant)))

(defn- hyperedge-from
  [bindings {:keys [valid-as-of system-as-of]}]
  (let [opts (cond-> {:bind bindings}
               valid-as-of (assoc :for-valid-time (temporal-filter valid-as-of))
               system-as-of (assoc :for-system-time (temporal-filter system-as-of)))]
    (if (> (count opts) 1)
      (list 'from :hyperedges opts)
      (list 'from :hyperedges bindings))))

(defn- fetch-hyperedge-doc
  ([node id temporal]
   (fetch-hyperedge-doc node id temporal fxt/safe-q))
  ([node id temporal query-fn]
   (first (query-fn node (fxt/pq '[p-id]
                                 (list '-> (hyperedge-from '[*] temporal)
                                       '(where (= xt/id p-id)))
                                 id)))))

;; PER-DOC HYDRATION, deliberately. Restored 2026-08-02 after the batched form
;; measured as a large regression on the live store.
;;
;; The batched attempt (2b887d3, chunked in 71b7c1e) replaced these point
;; lookups with a variadic `or` of id equalities, because XTQL has no working
;; set-membership predicate over a list — `in`, `contains?` and `any` are all
;; rejected on type grounds, though SQL `WHERE _id IN (?,?)` works
;; (probe_in_predicate.clj).
;;
;; Two things went wrong, in order of severity:
;;
;;   1. A disjunction of id equalities is NOT index-backed. Measured on the live
;;      34,480-doc `code/v05/var` type: a single `(= xt/id x)` costs ~50ms, but
;;      ONE 50-clause `or` costs ~40s — 50 rows in 40.4s, 100 rows in 76.8s,
;;      linear in chunks at ~40s each. The same shape on a 6,000-doc fixture ran
;;      5000 rows in 15.5s, which is why the fixture hid it entirely.
;;      Against that, this per-doc loop costs ~50ms/row: ~2.5s for those 50 rows.
;;      The batched form was therefore ~16x SLOWER on real data, while holding an
;;      expensive-read permit throughout and starving every other reader.
;;
;;   2. The disjunction also overruns the JVM method-size limit past ~250
;;      clauses ("Syntax error compiling reify*", then `xtdb.error.Fault`), and
;;      when the failure text matches `safe-q`'s tolerated patterns it is
;;      swallowed and the read returns EMPTY — a 200 with silent data loss.
;;
;; So this is slow-but-honest: O(n) point lookups at ~50ms each, 4 in flight.
;; Do NOT "optimise" it back into a disjunction without re-measuring on the LIVE
;; store — a fixture will report the opposite result. If a batched form is
;; wanted, test the SQL surface (`WHERE _id IN (…)`) first and check whether it
;; plans as index lookups; that is the open question for JUXT recorded in
;; TN-xtdb2-query-ceilings-and-ingest-memory-2026-08-02.md.
(defn- hydrate-hyperedge-window
  "Hydrate an ordered projected window with bounded concurrency, preserving
  order. Full hyperedge bodies never participate in the corpus-wide sort."
  [node projected temporal query-fn]
  (->> projected
       (partition-all 4)
       (mapcat (fn [batch]
                 (->> batch
                      (mapv #(future (fetch-hyperedge-doc node (:xt/id %) temporal
                                                         query-fn)))
                      (mapv deref))))
       (keep identity)))

(defn- hyperedges-query-uncached
  "GET /api/alpha/hyperedges?type=… and/or end=… (+limit/latest/after,
  +repo/source-file for type-only queries). When end is present, type is an
  optional pushed-down filter rather than a competing branch. :count is the
  true type total when unfiltered even if limit truncates; returned-count
  otherwise (contract §4)."
  [node {:keys [type end limit repo source-file after latest? include-total?]
         :or {include-total? true}
         :as opts}
   query-fn]
  (let [temporal (select-keys opts [:valid-as-of :system-as-of])]
  (cond
    end
    (let [end-id (if (uuid-shaped? end)
                   (or (some-> (fetch-entity node end) :entity/name) end)
                   end)
          targets (distinct [end end-id])
          n (long (or limit 100))
          t (some-> type normalize-type)
          projected (->> targets
                         (mapcat
                          (fn [target]
                            (let [clauses (cond-> ['(= ep p-target)]
                                            t (conj '(= hx/type p-type)))
                                  params (cond-> '[p-target p-limit] t (conj 'p-type))
                                  args (cond-> [target n] t (conj t))]
                              (query-fn
                               node
                               (apply fxt/pq params
                                      (list '->
                                            (hyperedge-from
                                             '[xt/id hx/type hx/endpoints] temporal)
                                            '(unnest {:ep hx/endpoints})
                                            (cons 'where clauses)
                                            '(return xt/id)
                                            '(order-by {:val xt/id :dir :asc})
                                            '(limit p-limit))
                                      args)))))
                         (reduce (fn [by-id row]
                                   (assoc by-id (:xt/id row) row)) {})
                         vals
                         (sort-by #(str (:xt/id %)))
                         (take n))
          docs (hydrate-hyperedge-window node projected temporal query-fn)
          out (mapv #(dissoc % :xt/id) docs)]
      {:hyperedges out :count (count out)})

    type
    (let [t (normalize-type type)
          limited? (and (not latest?) (int? limit) (pos? limit))
          ;; Values ride as parameters (fxt/pq) so the compiled plan is keyed
          ;; on which filters are present, not on their values.
          specs (cond-> [['p-type '(= hx/type p-type) t]]
                  ;; denormalized :prop/* columns (H4) let repo/source-file
                  ;; push down — the [*] whole-type pull timed out live on
                  ;; the 259k-doc edits type (2026-07-11)
                  repo (conj ['p-repo '(= prop/repo p-repo) repo])
                  source-file (conj ['p-source-file '(= prop/source-file p-source-file)
                                     source-file])
                  after (conj ['p-after '(> xt/id p-after) after])
                  limited? (conj ['p-limit nil limit]))
          params (mapv first specs)
          args (mapv #(nth % 2) specs)
          clauses (keep second specs)
          query-tail (cond-> [(cons 'where clauses)]
                       latest? (conj '(order-by {:val prop/timestamp :dir :desc})
                                     '(limit 1))
                       limited? (conj '(order-by {:val xt/id :dir :asc})
                                      '(limit p-limit)))
          bounded? (or latest? (and (int? limit) (pos? limit)))
          selected (query-fn
                    node
                    (apply fxt/pq params
                           (cons '->
                                 (cons (hyperedge-from
                                        (if bounded?
                                          hyperedge-window-cols
                                          '[*])
                                        temporal)
                                       query-tail))
                           args))
          docs (if bounded?
                 (hydrate-hyperedge-window node selected temporal query-fn)
                 selected)
          total (when include-total?
                  (if (or latest? repo source-file)
                    (count docs)
                    (count (query-fn node (fxt/pq '[p-type]
                                                  (list '->
                                                        (hyperedge-from
                                                         '[xt/id hx/type] temporal)
                                                        '(where (= hx/type p-type)))
                                                  t)))))
          prop-get (fn [d k kw-col]
                     (or (get d kw-col)
                         (get-in d [:hx/props (keyword k)])
                         (get-in d [:hx/props k])))
          filtered (cond->> docs
                     repo (filter #(= repo (str (prop-get % "repo" :prop/repo))))
                     source-file (filter #(= source-file
                                             (str (prop-get % "source-file" :prop/source-file)))))
          ;; Limited non-latest queries are ordered and bounded inside XTDB;
          ;; never hydrate the whole typed collection and truncate in Clojure.
          sorted (if (or latest? (and (int? limit) (pos? limit)))
                   filtered
                   (sort-by #(str (:xt/id %)) filtered))
          limited-seq (if (and (not latest?) (int? limit) (pos? limit))
                        (take limit sorted)
                        sorted)
          limited (vec limited-seq)
          out (mapv #(dissoc % :xt/id) limited)
          ;; The cursor advances over the SERVER window (`docs`), not the
          ;; client-filtered one. repo/source-file are re-filtered in Clojure
          ;; after the bounded read, so deriving the cursor from `limited` ended
          ;; a walk the moment any row was dropped — silently returning partial
          ;; results, and precisely on the repo+cursor path callers page with.
          ;; Emit whenever the SERVER returned a full window; the last projected
          ;; id is the correct resume point regardless of what survived filtering.
          server-window (vec docs)
          next-cursor (when (and (not latest?)
                                 (int? limit) (pos? limit)
                                 (= limit (count server-window)))
                        (:xt/id (peek server-window)))]
      (cond-> {:hyperedges out
               :count (if (or (not include-total?) latest? repo source-file)
                        (count out)
                        total)
               :count-exact? (boolean include-total?)}
        next-cursor (assoc :next-cursor next-cursor))))))

(defonce ^:private !hyperedge-query-cache (atom {}))

(defn invalidate-hyperedge-query-cache!
  "Invalidate materialized bounded query windows after a hyperedge mutation."
  []
  (reset! !hyperedge-query-cache {})
  nil)

(defn hyperedges-query
  "Read hyperedges, materializing bounded type windows that explicitly waive an
  exact total. The cache is invalidated synchronously by every server mutation."
  ([node opts]
   (hyperedges-query node opts fxt/safe-q))
  ([node opts query-fn]
   (let [{:keys [type limit include-total?]} opts
         ;; Only windows at or below the served ceiling are retained; anything
         ;; larger is served uncached (E-futon1b-gc-wedge).
         cacheable? (and type (int? limit) (pos? limit) (<= limit 1000)
                         (false? include-total?))
         cache-key [node opts]]
     (if-not cacheable?
       (hyperedges-query-uncached node opts query-fn)
       (if-let [cached (get @!hyperedge-query-cache cache-key)]
         cached
         (let [result (hyperedges-query-uncached node opts query-fn)]
           (when (>= (count @!hyperedge-query-cache) 32)
             (reset! !hyperedge-query-cache {}))
           (swap! !hyperedge-query-cache assoc cache-key result)
           result))))))

(def ^:private max-memory-projection-endpoints 20)
(def ^:private max-memory-projection-limit 100)
(def ^:private max-memory-projection-index-components 5000)
(def ^:private memory-projection-quiescence-timeout-ms
  (or (when-let [s (System/getenv "FUTON1B_PROJECTION_QUIESCENCE_TIMEOUT_MS")]
        (try (Long/parseLong (.trim ^String s)) (catch Exception _ nil)))
      600000))
(def ^:private memory-projection-quiescence-poll-ms 500)
;; Byte-offset quiescence is necessary but not sufficient: node-watermark is
;; compared by whole-map equality and carries :system-time, so anything the
;; node advances during the build window fails the check. On a 22 GB store the
;; select+hydrate window is seconds, which is long enough for that to happen
;; with no external writer at all — observed on Dionysus 2026-08-14, where a
;; fresh boot failed 295 consecutive times against a quiet store. Rebuild
;; rather than certifying a mixed snapshot, the same choice the point-refresh
;; path already makes below.
(def ^:private max-memory-projection-build-attempts
  (or (when-let [s (System/getenv "FUTON1B_PROJECTION_BUILD_ATTEMPTS")]
        (try (Long/parseLong (.trim ^String s)) (catch Exception _ nil)))
      5))
(defonce ^:private !memory-projection-indexes (atom {}))
(defonce ^:private !memory-projection-generations (atom {}))

(defn- memory-projection-generation
  [node]
  (get @!memory-projection-generations node 0))

(defn- advance-memory-projection-generation!
  [node]
  (get (swap! !memory-projection-generations update node (fnil inc 0))
       node))

(defn- node-watermark
  "Return the XTDB progress coordinates that delimit a coherent current read.

  A node can accept queries while replaying its durable log.  An index built
  across two different coordinates is not a current projection, even when
  every individual query succeeded."
  [node]
  (select-keys (xt/status node)
               [:latest-completed-txs
                :latest-submitted-msg-ids
                :latest-processed-msg-ids]))

(defn- max-msg-offset
  "Largest XTDB log coordinate in a status map. These values are BYTE
   OFFSETS, not message counts."
  [offsets]
  (->> (tree-seq coll? seq offsets)
       (filter number?)
       (reduce max -1)))

(defn restart-readiness-status
  "Expose the running node's submitted/processed byte offsets for the
   operator pre-restart gate."
  [node]
  (let [status (xt/status node)
        submitted (:latest-submitted-msg-ids status)
        processed (:latest-processed-msg-ids status)]
    {:unit "bytes"
     :latest-submitted-byte-offset (max-msg-offset submitted)
     :latest-processed-byte-offset (max-msg-offset processed)
     :submitted-byte-offsets submitted
     :processed-byte-offsets processed}))

(defn wait-for-indexing-quiescence!
  "Wait boundedly until XTDB's processed BYTE OFFSET reaches its submitted
   BYTE OFFSET. Returns timing/status evidence; throws loudly on timeout."
  ([node]
   (wait-for-indexing-quiescence!
    node memory-projection-quiescence-timeout-ms
    memory-projection-quiescence-poll-ms))
  ([node timeout-ms poll-ms]
   (let [started (System/nanoTime)]
     (loop []
       (let [{:keys [latest-submitted-byte-offset
                     latest-processed-byte-offset]
              :as status} (restart-readiness-status node)
             waited-ms (elapsed-ms started)]
         (cond
           (<= latest-submitted-byte-offset latest-processed-byte-offset)
           (assoc status :waited-ms waited-ms)

           (>= waited-ms timeout-ms)
           (throw (gates/layered-error
                   0 :indexing-quiescence-timeout
                   {:unit "bytes"
                    :waited-ms waited-ms
                    :timeout-ms timeout-ms
                    :latest-submitted-byte-offset
                    latest-submitted-byte-offset
                    :latest-processed-byte-offset
                    latest-processed-byte-offset}))

           :else
           (do (Thread/sleep (long poll-ms))
               (recur))))))))

(defn- any-equals
  "Disjunction of equalities against PARAM-SYMS (one per value; the values
  ride as query parameters — see fxt/pq)."
  [binding param-syms]
  (if (= 1 (count param-syms))
    (list '= binding (first param-syms))
    (cons 'or (map #(list '= binding %) param-syms))))

(defn- hydrate-memory-components
  "Hydrate selected ids through bounded indexed point reads.

  XTDB 2.1 turns both OR-filtered `[ * ]` queries and a rel/from join into
  additional table scans on the live corpus. Point predicates use the fast id
  path; the process-wide query semaphore keeps their concurrency bounded."
  [node edge-ids temporal]
  (if (seq edge-ids)
    (->> edge-ids
         (partition-all 4)
         (mapcat
          (fn [batch]
            (->> batch
                 (mapv
                  (fn [edge-id]
                    (future
                      (when-let [edge (fetch-hyperedge-doc node edge-id temporal)]
                        (let [memory-id
                              (get-in edge [:hx/props :roles :entry])
                              entry
                              (when (string? memory-id)
                                (fxt/q1
                                 node
                                 (fxt/pq '[p-id]
                                         '(-> (from :evidence [*])
                                              (where (= xt/id p-id)))
                                         memory-id)))]
                          (when entry
                            {:hyperedge-id edge-id
                             :hx/type (:hx/type edge)
                             :hx/endpoints (:hx/endpoints edge)
                             :hx/props (:hx/props edge)
                             :memory-id memory-id
                             :evidence/id (:evidence/id entry)
                             :evidence/type (:evidence/type entry)
                             :evidence/claim-type
                             (:evidence/claim-type entry)
                             :evidence/author (:evidence/author entry)
                             :evidence/session-id
                             (:evidence/session-id entry)
                             :evidence/body (:evidence/body entry)}))))))
                 (mapv deref))))
         (keep identity)
         vec)
    []))

(defn- hydrated-row->component
  [{:keys [hyperedge-id]
    hyperedge-type :hx/type
    hyperedge-endpoints :hx/endpoints
    hyperedge-props :hx/props
    evidence-id :evidence/id
    evidence-type :evidence/type
    claim-type :evidence/claim-type
    author :evidence/author
    session-id :evidence/session-id
    body :evidence/body}]
  {:hyperedge-id hyperedge-id
   :edge {:hx/id hyperedge-id
          :hx/type hyperedge-type
          :hx/endpoints hyperedge-endpoints
          :hx/props hyperedge-props}
   :entry
   (cond-> {:evidence/id evidence-id
            :evidence/type evidence-type
            :evidence/claim-type claim-type
            :evidence/author author
            :evidence/session-id session-id}
     (map? body) (assoc :evidence/body (select-keys body [:hook])))})

(defn- build-memory-projection-index
  [revision components source-watermark source-generation]
  (let [components (if (map? components) (vals components) components)
        components-by-id (into {} (map (juxt :hyperedge-id identity)) components)
        by-endpoint
        (->> components
             (reduce
              (fn [index component]
                (reduce (fn [index' endpoint]
                          (update index' endpoint (fnil conj []) component))
                        index
                        (get-in component [:edge :hx/endpoints])))
              {})
             (reduce-kv
              (fn [index endpoint endpoint-components]
                (assoc index endpoint
                       (vec (sort-by :hyperedge-id endpoint-components))))
              {}))]
    {:revision revision
     :built-at (str (java.time.Instant/now))
     :source-watermark source-watermark
     :source-generation source-generation
     :components-by-id components-by-id
     :by-endpoint by-endpoint}))

(defn initialize-memory-projection!
  "Build the bounded current-state memory index before the HTTP listener starts.

  This is a materialized projection, not a TTL cache: successful memory/assert
  puts and retractions advance it synchronously. Historical reads bypass it."
  [node]
  (locking !memory-projection-indexes
    (let [started (System/nanoTime)]
      (loop [attempt 1]
        (let [quiescence (wait-for-indexing-quiescence! node)
              source-watermark (node-watermark node)
              source-generation (memory-projection-generation node)
              selected+
              (fxt/safe-q
               node
               (list '->
                     '(from :hyperedges [xt/id hx/type])
                     (list 'where (list '= 'hx/type :memory/assert))
                     (list 'return 'xt/id)
                     (list 'order-by {:val 'xt/id :dir :asc})
                     (list 'limit
                           (inc max-memory-projection-index-components))))]
          (when (> (count selected+) max-memory-projection-index-components)
            (throw (gates/layered-error
                    0 :memory-projection-index-bound-exceeded
                    {:maximum max-memory-projection-index-components
                     :observed-at-least (count selected+)})))
          (let [rows (hydrate-memory-components
                      node (mapv :xt/id selected+) {})
                components (mapv hydrated-row->component rows)
                observed-watermark (node-watermark node)
                moved? (not= source-watermark observed-watermark)]
            (when (and moved?
                       (>= attempt max-memory-projection-build-attempts))
              (throw (gates/layered-error
                      0 :memory-projection-source-moved-after-quiescence
                      {:build-attempts attempt
                       :max-build-attempts max-memory-projection-build-attempts
                       :source-watermark source-watermark
                       :observed-watermark observed-watermark})))
            (if moved?
              (recur (inc attempt))
              (let [prior-revision
                    (get-in @!memory-projection-indexes [node :revision] 0)
                    index
                    (build-memory-projection-index
                     (inc prior-revision) components source-watermark
                     source-generation)]
                (swap! !memory-projection-indexes assoc node index)
                {:revision (:revision index)
                 :component-count (count components)
                 :endpoint-count (count (:by-endpoint index))
                 :build-attempts attempt
                 :quiescence-wait-ms (:waited-ms quiescence)
                 :build-ms (elapsed-ms started)}))))))))

(defn refresh-memory-projection-component!
  "Point-refresh one current memory/assert component after its verified put."
  [node edge-id]
  (when (contains? @!memory-projection-indexes node)
    (locking !memory-projection-indexes
      (let [source-watermark (node-watermark node)
            row (first (hydrate-memory-components node [edge-id] {}))
            component (when (= :memory/assert (:hx/type row))
                        (hydrated-row->component row))
            observed-watermark (node-watermark node)
            projection-relevant?
            (or component
                (contains? (get-in @!memory-projection-indexes
                                   [node :components-by-id])
                           edge-id))]
        (when projection-relevant?
          (let [source-generation
                (advance-memory-projection-generation! node)]
            (if (= source-watermark observed-watermark)
              (swap! !memory-projection-indexes
                     update node
                     (fn [{:keys [revision components-by-id]}]
                       (build-memory-projection-index
                        (inc revision)
                        (cond-> (dissoc components-by-id edge-id)
                          component (assoc edge-id component))
                        observed-watermark
                        source-generation)))
              ;; Another transaction crossed the point-refresh window. Rebuild
              ;; the whole bounded projection rather than certifying a mixed
              ;; snapshot.
              (initialize-memory-projection! node)))))))
  nil)

(defn- current-memory-projection-index
  [node]
  (locking !memory-projection-indexes
    (when-not (= (memory-projection-generation node)
                 (get-in @!memory-projection-indexes
                         [node :source-generation]))
      (initialize-memory-projection! node))
    (get @!memory-projection-indexes node)))

(defn- validate-memory-projection-request
  [{:keys [endpoints limit]}]
  (let [endpoints (vec (distinct endpoints))
        endpoint-count (count endpoints)]
    (when-not (and (pos? endpoint-count)
                   (<= endpoint-count max-memory-projection-endpoints)
                   (every? #(and (string? %) (not (str/blank? %))) endpoints))
      (throw (gates/layered-error
              4 :invalid-memory-projection-endpoints
              {:minimum 1
               :maximum max-memory-projection-endpoints
               :provided endpoint-count})))
    (when-not (and (int? limit)
                   (pos? limit)
                   (<= limit max-memory-projection-limit))
      (throw (gates/layered-error
              4 :invalid-memory-projection-limit
              {:minimum 1
               :maximum max-memory-projection-limit
               :provided limit})))
    endpoints))

(defn- memory-projection-components-uncached
  "Resolve several memory endpoints through one bounded membership scan.

  The response carries compact edge/entry components rather than duplicating
  the shared memory contract in Futon1b. Futon3c validates and projects these
  components with futon2.aif.memory-contract/compact-memory. Every requested
  endpoint gets a deterministic group; distinct edges and evidence entries are
  hydrated once per request."
  [node {:keys [endpoints limit valid-as-of system-as-of]}]
  (let [started (System/nanoTime)
        endpoints (validate-memory-projection-request
                   {:endpoints endpoints :limit limit})
        endpoint-count (count endpoints)
        temporal {:valid-as-of valid-as-of :system-as-of system-as-of}
        raw-limit (* endpoint-count limit)
        selection-start (System/nanoTime)
        endpoint-params (mapv #(symbol (str "p-ep" %)) (range endpoint-count))
        selected-rows+
        (fxt/safe-q
         node
         (apply fxt/pq endpoint-params
         (list '->
               (hyperedge-from '[xt/id hx/type hx/endpoints] temporal)
               (list 'unnest '{:matched-endpoint hx/endpoints})
               (list 'where
                     (list '= 'hx/type :memory/assert)
                     (any-equals 'matched-endpoint endpoint-params))
               (list 'return 'xt/id 'matched-endpoint)
               ;; A temporal scan can expose several system-time versions of
               ;; one edge. Collapse those versions before applying the
               ;; bounded-window sentinel, so the guard counts endpoint-edge
               ;; results rather than physical history rows.
               (list 'aggregate 'xt/id 'matched-endpoint)
               (list 'order-by
                     {:val 'matched-endpoint :dir :asc}
                     {:val 'xt/id :dir :asc})
               (list 'limit (inc raw-limit)))
         endpoints))
        ;; Keep the invariant explicit at the application boundary as well:
        ;; XTDB should already have grouped these rows, but repeated identical
        ;; projections must never consume the distinct-result budget.
        selected+ (vec (distinct selected-rows+))
        selection-ms (elapsed-ms selection-start)]
      (when (> (count selected+) raw-limit)
        (throw (gates/layered-error
                4 :memory-projection-result-bound-exceeded
                {:endpoint-count endpoint-count
                 :per-endpoint-limit limit
                 :maximum raw-limit})))
      (let [selected (vec selected+)
            edge-ids (->> selected (map :xt/id) distinct vec)
            hydration-start (System/nanoTime)
            hydrated (hydrate-memory-components node edge-ids temporal)
            hydration-ms (elapsed-ms hydration-start)
            entry-ids (->> hydrated (map :memory-id) distinct vec)
            hydrated-by-id
            (into {} (map (juxt :hyperedge-id identity)) hydrated)
            projection-start (System/nanoTime)
            selected-by-endpoint (group-by :matched-endpoint selected)
            groups
            (mapv
             (fn [endpoint]
               (let [selected-rows
                     (take limit (get selected-by-endpoint endpoint []))
                     rows (into [] (keep #(get hydrated-by-id (:xt/id %)))
                     selected-rows)
                     components (mapv hydrated-row->component rows)]
                 {:endpoint endpoint
                  :components components
                  :audit {:selected-count (count rows)
                          :missing-edge-or-entry-count
                          (- (count selected-rows)
                             (count rows))}}))
             endpoints)
            projection-ms (elapsed-ms projection-start)]
        {:ok true
         :endpoints endpoints
         :limit limit
         :temporal-basis
         (cond-> {:mode (if (or valid-as-of system-as-of) :as-of :current)}
           valid-as-of (assoc :valid-as-of (str valid-as-of))
           system-as-of (assoc :system-as-of (str system-as-of)))
         :groups groups
         :audit {:selected-row-count (count selected)
                 :distinct-edge-count (count edge-ids)
                 :distinct-entry-count (count entry-ids)
                 :hydrated-component-count (count hydrated)
                 :dropped-component-count (- (count edge-ids)
                                             (count hydrated))}
         :timing {:endpoint-selection-ms selection-ms
                  :component-hydration-ms hydration-ms
                  :projection-ms projection-ms
                  :service-total-ms (elapsed-ms started)}})))

(defn memory-projection-components
  "Resolve current memory endpoints from the synchronously maintained
  projection. Explicit bitemporal reads retain the bounded XTDB path."
  [node {:keys [endpoints limit valid-as-of system-as-of] :as request}]
  (if (or valid-as-of system-as-of)
    (memory-projection-components-uncached node request)
    (let [started (System/nanoTime)
          endpoints (validate-memory-projection-request
                     {:endpoints endpoints :limit limit})
          index (current-memory-projection-index node)
          lookup-start (System/nanoTime)
          groups
          (mapv
           (fn [endpoint]
             (let [components (vec (take limit
                                         (get-in index [:by-endpoint endpoint] [])))]
               {:endpoint endpoint
                :components components
                :audit {:selected-count (count components)
                        :missing-edge-or-entry-count 0}}))
           endpoints)
          components (mapcat :components groups)
          edge-ids (set (map :hyperedge-id components))
          entry-ids (set (map #(get-in % [:entry :evidence/id]) components))
          lookup-ms (elapsed-ms lookup-start)]
      {:ok true
       :endpoints endpoints
       :limit limit
       :temporal-basis {:mode :current
                        :projection-revision (:revision index)
                        :projection-generation (:source-generation index)
                        :projection-built-at (:built-at index)}
       :groups groups
       :audit {:selected-row-count (count components)
               :distinct-edge-count (count edge-ids)
               :distinct-entry-count (count entry-ids)
               :hydrated-component-count (count edge-ids)
               :dropped-component-count 0}
       :timing {:projection-lookup-ms lookup-ms
                :service-total-ms (elapsed-ms started)}})))

;; ---------------------------------------------------------------------------
;; Census (A5) — §7. Bound-type count, no doc materialization.
;; ---------------------------------------------------------------------------

(defn census [node {:keys [type entity-type]}]
  (cond
    type
    {:type type :kind :hyperedge
     :count (count (fxt/safe-q node (fxt/pq '[p-type]
                                            '(-> (from :hyperedges [xt/id hx/type])
                                                 (where (= hx/type p-type)))
                                            (normalize-type type))))}
    entity-type
    {:type entity-type :kind :entity
     :count (count (fxt/safe-q node (fxt/pq '[p-type]
                                            '(-> (from :entities [xt/id entity/type])
                                                 (where (= entity/type p-type)))
                                            (normalize-type entity-type))))}))
