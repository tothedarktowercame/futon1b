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
    (let [existing (fxt/q1 node (list '-> '(from :type-catalog [*])
                                  (list 'where (list '= 'xt/id
                                                     (type-id->xt-id kind type-id)))))
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
  (fxt/safe-q node (list '-> '(from :entities [*])
                   (list 'where (list '= 'entity/name name')))))

(defn fetch-entity
  "futon1a f1g/fetch-entity: :xt/id → :entity/name → :entity/external-id,
  deterministic smallest-id pick on duplicates."
  [node id]
  (or (fxt/q1 node (list '-> '(from :entities [*])
                     (list 'where (list '= 'xt/id id))))
      (->> (entities-by-name node id)
           (sort-by #(str (:entity/id %)))
           first)
      (->> (fxt/safe-q node (list '-> '(from :entities [*])
                            (list 'where (list '= 'entity/external-id id))))
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

(def ^:private retractable-tables #{:entities :hyperedges})

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
  (let [matches (fxt/safe-q node (list '-> '(from :entities [*])
                                 (list 'where
                                       (list '= 'entity/source source)
                                       (list '= 'entity/external-id external-id))))]
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
  [node {:keys [id name' type]}]
  (or id
      (let [candidates (entities-by-name node name')
            preferred (or (seq (filter #(= type (:entity/type %)) candidates))
                          (seq candidates))]
        (some->> preferred (sort-by #(str (:entity/id %))) first :entity/id))
      (str (random-uuid))))

(defn write-entity!
  "POST /api/alpha/entity — the route where every gate fires (contract §5)."
  [node payload]
  (let [name' (:name payload)
        type (normalize-type (:type payload))]
    (when (or (str/blank? (str name')) (nil? type))
      (throw (gates/layered-error 4 :missing-required
                                  {:required [:name :type]
                                   :got (select-keys payload [:name :type])})))
    (let [id (ensure-entity-id node {:id (:id payload) :name' name' :type type})
          doc (cond-> {:xt/id id :entity/id id :entity/name name' :entity/type type}
                (:external-id payload) (assoc :entity/external-id (:external-id payload))
                (:source payload) (assoc :entity/source (:source payload))
                (map? (:props payload)) (assoc :entity/props (:props payload)))
          gate-res (gates/gate-entity-id! doc)   ; L4 canonical-id (may throw 400)
          rescue (put-verified! node :entities doc)]
      (register-types! node [{:kind :entity :type-id type}])
      (cond-> {:profile "default"
               :entity (public-entity doc)
               :rescue rescue}
        (:queued? gate-res) (assoc :queued? true)))))

(defn entities-latest
  "GET /api/alpha/entities/latest — generic branch + the pattern/library
  sigil special-case (contract §5)."
  [node {:keys [type limit]}]
  (let [t (normalize-type type)
        n (long (max 1 (or limit 1)))
        all (fxt/safe-q node (list '-> '(from :entities [*])
                             (list 'where (list '= 'entity/type t))))
        sigiled (if (= t :pattern/library)
                  (let [sigil-src-ids
                        (->> (fxt/safe-q node '(-> (from :relations [relation/type relation/src])
                                             (where (= relation/type :pattern/has-sigil))))
                             (map :relation/src) set)]
                    (filter #(contains? sigil-src-ids (:entity/id %)) all))
                  all)
        docs (->> sigiled
                  (group-by :entity/name)
                  (map (fn [[_ ds]] (first (sort-by #(str (:entity/id %)) ds))))
                  (sort-by #(str (:entity/name %)))
                  (take n)
                  (mapv public-entity))]
    {:profile "default"
     :type (if t (subs (str t) 1) (str type))
     :entities docs}))

(defn entities-query
  "Backend-neutral typed entity read. Returns raw entity documents so callers
  can inspect domain fields written before the HTTP cutover as well as the
  equivalent fields carried in :entity/props by post-cutover writes."
  [node {:keys [type limit]}]
  (let [t (normalize-type type)
        docs (fxt/safe-q node (list '-> '(from :entities [*])
                                    (list 'where (list '= 'entity/type t))))
        docs (sort-by #(str (or (:entity/id %) (:xt/id %))) docs)
        docs (if (and (int? limit) (pos? limit)) (take limit docs) docs)]
    {:entities (mapv #(dissoc % :xt/id) docs)
     :count (count docs)}))

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
                    (let [clauses (cond-> []
                                    relation-type
                                    (conj (list '= 'relation/type
                                                (normalize-type relation-type)))
                                    from (conj (list '= 'relation/from from))
                                    to (conj (list '= 'relation/to to)))]
                      (cond-> (list '-> '(from :relations [xt/id relation/id relation/type
                                                           relation/from relation/to
                                                           relation/src relation/dst
                                                           relation/provenance]))
                        (seq clauses) (concat [(cons 'where clauses)]))))
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
                    (list '-> '(from :entities [xt/id entity/type])
                          (list 'where (list '= 'entity/type
                                            (normalize-type type)))))))

(defn- entity-type-inhabited?
  [node type]
  (boolean
   (seq (fxt/safe-q node
                    (list '-> '(from :entities [xt/id entity/type])
                          (list 'where (list '= 'entity/type
                                            (normalize-type type)))
                          '(limit 1))))))

(defn- hyperedge-type-inhabited?
  [node type endpoint-types]
  (let [docs (fxt/safe-q node
                         (list '-> '(from :hyperedges [xt/id hx/type hx/endpoints])
                               (list 'where (list '= 'hx/type
                                                 (normalize-type type)))))
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
   (when-let [doc (fxt/q1 node (list '->
                                     (hyperedge-from '[*] temporal)
                                     (list 'where (list '= 'xt/id id))))]
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

(defn- fetch-hyperedge-doc [node id temporal]
  (fxt/q1 node (list '-> (hyperedge-from '[*] temporal)
                     (list 'where (list '= 'xt/id id)))))

(defn- hydrate-hyperedge-window
  "Hydrate an ordered projected window with bounded concurrency, preserving
  order. Full hyperedge bodies never participate in the corpus-wide sort."
  [node projected temporal]
  (->> projected
       (partition-all 4)
       (mapcat (fn [batch]
                 (->> batch
                      (mapv #(future (fetch-hyperedge-doc node (:xt/id %) temporal)))
                      (mapv deref))))
       (keep identity)))

(defn- hyperedges-query-uncached
  "GET /api/alpha/hyperedges?type=… and/or end=… (+limit/latest,
  +repo/source-file for type-only queries). When end is present, type is an
  optional pushed-down filter rather than a competing branch. :count is the
  true type total when unfiltered even if limit truncates; returned-count
  otherwise (contract §4)."
  [node {:keys [type end limit repo source-file latest? include-total?]
         :or {include-total? true}
         :as opts}]
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
                            (let [clauses (cond-> [(list '= 'ep target)]
                                            t (conj (list '= 'hx/type t)))]
                              (fxt/safe-q
                               node
                               (list '->
                                     (hyperedge-from
                                      '[xt/id hx/type hx/endpoints] temporal)
                                     (list 'unnest '{:ep hx/endpoints})
                                     (cons 'where clauses)
                                     (list 'return 'xt/id)
                                     (list 'order-by {:val 'xt/id :dir :asc})
                                     (list 'limit n))))))
                         (reduce (fn [by-id row]
                                   (assoc by-id (:xt/id row) row)) {})
                         vals
                         (sort-by #(str (:xt/id %)))
                         (take n))
          docs (hydrate-hyperedge-window node projected temporal)
          out (mapv #(dissoc % :xt/id) docs)]
      {:hyperedges out :count (count out)})

    type
    (let [t (normalize-type type)
          clauses (cond-> [(list '= 'hx/type t)]
                    ;; denormalized :prop/* columns (H4) let repo/source-file
                    ;; push down — the [*] whole-type pull timed out live on
                    ;; the 259k-doc edits type (2026-07-11)
                    repo (conj (list '= 'prop/repo repo))
                    source-file (conj (list '= 'prop/source-file source-file)))
          query-tail (cond-> [(cons 'where clauses)]
                       latest? (conj (list 'order-by
                                           {:val 'prop/timestamp :dir :desc})
                                     '(limit 1))
                       (and (not latest?) (int? limit) (pos? limit))
                       (conj (list 'order-by {:val 'xt/id :dir :asc})
                             (list 'limit limit)))
          bounded? (or latest? (and (int? limit) (pos? limit)))
          selected (fxt/safe-q
                    node
                    (cons '->
                          (cons (hyperedge-from
                                 (if bounded?
                                   hyperedge-window-cols
                                   '[*])
                                 temporal)
                                query-tail)))
          docs (if bounded?
                 (hydrate-hyperedge-window node selected temporal)
                 selected)
          total (when include-total?
                  (if (or latest? repo source-file)
                    (count docs)
                    (count (fxt/safe-q node (list '->
                                                  (hyperedge-from
                                                   '[xt/id hx/type] temporal)
                                                  (list 'where (list '= 'hx/type t)))))))
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
          limited (if (and (not latest?) (int? limit) (pos? limit))
                    (take limit sorted)
                    sorted)
          out (mapv #(dissoc % :xt/id) limited)]
      {:hyperedges out
       :count (if (or (not include-total?) latest? repo source-file)
                (count out)
                total)
       :count-exact? (boolean include-total?)}))))

(defonce ^:private !hyperedge-query-cache (atom {}))

(defn invalidate-hyperedge-query-cache!
  "Invalidate materialized bounded query windows after a hyperedge mutation."
  []
  (reset! !hyperedge-query-cache {})
  nil)

(defn hyperedges-query
  "Read hyperedges, materializing bounded type windows that explicitly waive an
  exact total. The cache is invalidated synchronously by every server mutation."
  [node opts]
  (let [{:keys [type limit include-total?]} opts
        cacheable? (and type (int? limit) (pos? limit) (false? include-total?))
        cache-key [node opts]]
    (if-not cacheable?
      (hyperedges-query-uncached node opts)
      (if-let [cached (get @!hyperedge-query-cache cache-key)]
        cached
        (let [result (hyperedges-query-uncached node opts)]
          (when (>= (count @!hyperedge-query-cache) 32)
            (reset! !hyperedge-query-cache {}))
          (swap! !hyperedge-query-cache assoc cache-key result)
          result)))))

(def ^:private max-memory-projection-endpoints 20)
(def ^:private max-memory-projection-limit 100)
(def ^:private max-memory-projection-index-components 5000)
(defonce ^:private !memory-projection-indexes (atom {}))

(defn- any-equals
  [binding values]
  (if (= 1 (count values))
    (list '= binding (first values))
    (cons 'or (map #(list '= binding %) values))))

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
                                 (list '->
                                       '(from :evidence [*])
                                       (list 'where
                                             (list '= 'xt/id memory-id)))))]
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
  [revision components]
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
     :components-by-id components-by-id
     :by-endpoint by-endpoint}))

(defn initialize-memory-projection!
  "Build the bounded current-state memory index before the HTTP listener starts.

  This is a materialized projection, not a TTL cache: successful memory/assert
  puts and retractions advance it synchronously. Historical reads bypass it."
  [node]
  (let [started (System/nanoTime)
        selected+
        (fxt/safe-q
         node
         (list '->
               '(from :hyperedges [xt/id hx/type])
               (list 'where (list '= 'hx/type :memory/assert))
               (list 'return 'xt/id)
               (list 'order-by {:val 'xt/id :dir :asc})
               (list 'limit (inc max-memory-projection-index-components))))]
    (when (> (count selected+) max-memory-projection-index-components)
      (throw (gates/layered-error
              0 :memory-projection-index-bound-exceeded
              {:maximum max-memory-projection-index-components
               :observed-at-least (count selected+)})))
    (let [rows (hydrate-memory-components node (mapv :xt/id selected+) {})
          components (mapv hydrated-row->component rows)
          prior-revision (get-in @!memory-projection-indexes [node :revision] 0)
          index (build-memory-projection-index (inc prior-revision) components)]
      (swap! !memory-projection-indexes assoc node index)
      {:revision (:revision index)
       :component-count (count components)
       :endpoint-count (count (:by-endpoint index))
       :build-ms (elapsed-ms started)})))

(defn refresh-memory-projection-component!
  "Point-refresh one current memory/assert component after its verified put."
  [node edge-id]
  (when (contains? @!memory-projection-indexes node)
    (let [row (first (hydrate-memory-components node [edge-id] {}))
          component (when (= :memory/assert (:hx/type row))
                      (hydrated-row->component row))]
      (swap! !memory-projection-indexes
             update node
             (fn [{:keys [revision components-by-id]}]
               (build-memory-projection-index
                (inc revision)
                (cond-> (dissoc components-by-id edge-id)
                  component (assoc edge-id component)))))))
  nil)

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
        selected+
        (fxt/safe-q
         node
         (list '->
               (hyperedge-from '[xt/id hx/type hx/endpoints] temporal)
               (list 'unnest '{:matched-endpoint hx/endpoints})
               (list 'where
                     (list '= 'hx/type :memory/assert)
                     (any-equals 'matched-endpoint endpoints))
               (list 'return 'xt/id 'matched-endpoint)
               (list 'order-by
                     {:val 'matched-endpoint :dir :asc}
                     {:val 'xt/id :dir :asc})
               (list 'limit (inc raw-limit))))
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
          _ (when-not (contains? @!memory-projection-indexes node)
              (initialize-memory-projection! node))
          index (get @!memory-projection-indexes node)
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
     :count (count (fxt/safe-q node (list '-> '(from :hyperedges [xt/id hx/type])
                                    (list 'where (list '= 'hx/type (normalize-type type))))))}
    entity-type
    {:type entity-type :kind :entity
     :count (count (fxt/safe-q node (list '-> '(from :entities [xt/id entity/type])
                                    (list 'where (list '= 'entity/type
                                                       (normalize-type entity-type))))))}))
