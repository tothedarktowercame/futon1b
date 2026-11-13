(ns scripts.merge-duplicate-entities
  "Batched mission merge. Loading this file never writes. Supply resolved entries
  {:xid :winner :ids}; resolve winner with the writers' mission-ids authority.
  dry-run! prints plans; execute-one! requires explicit driver-stopped? true.
  Backups and logs belong outside this public repository."
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [futon1b-text :as text]
            [clojure.set :as set]
            [futon1b-graph :as graph]
            [futon1b-gates :as gates]
            [futon1b-entindex :as ent]
            [futon1b-hxindex :as hx]
            [futon1b-xt :as fxt]
            [xtdb.api :as xt]))

(def excluded-xids
  "Distinct documents under different watcher roots; never merge by external ID."
  #{"M-coordination-rewrite" "M-u88-contextual-preferences"})

(defn edge-key
  "Type plus ORDERED endpoints: direction is part of an edge (parent/child,
  from/to), so [a b] and [b a] are different edges."
  [d] [(:hx/type d) (vec (:hx/endpoints d))])

(defn- repoint
  "Rewrite every reference to a loser in edge doc D to WINNER: endpoints, and
  any string value equal to a loser id anywhere else (:hx/ends entity-id,
  :prop/*, :hx/props). Document ids stay unchanged."
  [d ls winner]
  (let [f (fn walk [x]
            (cond (and (string? x) (ls x)) winner
                  (map? x) (into (empty x) (map (fn [[k v]] [k (walk v)])) x)
                  (vector? x) (mapv walk x)
                  (sequential? x) (map walk x)
                  (set? x) (into #{} (map walk) x)
                  :else x))]
    (reduce-kv (fn [m k v] (if (#{:xt/id :hx/id} k) (assoc m k v) (assoc m k (f v))))
               (empty d) d)))

(defn plan
  "Pure planner. All documents and edge metadata are retained. Existing winner
  values (including nil) take precedence; conflicting loser-only values refuse.
  Explicit :property-resolutions {key {:expected #{old-values} :value new-value}}
  apply only when all live values still match the reviewed expected set."
  [{:keys [winner ids property-resolutions] :as entry} entities edges]
  (when (excluded-xids (:xid entry))
    (throw (ex-info "Excluded: distinct documents under different watcher roots"
                    {:xid (:xid entry)})))
  (let [by-id (into {} (map (juxt :xt/id identity) entities))
        existing-win (get by-id winner)
        base (first (sort-by :xt/id entities))
        win (or existing-win
                (when base (cond-> (assoc base :xt/id winner :entity/props {})
                             (contains? base :entity/id) (assoc :entity/id winner))))
        losers (vec (sort (remove #{winner} ids)))
        ;; Entity GET also resolves names. Do not leave a retired ID as the
        ;; winner's name, or GET loser would still resolve to the winner.
        win (if ((set losers) (:entity/name win)) (assoc win :entity/name winner) win)
        _ (when-not (and winner win (every? by-id losers))
            (throw (ex-info "Missing entity; refresh worklist" {:entry entry})))
        stub? (= :excursion-stub (:merge-kind entry))
        _ (if stub?
            (when-not (and existing-win (= :excursion/doc (:entity/type win))
                           (every? #(= :mission/doc (:entity/type (by-id %))) losers))
              (throw (ex-info "Excursion stub plan requires existing excursion winner and mission losers" {})))
            (when-not (every? #(= (:entity/type win) (:entity/type %)) entities)
              (throw (ex-info "Cross-type merge requires explicit reviewed policy" {}))))
        _ (when (and stub? (seq property-resolutions))
            (throw (ex-info "Excursion stub inspection cannot resolve properties" {})))
        _ (doseq [d entities]
            (when-not (= (:entity/external-id win) (:entity/external-id d))
              (throw (ex-info "External IDs differ" {:id (:xt/id d)}))))
        _ (doseq [[k {:keys [expected]}] property-resolutions]
            (let [actual (set (keep (fn [d]
                                      (let [p (:entity/props d)]
                                        (when (contains? p k) [k (get p k)]))) entities))
                  values (set (map second actual))]
              (when-not (= expected values)
                (throw (ex-info "Property resolution no longer matches live values"
                                {:key k :expected expected :actual values})))))
        prop-conflicts (->> losers
                            (mapcat (fn [id] (seq (:entity/props (by-id id)))))
                            (remove (fn [[k _]] (contains? (:entity/props win) k)))
                            (group-by first)
                            (keep (fn [[k pairs]]
                                    (let [values (set (map second pairs))]
                                      (when (> (count values) 1) {:key k :values values}))))
                            (remove #(contains? property-resolutions (:key %)))
                            (sort-by (comp str :key)) vec)
        props (reduce (fn [p id]
                        (reduce-kv (fn [p k v] (if (contains? p k) p (assoc p k v)))
                                   p (or (:entity/props (by-id id)) {})))
                      (or (:entity/props win) {}) losers)
        approved-stub? (and stub? (= :copy-missing-provenance-path (:stub-path-policy entry)))
        stub-paths (when approved-stub?
                     (set (keep #(get-in (by-id %) [:entity/props :mission/path]) losers)))
        _ (when (and approved-stub?
                     (not (contains? (:entity/props win) :provenance/path))
                     (not (and (= 1 (count stub-paths))
                               (string? (first stub-paths))
                               (seq (first stub-paths)))))
            (throw (ex-info "Missing or conflicting stub paths" {:paths stub-paths})))
        props (if stub? (or (:entity/props win) {})
                  (merge props (update-vals property-resolutions :value)))
        props (if (and approved-stub? (not (contains? props :provenance/path)))
                (assoc props :provenance/path (first stub-paths)) props)
        withheld (when stub?
                   (into {} (map (fn [id]
                                   [id (apply dissoc (:entity/props (by-id id))
                                              (keys (:entity/props win)))]) losers)))
        ls (set losers)
        affected (filter #(some ls (:hx/endpoints %)) edges)
        unchanged (remove #(some ls (:hx/endpoints %)) edges)
        result (reduce
                (fn [{:keys [seen] :as r} d]
                  (let [rewritten (repoint d ls winner)
                        k (edge-key rewritten)
                        eps (:hx/endpoints rewritten)
                        ;; an edge between winner and loser becomes a self-loop
                        self-loop? (and (> (count eps) 1) (apply = eps))]
                    (if (or (seen k) self-loop?)
                      (update r :drop conj d)
                      (-> r (update :rewrite conj rewritten) (update :seen conj k)))))
                {:seen (set (map edge-key unchanged)) :rewrite [] :drop []}
                (sort-by :xt/id affected))]
    (merge (select-keys entry [:xid :winner :property-resolutions :merge-kind :stub-path-policy]) result
           {:losers losers :entities entities :edges edges
            :prop-conflicts prop-conflicts
            :inspection-only? (and stub? (not approved-stub?)) :withheld-loser-props withheld
            :winner-created? (nil? existing-win)
            :winner-doc (assoc win :entity/props props)
            :props-to-merge (apply dissoc props (keys (:entity/props win)))
            :before (count (filter #(some #{winner} (:hx/endpoints %)) edges))
            :expected (count (filter #(some #{winner} (:hx/endpoints %))
                                     (concat unchanged (:rewrite result))))})))

(defn incident [node ids]
  (let [rows (mapcat #(fxt/timed-q
                      node (fxt/pq '[p-end]
                                   '(-> (from :hyperedges [xt/id hx/endpoints])
                                        (unnest {:ep hx/endpoints})
                                        (where (= ep p-end)) (return xt/id)) %)) ids)]
    (fxt/hydrate-by-ids node :hyperedges (sort (set (map :xt/id rows))))))

(defn read-plan [node {:keys [ids winner] :as entry}]
  (let [ids (vec (distinct (conj ids winner)))]
    (plan (assoc entry :ids ids)
          (fxt/hydrate-by-ids node :entities ids) (incident node ids))))

(defn summary [p]
  (-> (select-keys p [:xid :winner :losers :winner-created? :prop-conflicts :props-to-merge :property-resolutions :before :expected :merge-kind :stub-path-policy :inspection-only? :withheld-loser-props])
      (assoc :missing-required-props (filterv #(not (get-in p [:winner-doc :entity/props %]))
                                              (if (= :excursion-stub (:merge-kind p))
                                                [:excursion/status :provenance/path]
                                                [:mission/status :mission/path]))
             :rewrite-by-type (frequencies (map :hx/type (:rewrite p)))
             :drop-by-type (frequencies (map :hx/type (:drop p))))))

(defn sidecar-plan
  "Read-only sidecar snapshot for inspection. Never used as write documents."
  ([sidecar entry] (sidecar-plan sidecar entry nil))
  ([sidecar {:keys [ids winner] :as entry} node]
  (when-not winner (throw (ex-info "Unresolved canonical winner" {:xid (:xid entry)})))
  (with-open [conn (jdbc/get-connection
                   {:jdbcUrl (str "jdbc:sqlite:file:" sidecar "?mode=ro")})]
    (jdbc/with-transaction [tx conn]
      (let [q (fn [sql] (jdbc/execute! tx sql {:builder-fn rs/as-unqualified-maps}))
            ids (vec (distinct (conj ids winner)))
            entities (keep #(some-> (first (q ["SELECT doc FROM ent_node WHERE id = ?" %]))
                                   :doc text/decode-doc) ids)
            rows (mapcat #(q ["SELECT DISTINCT n.hx_id, n.doc_edn FROM hx_node n JOIN hx_edge e ON n.hx_id=e.hx_id WHERE e.endpoint = ?" %]) ids)
            edges (vals (into {} (map (fn [{:keys [hx_id doc_edn]}]
                                       [hx_id (try
                                                (when-not doc_edn (throw (ex-info "Missing cached document" {})))
                                                (assoc (edn/read-string doc_edn) :xt/id hx_id)
                                                (catch Exception e
                                                  (or (when node (first (fxt/hydrate-by-ids node :hyperedges [hx_id])))
                                                      (throw (ex-info "Unreadable sidecar edge document" {:id hx_id} e)))))]) rows)))]
        (plan (assoc entry :ids ids) entities edges))))))

(defn indexed-read-plan [node sidecar entry]
  (when-not (hx/reads-usable?)
    (throw (ex-info "Hyperedge sidecar is not ready" {})))
  (let [snapshot (sidecar-plan sidecar entry node)
        ids (conj (:losers snapshot) (:winner snapshot))
        edge-ids (mapv :xt/id (:edges snapshot))
        edges (fxt/hydrate-by-ids node :hyperedges edge-ids)]
    (when-not (= (set edge-ids) (set (map :xt/id edges)))
      (throw (ex-info "Sidecar changed; refresh before merging" {})))
    (plan (assoc entry :ids ids)
          (fxt/hydrate-by-ids node :entities ids) edges)))

(defn dry-run! [node entries output sidecar]
  (with-open [w (io/writer output)]
    (doseq [entry entries
            :when (not (excluded-xids (:xid entry)))]
      (let [result (try (assoc (summary (if sidecar (sidecar-plan sidecar entry node)
                                           (read-plan node entry))) :mode :DRY-RUN)
                        (catch Exception e {:xid (:xid entry) :mode :DRY-RUN
                                            :winner (:winner entry) :ids (:ids entry)
                                            :error (.getMessage e) :data (ex-data e)}))]
        (.write w (str (pr-str result) "\n")) (.flush w)
        (prn result))))
  {:dry-run output :count (count (remove #(excluded-xids (:xid %)) entries))
   :excluded (mapv :xid (filter #(excluded-xids (:xid %)) entries))})

(defn- backup! [root p]
  (let [dir (io/file root (str (java.util.UUID/randomUUID)))]
    (.mkdirs dir)
    ;; A fresh directory on every attempt preserves even an interrupted run.
    (with-open [out (java.io.FileOutputStream. (io/file dir "before.edn"))]
      (.write out (.getBytes (binding [*print-length* nil *print-level* nil]
                                   (pr-str (select-keys p [:xid :winner :losers :entities :edges]))) "UTF-8"))
      (.sync (.getFD out)))
    (str dir)))

(defn verify-sidecar! [sidecar p]
  (with-open [conn (jdbc/get-connection
                   {:jdbcUrl (str "jdbc:sqlite:file:" sidecar "?mode=ro")})]
    (let [ids (conj (:losers p) (:winner p))
          read-ids (fn [id]
                     (set (map :hx_id (jdbc/execute! conn
                                      ["SELECT DISTINCT hx_id FROM hx_edge WHERE endpoint = ?" id]
                                      {:builder-fn rs/as-unqualified-maps}))))]
      (loop [attempt 0]
        (let [by-end (into {} (map (fn [id] [id (read-ids id)]) ids))]
          (if (and (every? empty? (map by-end (:losers p)))
                   (= (:expected p) (count (by-end (:winner p)))))
            (by-end (:winner p))
            (if (< attempt 100)
              (do (Thread/sleep 100) (recur (inc attempt)))
              (throw (ex-info "Sidecar verification failed" {:counts (update-vals by-end count)})))))))))

(defn execute-one!
  "One mission, two batches: verify puts before deleting anything. Call only
  after the driver owner confirms stop. Re-read under existing writer locks."
  [node entry {:keys [driver-stopped? backup-root log-path sidecar]}]
  (when-not (and (true? driver-stopped?) backup-root log-path)
    (throw (ex-info "Require driver stop confirmation, backup root and log" {})))
  (#'graph/with-entity-id-locks
   (conj (:ids entry) (:winner entry))
   (fn [] (graph/with-memory-projection-mutation
     node
     (fn []
       (let [p (if sidecar (indexed-read-plan node sidecar entry) (read-plan node entry))
             puts (into [[:entities (:winner-doc p)]]
                        (map (fn [d] [:hyperedges d]) (:rewrite p)))
             deletes (into (mapv (fn [id] [:entities id]) (:losers p))
                           (map (fn [d] [:hyperedges (:xt/id d)]) (:drop p)))
             _ (doseq [id (concat (map (comp :xt/id second) puts) (map second deletes))]
                 (when (gates/arxana-id? id)
                   (throw (ex-info "Reserved arxana ID" {:id id}))))
             _ (when-not (every? #(get-in p [:winner-doc :entity/props %])
                                 (if (= :excursion-stub (:merge-kind p))
                                                [:excursion/status :provenance/path]
                                                [:mission/status :mission/path]))
                 (throw (ex-info "Merged winner lacks status/path" (summary p))))
             _ (when (:inspection-only? p)
                 (throw (ex-info "Excursion stub plan is inspection-only; property policy needs review" {})))
             _ (when (seq (:prop-conflicts p))
                 (throw (ex-info "Conflicting loser props; resolve before executing" (summary p))))
             backup (backup! backup-root p)]
         (try
           (graph/with-entity-mutation
            #(xt/execute-tx node (mapv (fn [[table d]] [:put-docs table d]) puts)))
           (doseq [[table d] puts]
             (when-not (= d (first (fxt/hydrate-by-ids node table [(:xt/id d)])))
               (throw (ex-info "Put readback mismatch; no deletions performed" {:id (:xt/id d)}))))
           (ent/await-hook! (ent/on-put! (:winner-doc p)))
           (doseq [d (:rewrite p)] (when-let [f (hx/on-put! d)] @f))
           (graph/retract-documents! node {:documents (mapv (fn [[t id]] {:table t :id id}) deletes)})
           (let [after (if sidecar
                         (fxt/hydrate-by-ids node :hyperedges (verify-sidecar! sidecar p))
                         (incident node (conj (:losers p) (:winner p))))]
             (when-not (and (= (:expected p) (count after))
                            (not-any? #(seq (set/intersection (set (:losers p))
                                                             (set (:hx/endpoints %)))) after))
               (throw (ex-info "Edge verification failed" (summary p)))))
           (let [receipt (assoc (summary p) :status :OK :backup backup)]
             (spit log-path (str (pr-str receipt) "\n") :append true)
             receipt)
           (catch Exception e
             (spit log-path (str (pr-str {:xid (:xid entry) :status :FAIL
                                          :backup backup :error (.getMessage e)}) "\n") :append true)
             (throw e))
           (finally
             (doseq [d (concat (:rewrite p) (:drop p))]
               (graph/invalidate-hyperedge-query-cache! (:hx/type d))
               (graph/refresh-memory-projection-component! node (:xt/id d)))))))))))

(defn run-clear!
  "Run the clear entries of a reviewed dry-run once. EXCLUDE is an explicit
  set of held mission IDs; DONE contains previously completed mission IDs.
  Every execution replans from live state. Failures are logged and never retried.
  Persist progress after each mission so a detached eval remains observable."
  [node plans {:keys [exclude done progress-path log-path] :as opts}]
  (let [state (atom {:status :running :ok [] :fail [] :skipped []})
        save! #(spit progress-path (pr-str @state))]
    (save!)
    (doseq [p plans]
      (let [xid (:xid p)
            reason (cond
                     (excluded-xids xid) :distinct-documents
                     (contains? exclude xid) :held
                     (contains? done xid) :already-done
                     (:inspection-only? p) :inspection-only
                     (:error p) :dry-run-error
                     (seq (:prop-conflicts p)) :property-conflict
                     (seq (:missing-required-props p)) :missing-properties)]
        (if reason
          (swap! state update :skipped conj {:xid xid :reason reason})
          (let [entry (assoc (select-keys p [:xid :winner :merge-kind :stub-path-policy :property-resolutions])
                             :ids (conj (vec (:losers p)) (:winner p)))
                log-size (.length (io/file log-path))]
            (swap! state assoc :current xid)
            (save!)
            (try
              (let [receipt (execute-one! node entry opts)]
                (swap! state update :ok conj receipt))
              (catch Exception e
                (let [failure {:xid xid :status :FAIL :error (.getMessage e)
                               :data (ex-data e)}]
                  ;; execute-one! logs post-backup failures itself. Cover its
                  ;; preflight failures here without duplicating the log line.
                  (when (= log-size (.length (io/file log-path)))
                    (spit log-path (str (pr-str failure) "\n") :append true))
                  (swap! state update :fail conj failure))))))
        (swap! state dissoc :current)
        (save!)))
    (swap! state assoc :status :complete)
    (save!)
    @state))
