(ns scripts.migrate-workplan
  "Historical workplan migration. Real exports/manifests belong outside git.
   Requires futon3c's registered constructors on the classpath (or loaded first).
   Pure transform never manufactures item entities or current model verdicts."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as str]
            [futon3c.arxana.records :as records]
            [futon3c.arxana.digest :as digest]
            [futon3c.arxana.type-registry :as types]
            [futon1b-arxana :as arx]
            [futon1b-arxana-index :as idx]
            [futon1b-xt :as fxt]
            [xtdb.api :as xt])
  (:import [java.io FileOutputStream]
           [java.nio.charset StandardCharsets]
           [java.util UUID]))

(def transform-version "workplan-history-1")
(def tables {:clusion/assert :arxana_clusions :evidence/append :arxana_evidence
             :assessment/append :arxana_assessments})
(defn operation [r]
  (cond (:type-revision r) :clusion/assert (:basis r) :evidence/append
        (:verdict r) :assessment/append
        :else (throw (ex-info "Unsupported record" {:id (:id r)}))))
(defn validate-record! [r]
  (let [rebuilt ((case (operation r) :clusion/assert records/make-clusion
                       :evidence/append records/make-attestation
                       :assessment/append records/make-assessment) r)]
    (when-not (= r rebuilt)
      (throw (ex-info "Record identity mismatch" {:id (:id r) :computed (:id rebuilt)})))
    r))
(defn clusion-deps [r]
  (cond-> (set (keep #(when (= :clusion (:kind %)) (get-in % [:target :clusion/id]))
                     (:endpoints r)))
    (:subject-clusion-id r) (conj (:subject-clusion-id r))))
(defn entity-deps [r]
  (set (keep #(when (= :entity (:kind %)) (get-in % [:target :entity/id])) (:endpoints r))))
(defn entity-endpoint [role mapping]
  {:role role :kind :entity :target {:entity/id (:entity-id mapping)}})
(defn topo-sort [rs]
  (let [ids (set (map :id rs))]
    (loop [pending (vec rs) done #{} result []]
      (if (empty? pending) result
          (let [ready (sort-by :id (filter #(set/subset? (set/intersection ids (clusion-deps %)) done) pending))]
            (when (empty? ready) (throw (ex-info "Dependency cycle" {:ids (mapv :id pending)})))
            (let [next-done (into done (map :id ready))]
              (recur (filterv #(not (next-done (:id %))) pending) next-done (into result ready))))))))

(defn transform
  "Returns one manifest row per original record, plus topologically ordered
   target records. Mapping is explicit; missing/ambiguous items quarantine all
   dependent history. Rebuildable projections remain fully archived in export."
  [source item-map]
  (doseq [r source] (validate-record! r))
  (let [by-id (into {} (map (juxt :id identity)) source)
        results (atom {}) target (atom {})
        add! (fn [r]
               (validate-record! r)
               (when-let [old (get @target (:id r))]
                 (when-not (= old r) (throw (ex-info "Transformed ID conflict" {:id (:id r)}))))
               (swap! target assoc (:id r) r) (:id r))]
    (doseq [r (topo-sort source)]
      (let [id (:id r) rev (:type-revision r)
            subject (by-id (:subject-clusion-id r))
            item-subject? (= [:workplan/item 1] (:type-revision subject))
            dependencies (map @results (clusion-deps r))
            blocked (filter #(= :quarantined (:disposition %)) dependencies)
            item-entry (get item-map (if item-subject? (:id subject) id))
            anchor (fn [type]
                     (records/make-clusion
                      {:type-revision [type 1]
                       :endpoints [(entity-endpoint :item item-entry)]
                       :asserted-content {:legacy/source-record-id id
                                          :legacy/freshness :unknown}}))
            result
            (cond
              (= rev [:workplan/item 1])
              (if (= :resolved (:status item-entry))
                {:disposition :mapped-item :entity-id (:entity-id item-entry)}
                {:disposition :quarantined :reason (or (:reason item-entry) :missing-mapping)})

              (seq blocked) {:disposition :quarantined :reason :dependency
                             :dependencies (vec (sort (clusion-deps r)))}

              (= rev [:workplan/catalog 1])
              (if (= "workplan/all-items" (get-in r [:asserted-content :catalog/id]))
                {:disposition :archive-only :reason :recomputed-catalog}
                {:disposition :planned :target-ids [(add! r)]})

              (or (#{[:workplan/names 1] [:workplan/mention 1]} rev)
                  (some #(#{:rebuilt-later :archive-only} (:disposition %)) dependencies))
              {:disposition :rebuilt-later :reason :writer-projection}

              (= rev [:workplan/serves 1])
              (let [eps (mapv (fn [ep]
                                (if (= :item (:role ep))
                                  (entity-endpoint :item (item-map (get-in ep [:target :clusion/id]))) ep))
                              (:endpoints r))]
                {:disposition :planned :target-ids [(add! (records/make-clusion
                                                         (assoc r :type-revision [:workplan/serves 2] :endpoints eps)))]})

              item-subject?
              (cond
                (= :model (:basis r))
                (let [a (anchor :giraffe/legacy-reading)
                      e (records/make-attestation
                         (assoc r :subject-clusion-id (:id a) :standing :unverified
                                :attest/kind :giraffe/legacy-reading
                                :legacy/reduction? (contains? (:content r) :pending/level)))]
                  {:disposition :planned :target-ids [(add! a) (add! e)]})
                (:verdict r)
                (let [a (anchor :workplan/closure-claim)
                      verdict (records/make-assessment (assoc r :subject-clusion-id (:id a)))]
                  {:disposition :planned :target-ids [(add! a) (add! verdict)]})
                :else {:disposition :rebuilt-later :reason :item-provenance})

              (:basis r)
              (let [subject-result (@results (:subject-clusion-id r))
                    target-id (first (:target-ids subject-result))]
                (when-not target-id (throw (ex-info "Unmapped evidence subject" {:id id})))
                {:disposition :planned
                 :target-ids [(add! (records/make-attestation
                                    (cond-> (assoc r :subject-clusion-id target-id)
                                      (= :model (:basis r)) (assoc :standing :unverified))))]})

              (:verdict r)
              {:disposition :planned :target-ids [(add! (records/make-assessment
                    (assoc r :subject-clusion-id (first (:target-ids (@results (:subject-clusion-id r)))))))]}

              :else {:disposition :planned :target-ids [(add! r)]})]
        (swap! results assoc id (merge result {:source-id id
                                              :source-sha256 (digest/sha256-hex (.getBytes (arx/canonical-record-data r) StandardCharsets/UTF_8))
                                              :source-type (or rev (operation r))
                                              :source-dependencies (vec (sort (clusion-deps r)))
                                              :transform-version transform-version}))))
    (let [rs (topo-sort (vals @target)) ids (set (map :id rs))]
      (doseq [r rs]
        (when-not (set/subset? (clusion-deps r) ids)
          (throw (ex-info "Dangling transformed dependency" {:id (:id r)}))))
      {:records rs :manifest (mapv @results (sort (keys @results)))
       :counts {:source (count source) :target (count rs)
                :dispositions (frequencies (map :disposition (vals @results)))
                :target-kinds (frequencies (map #(or (:type-revision %) (operation %)) rs))}})))

(defn point-doc [node table id]
  (first (fxt/hydrate-by-ids node table [id])))
(defn point-docs
  "Full point reads in groups of four; never occupy all eight query permits."
  [node table ids]
  (into {}
        (mapcat (fn [group]
                  (let [jobs (mapv (fn [id] (future [id (point-doc node table id)])) group)]
                    (mapv deref jobs))))
        (partition-all 4 ids)))

(defn record-docs
  "Bounded SQL reads of Arxana records; never scan the large entity table."
  [node table ids]
  (assert (contains? (set (vals tables)) table))
  (merge (zipmap ids (repeat nil))
         (into {} (mapcat (fn [batch]
                           (map (juxt :xt/id identity)
                                (fxt/safe-q node
                                  (into [(str "SELECT * FROM " (name table) " WHERE _id IN ("
                                              (str/join "," (repeat (count batch) "?")) ")")]
                                        batch))))
                         (partition-all 100 (distinct (seq ids)))))))
(defn batch-record-docs [node rs]
  (into {} (mapcat (fn [[op group]] (record-docs node (tables op) (mapv :id group)))
                  (group-by operation rs))))

(defn record-doc [r]
  (cond-> (merge {:xt/id (:id r) :record/data (arx/canonical-record-data r)
                 :record/operation (operation r)} (arx/flat-columns (operation r) r))
    (:subject-clusion-id r) (assoc :subject/clusion-id (:subject-clusion-id r))))
(defn durable-spit! [path data]
  (io/make-parents path)
  (with-open [out (FileOutputStream. (io/file path))]
    (.write out (.getBytes (pr-str data) StandardCharsets/UTF_8))
    (.flush out) (.sync (.getFD out))))
(defn preflight
  "Bounded point reads: caller supplies at most 100 records per batch."
  [node rs item-map]
  (when (> (count rs) 100) (throw (ex-info "Batch exceeds 100 records" {})))
  (doseq [r rs]
    (validate-record! r)
    (when (= [:workplan/item 1] (:type-revision r))
      (throw (ex-info "Item mirrors are forbidden" {:id (:id r)}))))
  (when-not (= (count rs) (count (set (map :id rs))))
    (throw (ex-info "Duplicate batch IDs" {})))
  (let [ids (set (map :id rs))
        before (batch-record-docs node rs)
        entities (point-docs node :entities (set (mapcat entity-deps rs)))
        by-entity (into {} (keep (fn [[_ m]] (when (:entity-id m) [(:entity-id m) m]))) item-map)]
    (doseq [r rs]
      (when-let [old (get before (:id r))]
        (when-not (= r (edn/read-string (:record/data old)))
          (throw (ex-info "Immutable record conflict" {:id (:id r)})))))
    (doseq [r rs, id (set/intersection ids (clusion-deps r))]
      (when-not (:type-revision (first (filter #(= id (:id %)) rs)))
        (throw (ex-info "Subject is not a clusion" {:id id}))))
    (doseq [[id doc] (record-docs node :arxana_clusions
                                 (set/difference (set (mapcat clusion-deps rs)) ids))]
      (when-not doc
        (throw (ex-info "Missing clusion dependency" {:id id}))))
    (doseq [[id doc] entities]
      (let [m (by-entity id)]
        (when-not (and doc m (= :resolved (:status m))
                       (= (:entity-type m) (:entity/type doc))
                       (= (:external-id m) (:entity/external-id doc))
                       (or (not (contains? m :entity-path))
                           (= (:entity-path m) (or (get-in doc [:entity/props :provenance/path])
                                                  (get-in doc [:entity/props :mission/path])))))
          (throw (ex-info "Item mapping changed or missing" {:id id :mapping m})))) )
    {:before before :entities entities
     :new (filterv #(nil? (before (:id %))) (topo-sort rs))
     :equal (filterv #(some? (before (:id %))) rs)}))

(defn- placeholders [n] (str/join "," (repeat n "?")))

(defn batch-ops
  "Atomic append protocol with grouped assertions and puts. In-batch subjects
   are validated before writes; external subjects are asserted in the tx."
  [new existing counter entity-docs]
  (let [ids (set (map :id new))
        mids (mapv #(str "workplan-migration:" transform-version ":" (:id %)) new)
        external (vec (sort (set/difference (set (mapcat clusion-deps new)) ids)))
        groups (sort-by key (group-by operation new))
        equal-groups (sort-by key (group-by operation existing))
        events (mapv (fn [r n] {:xt/id n :source/sequence n :record/id (:id r)
                                :record/table (tables (operation r))})
                     new (range (inc counter) (+ counter 1 (count new))))
        outcomes (mapv (fn [r mid]
                         {:xt/id mid :outcome/result :committed
                          :outcome/response (pr-str {:protocol/version arx/protocol-version :message/id mid
                                                    :operation (operation r)
                                                    :ok {:record/id (:id r)
                                                         :subject-clusion-id (:subject-clusion-id r)
                                                         :created? true}})}) new mids)]
    (vec
     (concat
      [(if (zero? counter)
         [:sql "ASSERT NOT EXISTS (SELECT 1 FROM arxana_index_state WHERE _id='source')"]
         [:sql "ASSERT EXISTS (SELECT 1 FROM arxana_index_state WHERE _id='source' AND source$sequence=?)" [counter]])]
      (when (seq external)
        [[:sql (str "ASSERT (SELECT COUNT(*) FROM arxana_clusions WHERE _id IN ("
                    (placeholders (count external)) ")) = ?")
          (conj external (count external))]])
      (when (seq entity-docs)
        [[:sql (str "ASSERT (SELECT COUNT(*) FROM entities WHERE "
                    (str/join " OR " (repeat (count entity-docs)
                                             "(_id=? AND entity$type=? AND entity$external_id=?)"))
                    ") = ?")
          (conj (vec (mapcat (fn [[id doc]] [id (:entity/type doc) (:entity/external-id doc)])
                            (sort-by key entity-docs)))
                (count entity-docs))]])
      (for [[op group] equal-groups]
        [:sql (str "ASSERT (SELECT COUNT(*) FROM " (name (tables op)) " WHERE "
                   (str/join " OR " (repeat (count group) "(_id=? AND record$data=?)")) ") = ?")
         (conj (vec (mapcat #(vector (:id %) (arx/canonical-record-data %)) group)) (count group))])
      (for [[op group] groups]
        [:sql (str "ASSERT NOT EXISTS (SELECT 1 FROM " (name (tables op)) " WHERE _id IN ("
                   (placeholders (count group)) "))") (mapv :id group)])
      [[:sql (str "ASSERT NOT EXISTS (SELECT 1 FROM arxana_outcomes WHERE _id IN ("
                  (placeholders (count mids)) "))") mids]]
      (for [[op group] groups] (into [:put-docs (tables op)] (map record-doc group)))
      [(into [:put-docs :arxana_index_events] events)
       (into [:put-docs :arxana_outcomes] outcomes)
       [:put-docs :arxana_index_state {:xt/id "source" :source/sequence (+ counter (count new))}]]))))

(defn append-batch!
  "Review-gated caller supplies a bounded dependency group. No automatic retry:
   CAS races/content conflicts abort atomically and are reported for inspection."
  [node index rs item-map backup-root]
  (locking index
    (let [{:keys [before entities new equal]} (preflight node rs item-map)
          counter (arx/source-sequence node)
          backup (str (io/file backup-root (str (UUID/randomUUID)) "before.edn"))]
      (durable-spit! backup {:source-sequence counter :registry types/registry-v1
                            :records (into {} (map (fn [[id doc]] [id (or doc :ABSENT)]) before))
                            :entities entities :proposed rs})
      (when (seq new)
        (xt/execute-tx node (batch-ops new equal counter entities)
                       {:metadata {:workplan-migration transform-version}}))
      (let [actual (batch-record-docs node rs)]
        (doseq [r rs]
          (when-not (= (record-doc r) (actual (:id r)))
            (throw (ex-info "Full record/flat-column readback mismatch" {:id (:id r) :backup backup})))))
      (let [caught-up (idx/catch-up! index node)]
        (when-not (= (idx/index-watermark index) (arx/source-sequence node))
          (throw (ex-info "Sidecar did not catch up" {:backup backup})))
        {:created (count new) :equal (count equal) :before-sequence counter
         :after-sequence (arx/source-sequence node) :index caught-up :backup backup
         :ids (mapv :id rs)}))))

(defn dry-run!
  "Inspect all target IDs in bounded batches; no writes. XTQL/SQL records are
   authoritative, not sidecar absence. Includes equal/conflicting immutable IDs."
  [node transformed]
  (let [rs (:records transformed)
        existing (into {}
                       (mapcat
                        (fn [[op group]]
                          (mapcat
                           (fn [batch]
                             (let [ids (mapv :id batch)
                                   query (str "SELECT _id, record$data FROM " (name (tables op))
                                              " WHERE _id IN (" (str/join "," (repeat (count ids) "?")) ")")]
                               (map (fn [doc] [(:xt/id doc) (edn/read-string (:record/data doc))])
                                    (fxt/safe-q node (into [query] ids)))))
                           (partition-all 100 group)))
                        (group-by operation rs)))
        targets (mapv (fn [r] {:id (:id r) :kind (or (:type-revision r) (operation r))
                               :disposition (cond (not (contains? existing (:id r))) :planned
                                                  (= r (existing (:id r))) :existing-identical
                                                  :else :conflict)}) rs)]
    {:source-counts (:counts transformed) :target-counts (frequencies (map :disposition targets))
     :target-source-sequence (arx/source-sequence node) :targets targets}))
