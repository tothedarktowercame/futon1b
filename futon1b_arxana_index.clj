(ns futon1b-arxana-index
  "Rebuildable C3 role/subject sidecar with an O(1) sequence watermark."
  (:require [clojure.edn :as edn]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [futon1b-arxana :as arxana]
            [futon1b-xt :as fxt])
  (:import [java.sql Connection]))

(def unqualified {:builder-fn rs/as-unqualified-maps})
(def ddl
  ["CREATE TABLE IF NOT EXISTS arxana_edge(edge_type TEXT NOT NULL, endpoint_role TEXT NOT NULL, endpoint_id TEXT NOT NULL, event_time TEXT NOT NULL, edge_id TEXT NOT NULL, PRIMARY KEY(edge_type,endpoint_role,endpoint_id,event_time,edge_id)) WITHOUT ROWID"
   "CREATE TABLE IF NOT EXISTS arxana_att(subject_id TEXT NOT NULL, event_time TEXT NOT NULL, att_id TEXT NOT NULL, PRIMARY KEY(subject_id,event_time,att_id)) WITHOUT ROWID"
   "CREATE TABLE IF NOT EXISTS arxana_assessment(subject_id TEXT NOT NULL, event_time TEXT NOT NULL, assessment_id TEXT NOT NULL, PRIMARY KEY(subject_id,event_time,assessment_id)) WITHOUT ROWID"
   "CREATE TABLE IF NOT EXISTS arxana_attempt(subject_id TEXT NOT NULL, interpreter_id TEXT NOT NULL, outcome TEXT NOT NULL, event_time TEXT NOT NULL, receipt_id TEXT NOT NULL, PRIMARY KEY(subject_id,interpreter_id,event_time,receipt_id)) WITHOUT ROWID"
   "CREATE TABLE IF NOT EXISTS arxana_score_event(pattern_id TEXT NOT NULL, window TEXT NOT NULL, status TEXT NOT NULL, reason TEXT NOT NULL, record_id TEXT NOT NULL, weight REAL NOT NULL, PRIMARY KEY(pattern_id,window,status,reason,record_id)) WITHOUT ROWID"
   "CREATE TABLE IF NOT EXISTS arxana_score(pattern_id TEXT NOT NULL, window TEXT NOT NULL, score REAL NOT NULL, PRIMARY KEY(pattern_id,window)) WITHOUT ROWID"
   "CREATE TABLE IF NOT EXISTS arxana_meta(k TEXT PRIMARY KEY,v TEXT NOT NULL)"])

(defn open-index [path]
  (let [ds (jdbc/get-datasource {:dbtype "sqlite" :dbname path})]
    (doseq [s ddl] (jdbc/execute! ds [s]))
    {:ds ds :path path}))

(defn- rows [node table]
  (fxt/safe-q node [(str "SELECT _id, record$data FROM " table)]))

(defn source-records [node]
  {:clusions (mapv (comp edn/read-string :record/data) (rows node "arxana_clusions"))
   :evidence (mapv (comp edn/read-string :record/data) (rows node "arxana_evidence"))
   :assessments (mapv (comp edn/read-string :record/data) (rows node "arxana_assessments"))
   :attempts (mapv (comp edn/read-string :record/data) (rows node "arxana_attempts"))})

(defn current-watermark [node] (arxana/source-sequence node))
(defn index-watermark [{:keys [ds]}]
  (some-> (:v (first (jdbc/execute! ds ["SELECT v FROM arxana_meta WHERE k='watermark'"] unqualified)))
          Long/parseLong))

(defn endpoint-id [endpoint]
  (or (get-in endpoint [:target :entity/id])
      (get-in endpoint [:target :ref/id])
      (get-in endpoint [:target :artifact/revision-id])
      (get-in endpoint [:target :clusion/id])
      (pr-str (:target endpoint))))

(defn- index-clusion! [^Connection tx c]
  (jdbc/execute! tx ["DELETE FROM arxana_edge WHERE edge_id=?" (:id c)])
  (with-open [edge (.prepareStatement tx "INSERT INTO arxana_edge VALUES(?,?,?,?,?)")]
    (doseq [ep (:endpoints c)]
      (doto edge
        (.setString 1 (pr-str (:type-revision c)))
        (.setString 2 (name (:role ep)))
        (.setString 3 (str (endpoint-id ep)))
        (.setString 4 (str (or (:event-time c) "")))
        (.setString 5 (:id c))
        (.addBatch)))
    (.executeBatch edge)))

(defn- pattern-info [clusion]
  (when (= [:pattern/occurrence 1] (:type-revision clusion))
    {:pattern-id (some->> (:endpoints clusion)
                          (some #(when (= :pattern (:role %)) %)) endpoint-id str)
     :window (pr-str (get-in clusion [:asserted-content :window]))}))

(defn- refresh-score! [^Connection tx pattern-id window]
  (let [score (double (or (:score (first (jdbc/execute!
                                          tx ["SELECT SUM(weight) AS score FROM arxana_score_event WHERE pattern_id=? AND window=?"
                                              pattern-id window] unqualified))) 0.0))]
    (jdbc/execute! tx ["INSERT INTO arxana_score VALUES(?,?,?) ON CONFLICT(pattern_id,window) DO UPDATE SET score=excluded.score"
                       pattern-id window score])))

(defn- add-score-event! [^Connection tx {:keys [pattern-id window status reason record-id weight]}]
  (when (and pattern-id window (some? weight))
    (jdbc/execute! tx ["INSERT OR REPLACE INTO arxana_score_event VALUES(?,?,?,?,?,?)"
                       pattern-id window (name status) (str reason) record-id (double weight)])
    (refresh-score! tx pattern-id window)))

(defn- index-attestation! [^Connection tx a clusion]
  (jdbc/execute! tx ["DELETE FROM arxana_att WHERE att_id=?" (:id a)])
  (jdbc/execute! tx ["INSERT INTO arxana_att VALUES(?,?,?)"
                     (:subject-clusion-id a) (str (or (:event-time a) "")) (:id a)])
  (when-let [{:keys [pattern-id window]} (pattern-info clusion)]
    (when (= :model (:basis a))
      (add-score-event! tx {:pattern-id pattern-id :window window
                            :status (if (= :complete (:coverage a)) :occurrence :partial)
                            :reason (or (:reason a) "verdict") :record-id (:id a)
                            :weight (if (= :complete (:coverage a)) 1.0 0.5)}))))

(defn- index-assessment! [^Connection tx assessment]
  (jdbc/execute! tx ["DELETE FROM arxana_assessment WHERE assessment_id=?" (:id assessment)])
  (jdbc/execute! tx ["INSERT INTO arxana_assessment VALUES(?,?,?)"
                     (:subject-clusion-id assessment)
                     (str (or (:time assessment) (:event-time assessment) ""))
                     (:id assessment)]))

(defn- index-attempt! [^Connection tx receipt]
  (let [subject (:subject/id receipt) interpreter (str (get-in receipt [:dynamics :id]))
        event-time (str (:time receipt))]
    (jdbc/execute! tx ["DELETE FROM arxana_attempt WHERE receipt_id=?" (:id receipt)])
    (jdbc/execute! tx ["INSERT INTO arxana_attempt VALUES(?,?,?,?,?)"
                       subject interpreter (name (:outcome receipt)) event-time (:id receipt)])
    (when (= :ran-no-output (:outcome receipt))
      (let [pattern-id (or (get-in receipt [:inputs :pattern :ref/id])
                           (get-in receipt [:inputs :pattern-id]))
            window (pr-str (get-in receipt [:inputs :envelope :window]))]
        (add-score-event! tx {:pattern-id pattern-id :window window :status :void
                              :reason (:reason receipt) :record-id (:id receipt)
                              :weight -0.5})))))

(defn rebuild! [{:keys [ds]} node]
  (let [started (System/nanoTime)
        before (current-watermark node)
        records (source-records node)
        after (current-watermark node)]
    (when-not (= before after)
      (throw (ex-info "source moved during rebuild" {:before before :after after})))
    (jdbc/with-transaction [tx ds]
      (jdbc/execute! tx ["DELETE FROM arxana_edge"])
      (jdbc/execute! tx ["DELETE FROM arxana_att"])
      (jdbc/execute! tx ["DELETE FROM arxana_assessment"])
      (jdbc/execute! tx ["DELETE FROM arxana_attempt"])
      (jdbc/execute! tx ["DELETE FROM arxana_score_event"])
      (jdbc/execute! tx ["DELETE FROM arxana_score"])
      (with-open [edge (.prepareStatement ^Connection tx "INSERT INTO arxana_edge VALUES(?,?,?,?,?)")
                  att (.prepareStatement ^Connection tx "INSERT INTO arxana_att VALUES(?,?,?)")]
        (doseq [[i c] (map-indexed vector (:clusions records))
                ep (:endpoints c)]
          (doto edge
            (.setString 1 (pr-str (:type-revision c)))
            (.setString 2 (name (:role ep)))
            (.setString 3 (str (endpoint-id ep)))
            (.setString 4 (str (or (:event-time c) "")))
            (.setString 5 (:id c))
            (.addBatch))
          (when (zero? (mod (inc i) 1000)) (.executeBatch edge)))
        (.executeBatch edge)
        (doseq [[i a] (map-indexed vector (:evidence records))]
          (doto att
            (.setString 1 (:subject-clusion-id a))
            (.setString 2 (str (or (:event-time a) "")))
            (.setString 3 (:id a))
            (.addBatch))
          (when (zero? (mod (inc i) 1000)) (.executeBatch att)))
        (.executeBatch att))
      (doseq [assessment (:assessments records)]
        (index-assessment! tx assessment))
      (let [clusions (into {} (map (juxt :id identity) (:clusions records)))]
        (doseq [a (:evidence records)]
          (when-let [c (get clusions (:subject-clusion-id a))]
            (when (= :model (:basis a))
              (when-let [{:keys [pattern-id window]} (pattern-info c)]
                (add-score-event! tx {:pattern-id pattern-id :window window
                                      :status (if (= :complete (:coverage a)) :occurrence :partial)
                                      :reason (or (:reason a) "verdict") :record-id (:id a)
                                      :weight (if (= :complete (:coverage a)) 1.0 0.5)})))))
        (doseq [receipt (:attempts records)] (index-attempt! tx receipt)))
      (jdbc/execute! tx ["INSERT INTO arxana_meta(k,v) VALUES('watermark',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v" after]))
    {:status :ok :watermark after :records (+ (count (:clusions records)) (count (:evidence records))
                                  (count (:assessments records)) (count (:attempts records)))
     :elapsed-ms (/ (- (System/nanoTime) started) 1000000.0)}))

(defn- events-after [node sequence]
  (fxt/safe-q node ["SELECT source$sequence, record$id, record$table FROM arxana_index_events WHERE source$sequence > ? ORDER BY source$sequence ASC"
                    sequence]))

(defn catch-up! [{:keys [ds] :as index} node]
  (let [started (System/nanoTime) from (or (index-watermark index) 0)
        target (current-watermark node) events (events-after node from)]
    (jdbc/with-transaction [tx ds]
      (doseq [event events]
        (let [id (:record/id event)]
          (case (keyword (:record/table event))
            :arxana_clusions (index-clusion! tx (arxana/clusion-by-id node id))
            :arxana_evidence (let [a (arxana/evidence-by-id node id)]
                               (index-attestation! tx a (arxana/clusion-by-id node (:subject-clusion-id a))))
            :arxana_assessments (index-assessment! tx (arxana/assessment-by-id node id))
            :arxana_attempts (index-attempt! tx (arxana/attempt-by-id node id)))))
      (jdbc/execute! tx ["INSERT INTO arxana_meta(k,v) VALUES('watermark',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v" target]))
    {:from from :watermark target :events (count events)
     :elapsed-ms (/ (- (System/nanoTime) started) 1000000.0)}))

(defn snapshot [{:keys [ds]}]
  {:edges (jdbc/execute! ds ["SELECT * FROM arxana_edge ORDER BY edge_type,endpoint_role,endpoint_id,event_time,edge_id"] unqualified)
   :attestations (jdbc/execute! ds ["SELECT * FROM arxana_att ORDER BY subject_id,event_time,att_id"] unqualified)
   :assessments (jdbc/execute! ds ["SELECT * FROM arxana_assessment ORDER BY subject_id,event_time,assessment_id"] unqualified)
   :attempts (jdbc/execute! ds ["SELECT * FROM arxana_attempt ORDER BY subject_id,interpreter_id,event_time,receipt_id"] unqualified)
   :scores (jdbc/execute! ds ["SELECT * FROM arxana_score ORDER BY pattern_id,window"] unqualified)})

(defn query-edges [{:keys [ds] :as index} node {:keys [edge-type endpoint-role endpoint-id limit order]
                                                  :or {limit 50 order :desc}}]
  (let [source (current-watermark node) cached (or (index-watermark index) 0)]
    (if (not= source cached)
      {:status :stale-index :source/watermark source :index/watermark cached}
      (let [limit (long (min 200 (max 1 limit)))
            dir (if (= order :asc) "ASC" "DESC")
            sql (str "SELECT edge_id,event_time FROM arxana_edge WHERE edge_type=? AND endpoint_role=? AND endpoint_id=? ORDER BY event_time " dir ", edge_id " dir " LIMIT ?")
            count-sql "SELECT COUNT(*) AS n FROM arxana_edge WHERE edge_type=? AND endpoint_role=? AND endpoint_id=?"
            args [(pr-str edge-type) (name endpoint-role) (str endpoint-id)]
            total (:n (first (jdbc/execute! ds (into [count-sql] args) unqualified)))]
        {:status :ok :watermark cached :limit limit
         :total total :truncated? (> total limit)
         :edge-ids (mapv :edge_id (jdbc/execute! ds (into [sql] (conj args limit)) unqualified))}))))

(defn attestations [{:keys [ds] :as index} node subject-id limit]
  (let [source (current-watermark node) cached (or (index-watermark index) 0)]
    (if (not= source cached)
      {:status :stale-index :source/watermark source :index/watermark cached}
      {:status :ok :watermark cached :limit (min 200 (max 1 limit))
       :attestation-ids (mapv :att_id
                              (jdbc/execute! ds ["SELECT att_id FROM arxana_att WHERE subject_id=? ORDER BY event_time DESC,att_id DESC LIMIT ?"
                                                 subject-id (min 200 (max 1 limit))] unqualified))})))

(defn assessments [{:keys [ds] :as index} node subject-id limit]
  (let [source (current-watermark node) cached (or (index-watermark index) 0)]
    (if (not= source cached)
      {:status :stale-index :source/watermark source :index/watermark cached}
      (let [limit (long (min 200 (max 1 limit)))
            total (:n (first (jdbc/execute!
                              ds ["SELECT COUNT(*) AS n FROM arxana_assessment WHERE subject_id=?"
                                  subject-id] unqualified)))
            ids (mapv :assessment_id
                      (jdbc/execute!
                       ds ["SELECT assessment_id FROM arxana_assessment WHERE subject_id=? ORDER BY event_time DESC,assessment_id DESC LIMIT ?"
                           subject-id limit] unqualified))]
        {:status :ok :watermark cached :limit limit :total total
         :truncated? (> total limit)
         :assessments (mapv #(arxana/assessment-by-id node %) ids)}))))

(defn attempts [{:keys [ds] :as index} node subject-id interpreter-id limit]
  (let [source (current-watermark node) cached (or (index-watermark index) 0)]
    (if (not= source cached)
      {:status :stale-index :source/watermark source :index/watermark cached}
      (let [limit (min 200 (max 1 limit))
            rows (jdbc/execute! ds
                                ["SELECT receipt_id,outcome,event_time FROM arxana_attempt WHERE subject_id=? AND interpreter_id=? ORDER BY event_time DESC,receipt_id DESC LIMIT ?"
                                 subject-id (str interpreter-id) limit]
                                unqualified)]
        {:status :ok :watermark cached :limit limit
         :receipts (mapv #(select-keys % [:receipt_id :outcome :event_time]) rows)}))))

(defn pattern-score [{:keys [ds] :as index} node pattern-id window]
  (let [source (current-watermark node) cached (or (index-watermark index) 0)]
    (if (not= source cached)
      {:status :stale-index :source/watermark source :index/watermark cached}
      {:status :ok :watermark cached
       :score (double (or (:score (first (jdbc/execute!
                                          ds ["SELECT score FROM arxana_score WHERE pattern_id=? AND window=?"
                                              (str pattern-id) (pr-str window)] unqualified)))
                          0.0))})))
