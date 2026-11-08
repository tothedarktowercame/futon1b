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
   "CREATE TABLE IF NOT EXISTS arxana_meta(k TEXT PRIMARY KEY,v TEXT NOT NULL)"])

(defn open-index [path]
  (let [ds (jdbc/get-datasource {:dbtype "sqlite" :dbname path})]
    (doseq [s ddl] (jdbc/execute! ds [s]))
    {:ds ds :path path}))

(defn- rows [node table]
  (fxt/safe-q node [(str "SELECT _id, record$data FROM " table)]))

(defn source-records [node]
  {:clusions (mapv (comp edn/read-string :record/data) (rows node "arxana_clusions"))
   :evidence (mapv (comp edn/read-string :record/data) (rows node "arxana_evidence"))})

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

(defn- index-attestation! [^Connection tx a]
  (jdbc/execute! tx ["DELETE FROM arxana_att WHERE att_id=?" (:id a)])
  (jdbc/execute! tx ["INSERT INTO arxana_att VALUES(?,?,?)"
                     (:subject-clusion-id a) (str (or (:event-time a) "")) (:id a)]))

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
      (jdbc/execute! tx ["INSERT INTO arxana_meta(k,v) VALUES('watermark',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v" after]))
    {:watermark after :records (+ (count (:clusions records)) (count (:evidence records)))
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
            :arxana_evidence (index-attestation! tx (arxana/evidence-by-id node id)))))
      (jdbc/execute! tx ["INSERT INTO arxana_meta(k,v) VALUES('watermark',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v" target]))
    {:from from :watermark target :events (count events)
     :elapsed-ms (/ (- (System/nanoTime) started) 1000000.0)}))

(defn snapshot [{:keys [ds]}]
  {:edges (jdbc/execute! ds ["SELECT * FROM arxana_edge ORDER BY edge_type,endpoint_role,endpoint_id,event_time,edge_id"] unqualified)
   :attestations (jdbc/execute! ds ["SELECT * FROM arxana_att ORDER BY subject_id,event_time,att_id"] unqualified)})

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
