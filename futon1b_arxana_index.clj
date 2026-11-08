(ns futon1b-arxana-index
  "Rebuildable C3 role/subject sidecar. The watermark is a digest of the
  authoritative record ids; stale caches refuse rather than answer."
  (:require [clojure.edn :as edn]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [futon1b-xt :as fxt])
  (:import [java.nio.charset StandardCharsets]
           [java.security MessageDigest]))

(def unqualified {:builder-fn rs/as-unqualified-maps})
(def ddl
  ["CREATE TABLE IF NOT EXISTS arxana_edge(edge_type TEXT NOT NULL, endpoint_role TEXT NOT NULL, endpoint_id TEXT NOT NULL, event_time TEXT NOT NULL, edge_id TEXT NOT NULL, PRIMARY KEY(edge_type,endpoint_role,endpoint_id,event_time,edge_id)) WITHOUT ROWID"
   "CREATE TABLE IF NOT EXISTS arxana_att(subject_id TEXT NOT NULL, event_time TEXT NOT NULL, att_id TEXT NOT NULL, PRIMARY KEY(subject_id,event_time,att_id)) WITHOUT ROWID"
   "CREATE TABLE IF NOT EXISTS arxana_meta(k TEXT PRIMARY KEY,v TEXT NOT NULL)"])

(defn open-index [path]
  (let [ds (jdbc/get-datasource {:dbtype "sqlite" :dbname path})]
    (doseq [s ddl] (jdbc/execute! ds [s]))
    {:ds ds :path path}))

(defn- sha256 [s]
  (let [d (.digest (MessageDigest/getInstance "SHA-256")
                   (.getBytes (str s) StandardCharsets/UTF_8))]
    (apply str (map #(format "%02x" %) d))))

(defn- rows [node table]
  (fxt/safe-q node [(str "SELECT _id, record$data FROM " table)]))

(defn source-records [node]
  {:clusions (mapv (comp edn/read-string :record/data) (rows node "arxana_clusions"))
   :evidence (mapv (comp edn/read-string :record/data) (rows node "arxana_evidence"))})

(defn watermark [records]
  (sha256 (pr-str (sort (concat (map :id (:clusions records))
                                (map :id (:evidence records)))))))

(defn current-watermark [node] (watermark (source-records node)))
(defn index-watermark [{:keys [ds]}]
  (:v (first (jdbc/execute! ds ["SELECT v FROM arxana_meta WHERE k='watermark'"] unqualified))))

(defn endpoint-id [endpoint]
  (or (get-in endpoint [:target :entity/id])
      (get-in endpoint [:target :ref/id])
      (get-in endpoint [:target :artifact/revision-id])
      (get-in endpoint [:target :clusion/id])
      (pr-str (:target endpoint))))

(defn rebuild! [{:keys [ds]} node]
  (let [started (System/nanoTime)
        records (source-records node)
        wm (watermark records)]
    (jdbc/with-transaction [tx ds]
      (jdbc/execute! tx ["DELETE FROM arxana_edge"])
      (jdbc/execute! tx ["DELETE FROM arxana_att"])
      (with-open [edge (.prepareStatement ^java.sql.Connection tx
                                          "INSERT INTO arxana_edge VALUES(?,?,?,?,?)")
                  att (.prepareStatement ^java.sql.Connection tx
                                         "INSERT INTO arxana_att VALUES(?,?,?)")]
        (doseq [c (:clusions records)
                ep (:endpoints c)]
          (doto edge
            (.setString 1 (pr-str (:type-revision c)))
            (.setString 2 (name (:role ep)))
            (.setString 3 (str (endpoint-id ep)))
            (.setString 4 (str (or (:event-time c) "")))
            (.setString 5 (:id c))
            (.addBatch)))
        (.executeBatch edge)
        (doseq [a (:evidence records)]
          (doto att
            (.setString 1 (:subject-clusion-id a))
            (.setString 2 (str (or (:event-time a) "")))
            (.setString 3 (:id a))
            (.addBatch)))
        (.executeBatch att))
      (jdbc/execute! tx ["INSERT INTO arxana_meta(k,v) VALUES('watermark',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v" wm]))
    {:watermark wm :records (+ (count (:clusions records)) (count (:evidence records)))
     :elapsed-ms (/ (- (System/nanoTime) started) 1000000.0)}))

(defn query-edges [{:keys [ds] :as index} node {:keys [edge-type endpoint-role endpoint-id limit order]
                                                  :or {limit 50 order :desc}}]
  (let [source (current-watermark node) cached (index-watermark index)]
    (if (not= source cached)
      {:status :stale-index :source/watermark source :index/watermark cached}
      (let [limit (long (min 200 (max 1 limit)))
            dir (if (= order :asc) "ASC" "DESC")
            sql (str "SELECT edge_id,event_time FROM arxana_edge WHERE edge_type=? AND endpoint_role=? AND endpoint_id=? ORDER BY event_time " dir ", edge_id " dir " LIMIT ?")]
        {:status :ok :watermark cached :limit limit
         :edge-ids (mapv :edge_id (jdbc/execute! ds [sql (pr-str edge-type) (name endpoint-role)
                                                       (str endpoint-id) limit] unqualified))}))))

(defn attestations [{:keys [ds] :as index} node subject-id limit]
  (let [source (current-watermark node) cached (index-watermark index)]
    (if (not= source cached)
      {:status :stale-index :source/watermark source :index/watermark cached}
      {:status :ok :watermark cached :limit (min 200 (max 1 limit))
       :attestation-ids (mapv :att_id
                              (jdbc/execute! ds ["SELECT att_id FROM arxana_att WHERE subject_id=? ORDER BY event_time DESC,att_id DESC LIMIT ?"
                                                 subject-id (min 200 (max 1 limit))] unqualified))})))
