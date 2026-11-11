(ns test-entity-alias-sidecar
  "Recorded-corpus acceptance for entity id/alias sidecar reads and migration.
  Run: clojure -M:node -m test-entity-alias-sidecar"
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is run-tests testing use-fixtures]]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [futon1b-entindex :as ent]
            [futon1b-graph :as graph]
            [futon1b-text :as text]
            [futon1b-xt :as fxt])
  (:import [java.nio.file Files]))

(def ^:private corpus-size 62799)
(def ^:private absent-threshold-ms 250.0)
(def ^:private unqualified {:builder-fn rs/as-unqualified-maps})
(def ^:dynamic *dir* nil)
(def ^:dynamic *docs* nil)

(defn- temp-dir []
  (-> (Files/createTempDirectory "futon1b-entity-alias-test-"
                                 (make-array java.nio.file.attribute.FileAttribute 0))
      .toFile .getAbsolutePath))

(defn- generated-docs []
  (let [shapes (edn/read-string
                (slurp (io/file "test-data/entity-sidecar-samples.edn")))]
    (mapv (fn [i]
            (let [sample (nth shapes (mod i (count shapes)))
                  id (format "recorded-shape-%05d" i)]
              (assoc sample :xt/id id :entity/id id
                     :entity/name (if (= i 31415) "alias/name-hit" (str "name/" id))
                     :entity/external-id (if (= i 27182) "alias/external-hit"
                                             (str "external/" id)))))
          (range corpus-size))))

(defn- old-schema-ds [path docs]
  (let [ds (jdbc/get-datasource {:dbtype "sqlite" :dbname path})]
    (jdbc/execute! ds ["CREATE TABLE ent_node (id TEXT PRIMARY KEY, type TEXT NOT NULL, doc BLOB NOT NULL)"])
    (jdbc/execute! ds ["CREATE INDEX ent_type_id ON ent_node(type,id)"])
    (jdbc/execute! ds ["CREATE TABLE ent_meta (k TEXT PRIMARY KEY, v TEXT)"])
    (jdbc/with-transaction [tx ds]
      (doseq [doc docs]
        (jdbc/execute-one! tx ["INSERT INTO ent_node(id,type,doc) VALUES(?,?,?)"
                               (str (:xt/id doc)) (str (:entity/type doc))
                               (text/encode-doc doc)])))
    ds))

(defn- open-gate! [ds]
  (jdbc/execute! ds ["INSERT INTO ent_meta(k,v) VALUES('checkpoint-ts','recorded-test')"])
  (jdbc/execute! ds ["INSERT INTO ent_meta(k,v) VALUES('hook-failures-at-catch-up',?)"
                     (str (:hook-failures @ent/!stats))])
  (reset! ent/!reads-enabled true))

(use-fixtures
 :once
 (fn [run]
   (let [dir (temp-dir)
         docs (generated-docs)]
     (text/init! {:path (str dir "/entities.db")})
     (ent/init!)
     ;; Use the production indexing function for every recorded-shape row.
     (doseq [doc docs] (ent/index-doc! @text/!ds doc))
     (open-gate! @text/!ds)
     (binding [*dir* dir *docs* docs] (run)))))

(deftest recorded-size-indexed-lookups
  (is (= corpus-size (:n (jdbc/execute-one! @text/!ds
                                            ["SELECT count(*) AS n FROM ent_node"]
                                            unqualified))))
  (is (= ["recorded-shape-31415"] (ent/alias-ids "alias/name-hit" true)))
  (is (= ["recorded-shape-27182"] (ent/alias-ids "alias/external-hit" true)))
  (is (= "recorded-shape-31415" (:xt/id (ent/entity-by-id "recorded-shape-31415"))))
  (let [started (System/nanoTime)
        result (ent/alias-ids "alias/definitely-absent" true)
        elapsed-ms (/ (double (- (System/nanoTime) started)) 1000000.0)]
    (println "  recorded-size absent alias elapsed-ms" elapsed-ms
             "threshold-ms" absent-threshold-ms)
    (is (empty? result))
    (is (< elapsed-ms absent-threshold-ms)
        (str "indexed absent alias exceeded diagnostic threshold: " elapsed-ms " ms"))))

(deftest graph-serving-and-fallback-contract
  (testing "serving gate: id and alias miss never touch XTDB"
    (with-redefs [fxt/safe-q (fn [& _] (throw (ex-info "XTDB scan forbidden" {})))]
      (let [result (graph/fetch-entity-warranted ::no-node "cold/sidecar-miss")
            repeated (graph/fetch-entity-warranted ::no-node "cold/sidecar-miss")]
        (is (nil? (:doc result)))
        (is (= :absent (get-in result [:warrant :warrant/claim])))
        (is (false? (get-in result [:warrant :warrant/reused?])))
        (is (true? (get-in repeated [:warrant :warrant/reused?]))))))
  (testing "closed gate falls back to the existing id and alias reads"
    (let [calls (atom 0)
          prior @ent/!reads-enabled]
      (reset! ent/!reads-enabled false)
      (try
        (with-redefs [fxt/safe-q (fn [& _] (swap! calls inc) [])]
          (is (nil? (:doc (graph/fetch-entity-warranted ::fake-node "closed-gate-miss"))))
          (is (= 2 @calls) "one id read and one alias scan used XTDB"))
        (finally (reset! ent/!reads-enabled prior))))))

(deftest old-ddl-migration-backfills-aliases
  (let [ds (old-schema-ds (str *dir* "/old-schema.db") *docs*)
        started (System/nanoTime)
        result (ent/init! {:ds ds})
        wall-ms (/ (double (- (System/nanoTime) started)) 1000000.0)
        hit (jdbc/execute-one! ds ["SELECT id FROM ent_node WHERE name = ?" "alias/name-hit"]
                               unqualified)
        external (jdbc/execute-one! ds
                                    ["SELECT id FROM ent_node WHERE external_id = ?"
                                     "alias/external-hit"] unqualified)]
    (println "  old-DDL migration rows" corpus-size "wall-ms" wall-ms
             "reported-ms" (get-in result [:alias-migration :elapsed-ms]))
    (is (= ["name" "external_id"] (get-in result [:alias-migration :columns-added])))
    (is (= "recorded-shape-31415" (:id hit)))
    (is (= "recorded-shape-27182" (:id external)))
    (let [again (:alias-migration (ent/init! {:ds ds}))]
      (is (empty? (:columns-added again)) "migration does not re-add columns")
      (is (false? (:backfilled? again)) "migration does not re-decode bodies"))))

(defn -main [& _]
  (let [{:keys [test pass fail error]} (run-tests 'test-entity-alias-sidecar)]
    (println (format "%d tests, %d assertions, %d failures, %d errors"
                     test (+ pass fail) fail error))
    (shutdown-agents)
    (System/exit (if (zero? (+ fail error)) 0 1))))
