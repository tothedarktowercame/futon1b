(ns test-merge-duplicate-entities
  "All fixture data below is invented; no copied mission or memory text."
  (:require [clojure.test :refer [deftest is run-tests]]
            [scripts.merge-duplicate-entities :as m]
            [futon1b-xt :as fxt]
            [futon1b-text :as text]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.node :as xtn]))

(def entry {:xid "M-invented" :winner "fixture/winner" :ids ["fixture/winner" "fixture/loser"]})
(def entities
  [{:xt/id "fixture/winner" :entity/external-id "M-invented"
    :entity/props {:mission/status "active" :mission/phase "verify"}}
   {:xt/id "fixture/loser" :entity/external-id "M-invented"
    :entity/props {:mission/status "old" :mission/path "/invented/M-invented.md"}}])
(defn edge [id t eps] {:xt/id id :hx/id id :hx/type t :hx/endpoints eps :hx/props {:invented true}})
(def edges
  [(edge "held" :held/on-mission ["item" "fixture/loser"])
   (edge "existing" :mission-scope/owns ["fixture/winner" "scope"])
   (edge "duplicate" :mission-scope/owns ["fixture/loser" "scope"])
   (edge "clock" :clock/clocked-on ["clock-item" "fixture/loser"])
   (edge "cluster" :cascade/cluster-member ["cluster" "fixture/loser"])
   (edge "code" (keyword "code" "v05-example") ["code" "fixture/loser"])])

(deftest invented-planner
  (let [p (m/plan entry entities edges)]
    (is (= #{"held" "clock" "cluster" "code"} (set (map :xt/id (:rewrite p)))))
    (is (= ["duplicate"] (mapv :xt/id (:drop p))))
    (is (= "active" (get-in p [:winner-doc :entity/props :mission/status])))
    (is (= "verify" (get-in p [:winner-doc :entity/props :mission/phase])))
    (is (= {:mission/path "/invented/M-invented.md"} (:props-to-merge p)))
    (is (= 5 (:expected p)))
    (is (every? #(= {:invented true} (:hx/props %)) (:rewrite p)))))

(deftest invented-rewrite-coalescence
  (let [p (m/plan entry entities [(edge "a" :held/on-mission ["x" "fixture/loser"])
                                 (edge "b" :held/on-mission ["fixture/loser" "x"])])]
    (is (= 1 (count (:rewrite p))))
    (is (= 1 (count (:drop p))))))

(deftest invented-missing-canonical-winner
  (let [p (m/plan (assoc entry :ids ["fixture/loser"])
                  [(assoc-in (second entities) [:entity/props :mission/status] "active")] edges)]
    (is (:winner-created? p))
    (is (= "fixture/winner" (get-in p [:winner-doc :xt/id])))
    (is (= "active" (get-in p [:winner-doc :entity/props :mission/status])))))

(deftest invented-property-conflict-and-null
  (is (nil? (get-in (m/plan entry (assoc-in entities [0 :entity/props :mission/status] nil) [])
                    [:winner-doc :entity/props :mission/status])))
  (is (= [:mission/path]
         (mapv :key (:prop-conflicts
                     (m/plan (update entry :ids conj "fixture/third")
                             (conj entities {:xt/id "fixture/third" :entity/external-id "M-invented"
                                             :entity/props {:mission/path "/invented/conflict.md"}}) []))))))


(deftest invented-read-only-sidecar
  (let [path (str (java.nio.file.Files/createTempFile
                   "invented-merge-sidecar" ".db" (make-array java.nio.file.attribute.FileAttribute 0)))
        ds {:jdbcUrl (str "jdbc:sqlite:" path)}]
    (jdbc/execute! ds ["CREATE TABLE ent_node (id TEXT PRIMARY KEY, doc BLOB)"])
    (jdbc/execute! ds ["CREATE TABLE hx_node (hx_id TEXT PRIMARY KEY, doc_edn TEXT)"])
    (jdbc/execute! ds ["CREATE TABLE hx_edge (hx_id TEXT, endpoint TEXT)"])
    (doseq [d entities]
      (jdbc/execute! ds ["INSERT INTO ent_node VALUES (?, ?)" (:xt/id d) (text/encode-doc d)]))
    (doseq [d edges]
      (jdbc/execute! ds ["INSERT INTO hx_node VALUES (?, ?)" (:xt/id d) (pr-str d)])
      (doseq [ep (:hx/endpoints d)]
        (jdbc/execute! ds ["INSERT INTO hx_edge VALUES (?, ?)" (:xt/id d) ep])))
    (is (= (m/summary (m/plan entry entities edges))
           (m/summary (m/sidecar-plan path entry))))))

(deftest invented-real-store-merge
  (with-open [node (xtn/start-node)]
    (let [root (str (java.nio.file.Files/createTempDirectory
                     "invented-merge-test" (make-array java.nio.file.attribute.FileAttribute 0)))
          opts {:driver-stopped? true :backup-root root :log-path (str root "/log.edn")}]
      (xt/execute-tx node (vec (concat (map #(vector :put-docs :entities %) entities)
                                      (map #(vector :put-docs :hyperedges %) edges))))
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"driver stop"
                            (m/execute-one! node entry (dissoc opts :driver-stopped?))))
      (is (= :OK (:status (m/execute-one! node entry opts))))
      (is (empty? (fxt/hydrate-by-ids node :entities ["fixture/loser"])))
      (is (empty? (fxt/hydrate-by-ids node :hyperedges ["duplicate"])))
      (is (= ["item" "fixture/winner"]
             (:hx/endpoints (first (fxt/hydrate-by-ids node :hyperedges ["held"])))))
      (is (= "active" (get-in (first (fxt/hydrate-by-ids node :entities ["fixture/winner"]))
                              [:entity/props :mission/status]))))))

(let [r (run-tests 'test-merge-duplicate-entities)]
  (shutdown-agents)
  (System/exit (if (zero? (+ (:fail r) (:error r))) 0 1)))
