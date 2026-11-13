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

(deftest excluded-identity-cannot-be-merged
  ;; Invented docs with an excluded external ID exercise the production guard.
  (doseq [xid m/excluded-xids]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Excluded: distinct documents"
                          (m/plan (assoc entry :xid xid) entities edges)))))

(deftest invented-planner
  (let [p (m/plan entry entities edges)]
    (is (= #{"held" "clock" "cluster" "code"} (set (map :xt/id (:rewrite p)))))
    (is (= ["duplicate"] (mapv :xt/id (:drop p))))
    (is (= "active" (get-in p [:winner-doc :entity/props :mission/status])))
    (is (= "verify" (get-in p [:winner-doc :entity/props :mission/phase])))
    (is (= {:mission/path "/invented/M-invented.md"} (:props-to-merge p)))
    (is (= 5 (:expected p)))
    (is (every? #(= {:invented true} (:hx/props %)) (:rewrite p)))))

(deftest invented-retired-id-name
  (let [ds (assoc-in entities [0 :entity/name] "fixture/loser")]
    (is (= "fixture/winner" (get-in (m/plan entry ds edges) [:winner-doc :entity/name])))
    (is (= ds (:entities (m/plan entry ds edges))))
    (is (= "A human title" (get-in (m/plan entry (assoc-in ds [0 :entity/name] "A human title") edges)
                                   [:winner-doc :entity/name]))))
  (let [ds (mapv #(assoc % :entity/name (:xt/id %)) entities)]
    (is (= "fixture/new-winner"
           (get-in (m/plan (assoc entry :winner "fixture/new-winner") ds edges) [:winner-doc :entity/name])))))

(deftest invented-reviewed-property-resolution
  (let [ds (mapv (fn [d id origin]
                   (assoc d :entity/props (assoc (:entity/props d)
                                                :mission/id id :migrated-from origin)))
                 entities ["M-invented.v1" "invented.v1"] ["origin-a" "origin-b"])
        resolutions {:mission/id {:expected #{"M-invented.v1" "invented.v1"} :value "invented.v1"}
                     :migrated-from {:expected #{"origin-a" "origin-b"} :value ["origin-a" "origin-b"]}}
        p (m/plan (assoc entry :property-resolutions resolutions) ds edges)]
    (is (= "M-invented.v1" (get-in (m/plan entry ds edges) [:winner-doc :entity/props :mission/id])))
    (is (= "invented.v1" (get-in p [:winner-doc :entity/props :mission/id])))
    (is (= ["origin-a" "origin-b"] (get-in p [:winner-doc :entity/props :migrated-from])))
    (is (= "M-invented" (get-in p [:winner-doc :entity/external-id])))
    (is (= "active" (get-in p [:winner-doc :entity/props :mission/status])))
    (is (= ds (:entities p)) "backups retain original unnormalized documents")
    (is (= resolutions (:property-resolutions (m/summary p))))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"no longer matches"
                          (m/plan (assoc entry :property-resolutions resolutions)
                                  (assoc-in ds [1 :entity/props :mission/id] "changed") edges)))
    (let [missing-winner (assoc entry :winner "fixture/new-winner")
          ds (assoc-in ds [1 :entity/props :mission/status] "active")]
      (is (seq (:prop-conflicts (m/plan missing-winner ds edges))))
      (is (empty? (:prop-conflicts (m/plan (assoc missing-winner :property-resolutions resolutions) ds edges)))))))

(deftest invented-rewrite-coalescence
  (let [p (m/plan entry entities [(edge "a" :held/on-mission ["x" "fixture/loser"])
                                 (edge "a2" :held/on-mission ["x" "fixture/loser"])
                                 (edge "b" :held/on-mission ["fixture/loser" "x"])])]
    ;; same direction coalesces; reversed direction is a different edge
    (is (= #{"a" "b"} (set (map :xt/id (:rewrite p)))))
    (is (= ["a2"] (mapv :xt/id (:drop p))))))

(deftest invented-references-beyond-endpoints
  ;; claude-13 review: real edges also name the mission in :hx/ends and props
  (let [d {:xt/id "hx|c|fixture/loser" :hx/id "hx|c|fixture/loser" :hx/type :cascade/cluster-member
           :hx/endpoints ["cluster" "fixture/loser"]
           :hx/ends [{:entity-id "cluster" :role :cluster} {:entity-id "fixture/loser" :role :mission}]
           :prop/mission "fixture/loser" :hx/props {:mission "fixture/loser" :note "fixture/loser-ish"}}
        r (first (:rewrite (m/plan entry entities [d])))]
    (is (= "hx|c|fixture/loser" (:xt/id r) (:hx/id r)) "ids unchanged")
    (is (= "fixture/winner" (get-in r [:hx/ends 1 :entity-id])))
    (is (= "fixture/winner" (:prop/mission r) (get-in r [:hx/props :mission])))
    (is (= "fixture/loser-ish" (get-in r [:hx/props :note])) "only exact matches")))

(deftest invented-winner-loser-edge-is-dropped
  (let [p (m/plan entry entities [(edge "nest" :mission-scope/nesting ["fixture/winner" "fixture/loser"])])]
    (is (empty? (:rewrite p)))
    (is (= ["nest"] (mapv :xt/id (:drop p))))))

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

(deftest invented-batch-continues-without-retry
  (let [root (str (java.nio.file.Files/createTempDirectory
                   "invented-batch-test" (make-array java.nio.file.attribute.FileAttribute 0)))
        calls (atom [])
        plans (mapv (fn [id] {:xid id :winner "fixture/winner" :losers ["fixture/loser"]})
                    ["invented-fail" "invented-ok" "invented-held" "invented-done"])]
    (with-redefs [m/execute-one! (fn [_ e _]
                                 (swap! calls conj (:xid e))
                                 (if (= "invented-fail" (:xid e))
                                   (throw (ex-info "Invented failure" {}))
                                   {:xid (:xid e) :status :OK}))]
      (let [r (m/run-clear! nil plans {:exclude #{"invented-held"} :done #{"invented-done"}
                                       :progress-path (str root "/progress.edn")
                                       :log-path (str root "/log.edn")})]
        (is (= ["invented-fail" "invented-ok"] @calls))
        (is (= :complete (:status r)))
        (is (= [1 1 2] (mapv #(count (get r %)) [:ok :fail :skipped])))
        (is (re-find #"Invented failure" (slurp (str root "/log.edn"))))))))

(let [r (run-tests 'test-merge-duplicate-entities)]
  (shutdown-agents)
  (System/exit (if (zero? (+ (:fail r) (:error r))) 0 1)))
