(ns test-ent-index
  "Throwaway-node acceptance tests for futon1b-entindex P1.

  Run: clojure -M:node -m test-ent-index
  Bad case: clojure -M:node -m test-ent-index --disable-tombstones"
  (:require [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [futon1b-entindex :as ent]
            [futon1b-graph :as graph]
            [futon1b-text :as text]
            [futon1b-xt :as fxt]
            [xtdb.api :as xt]
            [xtdb.node :as xtn])
  (:import [java.nio.file Files]))

(def ^:private unqualified {:builder-fn rs/as-unqualified-maps})

(defn- temp-dir []
  (-> (Files/createTempDirectory
       "futon1b-entindex-test-"
       (make-array java.nio.file.attribute.FileAttribute 0))
      .toFile .getAbsolutePath))

(defn- check! [label value]
  (println (format "  %-72s %s" label (if value "PASS" "FAIL")))
  (assert value label))

(defn- ds [] @text/!ds)

(defn- stored-doc [id]
  (some-> (jdbc/execute-one! (ds) ["SELECT doc FROM ent_node WHERE id = ?" id]
                             unqualified)
          :doc text/decode-doc))

(defn- ids-of-type [t]
  (mapv :id (jdbc/execute! (ds) ["SELECT id FROM ent_node WHERE type = ? ORDER BY id"
                                  (str t)] unqualified)))

(defn- entity [id t n]
  {:xt/id id :entity/id id :entity/name n :entity/type t
   :entity/props {:test true}})

(defn- clear-index! []
  (jdbc/execute! (ds) ["DELETE FROM ent_node"])
  (jdbc/execute! (ds) ["DELETE FROM ent_meta"]))

(defn -main [& args]
  (let [tombstones? (not (some #{"--disable-tombstones"} args))
        dir (temp-dir)]
    (with-open [node (xtn/start-node)]
      (text/init! {:path (str dir "/fts5-evidence.db")})
      (ent/init!)

      ;; Verified production write hook: complete hydrated row is cached.
      (let [doc (entity "ent:p1" :probe/old "P1 entity")]
        (graph/put-verified! node :entities doc)
        (let [hydrated (first (fxt/hydrate-by-ids node :entities ["ent:p1"]))]
          (check! "put -> sidecar doc equals the store's hydrated document"
                  (= hydrated (stored-doc "ent:p1")))
          (check! "put -> row is listed under its entity type"
                  (= ["ent:p1"] (ids-of-type :probe/old)))))

      ;; A same-id update replaces both type and body.
      (graph/put-verified! node :entities (entity "ent:p1" :probe/new "P1 moved"))
      (check! "type-changing update removes the old type membership"
              (empty? (ids-of-type :probe/old)))
      (check! "type-changing update adds the new type membership"
              (= ["ent:p1"] (ids-of-type :probe/new)))

      (graph/write-entities-batch!
       node {:entities [{:id "ent:batch:1" :name "batch one" :type :probe/batch}
                        {:id "ent:batch:2" :name "batch two" :type :probe/batch}]})
      (check! "verified entity batches synchronously index every document"
              (= ["ent:batch:1" "ent:batch:2"] (ids-of-type :probe/batch)))

      ;; Verified production retraction hook.
      (graph/retract-documents!
       node {:documents [{:table :entities :id "ent:p1"}
                         {:table :entities :id "ent:batch:1"}
                         {:table :entities :id "ent:batch:2"}]})
      (check! "retract removes the indexed row" (nil? (stored-doc "ent:p1")))

      ;; Explicit fill only: recreate current store state, then establish the
      ;; checkpoint from an empty index.
      (xt/execute-tx node [[:put-docs :entities (entity "ent:fill:1" :probe/fill "one")]
                             [:put-docs :entities (entity "ent:fill:2" :probe/fill "two")]])
      (clear-index!)
      (let [res (ent/fill! node :chunk 1 :pause-ms 0)
            hydrated (fxt/hydrate-by-ids node :entities ["ent:fill:1" "ent:fill:2"])]
        (check! "fill! hydrates in bounded chunks and sees both current entities"
                (= 2 (:filled res)))
        (check! "fill! documents equal the store's current hydrated entities"
                (= hydrated (mapv stored-doc ["ent:fill:1" "ent:fill:2"])))
        (check! "fill! leaves the read gate closed" (false? (ent/reads-usable?)))
        (check! "fill! refuses a second fill"
                (try (ent/fill! node) false (catch clojure.lang.ExceptionInfo _ true)))
        (ent/catch-up! node :force true)
        (check! "the first catch-up after fill opens the gate" (ent/reads-usable?)))

      ;; A fill that meets busy permits waits them out instead of failing.
      (clear-index!)
      (let [calls (atom 0)
            flaky (fn [f]
                    ;; every other store read finds the permits busy
                    (if (odd? (swap! calls inc))
                      (throw (ex-info "busy" {:timeout/phase :permit-acquire}))
                      (f)))
            res (ent/fill! node :chunk 1 :pause-ms 0 :busy-retry-ms 0
                           :with-page-permit flaky)]
        (check! "fill! retries busy permits and still fills every entity"
                (and (= 2 (:filled res))
                     (pos? (:busy-retries (:fill (ent/ent-stats)) 0)))))
      (ent/catch-up! node :force true)

      ;; Direct writes bypass hooks; the two system-time legs must repair them.
      (xt/execute-tx node [[:put-docs :entities
                             (entity "ent:direct" :probe/direct "direct")]])
      (check! "direct put is absent before catch-up" (nil? (stored-doc "ent:direct")))
      (ent/catch-up! node)
      (check! "catch-up repairs a direct put"
              (= (first (fxt/hydrate-by-ids node :entities ["ent:direct"]))
                 (stored-doc "ent:direct")))

      (xt/execute-tx node [[:delete-docs :entities "ent:direct"]])
      (check! "direct delete leaves a stale row before catch-up"
              (some? (stored-doc "ent:direct")))
      (ent/catch-up! node :force true :tombstones? tombstones?)
      ;; This is the named bad case: --disable-tombstones makes this assertion
      ;; fail, demonstrating that _system_from alone cannot repair a delete.
      (check! "catch-up repairs a direct delete through the tombstone leg"
              (nil? (stored-doc "ent:direct")))

      ;; Hook failure does not escape, closes the read gate, and is repaired.
      (let [doc (entity "ent:failed-hook" :probe/failure "failed hook")
            ds-atom text/!ds
            good @ds-atom]
        (reset! ds-atom (jdbc/get-datasource
                         {:dbtype "sqlite"
                          :dbname "/nonexistent-entindex-test/no.db"}))
        (try
          (check! "a failed sidecar hook does not fail the verified entity write"
                  (= :ok (graph/put-verified! node :entities doc)))
          (finally (reset! ds-atom good)))
        (check! "forced hook failure makes reads-usable? false"
                (false? (ent/reads-usable?)))
        (ent/catch-up! node :force true)
        (check! "the next catch-up repairs the row and reopens the gate"
                (and (= doc (stored-doc "ent:failed-hook"))
                     (ent/reads-usable?))))

      ;; The gate's promise: whenever reads-usable? is true, the index equals
      ;; the store. Break it the way production can: an entity write whose
      ;; hook fails lands while a catch-up is running. The injection happens
      ;; inside the k-th store page the catch-up takes, for every k, so it
      ;; lands before, between and after the legs. Bad case: read the next
      ;; checkpoint and the hook-failure count at the END of the run (P1 as
      ;; first committed) and the late k fail here.
      (let [index-matches-store?
            (fn []
              (let [store (into {} (map (juxt (comp str :xt/id) identity))
                                (xt/q node "SELECT * FROM entities"))
                    index (into {} (map (juxt :id (comp text/decode-doc :doc)))
                                (jdbc/execute! (ds) ["SELECT id, doc FROM ent_node"]
                                               unqualified))]
                (= store index)))]
        (doseq [k (range 1 13)]
          (let [calls (atom 0)
                id (str "ent:race:" k)
                inject! (fn []
                          (xt/execute-tx node [[:put-docs :entities
                                                (entity id :probe/race (str "race " k))]])
                          ;; what a failed on-put! hook records
                          (swap! ent/!stats update :hook-failures inc))
                wrapper (fn [f]
                          (when (= k (swap! calls inc)) (inject!))
                          (f))]
            (ent/catch-up! node :force true :with-page-permit wrapper)
            (check! (format "k=%2d: gate open only if the index equals the store" k)
                    (or (not (ent/reads-usable?)) (index-matches-store?)))
            (ent/catch-up! node :force true)
            (check! (format "k=%2d: the next catch-up repairs it and reopens the gate" k)
                    (and (ent/reads-usable?) (index-matches-store?))))))

      (println "ENT INDEX: ALL PASS"))
    (shutdown-agents)))
