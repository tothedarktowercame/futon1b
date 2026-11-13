(ns test-hx-source-file
  "Invented fixtures only: source-file cached view equals the XTQL definition."
  (:require [clojure.test :refer [deftest is testing run-tests]]
            [clojure.edn :as edn]
            [futon1b-graph :as graph]
            [futon1b-hxindex :as hx]
            [futon1b-text :as text]
            [futon1b-xt :as fxt]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.node :as xtn])
  (:import [java.nio.file Files]))

(def source "/invented/source_%'file.clj")
(def opts {:type :invented/namespace :repo "invented-repo" :source-file source
           :limit 1 :include-total? false})
(defn doc [id repo path]
  {:xt/id id :hx/id id :hx/type :invented/namespace :hx/endpoints []
   :prop/repo repo :prop/source-file path :hx/props {:note "invented"}})
(defn query [node opts]
  (graph/invalidate-hyperedge-query-cache!)
  (graph/hyperedges-query node opts))
(defn comparable [r] (dissoc r :hx-index :doc-projection))
(defn setup! []
  (let [dir (Files/createTempDirectory "invented-source-file" (make-array java.nio.file.attribute.FileAttribute 0))]
    (text/init! {:path (str dir "/sidecar.db")})
    (hx/init!)
    @text/!ds))
(defn ready-docs! [ds]
  (doseq [k ["hx-doc-backfill" "hx-node-backfill" "checkpoint-ts"]]
    (jdbc/execute! ds ["INSERT OR REPLACE INTO hx_meta(k,v) VALUES(?,?)" k "2026-01-01T00:00:00Z"])))

(deftest invented-xtql-parity-and-hooks
  (with-open [node (xtn/start-node)]
    (let [ds (setup!)
          docs [(doc "a" "invented-repo" source) (doc "b" "invented-repo" source)
                (doc "c" "other-repo" source) (doc "d" "invented-repo" "/invented/other.clj")
                (-> (doc "e" "invented-repo" source)
                    (dissoc :prop/source-file)
                    (assoc :hx/props {:source-file source}))]]
      (xt/execute-tx node (mapv #(vector :put-docs :hyperedges %) docs))
      (doseq [d docs] @(hx/on-put! d))
      (ready-docs! ds)
      (with-redefs [hx/reads-usable? (constantly true)]
        (testing "completion marker gates the fast path and preserves real XTQL fallback"
          (is (not (hx/source-file-projection-ready?)))
          (let [calls (atom 0)]
            (graph/invalidate-hyperedge-query-cache!)
            (is (= ["a"] (mapv :hx/id (:hyperedges
                                      (graph/hyperedges-query node opts
                                                             (fn [n q] (swap! calls inc) (fxt/safe-q n q)))))))
            (is (pos? @calls))))
        ;; Emulate pre-migration rows, then complete from cached documents.
        (jdbc/execute! ds ["UPDATE hx_node SET repo=NULL,source_file=NULL"])
        (is (:complete (hx/backfill-source-files! ds node :page 1 :budget-ms 10000)))
        (is (hx/source-file-projection-ready?))
        (doseq [o [opts (assoc opts :after "a") (assoc opts :after "b")
                   (assoc opts :limit 10 :fields ["hx/id" "prop/source-file" "hx/props.note"])
                   (assoc opts :source-file "/invented/missing.clj")]]
          (let [fast (query node o)
                slow (with-redefs [hx/source-file-projection-ready? (constantly false)] (query node o))]
            (is (= (comparable slow) (comparable fast)))
            (is (true? (get-in fast [:doc-projection :served?])))))
        (testing "ready query does not enter XTDB"
          (graph/invalidate-hyperedge-query-cache!)
          (is (= 1 (:count (graph/hyperedges-query node opts (fn [& _] (throw (Exception. "XTDB entered"))))))))
        (testing "put moves the indexed file, including batch hook and column removal"
          (let [moved (assoc (first docs) :prop/source-file "/invented/moved.clj")]
            (xt/execute-tx node [[:put-docs :hyperedges moved]])
            @(hx/on-put! moved)
            (is (= ["b"] (mapv :hx/id (:hyperedges (query node opts)))))
            (hx/index-docs! ds [(dissoc moved :prop/source-file)])
            (is (empty? (hx/type-source-file-docs (assoc opts :source-file "/invented/moved.clj"))))))
        (testing "retract removes the indexed document"
          (graph/retract-documents! node {:documents [{:table :hyperedges :id "b"}]})
          ;; The production delete hook is asynchronous; await the same hook
          ;; before asserting the sidecar, as the caller's read contract requires.
          @(hx/on-delete! "b")
          (is (empty? (:hyperedges (query node opts)))))
        (doseq [o [(assoc opts :latest? true) (assoc opts :include-total? true)
                   (assoc opts :mission "invented") (assoc opts :end "invented")
                   (assoc opts :valid-as-of "2026-01-01T00:00:00Z")
                   (assoc opts :system-as-of "2026-01-01T00:00:00Z")]]
          (is (not (graph/type-projection-servable? o))))
        (with-redefs [hx/reads-usable? (constantly false)]
          (is (not (graph/type-projection-servable? opts))))))))

(deftest invented-backfill-concurrent-hooks
  (let [ds (setup!)
        d (doc "a" "invented-repo" source)
        original-read edn/read-string
        fired (atom false)]
    (ready-docs! ds)
    (hx/index-doc! ds d)
    (jdbc/execute! ds ["UPDATE hx_node SET repo=NULL,source_file=NULL"])
    (testing "a concurrent delete cannot be resurrected by the backfill snapshot"
      (with-redefs [edn/read-string (fn [s]
                                     (when (compare-and-set! fired false true) (hx/delete-id! ds "a"))
                                     (original-read s))]
        (is (:complete (hx/backfill-source-files! ds nil :page 1 :budget-ms 10000))))
      (is (empty? (jdbc/execute! ds ["SELECT * FROM hx_node WHERE hx_id='a'"]))))
    (testing "a concurrent update keeps the new source column and document"
      (jdbc/execute! ds ["DELETE FROM hx_meta WHERE k='source-columns-v1'"])
      (hx/index-doc! ds d)
      (reset! fired false)
      (with-redefs [edn/read-string (fn [s]
                                     (when (compare-and-set! fired false true)
                                       (hx/index-doc! ds (assoc d :prop/source-file "/invented/new.clj")))
                                     (original-read s))]
        (hx/backfill-source-files! ds nil :page 1 :budget-ms 10000))
      (is (= ["a"] (mapv :xt/id (with-redefs [text/!ds (atom ds)]
                                 (hx/type-source-file-docs (assoc opts :source-file "/invented/new.clj")))))))))

(defn -main [& _]
  (let [r (run-tests 'test-hx-source-file)]
    (shutdown-agents)
    (System/exit (if (zero? (+ (:fail r) (:error r))) 0 1))))
