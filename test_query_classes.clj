(ns test-query-classes
  "Regression for the 2026-08-23 metaspace incident: XTDB 2 compiles each
   distinct query EXPRESSION (literals included) to JVM classes via `eval`, so
   an id spliced into an XTQL form costs ~6 classes per distinct value and the
   :7073 JVM reached 160k classloaders. Every read path must therefore go
   through `fxt/pq` (values as parameters). This test MEASURES it — it loads
   classes through the real read paths and asserts the count stays flat —
   rather than asserting form shape.

   Run: clojure -M:node -m test-query-classes"
  (:require [clojure.test :refer [deftest is run-tests testing]]
            [futon1b-evidence :as evidence]
            [futon1b-graph :as graph]
            [futon1b-server :as server]
            [futon1b-xt :as fxt]
            [xtdb.api :as xt]
            [xtdb.node :as xtn])
  (:import [java.lang.management ManagementFactory]))

(defn- loaded-classes []
  (.getLoadedClassCount (ManagementFactory/getClassLoadingMXBean)))

(defn- class-growth
  "Classes loaded while running F over N distinct ids, after one warm-up call
   (the first use of a query shape legitimately compiles once)."
  [n f]
  (f "warm-up-id")
  (let [c0 (loaded-classes)]
    (dotimes [i n] (f (str "distinct-" i)))
    (- (loaded-classes) c0)))

(def ^:private distinct-ids 150)
;; Generous: the parameterised shapes measured +0/+1 for 300 ids; an inlined
;; literal costs ~6 per id, so 150 ids would be ~900.
(def ^:private bound 40)

(deftest sensor-detects-per-literal-compilation
  ;; If this stops growing, XTDB has fixed upstream compilation and the other
  ;; assertions below are no longer load-bearing (but still true).
  (with-open [node (xtn/start-node)]
    (xt/execute-tx node [[:put-docs :entities {:xt/id "e1"}]])
    (let [growth (class-growth 30 #(fxt/safe-q node (list '-> '(from :entities [*])
                                                          (list 'where (list '= 'xt/id %)))))]
      (testing "inlined literal recompiles per value"
        (is (>= growth 100) (str "inlined growth " growth))))))

(deftest read-paths-keep-class-count-bounded
  (with-open [node (xtn/start-node)]
    (xt/execute-tx node [[:put-docs :entities {:xt/id "e1" :entity/id "e1" :entity/name "alpha"
                                               :entity/type :kind/a :entity/source "s"
                                               :entity/external-id "x1"}]
                         [:put-docs :hyperedges {:xt/id "h1" :hx/id "h1" :hx/type :t/a
                                                 :hx/endpoints ["e1"] :prop/repo "r"}]
                         [:put-docs :relations {:xt/id "r1" :relation/id "r1" :relation/type :rel/x
                                                :relation/from "e1" :relation/to "e1"}]
                         [:put-docs :evidence {:xt/id "ev1" :evidence/id "ev1" :evidence/at "2026-01-01"}]])
    (doseq [[label f]
            [["fxt/present?" #(fxt/present? node :entities %)]
             ["graph/fetch-entity (id, name, external-id)" #(graph/fetch-entity node %)]
             ["graph/entity-by-external" #(graph/entity-by-external node {:source % :external-id %})]
             ["graph/entities-latest by type" #(graph/entities-latest node {:type % :limit 5})]
             ["graph/entities-query by type+after+limit"
              #(graph/entities-query node {:type % :after % :limit 5})]
             ["graph/hyperedges-query type+repo+after+limit"
              #(graph/hyperedges-query node {:type % :repo % :after % :limit 5 :include-total? true})]
             ["graph/hyperedges-query end+type"
              #(graph/hyperedges-query node {:end % :type % :limit 5})]
             ["graph/hyperedge-by-id" #(graph/hyperedge-by-id node %)]
             ["graph/relations-query type+from+to"
              #(graph/relations-query node {:type % :from % :to %})]
             ["graph/census" (fn [v] (graph/census node {:type v}) (graph/census node {:entity-type v}))]
             ["server/fetch-current" #(server/fetch-current node %)]
             ["evidence/fetch-by-id" #(evidence/fetch-by-id node %)]]]
      (testing label
        (let [growth (class-growth distinct-ids f)]
          (is (< growth bound) (format "%s: +%d classes over %d distinct values" label growth distinct-ids)))))))

(defn -main [& _]
  (let [{:keys [fail error]} (run-tests 'test-query-classes)]
    (shutdown-agents)
    (System/exit (if (zero? (+ fail error)) 0 1))))
