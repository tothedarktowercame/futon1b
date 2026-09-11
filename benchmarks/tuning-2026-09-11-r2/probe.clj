(require '[cheshire.core :as json]
         '[xtdb.node :as xtn]
         '[xtdb.api :as xt]
         '[futon1b-xt :as fxt]
         '[futon1b-graph :as graph]
         '[futon1b-request-executor :as executor])
(import '[java.util.concurrent Callable TimeUnit])
(defn millis [t] (/ (- (System/nanoTime) t) 1e6))
(defn seed! [node]
 (doseq [batch (partition-all 500 (range 5000))]
  (xt/execute-tx node
    [(into [:put-docs :evidence]
       (map (fn [i] {:xt/id (str "seed-" i) :evidence/id (str "seed-" i)
                    :evidence/type :memory :evidence/author "synthetic"
                    :evidence/body {:claim (apply str (repeat 1024 "x"))}}) batch))]))
 (xt/execute-tx node
  [(into [:put-docs :hyperedges]
    (for [i (range 128)]
     {:xt/id (str "edge-" i) :hx/id (str "edge-" i) :hx/type :memory/assert
      :hx/endpoints [(str "seed-" i) "synthetic-pattern"]
      :hx/props {:roles {:entry (str "seed-" i)}}}))]))
(defn point! [node i]
 (assert (some? (fxt/q1 node
                         (fxt/pq '[id] '(-> (from :evidence [*]) (where (= xt/id id)))
                         (str "seed-" (mod i 5000)))))))
(defn workload [mixed?]
  (let [foreground (vec (for [i (range 64)]
                         {:kind (if (zero? (mod i 16)) :projection :point) :id i}))
        background (vec (for [i (range 32)]
                         {:kind (if (even? i) :scan :background-write) :id i}))]
    (if mixed?
      (vec (mapcat (fn [pair bg] (conj (vec pair) bg))
                   (partition 2 foreground) background))
      foreground)))
(defn invoke! [node {:keys [kind id]}]
 (case kind
  :point (point! node id)
  :projection (graph/initialize-memory-projection! node)
  :scan (do (fxt/safe-q node '(-> (from :evidence [xt/id]) (order-by xt/id) (limit 1000))) nil)
  :background-write (xt/execute-tx node [[:put-docs :other-writer {:xt/id (str "other-" id) :value id}]])))
(defn submit! [pool node op]
  (let [submitted (System/nanoTime)]
    (.submit pool ^Callable
      (reify Callable
        (call [_]
          (let [admission (executor/task-observation)
                execution (System/nanoTime)
                result (try (invoke! node op) {:ok true}
                            (catch Exception e
                              {:ok false :error-class (.getName (class e))
                               :error-code (or (:error/code (ex-data e))
                                               (get-in (ex-data e) [:error :reason]))}))]
            (merge op result {:queue-ms (:queue-wait-ms admission)
                              :service-ms (millis execution)
                              :total-ms (millis submitted)})))))))
(defn wave! [pool node operations]
  (let [jobs (mapv #(submit! pool node %) operations)]
    (mapv #(.get % 120 TimeUnit/SECONDS) jobs)))
(defn scenario! [workers mixed?]
  (with-open [node (xtn/start-node)]
    (seed! node)
    (point! node 0)
    (let [before (xt/status node)
          pool (executor/bounded-executor workers 16)
          started (System/nanoTime)]
      (try
        (let [results (vec (mapcat #(wave! pool node %)
                                   (partition-all 16 (workload mixed?))))]
          {:mixed mixed? :workers workers :elapsed-ms (millis started)
           :before (pr-str before) :after (pr-str (xt/status node))
           :heap-max-bytes (.maxMemory (Runtime/getRuntime))
           :jvm-args (vec (.getInputArguments (java.lang.management.ManagementFactory/getRuntimeMXBean)))
           :heap-used-bytes (- (.totalMemory (Runtime/getRuntime)) (.freeMemory (Runtime/getRuntime)))
           :gc (mapv (fn [b] {:name (.getName b) :count (.getCollectionCount b) :time-ms (.getCollectionTime b)})
                     (java.lang.management.ManagementFactory/getGarbageCollectorMXBeans))
           :results results})
        (finally (.shutdownNow pool))))))
(let [[workers out order] *command-line-args*
      modes (if (= order "mixed-first") [true false] [false true])
      results (mapv #(scenario! (parse-long workers) %) modes)]
  (spit out (json/generate-string {:scenarios results} {:pretty true})))
(shutdown-agents)
