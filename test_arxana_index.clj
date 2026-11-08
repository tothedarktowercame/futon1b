(ns test-arxana-index
  (:require [futon1b-arxana :as arxana]
            [futon1b-arxana-index :as index]
            [xtdb.node :as xtn])
  (:import [java.io File]))

(defn claim [n endpoint]
  {:id (format "arxana:clusion:sha256:%064x" n)
   :type-revision [:pattern/occurrence 1]
   :endpoints [{:role :pattern :kind :external-ref :target {:ref/id endpoint}}
               {:role :turn :kind :external-ref :target {:ref/id (str "turn-" n)}}]
   :asserted-content {:window [] :proposal "p"}
   :event-time (format "%08d" n)})
(defn put! [node n endpoint]
  (arxana/conditional-command!
   node {:protocol/version "arxana.v1" :message/id (str "m-" n)
         :operation :clusion/assert :body {:record (claim n endpoint)}}))

(defn -main [& _]
  (let [f (File/createTempFile "arxana-c3a-" ".db") _ (.delete f)]
    (try
      (with-open [node (xtn/start-node)]
        (doseq [n (range 60)] (put! node n "hot"))
        (let [idx (index/open-index (.getPath f))
              rebuild (index/rebuild! idx node)
              q (index/query-edges idx node {:edge-type [:pattern/occurrence 1]
                                             :endpoint-role :pattern
                                             :endpoint-id "hot" :limit 50})]
          (assert (= 50 (count (:edge-ids q))))
          (assert (= :ok (:status q)))
          (let [writer (future (doseq [n (range 60 100)] (put! node n "hot")))
                observed (loop [xs []]
                           (if (realized? writer) xs
                               (recur (conj xs (:status (index/query-edges
                                                        idx node {:edge-type [:pattern/occurrence 1]
                                                                  :endpoint-role :pattern
                                                                  :endpoint-id "hot" :limit 50}))))))]
            @writer
            (assert (every? #{:ok :stale-index} observed))
            (assert (= :stale-index (:status (index/query-edges
                                              idx node {:edge-type [:pattern/occurrence 1]
                                                        :endpoint-role :pattern
                                                        :endpoint-id "hot" :limit 50})))))
          (let [rebuilt (index/rebuild! idx node)
                q2 (index/query-edges idx node {:edge-type [:pattern/occurrence 1]
                                                :endpoint-role :pattern
                                                :endpoint-id "hot" :limit 50})]
            (assert (= :ok (:status q2)))
            (println (pr-str {:hot-key/count (count (:edge-ids q2))
                              :hot-key/query-limit 50
                              :initial-rebuild-ms (:elapsed-ms rebuild)
                              :prepared-rebuild-ms (:elapsed-ms rebuilt)
                              :records (:records rebuilt)
                              :concurrent-read-statuses [:ok :stale-index]})))))
      (println "C3a index checks PASS")
      (finally (.delete f))))
  (shutdown-agents))
