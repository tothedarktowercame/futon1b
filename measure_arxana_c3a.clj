(ns measure-arxana-c3a
  (:require [futon1b-arxana :as arxana]
            [futon1b-arxana-index :as index]
            [xtdb.api :as xt]
            [xtdb.node :as xtn])
  (:import [java.io File]))

(defn ms [started] (/ (- (System/nanoTime) started) 1000000.0))
(defn percentile [xs p]
  (nth (vec (sort xs)) (min (dec (count xs)) (long (Math/ceil (* p (count xs)))))))
(defn timed [f]
  (let [started (System/nanoTime) value (f)] {:value value :ms (ms started)}))
(defn stats [samples]
  {:median-ms (percentile samples 0.50) :p95-ms (percentile samples 0.95)})

(defn claim [n]
  (let [id (format "arxana:clusion:sha256:%064x" n)
        pattern (if (< n 100) "hot" (str "pattern-" n))]
    {:id id :type-revision [:pattern/occurrence 1]
     :endpoints [{:role :pattern :kind :external-ref :target {:ref/id pattern}}
                 {:role :turn :kind :external-ref :target {:ref/id (str "turn-" n)}}]
     :asserted-content {:window [] :proposal "p"}
     :event-time (format "%08d" n)}))

(defn attestation [n k]
  {:id (format "arxana:evidence:sha256:%063x%x" n k)
   :type :arxana/evidence-attestation
   :subject-clusion-id (format "arxana:clusion:sha256:%064x" n)
   :basis :model :standing :candidate :coverage :partial
   :event-time (format "%08d-%d" n k)})

(defn docs [table records]
  (mapv (fn [r] [:put-docs table {:xt/id (:id r)
                                  :record/data (pr-str r)
                                  :record/operation (case table
                                                      :arxana_clusions :clusion/assert
                                                      :arxana_evidence :evidence/append)}]) records))

(defn seed! [node]
  (doseq [chunk (partition-all 1000 (map claim (range 50000)))]
    (xt/execute-tx node (docs :arxana_clusions chunk)))
  (doseq [chunk (partition-all 1000 (for [n (range 50000) k (range 2)] (attestation n k)))]
    (xt/execute-tx node (docs :arxana_evidence chunk)))
  (xt/execute-tx node [[:put-docs :arxana_index_state
                          {:xt/id "source" :source/sequence 150000}]])
  150000)

(defn one-write! [node idx n]
  (let [record (claim n)
        started (System/nanoTime)
        outcome (arxana/conditional-command!
                 node {:protocol/version "arxana.v1" :message/id (str "scale-" n)
                       :operation :clusion/assert :body {:record record}})
        caught-up (index/catch-up! idx node)]
    {:elapsed-ms (ms started) :outcome (:status outcome) :catch-up caught-up}))

(defn bounded-traversal [idx node]
  ;; A bounded endpoint -> clusion -> endpoint-style walk. The index supplies
  ;; the reverse step; source records supply the other endpoints.
  (let [started (System/nanoTime)
        first-hop (index/query-edges idx node {:edge-type [:pattern/occurrence 1]
                                               :endpoint-role :pattern
                                               :endpoint-id "hot" :limit 50})
        endpoints (mapcat (fn [id] (:endpoints (arxana/clusion-by-id node id)))
                          (:edge-ids first-hop))]
    {:elapsed-ms (ms started) :status (:status first-hop)
     :relations (count (:edge-ids first-hop)) :endpoints (count endpoints)
     :truncated? (:truncated? first-hop)}))

(defn -main [& _]
  (let [db (File/createTempFile "arxana-c3a-scale-" ".db") _ (.delete db)]
    (try
      (with-open [node (xtn/start-node)]
        (let [seed-ms (:ms (timed #(seed! node)))
              idx (index/open-index (.getPath db))
              rebuilt (index/rebuild! idx node)
              hot #(index/query-edges idx node {:edge-type [:pattern/occurrence 1]
                                                 :endpoint-role :pattern
                                                 :endpoint-id "hot" :limit 50})
              att #(index/attestations idx node
                                       "arxana:clusion:sha256:0000000000000000000000000000000000000000000000000000000000000000" 50)
              hot-stats (stats (repeatedly 100 #(:ms (timed hot))))
              att-stats (stats (repeatedly 100 #(:ms (timed att))))
              traversal (bounded-traversal idx node)
              write (one-write! node idx 50000)
              stop? (atom false) stale (atom [])
              reader (future
                       (while (not @stop?)
                         (let [started (System/nanoTime) result (hot)]
                           (when (= :stale-index (:status result))
                             (swap! stale conj {:at (System/nanoTime) :ms (ms started)})))))
              write-start (System/nanoTime)
              _ (doseq [n (range 50001 50101)]
                  (arxana/conditional-command!
                   node {:protocol/version "arxana.v1" :message/id (str "concurrent-" n)
                         :operation :clusion/assert :body {:record (claim n)}}))
              _ (index/catch-up! idx node)
              concurrency-ms (ms write-start)
              _ (reset! stop? true) _ @reader
              stale-span-ms (if (seq @stale)
                              (/ (- (:at (last @stale)) (:at (first @stale))) 1000000.0)
                              0.0)
              report {:scale {:clusions 50000 :attestations 100000}
                      :seed-ms seed-ms
                      :full-rebuild-ms (:elapsed-ms rebuilt)
                      :hot-key (merge hot-stats {:limit 50 :total (:total (hot))})
                      :attestation-lookup att-stats
                      :bounded-traversal traversal
                      :write-including-catch-up write
                      :concurrent-writer {:writes 100 :elapsed-ms concurrency-ms
                                          :stale-reads (count @stale)
                                          :stale-span-ms stale-span-ms}
                      :budget-ms 5000}]
          (assert (= 150000 (:records rebuilt)))
          (assert (= 50 (count (:edge-ids (hot)))))
          (assert (= 2 (count (:attestation-ids (att)))))
          (assert (every? #(< % 5000) [(:p95-ms hot-stats) (:p95-ms att-stats)
                                       (:elapsed-ms traversal) (:elapsed-ms write)]))
          (prn report)))
      (finally (.delete db)))))
