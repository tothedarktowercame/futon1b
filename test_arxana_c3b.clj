(ns test-arxana-c3b
  (:require [futon1b-arxana :as arxana]
            [futon1b-arxana-index :as index]
            [xtdb.node :as xtn])
  (:import [java.io File]))

(def claim
  {:id (str "arxana:clusion:sha256:" (apply str (repeat 64 "1")))
   :type-revision [:pattern/occurrence 1]
   :endpoints [{:role :pattern :kind :external-ref :target {:ref/id "pattern-1"}}
               {:role :turn :kind :external-ref :target {:ref/id "turn-1"}}]
   :asserted-content {:window ["turn-1"] :proposal "p"}})

(defn command [mid operation record]
  {:protocol/version "arxana.v1" :message/id mid :operation operation
   :body {:record record}})

(defn -main [& _]
  (let [db (File/createTempFile "arxana-c3b-" ".db") _ (.delete db)]
    (try
      (with-open [node (xtn/start-node)]
        (let [idx (index/open-index (.getPath db))
              att-id (str "arxana:attestation:sha256:" (apply str (repeat 64 "2")))
              receipt-id (str "arxana:record:sha256:" (apply str (repeat 64 "3")))
              ref-id (str "arxana:record:sha256:" (apply str (repeat 64 "4")))
              att {:id att-id :subject-clusion-id (:id claim) :basis :model
                   :coverage :complete :event-time "1"}
              receipt {:id receipt-id :subject/id (:id claim)
                       :dynamics {:id "馬/occurrence" :version 1}
                       :inputs {:pattern {:ref/id "pattern-1"}
                                :envelope {:window ["turn-1"]}}
                       :outcome :ran-no-output :reason "void" :time "2"}
              ref {:id ref-id :ref/kind :http-record :store/id :minus40
                   :store/endpoint "http://127.0.0.1:7093" :record/id "hx-1"
                   :schema/authority :futon1b/v1 :valid-time "2" :revision/digest "abc"}]
          (arxana/conditional-command! node (command "claim" :clusion/assert claim))
          (arxana/conditional-command! node (command "att" :evidence/append att))
          (arxana/conditional-command! node (command "attempt" :attempt/append receipt))
          (arxana/conditional-command! node (command "ref" :external-reference/assert ref))
          (index/catch-up! idx node)
          (assert (= receipt (arxana/attempt-by-id node receipt-id)))
          (assert (= ref (arxana/external-reference-by-id node ref-id)))
          (assert (= 1 (count (:receipts (index/attempts idx node (:id claim)
                                                         "馬/occurrence" 10)))))
          (assert (= 0.5 (:score (index/pattern-score idx node "pattern-1" ["turn-1"]))))
          (let [incremental (index/snapshot idx)]
            (index/rebuild! idx node)
            (assert (= incremental (index/snapshot idx))))
          (println "C3b reserved route, receipts, aggregate rebuild PASS")))
      (finally (.delete db)))))
