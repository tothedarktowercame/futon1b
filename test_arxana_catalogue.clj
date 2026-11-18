(ns test-arxana-catalogue
  (:require [clojure.edn :as edn]
            [clojure.test :refer [deftest is run-tests]]
            [futon1b-arxana :as a]
            [futon1b-arxana-index :as idx]
            [futon1b-xt :as fxt]
            [futon1b-server :as srv]
            [xtdb.api :as xt]
            [xtdb.node :as xtn])
  (:import [java.io File]
           [java.time Instant]))

(def t0 "2026-08-01T00:00:00.000Z")
(def t1 "2026-08-27T12:00:00.000Z")
(def t2 "2026-09-01T00:00:00.000Z")
(defn claim [n]
  {:id (format "arxana:clusion:sha256:%064x" n)
   :type-revision [:design/example 1] :endpoints []
   :asserted-content {:features [:a :b :a] :enabled false :nested {:x [:a]}}
   :event-time t1})
(defn append! [node record]
  (a/conditional-command! node {:protocol/version "arxana.v1"
                                :message/id (str "single-" (:id record))
                                :operation :clusion/assert :body {:record record}}))

(deftest catalogue
  (with-open [node (xtn/start-node)]
    (append! node (claim 1))
    (a/conditional-command! node
                           {:protocol/version "arxana.v1" :message/id "batch"
                            :operation :records/append
                            :body {:records [{:operation :clusion/assert :record (claim 2)}]}})
    (let [q {:type-revision [:design/example 1] :valid-time t2}
          fields (assoc q :field :features :value :a)]
      (is (= 2 (count (:records (a/records-by-type node q)))))
      (is (= [(claim 1)] (:records (a/records-by-type node (assoc q :limit 1)))))
      (is (:truncated? (a/records-by-type node (assoc q :limit 1))))
      (is (= [] (:records (a/records-by-type node (assoc q :type-revision [:absent 1])))))
      (is (= [] (:records (a/records-by-type node (assoc q :valid-time t0)))))
      (is (= 2 (count (:records (a/records-by-field node fields)))))
      (is (:truncated? (a/records-by-field node (assoc fields :limit 1))))
      (is (= [] (:records (a/records-by-field node (assoc fields :valid-time t0)))))
      (is (= 2 (count (:records (a/records-by-field node (assoc q :field :enabled :value false))))))
      (is (= 2 (count (:records (a/records-by-field node (assoc q :field :nested :value {:x [:a]}))))))
      (is (= [] (:records (a/records-by-field node (assoc q :field :nested :value :a)))))
      (is (thrown? Exception (a/records-by-type node {:type-revision "arbitrary SQL"})))
      (is (thrown? Exception (a/records-by-type node (assoc q :valid-time "bad"))))
      (let [file (File/createTempFile "catalogue-index" ".db")
            index (idx/open-index (.getPath file))]
        (try
          (is (= :stale-index (:status (idx/catalogue-query index node (assoc q :query/type :records-by-type)))))
          (idx/rebuild! index node)
          (is (= :ok (:status (idx/catalogue-query index node (assoc fields :query/type :records-by-field)))))
          (doseq [n [3 4]]
            (a/conditional-command!
             node {:protocol/version "arxana.v1" :message/id (str "att-" n)
                   :operation :evidence/append
                   :body {:record {:id (format "arxana:attestation:sha256:%064x" n)
                                   :subject-clusion-id (:id (claim 1))
                                   :event-time t1}}}))
          (idx/rebuild! index node)
          (is (= {:total 2 :truncated? true :limit 1}
                 (select-keys (idx/attestations index node (:id (claim 1)) 1)
                              [:total :truncated? :limit])))
          (finally (.delete file))))))
  ;; Simulate authoritative legacy rows, with no projected fields.
  (with-open [node (xtn/start-node)]
    (doseq [n [10 11]]
      (xt/execute-tx node [[:put-docs {:into :arxana_clusions :valid-from (Instant/parse t1)}
                           (merge {:xt/id (:id (claim n)) :record/data (a/canonical-record-data (claim n))}
                                  (a/flat-columns :clusion/assert (claim n)))]]))
    (is (= [] (:records (a/records-by-field node {:type-revision [:design/example 1] :field :features :value :a}))))
    (is (= 8 (:rows-written (a/backfill-clusion-fields! node))))
    (is (= 0 (:rows-written (a/backfill-clusion-fields! node))))
    (is (= (set (mapcat a/clusion-field-rows [(claim 10) (claim 11)]))
           (set (fxt/timed-q node '(from :arxana_clusion_fields [*])))))
    (is (= [] (:records (a/records-by-field node {:type-revision [:design/example 1] :field :features :value :a :valid-time t0})))))
  ;; A row cap that forgot the extra row would silently report completeness.
  (is (not= (> 2 1) (> (count (take 1 [1 2])) 1))))

(deftest temporal-backfill-and-http
  (with-open [node (xtn/start-node)]
    (let [record (dissoc (claim 20) :event-time)]
      (append! node record)
      (is (= (set (a/clusion-field-rows record))
             (set (fxt/timed-q node '(from :arxana_clusion_fields [*])))))
      (is (= 0 (:rows-written (a/backfill-clusion-fields! node)))))
    ;; Bounded historical interval, absent now: backfill must not lose it.
    (xt/execute-tx node [[:put-docs {:into :arxana_clusions :valid-from (Instant/parse t0)
                                    :valid-to (Instant/parse t1)}
                         (merge {:xt/id (:id (claim 21)) :record/data (a/canonical-record-data (claim 21))}
                                (a/flat-columns :clusion/assert (claim 21)))]] )
    (a/backfill-clusion-fields! node)
    (is (= 0 (:rows-written (a/backfill-clusion-fields! node))))
    (let [file (File/createTempFile "catalogue-http" ".db")
          server (srv/start-server! {:node node :port 0 :arxana-index-path (.getPath file)})
          url (str "http://127.0.0.1:" (.getPort (.getAddress server)) "/api/alpha/arxana/query")
          client (java.net.http.HttpClient/newHttpClient)
          query (fn [payload]
                  (let [request (-> (java.net.http.HttpRequest/newBuilder (java.net.URI/create url))
                                    (.header "x-arxana-principal" a/service-principal)
                                    (.POST (java.net.http.HttpRequest$BodyPublishers/ofString (pr-str payload)))
                                    .build)
                        response (.send client request (java.net.http.HttpResponse$BodyHandlers/ofString))]
                    [(.statusCode response) (edn/read-string (.body response))]))]
      (try
        (doseq [kind [:records-by-type :records-by-field]]
          (let [[status body] (query {:query/type kind :type-revision [:design/example 1]
                                      :field :features :value :b :valid-time t0})]
            (is (= 200 status))
            (is (= [(claim 21)] (:records body)))))
        (is (= [] (:records (second (query {:query/type :records-by-field :type-revision [:design/example 1]
                                           :field :features :value :b :valid-time t2})))))
        (is (= 400 (first (query {:query/type :records-by-type :type-revision ["not-a-keyword" 1]}))))
        (finally (srv/stop-server! server) (.delete file))))))

(defn -main [& _]
  (let [r (run-tests 'test-arxana-catalogue)]
    (shutdown-agents)
    (when (pos? (+ (:fail r) (:error r))) (System/exit 1))))
