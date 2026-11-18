(ns test-arxana-record-queries
  (:require [clojure.edn :as edn]
            [clojure.test :refer [deftest is run-tests]]
            [futon1b-arxana :as a]
            [futon1b-xt :as fxt]
            [futon1b-server :as srv]
            [xtdb.api :as xt]
            [xtdb.node :as xtn]))
(def before "2026-08-01T00:00:00.000Z")
(def at "2026-09-01T00:00:00.000Z")
(def event "2026-08-27T12:00:00.000Z")
(defn id [kind n]
  (str "arxana:" (case kind :clusion "clusion" :attestation "attestation" "record") ":sha256:fixture-" n))
(def c {:id (id :clusion 0) :type-revision [:span 1] :endpoints []
        :asserted-content {:start 10 :end 20 :numbers [10 11 12] :not-numeric "12"} :event-time event})
(defn rec [kind n]
  (merge {:id (id kind n) :event-time event :start 10 :end 20}
         (case kind
           :attestation {:subject-clusion-id (:id c) :method {:check/id "check-X"} :content {:score 7}}
           :assessment {:subject-clusion-id (:id c) :verdict :accepted :reason "fixture"}
           :external-reference {:record/id "external" :store/id "fixture"}
           :attempt {:subject/id (:id c) :outcome :ran-with-output}
           :mark-occurrence {:locator {:start 10 :end 20}})))
(def operations {:clusion :clusion/assert :attestation :evidence/append
                 :assessment :assessment/append :external-reference :external-reference/assert
                 :attempt :attempt/append :mark-occurrence :mark-occurrence/assert})
(defn put! [node kind r]
  (a/conditional-command! node {:protocol/version "arxana.v1" :message/id (:id r)
                                :operation (operations kind) :body {:record r}}))
(defn range-query [field lower upper]
  {:type-revision [:span 1] :field field :lower {:value lower :inclusive? true}
   :upper {:value upper :inclusive? false} :valid-time at})

(deftest queries
  (with-open [node (xtn/start-node)]
    (put! node :clusion c)
    (let [c2 (assoc c :id (id :clusion 1) :asserted-content {:start 30 :end 40})
          a1 (rec :attestation 2) a2 (rec :attestation 3)]
      (a/conditional-command! node {:protocol/version "arxana.v1" :message/id "batch"
                                    :operation :records/append
                                    :body {:records [{:operation :clusion/assert :record c2}
                                                     {:operation :evidence/append :record a1}]}})
      (put! node :attestation a2)
      (doseq [[kind n] [[:external-reference 4] [:assessment 5] [:attempt 6] [:mark-occurrence 7]]]
        (put! node kind (rec kind n)))
      (let [q {:record-kind :attestation :field [:method :check/id] :value "check-X" :valid-time at}]
        (is (= [a1 a2] (:records (a/records-by-field node q))))
        (is (:truncated? (a/records-by-field node (assoc q :limit 1))))
        (is (empty? (:records (a/records-by-field node (assoc q :valid-time before))))))
      (is (= [c] (:records (a/records-by-field-range node (range-query :numbers 9 13)))))
      (is (empty? (:records (a/records-by-field-range node (range-query :not-numeric 9 13)))))
      (is (:truncated? (a/records-by-field-range node (assoc (range-query :start 0 100) :limit 1))))
      (is (empty? (:records (a/records-by-field-range node (assoc (range-query :start 0 100) :valid-time before)))))
      (is (empty? (:records (a/records-by-field-range node (range-query :start 0 10)))))
      (is (= [c] (:records (a/records-by-field-range node (assoc-in (range-query :start 0 10) [:upper :inclusive?] true)))))
      (is (thrown? Exception (a/records-by-field-range node (dissoc (range-query :start 0 10) :upper))))
      ;; Half-open overlap [15,25): start<25 AND end>15, intersect two bounded queries.
      (let [starts (set (map :id (:records (a/records-by-field-range node (range-query :start 0 25)))))
            ends (set (map :id (:records (a/records-by-field-range node
                        (assoc-in (assoc-in (range-query :end 15 100) [:lower :inclusive?] false)
                                  [:upper :inclusive?] true)))))]
        (is (= #{(:id c)} (set (filter starts ends)))))
      (let [counts (:counts (a/type-census node {:valid-time at}))
            direct (reduce + (for [[_ table] a/record-kind-tables]
                               (:n (first (fxt/timed-q node
                                (list '-> (list 'from table '[xt/id]) '(aggregate {:n (row-count)})))))))]
        (is (= direct (reduce + (map :count counts))))
        (is (= 8 direct))
        (is (:truncated? (a/type-census node {:valid-time at :limit 1})))
        (is (every? #(zero? (:count %)) (:counts (a/type-census node {:valid-time before})))))
      (is (= 0 (:rows-written (a/backfill-record-fields! node)))))))

(deftest backfill-all-kinds
  (with-open [node (xtn/start-node)]
    (let [rs (into {:clusion c} (map-indexed (fn [n k] [k (rec k (+ n 10))])
                                            [:attestation :assessment :external-reference :attempt :mark-occurrence]))]
      (doseq [[kind r] rs]
        (xt/execute-tx node [[:put-docs {:into (a/record-kind-tables kind)
                                        :valid-from (java.time.Instant/parse event)}
                             (merge {:xt/id (:id r) :record/data (a/canonical-record-data r)}
                                    (a/flat-columns (operations kind) r))]]))
      ;; Prior packet-19 rows lack the new typed/kind columns; upgrade in place.
      (xt/execute-tx node
        [(into [:put-docs {:into :arxana_clusion_fields :valid-from (java.time.Instant/parse event)}]
               (map #(dissoc % :record/kind :record/table :field/number) (a/clusion-field-rows c)))])
      (is (pos? (:rows-written (a/backfill-record-fields! node))))
      (is (= (set (mapcat (fn [[kind r]] (a/record-field-rows kind r)) rs))
             (set (fxt/timed-q node '(from :arxana_clusion_fields [*])))))
      (with-redefs [xt/execute-tx (fn [& _] (throw (ex-info "second backfill wrote" {})))]
        (is (= 0 (:rows-written (a/backfill-record-fields! node)))))
      (is (= [(rs :external-reference)]
             (:records (a/records-by-field node {:record-kind :external-reference :field :store/id
                                                 :value "fixture" :valid-time at}))))
      (is (= [(rs :attestation)]
             (:records (a/records-by-field-range node {:record-kind :attestation :field [:content :score]
                        :lower {:value 7 :inclusive? true} :upper {:value 7 :inclusive? true} :valid-time at})))))))

(deftest http-query-dispatch
  (with-open [node (xtn/start-node)]
    (put! node :clusion c)
    (let [file (java.io.File/createTempFile "record-query" ".db")
          server (srv/start-server! {:node node :port 0 :arxana-index-path (.getPath file)})
          client (java.net.http.HttpClient/newHttpClient)
          url (java.net.URI/create (str "http://127.0.0.1:" (.getPort (.getAddress server)) "/api/alpha/arxana/query"))
          query (fn [body]
                  (let [request (-> (java.net.http.HttpRequest/newBuilder url)
                                    (.header "x-arxana-principal" a/service-principal)
                                    (.POST (java.net.http.HttpRequest$BodyPublishers/ofString (pr-str body)))
                                    .build)
                        response (.send client request (java.net.http.HttpResponse$BodyHandlers/ofString))]
                    [(.statusCode response) (edn/read-string (.body response))]))]
      (try
        (let [[status body] (query (assoc (range-query :start 0 25) :query/type :records-by-field-range))]
          (is (= 200 status))
          (is (= [c] (:records body))))
        (let [[status body] (query {:query/type :type-census :valid-time at :limit 1})]
          (is (= 200 status))
          (is (:truncated? body)))
        (is (= 400 (first (query {:query/type :records-by-field-range :type-revision [:span 1]
                                 :field :start :lower {:value "10" :inclusive? true}
                                 :upper {:value 20 :inclusive? true}}))))
        (finally (srv/stop-server! server) (.delete file))))))

(defn -main [& _]
  (let [r (run-tests 'test-arxana-record-queries)]
    (shutdown-agents)
    (when (pos? (+ (:fail r) (:error r))) (System/exit 1))))
