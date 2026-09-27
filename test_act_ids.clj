(ns test-act-ids
  "P6b: minted act identity and durable idempotency, real XTDB and HTTP.
  Run: clojure -M:node -m test-act-ids"
  (:require [clojure.edn :as edn]
            [clojure.test :refer [deftest is testing run-tests]]
            [futon1b-server :as server]
            [futon1b-graph :as graph]
            [futon1b-gates :as gates]
            [futon1b-xt :as fxt]
            [xtdb.node :as xtn])
  (:import [java.net URI]
           [java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers HttpResponse$BodyHandlers]
           [java.time Instant]))

(defn- post! [client base payload]
  (let [response (.send ^HttpClient client
                        (-> (HttpRequest/newBuilder (URI/create (str base "/api/alpha/hyperedge")))
                            (.header "Content-Type" "application/edn")
                            (.header "x-penholder" "api")
                            (.POST (HttpRequest$BodyPublishers/ofString (pr-str payload))) .build)
                        (HttpResponse$BodyHandlers/ofString))]
    [(.statusCode response) (edn/read-string (.body response))]))

(deftest ^:slow minted-acts-remain-distinct
  (gates/seed-mission-contract!)
  (with-open [node (xtn/start-node)]
    (let [srv (server/start-server! {:node node :port 0 :bind-host "127.0.0.1"})
          base (str "http://127.0.0.1:" (.getPort (.getAddress srv)))
          client (HttpClient/newHttpClient)
          put! #(post! client base %)
          t1 (.minusSeconds (Instant/now) 120)
          t2 (.minusSeconds (Instant/now) 60)
          payload {:hx/type :test/constrain :hx/endpoints ["joe" "rule"]
                   :hx/mint-id true :hx/valid-time (str t1)}
          ids-at (fn [type opts]
                   (set (map :hx/id (:hyperedges
                                    (graph/hyperedges-query node (merge {:type type :limit 100} opts))))))]
      (try
        (testing "the named bad case: two identical acts, independently retractable"
          (let [system-before (.minusSeconds (Instant/now) 1)
                [sa a] (put! payload)
                [sb b] (put! payload)
                system-after (Instant/now)
                ids #{(:hx/id a) (:hx/id b)}]
            (is (= [200 200] [sa sb]))
            (is (every? :ok [a b]))
            (is (= 2 (count ids)))
            (is (every? #(re-matches #"act:[0-9a-f-]{36}" %) ids))
            (is (= ids (ids-at :test/constrain {})))
            (is (empty? (ids-at :test/constrain {:system-as-of system-before})))
            (is (= ids (ids-at :test/constrain {:system-as-of system-after})))
            (let [[status res] (put! (-> payload (dissoc :hx/mint-id)
                                        (assoc :hx/id (:hx/id a) :hx/op "retract" :hx/valid-time (str t2))))]
              (is (= 200 status))
              (is (:retracted? res)))
            (is (= #{(:hx/id b)} (ids-at :test/constrain {})))
            (is (= ids (ids-at :test/constrain {:valid-as-of (.plusSeconds t1 1)})))
            (is (= ids (ids-at :test/constrain {:system-as-of system-after})))
            (is (= ids (ids-at :test/constrain {:system-as-of system-after
                                               :valid-as-of (.plusSeconds t1 1)})))
            (is (= 200 (first (put! (-> payload (dissoc :hx/mint-id)
                                       (assoc :hx/id (:hx/id b) :hx/op "retract"))))))
            (is (empty? (ids-at :test/constrain {})))))

        (testing "keyed retries, including concurrent delivery and post-retraction retry"
          (let [p (assoc payload :hx/type :test/keyed :hx/idempotency-key "delivery-1")
                replies (->> (range 6) (mapv (fn [_] (future (put! p)))) (mapv deref))
                id (get-in replies [0 1 :hx/id])]
            (is (every? #(= 200 (first %)) replies))
            (is (= #{id} (set (map #(get-in % [1 :hx/id]) replies))))
            (is (= 5 (count (filter #(get-in % [1 :no-op?]) replies))))
            (is (= #{id} (ids-at :test/keyed {})))
            (is (= id (:act/id (first (fxt/safe-q node '(from :hyperedge-act-keys [*]))))))
            (let [[status res] (put! (assoc p :hx/endpoints ["joe" "different-rule"]))]
              (is (= 409 status))
              (is (= :idempotency-conflict (get-in res [:error :reason]))))
            (is (= #{id} (ids-at :test/keyed {})))
            (is (= 200 (first (put! (-> p (dissoc :hx/mint-id :hx/idempotency-key)
                                       (assoc :hx/id id :hx/op "retract" :hx/valid-time (str t2)))))))
            (let [[status res] (put! p)]
              (is (= 200 status)) (is (= id (:hx/id res))) (is (:no-op? res)))
            (is (empty? (ids-at :test/keyed {})) "retry must not resurrect the retracted act")
            ;; Reload changes no authority: the same receipt is read from XTDB.
            (require 'futon1b-server :reload)
            (is (= 409 (first (put! (assoc p :hx/endpoints ["changed-after-reload"])))))
            (is (= id (get-in (put! p) [1 :hx/id])))
            (let [[status res] (put! (assoc p :hx/idempotency-key "delivery-2"))]
              (is (= 200 status)) (is (not= id (:hx/id res)))
              (is (= #{(:hx/id res)} (ids-at :test/keyed {}))))))

        (testing "non-minted writes retain derived IDs and no-op behaviour"
          (let [p {:hx/type :test/legacy :hx/endpoints ["joe" "rule"]}
                [sa a] (put! p) [sb b] (put! p)]
            (is (= [200 200] [sa sb]))
            (is (= "hx:test/legacy:joe.rule" (:hx/id a) (:hx/id b)))
            (is (:no-op? b))
            (is (= #{(:hx/id a)} (ids-at :test/legacy {})))))

        (testing "future-valid act is verified at its validity, not current time"
          (let [future-time (.plusSeconds (Instant/now) 120)
                [status res] (put! (assoc payload :hx/type :test/future :hx/valid-time (str future-time)))]
            (is (= 200 status)) (is (:ok res))
            (is (empty? (ids-at :test/future {})))
            (is (= #{(:hx/id res)} (ids-at :test/future {:valid-as-of (.plusSeconds future-time 1)})))))

        (testing "invalid opt-ins fail explicitly"
          (doseq [p [(assoc payload :hx/id "chosen-id")
                     (assoc payload :hx/op "retract")
                     (assoc payload :hx/mint-id "true")
                     (assoc payload :hx/idempotency-key "")
                     (assoc payload :hx/idempotency-key nil)
                     (-> payload (dissoc :hx/mint-id) (assoc :hx/idempotency-key "key"))]]
            (is (= 400 (first (put! p)))))
          (is (thrown? clojure.lang.ExceptionInfo (server/build-hyperedge-doc payload))))
        (finally (server/stop-server! srv))))))

(defn -main [& _]
  (let [result (run-tests 'test-act-ids)]
    (shutdown-agents)
    (System/exit (if (zero? (+ (:fail result) (:error result))) 0 1))))
