(ns test-hyperedges-batch
  "POST /api/alpha/hyperedges/batch against the single-write route it batches.

  Run: clojure -M:node -m test-hyperedges-batch"
  (:require [clojure.edn :as edn]
            [clojure.test :refer [deftest is testing run-tests]]
            [futon1b-server :as server]
            [futon1b-gates :as gates]
            [futon1b-graph :as graph]
            [futon1b-text :as text]
            [futon1b-hxindex :as hx]
            [xtdb.api :as xt]
            [xtdb.node :as xtn])
  (:import [java.net URI]
           [java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers HttpResponse$BodyHandlers]
           [java.nio.file Files]))

(defn- post [client base path body]
  (let [r (.send client
                 (-> (HttpRequest/newBuilder (URI/create (str base path)))
                     (.header "Content-Type" "application/edn")
                     (.header "x-penholder" "api")
                     (.POST (HttpRequest$BodyPublishers/ofString (pr-str body)))
                     .build)
                 (HttpResponse$BodyHandlers/ofString))]
    [(.statusCode r) (edn/read-string (.body r))]))

(defn- var-payload [qname vt]
  {:hx/type "code/v05/var" :hx/endpoints [(str "repo/" qname)]
   :hx/labels ["repo"] :hx/valid-time vt
   :hx/props {"var/qname" qname "var/kind" "defn"}})

(deftest batch-writes-match-single-writes
  (gates/seed-mission-contract!)
  (with-open [node (xtn/start-node)]
    (let [dir (.toFile (Files/createTempDirectory "hx-batch" (make-array java.nio.file.attribute.FileAttribute 0)))
          _ (text/init! {:path (str dir "/sidecar.db")})
          _ (hx/init!)
          srv (server/start-server! {:node node :port 0 :bind-host "127.0.0.1"})
          base (str "http://127.0.0.1:" (.getPort (.getAddress srv)))
          client (HttpClient/newHttpClient)
          vt 1790000000000
          query (fn [t] (set (map :hx/id (:hyperedges (graph/hyperedges-query node {:type t :limit 100})))))]
      (try
        (testing "batched and unbatched items, each readable on return"
          (let [items [(var-payload "a/one" vt) (var-payload "a/two" vt) (var-payload "a/three" vt)
                       ;; no valid-time: goes through the single-write path
                       {:hx/type "code/v05/mission-doc" :hx/endpoints ["M-x"] :hx/labels ["repo"]}]
                [status body] (post client base "/api/alpha/hyperedges/batch" {:hyperedges items})]
            (is (= 200 status))
            (is (:ok body))
            (is (= 4 (count (:results body))))
            (is (every? :ok (:results body)))
            (is (= (set (map :hx/id (take 3 (:results body)))) (query (keyword "code/v05/var"))))
            (is (contains? (query (keyword "code/v05/mission-doc")) (:hx/id (last (:results body)))))))

        (testing "a batched doc is the doc the single route writes"
          (let [[_ single] (post client base "/api/alpha/hyperedge" (var-payload "b/single" vt))
                [_ batch] (post client base "/api/alpha/hyperedges/batch"
                                {:hyperedges [(var-payload "b/batch" vt)]})
                strip #(dissoc % :xt/id :hx/id :hx/endpoints :hx/ends :hx/props)]
            (is (:ok single))
            (is (:ok batch))
            (is (= (strip (graph/hyperedge-by-id node (:hx/id single)))
                   (strip (graph/hyperedge-by-id node (-> batch :results first :hx/id)))))))

        (testing "a malformed item fails alone"
          (let [[status body] (post client base "/api/alpha/hyperedges/batch"
                                    {:hyperedges [(var-payload "c/good" vt)
                                                  {:hx/endpoints ["no-type"] :hx/valid-time vt}]})]
            (is (= 200 status))
            (is (false? (:ok body)))
            (is (:ok (first (:results body))))
            (is (false? (:ok (second (:results body)))))))

        (testing "a failed batch transaction falls back to single writes"
          (let [real xt/execute-tx]
            (with-redefs [xt/execute-tx (fn [n ops & more]
                                          (if (> (count ops) 1)
                                            (throw (ex-info "simulated batch failure" {}))
                                            (apply real n ops more)))]
              (let [[_ body] (post client base "/api/alpha/hyperedges/batch"
                                   {:hyperedges [(var-payload "d/one" vt) (var-payload "d/two" vt)]})]
                (is (:ok body))
                (is (every? :ok (:results body)))
                (is (every? (query (keyword "code/v05/var")) (map :hx/id (:results body))))))))

        (testing "the batch size is bounded"
          (is (thrown? Exception
                       (server/write-hyperedges-batch!
                        node (vec (for [i (range 501)] (var-payload (str "e/" i) vt)))))))
        (finally (.stop srv 0))))))

(defn -main [& _]
  (let [result (run-tests 'test-hyperedges-batch)]
    (shutdown-agents)
    (System/exit (if (zero? (+ (:fail result) (:error result))) 0 1))))
