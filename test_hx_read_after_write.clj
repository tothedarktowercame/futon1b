(ns test-hx-read-after-write
  "Latch-controlled HTTP regression: a successful write must be immediately readable."
  (:require [clojure.edn :as edn]
            [clojure.test :refer [deftest is run-tests]]
            [futon1b-server :as server]
            [futon1b-gates :as gates]
            [futon1b-graph :as graph]
            [futon1b-text :as text]
            [futon1b-hxindex :as hx]
            [xtdb.node :as xtn])
  (:import [java.net URI]
           [java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers HttpResponse$BodyHandlers]
           [java.nio.file Files]))

(deftest pending-sidecar-write-cannot-return-success
  (gates/seed-mission-contract!)
  (with-open [node (xtn/start-node)]
    (let [dir (.toFile (Files/createTempDirectory "hx-read-after-write" (make-array java.nio.file.attribute.FileAttribute 0)))
          _ (text/init! {:path (str dir "/sidecar.db")})
          _ (hx/init!)
          _ (hx/catch-up! node)
          srv (server/start-server! {:node node :port 0 :bind-host "127.0.0.1"})
          base (str "http://127.0.0.1:" (.getPort (.getAddress srv)))
          client (HttpClient/newHttpClient)
          index-var (ns-resolve 'futon1b-hxindex 'index-doc!)
          index-doc @index-var]
      (try
        (doseq [mint? [true false false]]
          (let [entered (promise) release (promise)
                type (if mint? :test/minted-race :test/legacy-race)]
            (with-redefs-fn
              {index-var (fn [ds doc] (deliver entered true) @release (index-doc ds doc))}
              (fn []
                (let [write (future
                              (let [r (.send client
                                             (-> (HttpRequest/newBuilder (URI/create (str base "/api/alpha/hyperedge")))
                                                 (.header "Content-Type" "application/edn")
                                                 (.header "x-penholder" "api")
                                                 (.POST (HttpRequest$BodyPublishers/ofString
                                                         (pr-str (cond-> {:hx/type type :hx/endpoints ["race-end"]}
                                                                   mint? (assoc :hx/mint-id true))))) .build)
                                             (HttpResponse$BodyHandlers/ofString))]
                                [(.statusCode r) (edn/read-string (.body r))]))
                      query #(graph/hyperedges-query node {:type type :end "race-end" :limit 100})]
                  (try
                    (is (= true (deref entered 15000 :timeout)))
                    (let [early (deref write 500 :pending)]
                      ;; On the old code this is a 200 and the held hook leaves
                      ;; an empty indexed read: reproduce the actual omission.
                      (when (vector? early)
                        (is (some #{(get-in early [1 :hx/id])} (map :hx/id (:hyperedges (query))))
                            "200 must not precede read visibility"))
                      (is (= :pending early) "response waits for its own index hook"))
                    ;; Populate the cache during the pending write too.
                    (query)
                    (finally (deliver release true)))
                  (let [[status body] (deref write 15000 :timeout)]
                    (is (= 200 status))
                    (is (some #{(:hx/id body)} (map :hx/id (:hyperedges (query))))
                        "immediate read after response, including cache invalidation")))))))
        (finally (.stop srv 0))))))

(deftest invalidated-query-cannot-repopulate-cache
  (let [uncached (ns-resolve 'futon1b-graph 'hyperedges-query-uncached)
        entered (promise) release (promise) calls (atom 0)
        opts {:type :test/cache-race :limit 10 :include-total? false}]
    (graph/invalidate-hyperedge-query-cache!)
    (with-redefs-fn
      {uncached (fn [& _]
                  (if (= 1 (swap! calls inc))
                    (do (deliver entered true) @release {:hyperedges []})
                    {:hyperedges [{:hx/id "new"}]}))}
      (fn []
        (let [read (future (graph/hyperedges-query :test-node opts))]
          (try
            (is (= true (deref entered 1000 :timeout)))
            (graph/invalidate-hyperedge-query-cache! :test/cache-race)
            (finally (deliver release true)))
          (is (= {:hyperedges []} @read))
          (is (= {:hyperedges [{:hx/id "new"}]} (graph/hyperedges-query :test-node opts)))
          (is (= 2 @calls)))))))

(defn -main [& _]
  (let [r (run-tests 'test-hx-read-after-write)]
    (shutdown-agents)
    (System/exit (if (zero? (+ (:fail r) (:error r))) 0 1))))
