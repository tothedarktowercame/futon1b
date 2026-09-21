(ns test-evidence-ordering
  (:require [clojure.edn :as edn]
            [clojure.test :refer [deftest is run-tests]]
            [futon1b-evidence :as evidence]
            [futon1b-gates :as gates]
            [futon1b-server :as server]
            [xtdb.api :as xt]
            [xtdb.node :as xtn])
  (:import [java.net URI URLEncoder]
           [java.net.http HttpClient HttpRequest HttpResponse$BodyHandlers]))

(defn- row-key [row] [(:evidence/at row) (:evidence/id row)])
(defn- violations [rows]
  (count (filter (fn [[a b]] (not (pos? (compare (row-key a) (row-key b)))))
                 (partition 2 1 rows))))

(defn- fixture-doc [i]
  ;; Interleaved days, repeated timestamps and unrelated hash-ordered IDs.
  {:xt/id (format "order-%05d" i)
   :evidence/id (format "order-%05d" i)
   :evidence/at (format "2026-09-%02dT12:00:00.000Z" (+ 1 (mod i 20)))
   :evidence/type :claim :evidence/claim-type :observation
   :evidence/author "ordering-regression"
   :evidence/tags [(if (even? i) "context-retrieval" "other")]
   :evidence/body {:ordinal i}})

(defn- fetch-page [client base cursor]
  (let [url (str base "/api/alpha/evidence?tags=context-retrieval"
                 "&since=2026-08-22T00:00:00Z&before=2026-09-21T17:19:12Z&limit=1000"
                 (when cursor
                   (str "&cursor-at=" (URLEncoder/encode (:at cursor) "UTF-8")
                        "&cursor-id=" (URLEncoder/encode (:id cursor) "UTF-8"))))
        response (.send ^HttpClient client
                        (-> (HttpRequest/newBuilder (URI/create url))
                            (.header "Accept" "application/edn") .GET .build)
                        (HttpResponse$BodyHandlers/ofString))]
    (is (= 200 (.statusCode response)))
    (edn/read-string (.body response))))

(deftest ^:slow real-store-global-newest-first
  (gates/seed-mission-contract!)
  (with-open [node (xtn/start-node)]
    (let [docs (mapv fixture-doc (range 110000))
          expected (vec (sort-by row-key #(compare %2 %1)
                                 (filter #(= ["context-retrieval"] (:evidence/tags %)) docs)))
          _ (doseq [batch (partition-all 1000 docs)]
              (xt/execute-tx node [(into [:put-docs :evidence] batch)]))
          srv (server/start-server! {:node node :port 0 :bind-host "127.0.0.1"})
          base (str "http://127.0.0.1:" (.getPort (.getAddress srv)))
          client (HttpClient/newHttpClient)]
      (try
        ;; Diagnose the layer independently of the HTTP hydration path.
        (let [project (#'evidence/fetch-newest-projected-page
                       node {:since "2026-08-22T00:00:00Z" :before "2026-09-21T17:19:12Z"}
                       nil 1000 '[xt/id evidence/id evidence/at])
              hydrated (#'evidence/hydrate-projected node project)]
          (println "projection violations:" (violations project)
                   "hydration preserves IDs:" (= (mapv :xt/id project) (mapv :xt/id hydrated)))
          (is (zero? (violations project)))
          (is (= (mapv :xt/id project) (mapv :xt/id hydrated))))
        (let [pages (loop [cursor nil result []]
                      (let [page (fetch-page client base cursor)
                            result (conj result page)]
                        (if (and (:next-cursor page) (< (count result) 120))
                          (recur (:next-cursor page) result)
                          result)))
              rows (vec (mapcat :entries pages))
              ids (mapv :evidence/id rows)]
          (println "page counts:" (mapv :count pages) "returned:" (count rows)
                   "expected:" (count expected) "violations:" (violations rows)
                   "unique:" (count (set ids)))
          (is (= 1000 (:count (first pages))))
          (is (nil? (:next-cursor (last pages))))
          (is (zero? (violations rows)))
          (is (= (count ids) (count (set ids))))
          (is (true? (= (set (map :evidence/id expected)) (set ids))) "complete bounded identity set")
          (is (true? (= (mapv :evidence/id expected) ids)) "global order before pagination"))
        (finally (server/stop-server! srv))))))

(defn -main [& _]
  (let [result (run-tests 'test-evidence-ordering)]
    (shutdown-agents)
    (System/exit (if (zero? (+ (:fail result) (:error result))) 0 1))))
