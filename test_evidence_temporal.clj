(ns test-evidence-temporal
  "P6 temporal evidence reads against a private XTDB node and real HTTP.
  Run: clojure -M:node -m test-evidence-temporal"
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing run-tests]]
            [futon1b-evidence :as evidence]
            [futon1b-gates :as gates]
            [futon1b-server :as server]
            [futon1b-text :as text]
            [xtdb.api :as xt]
            [xtdb.node :as xtn])
  (:import [java.net URI URLEncoder]
           [java.net.http HttpClient HttpRequest HttpResponse$BodyHandlers]
           [java.nio.file Files]
           [java.time Instant]))

(defn- get-page [client base route params]
  (let [query (str/join "&" (for [[k v] params]
                              (str k "=" (URLEncoder/encode (str v) "UTF-8"))))
        response (.send ^HttpClient client
                        (-> (HttpRequest/newBuilder (URI/create (str base route "?" query)))
                            (.header "Accept" "application/edn") .GET .build)
                        (HttpResponse$BodyHandlers/ofString))]
    [(.statusCode response) (edn/read-string (.body response))]))

(defn- doc [id author tag]
  {:xt/id id :evidence/id id :evidence/author author
   :evidence/type :note :evidence/claim-type :observation
   :evidence/at "2000-01-01T00:00:00Z" :evidence/tags [tag]
   :evidence/body {:version "original"}})

(deftest ^:slow temporal-evidence-over-http
  (gates/seed-mission-contract!)
  (with-open [node (xtn/start-node)]
    (let [srv (server/start-server! {:node node :port 0 :bind-host "127.0.0.1"})
          base (str "http://127.0.0.1:" (.getPort (.getAddress srv)))
          client (HttpClient/newHttpClient)
          get! #(get-page client base "/api/alpha/evidence" %)
          query {"author" "p6-writer"}
          original (doc "p6-written" "p6-writer" "original-tag")
          before (str (.minusSeconds (Instant/now) 1))]
      (try
        (testing "the production writer, with backdated event time"
          (is (= 201 (first (evidence/write-evidence! node (dissoc original :xt/id)))))
          (let [after (str (Instant/now))
                current (get! query)]
            (is (= 200 (first current)))
            (is (= ["p6-written"] (mapv :evidence/id (:entries (second current)))))
            (doseq [axis ["system-as-of" "valid-as-of"]]
              (let [[status page] (get! (assoc query axis before))]
                (is (= 200 status))
                (is (empty? (:entries page)) (str axis " before insertion excludes")))
              (is (= current (get! (assoc query axis after)))
                  (str axis " after insertion equals unchanged current result")))
            (is (= current (get! (assoc query "system-as-of" after "valid-as-of" after))))
            ;; A client-side :evidence/at filter would get this case wrong:
            ;; event time is 2000, but the store only learned of the event now.
            (is (= 1 (get-in (get! (assoc query "before" "2020-01-01T00:00:00Z")) [1 :count])))
            (is (= 0 (get-in (get! (assoc query "system-as-of" "2020-01-01T00:00:00Z")) [1 :count])))
            (println "P6 writer: pre-insertion excludes; post includes; absent unchanged; event-time differs: PASS")

            (testing "historical hydration and tag membership use the same basis"
              ;; Raw XTDB correction is a test fixture, not an added public write
              ;; capability. It exposes a hydration bug hidden by append-only ids.
              (xt/execute-tx node [[:put-docs :evidence
                                    (-> original
                                        (assoc :evidence/tags ["replacement-tag"])
                                        (assoc-in [:evidence/body :version] "replacement"))]])
              (let [dir (Files/createTempDirectory "p6-tags-" (make-array java.nio.file.attribute.FileAttribute 0))]
                (text/init! {:path (str dir "/fts5.db")})
                (text/catch-up! node))
              (is (some? (text/checkpoint-at)) "real current-only sidecar is populated")
              (is (empty? (text/tag-candidates {:tags ["original-tag"] :limit 10})))
              (let [params (assoc query "system-as-of" after "tags" "original-tag" "limit" "1")
                    [status page] (get! params)
                    cursor (:next-cursor page)]
                (is (= 200 status))
                (is (= [(dissoc original :xt/id)] (:entries page)) "historical body, not replacement")
                (is (some? cursor))
                (is (= 0 (get-in (get! (assoc params "cursor-at" (:at cursor)
                                                            "cursor-id" (:id cursor))) [1 :count])))
                (is (= [200 {:count 1}]
                       (get-page client base "/api/alpha/evidence/count" params))))
              (is (= "replacement" (get-in (get! query) [1 :entries 0 :evidence/body :version])))
              (println "P6 historical hydration, current-only tag index, pagination and count: PASS"))))

        (testing "two genuinely different XTDB axes on backfilled valid-time data"
          ;; Today's HTTP writer cannot set valid-from. Direct XTDB fixtures can
          ;; and prove the reader does not alias one axis to the other.
          (xt/execute-tx node [[:put-docs {:into :evidence
                                          :valid-from (Instant/parse "2000-01-01T00:00:00Z")}
                                (doc "p6-backfill" "p6-backfill" "backfill")]])
          (let [q {"author" "p6-backfill"}
                t "2020-01-01T00:00:00Z"]
            (is (= 1 (get-in (get! (assoc q "valid-as-of" t)) [1 :count])))
            (is (= 0 (get-in (get! (assoc q "system-as-of" t)) [1 :count])))
            (is (= 0 (get-in (get! (assoc q "valid-as-of" t "system-as-of" t)) [1 :count])))
            (println "P6 two-axis bad case: valid-as-of count 1; system-as-of count 0: PASS")))

        (testing "invalid instants are explicit 400s for both list and count"
          (doseq [axis ["system-as-of" "valid-as-of"]
                  value ["junk" "" "2026-09-24"]
                  route ["/api/alpha/evidence" "/api/alpha/evidence/count"]]
            (let [[status response] (get-page client base route {axis value})]
              (is (= 400 status))
              (is (= :invalid-temporal-instant (get-in response [:error :reason])))
              (is (str/includes? (get-in response [:error :context :message] "") axis))))
          (is (= 400 (first (evidence/query-evidence-response node {"system-as-of" "junk"}))))
          (println "P6 malformed system-as-of and valid-as-of: HTTP 400: PASS"))

        (testing "instants are parameters, not a new compiled form per request"
          (let [q1 (#'evidence/page-query {:system-as-of (Instant/parse "2020-01-01T00:00:00Z")}
                                          nil '[xt/id evidence/at])
                q2 (#'evidence/page-query {:system-as-of (Instant/parse "2021-01-01T00:00:00Z")}
                                          nil '[xt/id evidence/at])]
            (is (= (first q1) (first q2)))
            (is (not= (rest q1) (rest q2)))))
        (finally
          (reset! text/!ds nil)
          (server/stop-server! srv))))))

(defn -main [& _]
  (let [result (run-tests 'test-evidence-temporal)]
    (shutdown-agents)
    (System/exit (if (zero? (+ (:fail result) (:error result))) 0 1))))
