;; test_json — API-CONTRACT §0 wire-format cases: the futon3c watchers POST
;; application/json with string keys (keywordized server-side, exactly as
;; futon1a does) and parse JSON responses. Found live at Phase C Gate 2:
;; every watcher write 500ed against the EDN-only v1 server.
;;
;; Run: clojure -M:node -m test-json
(ns test-json
  (:require [cheshire.core :as json]
            [futon1b-gates :as gates]
            [futon1b-server :as srv]
            [xtdb.node :as xtn])
  (:import [java.net URI]
           [java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers
            HttpResponse$BodyHandlers]))

(def client (HttpClient/newHttpClient))

(defn- req-json [method url body headers]
  (let [b (-> (HttpRequest/newBuilder (URI/create url))
              (.method method (if body
                                (HttpRequest$BodyPublishers/ofString
                                 (json/generate-string body))
                                (HttpRequest$BodyPublishers/noBody))))
        b (reduce (fn [b [k v]] (.header b k v)) b
                  (merge {"Content-Type" "application/json"} (or headers {})))
        resp (.send client (.build b) (HttpResponse$BodyHandlers/ofString))]
    {:status (.statusCode resp)
     :ctype (.orElse (.firstValue (.headers resp) "Content-Type") "")
     :body (try (json/parse-string (.body resp) true)
                (catch Exception _ (.body resp)))}))

(def !fails (atom 0))
(defn check! [label ok? detail]
  (when-not ok? (swap! !fails inc))
  (println (format "  %-56s %s" label (if ok? "PASS" (str "FAIL " (pr-str detail))))))

(defn run-tests [base]
  ;; the exact shape commit_ingest.clj / file_ingest.clj send
  (let [r (req-json "POST" (str base "/api/alpha/hyperedge")
                    {"hx/type" "code/v05/commit"
                     "hx/endpoints" ["commit:futon3c-d:abc123"]
                     "hx/props" {"repo" "futon3c-d" "timestamp" 1720000000000}}
                    {"X-Penholder" "api"})]
    (check! "watcher-shaped JSON hyperedge POST -> 200 ok"
            (and (= 200 (:status r)) (true? (get-in r [:body :ok]))
                 (= "hx:code/v05/commit:commit:futon3c-d:abc123"
                    (get-in r [:body :hx/id])))
            r)
    (check! "JSON request -> JSON response (watchers parse it)"
            (clojure.string/includes? (:ctype r) "application/json") (:ctype r)))
  (let [r (req-json "POST" (str base "/api/alpha/hyperedge")
                    {"hx/type" "code/v05/commit"
                     "hx/endpoints" ["commit:futon3c-d:abc123"]
                     "hx/props" {"repo" "futon3c-d" "timestamp" 1720000000000}}
                    {"X-Penholder" "api"})]
    (check! "identical JSON re-post -> no-op guard fires"
            (true? (get-in r [:body :no-op?])) r))
  (let [r (req-json "POST" (str base "/api/alpha/evidence")
                    {"evidence/type" "claim" "evidence/claim-type" "observation"
                     "evidence/author" "json-test" "evidence/id" "j1"
                     "evidence/body" {"event" "probe"}}
                    {"X-Penholder" "api"})]
    (check! "JSON evidence POST -> 201"
            (and (= 201 (:status r)) (= "j1" (get-in r [:body :evidence/id]))) r))
  (let [r (req-json "GET" (str base "/api/alpha/hyperedges?type=code/v05/commit") nil nil)]
    (check! "GET with JSON Accept -> JSON body, count 1"
            (= 1 (get-in r [:body :count])) r))
  ;; EDN path must be unchanged
  (let [b (-> (HttpRequest/newBuilder (URI/create (str base "/api/alpha/evidence/j1")))
              (.GET) (.build))
        resp (.send client b (HttpResponse$BodyHandlers/ofString))]
    (check! "headerless GET still EDN"
            (and (= 200 (.statusCode resp))
                 (clojure.string/starts-with? (.body resp) "{:"))
            (.body resp))))

(defn -main [& _]
  (gates/seed-mission-contract!)
  (with-open [node (xtn/start-node)]
    (let [server (srv/start-server! {:node node :port 0})
          port (.getPort (.getAddress server))]
      (try (run-tests (str "http://127.0.0.1:" port))
           (finally (srv/stop-server! server)))))
  (println (format "%nfails: %d" @!fails))
  (System/exit (min 1 @!fails)))
