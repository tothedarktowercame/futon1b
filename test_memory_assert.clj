(ns test-memory-assert
  (:require [clojure.edn :as edn]
            [futon1b-gates :as gates]
            [futon1b-server :as srv]
            [xtdb.node :as xtn])
  (:import [java.net URI URLEncoder]
           [java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers
            HttpResponse$BodyHandlers]))

(def client (HttpClient/newHttpClient))
(def !results (atom []))
(def penholder {"x-penholder" "joe"})

(defn- req [method url body headers]
  (let [builder (-> (HttpRequest/newBuilder (URI/create url))
                    (.method method
                             (if body
                               (HttpRequest$BodyPublishers/ofString
                                (pr-str body))
                               (HttpRequest$BodyPublishers/noBody))))
        builder (reduce (fn [b [k v]] (.header b k v))
                        builder (or headers {}))
        response (.send client (.build builder)
                        (HttpResponse$BodyHandlers/ofString))]
    {:status (.statusCode response)
     :body (edn/read-string (.body response))}))

(defn- encoded [s] (URLEncoder/encode s "UTF-8"))

(defn- check! [label ok? detail]
  (swap! !results conj {:label label :ok? (boolean ok?) :detail detail})
  (println (format "  %-62s %s" label
                   (if ok? "PASS" (str "FAIL " (pr-str detail))))))

(defn- evidence [id]
  {:evidence/id id
   :evidence/type :memory
   :evidence/claim-type :assertion
   :evidence/author "test-memory-assert"
   :evidence/body {:claim "paired write"}})

(defn- hyperedge [id evidence-id]
  {:hx/id id :hx/type :memory/assert
   :hx/endpoints [evidence-id "subject-1"]})

(defn- pair [evidence-id hyperedge-id]
  {:evidence (evidence evidence-id)
   :hyperedge (hyperedge hyperedge-id evidence-id)})

(defn run-tests [base]
  (let [route (str base "/api/alpha/memory/assert")
        evidence-url #(str base "/api/alpha/evidence/" (encoded %))
        hyperedge-url #(str base "/api/alpha/hyperedge/" (encoded %))]
    (println "— atomic memory assertion")
    (let [payload (pair "memory-assert-e1" "memory-assert-h1")
          response (req "POST" route payload penholder)]
      (check! "happy path returns 201 with paired ids"
              (and (= 201 (:status response))
                   (= "memory-assert-e1" (get-in response [:body :evidence/id]))
                   (= "memory-assert-h1" (get-in response [:body :hx/id])))
              response)
      (check! "happy path evidence is readable"
              (= 200 (:status (req "GET" (evidence-url "memory-assert-e1") nil nil)))
              response)
      (check! "happy path hyperedge is readable"
              (= 200 (:status (req "GET" (hyperedge-url "memory-assert-h1") nil nil)))
              response)
      (let [again (req "POST" route payload penholder)]
        (check! "identical pair re-post retains evidence duplicate 409"
                (= 409 (:status again)) again)))

    (let [payload {:evidence (evidence "invalid-hx-evidence")
                   :hyperedge {:hx/id "invalid-hx"
                               :hx/type :memory/assert
                               :hx/endpoints [{:role :subject}]}}
          response (req "POST" route payload penholder)]
      (check! "invalid hyperedge returns 400"
              (= 400 (:status response)) response)
      (check! "invalid hyperedge writes no evidence"
              (= 404 (:status (req "GET" (evidence-url "invalid-hx-evidence")
                                   nil nil)))
              response))

    (let [payload {:evidence (evidence "unattached-evidence")
                   :hyperedge (hyperedge "unattached-hx" "different-evidence")}
          response (req "POST" route payload penholder)]
      (check! "unattached pair returns 400"
              (and (= 400 (:status response))
                   (= :memory-assert-evidence-endpoint-missing
                      (get-in response [:body :error :reason])))
              response)
      (check! "unattached pair writes neither document"
              (and (= 404 (:status (req "GET" (evidence-url "unattached-evidence")
                                        nil nil)))
                   (= 404 (:status (req "GET" (hyperedge-url "unattached-hx")
                                        nil nil))))
              response))

    (let [response
          (req "POST" route
               {:evidence (evidence "memory-assert-e1")
                :hyperedge (hyperedge "duplicate-evidence-new-hx"
                                      "memory-assert-e1")}
               penholder)]
      (check! "duplicate evidence id returns 409"
              (= 409 (:status response)) response)
      (check! "duplicate evidence refusal writes no hyperedge"
              (= 404 (:status
                      (req "GET" (hyperedge-url "duplicate-evidence-new-hx")
                           nil nil)))
              response))))

(defn -main [& _]
  (gates/seed-mission-contract!)
  (with-open [node (xtn/start-node)]
    (let [server (srv/start-server! {:node node :port 0})
          base (str "http://127.0.0.1:" (.getPort (.getAddress server)))]
      (try
        (run-tests base)
        (finally (srv/stop-server! server)))))
  (let [results @!results
        failures (remove :ok? results)]
    (println (format "%n%d/%d PASS"
                     (- (count results) (count failures)) (count results)))
    (System/exit (if (seq failures) 1 0))))
