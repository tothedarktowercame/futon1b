(ns test-scope-route
  "P6b: the /api/alpha/scopes route (futon1b-server/scopes-route) against a
   throwaway sidecar and a small fixture run (fixture approach copied from
   test-scope-index). Covers Q6/Q7/Q8 through the route, the 400s (no
   params, both params, malformed lines), the typed 503 refusal when the
   sidecar is unattached, and the empty FUTON1B_SCOPE_RUNS case.

   Bad case: a route registered by VALUE keeps calling the old closure
   after the route fn is redefined; registered through its VAR it picks
   up the redefinition (TN-entities-speedups-2026-09-26.md). Both shown.

  Run: clojure -M:node -m test-scope-route
  (FUTON1B_SCOPE_RUNS must be unset — the empty-env case asserts nothing
   is indexed.)"
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            futon1b-server
            [futon1b-text :as text]
            [futon1b-scopeindex :as scope])
  (:import [com.sun.net.httpserver Headers HttpExchange HttpHandler]
           [java.io ByteArrayInputStream ByteArrayOutputStream]
           [java.net URI]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- temp-dir []
  (-> (Files/createTempDirectory
       "futon1b-scoperoute-test-"
       (make-array FileAttribute 0))
      .toFile
      .getAbsolutePath))

(defn- check! [label value]
  (println (format "  %-72s %s" label (if value "PASS" "FAIL")))
  (assert value label))

;; --- Fixture (same shape as test-scope-index, trimmed to what the route
;; --- needs: Q6 ordering, a Q7 closed-interval boundary, Q8 across papers).

(def fixture-files
  {"p1_0001.edn"
   "{:paper/id \"p1\" :passage/id \"p1:pass-0001:L10-20\"
     :source {:lines [10 20] :kind :expository}
     :provenance {:generator \"expository-json/v1\" :model \"fixture\"}
     :scopes [{:id :s1 :kind :connection :source {:lines [10 20]}}
              {:id :s2 :kind :universal-property/characterizes
               :source {:lines [12 14]}}]}"
   "p2_0001.edn"
   "{:paper/id \"p2\" :passage/id \"p2:pass-0001:L1-5\"
     :source {:lines [1 5] :kind :expository}
     :provenance {:generator \"expository-json/v2\" :model \"fixture\"}
     :scopes [{:id :s1 :kind :connection :source {:lines [1 3]}}]}"})

(defn- write-fixture! [run-dir]
  (let [expo (io/file run-dir "artifacts/expo")]
    (.mkdirs expo)
    (doseq [[name body] fixture-files]
      (spit (io/file expo name) body))))

;; --- Fake HttpExchange: enough of the abstract surface for
;; --- query-params / respond! / handler.

(defn- fake-exchange [method uri]
  (let [headers (Headers.)
        body (ByteArrayOutputStream.)
        status (atom nil)]
    {:ex (proxy [HttpExchange] []
           (getRequestMethod [] method)
           (getRequestURI [] (URI/create uri))
           (getRequestHeaders [] (Headers.))
           (getResponseHeaders [] headers)
           (getRequestBody [] (ByteArrayInputStream. (byte-array 0)))
           (sendResponseHeaders [s _] (reset! status s))
           (getResponseBody [] body)
           (getRemoteAddress [] nil)
           (getLocalAddress [] nil)
           (getProtocol [] "HTTP/1.1")
           (getAttribute [_] nil)
           (setAttribute [_ _] nil)
           (setStreams [_ _] nil)
           (getPrincipal [] nil)
           (getHttpContext [] nil)
           (close [] nil))
     :status status
     :body (fn [] (edn/read-string (.toString body "UTF-8")))}))

(defn- call-handler [^HttpHandler h method uri]
  (let [{:keys [ex status body]} (fake-exchange method uri)]
    (.handle h ex)
    {:status @status :body (body)}))

(defn -main [& _]
  (when (System/getenv "FUTON1B_SCOPE_RUNS")
    (throw (ex-info "test-scope-route needs FUTON1B_SCOPE_RUNS unset" {})))
  (let [dir (temp-dir)
        run-dir (io/file dir "route-test-run")
        handler-fn @(ns-resolve 'futon1b-server 'handler)
        scopes-var (ns-resolve 'futon1b-server 'scopes-route)
        env-index-fn @(ns-resolve 'futon1b-server 'scope-index-runs-from-env!)
        ds-atom @(ns-resolve 'futon1b-text '!ds)]
    (write-fixture! run-dir)
    (text/init! {:path (str dir "/throwaway.db")})
    (scope/init!)

    (println "== P6b scopes route tests ==")

    (let [r (scope/index-run! @ds-atom (.getPath run-dir))]
      (check! "fixture run indexed" (= 3 (:rows r))))

    ;; Registered the way start-server! does it: through the var.
    (let [h (handler-fn scopes-var)]

      ;; --- Q6 through the route ------------------------------------------
      (let [{:keys [status body]} (call-handler h "GET" "/api/alpha/scopes?paper=p1")]
        (check! "Q6 status 200" (= 200 status))
        (check! "Q6 count" (= 2 (:count body)))
        (check! "Q6 rows ordered by line"
                (= [[10 20] [12 14]]
                   (mapv (juxt :l0 :l1) (:scopes body)))))

      ;; --- Q7 through the route (closed interval: 14 touches [12 14]) -----
      (let [{:keys [status body]}
            (call-handler h "GET" "/api/alpha/scopes?paper=p1&lines=14-99")]
        (check! "Q7 status 200" (= 200 status))
        (check! "Q7 closed-interval touch included"
                (some #{[12 14]} (mapv (juxt :l0 :l1) (:scopes body)))))
      (let [{:keys [body]}
            (call-handler h "GET" "/api/alpha/scopes?paper=p1&lines=15-99")]
        (check! "Q7 clearing interval excluded"
                (not-any? #{[12 14]} (mapv (juxt :l0 :l1) (:scopes body)))))

      ;; --- Q8 through the route, with and without ?run= --------------------
      (let [{:keys [status body]} (call-handler h "GET" "/api/alpha/scopes?kind=connection")]
        (check! "Q8 status 200" (= 200 status))
        (check! "Q8 across papers"
                (= #{"p1" "p2"} (set (map :paper (:scopes body))))))
      (let [{:keys [body]}
            (call-handler h "GET" "/api/alpha/scopes?kind=connection&run=route-test-run")]
        (check! "Q8 with run" (= 2 (:count body))))
      (let [{:keys [body]}
            (call-handler h "GET" "/api/alpha/scopes?kind=connection&run=no-such-run")]
        (check! "Q8 unknown run is empty, not an error" (= 0 (:count body))))

      ;; --- 400s -------------------------------------------------------------
      (doseq [[label uri]
              [["no params -> 400" "/api/alpha/scopes"]
               ["both params -> 400" "/api/alpha/scopes?paper=p1&kind=connection"]
               ["malformed lines -> 400" "/api/alpha/scopes?paper=p1&lines=foo"]
               ["reversed lines -> 400" "/api/alpha/scopes?paper=p1&lines=99-14"]
               ["lines with kind -> 400" "/api/alpha/scopes?kind=connection&lines=1-2"]]
              :let [{:keys [status]} (call-handler h "GET" uri)]]
        (check! label (= 400 status)))

      ;; --- typed refusal when the sidecar is unattached ---------------------
      (let [saved @ds-atom]
        (reset! ds-atom nil)
        (try
          (let [{:keys [status body]} (call-handler h "GET" "/api/alpha/scopes?paper=p1")]
            (check! "no sidecar -> 503 typed refusal" (= 503 status))
            (check! "refusal is typed" (= :scope-sidecar-unavailable
                                          (:error body))))
          (finally (reset! ds-atom saved))))

      ;; --- POST is not this route -------------------------------------------
      (let [{:keys [status]} (call-handler h "POST" "/api/alpha/scopes?paper=p1")]
        (check! "POST -> 405" (= 405 status))))

    ;; --- empty FUTON1B_SCOPE_RUNS indexes nothing ---------------------------
    (let [last-run-before (:last-index-run @scope/!stats)
          n (env-index-fn)]
      (check! "empty env indexes zero runs" (= 0 n))
      (check! "empty env ran no index (stats untouched)"
              (= last-run-before (:last-index-run @scope/!stats))))

    ;; --- bad case: route captured by VALUE vs through the VAR ---------------
    ;; Redefine the route fn after registration: the var-registered handler
    ;; must serve the NEW body, the by-value handler the OLD one
    ;; (TN-entities-speedups-2026-09-26.md — routes captured by value need
    ;; a restart).
    (let [respond-str @(ns-resolve 'futon1b-server 'respond!)
          old-fn (fn [ex] (respond-str ex 200 (pr-str {:version 1})))
          new-fn (fn [ex] (respond-str ex 200 (pr-str {:version 2})))]
      (intern 'test-scope-route 'redefinable-route old-fn)
      (let [route-var (ns-resolve 'test-scope-route 'redefinable-route)
            by-var (handler-fn route-var)
            by-val (handler-fn @route-var)
            _ (intern 'test-scope-route 'redefinable-route new-fn)
            v (call-handler by-var "GET" "/anything")
            o (call-handler by-val "GET" "/anything")]
        (check! "bad case: var-registered handler picks up the redefinition"
                (= 2 (:version (:body v))))
        (check! "bad case: by-value handler still calls the OLD closure"
                (= 1 (:version (:body o))))))

    (println "ALL PASS")
    (System/exit 0)))
