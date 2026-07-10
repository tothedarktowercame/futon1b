;; test_a1a2 — smoke test for the A1 (evidence routes) + A2 (gates) slice of
;; E-futon1b-operational-switchover, exercised over real HTTP against an
;; in-memory node (same style as migration/test_slice.clj: assertions +
;; PASS/FAIL summary, non-zero exit on failure).
;;
;; Run: clojure -M:node -m test-a1a2
(ns test-a1a2
  (:require [clojure.edn :as edn]
            [futon1b-gates :as gates]
            [futon1b-server :as srv]
            [xtdb.node :as xtn])
  (:import [java.net URI]
           [java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers
            HttpResponse$BodyHandlers]))

(def client (HttpClient/newHttpClient))

(defn- req
  ([method url] (req method url nil nil))
  ([method url body headers]
   (let [b (-> (HttpRequest/newBuilder (URI/create url))
               (.method method (if body
                                 (HttpRequest$BodyPublishers/ofString (pr-str body))
                                 (HttpRequest$BodyPublishers/noBody))))
         b (reduce (fn [b [k v]] (.header b k v)) b (or headers {}))
         resp (.send client (.build b) (HttpResponse$BodyHandlers/ofString))]
     {:status (.statusCode resp)
      :body (try (edn/read-string (.body resp)) (catch Exception _ (.body resp)))})))

(def !results (atom []))

(defn check! [label ok? detail]
  (swap! !results conj {:label label :ok? (boolean ok?) :detail detail})
  (println (format "  %-52s %s" label (if ok? "PASS" (str "FAIL " (pr-str detail))))))

(def ph {"x-penholder" "joe"})

(defn run-tests [base]
  (let [E (str base "/api/alpha/evidence")]

    (println "— A2 gates: penholder")
    (let [r (req "POST" E {:evidence/type :claim :evidence/claim-type :observation
                           :evidence/author "t"} nil)]
      (check! "no penholder -> 403 :missing-penholder"
              (and (= 403 (:status r))
                   (= :missing-penholder (get-in r [:body :error :reason])))
              r))
    (let [r (req "POST" E {:evidence/type :claim :evidence/claim-type :observation
                           :evidence/author "t"} {"x-penholder" "mallory"})]
      (check! "unknown penholder -> 403 :forbidden"
              (and (= 403 (:status r))
                   (= :forbidden (get-in r [:body :error :reason])))
              r))
    (let [r (req "POST" E {:evidence/type :claim :evidence/claim-type :observation
                           :evidence/author "t" :penholder "api"
                           :evidence/id "ph-body"
                           :evidence/at "2026-07-09T00:00:00Z"} nil)]
      (check! "body :penholder accepted" (= 201 (:status r)) r))

    (println "— A1 evidence: write path")
    (let [r (req "POST" E {:evidence/claim-type :observation :evidence/author "t"} ph)]
      (check! "missing type -> plain 400"
              (and (= 400 (:status r)) (= "evidence/type required" (get-in r [:body :error])))
              r))
    (let [r (req "POST" E {:evidence/id "e1" :evidence/type :claim
                           :evidence/claim-type :observation :evidence/author "alice"
                           :evidence/at "2026-07-10T10:00:00Z"
                           :evidence/session-id "s1"
                           :evidence/tags [:alpha :beta]
                           :evidence/body {"json-key" 1}} ph)]
      (check! "write -> 201 {:ok true :evidence/id :entry}"
              (and (= 201 (:status r)) (:ok (:body r))
                   (= "e1" (get-in r [:body :evidence/id]))
                   (map? (get-in r [:body :entry])))
              r))
    (let [r (req "POST" E {:evidence/id "e1" :evidence/type :claim
                           :evidence/claim-type :observation :evidence/author "alice"} ph)]
      (check! "duplicate id -> 409"
              (and (= 409 (:status r)) (= "duplicate evidence id" (get-in r [:body :error])))
              r))
    ;; corpus for query tests
    (doseq [[id typ author at sid tags eph reply]
            [["e2" :note "bob" "2026-07-10T11:00:00Z" "s1" ["alpha"] nil "e1"]
             ["e3" :claim "alice" "2026-07-10T12:00:00Z" "s2" [] nil "e2"]
             ["e4" :note "carol" "2026-07-10T13:00:00Z" nil ["beta"] true nil]]]
      (req "POST" E (cond-> {:evidence/id id :evidence/type typ
                             :evidence/claim-type :observation
                             :evidence/author author :evidence/at at
                             :evidence/tags tags}
                      sid (assoc :evidence/session-id sid)
                      eph (assoc :evidence/ephemeral? true)
                      reply (assoc :evidence/in-reply-to reply))
           ph))

    (println "— A1 evidence: reads")
    (let [r (req "GET" (str E "/e1"))]
      (check! "get by id -> 200 doc minus :xt/id"
              (and (= 200 (:status r)) (= "e1" (get-in r [:body :evidence/id]))
                   (not (contains? (:body r) :xt/id)))
              r))
    (let [r (req "GET" (str E "/nope"))]
      (check! "unknown id -> 404" (= 404 (:status r)) r))
    (let [r (req "GET" (str E "?type=claim"))]
      (check! "?type=claim -> 3 (e1 e3 ph-body)"
              (and (= 200 (:status r)) (= 3 (:count (:body r))))
              r))
    (let [r (req "GET" (str E "?author=alice&type=claim"))]
      (check! "?author&type compound" (= 2 (:count (:body r))) r))
    (let [r (req "GET" (str E "?since=2026-07-10T12:00:00Z"))]
      (check! "?since inclusive -> e3 e4" (= 2 (:count (:body r))) r))
    (let [r (req "GET" (str E "?before=2026-07-10T11:00:00Z"))]
      (check! "?before exclusive -> e1 + ph-body" (= 2 (:count (:body r))) r))
    (let [r (req "GET" (str E "?tags=alpha,beta"))]
      (check! "?tags AND -> e1 only" (= 1 (:count (:body r))) r))
    (let [r (req "GET" (str E "?include-ephemeral=false"))]
      (check! "include-ephemeral=false excludes e4"
              (not-any? #(= "e4" (:evidence/id %)) (get-in r [:body :entries]))
              r))
    (let [r (req "GET" E)]
      (check! "param-absent includes ephemeral (futon1a compat)"
              (some #(= "e4" (:evidence/id %)) (get-in r [:body :entries]))
              r))
    (let [r (req "GET" (str E "?limit=2"))
          ids (mapv :evidence/id (get-in r [:body :entries]))]
      (check! "limit + newest-first" (= ["e4" "e3"] ids) r))
    (let [r (req "GET" (str E "/count?type=note"))]
      (check! "/count -> {:count 2}" (= 2 (get-in r [:body :count])) r))

    (println "— A1 evidence: sessions + chain")
    (let [r (req "GET" (str E "/sessions"))
          b (:body r)]
      (check! "sessions envelope + newest-first"
              (and (= 2 (:total-sessions b)) (= 3 (:total-entries b))
                   (= "s2" (:session-id (first (:sessions b))))
                   (= [":claim"] (:types (first (:sessions b)))))
              b))
    (let [r (req "GET" (str E "/sessions?author=alice"))]
      (check! "sessions author filter" (= 2 (:total-sessions (:body r))) r))
    (let [r (req "GET" (str E "/e3/chain"))
          ids (mapv :evidence/id (get-in r [:body :chain]))]
      (check! "chain oldest-first" (= ["e1" "e2" "e3"] ids) r))

    (println "— A2 gates: hyperedge penholder + no-op guard intact")
    (let [r (req "POST" (str base "/api/alpha/hyperedge")
                 {:hx/type :test/edge :hx/endpoints ["a" "b"]} nil)]
      (check! "hyperedge no penholder -> 403" (= 403 (:status r)) r))
    (let [r1 (req "POST" (str base "/api/alpha/hyperedge")
                  {:hx/type :test/edge :hx/endpoints ["a" "b"]} ph)
          r2 (req "POST" (str base "/api/alpha/hyperedge")
                  {:hx/type :test/edge :hx/endpoints ["a" "b"]} ph)]
      (check! "hyperedge write then no-op"
              (and (= 200 (:status r1)) (:ok (:body r1))
                   (true? (get-in r2 [:body :no-op?])))
              [r1 r2]))

    (println "— A2 gates: canonical-id (in-process; entity route lands in A3)")
    (check! "mission id canonical -> accept"
            (nil? (gates/gate-entity-id!
                   {:entity/type :mission/doc
                    :entity/name "futon3c-d/mission/first-flights"}))
            nil)
    (check! "bare M-* -> queued, write proceeds"
            (:queued? (gates/gate-entity-id!
                       {:entity/type :mission/doc :entity/name "M-first-flights"}))
            nil)
    (let [r (try (gates/gate-entity-id!
                  {:entity/type :mission/doc
                   :entity/name "futon3c-desktop-save-d/mission/x"})
                 (catch Exception e (:error (ex-data e))))]
      (check! "drift repo -> L4 :non-canonical-id"
              (and (= 4 (:layer r)) (= :non-canonical-id (:reason r)))
              r))
    (check! "unregistered type -> gate silent"
            (nil? (gates/gate-entity-id! {:entity/type :whatever/doc :entity/name "x"}))
            nil)))

(defn -main [& _]
  (gates/seed-mission-contract!)
  (with-open [node (xtn/start-node)]
    (let [server (srv/start-server! {:node node :port 0})
          port (.getPort (.getAddress server))
          base (str "http://127.0.0.1:" port)]
      (try
        (run-tests base)
        (finally (.stop server 0)))))
  (let [results @!results
        fails (remove :ok? results)]
    (println (format "%n%d/%d PASS" (- (count results) (count fails)) (count results)))
    (System/exit (if (seq fails) 1 0))))
