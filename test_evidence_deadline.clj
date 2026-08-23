;; test-evidence-deadline — regression for E-futon1b-gc-wedge (2026-08-23):
;; parameterised evidence page query (stable shape, cursor/limit as params),
;; JDBC deadline via futon1b-xt/timed-q, whole-request scan bound,
;; hyperedge window cap, and the /health holder/GC surface.
;;
;; Run: clojure -M:node -m test-evidence-deadline
(ns test-evidence-deadline
  (:require [clojure.edn :as edn]
            [futon1b-gates :as gates]
            [futon1b-graph :as graph]
            [futon1b-server :as srv]
            [futon1b-xt :as fxt]
            [xtdb.node :as xtn])
  (:import [java.net URI]
           [java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers
            HttpResponse$BodyHandlers]))

(def client (HttpClient/newHttpClient))
(def !fails (atom 0))

(defn- check! [label value]
  (println (format "  %-68s %s" label (if value "PASS" "FAIL")))
  (when-not value (swap! !fails inc))
  value)

(defn- req [method url body]
  (let [b (-> (HttpRequest/newBuilder (URI/create url))
              (.method method (if body
                                (HttpRequest$BodyPublishers/ofString (pr-str body))
                                (HttpRequest$BodyPublishers/noBody)))
              (.header "Content-Type" "application/edn")
              (.header "x-penholder" "api"))
        resp (.send client (.build b) (HttpResponse$BodyHandlers/ofString))
        raw (.body resp)]
    {:status (.statusCode resp)
     :body (try (edn/read-string raw) (catch Exception _ raw))}))

(defn- seed! [base n]
  (dotimes [i n]
    (let [r (req "POST" (str base "/api/alpha/evidence")
                 {:evidence/id (format "d%03d" i)
                  :evidence/type (if (even? i) :claim :note)
                  :evidence/claim-type :observation
                  :evidence/author (if (zero? (mod i 3)) "alice" "bob")
                  :evidence/at (format "2026-08-23T10:%02d:%02d.000Z" (quot i 60) (mod i 60))
                  :evidence/tags (cond-> ["t"] (zero? (mod i 10)) (conj "rare"))
                  :evidence/subject {:ref/type :pattern :ref/id (str "pat-" (mod i 4))}
                  :evidence/body {:i i}})]
      (when-not (= 201 (:status r))
        (throw (ex-info "seed failed" r))))))

(defn- run-tests [base node]
  (seed! base 120)

  ;; --- parameterised page shape ---------------------------------------
  (let [pq (ns-resolve 'futon1b-evidence 'page-query)
        q1 (pq {:type :claim :author "alice"} nil 50 '[xt/id evidence/at])
        q2 (pq {:type :note :author "bob"} ["2026-08-23T10:00:59.000Z" "d059"] 7 '[xt/id evidence/at])
        q3 (pq {:type :note :author "bob"} ["2026-08-23T10:00:10.000Z" "d010"] 7 '[xt/id evidence/at])]
    (check! "page form is a (fn [...] ...) with filter values as args"
            (and (= 'fn (first (first q1))) (= [:claim "alice" 50] (rest q1))))
    (check! "same filter set + cursor presence => identical form text"
            (= (pr-str (first q2)) (pr-str (first q3))))
    (check! "no cursor and cursor are the only two variants (differ)"
            (not= (pr-str (first q1)) (pr-str (first q2)))))

  ;; --- pages + cursor + post-filters ----------------------------------
  (let [r (req "GET" (str base "/api/alpha/evidence?limit=10") nil)
        b (:body r)]
    (check! "first page 200 with 10 newest" (and (= 200 (:status r)) (= 10 (:count b))))
    (check! "first page newest first" (= "d119" (:evidence/id (first (:entries b)))))
    (check! "first page reports :scanned" (= 10 (:scanned b)))
    (let [{:keys [at id]} (:next-cursor b)
          r2 (req "GET" (str base "/api/alpha/evidence?limit=10&cursor-at=" at "&cursor-id=" id) nil)]
      (check! "cursor page continues at d109"
              (= "d109" (:evidence/id (first (:entries (:body r2))))))))
  (let [r (req "GET" (str base "/api/alpha/evidence?type=claim&author=alice&limit=1000") nil)]
    (check! "pushdown type+author: 20 claims by alice"
            (= 20 (:count (:body r)))))
  (let [r (req "GET" (str base "/api/alpha/evidence?tags=rare&limit=5") nil)]
    (check! "tag post-filter page: 5 rare, newest d110"
            (and (= 5 (:count (:body r))) (= "d110" (:evidence/id (first (:entries (:body r))))))))
  (let [r (req "GET" (str base "/api/alpha/evidence?subject-type=pattern&subject-id=pat-1&limit=100") nil)]
    (check! "subject post-filter (projects evidence/subject): 30 on pat-1"
            (= 30 (:count (:body r)))))
  (let [r (req "GET" (str base "/api/alpha/evidence/count?type=note") nil)]
    (check! "/count type=note = 60 via parameterised deadlined scan"
            (= 60 (:count (:body r)))))
  (let [r (req "GET" (str base "/api/alpha/evidence?since=2026-08-23T10:01:50.000Z&limit=100") nil)]
    (check! "since pushdown as param: 10 entries" (= 10 (:count (:body r)))))

  ;; Hold the route's XTDB read until its holder is observable, then simulate
  ;; the deadline raised by timed-q. This exercises the HTTP error mapping and
  ;; the permit/holder finally block without making the suite wait 60 seconds;
  ;; timed-q's real deadline is covered by the JDBC deadline probe below.
  (let [entered (promise)
        release (promise)
        timeout-key (keyword "futon1b-xt" "timeout")
        t0 (System/currentTimeMillis)]
    (with-redefs [fxt/timed-q
                  (fn [& _]
                    (deliver entered true)
                    @release
                    (throw (ex-info "test query deadline"
                                    {:futon1b/error timeout-key
                                     :timeout-s 60})))]
      (let [response (future
                       (req "GET"
                            (str base "/api/alpha/entities?type=deadline-fixture&limit=1")
                            nil))]
        (check! "stalled /entities query enters timed-q"
                (true? (deref entered 5000 false)))
        (check! "cheap /health observes the stalled /entities holder"
                (= 1 (count (:holders
                             (:body (req "GET" (str base "/health") nil))))))
        (deliver release true)
        (let [r (deref response 5000 ::response-timeout)
              elapsed (- (System/currentTimeMillis) t0)]
          (check! "stalled /entities returns 504 within 65s"
                  (and (map? r) (= 504 (:status r)) (< elapsed 65000)))
          (check! "/entities timeout reports query-deadline-exceeded"
                  (= :query-deadline-exceeded (get-in r [:body :error])))
          (check! "cheap /health holder disappears after /entities timeout"
                  (empty? (:holders
                           (:body (req "GET" (str base "/health") nil)))))))))

  (let [r (req "GET" (str base "/api/alpha/entities?type=deadline-fixture&limit=1") nil)]
    (check! "fast /entities limit=1 remains 200" (= 200 (:status r))))

  ;; --- whole-request scan bound ---------------------------------------
  (let [ev-ns (find-ns 'futon1b-evidence)
        v (ns-resolve ev-ns 'max-scanned-rows-per-request)
        sp (ns-resolve ev-ns 'scan-page-size)]
    (with-redefs-fn {v 40 sp 20}
      (fn []
        (let [r (req "GET" (str base "/api/alpha/evidence?tags=rare&limit=100") nil)
              b (:body r)]
          (check! "scan ceiling: :incomplete true with cursor, not a corpus walk"
                  (and (= 200 (:status r)) (true? (:incomplete b))
                       (some? (:next-cursor b)) (= 40 (:scanned b))
                       (= 4 (:count b))))))))

  ;; --- JDBC deadline ----------------------------------------------------
  (let [slow "SELECT COUNT(*) AS n FROM evidence a, evidence b, evidence c, evidence d"
        t0 (System/currentTimeMillis)
        res (try (fxt/timed-q node [slow] 2)
                 (catch Exception e e))
        elapsed (- (System/currentTimeMillis) t0)]
    (println "    deadline probe:" (if (instance? Throwable res) (.getMessage ^Throwable res) res) "in" elapsed "ms")
    (check! "timed-q: 4-way cross join either finishes or times out under ~10s"
            (< elapsed 10000))
    (when (instance? Throwable res)
      (check! "timed-q: expiry is classified as a timeout"
              (fxt/timeout-error? res))))
  (check! "timed-q: fast query returns rows"
          (= 120 (:n (first (fxt/timed-q node ["SELECT COUNT(*) AS n FROM evidence"])))))
  (check! "timed-q: XTQL fn form with params"
          (= 1 (count (fxt/timed-q node ['(fn [i] (-> (from :evidence [evidence/id])
                                                      (where (= evidence/id i))))
                                          "d007"]))))

  ;; --- hyperedge cap ------------------------------------------------------
  (let [r (req "GET" (str base "/api/alpha/hyperedges?type=x&limit=1001") nil)]
    (check! "hyperedges limit=1001 -> 400" (= 400 (:status r))))
  (let [r (req "GET" (str base "/api/alpha/hyperedges?type=x&limit=1000") nil)]
    (check! "hyperedges limit=1000 -> 200" (= 200 (:status r))))

  ;; Hold the route's XTDB read until its holder is observable, then simulate
  ;; the deadline raised by timed-q. This exercises the HTTP error mapping and
  ;; the permit/holder finally block without making the suite wait 60 seconds.
  (let [entered (promise)
        release (promise)
        timeout-key (keyword "futon1b-xt" "timeout")
        t0 (System/currentTimeMillis)]
    (graph/invalidate-hyperedge-query-cache!)
    (with-redefs [fxt/timed-q
                  (fn [& _]
                    (deliver entered true)
                    @release
                    (throw (ex-info "test query deadline"
                                    {:futon1b/error timeout-key
                                     :timeout-s 60})))]
      (let [response (future
                       (req "GET"
                            (str base "/api/alpha/hyperedges?type=deadline-fixture&limit=1")
                            nil))]
        (check! "stalled /hyperedges query enters timed-q"
                (true? (deref entered 5000 false)))
        (check! "cheap /health observes the stalled /hyperedges holder"
                (= 1 (count (:holders
                             (:body (req "GET" (str base "/health") nil))))))
        (deliver release true)
        (let [r (deref response 5000 ::response-timeout)
              elapsed (- (System/currentTimeMillis) t0)]
          (check! "stalled /hyperedges returns 504 within 65s"
                  (and (map? r) (= 504 (:status r)) (< elapsed 65000)))
          (check! "/hyperedges timeout reports query-deadline-exceeded"
                  (= :query-deadline-exceeded (get-in r [:body :error])))
          (check! "cheap /health holder disappears after /hyperedges timeout"
                  (empty? (:holders
                           (:body (req "GET" (str base "/health") nil)))))))))

  ;; --- health observability --------------------------------------------
  (let [b (:body (req "GET" (str base "/health") nil))]
    (check! "/health reports permits, holders, stats, heap, gc"
            (and (= 2 (:permits/available b)) (= [] (:holders b))
                 (pos? (:admitted (:stats b))) (pos? (get-in b [:heap :max-mb]))
                 (map? (:gc b))))
    (check! "/health stats completed = admitted (all reads returned permits)"
            (= (:admitted (:stats b)) (+ (:completed (:stats b)) (:errored (:stats b)) (:timed-out (:stats b)))))))

(defn -main [& _]
  (gates/seed-mission-contract!)
  (with-open [node (xtn/start-node)]
    (let [server (srv/start-server! {:node node :port 0 :bind-host "127.0.0.1"})
          port (.getPort (.getAddress server))]
      (try (run-tests (str "http://127.0.0.1:" port) node)
           (finally (srv/stop-server! server)))))
  (println (format "%nfails: %d" @!fails))
  (System/exit (min 1 @!fails)))
