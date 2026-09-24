;; The write log: a rescued or failed put must leave a record readable from
;; OUTSIDE the JVM. The bad case is the 2026-09-23 one -- a Ratio inside
;; :props reaches :rescued-2 and the store's message must be on record with
;; the id, table and stage. Asserts on the recorded message and on the file
;; and the route, never only on the return keyword (that already worked and
;; was not enough).
;;
;; Run: clojure -M:node -m test-write-log
(ns test-write-log
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [futon1b-evidence :as ev]
            [futon1b-gates :as gates]
            [futon1b-graph :as graph]
            [futon1b-server :as srv]
            [futon1b-write-log :as write-log]
            [xtdb.node :as xtn])
  (:import [java.net URI]
           [java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers
            HttpResponse$BodyHandlers]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(def client (HttpClient/newHttpClient))
(def !results (atom []))
(def ph {"x-penholder" "joe"})

(defn- req [method url body headers]
  (let [builder (-> (HttpRequest/newBuilder (URI/create url))
                    (.method method
                             (if body
                               (HttpRequest$BodyPublishers/ofString (pr-str body))
                               (HttpRequest$BodyPublishers/noBody))))
        builder (reduce (fn [b [k v]] (.header b k v)) builder (or headers {}))
        response (.send client (.build builder) (HttpResponse$BodyHandlers/ofString))
        raw (.body response)]
    {:status (.statusCode response)
     :body (try (edn/read-string raw) (catch Throwable _ raw))}))

(defn- check! [label ok? detail]
  (swap! !results conj {:label label :ok? (boolean ok?) :detail detail})
  (println (format "  %-66s %s" label (if ok? "PASS" (str "FAIL " (pr-str detail))))))

(defn- file-entries [path]
  (if (.exists (java.io.File. ^String path))
    (->> (str/split-lines (slurp path))
         (remove str/blank?)
         (mapv edn/read-string))
    []))

(def ratio-message-fragment "clojure.lang.Ratio")

(defn- run-tests [base path]
  (let [ENT (str base "/api/alpha/entity")
        HX (str base "/api/alpha/hyperedge")
        LOG (str base "/api/alpha/write-log")
        put-failed (fn [entries] (filterv #(= :put-failed (:kind %)) entries))]

    ;; --- 1. the bad case: a Ratio inside :props --------------------------
    (let [r (req "POST" ENT {:name "Ratio bearer" :type "gadget" :source "t"
                             :external-id "ratio-1"
                             :props {:precedence [{:id :a :theta 3/4}]}} ph)
          ent-id (get-in r [:body :entity :id])
          mem (put-failed @graph/!shape-log)
          hit (first (filter #(= ent-id (:xt/id %)) mem))]
      (check! "ratio entity write -> 200 with :rescue :rescued-2 (envelope unchanged)"
              (and (= 200 (:status r)) (= :rescued-2 (get-in r [:body :rescue])))
              r)
      (check! "in-memory record names the store's message, id, table and stage"
              (and hit
                   (str/includes? (:message hit) ratio-message-fragment)
                   (= :entities (:table hit))
                   (= :put (:stage hit))
                   (string? (:at hit)))
              hit)
      (check! "one failed attempt per stage before :rescued-2 (:put and :rescue-1)"
              (= [:put :rescue-1] (mapv :stage (filter #(= ent-id (:xt/id %)) mem)))
              (mapv :stage (filter #(= ent-id (:xt/id %)) mem)))
      (when hit (println "  recorded message:" (pr-str (:message hit))))

      ;; --- 2. readable from outside the process ---------------------------
      (let [on-disk (put-failed (file-entries path))
            disk-hit (first (filter #(= ent-id (:xt/id %)) on-disk))]
        (check! "durable file holds the same record (id, table, stage, message)"
                (and disk-hit
                     (str/includes? (:message disk-hit) ratio-message-fragment)
                     (= :entities (:table disk-hit))
                     (= :put (:stage disk-hit)))
                {:path path :entry disk-hit}))
      (let [r (req "GET" (str LOG "?kind=put-failed&limit=10") nil nil)
            route-hit (first (filter #(= ent-id (:xt/id %)) (get-in r [:body :entries])))]
        (check! "GET /api/alpha/write-log returns the record and names the file"
                (and (= 200 (:status r))
                     (true? (get-in r [:body :ok]))
                     (= path (get-in r [:body :file]))
                     route-hit
                     (str/includes? (:message route-hit) ratio-message-fragment))
                r))
      (let [r (req "POST" LOG {} ph)]
        (check! "write-log route is GET only" (= 405 (:status r)) r)))

    ;; --- 3. a clean write records nothing --------------------------------
    (let [before-mem (count @graph/!shape-log)
          before-disk (count (file-entries path))
          r (req "POST" ENT {:name "Clean" :type "gadget" :source "t"
                             :external-id "clean-1" :props {:theta 0.75}} ph)]
      (check! "clean entity write -> :rescue :ok, envelope unchanged"
              (and (= 200 (:status r)) (= :ok (get-in r [:body :rescue])))
              r)
      (check! "clean write adds no in-memory entry"
              (= before-mem (count @graph/!shape-log)) (count @graph/!shape-log))
      (check! "clean write adds no file line"
              (= before-disk (count (file-entries path))) (count (file-entries path))))

    ;; --- 4. hyperedges: the site that used to pass nil ---------------------
    ;; H4 denormalizes :hx/props to top-level :prop/* columns, so a Ratio
    ;; there is a top-level SCALAR and no rescue stage stringifies scalars:
    ;; the doc fails all three stages and the route's existing verified-put
    ;; 500 stands. Before this change that 500 carried no store message
    ;; anywhere; now all three attempts are on record.
    (let [r (req "POST" HX {:hx/type :test/edge :hx/endpoints ["a" "b"]
                            :hx/props {:theta 1/4}} ph)
          hx-id (get-in r [:body :hx/id])
          hits (filterv #(and (= :hyperedges (:table %)) (= hx-id (:xt/id %)))
                        (put-failed @graph/!shape-log))
          hit (first hits)]
      (check! "ratio hyperedge write -> existing 500 verified-put refusal (unchanged)"
              (and (= 500 (:status r))
                   (= "verified put: doc absent after rescue ladder"
                      (get-in r [:body :error])))
              r)
      (check! "all three failed attempts recorded (:put :rescue-1 :rescue-2)"
              (= [:put :rescue-1 :rescue-2] (mapv :stage hits))
              (mapv :stage hits))
      (check! "hyperedge failure is recorded with the store's message"
              (and hit (str/includes? (:message hit) ratio-message-fragment))
              hit)
      (check! "hyperedge record reached the durable file"
              (some #(and (= hx-id (:xt/id %)) (= :hyperedges (:table %)))
                    (put-failed (file-entries path)))
              hx-id))

    ;; --- 5. the evidence log is served by the same route ------------------
    (check! "route merges both shape logs (evidence log is a registered source)"
            (= (+ (count @graph/!shape-log) (count @ev/!shape-log))
               (:count (write-log/entries [graph/!shape-log ev/!shape-log] {})))
            [(count @graph/!shape-log) (count @ev/!shape-log)])))

(defn -main [& _]
  (gates/seed-mission-contract!)
  (let [dir (str (Files/createTempDirectory "futon1b-write-log" (make-array FileAttribute 0)))
        path (write-log/file-path dir)]
    (with-open [node (xtn/start-node)]
      (let [server (srv/start-server! {:node node :port 0 :write-log-path path})
            base (str "http://127.0.0.1:" (.getPort (.getAddress server)))]
        (try
          (run-tests base path)
          (finally
            (srv/stop-server! server)
            (write-log/detach-file! [graph/!shape-log ev/!shape-log]))))))
  (let [results @!results
        failures (remove :ok? results)]
    (println (format "%n%d/%d PASS" (- (count results) (count failures)) (count results)))
    (shutdown-agents)
    (System/exit (if (seq failures) 1 0))))
