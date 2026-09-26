;; test_alias_warrants — alias-scan warrants on GET /api/alpha/entity/{id}
;; (futon1b_graph.clj, "Alias warrants"), over real HTTP against an in-memory
;; node. Each check names the way a warrant could lie and shows it does not:
;; a write, batch write or retraction after an absence/resolution warrant; a
;; scan that overlaps a write; a doc changed without going through the write
;; paths.
;;
;; Run: clojure -M:node -m test-alias-warrants
(ns test-alias-warrants
  (:require [clojure.edn :as edn]
            [futon1b-gates :as gates]
            [futon1b-graph :as graph]
            [futon1b-server :as srv]
            [xtdb.api :as xt]
            [xtdb.node :as xtn])
  (:import [java.net URI URLEncoder]
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
  (println (format "  %-62s %s" label (if ok? "PASS" (str "FAIL " (pr-str detail))))))

(def ph {"x-penholder" "joe"})

(defn- enc [s] (URLEncoder/encode s "UTF-8"))

(defn run-tests [node base]
  (let [ENT (str base "/api/alpha/entity")
        get-ent #(req "GET" (str ENT "/" (enc %)))
        stats #(graph/alias-warrant-snapshot)]

    (println "— absence is warranted, then reused without a rescan")
    (let [r1 (get-ent "pat/missing")
          issued (:issued (stats))
          r2 (get-ent "pat/missing")
          w1 (get-in r1 [:body :warrant])
          w2 (get-in r2 [:body :warrant])]
      (check! "miss -> 404 carrying an :absent warrant"
              (and (= 404 (:status r1)) (= :absent (:warrant/claim w1))
                   (= "pat/missing" (:warrant/subject w1))
                   (false? (:warrant/reused? w1)))
              r1)
      (check! "repeat miss reuses the warrant; no new scan issued"
              (and (= 404 (:status r2)) (true? (:warrant/reused? w2))
                   (= (:warrant/basis w1) (:warrant/basis w2))
                   (= issued (:issued (stats))))
              [r2 (stats)])
      (check! "/health exposes the warrant counters"
              (map? (:alias-warrants (srv/expensive-read-snapshot)))
              (srv/expensive-read-snapshot)))

    (println "— a write retires an absence warrant")
    (let [_ (get-ent "pat/appears")
          w (req "POST" ENT {:name "pat/appears" :type "gadget"} ph)
          r (get-ent "pat/appears")]
      (check! "entity named after a warranted miss is found after write"
              (and (= 200 (:status w)) (= 200 (:status r))
                   (= "pat/appears" (get-in r [:body :entity :name])))
              [w r]))
    (let [_ (get-ent "pat/batch")
          w (req "POST" (str base "/api/alpha/entities/batch")
                 {:entities [{:name "pat/batch" :type "gadget"}]} ph)
          r (get-ent "pat/batch")]
      (check! "batch write also retires the absence warrant"
              (and (= 200 (:status w)) (= 200 (:status r))) [w r]))
    (let [_ (get-ent "ext-7")
          w (req "POST" ENT {:name "Seven" :type "gadget" :source "t"
                             :external-id "ext-7"} ph)
          r (get-ent "ext-7")]
      (check! "external-id alias found after write"
              (and (= 200 (:status w)) (= "Seven" (get-in r [:body :entity :name])))
              [w r]))

    (println "— a retraction retires a resolved warrant")
    (let [_ (req "POST" ENT {:id "id-gone" :name "pat/gone" :type "gadget"} ph)
          r1 (get-ent "pat/gone")
          _ (get-ent "pat/gone")
          rechecked (:rechecked-out (stats))
          x (req "POST" (str base "/api/alpha/documents/retract")
                 {:documents [{:table :entities :id "id-gone"}]} ph)
          r2 (get-ent "pat/gone")]
      (check! "resolved by name, then 404 :absent after retraction"
              (and (= 200 (:status r1)) (= 200 (:status x))
                   (= 404 (:status r2))
                   (= :absent (get-in r2 [:body :warrant :warrant/claim])))
              [r1 x r2])
      ;; Without the retraction's bump the re-check would still catch the
      ;; deletion; an unchanged :rechecked-out shows the warrant was retired.
      (check! "retraction retired the warrant (no re-check needed)"
              (= rechecked (:rechecked-out (stats)))
              (stats)))
    (let [_ (req "POST" (str base "/api/alpha/hyperedge")
                 {:hx/id "hx:aw" :hx/type "test/aw"
                  :hx/endpoints ["pat/appears" "pat/batch"]} ph)
          _ (get-ent "pat/kept")
          x (req "POST" (str base "/api/alpha/documents/retract")
                 {:documents [{:table :hyperedges :id "hx:aw"}]} ph)
          r (get-ent "pat/kept")]
      (check! "hyperedge-only retraction keeps entity warrants"
              (and (= 200 (:status x))
                   (true? (get-in r [:body :warrant :warrant/reused?])))
              [x r]))

    (println "— direct put-verified! on :entities bumps by itself")
    (let [_ (get-ent "pat/direct")
          _ (graph/put-verified! node :entities
                                 {:xt/id "id-direct" :entity/id "id-direct"
                                  :entity/name "pat/direct" :entity/type :gadget})
          r (get-ent "pat/direct")]
      (check! "entity put via put-verified! is found after a warranted miss"
              (= 200 (:status r)) r))

    (println "— a failed scan issues nothing")
    (let [issued (:issued (stats))
          r1 (with-redefs [graph/scan-alias-ids
                           (fn [_ _ _] (throw (ex-info "scan failed" {})))]
               (get-ent "pat/errored"))
          issued-after-error (:issued (stats))
          r2 (get-ent "pat/errored")]
      (check! "throwing scan -> error, not a warranted 404"
              (and (not= 404 (:status r1))
                   (nil? (get-in r1 [:body :warrant]))
                   (= issued issued-after-error))
              r1)
      (check! "next lookup scans afresh"
              (false? (get-in r2 [:body :warrant :warrant/reused?]))
              r2))

    (println "— long subjects are answered, not kept")
    (let [long-id (apply str (repeat 2000 "x"))
          _ (get-ent long-id)
          r (get-ent long-id)]
      (check! "2000-char subject is never reused"
              (and (= 404 (:status r))
                   (false? (get-in r [:body :warrant :warrant/reused?])))
              (dissoc (:body r) :entity-id)))

    (println "— the store disposes: out-of-band change fails the re-check")
    (let [_ (req "POST" ENT {:id "id-renamed" :name "pat/old-name" :type "gadget"} ph)
          _ (get-ent "pat/old-name")
          rechecked (:rechecked-out (stats))
          ;; Deliberately bypass the write paths: the warrant's basis is still
          ;; current, so only the hydrated re-check can catch this.
          _ (xt/execute-tx node [[:put-docs :entities
                                  {:xt/id "id-renamed" :entity/id "id-renamed"
                                   :entity/name "pat/new-name"
                                   :entity/type :gadget}]])
          r (get-ent "pat/old-name")]
      (check! "stale resolved warrant is dropped, rescan says absent"
              (and (= 404 (:status r))
                   (= (inc rechecked) (:rechecked-out (stats))))
              [r (stats)]))

    (println "— a scan that overlaps a write is not kept")
    (let [scan @#'graph/scan-alias-ids
          superseded (:superseded (stats))
          r1 (with-redefs [graph/scan-alias-ids
                           (fn [n v e?]
                             (let [ids (scan n v e?)]
                               ;; a write commits while the scan is in flight
                               (graph/invalidate-alias-warrants!)
                               ids))]
               (get-ent "pat/raced"))
          r2 (get-ent "pat/raced")]
      (check! "overlapping scan answers but is counted superseded"
              (and (= 404 (:status r1))
                   (= (inc superseded) (:superseded (stats))))
              [r1 (stats)])
      (check! "next lookup rescans rather than reusing it"
              (false? (get-in r2 [:body :warrant :warrant/reused?]))
              r2))

    (println "— minted ids skip the alias scan")
    (let [r (get-ent (str (random-uuid)))]
      (check! "uuid-shaped miss -> 404 without a warrant"
              (and (= 404 (:status r)) (nil? (get-in r [:body :warrant])))
              r))))

(defn -main [& _]
  (gates/seed-mission-contract!)
  (with-open [node (xtn/start-node)]
    (let [server (srv/start-server! {:node node :port 0})
          base (str "http://127.0.0.1:" (.getPort (.getAddress server)))]
      (try
        (run-tests node base)
        (finally (srv/stop-server! server)))))
  (println "— a new node does not inherit the old node's warrants")
  (with-open [node-a (xtn/start-node)
              node-b (xtn/start-node)]
    (let [start #(srv/start-server! {:node % :port 0})
          url #(str "http://127.0.0.1:" (.getPort (.getAddress %))
                    "/api/alpha/entity/" (enc "pat/elsewhere"))
          server-a (start node-a)
          miss (req "GET" (url server-a))
          _ (srv/stop-server! server-a)
          _ (xt/execute-tx node-b [[:put-docs :entities
                                    {:xt/id "id-elsewhere" :entity/id "id-elsewhere"
                                     :entity/name "pat/elsewhere"
                                     :entity/type :gadget}]])
          server-b (start node-b)
          hit (try (req "GET" (url server-b))
                   (finally (srv/stop-server! server-b)))]
      (check! "absent on store A, found on store B after server swap"
              (and (= 404 (:status miss)) (= 200 (:status hit)))
              [miss hit])))
  (let [results @!results
        failures (remove :ok? results)]
    (println (format "%n%d/%d PASS"
                     (- (count results) (count failures)) (count results)))
    (shutdown-agents)
    (System/exit (if (seq failures) 1 0))))
