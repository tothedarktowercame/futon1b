;; test_a3a4a5 — smoke test for the A3 (entities/relations), A4 (hyperedge
;; reads), A5 (census/types) slices of E-futon1b-operational-switchover,
;; over real HTTP against an in-memory node.
;;
;; Run: clojure -M:node -m test-a3a4a5
(ns test-a3a4a5
  (:require [clojure.edn :as edn]
            [clojure.string :as cstr]
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
  (println (format "  %-56s %s" label (if ok? "PASS" (str "FAIL " (pr-str detail))))))

(def ph {"x-penholder" "joe"})

(defn run-tests [base]
  (let [ENT (str base "/api/alpha/entity")
        REL (str base "/api/alpha/relation")
        HX (str base "/api/alpha/hyperedge")
        HXS (str base "/api/alpha/hyperedges")]

    (println "— A3 entities: write path + gates")
    (let [r (req "POST" ENT {:name "Widget"} ph)]
      (check! "missing :type -> 400 :missing-required"
              (and (= 400 (:status r))
                   (= :missing-required (get-in r [:body :error :reason])))
              r))
    (let [r (req "POST" ENT {:name "Widget" :type "gadget"} nil)]
      (check! "no penholder -> 403" (= 403 (:status r)) r))
    (let [r (req "POST" ENT {:name "Widget" :type "gadget" :source "t"
                             :external-id "w-1" :props {:color "red"}} ph)
          ent (get-in r [:body :entity])]
      (check! "write -> 200 public entity shape"
              (and (= 200 (:status r)) (= "Widget" (:name ent))
                   (= :gadget (:type ent)) (string? (:id ent))
                   (= {:color "red"} (:props ent)))
              r))
    (let [r1 (req "POST" ENT {:name "Widget" :type "gadget"} ph)
          r2 (req "POST" ENT {:name "Widget" :type "gadget"} ph)]
      (check! "ensure-by-name: same id on re-post"
              (and (= 200 (:status r1)) (string? (get-in r1 [:body :entity :id]))
                   (= (get-in r1 [:body :entity :id]) (get-in r2 [:body :entity :id])))
              [r1 r2]))

    (println "— A3 entities: canonical-id gate over HTTP")
    (let [r (req "POST" ENT {:name "futon3c-d/mission/first-flights"
                             :type "mission/doc" :external-id "x"} ph)]
      (check! "canonical mission id -> 200" (= 200 (:status r)) r))
    (let [r (req "POST" ENT {:name "M-parked-alias" :type "mission/doc"
                             :external-id "x"} ph)]
      (check! "M-* alias -> 200 + :queued?"
              (and (= 200 (:status r)) (true? (get-in r [:body :queued?])))
              r))
    (let [r (req "POST" ENT {:name "futon3c-desktop-save-d/mission/x"
                             :type "mission/doc" :external-id "x"} ph)]
      (check! "drift repo -> 400 :non-canonical-id"
              (and (= 400 (:status r))
                   (= :non-canonical-id (get-in r [:body :error :reason])))
              r))

    (println "— A3 entities: reads")
    (let [id (get-in (req "POST" ENT {:name "Widget" :type "gadget"} ph)
                     [:body :entity :id])
          r (req "GET" (str ENT "/" id))]
      (check! "get by id -> 200" (= "Widget" (get-in r [:body :entity :name])) r))
    (let [r (req "GET" (str ENT "/Widget"))]
      (check! "get by name fallback -> 200"
              (= "Widget" (get-in r [:body :entity :name])) r))
    (let [r (req "GET" (str ENT "/nope-no-such"))]
      (check! "unknown -> 404 Entity not found"
              (and (= 404 (:status r)) (= "Entity not found" (get-in r [:body :error])))
              r))
    (let [_ (req "POST" ENT {:name "Gizmo" :type "gadget" :source "t"
                             :external-id "g-1"} ph)
          r (req "GET" (str ENT "?source=t&external-id=g-1"))]
      (check! "by source+external-id -> 200 {:entity}"
              (and (= 200 (:status r)) (= "Gizmo" (get-in r [:body :entity :entity/name])))
              r))
    (let [r (req "GET" (str ENT "?source=t&external-id=nope"))]
      (check! "unknown external-id -> 404 :not-found"
              (and (= 404 (:status r)) (= :not-found (get-in r [:body :error :reason])))
              r))
    (let [_ (req "POST" ENT {:name "Sprocket" :type "gadget"} ph)
          r (req "GET" (str base "/api/alpha/entities/latest?type=gadget&limit=5"))
          names (mapv :name (get-in r [:body :entities]))]
      (check! "entities/latest sorted by name"
              (= ["Gizmo" "Sprocket" "Widget"] names) r))
    (let [r (req "GET" (str base "/api/alpha/entities/latest"))]
      (check! "latest without type -> 400" (= 400 (:status r)) r))

    (println "— A3 relations")
    (let [r (req "POST" REL {:type "uses" :src "Widget"} ph)]
      (check! "missing dst -> 400" (= 400 (:status r)) r))
    (let [r (req "POST" REL {:type "uses" :src "Widget" :dst "Sprocket"
                             :provenance {:note "wired"}} ph)
          rel (get-in r [:body :relation])]
      (check! "relation by names -> 200 stable rel| id"
              (and (= 200 (:status r))
                   (cstr/starts-with? (:id rel) "rel|")
                   (cstr/includes? (:id rel) "|uses|")
                   (= :uses (:relation/type rel)))
              r))
    (let [r (req "POST" REL {:type "uses" :src "no-such-entity-xyz" :dst "Widget"} ph)]
      (check! "unresolvable src -> 500 wart"
              (and (= 500 (:status r))
                   (= "relation requires resolvable src/dst and type"
                      (get-in r [:body :error :message])))
              r))

    (println "— A4 hyperedge reads")
    (req "POST" HX {:hx/type :test/edge :hx/endpoints ["a" "b"]
                    :hx/props {:repo "r1" :source-file "f.clj"}} ph)
    (req "POST" HX {:hx/type :test/edge :hx/endpoints ["b" "c"]
                    :hx/props {:repo "r2"}} ph)
    (req "POST" HX {:hx/type :other/edge :hx/endpoints ["a" "z"]} ph)
    (let [r (req "GET" (str HX "/hx:test/edge:a.b"))]
      (check! "GET hyperedge by id (colons+slashes in tail)"
              (and (= 200 (:status r)) (= :test/edge (get-in r [:body :hx/type]))
                   (= {:repo "r1" :source-file "f.clj"} (get-in r [:body :hx/props])))
              r))
    (let [r (req "GET" (str HX "/hx:nope:x"))]
      (check! "unknown hx id -> 404" (= 404 (:status r)) r))
    (let [r (req "GET" (str HXS "?type=test/edge"))]
      (check! "?type -> 2 with true total"
              (and (= 2 (:count (:body r))) (= 2 (count (get-in r [:body :hyperedges]))))
              r))
    (let [r (req "GET" (str HXS "?type=test/edge&limit=1"))]
      (check! "limit truncates, :count stays true total"
              (and (= 2 (:count (:body r))) (= 1 (count (get-in r [:body :hyperedges]))))
              r))
    (let [r (req "GET" (str HXS "?type=test/edge&repo=r1"))]
      (check! "repo props filter -> 1, filtered count"
              (and (= 1 (:count (:body r)))
                   (= "r1" (get-in r [:body :hyperedges 0 :hx/props :repo])))
              r))
    (let [r (req "GET" (str HXS "?end=a"))]
      (check! "?end=a -> 2 across types" (= 2 (:count (:body r))) r))
    (let [r (req "GET" HXS)]
      (check! "no params -> 400" (= 400 (:status r)) r))

    (println "— A5 census + types")
    (let [r (req "GET" (str base "/api/alpha/census?type=test/edge"))]
      (check! "census hx type" (= {:type "test/edge" :kind :hyperedge :count 2}
                                  (:body r)) r))
    (let [r (req "GET" (str base "/api/alpha/census?entity-type=gadget"))]
      (check! "census entity type -> 3"
              (and (= :entity (get-in r [:body :kind])) (= 3 (get-in r [:body :count])))
              r))
    (let [r (req "GET" (str base "/api/alpha/census"))]
      (check! "census no params -> 400" (= 400 (:status r)) r))
    (let [r (req "GET" (str base "/api/alpha/types"))
          ids (set (map :type/id (get-in r [:body :types])))]
      (check! "auto-registered types listed (:gadget :uses :mission/doc)"
              (and (contains? ids :gadget) (contains? ids :uses)
                   (contains? ids :mission/doc))
              ids))
    (let [r (req "POST" (str base "/api/alpha/types/parent")
                 {:type/id "gadget" :type/kind "entity" :type/parent "thing"} ph)]
      (check! "types/parent header-only penholder -> 403 (body-only rule)"
              (= 403 (:status r)) r))
    (let [r (req "POST" (str base "/api/alpha/types/parent")
                 {:penholder "joe" :type/id "gadget" :type/kind "entity"
                  :type/parent "thing"} nil)
          types (get-in (req "GET" (str base "/api/alpha/types")) [:body :types])
          gadget (first (filter #(= :gadget (:type/id %)) types))]
      (check! "types/parent updates parent"
              (and (= 200 (:status r)) (= :thing (:type/parent gadget)))
              [r gadget]))
    (let [r (req "POST" (str base "/api/alpha/types/merge")
                 {:penholder "joe" :type/id "gadget" :type/kind "entity"
                  :type/aliases ["widget-kind"]} nil)
          types (get-in (req "GET" (str base "/api/alpha/types")) [:body :types])
          gadget (first (filter #(= :gadget (:type/id %)) types))]
      (check! "types/merge sets aliases"
              (and (= 200 (:status r)) (= [:widget-kind] (:type/aliases gadget)))
              [r gadget]))))

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
