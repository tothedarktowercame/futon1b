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
        RELS (str base "/api/alpha/relations")
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
    (let [r (req "GET" (str RELS "?type=uses"))]
      (check! "typed relation read returns the stored relation"
              (and (= 200 (:status r))
                   (= 1 (get-in r [:body :count]))
                   (= :uses (get-in r [:body :relations 0 :relation/type])))
              r))
    (let [r (req "GET" (str RELS "?types=uses,unused&hydrate=true"))]
      (check! "multi-type relation snapshot hydrates endpoint entities once"
              (and (= 200 (:status r))
                   (= 1 (get-in r [:body :count]))
                   (= #{"Widget" "Sprocket"}
                      (set (map :entity/name (get-in r [:body :entities])))))
              r))
    (req "POST" ENT {:name "DomainProps" :type "gadget"
                      :props {:color "red"}} ph)
    (let [r (req "GET" (str base "/api/alpha/entities?type=gadget&limit=10"))]
      (check! "typed raw entity read preserves domain props"
              (and (= 200 (:status r))
                   (= 4 (get-in r [:body :count]))
                   (= "red" (some #(get-in % [:entity/props :color])
                                   (get-in r [:body :entities]))))
              r))

    (println "— A3 relations/batch (contract §6 batch variant)")
    (let [r (req "POST" (str RELS "/batch")
                 {:relations [{:type "batch/wires" :src "Widget" :dst "Gizmo"
                               :provenance {:note "w1"}}
                              {:type "batch/wires" :src "Gizmo" :dst "Sprocket"
                               :props {:label "w2"}}]}
                 ph)
          rels (get-in r [:body :relations])]
      (check! "batch write -> 200, :count 2, stable rel| ids"
              (and (= 200 (:status r))
                   (= 2 (get-in r [:body :count]))
                   (every? #(cstr/starts-with? (:id %) "rel|") rels)
                   (every? #(= :batch/wires (:relation/type %)) rels))
              r))
    (let [r (req "GET" (str RELS "?type=batch/wires"))]
      (check! "both batch relations stored and readable"
              (and (= 200 (:status r)) (= 2 (get-in r [:body :count])))
              r))
    (let [r (req "POST" (str RELS "/batch")
                 {:relations [{:type "batch/wires" :src "Widget" :dst "Gizmo"}]}
                 nil)]
      (check! "batch without penholder -> 403" (= 403 (:status r)) r))
    (let [r (req "POST" (str RELS "/batch") {} ph)]
      (check! "batch missing :relations -> 400 :missing-required"
              (and (= 400 (:status r))
                   (= :missing-required (get-in r [:body :error :reason])))
              r))
    (let [r (req "POST" (str RELS "/batch") {:relations []} ph)]
      (check! "batch empty :relations -> 400 :invalid-relations-batch"
              (and (= 400 (:status r))
                   (= :invalid-relations-batch (get-in r [:body :error :reason])))
              r))
    (let [r (req "POST" (str RELS "/batch")
                 {:relations [{:type "batch/partial" :src "Widget" :dst "Gizmo"}
                              {:type "batch/partial" :src "Widget"}]}
                 ph)
          stored (req "GET" (str RELS "?type=batch/partial"))]
      (check! "item missing dst -> 400, and NOTHING from the batch is written"
              (and (= 400 (:status r))
                   (= :missing-required (get-in r [:body :error :reason]))
                   (= 0 (get-in stored [:body :count])))
              [r stored]))
    (let [r (req "POST" (str RELS "/batch")
                 {:relations [{:type "batch/dangling" :src "Widget"
                               :dst "0b16c1b1-0000-4000-8000-000000000000"}]}
                 ph)
          stored (req "GET" (str RELS "?type=batch/dangling"))]
      (check! "absent uuid endpoint -> 500 L2 :missing-endpoint, nothing written"
              (and (= 500 (:status r))
                   (= :missing-endpoint (get-in r [:body :error :reason]))
                   (= 0 (get-in stored [:body :count])))
              [r stored]))
    (let [r1 (req "POST" (str RELS "/batch")
                  {:relations [{:type "batch/wires" :src "Widget" :dst "Gizmo"
                                :provenance {:note "w1"}}]}
                  ph)
          r2 (req "GET" (str RELS "?type=batch/wires"))]
      (check! "re-posting a batch item is idempotent (stable id, count holds)"
              (and (= 200 (:status r1)) (= 2 (get-in r2 [:body :count])))
              [r1 r2]))

    (println "— A4 hyperedge reads")
    (req "POST" HX {:hx/type :test/edge :hx/endpoints ["a" "b"]
                    :hx/props {:repo "r1" :source-file "f.clj"}} ph)
    (req "POST" HX {:hx/type :test/edge :hx/endpoints ["b" "c"]
                    :hx/props {:repo "r2"}} ph)
    (req "POST" HX {:hx/type :other/edge :hx/endpoints ["a" "z"]} ph)
    (req "POST" ENT {:id "a" :name "a" :type "left-kind"} ph)
    (req "POST" ENT {:id "b" :name "b" :type "right-kind"} ph)
    (req "POST" HX {:hx/type (keyword "code/v05/commit") :hx/endpoints ["old"]
                    :hx/props {:repo "r1" :timestamp 10}} ph)
    (req "POST" HX {:hx/type (keyword "code/v05/commit") :hx/endpoints ["new"]
                    :hx/props {:repo "r1" :timestamp 20}} ph)
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
    (let [r (req "GET" (str HXS "?type=test/edge&limit=1&include-total=false"))]
      (check! "caller can skip the exact-total scan"
              (and (= 1 (:count (:body r)))
                   (false? (:count-exact? (:body r)))
                   (= 1 (count (get-in r [:body :hyperedges]))))
              r))
    (let [r (req "GET" (str HXS "?type=test/edge&repo=r1"))]
      (check! "repo props filter -> 1, filtered count"
              (and (= 1 (:count (:body r)))
                   (= "r1" (get-in r [:body :hyperedges 0 :hx/props :repo])))
              r))
    (let [r (req "GET" (str HXS "?type=code/v05/commit&repo=r1&latest=true&limit=1"))]
      (check! "latest orders by denormalized timestamp and returns one"
              (and (= 1 (:count (:body r)))
                   (= ["new"] (get-in r [:body :hyperedges 0 :hx/endpoints])))
              r))
    (let [r (req "GET" (str HXS "?end=a"))]
      (check! "?end=a -> 2 across types" (= 2 (:count (:body r))) r))
    (let [r (req "GET" (str HXS "?end=a&limit=1"))]
      (check! "?end limit is pushed down before full-doc hydration"
              (and (= 1 (:count (:body r)))
                   (= 1 (count (get-in r [:body :hyperedges]))))
              r))
    (let [r (req "GET" HXS)]
      (check! "no params -> 400" (= 400 (:status r)) r))
    (let [r (req "POST" (str base "/api/alpha/graph/inhabited")
                 {:bindings [{:id :entity :kind :entity :type :gadget}
                             {:id :edge :kind :hyperedge :type :test/edge
                              :endpoint-types [:left-kind :right-kind]}
                             {:id :absent :kind :entity :type :not-present}]}
                 nil)
          rows (get-in r [:body :bindings])]
      (check! "batch inhabitation proves entity and typed hyperedge bindings"
              (and (= 200 (:status r))
                   (= [true true false] (mapv :inhabited? rows)))
              r))

    (println "— maintenance document retraction")
    (let [_ (req "POST" ENT {:id "retract-me" :name "RetractMe" :type "gadget"} ph)
          _ (req "POST" HX {:hx/id "hx:retract-me" :hx/type "test/retract"
                             :hx/endpoints ["retract-me" "Widget"]} ph)
          no-ph (req "POST" (str base "/api/alpha/documents/retract")
                     {:documents [{:table :hyperedges :id "hx:retract-me"}]} nil)
          retract (req "POST" (str base "/api/alpha/documents/retract")
                       {:documents [{:table :hyperedges :id "hx:retract-me"}
                                    {:table :entities :id "retract-me"}]} ph)]
      (check! "document retract requires penholder"
              (= 403 (:status no-ph)) no-ph)
      (check! "document retract atomically accepts entity + hyperedge"
              (and (= 200 (:status retract)) (= 2 (get-in retract [:body :count])))
              retract)
      (check! "document retract read-back is absent"
              (and (= 404 (:status (req "GET" (str HX "/hx%3Aretract-me"))))
                   (= 404 (:status (req "GET" (str ENT "/retract-me")))))
              retract))
    (let [_ (req "POST" ENT {:id "retain-me" :name "RetainMe" :type "gadget"} ph)
          rejected (req "POST" (str base "/api/alpha/documents/retract")
                        {:documents [{:table :entities :id "retain-me"}
                                     {:table :evidence :id "not-allowed"}]} ph)]
      (check! "invalid batch rejects before deleting its valid prefix"
              (and (= 400 (:status rejected))
                   (= 200 (:status (req "GET" (str ENT "/retain-me")))))
              rejected)
      (req "POST" (str base "/api/alpha/documents/retract")
           {:documents [{:table :entities :id "retain-me"}]} ph))
    (let [r (req "POST" (str base "/api/alpha/documents/retract")
                 {:documents [{:table :entities :id "already-absent"}]} ph)]
      (check! "document retract is idempotent for an absent id"
              (and (= 200 (:status r)) (= 1 (get-in r [:body :count]))) r))

    (println "— A5 census + types")
    (let [r (req "GET" (str base "/api/alpha/census?type=test/edge"))]
      (check! "census hx type" (= {:type "test/edge" :kind :hyperedge :count 2}
                                  (:body r)) r))
    (let [r (req "GET" (str base "/api/alpha/census?entity-type=gadget"))]
      (check! "census entity type -> 4"
              (and (= :entity (get-in r [:body :kind])) (= 4 (get-in r [:body :count])))
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
        (finally (srv/stop-server! server)))))
  (let [results @!results
        fails (remove :ok? results)]
    (println (format "%n%d/%d PASS" (- (count results) (count fails)) (count results)))
    (System/exit (if (seq fails) 1 0))))
