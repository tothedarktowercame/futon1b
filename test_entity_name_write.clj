;; test_entity_name_write — a name-resolved entity write (POST /api/alpha/entity
;; with no :id) must resolve the existing entity through the entity sidecar's
;; indexed columns as point reads, with the XTDB path as fallback when the
;; sidecar is not ready. Futon3c's watcher writes excursion entities this way
;; (file_ingest.clj ingest-excursion-doc! → post-entity! with :name, no :id),
;; and on 2026-11-14 each such write took ~20 s live because the alias lookup
;; hydrated ids through XTDB (`hydrate-by-ids`, a per-id equality scan over the
;; whole table) even while the sidecar was serving complete.
;;
;; Run: clojure -M:node -m test-entity-name-write
(ns test-entity-name-write
  (:require [next.jdbc :as jdbc]
            [futon1b-entindex :as ent]
            [futon1b-gates :as gates]
            [futon1b-graph :as graph]
            [futon1b-text :as text]
            [futon1b-xt :as fxt]
            [xtdb.node :as xtn])
  (:import [java.nio.file Files]))

(def !results (atom []))

(defn check! [label ok? detail]
  (swap! !results conj {:label label :ok? (boolean ok?) :detail detail})
  (println (format "  %-62s %s" label (if ok? "PASS" (str "FAIL " (pr-str detail))))))

(defn- temp-dir []
  (-> (Files/createTempDirectory "futon1b-name-write-test-"
                                 (make-array java.nio.file.attribute.FileAttribute 0))
      .toFile .getAbsolutePath))

(defn- open-gate! []
  ;; Same gate-opening the production catch-up performs: checkpoint recorded,
  ;; hook failures consistent, reads enabled (test_entity_alias_sidecar does
  ;; this with raw SQL because the gate helpers are private to the sidecar
  ;; lifecycle, which the test drives by hand).
  (jdbc/execute! @text/!ds ["INSERT OR REPLACE INTO ent_meta(k,v) VALUES('checkpoint-ts','name-write-test')"])
  (jdbc/execute! @text/!ds ["INSERT OR REPLACE INTO ent_meta(k,v) VALUES('hook-failures-at-catch-up',?)"
                            (str (:hook-failures @ent/!stats))])
  (reset! ent/!reads-enabled true))

(defn- close-gate! []
  (reset! ent/!reads-enabled false))

(defn- entity-id [node name]
  (-> (fxt/safe-q node (fxt/pq '[p-v]
                                '(-> (from :entities [xt/id entity/name])
                                     (where (= entity/name p-v)))
                                name))
      first :xt/id))

(defn run-tests [node]
  (gates/seed-mission-contract!)

  (println "— name-resolved write while the sidecar serves: point reads only")
  (let [first (graph/write-entity! node {:name "futon2-d/excursion/kimi-task-303"
                                         :type "excursion/doc"
                                         :external-id "E-kimi-task-303"
                                         :props {:excursion/id "kimi-task-303"
                                                 :excursion/status "open"}})
        id-1 (get-in first [:entity :id])
        xt-count (fn [] (count (fxt/safe-q node (fxt/pq '[p-v]
                                                        '(-> (from :entities [xt/id entity/name])
                                                             (where (= entity/name p-v)))
                                                        "futon2-d/excursion/kimi-task-303"))))
        ;; The alias hydration must not touch XTDB while the gate is open.
        second (with-redefs [fxt/hydrate-by-ids
                             (fn [& _] (throw (ex-info "XTDB hydrate forbidden while sidecar serves" {})))]
                 (graph/write-entity! node {:name "futon2-d/excursion/kimi-task-303"
                                            :type "excursion/doc"
                                            :external-id "E-kimi-task-303"
                                            :props {:excursion/id "kimi-task-303"
                                                    :excursion/status "closed"}}))]
    (check! "first write creates the entity"
            (and (string? id-1) (get-in first [:entity :name]))
            first)
    (check! "same-name write re-resolves to the same entity id"
            (= id-1 (get-in second [:entity :id]))
            second)
    (check! "no duplicate entity was created"
            (and (= 1 (xt-count)) (= 1 (count (ent/alias-ids "futon2-d/excursion/kimi-task-303" false))))
            (xt-count))
    (check! "the write replaced the document (no second version, new props)"
            (= "closed" (get-in (ent/entity-by-id id-1) [:entity/props :excursion/status]))
            (ent/entity-by-id id-1)))

  (println "— sidecar not ready: XTDB fallback resolves the same entity")
  (close-gate!)
  (let [before (entity-id node "futon2-d/excursion/kimi-task-303")
        res (graph/write-entity! node {:name "futon2-d/excursion/kimi-task-303"
                                       :type "excursion/doc"
                                       :external-id "E-kimi-task-303"
                                       :props {:excursion/id "kimi-task-303"
                                               :excursion/status "open"}})
        after (entity-id node "futon2-d/excursion/kimi-task-303")]
    (check! "closed gate: fallback still writes the same entity"
            (and (some? before) (= before after)
                 (= after (get-in res [:entity :id])))
            [before after])
    (check! "closed gate: still exactly one entity by that name"
            (= 1 (count (fxt/safe-q node (fxt/pq '[p-v]
                                                 '(-> (from :entities [xt/id entity/name])
                                                      (where (= entity/name p-v)))
                                                 "futon2-d/excursion/kimi-task-303"))))
            nil)))

(defn run-new-name-tests [node]
  (open-gate!)
  (println "— a name that does not exist creates a new entity")
  (let [res (graph/write-entity! node {:name "futon2-d/excursion/never-seen-before"
                                       :type "excursion/doc"})
        id (get-in res [:entity :id])]
    (check! "new name mints a fresh id and no alias pre-existed"
            (and (string? id) (empty? (drop 1 (ent/alias-ids "futon2-d/excursion/never-seen-before" false))))
            res)
    (check! "the new entity is immediately readable by name from the sidecar"
            (= id (first (ent/alias-ids "futon2-d/excursion/never-seen-before" false)))
            (ent/alias-ids "futon2-d/excursion/never-seen-before" false))))

(defn -main [& _]
  (let [dir (temp-dir)]
    (text/init! {:path (str dir "/entities.db")})
    (ent/init!)
    (with-open [node (xtn/start-node)]
      (open-gate!)
      (run-tests node)
      (run-new-name-tests node)))
  (let [results @!results
        failures (remove :ok? results)]
    (println (format "%n%d/%d PASS"
                     (- (count results) (count failures)) (count results)))
    (doseq [f failures] (println "FAIL:" (:label f) (pr-str (:detail f))))
    (System/exit (if (seq failures) 1 0))))
