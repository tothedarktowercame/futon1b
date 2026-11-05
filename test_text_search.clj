(ns test-text-search
  "Throwaway-node tests for the FTS5 candidate/re-check seam.

  Run: clojure -M:node -m test-text-search"
  (:require [clojure.edn :as edn]
            [clojure.java.shell :as shell]
            [futon1b-server :as server]
            [futon1b-text :as text]
            [xtdb.api :as xt]
            [xtdb.node :as xtn])
  (:import [java.net URI URLEncoder]
           [java.net.http HttpClient HttpRequest HttpResponse$BodyHandlers]
           [java.nio.file Files]))

(defn- temp-dir []
  (-> (Files/createTempDirectory
       "futon1b-text-test-"
       (make-array java.nio.file.attribute.FileAttribute 0))
      .toFile
      .getAbsolutePath))

(defn- check! [label value]
  (println (format "  %-62s %s" label (if value "PASS" "FAIL")))
  (assert value label))

(defn- old-serial-search
  "Pre-change search shape, retained only for the throwaway benchmark."
  [node params]
  (let [ds @(var-get (ns-resolve 'futon1b-text '!ds))
        candidate-fn (ns-resolve 'futon1b-text 'candidates)
        fetch-fn (ns-resolve 'futon1b-text 'fetch-doc)
        k (:limit params)
        cands (candidate-fn ds params)]
    (into []
          (comp
           (keep (fn [{:keys [id score]}]
                   (when-let [doc (fetch-fn node id '[*])]
                     {:score score :entry (dissoc doc :xt/id)})))
           (take k))
          cands)))

(defn- elapsed-ms [f]
  (let [started (System/nanoTime)]
    (f)
    (/ (double (- (System/nanoTime) started)) 1000000.0)))

(defn- median-ms [f]
  (nth (vec (sort (repeatedly 3 #(elapsed-ms f)))) 1))

(defn- get-edn [url]
  (let [request (-> (HttpRequest/newBuilder (URI/create url)) .GET .build)
        response (.send (HttpClient/newHttpClient)
                        request
                        (HttpResponse$BodyHandlers/ofString))]
    {:status (.statusCode response)
     :body (edn/read-string (.body response))}))

(defn -main [& _]
  (let [dir (temp-dir)
        ordinary-docs (vec
                       (for [i (range 48)]
                {:xt/id (str "text-" i)
                 :evidence/id (str "text-" i)
                 :evidence/type (if (even? i) :memory/assert :note)
                 :evidence/claim-type :observation
                 :evidence/author "joe"
                 :evidence/session-id "text-test"
                 :evidence/at (format "2026-07-31T00:%02d:00Z" i)
                 :evidence/ephemeral? (= i 0)
                 :evidence/body
                 (str "common token body " i " "
                      (nth ["substrate handoff mission"
                            "peradam gflownets"
                            "zabuton correction lexicon"
                            "evidence backend futon1b sunflower"]
                           (mod i 4)))}))
        scope-docs
        [{:xt/id "scope-operator" :evidence/id "scope-operator"
          :evidence/type :coordination :evidence/author "joe"
          :evidence/at "2026-07-31T01:00:00Z" :evidence/tags [:chat :turn]
          :evidence/body "scopephrase operator turn"}
         {:xt/id "scope-assistant" :evidence/id "scope-assistant"
          :evidence/type :coordination :evidence/author "claude-8"
          :evidence/at "2026-07-31T01:01:00Z" :evidence/tags [:chat :turn]
          :evidence/body "scopephrase assistant reply"}
         {:xt/id "scope-commit" :evidence/id "scope-commit"
          :evidence/type :coordination :evidence/author "claude-8"
          :evidence/at "2026-07-31T01:02:00Z" :evidence/tags [:turn-commits]
          :evidence/body "scopephrase committed work"}
         {:xt/id "scope-artifact" :evidence/id "scope-artifact"
          :evidence/type :artifact-activation :evidence/author "artifact-activation"
          :evidence/at "2026-07-31T01:03:00Z" :evidence/tags [:artifact-activation]
          :evidence/body "scopephrase artifact derived"}
         {:xt/id "scope-start" :evidence/id "scope-start"
          :evidence/type :coordination :evidence/author "zai-1"
          :evidence/at "2026-07-31T01:04:00Z" :evidence/tags [:turn-start]
          :evidence/body "scopephrase harness start derived"}
         {:xt/id "scope-round" :evidence/id "scope-round"
          :evidence/type :coordination :evidence/author "象-1"
          :evidence/at "2026-07-31T01:05:00Z" :evidence/tags [:turn-round]
          :evidence/body "scopephrase circus round derived"}
         {:xt/id "scope-xiang" :evidence/id "scope-xiang"
          :evidence/type :memory :evidence/author "xiang-turn-service"
          :evidence/at "2026-07-31T01:06:00Z" :evidence/tags [:xiang-turn]
          :evidence/body "scopephrase settled turn derived"}
         {:xt/id "scope-ma" :evidence/id "scope-ma"
          :evidence/type :observation :evidence/author "馬"
          :evidence/at "2026-07-31T01:07:00Z" :evidence/tags [:ma-observation]
          :evidence/body "scopephrase observation derived"}]
        docs (into ordinary-docs scope-docs)]
    (with-open [node (xtn/start-node)]
      (xt/execute-tx node (mapv (fn [doc] [:put-docs :evidence doc]) docs))
      (text/init! {:path (str dir "/fts5-evidence.db")})
      (text/catch-up! node)
      (let [params {:q "common" :limit 6 :include-ephemeral false :scope :all}
            hydrated (text/search node (assoc params :hydrate true))
            projected (text/search node (assoc params :hydrate false))
            hydrated-ids (mapv #(get-in % [:entry :evidence/id])
                               (:results hydrated))
            projected-ids (mapv :evidence/id (:results projected))]
        (check! "hydrate=false preserves the hydrated id set and order"
                (= hydrated-ids projected-ids))
        (check! "hydrate=false returns no body or full :entry"
                (every? #(and (not (contains? % :entry))
                              (not (contains? % :evidence/body)))
                        (:results projected)))
        (check! "default/full mode retains the existing response shape"
                (every? #(contains? % :entry) (:results hydrated)))
        (check! "store-side ephemeral re-check is preserved"
                (not-any? #{"text-0"} projected-ids))
        ;; :ids must be checked against an INDEPENDENT route to the same fact.
        ;; The original bug shipped because it was compared against
        ;; (mapv :evidence/id results) -- its own implementation, which agrees
        ;; with itself in both modes, nils included. hydrated-ids above is
        ;; derived via [:entry :evidence/id], so it is a genuine second opinion.
        (check! ":ids is populated under hydration, the mode it exists for"
                (and (= 6 (count (:ids hydrated)))
                     (not-any? nil? (:ids hydrated))))
        (check! ":ids matches the ids reached via [:entry :evidence/id]"
                (= (vec (:ids hydrated)) hydrated-ids))
        (check! ":ids agrees across hydrate modes"
                (= (vec (:ids hydrated)) (vec (:ids projected)))))
      (let [{:keys [df indexed]} (text/document-frequencies ["common" "missing"])]
        (check! "df reports index-only per-term document frequencies"
                (= {"common" 48 "missing" 0} df))
        (check! "df reports total indexed rows" (= 56 indexed)))
      (let [first-page (text/search node {:q "common" :limit 3 :hydrate false :scope :all})
            second-page (text/search node {:q "common" :limit 2 :offset 1
                                           :hydrate false :scope :all})]
        (check! "candidate offset preserves ranked-window pagination"
                (= (subvec (mapv :evidence/id (:results first-page)) 1 3)
                   (mapv :evidence/id (:results second-page)))))
      (let [params {:q "common" :limit 20 :scope :all}
            _ (old-serial-search node params)
            _ (text/search node params)
            before (median-ms #(old-serial-search node params))
            projected (median-ms #(text/search node (assoc params :hydrate false)))
            hydrated (median-ms #(text/search node params))]
        (println
         (format
          "  throwaway benchmark limit=20 (median n=3): before=%.2fms projected=%.2fms hydrated=%.2fms"
          before projected hydrated)))
      (let [http-server (server/start-server! {:node node :store-dir dir
                                               :port 0 :health-port nil})
            port (.getPort (.getAddress http-server))
            base (str "http://127.0.0.1:" port
                      "/api/alpha/evidence/text-search")]
        (try
          (let [q (URLEncoder/encode "common" "UTF-8")
                full (get-edn (str base "?q=" q "&limit=6&scope=all"))
                light (get-edn (str base "?q=" q "&limit=6&hydrate=false&scope=all"))
                full-ids (mapv #(get-in % [:entry :evidence/id])
                               (get-in full [:body :results]))
                light-ids (mapv :evidence/id (get-in light [:body :results]))
                df (get-edn (str base "?df=common,missing"))
                bad-offset (get-edn (str base "?q=" q "&offset=10001"))]
            (check! "HTTP successful search is explicitly :ok true"
                    (true? (get-in full [:body :ok])))
            (check! "HTTP hydrate=false preserves ids and order"
                    (= full-ids light-ids))
            (check! "HTTP df mode returns frequencies without q"
                    (and (true? (get-in df [:body :ok]))
                         (= {"common" 48 "missing" 0}
                            (get-in df [:body :df]))))
            (check! "HTTP offset cap is enforced"
                    (= 400 (:status bad-offset))))
          (let [q (URLEncoder/encode "scopephrase" "UTF-8")
                acts (get-edn (str base "?q=" q "&limit=3"))
                all (get-edn (str base "?q=" q "&limit=20&scope=all"))
                act-ids (set (get-in acts [:body :ids]))
                all-ids (set (get-in all [:body :ids]))]
            (check! "default acts scope returns exactly participant acts before limit"
                    (= #{"scope-operator" "scope-assistant" "scope-commit"}
                       act-ids))
            (check! "scope=all retains today's unfiltered behaviour"
                    (= (set (map :evidence/id scope-docs)) all-ids))
            (check! "acts response names scope and rows removed"
                    (and (= :acts (get-in acts [:body :scope]))
                         (= 5 (get-in acts [:body :scope-removed]))))
            (check! "all response names scope and removes nothing"
                    (and (= :all (get-in all [:body :scope]))
                         (zero? (get-in all [:body :scope-removed]))))
            ;; The running server's route predates the scope parameter and
            ;; calls search without one; that must still mean acts.
            (check! "search with no :scope returns acts only"
                    (= #{"scope-operator" "scope-assistant" "scope-commit"}
                       (set (:ids (text/search node {:q "scopephrase" :limit 3})))))
            (check! "memory search asks for every family"
                    (re-find #":scope :all" (slurp "zai_memory_1b.clj"))))
          (let [{:keys [exit out err]}
                (shell/sh "bb" "fts_oracle.clj"
                          (str "http://127.0.0.1:" port))]
            (println out)
            (when-not (zero? exit) (println err))
            (check! "fts_oracle.clj search semantics are preserved"
                    (zero? exit)))
          (finally
            (server/stop-server! http-server))))))
  (println "TEXT SEARCH: ALL PASS")
  (shutdown-agents))
