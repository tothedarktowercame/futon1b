;; futon1b-server — the second JVM (approved by Joe 2026-07-10): one XTDB 2
;; node owning the futon1b store, serving the ingestion + Zai-memory seam
;; over HTTP/EDN on :7073.
;;
;; Why a server: the futon3c JVM embeds futon1a (XTDB 1.24.0) under the SAME
;; Maven coordinates futon1b needs at 2.0.0, so XTDB 2 can never load there —
;; every consumer reaches this store out-of-process. XTDB 2 local stores are
;; single-process: while this server runs, no dev JVM may open --store-dir.
;;
;; Endpoints (EDN in, EDN out — no JSON dependency):
;;   GET  /health                     → cheap liveness {:ok true}
;;        /health?deep=true           → explicit full table census
;;   POST /api/alpha/hyperedge        → watcher-compatible payload:
;;        {:hx/type <kw-or-str> :hx/endpoints [id...] :hx/id <opt>
;;         :hx/labels [...] :hx/props {...} :hx/op "retract" <opt>}
;;        Stable id mirrors futon1a (hx:<type>:<sorted-endpoints>). Plain
;;        puts get the no-op guard (identical doc → skip, {:no-op? true})
;;        and a VERIFIED put — XTDB 2.0.0 batch puts can drop rows silently
;;        (E-futon1a-to-futon1b 2026-07-10), so every put is point-read
;;        back and escalated through the rescue ladder if absent.
;;   GET  /api/alpha/memory/search    → §12.3 envelope via zai-memory-1b;
;;        params: type author since tags (comma-sep) limit.
;;   POST /api/alpha/evidence + GET /{id} /{id}/chain /count /sessions and
;;        GET /api/alpha/evidence?… — API-CONTRACT.md §3 (A1, operational
;;        switchover). Writes gated by penholder (futon1b-gates, A2).
;;
;; Run: cd /home/joe/code/futon1b && \
;;      clojure -M:node -m futon1b-server --store-dir migration-store --port 7073
;;      (lucy: --port 7074 — nginx owns :7073 there)
(ns futon1b-server
  (:require [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [migration.transform :as xf]
            [migration.ingest :as ingest]
            [futon1b-gates :as gates]
            [futon1b-evidence :as ev]
            [futon1b-graph :as graph]
            [zai-memory-1b :as zm]
            [futon1b-xt :as fxt]
            [futon1b-text :as text]
            [xtdb.api :as xt])
  (:import [com.sun.net.httpserver HttpServer HttpHandler HttpExchange]
           [java.net InetSocketAddress URLDecoder]
           [java.nio.charset StandardCharsets])
  (:gen-class))

(defonce !node (atom nil))

;; ---------------------------------------------------------------------------
;; Hyperedge write path.
;; ---------------------------------------------------------------------------

(defn stable-hyperedge-id
  "Mirror of futon1a.compat.futon1-write/stable-hyperedge-id so the reindex
  produces the SAME ids futon1a would."
  [hx-type endpoint-ids]
  (str "hx:"
       (if (keyword? hx-type) (subs (str hx-type) 1) (str hx-type))
       ":"
       (str/join "." (sort (map str endpoint-ids)))))

(defn- normalize-type [t]
  (cond (keyword? t) t
        (string? t) (keyword t)
        :else (throw (ex-info "hx/type required" {:got t}))))

(defn build-hyperedge-doc
  "Watcher payload → futon1b doc (pre-transform)."
  [payload]
  (let [hx-type (normalize-type (or (:hx/type payload) (:type payload)))
        endpoints (mapv str (or (:hx/endpoints payload) (:endpoints payload)))
        _ (when (empty? endpoints)
            (throw (ex-info "hx/endpoints must be non-empty" {:payload payload})))
        hx-id (or (:hx/id payload) (:id payload)
                  (stable-hyperedge-id hx-type endpoints))
        labels (some->> (or (:hx/labels payload) (:labels payload))
                        (mapv normalize-type))]
    (cond-> {:xt/id hx-id
             :hx/id hx-id
             :hx/type hx-type
             :hx/endpoints endpoints
             :hx/ends (mapv (fn [e] {:entity-id e}) endpoints)}
      (seq labels) (assoc :hx/labels labels)
      (:hx/props payload) (assoc :hx/props (:hx/props payload))
      (:props payload) (assoc :hx/props (:props payload)))))

(defn fetch-current [node id]
  (fxt/q1 node (list '-> '(from :hyperedges [*])
                     (list 'where (list '= 'xt/id id)))))

(defn- present? [node id]
  (fxt/present? node :hyperedges id))

(defn upsert-hyperedge!
  "Transform + no-op guard + VERIFIED put. Returns response map."
  [node payload]
  (let [retract? (= "retract" (some-> (or (:hx/op payload) (:op payload))
                                      name str/lower-case))
        doc (xf/transform-doc (build-hyperedge-doc payload))
        id (:xt/id doc)]
    (if retract?
      (do (xt/execute-tx node [[:delete-docs :hyperedges id]])
          (graph/invalidate-hyperedge-query-cache!)
          {:ok true :hx/id id :retracted? true})
      ;; No-op guard: compare against stored (project the doc's own keys —
      ;; [*] binds nothing in XTDB 2.0.0).
      (let [stored (first (fxt/safe-q node
                                (list '-> (list 'from :hyperedges
                                                (into ['xt/id]
                                                      (comp (remove #{:xt/id})
                                                            (map #(symbol (namespace %) (name %))))
                                                      (keys doc)))
                                      (list 'where (list '= 'xt/id id)))))
            ;; nil-valued keys are stored as absent (XTDB 2 semantics)
            doc-cmp (into {} (filter (comp some? val)) doc)
            stored-cmp (when stored (into {} (filter (comp some? val)) stored))]
        (if (= doc-cmp stored-cmp)
          {:ok true :hx/id id :no-op? true}
          (let [res (ingest/put-doc-with-rescue! node :hyperedges doc nil)]
            (if (present? node id)
              (do (graph/invalidate-hyperedge-query-cache!)
                  {:ok true :hx/id id :rescue (when (keyword? res) res)})
              {:ok false :hx/id id
               :error "verified put: doc absent after rescue ladder"})))))))

;; ---------------------------------------------------------------------------
;; HTTP plumbing.
;; ---------------------------------------------------------------------------

(defn- read-body [^HttpExchange ex]
  (slurp (.getRequestBody ex)))

(defn- json-request? [^HttpExchange ex]
  (some-> (.getFirst (.getRequestHeaders ex) "Content-Type")
          str/lower-case
          (str/includes? "application/json")))

(defn- wants-json? [^HttpExchange ex]
  ;; futon1a app.clj:47-58 — JSON response when Accept OR Content-Type
  ;; mentions application/json.
  (boolean
   (some #(some-> (.getFirst (.getRequestHeaders ex) %)
                  str/lower-case
                  (str/includes? "application/json"))
         ["Accept" "Content-Type"])))

(defn- parse-payload
  "API-CONTRACT §0: JSON bodies (watchers!) decode with keywordized keys —
  \"hx/type\" becomes :hx/type, so JSON and EDN hit the same code paths.
  Otherwise EDN. Empty body -> {}."
  [^HttpExchange ex]
  (let [body (read-body ex)]
    (cond
      (str/blank? body) {}
      (json-request? ex) (json/parse-string body keyword)
      :else (edn/read-string body))))

(defn- respond-str! [^HttpExchange ex status ^String body ^String ctype]
  (let [bytes (.getBytes body StandardCharsets/UTF_8)]
    (.add (.getResponseHeaders ex) "Content-Type" ctype)
    (.sendResponseHeaders ex status (alength bytes))
    (with-open [os (.getResponseBody ex)]
      (.write os bytes))))

(def ^:dynamic *json-response?* false)

(defn- respond!
  "EDN by default; JSON when the request asked for it (contract §0).
  Accepts the response VALUE (map) — encoding happens here."
  [^HttpExchange ex status body]
  (let [v (if (string? body) (edn/read-string body) body)]
    (if *json-response?*
      (respond-str! ex status (json/generate-string v {:escape-non-ascii true})
                    "application/json; charset=utf-8")
      (respond-str! ex status (pr-str v) "application/edn; charset=utf-8"))))

(defn- query-params [^HttpExchange ex]
  (into {}
        (keep (fn [kv]
                (let [[k v] (str/split kv #"=" 2)]
                  (when (and k v)
                    [k (URLDecoder/decode v "UTF-8")]))))
        (some-> (.getQuery (.getRequestURI ex)) (str/split #"&"))))

(defn- handler ^HttpHandler [route-fn]
  (reify HttpHandler
    (handle [_ ex]
      (let [started (System/nanoTime)
            method (.getRequestMethod ex)
            uri (str (.getRequestURI ex))]
        (println (format "[futon1b-request] start method=%s uri=%s" method uri))
        (flush)
        (binding [*json-response?* (wants-json? ex)]
          (try
            (route-fn ex)
            (println (format "[futon1b-request] end method=%s uri=%s elapsed-ms=%d outcome=ok"
                             method uri (quot (- (System/nanoTime) started) 1000000)))
            (catch Exception e
              ;; Layered gate errors keep futon1a's envelope + status mapping
              ;; (API-CONTRACT §0); anything else is the generic 500.
              (let [[status body] (gates/error->response e)]
                (println (format "[futon1b-request] end method=%s uri=%s elapsed-ms=%d outcome=error status=%d class=%s"
                                 method uri (quot (- (System/nanoTime) started) 1000000)
                                 status (.getName (class e))))
                (respond! ex status body)))
            (finally (flush))))))))

(defn- penholder!
  "Resolve (body → x-penholder header → default) and authorize (L3)."
  [^HttpExchange ex payload]
  (gates/authorize!
   (gates/resolve-penholder payload
                            (.getFirst (.getRequestHeaders ex) "x-penholder"))))

(defn- uri-tail
  "Path remainder after prefix, leading slash stripped, URL-decoded."
  [^HttpExchange ex prefix]
  (let [path (.getPath (.getRequestURI ex))
        tail (subs path (min (count prefix) (count path)))]
    (URLDecoder/decode (if (str/starts-with? tail "/") (subs tail 1) tail)
                       "UTF-8")))

(defn- parse-limit [p]
  (some-> (p "limit") (as-> s (try (Long/parseLong s) (catch Exception _ nil)))))

(def ^:private tables
  [:hyperedges :entities :evidence :relations :type-catalog :docs :misc])

(defn- health [^HttpExchange ex]
  ;; Liveness must never materialize or count the corpus. Operators can request
  ;; the expensive census explicitly while profiling with ?deep=true.
  (if (= "true" ((query-params ex) "deep"))
    (let [node @!node
          counts (into {}
                       (for [t tables]
                         [t (count (fxt/safe-q node (list 'from t ['xt/id])))]))]
      (respond! ex 200 (pr-str {:ok true :deep true :tables counts})))
    (respond! ex 200 (pr-str {:ok true :deep false :node-open? (some? @!node)}))))

(defn- hyperedge-route [^HttpExchange ex]
  (let [method (.getRequestMethod ex)
        tail (uri-tail ex "/api/alpha/hyperedge")]
    (cond
      (and (= method "POST") (str/blank? tail))
      (let [payload (parse-payload ex)
            _ (penholder! ex payload)
            res (upsert-hyperedge! @!node payload)]
        (respond! ex (if (:ok res) 200 500) (pr-str res)))

      (and (= method "GET") (not (str/blank? tail)))
      (if-let [doc (graph/hyperedge-by-id @!node tail)]
        (respond! ex 200 (pr-str doc))
        (respond! ex 404 (pr-str {:error "not found" :hx/id tail})))

      :else
      (respond! ex 405 (pr-str {:ok false :error "POST (write) or GET /{id}"})))))

(defn- evidence-route
  "Dispatch everything under /api/alpha/evidence (API-CONTRACT §3).
  Subpath routing mirrors futon1a's match order: sessions and count
  before the generic {id} prefix; {id}/chain by suffix."
  [^HttpExchange ex]
  (let [method (.getRequestMethod ex)
        path (.getPath (.getRequestURI ex))
        tail (subs path (min (count "/api/alpha/evidence") (count path)))
        tail (if (str/starts-with? tail "/") (subs tail 1) tail)]
    (cond
      (and (= method "POST") (str/blank? tail))
      (let [payload (parse-payload ex)
            _ (penholder! ex payload)
            [status body] (ev/write-evidence! @!node payload)]
        (respond! ex status (pr-str body)))

      (not= method "GET")
      (respond! ex 405 (pr-str {:ok false :error "GET/POST only"}))

      (str/blank? tail)
      (respond! ex 200 (pr-str (ev/query-evidence @!node (query-params ex))))

      (= tail "count")
      (respond! ex 200 (pr-str (ev/count-evidence @!node (query-params ex))))

      (= tail "sessions")
      (respond! ex 200 (pr-str (ev/sessions @!node (query-params ex))))

      (str/ends-with? tail "/chain")
      (let [id (subs tail 0 (- (count tail) (count "/chain")))]
        (respond! ex 200 (pr-str (ev/chain @!node id))))

      :else
      (if-let [doc (ev/fetch-by-id @!node tail)]
        (if (:evidence/id doc)
          (respond! ex 200 (pr-str (dissoc doc :xt/id)))
          (respond! ex 404 (pr-str {:error "not found" :evidence/id tail})))
        (respond! ex 404 (pr-str {:error "not found" :evidence/id tail}))))))

(defn- entity-route [^HttpExchange ex]
  (let [method (.getRequestMethod ex)
        tail (uri-tail ex "/api/alpha/entity")
        p (query-params ex)]
    (cond
      (and (= method "POST") (str/blank? tail))
      (let [payload (parse-payload ex)
            _ (penholder! ex payload)
            res (graph/write-entity! @!node payload)]
        (respond! ex 200 (pr-str res)))

      (not= method "GET")
      (respond! ex 405 (pr-str {:ok false :error "GET/POST only"}))

      (str/blank? tail)
      (let [[status body] (graph/entity-by-external
                           @!node {:source (p "source") :external-id (p "external-id")})]
        (respond! ex status (pr-str body)))

      :else
      (if-let [doc (graph/fetch-entity @!node tail)]
        (respond! ex 200 (pr-str {:profile "default"
                                  :entity (graph/public-entity doc)}))
        (respond! ex 404 (pr-str {:error "Entity not found"
                                  :profile "default" :entity-id tail}))))))

(defn- entities-latest-route [^HttpExchange ex]
  (let [p (query-params ex)]
    (when-not (p "type")
      (throw (gates/layered-error 4 :missing-required {:required ["type"]})))
    (respond! ex 200 (pr-str (graph/entities-latest
                              @!node {:type (p "type") :limit (parse-limit p)})))))

(defn- entities-route [^HttpExchange ex]
  (let [p (query-params ex)]
    (if (p "type")
      (respond! ex 200 (pr-str (graph/entities-query
                                @!node {:type (p "type")
                                        :limit (parse-limit p)})))
      (respond! ex 400 (pr-str {:error "entities requires ?type=<entity-type>"})))))

(defn- relation-route [^HttpExchange ex]
  (if (= "POST" (.getRequestMethod ex))
    (let [payload (parse-payload ex)
          _ (penholder! ex payload)
          res (graph/write-relation! @!node payload)]
      (respond! ex 200 (pr-str res)))
    (respond! ex 405 (pr-str {:ok false :error "POST only"}))))

(defn- relations-route [^HttpExchange ex]
  (let [p (query-params ex)]
    (if (or (p "type") (p "types") (p "from") (p "to"))
      (respond! ex 200 (pr-str (graph/relations-query
                                @!node {:type (p "type")
                                        :types (some-> (p "types")
                                                       (str/split #","))
                                        :from (p "from")
                                        :to (p "to")
                                        :limit (parse-limit p)
                                        :hydrate? (= "true" (p "hydrate"))})))
      (respond! ex 400
                (pr-str {:error "relations requires type, from, or to"})))))

(defn- graph-inhabited-route [^HttpExchange ex]
  (if (= "POST" (.getRequestMethod ex))
    (let [payload (parse-payload ex)
          bindings (:bindings payload)]
      (if (vector? bindings)
        (respond! ex 200
                  (pr-str {:bindings (graph/inhabitation @!node bindings)}))
        (respond! ex 400 (pr-str {:error "vector :bindings required"}))))
    (respond! ex 405 (pr-str {:ok false :error "POST only"}))))

(defn- hyperedges-route [^HttpExchange ex]
  (let [p (query-params ex)]
    (if (or (p "type") (p "end"))
      (respond! ex 200 (pr-str (graph/hyperedges-query
                                @!node {:type (p "type") :end (p "end")
                                        :limit (parse-limit p)
                                        :repo (p "repo")
                                        :source-file (p "source-file")
                                        :include-total? (not= "false" (p "include-total"))
                                        :latest? (= "true" (p "latest"))})))
      (respond! ex 400 (pr-str {:error "type or end parameter required"})))))

(defn- census-route [^HttpExchange ex]
  (let [p (query-params ex)]
    (if (or (p "type") (p "entity-type"))
      (respond! ex 200 (pr-str (graph/census @!node {:type (p "type")
                                                     :entity-type (p "entity-type")})))
      (respond! ex 400
                (pr-str {:error "census requires ?type=<hx-type> or ?entity-type=<type>"})))))

(defn- types-route [^HttpExchange ex]
  (let [method (.getRequestMethod ex)
        tail (uri-tail ex "/api/alpha/types")]
    (cond
      (and (= method "GET") (str/blank? tail))
      (respond! ex 200 (pr-str (graph/list-types @!node)))

      (and (= method "POST") (#{"parent" "merge"} tail))
      (let [payload (parse-payload ex)]
        (respond! ex 200 (pr-str (graph/types-mutate! @!node (keyword tail) payload))))

      :else
      (respond! ex 404 (pr-str {:error {:reason :not-found :path (str "/types/" tail)}})))))

(defn- memory-search-route [^HttpExchange ex]
  (let [p (query-params ex)
        opts (cond-> {}
               (p "type")   (assoc :type (keyword (p "type")))
               (p "author") (assoc :author (p "author"))
               (p "since")  (assoc :since (p "since"))
               (p "tags")   (assoc :tags (mapv keyword (str/split (p "tags") #",")))
               (p "text")   (assoc :text (p "text"))
               (p "limit")  (assoc :limit (Long/parseLong (p "limit"))))]
    (respond! ex 200 (pr-str (zm/memory-search @!node opts)))))

(defn- text-search-route
  "GET /api/alpha/evidence/text-search?q=…&author=&session-id=&since=&before=
   &include-ephemeral=&limit= — D1 sidecar search (candidates + re-check).
   ?stats=true returns index stats instead. POST {:op :catch-up} (penholder-
   gated) starts a background catch-up/rebuild."
  [^HttpExchange ex]
  (let [method (.getRequestMethod ex)
        p (query-params ex)]
    (cond
      (= method "POST")
      (let [payload (parse-payload ex)
            _ (penholder! ex payload)]
        (future
          (try (println "[fts] catch-up:" (pr-str (text/catch-up! @!node)))
               (catch Throwable t (println "[fts] catch-up failed:" (.getMessage t)))))
        (respond! ex 202 (pr-str {:ok true :started :catch-up})))

      (not= method "GET")
      (respond! ex 405 (pr-str {:ok false :error "GET (search) or POST (catch-up)"}))

      (= "true" (p "stats"))
      (respond! ex 200 (pr-str (text/stats)))

      (str/blank? (str (p "q")))
      (respond! ex 400 (pr-str {:error "q parameter required"}))

      :else
      (respond! ex 200
                (pr-str (text/search @!node
                                     (cond-> {:q (p "q")}
                                       (p "author") (assoc :author (p "author"))
                                       (p "session-id") (assoc :session-id (p "session-id"))
                                       (p "since") (assoc :since (p "since"))
                                       (p "before") (assoc :before (p "before"))
                                       (p "include-ephemeral")
                                       (assoc :include-ephemeral
                                              (= "true" (str/lower-case (p "include-ephemeral"))))
                                       (parse-limit p) (assoc :limit (parse-limit p)))))))))

(defonce ^:private !server-executors (atom {}))

(defn stop-server!
  "Stop a server returned by start-server! and release its worker threads."
  [^HttpServer server]
  (.stop server 0)
  (when-let [executor (get @!server-executors server)]
    (swap! !server-executors dissoc server)
    (.shutdownNow ^java.util.concurrent.ExecutorService executor))
  nil)

(defn start-server! [{:keys [store-dir port node]}]
  (gates/seed-mission-contract!)
  (reset! !node (or node (zm/open-store store-dir)))
  ;; D1 sidecar: open/create the FTS5 db beside the store, then catch up in
  ;; the background (first boot = full build; steady state = the tail since
  ;; last-at). Failures here must not block serving.
  (try
    (let [{:keys [path last-at]} (text/init! {:store-dir store-dir})]
      (println (format "[fts] sidecar at %s (last-at %s)" path (or last-at "none — full build")))
      (doto (Thread. (fn []
                       (try (println "[fts] catch-up:" (pr-str (text/catch-up! @!node)))
                            (catch Throwable t
                              (println "[fts] catch-up failed:" (.getMessage t))))))
        (.setDaemon true)
        (.start)))
    (catch Throwable t
      (println "[fts] init failed (serving continues):" (.getMessage t))))
  (let [server (HttpServer/create (InetSocketAddress. (int port)) 0)
        executor (java.util.concurrent.Executors/newFixedThreadPool 4)]
    (.createContext server "/health" (handler health))
    ;; NB JDK HttpServer matches contexts by raw string prefix — the longer
    ;; /api/alpha/hyperedges context must be registered so it wins over
    ;; /api/alpha/hyperedge for the plural query route (same for entities).
    (.createContext server "/api/alpha/hyperedge" (handler hyperedge-route))
    (.createContext server "/api/alpha/hyperedges" (handler hyperedges-route))
    (.createContext server "/api/alpha/evidence" (handler evidence-route))
    ;; longer prefix wins (see NB above): text-search must out-rank /evidence
    (.createContext server "/api/alpha/evidence/text-search" (handler text-search-route))
    (.createContext server "/api/alpha/entity" (handler entity-route))
    (.createContext server "/api/alpha/entities" (handler entities-route))
    (.createContext server "/api/alpha/entities/latest" (handler entities-latest-route))
    (.createContext server "/api/alpha/relation" (handler relation-route))
    (.createContext server "/api/alpha/relations" (handler relations-route))
    (.createContext server "/api/alpha/graph/inhabited" (handler graph-inhabited-route))
    (.createContext server "/api/alpha/census" (handler census-route))
    (.createContext server "/api/alpha/types" (handler types-route))
    (.createContext server "/api/alpha/memory/search" (handler memory-search-route))
    (.setExecutor server executor)
    (swap! !server-executors assoc server executor)
    (.start server)
    (println (format "futon1b-server up on :%d (store %s)" port store-dir))
    server))

(defn- parse-args [args]
  (loop [args args opts {:store-dir "migration-store" :port 7073}]
    (if-not (seq args)
      opts
      (case (first args)
        "--store-dir" (recur (nnext args) (assoc opts :store-dir (second args)))
        "--port"      (recur (nnext args) (assoc opts :port (Long/parseLong (second args))))
        (throw (ex-info (str "Unknown arg: " (first args)) {:args args}))))))

(defn -main [& args]
  (start-server! (parse-args args))
  ;; block forever — the server owns this JVM.
  @(promise))
