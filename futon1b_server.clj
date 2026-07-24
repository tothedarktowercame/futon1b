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
;;   POST /api/alpha/documents/retract → atomic, gated entity/hyperedge
;;        retraction with validation before write and read-back verification.
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
           [java.nio.charset StandardCharsets]
           [java.time Instant]
           [java.util.concurrent ArrayBlockingQueue Semaphore ThreadPoolExecutor
            ThreadPoolExecutor$AbortPolicy TimeUnit])
  (:gen-class))

(defonce !node (atom nil))

(declare parse-instant)

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
        valid-from (some-> (or (:hx/valid-time payload) (:valid-time payload))
                           parse-instant)
        doc (xf/transform-doc (build-hyperedge-doc payload))
        id (:xt/id doc)]
    (letfn [(mutate! []
              (if retract?
                (do (xt/execute-tx node [[:delete-docs
                                          (cond-> {:from :hyperedges}
                                            valid-from
                                            (assoc :valid-from valid-from))
                                          id]])
                    (graph/invalidate-hyperedge-query-cache!)
                    (when (= :memory/assert (:hx/type doc))
                      (graph/refresh-memory-projection-component! node id))
                    {:ok true :hx/id id :retracted? true})
                ;; No-op guard: compare against stored (project the doc's own
                ;; keys — [*] binds nothing in XTDB 2.0.0).
                (let [stored
                      (first
                       (fxt/safe-q
                        node
                        (list '->
                              (list 'from :hyperedges
                                    (into ['xt/id]
                                          (comp
                                           (remove #{:xt/id})
                                           (map #(symbol (namespace %)
                                                         (name %))))
                                          (keys doc)))
                              (list 'where (list '= 'xt/id id)))))
                      ;; nil-valued keys are stored as absent (XTDB 2 semantics)
                      doc-cmp (into {} (filter (comp some? val)) doc)
                      stored-cmp
                      (when stored
                        (into {} (filter (comp some? val)) stored))]
                  (if (and (nil? valid-from) (= doc-cmp stored-cmp))
                    {:ok true :hx/id id :no-op? true}
                    (let [res
                          (ingest/put-doc-with-rescue!
                           node :hyperedges doc nil valid-from)]
                      (if (present? node id)
                        (do
                          (graph/invalidate-hyperedge-query-cache!)
                          (when (= :memory/assert (:hx/type doc))
                            (graph/refresh-memory-projection-component! node id))
                          {:ok true :hx/id id
                           :rescue (when (keyword? res) res)})
                        {:ok false :hx/id id
                         :error
                         "verified put: doc absent after rescue ladder"}))))))]
      (if (= :memory/assert (:hx/type doc))
        (graph/with-memory-projection-mutation node mutate!)
        (mutate!)))))

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

(defn- parse-instant
  [value]
  (when (some? value)
    (try
      (cond
        (instance? Instant value) value
        (number? value) (Instant/ofEpochMilli (long value))
        (re-matches #"-?\d+" (str value))
        (Instant/ofEpochMilli (Long/parseLong (str value)))
        :else (Instant/parse (str value)))
      (catch Throwable t
        (throw (ex-info "invalid temporal instant"
                        {:value value :expected :epoch-millis-or-iso-8601}
                        t))))))

(defn- request-trace-id
  [^HttpExchange ex]
  (some-> (.getFirst (.getRequestHeaders ex) "x-trace-id")
          (str/replace #"[^A-Za-z0-9._:-]" "_")
          (#(subs % 0 (min 128 (count %))))
          not-empty))

(defn- handler ^HttpHandler [route-fn]
  (reify HttpHandler
    (handle [_ ex]
      (let [started (System/nanoTime)
            method (.getRequestMethod ex)
            uri (str (.getRequestURI ex))
            trace-id (or (request-trace-id ex) "-")]
        (println (format "[futon1b-request] start method=%s uri=%s trace-id=%s"
                         method uri trace-id))
        (flush)
        (binding [*json-response?* (wants-json? ex)]
          (try
            (route-fn ex)
            (println (format "[futon1b-request] end method=%s uri=%s trace-id=%s elapsed-ms=%d outcome=ok"
                             method uri trace-id
                             (quot (- (System/nanoTime) started) 1000000)))
            (catch java.io.IOException e
              ;; A client may abandon a slow request before its response is
              ;; ready. The response write then throws; attempting a second
              ;; 500 response on the same closed exchange only throws again.
              (println (format "[futon1b-request] end method=%s uri=%s trace-id=%s elapsed-ms=%d outcome=client-disconnected class=%s"
                               method uri trace-id
                               (quot (- (System/nanoTime) started) 1000000)
                               (.getName (class e))))
              (.close ^HttpExchange ex))
            (catch Exception e
              ;; Layered gate errors keep futon1a's envelope + status mapping
              ;; (API-CONTRACT §0); anything else is the generic 500.
              (let [[status body] (gates/error->response e)]
                (println (format "[futon1b-request] end method=%s uri=%s trace-id=%s elapsed-ms=%d outcome=error status=%d class=%s message=%s"
                                 method uri trace-id
                                 (quot (- (System/nanoTime) started) 1000000)
                                 status (.getName (class e)) (pr-str (.getMessage e))))
                (try
                  (respond! ex status body)
                  (catch java.io.IOException write-error
                    (println (format "[futon1b-request] error-response-abandoned method=%s uri=%s trace-id=%s class=%s"
                                     method uri trace-id
                                     (.getName (class write-error))))
                    (.close ^HttpExchange ex)))))
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

(def ^:private default-result-limit 100)
(def ^:private max-result-limit 5000)

(defn- parse-limit
  "Validated server-side result bound for list/search endpoints."
  [p]
  (let [raw (p "limit")
        parsed (if raw
                 (try (Long/parseLong raw) (catch Exception _ ::invalid))
                 default-result-limit)]
    (when (or (= ::invalid parsed)
              (not (pos-int? parsed))
              (> parsed max-result-limit))
      (throw (gates/layered-error
              4 :invalid-limit
              {:provided raw :minimum 1 :maximum max-result-limit})))
    parsed))

(defonce ^:private expensive-read-permit
  ;; Corpus scans are admitted globally, not once per route or request. Two
  ;; scans at a time leave two HTTP workers available for writes, point reads,
  ;; and liveness. Contending scans fail fast with a retryable 503.
  (Semaphore. 2 true))

(defn- with-expensive-read!
  [^HttpExchange ex f]
  (if (.tryAcquire expensive-read-permit)
    (try
      (f)
      (finally (.release expensive-read-permit)))
    (do
      (.set (.getResponseHeaders ex) "Retry-After" "1")
      (println (format "[futon1b-admission] rejected uri=%s reason=expensive-read-busy"
                       (str (.getRequestURI ex))))
      (respond! ex 503 (pr-str {:ok false
                                :error :expensive-read-busy
                                :retry-after-seconds 1})))))

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
      (with-expensive-read!
        ex
        (fn []
          (let [[status body]
                (ev/query-evidence-response @!node (query-params ex))]
            (respond! ex status (pr-str body)))))

      (= tail "count")
      (with-expensive-read!
        ex #(respond! ex 200 (pr-str (ev/count-evidence @!node (query-params ex)))))

      (= tail "sessions")
      (with-expensive-read!
        ex #(respond! ex 200 (pr-str (ev/sessions @!node (query-params ex)))))

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

(defn- documents-retract-route [^HttpExchange ex]
  (if (= "POST" (.getRequestMethod ex))
    (let [payload (parse-payload ex)
          _ (penholder! ex payload)]
      (respond! ex 200 (pr-str (graph/retract-documents! @!node payload))))
    (respond! ex 405 (pr-str {:ok false :error "POST only"}))))

(defn- entities-latest-route [^HttpExchange ex]
  (let [p (query-params ex)]
    (when-not (p "type")
      (throw (gates/layered-error 4 :missing-required {:required ["type"]})))
    (with-expensive-read!
      ex #(respond! ex 200 (pr-str (graph/entities-latest
                                    @!node {:type (p "type")
                                            :limit (parse-limit p)}))))))

(defn- entities-route [^HttpExchange ex]
  (let [p (query-params ex)]
    (if (p "type")
      (with-expensive-read!
        ex #(respond! ex 200 (pr-str (graph/entities-query
                                      @!node {:type (p "type")
                                              :limit (parse-limit p)}))))
      (respond! ex 400 (pr-str {:error "entities requires ?type=<entity-type>"})))))

(defn- relation-route [^HttpExchange ex]
  (if (= "POST" (.getRequestMethod ex))
    (let [payload (parse-payload ex)
          _ (penholder! ex payload)
          res (graph/write-relation! @!node payload)]
      (respond! ex 200 (pr-str res)))
    (respond! ex 405 (pr-str {:ok false :error "POST only"}))))

(defn- relations-batch-route [^HttpExchange ex]
  (if (= "POST" (.getRequestMethod ex))
    (let [payload (parse-payload ex)
          _ (penholder! ex payload)
          res (graph/write-relations-batch! @!node payload)]
      (respond! ex 200 (pr-str res)))
    (respond! ex 405 (pr-str {:ok false :error "POST only"}))))

(defn- relations-route [^HttpExchange ex]
  (let [p (query-params ex)]
    (if (or (p "type") (p "types") (p "from") (p "to"))
      (with-expensive-read!
        ex #(respond! ex 200 (pr-str (graph/relations-query
                                      @!node {:type (p "type")
                                              :types (some-> (p "types")
                                                             (str/split #","))
                                              :from (p "from")
                                              :to (p "to")
                                              :limit (parse-limit p)
                                              :hydrate? (= "true" (p "hydrate"))}))))
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
      (with-expensive-read!
        ex #(respond! ex 200 (pr-str (graph/hyperedges-query
                                      @!node {:type (p "type") :end (p "end")
                                              :limit (parse-limit p)
                                              :repo (p "repo")
                                              :source-file (p "source-file")
                                              :valid-as-of
                                              (parse-instant
                                               (or (p "valid-as-of")
                                                   (p "as-of")))
                                              :system-as-of
                                              (parse-instant (p "system-as-of"))
                                              :include-total? (not= "false" (p "include-total"))
                                              :latest? (= "true" (p "latest"))}))))
      (respond! ex 400 (pr-str {:error "type or end parameter required"})))))

(defn- census-route [^HttpExchange ex]
  (let [p (query-params ex)]
    (if (or (p "type") (p "entity-type"))
      (with-expensive-read!
        ex #(respond! ex 200 (pr-str (graph/census
                                      @!node {:type (p "type")
                                              :entity-type (p "entity-type")}))))
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
        opts (cond-> {:limit (parse-limit p)}
               (p "type")   (assoc :type (keyword (p "type")))
               (p "author") (assoc :author (p "author"))
               (p "since")  (assoc :since (p "since"))
               (p "tags")   (assoc :tags (mapv keyword (str/split (p "tags") #",")))
               (p "text")   (assoc :text (p "text")))]
    (with-expensive-read!
      ex #(respond! ex 200 (pr-str (zm/memory-search @!node opts))))))

(defn- respond-memory-projection!
  [^HttpExchange ex opts]
  (respond! ex 200
            (pr-str
             (cond-> (graph/memory-projection-components @!node opts)
               (request-trace-id ex)
               (assoc :trace-id (request-trace-id ex))))))

(defn- memory-projection-route
  "POST a bounded read-only multi-endpoint memory projection.

  This is POST because the bounded endpoint vector and temporal basis are a
  structured query, not because it mutates the store."
  [^HttpExchange ex]
  (if (= "POST" (.getRequestMethod ex))
    (let [payload (parse-payload ex)
          opts {:endpoints (:endpoints payload)
                :limit (or (:limit payload) 10)
                :valid-as-of
                (parse-instant (or (:valid-as-of payload)
                                   (:as-of payload)))
                :system-as-of
                (parse-instant (:system-as-of payload))}]
      ;; Current projection is a synchronously maintained in-memory index,
      ;; not a corpus scan. It must remain available while the heavyweight
      ;; WM census holds both expensive-read permits. Explicit bitemporal
      ;; projection still reaches XTDB and therefore retains admission.
      (if (or (:valid-as-of opts) (:system-as-of opts))
        (with-expensive-read! ex #(respond-memory-projection! ex opts))
        (respond-memory-projection! ex opts)))
    (respond! ex 405 (pr-str {:ok false :error "POST only"}))))

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
      (with-expensive-read!
        ex
        #(respond! ex 200
                   (pr-str (text/search @!node
                                        (cond-> {:q (p "q")
                                                 :limit (parse-limit p)}
                                          (p "author") (assoc :author (p "author"))
                                          (p "session-id") (assoc :session-id (p "session-id"))
                                          (p "since") (assoc :since (p "since"))
                                          (p "before") (assoc :before (p "before"))
                                          (p "include-ephemeral")
                                          (assoc :include-ephemeral
                                                 (= "true" (str/lower-case
                                                            (p "include-ephemeral"))))))))))))

(defonce ^:private !server-executors (atom {}))

(defn- bounded-executor
  "A fixed worker pool with a finite handoff queue. JDK HttpServer otherwise
  feeds Executors/newFixedThreadPool's unbounded queue: timed-out clients can
  accumulate thousands of Exchange objects until the dispatcher itself OOMs."
  [threads queue-capacity]
  (ThreadPoolExecutor. threads threads 0 TimeUnit/MILLISECONDS
                       (ArrayBlockingQueue. queue-capacity)
                       (ThreadPoolExecutor$AbortPolicy.)))

(defn stop-server!
  "Stop a server returned by start-server! and release its worker threads."
  [^HttpServer server]
  (.stop server 0)
  (when-let [{:keys [executor companions]} (get @!server-executors server)]
    (swap! !server-executors dissoc server)
    (.shutdownNow ^java.util.concurrent.ExecutorService executor)
    (doseq [{companion-server :server companion-executor :executor} companions]
      (.stop ^HttpServer companion-server 0)
      (.shutdownNow ^java.util.concurrent.ExecutorService companion-executor)))
  nil)

(defn start-server! [{:keys [store-dir port health-port node]}]
  (gates/seed-mission-contract!)
  (reset! !node (or node (zm/open-store store-dir)))
  ;; Build the coherent current-memory projection before accepting traffic.
  ;; A failed/bounded build is a startup failure, never a partially warm route.
  (println "[memory-projection] current index"
           (pr-str (graph/initialize-memory-projection! @!node)))
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
  (let [server (HttpServer/create (InetSocketAddress. (int port)) 50)
        executor (bounded-executor 4 16)
        health-server (when health-port
                        (HttpServer/create (InetSocketAddress. (int health-port)) 8))
        health-executor (when health-server (bounded-executor 1 4))]
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
    (.createContext server "/api/alpha/documents/retract" (handler documents-retract-route))
    (.createContext server "/api/alpha/relation" (handler relation-route))
    (.createContext server "/api/alpha/relations" (handler relations-route))
    ;; longer prefix wins (see NB above): batch must out-rank /relations
    (.createContext server "/api/alpha/relations/batch" (handler relations-batch-route))
    (.createContext server "/api/alpha/graph/inhabited" (handler graph-inhabited-route))
    (.createContext server "/api/alpha/census" (handler census-route))
    (.createContext server "/api/alpha/types" (handler types-route))
    (.createContext server "/api/alpha/memory/search" (handler memory-search-route))
    (.createContext server "/api/alpha/memory/projection"
                    (handler memory-projection-route))
    (.setExecutor server executor)
    (when health-server
      (.createContext health-server "/health" (handler health))
      (.setExecutor health-server health-executor))
    (swap! !server-executors assoc server
           {:executor executor
            :companions (cond-> []
                          health-server
                          (conj {:server health-server :executor health-executor}))})
    (.start server)
    (when health-server (.start health-server))
    (println (format "futon1b-server up on :%d (store %s)" port store-dir))
    (when health-server
      (println (format "futon1b independent liveness up on :%d" health-port)))
    server))

(defn- parse-args [args]
  (loop [args args opts {:store-dir "migration-store" :port 7073}]
    (if-not (seq args)
      opts
      (case (first args)
        "--store-dir" (recur (nnext args) (assoc opts :store-dir (second args)))
        "--port"      (recur (nnext args) (assoc opts :port (Long/parseLong (second args))))
        "--health-port" (recur (nnext args) (assoc opts :health-port
                                                    (Long/parseLong (second args))))
        (throw (ex-info (str "Unknown arg: " (first args)) {:args args}))))))

(defn -main [& args]
  (start-server! (parse-args args))
  ;; block forever — the server owns this JVM.
  @(promise))
