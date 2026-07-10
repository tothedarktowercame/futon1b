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
;;   GET  /health                     → {:ok true :tables {...}}
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
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [migration.transform :as xf]
            [migration.ingest :as ingest]
            [futon1b-gates :as gates]
            [futon1b-evidence :as ev]
            [zai-memory-1b :as zm]
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
  (first (xt/q node (list '-> '(from :hyperedges [*])
                          (list 'where (list '= 'xt/id id))))))

(defn- present? [node id]
  (seq (xt/q node (list '-> '(from :hyperedges [xt/id])
                        (list 'where (list '= 'xt/id id))))))

(defn upsert-hyperedge!
  "Transform + no-op guard + VERIFIED put. Returns response map."
  [node payload]
  (let [retract? (= "retract" (some-> (or (:hx/op payload) (:op payload))
                                      name str/lower-case))
        doc (xf/transform-doc (build-hyperedge-doc payload))
        id (:xt/id doc)]
    (if retract?
      (do (xt/execute-tx node [[:delete-docs :hyperedges id]])
          {:ok true :hx/id id :retracted? true})
      ;; No-op guard: compare against stored (project the doc's own keys —
      ;; [*] binds nothing in XTDB 2.0.0).
      (let [stored (first (xt/q node
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
              {:ok true :hx/id id :rescue (when (keyword? res) res)}
              {:ok false :hx/id id
               :error "verified put: doc absent after rescue ladder"})))))))

;; ---------------------------------------------------------------------------
;; HTTP plumbing.
;; ---------------------------------------------------------------------------

(defn- read-body [^HttpExchange ex]
  (slurp (.getRequestBody ex)))

(defn- respond! [^HttpExchange ex status ^String body]
  (let [bytes (.getBytes body StandardCharsets/UTF_8)]
    (.add (.getResponseHeaders ex) "Content-Type" "application/edn")
    (.sendResponseHeaders ex status (alength bytes))
    (with-open [os (.getResponseBody ex)]
      (.write os bytes))))

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
      (try
        (route-fn ex)
        (catch Exception e
          ;; Layered gate errors keep futon1a's envelope + status mapping
          ;; (API-CONTRACT §0); anything else is the generic 500.
          (let [[status body] (gates/error->response e)]
            (respond! ex status (pr-str body))))))))

(defn- penholder!
  "Resolve (body → x-penholder header → default) and authorize (L3)."
  [^HttpExchange ex payload]
  (gates/authorize!
   (gates/resolve-penholder payload
                            (.getFirst (.getRequestHeaders ex) "x-penholder"))))

(def ^:private tables
  [:hyperedges :entities :evidence :relations :type-catalog :docs :misc])

(defn- health [^HttpExchange ex]
  (let [node @!node
        counts (into {}
                     (for [t tables]
                       [t (count (xt/q node (list 'from t ['xt/id])))]))]
    (respond! ex 200 (pr-str {:ok true :tables counts}))))

(defn- hyperedge-route [^HttpExchange ex]
  (if (= "POST" (.getRequestMethod ex))
    (let [payload (edn/read-string (read-body ex))
          _ (penholder! ex payload)
          res (upsert-hyperedge! @!node payload)]
      (respond! ex (if (:ok res) 200 500) (pr-str res)))
    (respond! ex 405 (pr-str {:ok false :error "POST only"}))))

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
      (let [payload (edn/read-string (read-body ex))
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

(defn- memory-search-route [^HttpExchange ex]
  (let [p (query-params ex)
        opts (cond-> {}
               (p "type")   (assoc :type (keyword (p "type")))
               (p "author") (assoc :author (p "author"))
               (p "since")  (assoc :since (p "since"))
               (p "tags")   (assoc :tags (mapv keyword (str/split (p "tags") #",")))
               (p "limit")  (assoc :limit (Long/parseLong (p "limit"))))]
    (respond! ex 200 (pr-str (zm/memory-search @!node opts)))))

(defn start-server! [{:keys [store-dir port node]}]
  (gates/seed-mission-contract!)
  (reset! !node (or node (zm/open-store store-dir)))
  (let [server (HttpServer/create (InetSocketAddress. (int port)) 0)]
    (.createContext server "/health" (handler health))
    (.createContext server "/api/alpha/hyperedge" (handler hyperedge-route))
    (.createContext server "/api/alpha/evidence" (handler evidence-route))
    (.createContext server "/api/alpha/memory/search" (handler memory-search-route))
    (.setExecutor server (java.util.concurrent.Executors/newFixedThreadPool 4))
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
