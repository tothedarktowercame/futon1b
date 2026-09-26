;; futon1b-drawbridge — nREPL over HTTP plus a plain /eval bridge INSIDE the
;; serving JVM, so a query can be timed where it runs instead of from outside
;; :7073 (Joe, 2026-09-26: "let's install one, it would make life much
;; easier"). A port of futon3c's repl.http (the :6768 endpoint): same routes,
;; same auth, same eval contract, same two refusals, so the tooling that talks
;; to :6768 talks to this one unchanged apart from the port and the token file.
;;
;;   /repl        nREPL over HTTP (Drawbridge protocol; CIDER connects here)
;;   /eval        POST Clojure code as the body, EDN back: {:ok true :value v}
;;                or {:ok false :error msg :type class}; 504 on timeout
;;   /admin/eval  the same evaluator, requiring x-drawbridge-profile: dev-admin
;;
;; Auth: x-admin-token header (or ?token=) must equal the token, and the
;; remote address must be in the allowlist (loopback by default). Binds
;; 127.0.0.1 only.
;;
;;   curl -s -H "x-admin-token: $(cat .admintoken)" \
;;     --data-binary '(count (futon1b-xt/timed-q @futon1b-server/!node ...))' \
;;     http://127.0.0.1:6769/eval
;;
;; Every non-blank request is written to /tmp/futon1b-eval.log before and
;; after eval, so a body that wedges the node still leaves a trail.
(ns futon1b-drawbridge
  (:require [cemerick.drawbridge :as db]
            [clojure.string :as str]
            [org.httpkit.server :as http]
            [ring.middleware.keyword-params :as ring-keyword]
            [ring.middleware.nested-params :as ring-nested]
            [ring.middleware.params :as ring-params]
            [ring.middleware.session :as ring-session]
            [ring.util.codec :as codec]))

(defonce server (atom nil))

(defn- normalize-allow [allow]
  (cond
    (nil? allow) nil
    (set? allow) allow
    (sequential? allow) (set allow)
    :else #{allow}))

(defn- wrap-token [handler token allow]
  (fn [request]
    (let [remote (:remote-addr request)
          allow-set (normalize-allow allow)
          allowed? (if (seq allow-set) (contains? allow-set remote) true)
          supplied (or (get-in request [:headers "x-admin-token"])
                       (some-> (:query-string request)
                               (codec/form-decode "UTF-8")
                               (get "token")))
          supplied (some-> supplied str/trim)
          token (some-> token str/trim)]
      (if (and allowed? (= supplied token))
        (handler request)
        {:status 403
         :headers {"content-type" "text/plain"}
         :body "forbidden"}))))

;; ---------------------------------------------------------------------------
;; /eval

(def ^:private eval-timeout-ms
  "Maximum time for a single eval request (5 minutes)."
  300000)

(def ^:private eval-log-file "/tmp/futon1b-eval.log")
(def ^:private eval-log-max-bytes 5000000)
(def ^:private eval-log-summary-chars 500)

(defn- truncate-str [s n]
  (when (some? s)
    (let [s (str s)]
      (if (> (count s) n) (str (subs s 0 n) "…<truncated>") s))))

(defn- eval-log!
  "Append one EDN line to the eval forensic log; rotates to .1 at max bytes."
  [entry]
  (try
    (let [f (java.io.File. eval-log-file)]
      (when (and (.exists f) (> (.length f) eval-log-max-bytes))
        (.renameTo f (java.io.File. (str eval-log-file ".1"))))
      (spit eval-log-file
            (str (pr-str (assoc entry :at (str (java.time.Instant/now)))) \newline)
            :append true))
    (catch Throwable _ nil)))

(defn- read-body [request]
  (when-let [body (:body request)]
    (cond
      (string? body) body
      (instance? java.io.InputStream body) (slurp body)
      :else (str body))))

(defonce ^{:doc "Sandbox ns for /eval. load-string evaluates with *ns* bound to
   clojure.core on a worker thread, so a bare top-level (def foo ...) in a
   payload would clobber clojure.core/foo JVM-wide (this bit futon3c on
   2026-07-05). Binding *ns* here confines agent defs to a scratch ns."}
  eval-sandbox-ns
  (let [n (create-ns 'futon1b-drawbridge.eval-sandbox)]
    (binding [*ns* n] (clojure.core/refer-clojure))
    n))

(def ^:private refresh-refusal
  (str "REFUSED: clojure.tools.namespace refresh/reload is not permitted here.\n\n"
       "refresh REMOVES namespaces and recreates them: defonce state (the open\n"
       "XTDB node in futon1b-server/!node, the executors, the caches) comes back\n"
       "empty, and live handlers keep serving from the old Vars while anything\n"
       "newly resolved reaches the recreated ones. Redefine Vars in place with a\n"
       "targeted (require 'ns :reload) or load-file of a file on this JVM's\n"
       "classpath; anything that changes the node, the routes or the executors\n"
       "needs a restart (scripts/restart-futon1b-detached.sh)."))

(def ^:private pool-destruction-refusal
  (str "REFUSED: this eval would destroy JVM-wide execution machinery.\n\n"
       "(shutdown-agents) and (System/exit ...) are how a one-shot clojure -M\n"
       "script ends. This is the serving JVM: shutdown-agents kills the pools\n"
       "behind every future, send, send-off and pmap in the process, including\n"
       "the one this endpoint evaluates in, and nothing restarts them (futon3c,\n"
       "2026-09-20). Drop the form. A deliberate shutdown is a systemctl stop."))

(defn- request-profile [request]
  (if (= "/admin/eval" (:uri request)) :dev-admin :dev-serve))

(defn- refresh-attempt?
  "Blunt textual guard, run before any parsing: a false positive costs a
   restart, a false negative costs the running image."
  [code]
  (let [c (str code)]
    (boolean
     (or (str/includes? c "tools.namespace")
         (re-find #"\((?:[\w.-]+/)?refresh(?:-all|-dirs)?\s*[\)\s]" c)
         (re-find #"\((?:[\w.-]+/)?clear\s*\)" c)))))

(defn- pool-destruction-attempt? [code]
  (let [c (str code)]
    (boolean
     (or (re-find #"\((?:clojure\.core/)?shutdown-agents\s*\)" c)
         (re-find #"\(System/exit\b" c)))))

(defn- edn-response [status body]
  {:status status
   :headers {"content-type" "application/edn"}
   :body (pr-str body)})

(defn- evaluate [code]
  (try
    {:ok true :value (binding [*ns* eval-sandbox-ns] (load-string code))}
    (catch Throwable t
      {:ok false :error (.getMessage t) :type (.getName (class t))})))

(defn- eval-handler [request]
  (if (not= :post (:request-method request))
    {:status 405 :headers {"content-type" "text/plain"} :body "POST only"}
    (let [code (read-body request)
          remote (:remote-addr request)
          profile (request-profile request)]
      (cond
        (str/blank? code)
        {:status 400 :headers {"content-type" "text/plain"} :body "empty code"}

        (refresh-attempt? code)
        (do (eval-log! {:type :refused :profile profile :remote remote
                        :reason :refresh :bytes (count code) :code code})
            {:status 403 :headers {"content-type" "text/plain"} :body refresh-refusal})

        (and (= :dev-serve profile) (pool-destruction-attempt? code))
        (do (eval-log! {:type :refused :profile profile :remote remote
                        :reason :pool-destruction :bytes (count code) :code code})
            {:status 403 :headers {"content-type" "text/plain"}
             :body pool-destruction-refusal})

        :else
        (let [start-ns (System/nanoTime)]
          (eval-log! {:type :request :profile profile :remote remote
                      :bytes (count code) :code code})
          (try
            (let [[result on-request-thread?]
                  (try
                    (let [f (future (evaluate code))
                          r (deref f eval-timeout-ms ::timeout)]
                      (when (= r ::timeout) (future-cancel f))
                      [r false])
                    (catch java.util.concurrent.RejectedExecutionException t
                      (println "[drawbridge] /eval: send-off pool rejected the task;"
                               "evaluating on the request thread, no timeout."
                               (.getMessage t))
                      [(evaluate code) true]))
                  elapsed-ms (long (/ (- (System/nanoTime) start-ns) 1000000))]
              (if (= result ::timeout)
                (do (eval-log! {:type :response :profile profile :remote remote
                                :elapsed-ms elapsed-ms :ok false :error "eval timeout"})
                    (edn-response 504 {:ok false :error "eval timeout"
                                       :timeout-ms eval-timeout-ms}))
                (do (eval-log! {:type :response :profile profile :remote remote
                                :elapsed-ms elapsed-ms :ok (boolean (:ok result))
                                :on-request-thread on-request-thread?
                                :summary (if (:ok result)
                                           (truncate-str (pr-str (:value result))
                                                         eval-log-summary-chars)
                                           (truncate-str (str (:type result) ": "
                                                              (:error result))
                                                         eval-log-summary-chars))})
                    (edn-response (if (:ok result) 200 500)
                                  (cond-> result
                                    on-request-thread? (assoc :on-request-thread true))))))
            (catch Throwable t
              (eval-log! {:type :response :profile profile :remote remote :ok false
                          :error (.getMessage t) :exception-type (.getName (class t))})
              (edn-response 500 {:ok false :error (.getMessage t)
                                 :type (.getName (class t))}))))))))

(defn- nrepl-handler-with-refresh-guard
  "The same textual guard on a Drawbridge request's bencoded body; a safe
   request gets a replayable stream of the same bytes."
  [nrepl-handler request]
  (let [body (:body request)
        [encoded replay-body]
        (if (instance? java.io.InputStream body)
          (let [bytes (.readAllBytes ^java.io.InputStream body)]
            [(String. bytes "UTF-8") (java.io.ByteArrayInputStream. bytes)])
          [(read-body request) body])]
    (if (refresh-attempt? encoded)
      {:status 403 :headers {"content-type" "text/plain"} :body refresh-refusal}
      (nrepl-handler (if (instance? java.io.InputStream body)
                       (assoc request :body replay-body)
                       request)))))

(defn- route-handler [nrepl-handler]
  (fn [request]
    (case (:uri request)
      "/eval" (eval-handler request)
      "/admin/eval"
      (if (= "dev-admin" (get-in request [:headers "x-drawbridge-profile"]))
        (eval-handler request)
        {:status 403 :headers {"content-type" "text/plain"}
         :body "dev-admin profile header required"})
      (nrepl-handler-with-refresh-guard nrepl-handler request))))

(defn read-token
  "The admin token: FUTON1B_ADMIN_TOKEN, else the .admintoken file in the
   working directory, else nil (which start! refuses)."
  []
  (or (not-empty (System/getenv "FUTON1B_ADMIN_TOKEN"))
      (let [f (java.io.File. ".admintoken")]
        (when (.exists f) (not-empty (str/trim (slurp f)))))))

(defn start!
  "Start the endpoint. Options: {:port 6769 :bind \"127.0.0.1\"
   :token \"...\" :allow [\"127.0.0.1\" \"::1\"]}. Refuses to start without a
   token: an unauthenticated eval in the store's JVM is not a mode."
  [{:keys [port token bind allow]
    :or {port 6769 bind "127.0.0.1" allow ["127.0.0.1" "::1"]}}]
  (when (str/blank? token)
    (throw (ex-info "futon1b-drawbridge: no admin token (FUTON1B_ADMIN_TOKEN or ./.admintoken)"
                    {:kind :drawbridge-token-missing})))
  (when-let [stop-fn @server] (stop-fn))
  (let [handler (-> (route-handler
                     (-> (db/ring-handler)
                         ring-keyword/wrap-keyword-params
                         ring-nested/wrap-nested-params
                         ring-params/wrap-params
                         ring-session/wrap-session))
                    (wrap-token token allow))
        stop-fn (http/run-server handler {:ip bind :port port})]
    (reset! server stop-fn)
    (println (format "[drawbridge] http://%s:%s/repl + /eval + /admin/eval (allow: %s)"
                     bind port (pr-str allow)))
    stop-fn))

(defn stop! []
  (when-let [stop-fn @server]
    (stop-fn)
    (reset! server nil)
    (println "[drawbridge] stopped")))
