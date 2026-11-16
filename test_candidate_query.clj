(ns test-candidate-query
  "Throwaway-node oracle and stale-read tests for the unified candidate query.

  Run: clojure -M:node -m test-candidate-query"
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [futon1b-server :as server]
            [futon1b-text :as text]
            [xtdb.api :as xt]
            [xtdb.node :as xtn])
  (:import [java.net URI URLEncoder]
           [java.net.http HttpClient HttpRequest HttpResponse$BodyHandlers]
           [java.nio.file Files]))

(def ^:private unqualified {:builder-fn rs/as-unqualified-maps})

(defn- temp-dir []
  (-> (Files/createTempDirectory
       "futon1b-candidate-query-test-"
       (make-array java.nio.file.attribute.FileAttribute 0))
      .toFile
      .getAbsolutePath))

(defn- check! [label value]
  (println (format "  %-68s %s" label (if value "PASS" "FAIL")))
  (assert value label))

(defn- priv [sym]
  (ns-resolve 'futon1b-text sym))

(defn- doc
  [id at {:keys [type claim-type author tags subject pattern-id body]
          :or {type :turn claim-type :observation}}]
  {:xt/id id
   :evidence/id id
   :evidence/type type
   :evidence/claim-type claim-type
   :evidence/author author
   :evidence/session-id "candidate-query-test"
   :evidence/at at
   :evidence/tags (vec tags)
   :evidence/subject subject
   :evidence/pattern-id pattern-id
   :evidence/body body})

(defn- enum-value [v]
  (some-> (if (keyword? v) (subs (str v) 1) (str v))
          (str/replace-first #"^:" "")))

(defn- tokens [s]
  (set (re-seq #"[\p{L}\p{N}]+" (str/lower-case (str s)))))

(defn- oracle-match?
  [d {:keys [q author session-id since before type claim-type tags
             subject-type subject-id pattern-id]}]
  (let [body-tokens (tokens (:evidence/body d))
        query-tokens (tokens q)
        subject (:evidence/subject d)
        stored-tags (set (map enum-value (:evidence/tags d)))]
    (and (or (str/blank? (str q)) (every? body-tokens query-tokens))
         (or (nil? author) (= (str author) (str (:evidence/author d))))
         (or (nil? session-id)
             (= (str session-id) (str (:evidence/session-id d))))
         (or (nil? since) (not (neg? (compare (str (:evidence/at d))
                                               (str since)))))
         (or (nil? before) (neg? (compare (str (:evidence/at d))
                                           (str before))))
         (or (nil? type) (= (enum-value type)
                            (enum-value (:evidence/type d))))
         (or (nil? claim-type)
             (= (enum-value claim-type)
                (enum-value (:evidence/claim-type d))))
         (or (empty? tags)
             (every? stored-tags (map enum-value tags)))
         (or (nil? subject-type)
             (= (enum-value subject-type)
                (enum-value (:ref/type subject))))
         (or (nil? subject-id) (= (str subject-id) (str (:ref/id subject))))
         (or (nil? pattern-id)
             (= (enum-value pattern-id)
                (enum-value (:evidence/pattern-id d)))))))

(defn- result-ids [response]
  (set (map #(get-in % [:entry :evidence/id]) (:results response))))

(defn- oracle-ids [docs params]
  (set (map :evidence/id (filter #(oracle-match? % params) docs))))

(defn- get-edn [url]
  (let [request (-> (HttpRequest/newBuilder (URI/create url)) .GET .build)
        response (.send (HttpClient/newHttpClient)
                        request
                        (HttpResponse$BodyHandlers/ofString))]
    {:status (.statusCode response)
     :body (edn/read-string (.body response))}))

(defn- test-oracle-and-voice! [node ds docs]
  (let [queries
        [{:name "content + author + claim + tag"
          :params {:q "outside signal" :author "joe"
                   :claim-type :observation :tags [:user] :limit 100 :scope :all}}
         {:name "attribute-only type + tag"
          :params {:type :note :tags [:memory] :limit 100 :scope :all}}
         {:name "attribute-only subject + pattern"
          :params {:subject-type :thread :subject-id "thread-2"
                   :pattern-id :agent/pause :limit 100 :scope :all}}
         {:name "content + two-tag AND"
          :params {:q "signal" :tags [:user :field] :limit 100 :scope :all}}]]
    (doseq [{:keys [name params]} queries]
      (let [expected (oracle-ids docs params)
            response (text/search node params)
            actual (result-ids response)]
        (check! (str "store-scan oracle agrees for " name)
                (= expected actual))))

    ;; Contaminate the candidate projection for the agent turn so both voices
    ;; are proposed. The store-side re-check must still certify only Joe's.
    (jdbc/execute! ds ["UPDATE ev_attr SET author = ? WHERE id = ?"
                       "joe" "voice-agent"])
    (jdbc/execute! ds ["INSERT OR IGNORE INTO ev_tags(id, tag) VALUES (?,?)"
                       "voice-agent" ":user"])
    (let [params {:q "outside signal" :author "joe" :tags [:user]
                  :limit 100 :scope :all}
          candidate-ids (set (map :id ((priv 'candidates) ds params)))
          response (text/search node params)]
      (check! "voice sweep candidate layer can include the doctored agent turn"
              (= #{"voice-operator" "voice-agent"} candidate-ids))
      (check! "voice sweep is certified to the operator turn at re-check"
              (= #{"voice-operator"} (result-ids response)))
      (check! "voice certification exposes candidate rejection"
              (> (:checked response) (:count response))))
    ;; Restore the derived projection before later oracle checks.
    ((priv 'index-batch!) ds [(first (filter #(= "voice-agent" (:xt/id %)) docs))])))

(defn- test-attribute-only! [node]
  (let [response (text/search node {:type :note :tags [:memory] :limit 10 :scope :all})
        result (first (:results response))]
    (check! "attribute-only mode returns candidates without an FTS MATCH"
            (= #{"memory-note"} (result-ids response)))
    (check! "attribute-only mode retains existing hydrated result shape"
            (and (nil? (:score result))
                 (string? (get-in result [:entry :evidence/body]))))))

(defn- test-stale-read-and-repair! [node ds]
  (let [fresh (doc "stale-target" "2026-08-17T04:00:00Z"
                   {:author "truth"
                    :tags [:truth]
                    :body "stale repair witness"})]
    ;; Simulate a live append: it enters the store and index but, per C6, does
    ;; not advance the catch-up checkpoint. That makes the next catch-up the
    ;; ordinary repair path for the subsequently doctored derived row.
    (xt/execute-tx node [[:put-docs :evidence fresh]])
    ((priv 'index-batch!) ds [fresh])
    (jdbc/execute! ds ["UPDATE ev_attr SET author = ? WHERE id = ?"
                       "ghost" "stale-target"])
    (let [params {:author "ghost" :limit 10 :scope :all}
          candidate-ids (set (map :id ((priv 'candidates) ds params)))
          before-rejections (:recheck-rejections (text/stats))
          response (text/search node params)]
      (check! "doctored ev_attr row surfaces from the candidate layer"
              (= #{"stale-target"} candidate-ids))
      (check! "store contradiction is dropped and checked exceeds count"
              (and (zero? (:count response))
                   (> (:checked response) (:count response))))
      (check! "cumulative rejection delta equals checked minus count"
              (= (- (:recheck-rejections (text/stats)) before-rejections)
                 (- (:checked response) (:count response)))))
    (text/catch-up! node :page 2)
    (check! "the next catch-up repairs the doctored candidate row"
            (and (empty? ((priv 'candidates) ds {:author "ghost" :limit 10 :scope :all}))
                 (= "truth"
                    (:author (jdbc/execute-one!
                              ds ["SELECT author FROM ev_attr WHERE id = ?"
                                  "stale-target"]
                              unqualified)))))))

(defn- test-basis-stats! [node ds]
  (let [stats (text/stats node)
        basis-ids ((priv 'read-meta-edn) ds "basis-tx-ids")]
    (check! "normalized transaction ids are committed with the drain basis"
            (map? basis-ids))
    (check! "stats publishes numeric tx lag and wall-clock age"
            (and (integer? (get-in stats [:staleness :tx-lag]))
                 (integer? (get-in stats [:staleness :age-ms]))))
    (check! "stats publishes the declared projection and named residual"
            (and (= text/projection (:projection stats))
                 (= text/residual (:residual stats)))))
  (xt/execute-tx
   node
   [[:put-docs :evidence
     (doc "after-basis-lag" "2026-08-17T05:00:00Z"
          {:author "lag-witness" :tags [:lag]
           :body "transaction after completed basis"})]])
  (check! "tx staleness is a positive distance after a later store commit"
          (pos? (get-in (text/stats node) [:staleness :tx-lag]))))

(defn- test-memory-sidecar-recall! []
  (let [called (atom [])
        run #(with-redefs-fn
               {(priv 'fetch-doc) (fn [& _] (swap! called conj :recheck)
                                    (throw (AssertionError. "XTDB recheck called")))
                (priv 'hydrate-survivors) (fn [& _] (swap! called conj :hydrate)
                                            (throw (AssertionError. "XTDB hydrate called")))}
               (fn [] (text/memory-recall-search
                       {:q "orchid recall" :limit 50})))
        t0 (System/nanoTime)
        response (run)
        elapsed-ms (/ (- (System/nanoTime) t0) 1000000.0)
        results (:results response)]
    (check! "memory sidecar recall makes zero XTDB point reads"
            (empty? @called))
    (check! "memory sidecar caps, ranks, and returns the public shape"
            (and (= :memory-sidecar (:source response))
                 (<= (:count response) text/memory-recall-max-limit)
                 (= #{"mem-new" "mem-two" "mem-ranked"}
                    (set (map #(get-in % [:entry :evidence/id]) results)))
                 (every? #(= #{:evidence/id :evidence/at :evidence/subject
                               :evidence/body}
                             (set (keys (:entry %)))) results)
                 (every? #(= #{:source :project :name :hook}
                             (set (keys (get-in % [:entry :evidence/body])))) results)))
    (check! "newest indexed version suppresses the older matching version"
            (not (some #(= "mem-old" (get-in % [:entry :evidence/id])) results)))
    (check! "memory-only FTS preserves bm25 rank order"
            (= "mem-ranked" (get-in results [0 :entry :evidence/id])))
    (check! "a memory appended after init through on-append is found"
            (some #(= "mem-two" (get-in % [:entry :evidence/id])) results))
    (check! "memory recall applies the requested limit after ranking"
            (= 2 (:count (text/memory-recall-search
                          {:q "orchid recall" :limit 2}))))
    (check! "a matching non-memory without the source tag is excluded"
            (not (some #(= "mem-decoy" (get-in % [:entry :evidence/id])) results)))
    (check! "memory sidecar response carries checkpoint and basis"
            (let [basis (:index-basis response)]
              (and (= 2 (count (:checkpoint basis)))
                   (contains? basis :basis-tx)
                   (contains? basis :basis-captured-at))))
    (println (format "  memory-sidecar in-test read %.3f ms" elapsed-ms))))

(defn- test-memory-sidecar-backfill! [ds]
  (let [bulk (mapv (fn [i]
                     (doc (str "bulk-memory-" i) (format "2026-08-18T00:%02d:%02dZ"
                                                         (quot i 60) (mod i 60))
                          {:type :memory :author "minus40"
                           :tags ["source/claude-code"]
                           :subject {:ref/type :memory
                                     :ref/id (str "cc:invented/bulk-" i)}
                           :body {:source "claude-code" :project "invented"
                                  :name (str "bulk-" i)
                                  :hook (str "bulk fixture token " i)}}))
                   (range 1500))]
    (doseq [batch (partition-all 100 bulk)] ((priv 'index-batch!) ds batch))
    (jdbc/with-transaction [tx ds]
      (jdbc/execute! tx ["DELETE FROM memory_fts"])
      (jdbc/execute! tx ["DELETE FROM memory_fts_map"])
      (jdbc/execute! tx ["DELETE FROM fts_meta WHERE k = 'memory-fts-backfill'"]))
    (let [t0 (System/nanoTime)
          first-run (text/backfill-memory-fts! ds)
          elapsed-ms (/ (- (System/nanoTime) t0) 1000000.0)
          second-run (text/backfill-memory-fts! ds)]
      (check! "bounded memory FTS backfill fills the 1500-row fixture"
              (= 1503 (:filled first-run)))
      (check! "memory FTS backfill checkpoint makes the second run a no-op"
              (= {:filled 0 :already :complete} second-run))
      (println (format "  memory-fts 1500-row backfill %.3f ms" elapsed-ms)))))

(defn- test-http-surface! [node dir]
  (let [http-server (server/start-server! {:node node :store-dir dir
                                           :port 0 :health-port nil})
        port (.getPort (.getAddress http-server))
        base (str "http://127.0.0.1:" port
                  "/api/alpha/evidence/text-search")]
    (try
      (let [q (URLEncoder/encode "outside signal" "UTF-8")
            composed (get-edn
                      (str base "?q=" q
                           "&author=joe&claim-type=observation"
                           "&tags=user&tags=field&limit=10&scope=all"))
            attribute-only (get-edn
                            (str base "?type=note&tags=memory&limit=10&scope=all"))
            memory-sidecar (get-edn
                            (str base "?source=memory-sidecar&q=orchid%20recall&limit=50"))
            stats (get-edn (str base "?stats=true"))]
        (check! "HTTP accepts repeated tags on composed content queries"
                (= #{"voice-operator"} (result-ids (:body composed))))
        (check! "HTTP accepts q-less attribute-only queries"
                (= #{"memory-note"} (result-ids (:body attribute-only))))
        (check! "search responses carry checkpoint and basis coordinates"
                (let [basis (get-in composed [:body :index-basis])]
                  (and (= 2 (count (:checkpoint basis)))
                       (string? (:basis-tx basis))
                       (string? (:basis-captured-at basis)))))
        (check! "HTTP memory-sidecar mode is capped and returns newest subjects"
                (and (= 200 (:status memory-sidecar))
                     (= #{"mem-new" "mem-two" "mem-ranked"}
                        (set (map #(get-in % [:entry :evidence/id])
                                  (get-in memory-sidecar [:body :results]))))))
        (check! "HTTP stats exposes staleness, projection, and residual"
                (and (number? (get-in stats [:body :staleness :tx-lag]))
                     (seq (get-in stats [:body :projection]))
                     (map? (get-in stats [:body :residual])))))
      (finally
        (server/stop-server! http-server)))))

(defn -main [& _]
  (let [dir (temp-dir)
        docs [(doc "voice-operator" "2026-08-17T03:00:00Z"
                   {:author "joe" :tags [:user :field]
                    :subject {:ref/type :thread :ref/id "thread-1"}
                    :pattern-id :operator/assertion
                    :body "outside signal sunflower"})
              (doc "voice-agent" "2026-08-17T03:01:00Z"
                   {:author "claude" :tags [:assistant :field]
                    :subject {:ref/type :thread :ref/id "thread-1"}
                    :pattern-id :agent/summary
                    :body "outside signal sunflower"})
              (doc "memory-note" "2026-08-17T03:02:00Z"
                   {:type :note :author "joe" :tags ["memory"]
                    :body "substrate archive"})
              (doc "subject-note" "2026-08-17T03:03:00Z"
                   {:type :note :author "alice" :tags [:review :field]
                    :subject {:ref/type :thread :ref/id "thread-2"}
                    :pattern-id :agent/pause
                    :body "review signal"})]
        memory-docs
        [(doc "mem-old" "2026-08-17T03:04:00Z"
                   {:type :memory :author "minus40" :tags ["source/claude-code"]
                    :subject {:ref/type :memory :ref/id "cc:invented/orchid"}
                    :body {:source "claude-code" :project "invented"
                           :name "orchid" :hook "orchid recall old"}})
              (doc "mem-new" "2026-08-17T03:05:00Z"
                   {:type :memory :author "minus40" :tags ["source/claude-code"]
                    :subject {:ref/type :memory :ref/id "cc:invented/orchid"}
                    :body {:source "claude-code" :project "invented"
                           :name "orchid" :hook "orchid recall current"}})
              (doc "mem-two" "2026-08-17T03:06:00Z"
                   {:type :memory :author "minus40" :tags ["source/claude-code"]
                    :subject {:ref/type :memory :ref/id "cc:invented/second"}
                    :body {:source "claude-code" :project "invented"
                           :name "second" :hook "orchid recall companion"}})
              (doc "mem-ranked" "2026-08-17T03:06:30Z"
                   {:type :memory :author "minus40" :tags ["source/claude-code"]
                    :subject {:ref/type :memory :ref/id "cc:invented/ranked"}
                    :body {:source "claude-code" :project "invented"
                           :name "ranked"
                           :hook "orchid orchid orchid recall recall recall"}})
              (doc "mem-decoy" "2026-08-17T03:07:00Z"
                   {:type :memory :author "other" :tags [:other]
                    :subject {:ref/type :memory :ref/id "cc:invented/decoy"}
                    :body {:source "claude-code" :project "invented"
                           :name "decoy" :hook "orchid recall decoy"}})]]
    (with-open [node (xtn/start-node)]
      (xt/execute-tx node (mapv (fn [d] [:put-docs :evidence d]) docs))
      (text/init! {:path (str dir "/fts5-evidence.db")})
      (text/catch-up! node :page 2)
      (let [ds @(var-get (priv '!ds))]
        (test-oracle-and-voice! node ds docs)
        (test-attribute-only! node)
        (test-stale-read-and-repair! node ds)
        (test-basis-stats! node ds)
        (xt/execute-tx node (mapv (fn [d] [:put-docs :evidence d]) memory-docs))
        (text/reconcile! node)
        ;; Exercise the normal live index hook: it indexes and queues ev_doc.
        (doseq [d memory-docs]
          (text/on-append! d))
        (check! "normal append path fills ev_doc for memory fixtures"
                (loop [n 0]
                  (cond (= 5 (count (text/cached-docs
                                     ["mem-old" "mem-new" "mem-two"
                                      "mem-ranked" "mem-decoy"]))) true
                        (< 200 n) false
                        :else (do (Thread/sleep 10) (recur (inc n))))))
        (test-memory-sidecar-recall!)
        (test-memory-sidecar-backfill! ds)
        (test-http-surface! node dir))))
  (println "CANDIDATE QUERY: ALL PASS")
  (shutdown-agents))
