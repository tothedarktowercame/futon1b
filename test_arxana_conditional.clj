;; test-arxana-conditional — M-jvm-arxana INSTANTIATE increment 2 tripwires
;; (D3 "Required traces", D7 acceptance plan):
;;   1. two writers expecting the same head: exactly one wins, the loser is
;;      stale, the head is the winner's and exactly one journal row exists;
;;   2. retry of the same message id returns the original outcome and does
;;      not reapply (crash/republish: head and journal unchanged);
;;   3. an unauthenticated request is refused (403) and writes nothing;
;;   4. generic write routes refuse the reserved arxana/* namespace;
;;   5. a refused command leaves no partial mutation (head, revision,
;;      journal, outcome and attached records all absent).
;;
;; Run: clojure -M:node -m test-arxana-conditional
(ns test-arxana-conditional
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [futon1b-arxana :as arxana]
            [futon1b-gates :as gates]
            [futon1b-hxindex :as hx]
            [futon1b-server :as srv]
            [xtdb.node :as xtn]
            [xtdb.api :as xt])
  (:import [java.net URI]
           [java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers
            HttpResponse$BodyHandlers]))

(def client (HttpClient/newHttpClient))
(def !results (atom []))
(def ph {"x-penholder" "joe"})
(def arxana-principal (str arxana/service-principal))

(defn- req [method url body headers]
  (let [builder (-> (HttpRequest/newBuilder (URI/create url))
                    (.method method
                             (if body
                               (HttpRequest$BodyPublishers/ofString (pr-str body))
                               (HttpRequest$BodyPublishers/noBody))))
        builder (reduce (fn [b [k v]] (.header b k v)) builder (or headers {}))
        response (.send client (.build builder) (HttpResponse$BodyHandlers/ofString))
        raw (.body response)]
    {:status (.statusCode response)
     :body (try (edn/read-string raw) (catch Throwable _ raw))}))

(defn- check! [label ok? detail]
  (swap! !results conj {:label label :ok? (boolean ok?) :detail detail})
  (println (format "  %-72s %s" label (if ok? "PASS" (str "FAIL " (pr-str detail))))))

(defn- command
  ([message-id target expected bytes]
   (command message-id target expected bytes nil))
  ([message-id target expected bytes extra-command]
   {:protocol/version "arxana.v1"
    :message/id message-id
    :operation :revision/ingest
    :principal/id arxana-principal
    :body {:target/id target
           :expected/revision-id expected
           :command (merge {:revision/canonical-bytes bytes
                            :revision/media-type "text/plain"}
                           extra-command)}}))

(defn- run-tests [base node]
  (let [CMD (str base "/api/alpha/arxana/command")
        ENT (str base "/api/alpha/entity")
        HX (str base "/api/alpha/hyperedge")
        post (fn [body headers] (req "POST" CMD body headers))
        ah {"x-arxana-principal" arxana-principal}]

    ;; --- 1. authenticated first commit (expected :absent) -----------------
    (let [r (post (command "m-seed" "artifact:A" :absent "revision-one-bytes") ah)
          rev (get-in r [:body :ok :revision/id])]
      (check! "authenticated :absent command commits -> 200 with revision id"
              (and (= 200 (:status r))
                   (str/starts-with? (str rev) "arxana:revision:sha256:"))
              r)
      (check! "head moved to the committed revision"
              (= rev (arxana/current-head node "artifact:A"))
              (arxana/current-head node "artifact:A"))
      (check! "exactly one journal row for the target"
              (= 1 (count (arxana/journal-entries node "artifact:A")))
              (arxana/journal-entries node "artifact:A"))

      ;; --- 2. two writers, one winner -------------------------------------
      (let [f1 (future (post (command "m-race-1" "artifact:A" rev "writer-one") ah))
            f2 (future (post (command "m-race-2" "artifact:A" rev "writer-two") ah))
            rs [@f1 @f2]
            winners (filter #(= 200 (:status %)) rs)
            losers (filter #(= 409 (:status %)) rs)
            head-after (arxana/current-head node "artifact:A")]
        (check! "two writers on one expected head: exactly one commits"
                (= 1 (count winners)) rs)
        (check! "the loser is refused stale (409, category :stale)"
                (and (= 1 (count losers))
                     (= :stale (get-in (first losers) [:body :error :context :category]))
                     (= :arxana/stale-head (get-in (first losers) [:body :error :context :code])))
                losers)
        (check! "stale refusal names current head and submitted base"
                (let [ctx (get-in (first losers) [:body :error :context])]
                  (and (= head-after (:current-head ctx))
                       (= rev (:submitted-base ctx))
                       (= [:refresh :rebase-and-resubmit :export-patch]
                          (get-in ctx [:recoverable :choices]))))
                (first losers))
        (check! "head is the winner's revision and only one journal row exists"
                (and (= (get-in (first winners) [:body :ok :revision/id]) head-after)
                     (= 2 (count (arxana/journal-entries node "artifact:A"))))
                {:head head-after
                 :journal (count (arxana/journal-entries node "artifact:A"))})

        ;; --- 3. retry same message id: original outcome, no reapply -------
        (let [winner-id (get-in (first winners) [:body :message/id])
              winner-body (:body (first winners))
              retry (post (command winner-id "artifact:A" rev "writer-one") ah)]
          (check! "retry returns the original outcome marked duplicate"
                  (and (= 200 (:status retry))
                       (true? (:duplicate? (:body retry)))
                       (= (:ok winner-body) (:ok (:body retry))))
                  retry)
          (check! "retry did not reapply: head and journal unchanged"
                  (and (= head-after (arxana/current-head node "artifact:A"))
                       (= 2 (count (arxana/journal-entries node "artifact:A"))))
                  {:head (arxana/current-head node "artifact:A")
                   :journal (count (arxana/journal-entries node "artifact:A"))}))

        ;; --- 5. no partial mutation on refusal ----------------------------
        (let [before-head (arxana/current-head node "artifact:A")
              before-journal (count (arxana/journal-entries node "artifact:A"))
              r (post (command "m-partial" "artifact:A" "arxana:revision:sha256:bogus"
                               "never-applied"
                               {:entity-records [{:name "orphan" :type :arxana/manifestation
                                                  :external-id "arxana:m:partial"}]})
                      ah)
              orphan (req "GET" (str ENT "?source=t&external-id=arxana:m:partial") nil nil)]
          (check! "wrong expected head -> 409 stale"
                  (and (= 409 (:status r))
                       (= :stale (get-in r [:body :error :context :category])))
                  r)
          (check! "refusal left head and journal unchanged"
                  (and (= before-head (arxana/current-head node "artifact:A"))
                       (= before-journal (count (arxana/journal-entries node "artifact:A"))))
                  {:head (arxana/current-head node "artifact:A")})
          (check! "refusal stored no outcome for the refused message id"
                  (nil? (arxana/outcome-by-message node "m-partial"))
                  (arxana/outcome-by-message node "m-partial"))
          (check! "attached entity record was not written (no partial mutation)"
                  (= 404 (:status orphan)) orphan)))

      ;; --- 4. unauthenticated request --------------------------------------
      (let [no-principal (post (command "m-anon" "artifact:B" :absent "anon") nil)
            wrong-principal (post (command "m-anon2" "artifact:B" :absent "anon")
                                  {"x-arxana-principal" "mallory"})]
        (check! "missing principal -> 403"
                (= 403 (:status no-principal)) no-principal)
        (check! "wrong principal -> 403"
                (= 403 (:status wrong-principal)) wrong-principal)
        (check! "unauthenticated requests wrote nothing"
                (and (nil? (arxana/current-head node "artifact:B"))
                     (nil? (arxana/outcome-by-message node "m-anon"))
                     (nil? (arxana/outcome-by-message node "m-anon2")))
                (arxana/current-head node "artifact:B")))

      ;; --- 6. generic-route bypass ----------------------------------------
      (let [r-ent (req "POST" ENT {:name "bypass" :type :arxana/artifact-revision
                                   :source "t" :external-id "arxana:bypass-1"}
                       ph)
            r-hx (req "POST" HX {:hx/type :arxana/corresponds
                                 :hx/endpoints ["a" "b"]} ph)
            r-prop (req "POST" ENT {:name "bypass-prop" :type "gadget"
                                    :source "t" :external-id "arxana:bypass-2"
                                    :props {:arxana/salience 9}} ph)
            read-back (req "GET" (str ENT "?source=t&external-id=arxana:bypass-1") nil nil)]
        (check! "generic entity route refuses arxana/* type (403)"
                (and (= 403 (:status r-ent))
                     (= :reserved-arxana-namespace (get-in r-ent [:body :error :reason])))
                r-ent)
        (check! "generic hyperedge route refuses arxana/* type (403)"
                (= 403 (:status r-hx)) r-hx)
        (check! "generic entity route refuses arxana/* property (403)"
                (= 403 (:status r-prop)) r-prop)
        (check! "bypass left nothing readable"
                (= 404 (:status read-back)) read-back))

      ;; --- 7. conditional route commits attached arxana records atomically -
      (let [hx-seen (atom [])
            r (with-redefs [hx/on-put! (fn [d] (swap! hx-seen conj d) nil)]
               (post (command "m-with-records" "artifact:C" :absent "artifact-c-bytes"
                             {:entity-records [{:name "manifestation-1"
                                                :type :arxana/manifestation
                                                :external-id "arxana:m:1"
                                                :source "t"}]
                              :hyperedge-records [{:hx/type :arxana/observed-in
                                                   :hx/endpoints ["m:1" "r:1"]}]})
                    ah))
            rev (get-in r [:body :ok :revision/id])
            read-ent (req "GET" (str ENT "?source=t&external-id=arxana:m:1") nil nil)]
        (check! "command with attached records commits (200)"
                (= 200 (:status r)) r)
        (check! "attached arxana entity is durable after the atomic commit"
                (= 200 (:status read-ent)) read-ent)
        ;; 2026-11-14: Arxana's hyperedge puts never reached the hyperedge
        ;; sidecar index; every other write path calls hx/on-put!.
        (check! "attached arxana hyperedge goes to the hyperedge index hook"
                (= [[:arxana/observed-in ["m:1" "r:1"]]]
                   (mapv (juxt :hx/type :hx/endpoints) @hx-seen))
                @hx-seen)
        (check! "revision id is content-addressed (sha256 of canonical bytes)"
                (= rev (str "arxana:revision:sha256:"
                            (let [digest (java.security.MessageDigest/getInstance "SHA-256")]
                              (.update digest (.getBytes "artifact-c-bytes" "UTF-8"))
                              (apply str (map #(format "%02x" %) (.digest digest))))))
                rev)
        ;; immutable revision + head/journal/outcome are in the same tx: a
        ;; duplicate of this message id returns the stored outcome.
        (let [retry (post (command "m-with-records" "artifact:C" :absent "artifact-c-bytes"
                                   {:entity-records [{:name "manifestation-1"
                                                      :type :arxana/manifestation
                                                      :external-id "arxana:m:1"
                                                      :source "t"}]})
                          ah)]
          (check! "crash/republish returns stored outcome without reapply"
                  (and (= 200 (:status retry))
                       (true? (:duplicate? (:body retry)))
                       (= rev (get-in retry [:body :ok :revision/id]))
                       (= 1 (count (arxana/journal-entries node "artifact:C"))))
                  retry))))))

(defn- event-time-tests [node]
  (let [record (fn [suffix] {:id (str "arxana:clusion:sha256:time-" suffix)
                             :type-revision [:workplan/item 1] :endpoints []
                             :asserted-content {:item/id (str "M-toy-" suffix)}})
        envelope (fn [mid r batch?]
                   {:protocol/version "arxana.v1" :message/id mid
                    :operation (if batch? :records/append :clusion/assert)
                    :body (if batch? {:records [{:operation :clusion/assert :record r}]}
                              {:record r})})
        asof (fn [id date]
               (xt/q node [(str "SELECT _id FROM arxana_clusions FOR VALID_TIME AS OF TIMESTAMP '"
                                date "' WHERE _id = ?") id]))
        refusal (fn [cmd]
                  (try (arxana/conditional-command! node cmd) nil
                       (catch clojure.lang.ExceptionInfo e (ex-data e))))]
    (doseq [batch? [false true]
            :let [label (if batch? "batch" "single")
                  r (assoc (record label) :event-time "2026-08-27T12:00:00.000Z")
                  cmd (envelope (str "time-" label) r batch?)
                  before (arxana/source-sequence node)
                  result (arxana/conditional-command! node cmd)]]
      (check! (str label " event-time visible AS OF September")
              (= 1 (count (asof (:id r) "2026-09-01T00:00:00Z"))) result)
      (check! (str label " event-time absent AS OF August")
              (empty? (asof (:id r) "2026-08-01T00:00:00Z")) result)
      (check! (str label " past event is current and indexed")
              (and (= r (arxana/clusion-by-id node (:id r)))
                   (= (inc before) (arxana/source-sequence node))) result)
      (check! (str label " immutable conflict still refuses")
              (= :arxana-immutable-record-conflict
                 (get-in (refusal (envelope (str "conflict-" label)
                                            (assoc r :asserted-content {:changed true}) batch?))
                         [:error :reason])) nil)
      (check! (str label " message replay still succeeds")
              (:duplicate? (arxana/conditional-command! node cmd)) nil)
      (let [undated (record (str label "-undated"))
            before-write (java.time.Instant/now)]
        (arxana/conditional-command! node (envelope (str "undated-" label) undated batch?))
        (check! (str label " undated record starts at write time")
                (and (= undated (arxana/clusion-by-id node (:id undated)))
                     (empty? (asof (:id undated) (str before-write)))) nil))
      (doseq [[kind value] [["future" "2999-01-01T00:00:00.000Z"]
                            ["invalid" "not-an-instant"] ["invalid" nil]
                            ["invalid" "2026-99-01T00:00:00.000Z"]]]
        (let [bad (assoc (record (str label "-bad-" (hash value))) :event-time value)
              mid (str "bad-" label "-" (hash value))
              fresh (record (str label "-atomic-" (hash value)))
              cmd (cond-> (envelope mid bad batch?)
                    batch? (update-in [:body :records] #(into [{:operation :clusion/assert :record fresh}] %)))
              before (arxana/source-sequence node)
              error (refusal cmd)]
          (check! (str label " refuses " kind " event-time " (pr-str value))
                  (= (keyword (str "arxana-" kind "-event-time")) (get-in error [:error :reason])) error)
          (check! (str label " refused time leaves no records/outcome/index change")
                  (and (nil? (arxana/clusion-by-id node (:id bad)))
                       (nil? (arxana/clusion-by-id node (:id fresh)))
                       (nil? (arxana/outcome-by-message node mid))
                       (= before (arxana/source-sequence node))) error))))))

(defn -main [& _]
  (gates/seed-mission-contract!)
  (with-open [node (xtn/start-node)]
    (let [server (srv/start-server! {:node node :port 0})
          base (str "http://127.0.0.1:" (.getPort (.getAddress server)))]
      (try
        (run-tests base node)
        (event-time-tests node)
        (finally
          (srv/stop-server! server)))))
  (let [results @!results
        failures (remove :ok? results)]
    (println (format "%n%d/%d PASS" (- (count results) (count failures)) (count results)))
    (shutdown-agents)
    (System/exit (if (seq failures) 1 0))))
