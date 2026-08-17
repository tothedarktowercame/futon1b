(ns test-candidate-index
  "Throwaway-node tests for candidate-index storage and C6 basis commits.

  Run: clojure -M:node -m test-candidate-index"
  (:require [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [futon1b-text :as text]
            [xtdb.api :as xt]
            [xtdb.node :as xtn])
  (:import [java.nio.file Files]
           [java.time Instant]))

(def ^:private unqualified {:builder-fn rs/as-unqualified-maps})

(defn- temp-dir []
  (-> (Files/createTempDirectory
       "futon1b-candidate-index-test-"
       (make-array java.nio.file.attribute.FileAttribute 0))
      .toFile
      .getAbsolutePath))

(defn- check! [label value]
  (println (format "  %-62s %s" label (if value "PASS" "FAIL")))
  (assert value label))

(defn- priv [sym]
  (ns-resolve 'futon1b-text sym))

(defn- ids-in [ds table]
  (set (map :id (jdbc/execute! ds [(str "SELECT id FROM " table)]
                                    unqualified))))

(defn- row-count [ds sql & params]
  (long (:n (jdbc/execute-one! ds (into [sql] params) unqualified))))

(defn- meta-map [ds]
  (into {}
        (map (juxt :k :v))
        (jdbc/execute! ds ["SELECT k, v FROM fts_meta"] unqualified)))

(defn- doc [id at tags]
  {:xt/id id
   :evidence/id id
   :evidence/type :claim
   :evidence/claim-type :observation
   :evidence/author :joe
   :evidence/session-id "candidate-index-test"
   :evidence/at at
   :evidence/tags tags
   :evidence/subject {:ref/type :thread :ref/id "thread-1"}
   :evidence/pattern-id :agent/pause
   :evidence/ephemeral? true
   :evidence/conjecture? false
   :evidence/body (str "candidate index body " id)})

(defn- test-three-table-batch! [ds]
  (let [index-batch! (priv 'index-batch!)
        a (doc "candidate-a" "2026-08-17T00:00:00Z" [:alpha :beta :gamma])
        b (doc "candidate-b" "2026-08-17T00:01:00Z" [:beta])]
    (index-batch! ds [a b])
    (let [fts-ids (ids-in ds "ev_fts")
          attr-ids (ids-in ds "ev_attr")
          tag-ids (ids-in ds "ev_tags")]
      (check! "one batch leaves ev_fts, ev_attr, and ev_tags on the same ids"
              (= #{"candidate-a" "candidate-b"} fts-ids attr-ids tag-ids)))
    (check! "a document with N tags produces exactly N ev_tags rows"
            (= 3 (row-count ds
                            "SELECT count(*) AS n FROM ev_tags WHERE id = ?"
                            "candidate-a")))
    (let [attr (jdbc/execute-one!
                ds
                ["SELECT type, claim_type, author, subject_type, pattern_id,
                         ephemeral, conjecture
                  FROM ev_attr WHERE id = ?"
                 "candidate-a"]
                unqualified)]
      (check! "attribute values retain keyword leading colons and int flags"
              (= {:type ":claim"
                  :claim_type ":observation"
                  :author ":joe"
                  :subject_type ":thread"
                  :pattern_id ":agent/pause"
                  :ephemeral 1
                  :conjecture 0}
                 attr)))
    (index-batch! ds [(assoc a :evidence/tags [:gamma])])
    (check! "re-indexing replaces tag rows instead of duplicating them"
            (and (= 1 (row-count ds
                                 "SELECT count(*) AS n FROM ev_tags WHERE id = ?"
                                 "candidate-a"))
                 (= 2 (row-count ds "SELECT count(*) AS n FROM ev_fts"))
                 (= 2 (row-count ds "SELECT count(*) AS n FROM ev_attr"))))))

(defn- test-completed-basis! [node]
  (let [scan-after (priv 'scan-after)
        before (select-keys
                (xt/status node)
                [:latest-completed-txs :latest-submitted-msg-ids
                 :latest-processed-msg-ids])
        calls (atom 0)
        indexed-doc (doc "basis-doc" "2026-08-17T01:00:00Z" [:basis])]
    (with-redefs-fn
      {scan-after
       (fn [scan-node _after _page]
         (if (= 1 (swap! calls inc))
           (do
             (xt/execute-tx
              scan-node
              [[:put-docs :evidence
                (doc "after-basis-capture" "2026-08-17T01:01:00Z" [:later])]])
             [indexed-doc])
           []))}
      #(text/catch-up! node :page 1))
    (let [{:keys [tx captured-at]} (:basis (text/stats))
          after (select-keys
                 (xt/status node)
                 [:latest-completed-txs :latest-submitted-msg-ids
                  :latest-processed-msg-ids])]
      (check! "completed catch-up commits the status captured before scanning"
              (= (pr-str before) tx))
      (check! "basis does not over-claim a transaction submitted mid-scan"
              (not= (pr-str after) tx))
      (check! "completed catch-up records a parseable basis capture instant"
              (instance? Instant (Instant/parse captured-at))))))

(defn- test-failed-basis! [node ds]
  (let [meta-set! (priv 'meta-set!)
        scan-after (priv 'scan-after)
        checkpoint-at "2026-08-17T02:00:00Z"
        checkpoint-id "checkpoint-before-failure"
        old-basis "{:latest-completed-txs {:foo 7}}"
        old-captured-at "2026-08-17T02:00:01Z"
        calls (atom 0)]
    (meta-set! ds "last-at" checkpoint-at)
    (meta-set! ds "last-id" checkpoint-id)
    (meta-set! ds "basis-tx" old-basis)
    (meta-set! ds "basis-captured-at" old-captured-at)
    (let [before (meta-map ds)
          threw?
          (try
            (with-redefs-fn
              {scan-after
               (fn [_node _after _page]
                 (if (= 1 (swap! calls inc))
                   [(doc "indexed-before-failure"
                         "2026-08-17T02:01:00Z" [:failure])]
                   (throw (ex-info "mid-scan failure" {}))))}
              #(text/catch-up! node :page 1))
            false
            (catch clojure.lang.ExceptionInfo _ true))
          after (meta-map ds)]
      (check! "the injected catch-up failure reaches the caller" threw?)
      (check! "mid-scan failure advances neither checkpoint nor basis"
              (= (select-keys before
                              ["last-at" "last-id" "basis-tx"
                               "basis-captured-at"])
                 (select-keys after
                              ["last-at" "last-id" "basis-tx"
                               "basis-captured-at"]))))))

(defn -main [& _]
  (let [dir (temp-dir)]
    (with-open [node (xtn/start-node)]
      (text/init! {:path (str dir "/fts5-evidence.db")})
      (let [ds @(var-get (priv '!ds))]
        (check! "stats is nil-safe before basis metadata exists"
                (= {:tx nil :captured-at nil} (:basis (text/stats))))
        (test-three-table-batch! ds)
        (test-completed-basis! node)
        (test-failed-basis! node ds))))
  (println "CANDIDATE INDEX: ALL PASS")
  (shutdown-agents))
