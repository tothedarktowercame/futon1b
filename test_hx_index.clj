(ns test-hx-index
  "Throwaway-node tests for the P1 hyperedge sidecar index (futon1b-hxindex):
   hook upsert/replace/delete, hook failure isolation + catch-up repair,
   tombstone-leg delete repair (and the bad case without it), rebuild counts.

  Run: clojure -M:node -m test-hx-index"
  (:require [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [futon1b-text :as text]
            [futon1b-hxindex :as hx]
            [futon1b-xt :as fxt]
            [xtdb.api :as xt]
            [xtdb.node :as xtn])
  (:import [java.nio.file Files]))

(def ^:private unqualified {:builder-fn rs/as-unqualified-maps})

(defn- temp-dir []
  (-> (Files/createTempDirectory
       "futon1b-hxindex-test-"
       (make-array java.nio.file.attribute.FileAttribute 0))
      .toFile
      .getAbsolutePath))

(defn- check! [label value]
  (println (format "  %-66s %s" label (if value "PASS" "FAIL")))
  (assert value label))

(defn- wait-for [timeout-ms pred]
  (let [deadline (+ (System/currentTimeMillis) (long timeout-ms))]
    (loop []
      (cond (pred) true
            (> (System/currentTimeMillis) deadline) false
            :else (do (Thread/sleep 100) (recur))))))

(defn- ds [] @(var-get (ns-resolve 'futon1b-text '!ds)))

(defn- rows-for [id]
  (->> (jdbc/execute! (ds) ["SELECT pos, endpoint FROM hx_edge
                             WHERE hx_id = ? ORDER BY pos" id]
                      unqualified)
       (mapv (juxt :pos :endpoint))))

(defn- row-count [id]
  (:n (jdbc/execute-one! (ds) ["SELECT count(*) AS n FROM hx_edge WHERE hx_id = ?" id]
                         unqualified)))

(defn- node-type [id]
  (:type (jdbc/execute-one! (ds) ["SELECT type FROM hx_node WHERE hx_id = ?" id]
                            unqualified)))

(defn- hx-doc [id type endpoints]
  {:xt/id id :hx/id id :hx/type type :hx/endpoints (vec endpoints)})

(defn- xtdb-type-counts [node]
  (into {}
        (map (fn [row] [(str (:type row)) (:n row)]))
        (fxt/timed-q node ["SELECT hx$type AS type, count(*) AS n
                            FROM hyperedges GROUP BY hx$type"])))

(defn- sidecar-type-counts []
  (into {}
        (map (fn [row] [(:type row) (:hyperedges row)]))
        (:per-type (hx/hx-stats))))

(defn -main [& _]
  (let [dir (temp-dir)]
    (with-open [node (xtn/start-node)]
      (text/init! {:path (str dir "/fts5-evidence.db")})
      (hx/init!)

      ;; ---- hook: put → rows with the right positions ----------------------
      (let [d (hx-doc "hx:put:1" :probe/edits ["a" "b" "c"])]
        (xt/execute-tx node [[:put-docs :hyperedges d]])
        (hx/on-put! d)
        (check! "hooked put lands rows in endpoint order"
                (wait-for 5000 #(= [[0 "a"] [1 "b"] [2 "c"]] (rows-for "hx:put:1"))))
        (check! "hooked put lands the hx_node row (P3c)"
                (wait-for 5000 #(= ":probe/edits" (node-type "hx:put:1")))))

      ;; ---- hook: re-put with changed endpoints → old gone, new present ----
      (let [d2 (hx-doc "hx:put:1" :probe/edits ["b" "d"])]
        (xt/execute-tx node [[:put-docs :hyperedges d2]])
        (hx/on-put! d2)
        (check! "re-put replaces rows (positions may change)"
                (wait-for 5000 #(= [[0 "b"] [1 "d"]] (rows-for "hx:put:1")))))

      ;; ---- hook: delete → rows gone ---------------------------------------
      (do (xt/execute-tx node [[:delete-docs :hyperedges "hx:put:1"]])
          (hx/on-delete! "hx:put:1")
          (check! "hooked delete drops every row"
                  (wait-for 5000 #(zero? (row-count "hx:put:1"))))
          (check! "hooked delete drops the hx_node row (P3c)"
                  (wait-for 5000 #(nil? (node-type "hx:put:1")))))

      ;; ---- hook missed: request unaffected, catch-up repairs upsert -------
      (let [d (hx-doc "hx:missed:1" :probe/edits ["x" "y"])]
        (xt/execute-tx node [[:put-docs :hyperedges d]])
        (check! "a put whose hook never ran is absent from the index"
                (zero? (row-count "hx:missed:1")))
        (let [res (hx/catch-up! node)]
          (check! "one catch-up repairs the missed put"
                  (= [[0 "x"] [1 "y"]] (rows-for "hx:missed:1")))
          (check! "catch-up repairs the missed put's hx_node row (P3c)"
                  (= ":probe/edits" (node-type "hx:missed:1")))
          (check! "catch-up reports its work" (pos? (long (or (:changed res) (:filled res)))))))

      ;; ---- hook throws: attributable, request already succeeded -----------
      (let [stats-var (var-get (ns-resolve 'futon1b-hxindex '!stats))
            before (:hook-failures @stats-var)
            text-ds (var-get (ns-resolve 'futon1b-text '!ds))
            good-ds @text-ds]
        (reset! text-ds
                (jdbc/get-datasource
                 {:dbtype "sqlite"
                  :dbname "/nonexistent-futon1b-dir/does-not-exist.db"}))
        (try
          (hx/on-put! (hx-doc "hx:boom:1" :probe/edits ["z"]))
          (check! "a throwing hook records WHICH id failed"
                  (wait-for 5000
                            #(= "hx:boom:1" (:id (:last-hook-error @stats-var)))))
          (check! "the hook failure counter advances"
                  (> (long (:hook-failures @stats-var)) (long before)))
          (finally (reset! text-ds good-ds))))

      ;; ---- delete repair rides the _system_to leg -------------------------
      ;; hx:missed:1 is in the index (catch-up above). Delete it in the store
      ;; with no hook: only the tombstone query can see that (P0 §3).
      (do (xt/execute-tx node [[:delete-docs :hyperedges "hx:missed:1"]])
          (check! "deleted id still indexed before catch-up"
                  (pos? (row-count "hx:missed:1")))

          ;; ** BAD CASE **: catch-up reading only _system_from must NOT
          ;; repair the delete.
          (hx/catch-up! node :force true :tombstones? false)
          (check! "BAD CASE: without the tombstone leg the delete is NOT repaired"
                  (pos? (row-count "hx:missed:1")))
          (check! "BAD CASE: hx_node also keeps the deleted id"
                  (some? (node-type "hx:missed:1")))

          ;; restored: both legs
          (hx/catch-up! node :force true)
          (check! "with the tombstone leg restored the delete repairs"
                  (zero? (row-count "hx:missed:1")))
          (check! "with the tombstone leg restored the hx_node row repairs"
                  (nil? (node-type "hx:missed:1"))))

      ;; ---- no-new-tx guard --------------------------------------------------
      (check! "a catch-up with no new transaction skips"
              (= :no-new-tx (:skipped (hx/catch-up! node))))

      ;; ---- rebuild from empty reproduces per-type store counts ------------
      (do (xt/execute-tx node [[:put-docs :hyperedges
                                (hx-doc "hx:r1" :probe/commits ["c1"])]
                               [:put-docs :hyperedges
                                (hx-doc "hx:r2" :probe/commits ["c1" "c2"])]
                               [:put-docs :hyperedges
                                (hx-doc "hx:r3" :probe/other ["o1"])]])
          (hx/catch-up! node)
          (let [res (hx/rebuild! node)
                expected (xtdb-type-counts node)
                got (sidecar-type-counts)]
            (println "  rebuild on test store took"
                     (:total-elapsed-ms res) "ms")
            (check! "rebuild from empty reproduces per-type XTDB counts"
                    (= expected got))
            (check! "rebuild saw every type" (= 2 (count got)))
            ;; review of 106fa1d: an empty index fills from current rows, not
            ;; by walking every version from the epoch one scan per page
            (check! "rebuild of an empty index goes through the fill"
                    (pos? (long (or (:filled res) 0))))
            (check! "the fill keeps endpoint positions"
                    (= [[0 "c1"] [1 "c2"]]
                       (rows-for "hx:r2")))
            ;; writes after the fill are repaired by the next catch-up
            (xt/execute-tx node [[:put-docs :hyperedges
                                  (hx-doc "hx:r4" :probe/other ["o4"])]
                                 [:delete-docs :hyperedges "hx:r3"]])
            (hx/catch-up! node)
            (check! "a put after the fill is caught up" (= 1 (row-count "hx:r4")))
            (check! "a delete after the fill is caught up" (zero? (row-count "hx:r3")))))

      ;; ---- P3c: zero-endpoint hyperedges ------------------------------------
      ;; A hyperedge with no endpoints has NO hx_edge rows; hx_node must
      ;; still carry it, from every write path.
      (let [d (hx-doc "hx:zero:1" :probe/bare [])]
        (xt/execute-tx node [[:put-docs :hyperedges d]])
        (hx/on-put! d)
        (check! "hooked zero-endpoint put: no hx_edge rows but an hx_node row"
                (wait-for 5000 #(and (zero? (row-count "hx:zero:1"))
                                     (= ":probe/bare" (node-type "hx:zero:1")))))
        (check! "type-count counts the zero-endpoint hyperedge"
                (= 1 (hx/type-count :probe/bare)))
        (check! "type-end-candidates never returns a zero-endpoint hyperedge"
                (not-any? #{"hx:zero:1"}
                          (hx/type-end-candidates
                           {:type :probe/bare :endpoints [""] :after "" :fetch 10})))
        ;; unhooked retype: catch-up moves the hx_node row
        (xt/execute-tx node [[:put-docs :hyperedges
                              (hx-doc "hx:zero:1" :probe/moved [])]])
        (hx/catch-up! node)
        (check! "catch-up retypes a zero-endpoint hyperedge in hx_node"
                (and (= ":probe/moved" (node-type "hx:zero:1"))
                     (zero? (hx/type-count :probe/bare))
                     (= 1 (hx/type-count :probe/moved)))))

      ;; ---- periodic: a skipped run is recorded and retried ------------------
      (let [calls (atom 0)]
        (hx/stop-periodic-catch-up!)
        (hx/start-periodic-catch-up!
         node :interval-ms 300 :skip-retry-ms 10
         :catch-up-fn (fn [] (if (< (swap! calls inc) 3)
                               {:skipped :expensive-read-busy}
                               {:changed 0})))
        ;; one scheduled run lands at 300 ms; only the skip retries (10 ms
        ;; apart) can reach 3 calls before 450 ms
        (Thread/sleep 450)
        (hx/stop-periodic-catch-up!)
        (let [lp (:last-periodic (hx/hx-stats))]
          (check! "periodic catch-up records a skip and retries it within the interval"
                  (and (>= @calls 3)
                       (nil? (get-in lp [:result :skipped]))
                       (some? (:at lp))))))

      ;; ---- P4: oracle --------------------------------------------------------
      (let [o (hx/oracle node)]
        (check! "P4: oracle is clean after fill + hooked writes/retracts"
                (and (empty? (:mismatches o))
                     (empty? (:endpoint-mismatches o))))
        (check! "P4: oracle reports shape (types, sampled ids, checkpoint, elapsed)"
                (and (pos? (long (:types-checked o)))
                     (pos? (long (:sampled-ids o)))
                     (some? (:checkpoint o))
                     (number? (:elapsed-ms o))))
        (check! "P4: hx-stats records :last-oracle"
                (some? (:last-oracle (hx/hx-stats)))))

      ;; ** BAD CASE A **: an hx_node row deleted behind the index's back.
      (jdbc/execute! (ds) ["DELETE FROM hx_node WHERE hx_id = ?" "hx:r1"])
      (let [o (hx/oracle node)]
        (check! "P4 BAD CASE A: deleted hx_node row caught by the per-type count check"
                (some #(and (= ":probe/commits" (:type %))
                            (= 1 (:sidecar %)) (= 2 (:xtdb %)))
                      (:mismatches o)))
        (check! "P4 BAD CASE A: the count check re-checked once after a catch-up"
                (every? :rechecked (:mismatches o))))
      (let [o (hx/oracle node :recheck false)]
        (check! "P4: :recheck false is strictly read-only (no drain attempt)"
                (some #(false? (:rechecked %)) (:mismatches o))))
      (hx/rebuild! node)
      (check! "P4: rebuild after a deleted hx_node row leaves the oracle clean"
              (let [o (hx/oracle node)]
                (and (empty? (:mismatches o))
                     (empty? (:endpoint-mismatches o)))))

      ;; ** BAD CASE B **: an extra hx_edge endpoint row planted.
      (jdbc/execute! (ds) ["INSERT INTO hx_edge(hx_id, type, pos, endpoint)
                            VALUES ('hx:r2', ':probe/commits', 5, 'planted:endpoint')"])
      (let [o (hx/oracle node)]
        (check! "P4 BAD CASE B: planted hx_edge row caught by the endpoint sample check"
                (some #(and (= "hx:r2" (:id %))
                            (= ["c1" "c2" "planted:endpoint"] (:sidecar %))
                            (= ["c1" "c2"] (:xtdb %)))
                      (:endpoint-mismatches o)))
        (check! "P4 BAD CASE B: hx_node counts stay clean (the count check cannot see it)"
                (empty? (:mismatches o))))
      (hx/rebuild! node)

      ;; ** BAD CASE C **: a type's count off by one (planted hx_node row).
      (jdbc/execute! (ds) ["INSERT INTO hx_node(hx_id, type)
                            VALUES ('hx:fake:1', ':probe/commits')"])
      (let [o (hx/oracle node)]
        (check! "P4 BAD CASE C: planted hx_node row caught by the per-type count check"
                (some #(and (= ":probe/commits" (:type %))
                            (= 3 (:sidecar %)) (= 2 (:xtdb %)))
                      (:mismatches o)))
        (check! "P4 BAD CASE C: the endpoint sample check corroborates (no XTDB doc)"
                (some #(and (= "hx:fake:1" (:id %)) (nil? (:xtdb %)))
                      (:endpoint-mismatches o))))

      ;; full rebuild after planted damage reproduces XTDB counts exactly
      (let [res (hx/rebuild! node)
            o (hx/oracle node)]
        (check! "P4: full rebuild after planted damage reproduces XTDB counts (oracle clean)"
                (and (empty? (:mismatches o))
                     (empty? (:endpoint-mismatches o))
                     (empty? (:raced o))))
        (check! "P4: the post-damage rebuild refilled the index"
                (pos? (long (or (:filled res) 0)))))

      ;; ---- stats ------------------------------------------------------------
      (let [s (hx/hx-stats)]
        (check! "hx-stats reports per-type counts, checkpoint, hook failures"
                (and (seq (:per-type s))
                     (some? (get-in s [:checkpoint :ts]))
                     (number? (:hook-failures s))
                     (some? (get-in s [:last-catch-up :elapsed-ms]))))))
    (println "HX INDEX: ALL PASS"))
  (shutdown-agents))
