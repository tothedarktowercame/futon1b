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
                (wait-for 5000 #(= [[0 "a"] [1 "b"] [2 "c"]] (rows-for "hx:put:1")))))

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
                  (wait-for 5000 #(zero? (row-count "hx:put:1")))))

      ;; ---- hook missed: request unaffected, catch-up repairs upsert -------
      (let [d (hx-doc "hx:missed:1" :probe/edits ["x" "y"])]
        (xt/execute-tx node [[:put-docs :hyperedges d]])
        (check! "a put whose hook never ran is absent from the index"
                (zero? (row-count "hx:missed:1")))
        (let [res (hx/catch-up! node)]
          (check! "one catch-up repairs the missed put"
                  (= [[0 "x"] [1 "y"]] (rows-for "hx:missed:1")))
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

          ;; restored: both legs
          (hx/catch-up! node :force true)
          (check! "with the tombstone leg restored the delete repairs"
                  (zero? (row-count "hx:missed:1"))))

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

      ;; ---- stats ------------------------------------------------------------
      (let [s (hx/hx-stats)]
        (check! "hx-stats reports per-type counts, checkpoint, hook failures"
                (and (seq (:per-type s))
                     (some? (get-in s [:checkpoint :ts]))
                     (number? (:hook-failures s))
                     (some? (get-in s [:last-catch-up :elapsed-ms]))))))
    (println "HX INDEX: ALL PASS"))
  (shutdown-agents))
