(ns test-hx-reads
  "Throwaway-node tests for P2: type+end hyperedge reads served from the
   SQLite sidecar, re-checked against XTDB (DESIGN-hyperedge-scope-sidecar-
   2026-09-26 §7, futon1b-graph/hyperedges-indexed-type-end).

   Covers: parity (same rows/cursor/count as the scan path, including a
   request that supplies `after`), a planted stale candidate, as-of and
   end-less requests staying on the scan path, the index-unusable fallbacks
   (no checkpoint; hook failure since the last catch-up), and the bad case
   (see -main: the re-check is what drops the stale candidate).

  Run: clojure -M:node -m test-hx-reads"
  (:require [futon1b-text :as text]
            [futon1b-hxindex :as hx]
            [futon1b-graph :as graph]
            [xtdb.api :as xt]
            [xtdb.node :as xtn])
  (:import [java.nio.file Files]
           [java.time Instant]))

(defn- temp-dir []
  (-> (Files/createTempDirectory
       "futon1b-hxreads-test-"
       (make-array java.nio.file.attribute.FileAttribute 0))
      .toFile
      .getAbsolutePath))

(defn- check! [label value]
  (println (format "  %-72s %s" label (if value "PASS" "FAIL")))
  (assert value label))

(defn- hx-doc [id type endpoints]
  {:xt/id id :hx/id id :hx/type type :hx/endpoints (vec endpoints)})

(defn- query
  "Uncached hyperedges-query (the cache keys on opts, and these tests compare
   index-on against index-off with the SAME opts)."
  [node opts]
  (graph/invalidate-hyperedge-query-cache!)
  (graph/hyperedges-query node opts))

(defn- comparable
  "The scan-path shape: drop the index-only annotation before comparing."
  [r]
  (dissoc r :hx-index))

(defn -main [& _]
  (let [dir (temp-dir)
        reads-enabled (var-get (ns-resolve 'futon1b-hxindex '!reads-enabled))
        stats-var (var-get (ns-resolve 'futon1b-hxindex '!stats))]
    (with-open [node (xtn/start-node)]
      (text/init! {:path (str dir "/fts5-evidence.db")})
      (hx/init!)
      (reset! reads-enabled true)

      ;; ---- fixture: several types, several endpoints -----------------------
      ;; probe/edits at E1: five rows (windowing is exercised at limit 2).
      ;; probe/edits at E2: one row. probe/commits at E1: one row (a different
      ;; type sharing the endpoint — must not leak into a type+end read).
      (xt/execute-tx
       node
       (into [[:put-docs :hyperedges (hx-doc "hx:e2" :probe/edits ["E2"])]
              [:put-docs :hyperedges (hx-doc "hx:c1" :probe/commits ["E1"])]]
             (map (fn [i]
                    [:put-docs :hyperedges
                     (hx-doc (format "hx:e1:%02d" i) :probe/edits ["E1" "SHARED"])])
                  (range 5))))
      (hx/catch-up! node)

      ;; ---- parity: indexed path == scan path -------------------------------
      (let [opts {:type "probe/edits" :end "E1" :limit 2}
            indexed (query node opts)
            _ (reset! reads-enabled false)
            scanned (query node opts)
            _ (reset! reads-enabled true)]
        (check! "the index served it (:hx-index annotation present)"
                (some? (:hx-index indexed)))
        (check! "the scan path carries no annotation"
                (not (contains? scanned :hx-index)))
        (check! "same rows, same count (limit 2 of 5)"
                (and (= 2 (:count indexed) (:count scanned))
                     (= (:hyperedges indexed) (:hyperedges scanned))))
        (check! "same cursor (the end branch has none, on both paths)"
                (and (not (contains? indexed :next-cursor))
                     (not (contains? scanned :next-cursor))))
        (check! ":hx-index reports the checkpoint and zero hook failures"
                (and (some? (get-in indexed [:hx-index :checkpoint :ts]))
                     (= 0 (get-in indexed [:hx-index :hook-failures])))))

      ;; parity across the whole window: no limit → all five rows
      (let [indexed (query node {:type "probe/edits" :end "E1"})
            _ (reset! reads-enabled false)
            scanned (query node {:type "probe/edits" :end "E1"})
            _ (reset! reads-enabled true)]
        (check! "unlimited: same five rows in the same order"
                (and (= 5 (:count indexed))
                     (= (:hyperedges indexed) (:hyperedges scanned)))))

      ;; parity with `after` supplied: the existing end branch ignores it,
      ;; so the indexed path must ignore it identically (byte-for-byte)
      (let [opts {:type "probe/edits" :end "E1" :limit 3 :after "hx:e1:02"}
            indexed (query node opts)
            _ (reset! reads-enabled false)
            scanned (query node opts)
            _ (reset! reads-enabled true)]
        (check! "an `after` request returns identical windows on both paths"
                (= (comparable indexed) (comparable scanned))))

      ;; ---- planted stale candidate ------------------------------------------
      ;; Delete hx:e1:01 in XTDB with no hook and no catch-up: the index still
      ;; holds its rows. The re-check must drop it.
      (xt/execute-tx node [[:delete-docs :hyperedges "hx:e1:01"]])
      (let [candidates (hx/type-end-candidates
                        {:type :probe/edits :endpoints ["E1"] :after "" :fetch 10})]
        (check! "the index still holds the stale candidate (planted)"
                (some #{"hx:e1:01"} candidates)))
      (let [indexed (query node {:type "probe/edits" :end "E1"})
            _ (reset! reads-enabled false)
            scanned (query node {:type "probe/edits" :end "E1"})
            _ (reset! reads-enabled true)
            indexed-ids (mapv :hx/id (:hyperedges indexed))
            scanned-ids (mapv :hx/id (:hyperedges scanned))]
        (check! "the stale candidate is NOT returned by the indexed path"
                (not-any? #(= "hx:e1:01" %) indexed-ids))
        ;; the stale id occupied a window slot; the scan path backfills it
        ;; with the next live id, and so must the indexed path
        (check! "indexed and scan paths agree exactly after the stale drop"
                (and (= scanned-ids indexed-ids)
                     (= 4 (:count indexed) (:count scanned))))
        ;; restore the deleted row so later checks see the full fixture
        (xt/execute-tx node [[:put-docs :hyperedges
                              (hx-doc "hx:e1:01" :probe/edits ["E1" "SHARED"])]])
        (hx/catch-up! node))

      ;; ---- non-indexable parameter combinations stay on the scan path ------
      (check! "type without end: scan path"
              (not (contains? (query node {:type "probe/edits" :limit 2})
                              :hx-index)))
      (check! "end without type: scan path"
              (not (contains? (query node {:end "E1" :limit 2})
                              :hx-index)))
      (check! "valid-as-of: scan path"
              (not (contains? (query node {:type "probe/edits" :end "E1" :limit 2
                                           :valid-as-of (Instant/now)})
                              :hx-index)))
      (check! "system-as-of: scan path"
              (not (contains? (query node {:type "probe/edits" :end "E1" :limit 2
                                           :system-as-of (Instant/now)})
                              :hx-index)))

      ;; ---- unusable index ----------------------------------------------------
      ;; (a) a hook failure since the last catch-up: a fresh hyperedge could
      ;; be missing from the candidates in a way the re-check cannot see.
      (let [before (:hook-failures @stats-var)]
        (swap! stats-var update :hook-failures inc)
        (check! "reads-usable? is false after an unrepaired hook failure"
                (not (hx/reads-usable?)))
        (check! "a hook failure since the last catch-up falls back to the scan"
                (not (contains? (query node {:type "probe/edits" :end "E1" :limit 2})
                                :hx-index)))
        (swap! stats-var assoc :hook-failures before)
        ;; a catch-up re-baselines the failure count
        (xt/execute-tx node [[:put-docs :hyperedges
                              (hx-doc "hx:noop" :probe/other ["O"])]])
        (hx/catch-up! node)
        (check! "reads-usable? recovers once catch-up owns the failure count"
                (hx/reads-usable?)))

      ;; (b) no checkpoint at all: fresh sidecar, fill never ran.
      (let [dir2 (temp-dir)]
        (text/init! {:path (str dir2 "/fts5-evidence.db")})
        (hx/init!)
        (check! "no checkpoint: reads-usable? is false"
                (not (hx/reads-usable?)))
        (check! "no checkpoint: the scan path runs (results, no annotation)"
                (let [r (query node {:type "probe/edits" :end "E1" :limit 2})]
                  (and (not (contains? r :hx-index))
                       (= 2 (:count r)))))))

    (println "HX READS: ALL PASS"))
  (shutdown-agents))
