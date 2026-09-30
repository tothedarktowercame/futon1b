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
  (:require [clojure.string :as str]
            [futon1b-text :as text]
            [futon1b-hxindex :as hx]
            [futon1b-graph :as graph]
            [futon1b-xt :as fxt]
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
                     (cond-> (hx-doc (format "hx:e1:%02d" i) :probe/edits ["E1" "SHARED"])
                       ;; one row carries props/repo so the fields projection
                       ;; has something to select
                       (= i 0) (assoc :hx/props {:note "p2b"}
                                      :prop/repo "repo-a"))])
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

      ;; ---- P2b: fields projection parity ------------------------------------
      ;; The full read selects only the columns the fields need and projects
      ;; exactly as the scan path does.
      (let [opts {:type "probe/edits" :end "E1" :limit 3
                  :fields ["hx/id" "hx/type" "hx/props.note" "prop/repo"]}
            indexed (query node opts)
            _ (reset! reads-enabled false)
            scanned (query node opts)
            _ (reset! reads-enabled true)]
        (check! "fields: the index served it"
                (some? (:hx-index indexed)))
        (check! "fields: same projected rows, same order, same count"
                (and (= 3 (:count indexed) (:count scanned))
                     (= (:hyperedges indexed) (:hyperedges scanned))))
        (check! "fields: only the requested fields are present"
                (= {:hx/id "hx:e1:00" :hx/type :probe/edits
                    :hx/props {:note "p2b"} :prop/repo "repo-a"}
                   (first (:hyperedges indexed)))))

      ;; ---- P2b: the full-document read runs only for returned ids -----------
      ;; limit 2 of 5 candidates: the narrow re-check sees the first keyset
      ;; page, and the `SELECT *` read must receive exactly the two returned
      ;; ids, not all five candidates.
      (let [full-read-ids (atom [])
            counting-qf (fn [n qa]
                          (when (and (string? (first qa))
                                     (str/starts-with?
                                      (first qa)
                                      "SELECT * FROM hyperedges WHERE _id IN"))
                            (swap! full-read-ids into (rest qa)))
                          (fxt/safe-q n qa))
            _ (graph/invalidate-hyperedge-query-cache!)
            r (graph/hyperedges-query
               node {:type "probe/edits" :end "E1" :limit 2} counting-qf)]
        (check! "counting wrapper: the index served it"
                (some? (:hx-index r)))
        (check! "the full-document read ran only for the returned ids"
                (and (= 2 (:count r))
                     (= (sort @full-read-ids)
                        (sort (map :hx/id (:hyperedges r)))))))

      ;; ---- P2b: stale by endpoints-changed-unhooked -------------------------
      ;; Rewrite hx:e1:02's endpoints in XTDB with no hook and no catch-up:
      ;; the index still lists it under E1. The narrow re-check (id, type,
      ;; endpoints) must drop it — no full document is needed to see the
      ;; endpoints moved.
      (xt/execute-tx node [[:put-docs :hyperedges
                            (hx-doc "hx:e1:02" :probe/edits ["ELSEWHERE"])]])
      (let [candidates (hx/type-end-candidates
                        {:type :probe/edits :endpoints ["E1"] :after "" :fetch 10})]
        (check! "the index still holds the endpoint-changed candidate (planted)"
                (some #{"hx:e1:02"} candidates)))
      (let [indexed (query node {:type "probe/edits" :end "E1"})
            _ (reset! reads-enabled false)
            scanned (query node {:type "probe/edits" :end "E1"})
            _ (reset! reads-enabled true)]
        (check! "endpoint-changed unhooked candidate dropped by the narrow re-check"
                (not-any? #(= "hx:e1:02" %) (map :hx/id (:hyperedges indexed))))
        (check! "indexed and scan paths agree exactly after the endpoints drop"
                (= (comparable indexed) (comparable scanned))))
      ;; restore the row so later checks see the full fixture
      (xt/execute-tx node [[:put-docs :hyperedges
                            (hx-doc "hx:e1:02" :probe/edits ["E1" "SHARED"])]])
      (hx/catch-up! node)

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

      ;; ---- P3: one XTDB read per candidate page ----------------------------
      ;; The type re-check and the body read are a single `_id IN` read (each
      ;; is a whole-table scan live). A type-only read whose candidates fit
      ;; one page makes exactly one XTDB call, with or without `fields`.
      (doseq [fields [nil ["hx/endpoints"]]]
        (let [calls (atom 0)
              counting (fn [n q] (swap! calls inc) (futon1b-xt/safe-q n q))
              _ (graph/invalidate-hyperedge-query-cache!)
              r (graph/hyperedges-query node (cond-> {:type "probe/edits" :limit 10}
                                               fields (assoc :fields fields))
                                        counting)]
          (check! (str "P3: an indexed type-only read makes ONE XTDB read (fields "
                       (pr-str fields) ")")
                  (and (some? (:hx-index r)) (= 6 (count (:hyperedges r))) (= 1 @calls)))))

      ;; ---- P3: type-only reads from the index -------------------------------
      ;; Fixture census: probe/edits has 6 rows (hx:e1:00..04, hx:e2),
      ;; probe/commits 1, probe/other 1 (added in the hook-failure section —
      ;; this block runs BEFORE it, so probe/other does not exist yet).
      (let [opts {:type "probe/edits" :limit 2}
            indexed (query node opts)
            _ (reset! reads-enabled false)
            scanned (query node opts)
            _ (reset! reads-enabled true)]
        (check! "P3: the index served a type-only limited read"
                (some? (:hx-index indexed)))
        (check! "P3: page 1 rows, order, count, cursor match the scan path"
                (= (comparable indexed) (comparable scanned)))
        (check! "P3: page 1 emits the expected cursor"
                (= "hx:e1:01" (:next-cursor indexed))))

      ;; cursor-driven page 2, with and without fields
      (let [opts {:type "probe/edits" :limit 2 :after "hx:e1:01"}
            indexed (query node opts)
            _ (reset! reads-enabled false)
            scanned (query node opts)
            _ (reset! reads-enabled true)]
        (check! "P3: page 2 (cursor) matches the scan path"
                (= (comparable indexed) (comparable scanned)))
        (check! "P3: page 2 rows are the expected window"
                (= ["hx:e1:02" "hx:e1:03"] (mapv :hx/id (:hyperedges indexed)))))
      (let [opts {:type "probe/edits" :limit 2 :after "hx:e1:01"
                  :fields ["hx/id" "hx/type" "hx/props.note" "prop/repo"]}
            indexed (query node opts)
            _ (reset! reads-enabled false)
            scanned (query node opts)
            _ (reset! reads-enabled true)]
        (check! "P3: page 2 with fields matches the scan path"
                (= (comparable indexed) (comparable scanned))))

      ;; include-total: exact sidecar count vs the scan path's XTDB census
      (let [opts {:type "probe/edits" :limit 2 :include-total? true}
            indexed (query node opts)
            _ (reset! reads-enabled false)
            scanned (query node opts)
            _ (reset! reads-enabled true)]
        (check! "P3: include-total count and count-exact? match the scan path"
                (and (= 6 (:count indexed) (:count scanned))
                     (= 6 (hx/type-count :probe/edits))
                     (true? (:count-exact? indexed))
                     (= (:count-exact? scanned) (:count-exact? indexed))))
        (check! "P3: include-total still returns the limited window"
                (= 2 (count (:hyperedges indexed)))))

      ;; cross-path cursor: a cursor minted by the INDEXED path must resume
      ;; correctly on the scan path (and vice versa)
      (let [indexed-p1 (query node {:type "probe/edits" :limit 2})
            cursor (:next-cursor indexed-p1)
            _ (reset! reads-enabled false)
            scanned-p2 (query node {:type "probe/edits" :limit 2 :after cursor})
            scanned-p1 (query node {:type "probe/edits" :limit 2})
            _ (reset! reads-enabled true)
            indexed-p2 (query node {:type "probe/edits" :limit 2
                                    :after (:next-cursor scanned-p1)})]
        (check! "P3: indexed cursor drives the scan path to the same page 2"
                (and (not (contains? scanned-p2 :hx-index))
                     (= ["hx:e1:02" "hx:e1:03"]
                        (mapv :hx/id (:hyperedges scanned-p2)))))
        (check! "P3: scan cursor drives the indexed path to the same page 2"
                (and (some? (:hx-index indexed-p2))
                     (= (comparable scanned-p2) (comparable indexed-p2)))))

      ;; P3 stale candidate: delete hx:e1:04 unhooked; the narrow (id, type)
      ;; re-check must drop it, and the window must backfill. (Bad case:
      ;; return candidates WITHOUT the re-check and this test fails — run
      ;; during development by stubbing the re-check to identity.)
      (xt/execute-tx node [[:delete-docs :hyperedges "hx:e1:04"]])
      (let [indexed (query node {:type "probe/edits" :limit 3})
            _ (reset! reads-enabled false)
            scanned (query node {:type "probe/edits" :limit 3})
            _ (reset! reads-enabled true)]
        (check! "P3: unhooked delete dropped from a type-only read"
                (not-any? #(= "hx:e1:04" %) (map :hx/id (:hyperedges indexed))))
        (check! "P3: indexed and scan paths agree exactly after the stale drop"
                (= (comparable indexed) (comparable scanned))))
      (xt/execute-tx node [[:put-docs :hyperedges
                            (hx-doc "hx:e1:04" :probe/edits ["E1" "SHARED"])]])
      (hx/catch-up! node)
      ;; type changed unhooked: re-listed under the OLD type, re-check drops it
      (xt/execute-tx node [[:put-docs :hyperedges
                            (hx-doc "hx:e1:04" :probe/moved ["E1"])]])
      (let [limited (query node {:type "probe/edits" :limit 10})]
        (check! "P3: type-changed unhooked candidate dropped by the re-check"
                (not-any? #(= "hx:e1:04" %) (map :hx/id (:hyperedges limited)))))
      (xt/execute-tx node [[:put-docs :hyperedges
                            (hx-doc "hx:e1:04" :probe/edits ["E1" "SHARED"])]])
      (hx/catch-up! node)
      ;; as-of stays on the scan path for type-only reads too
      (check! "P3: type-only valid-as-of: scan path"
              (not (contains? (query node {:type "probe/edits" :limit 2
                                           :valid-as-of (Instant/now)})
                              :hx-index)))


      (check! "type-only with a repo filter: scan path"
              (not (contains? (query node {:type "probe/edits" :limit 2
                                           :repo "repo-a"})
                              :hx-index)))
      (check! "type-only with a mission filter: scan path"
              (not (contains? (query node {:type "probe/edits" :limit 2
                                           :mission "m1"})
                              :hx-index)))
      (check! "type-only latest: scan path"
              (not (contains? (query node {:type "probe/edits" :latest? true})
                              :hx-index)))
      (check! "type-only without a limit: scan path"
              (not (contains? (query node {:type "probe/edits"})
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

      ;; ---- P3c: census from the sidecar incl. zero-endpoint hyperedges -------
      ;; hx:zero1 has an empty endpoints vector, hx:zero2 no endpoints key at
      ;; all. Both are invisible to hx_edge; hx_node must count them. These
      ;; are unhooked writes repaired by catch-up — the parity check below is
      ;; the "unhooked writes + catch-up" case.
      (xt/execute-tx node [[:put-docs :hyperedges (hx-doc "hx:zero1" :probe/edits [])]
                           [:put-docs :hyperedges {:xt/id "hx:zero2" :hx/id "hx:zero2"
                                                   :hx/type :probe/edits}]])
      (hx/catch-up! node)
      (check! "P3c: type-count includes zero-endpoint hyperedges (6 + 2)"
              (= 8 (hx/type-count :probe/edits)))
      (let [indexed (graph/census node {:type "probe/edits"})
            _ (reset! reads-enabled false)
            scanned (graph/census node {:type "probe/edits"})
            _ (reset! reads-enabled true)]
        (check! "P3c: census served from the sidecar (:hx-index present)"
                (some? (:hx-index indexed)))
        (check! "P3c: census falls back to the scan when reads are disabled"
                (not (contains? scanned :hx-index)))
        (check! "P3c: census parity with the scan incl. zero-endpoint hyperedges"
                (= 8 (:count indexed) (:count scanned))))
      (check! "P3c: P3 include-total equals census on the same store"
              (= (:count (graph/census node {:type "probe/edits"}))
                 (:count (query node {:type "probe/edits" :limit 1
                                      :include-total? true}))))
      (let [ids (set (map :hx/id (:hyperedges
                                  (query node {:type "probe/edits" :limit 50}))))]
        (check! "P3c: zero-endpoint hyperedges are type-only read candidates"
                (and (contains? ids "hx:zero1") (contains? ids "hx:zero2"))))
      (check! "P3c: a zero-endpoint hyperedge is never a type+end match"
              (and (not-any? #{"hx:zero1" "hx:zero2"}
                             (hx/type-end-candidates
                              {:type :probe/edits :endpoints ["" "E1" "SHARED"]
                               :after "" :fetch 100}))
                   (empty? (:hyperedges
                            (query node {:type "probe/edits" :end ""
                                         :limit 50})))))
      ;; hooked write/delete: index-doc!/delete-id! are what the hooks run;
      ;; the census must move immediately, before any catch-up
      (let [ds @(var-get (ns-resolve 'futon1b-text '!ds))]
        (hx/index-doc! ds (hx-doc "hx:hooked:zero" :probe/edits []))
        (check! "P3c: a hooked zero-endpoint write is counted immediately"
                (= 9 (:count (graph/census node {:type "probe/edits"}))))
        (hx/delete-id! ds "hx:hooked:zero")
        (check! "P3c: a hooked delete is uncounted immediately"
                (= 8 (:count (graph/census node {:type "probe/edits"})))))
      ;; BAD CASE (run during development): drop the zero-endpoint handling —
      ;; e.g. the hx_node upsert in repair-ids! or the zero-endpoint walk in
      ;; backfill-hx-node! — and "census parity with the scan incl.
      ;; zero-endpoint hyperedges" FAILs (7 or 6 vs 8). Verified by stubbing.

      ;; ---- P3d: endpoint-prefix reads (type + end-prefix) -------------------
      ;; Fixture: probe/prefix rows with a repo-shaped prefix family
      ;; (code/v05/edits/…), a non-ASCII prefix family (dir:abcd→…), one
      ;; endpoint that IS the prefix (dir:abcd), one endpoint equal to the
      ;; prefix minus the trailing separator (code/v05/edits — excluded by
      ;; the slashed prefix), and a matching endpoint under a DIFFERENT type
      ;; (hx:q:00 — must never leak into a probe/prefix read).
      (xt/execute-tx
       node
       [[:put-docs :hyperedges (hx-doc "hx:p:00" :probe/prefix ["code/v05/edits/aaa" "other"])]
        [:put-docs :hyperedges (hx-doc "hx:p:01" :probe/prefix ["code/v05/edits/bbb"])]
        [:put-docs :hyperedges (hx-doc "hx:p:02" :probe/prefix ["code/v06/edits/ccc"])]
        [:put-docs :hyperedges (hx-doc "hx:p:03" :probe/prefix ["dir:abcd→file1"])]
        [:put-docs :hyperedges (hx-doc "hx:p:04" :probe/prefix ["dir:abcd→file2"])]
        [:put-docs :hyperedges (hx-doc "hx:p:05" :probe/prefix ["dir:abcd"])]
        [:put-docs :hyperedges (hx-doc "hx:p:06" :probe/prefix ["code/v05/edits"])]
        [:put-docs :hyperedges (hx-doc "hx:p:07" :probe/prefix ["code/v05/edits/ddd"])]
        [:put-docs :hyperedges (hx-doc "hx:p:08" :probe/prefix ["code/v05/edits/eee"])]
        [:put-docs :hyperedges (hx-doc "hx:q:00" :probe/other ["code/v05/edits/zzz"])]])
      (hx/catch-up! node)

      ;; membership parity: the prefix read (ALL pages) returns exactly the
      ;; set a full scan of the type filtered in Clojure by str/starts-with?
      ;; gives. There is no server-side scan path for end-prefix, so the
      ;; baseline is computed here from a type-only scan.
      (let [prefix "code/v05/edits/"
            paged (loop [after nil acc []]
                    (let [r (query node (cond-> {:type "probe/prefix"
                                                 :end-prefix prefix
                                                 :limit 2}
                                          after (assoc :after after)))]
                      (if (:next-cursor r)
                        (recur (:next-cursor r) (into acc (:hyperedges r)))
                        (into acc (:hyperedges r)))))
            _ (reset! reads-enabled false)
            all (query node {:type "probe/prefix" :limit 50})
            _ (reset! reads-enabled true)
            expected (->> (:hyperedges all)
                          (filter #(some (fn [e] (str/starts-with? (str e) prefix))
                                         (:hx/endpoints %)))
                          (map :hx/id)
                          set)
            paged-ids (map :hx/id paged)]
        (check! "P3d: the index served the prefix read (:hx-index present)"
                (some? (:hx-index (query node {:type "probe/prefix"
                                               :end-prefix prefix :limit 2}))))
        (check! "P3d: all-pages prefix read == scan filtered by str/starts-with?"
                (= expected (set paged-ids)))
        (check! "P3d: cursor paging covers each id exactly once"
                (and (= (count paged-ids) (count (set paged-ids)))
                     (= 4 (count paged-ids)))))

      ;; include-total: exact sidecar count over the range
      (let [r (query node {:type "probe/prefix" :end-prefix "code/v05/edits/"
                           :limit 2 :include-total? true})]
        (check! "P3d: include-total equals the set size (count(DISTINCT hx_id))"
                (and (= 4 (:count r))
                     (true? (:count-exact? r))
                     (= 4 (hx/type-end-prefix-count
                           :probe/prefix "code/v05/edits/"))
                     (= 2 (count (:hyperedges r))))))

      ;; non-ASCII prefix and a prefix that is itself a full endpoint
      (let [r (query node {:type "probe/prefix" :end-prefix "dir:abcd→" :limit 10})]
        (check! "P3d: non-ASCII prefix (dir:abcd→) matches only the → endpoints"
                (= ["hx:p:03" "hx:p:04"] (mapv :hx/id (:hyperedges r)))))
      (let [r (query node {:type "probe/prefix" :end-prefix "dir:abcd" :limit 10})]
        (check! "P3d: a prefix that is itself a full endpoint includes it"
                (= ["hx:p:03" "hx:p:04" "hx:p:05"]
                   (mapv :hx/id (:hyperedges r)))))
      (let [r (query node {:type "probe/prefix" :end-prefix "code/v05/edits" :limit 10})]
        (check! "P3d: prefix without the trailing separator also matches the exact endpoint"
                (= ["hx:p:00" "hx:p:01" "hx:p:06" "hx:p:07" "hx:p:08"]
                   (mapv :hx/id (:hyperedges r)))))

      ;; stale candidate: delete hx:p:01 unhooked; the narrow (id, type,
      ;; endpoints) re-check must drop it and the window must backfill.
      (xt/execute-tx node [[:delete-docs :hyperedges "hx:p:01"]])
      (let [cands (hx/type-end-prefix-candidates
                   {:type :probe/prefix :prefix "code/v05/edits/"
                    :after "" :fetch 10})]
        (check! "P3d: the index still holds the stale candidate (planted)"
                (some #{"hx:p:01"} cands)))
      (let [r (query node {:type "probe/prefix" :end-prefix "code/v05/edits/"
                           :limit 10})]
        (check! "P3d: stale candidate dropped by the narrow re-check"
                (= ["hx:p:00" "hx:p:07" "hx:p:08"]
                   (mapv :hx/id (:hyperedges r)))))
      (xt/execute-tx node [[:put-docs :hyperedges
                            (hx-doc "hx:p:01" :probe/prefix ["code/v05/edits/bbb"])]])
      (hx/catch-up! node)

      ;; refusals
      (check! "P3d: empty end-prefix refused at layer 4 (400), no type scan"
              (try (query node {:type "probe/prefix" :end-prefix "" :limit 2})
                   false
                   (catch clojure.lang.ExceptionInfo e
                     (= 4 (get-in (ex-data e) [:error :layer])))))
      (check! "P3d: end-prefix without type refused at layer 4 (400)"
              (try (query node {:end-prefix "code/v05/" :limit 2})
                   false
                   (catch clojure.lang.ExceptionInfo e
                     (= 4 (get-in (ex-data e) [:error :layer])))))
      (let [before (:hook-failures @stats-var)]
        (swap! stats-var update :hook-failures inc)
        (check! "P3d: unusable index → typed 503 refusal, not a slow scan"
                (try (query node {:type "probe/prefix"
                                  :end-prefix "code/v05/edits/" :limit 2})
                     false
                     (catch clojure.lang.ExceptionInfo e
                       (let [err (:error (ex-data e))]
                         (and (= 0 (:layer err))
                              (= :hx-index-unusable (:reason err)))))))
        (swap! stats-var assoc :hook-failures before)
        (check! "P3d: reads-usable? restored for the sections below"
                (hx/reads-usable?)))
      ;; BAD CASE (verified during development): compute the upper bound
      ;; wrongly as (str prefix "z") in hx/prefix-successor. "dir:abcdz" is
      ;; BELOW "dir:abcd→…" (→ is U+2192 > \z), so "P3d: a prefix that is
      ;; itself a full endpoint includes it" FAILs (returns [hx:p:05]
      ;; instead of [hx:p:03 hx:p:04 hx:p:05]). (The →-suffixed prefix
      ;; survives that particular wrong bound only because the fixture's
      ;; suffix char \f < \z — the full-endpoint prefix is the reliable
      ;; detector.)

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
                       (= 2 (:count r)))))
        (check! "P3c: no hx_node backfill yet: census falls back to the scan"
                (let [r (graph/census node {:type "probe/edits"})]
                  (and (not (hx/node-index-ready?))
                       (not (contains? r :hx-index))
                       (= 8 (:count r)))))))

    (println "HX READS: ALL PASS"))
  (shutdown-agents))
