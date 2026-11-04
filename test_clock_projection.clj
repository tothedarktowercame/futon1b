(ns test-clock-projection
  (:require [futon1b-graph :as graph]
            [futon1b-hxindex :as hx]
            [futon1b-text :as text]
            [xtdb.api :as xt]
            [xtdb.node :as xtn])
  (:import [java.nio.file Files]))

(defn- temp-dir []
  (-> (Files/createTempDirectory
       "futon1b-clock-projection-"
       (make-array java.nio.file.attribute.FileAttribute 0))
      .toFile .getAbsolutePath))

(defn- check! [label value]
  (println (format "  %-68s %s" label (if value "PASS" "FAIL")))
  (assert value label))

(defn- clock-doc [id agent target n]
  {:xt/id id :hx/id id :hx/type :clock/clocked-on
   :hx/endpoints [(str "agent:" agent) target]
   :hx/ends [(str "agent:" agent) target]
   :hx/props {:agent-id agent :session-id (str "s-" n)
              :mission-id target :clocked-at-ms n
              :witness (str "decision-" n)}})

(defn- uncached [node opts]
  (graph/invalidate-hyperedge-query-cache!)
  (graph/hyperedges-query node opts))

(defn- rows [response]
  (:hyperedges response))

(defn- without-marker [response]
  (dissoc response :clock-projection :hx-index))

(defn -main [& _]
  (let [dir (temp-dir)
        reads-enabled (var-get (ns-resolve 'futon1b-hxindex '!reads-enabled))]
    (with-open [node (xtn/start-node)]
      (text/init! {:path (str dir "/sidecar.db")})
      (hx/init!)
      (reset! reads-enabled true)

      ;; A predates the projection schema; catch-up performs the one-off
      ;; current-valid backfill before the normal incremental legs.
      (let [a (clock-doc "clock:001" "a1" "M-A" 1)]
        (xt/execute-tx node [[:put-docs :hyperedges a]]))
      (hx/catch-up! node :force true)
      (check! "one-off backfill marks the clock projection ready"
              (hx/clock-projection-ready?))

      ;; A switch ends A's current validity and puts B. The verified server
      ;; path calls these same hooks after the two XTDB transactions.
      (let [a (clock-doc "clock:001" "a1" "M-A" 1)
            b (clock-doc "clock:002" "a1" "M-B" 2)]
        (xt/execute-tx node [[:delete-docs :hyperedges (:xt/id a)]])
        @(hx/on-delete! (:xt/id a))
        (xt/execute-tx node [[:put-docs :hyperedges b]])
        @(hx/on-put! b)
        (let [projected (uncached node {:type "clock/clocked-on" :limit 50})
              _ (reset! reads-enabled false)
              hydrated (uncached node {:type "clock/clocked-on" :limit 50})
              _ (reset! reads-enabled true)]
          (check! "switch: projection serves B only"
                  (= ["clock:002"] (mapv :hx/id (rows projected))))
          (check! "switch: projection equals current-valid XTDB hydration"
                  (= (without-marker projected) (without-marker hydrated)))))

      ;; A stale/unusable sidecar must fall back, and say that it did.
      (reset! reads-enabled false)
      (let [fallback (uncached node {:type "clock/clocked-on" :limit 50})]
        (check! "stale sidecar: hydration path is marked"
                (= {:served? false :fallback :sidecar-not-current}
                   (:clock-projection fallback)))
        (check! "stale sidecar: current row still returned"
                (= ["clock:002"] (mapv :hx/id (rows fallback)))))
      (reset! reads-enabled true)

      ;; Add enough rows for two pages and compare both paths page by page.
      (doseq [n (range 3 8)]
        (let [doc (clock-doc (format "clock:%03d" n) (str "a" n)
                             (str "M-" n) n)]
          (xt/execute-tx node [[:put-docs :hyperedges doc]])
          @(hx/on-put! doc)))
      (let [p1 (uncached node {:type "clock/clocked-on" :limit 3})
            p2 (uncached node {:type "clock/clocked-on" :limit 3
                               :after (:next-cursor p1)})
            _ (reset! reads-enabled false)
            h1 (uncached node {:type "clock/clocked-on" :limit 3})
            h2 (uncached node {:type "clock/clocked-on" :limit 3
                               :after (:next-cursor h1)})
            _ (reset! reads-enabled true)]
        (check! "paging: first page rows and cursor equal hydration"
                (= (without-marker p1) (without-marker h1)))
        (check! "paging: second page rows and cursor equal hydration"
                (= (without-marker p2) (without-marker h2))))

      ;; Branch-local timing with 50 current clock rows. Query cache is flushed
      ;; before each measurement; only sidecar serving changes.
      (doseq [n (range 8 52)]
        (let [doc (clock-doc (format "clock:%03d" n) (str "a" n)
                             (str "M-" n) n)]
          (xt/execute-tx node [[:put-docs :hyperedges doc]])
          @(hx/on-put! doc)))
      (reset! reads-enabled false)
      (let [before (System/nanoTime)
            _ (uncached node {:type "clock/clocked-on" :limit 50})
            before-ms (/ (- (System/nanoTime) before) 1e6)
            _ (reset! reads-enabled true)
            after (System/nanoTime)
            result (uncached node {:type "clock/clocked-on" :limit 50})
            after-ms (/ (- (System/nanoTime) after) 1e6)]
        (check! "timing fixture returns 50 rows" (= 50 (count (rows result))))
        (println (format "  timing cold limit=50: hydration %.1f ms, projection %.1f ms"
                         before-ms after-ms))))
    (shutdown-agents)))
