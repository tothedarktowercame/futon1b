(ns test-evidence-index-window
  "Evidence list reads served from the text sidecar alone (futon1b-evidence
  index-window), against the store scan they replace, and the conditions
  under which the sidecar may serve them (text/complete?).

  Run: clojure -M:node -m test-evidence-index-window"
  (:require [clojure.test :refer [deftest is testing run-tests]]
            [futon1b-evidence :as evidence]
            [futon1b-text :as text]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.node :as xtn])
  (:import [java.nio.file Files]
           [java.time Instant]))

(defn- temp-db []
  (str (-> (Files/createTempDirectory
            "futon1b-index-window-test-"
            (make-array java.nio.file.attribute.FileAttribute 0))
           .toFile .getAbsolutePath)
       "/fts5-evidence.db"))

(def ^:private base (Instant/parse "2026-09-01T00:00:00Z"))

(defn- at-hours [h] (str (.plusSeconds base (* 3600 h))))

(defn- fixture-doc [i]
  (cond-> {:xt/id (format "n-%04d" i)
           :evidence/id (format "n-%04d" i)
           ;; Pairs share an :at, so cursors cross (at, id) ties.
           :evidence/at (at-hours (quot i 2))
           :evidence/type (if (even? i) :coordination :note)
           :evidence/claim-type (if (zero? (mod i 4)) :observation :claim)
           :evidence/author (if (zero? (mod i 3)) "b" "a")
           :evidence/session-id (str "s" (mod i 3))
           :evidence/ephemeral? (zero? (mod i 7))
           :evidence/tags (cond-> []
                            (zero? (mod i 3)) (conj (if (odd? i) :alpha "alpha"))
                            (zero? (mod i 5)) (conj :beta))
           :evidence/body {:i i}}
    (zero? (mod i 11)) (assoc :evidence/subject {:ref/type :session :ref/id (str "r" i)})
    (zero? (mod i 13)) (assoc :evidence/fork-of "n-0003")))

(def ^:private n-docs 400)

(defn- run-query
  "Every page of Q at LIMIT, following cursors."
  [node q limit]
  (loop [cursor nil pages []]
    (let [params (cond-> (assoc q "limit" (str limit))
                   cursor (assoc "cursor-at" (:at cursor) "cursor-id" (:id cursor)))
          [status body] (evidence/query-evidence-response node params)
          pages (conj pages body)]
      (assert (= 200 status) (pr-str body))
      (if (and (:next-cursor body) (< (count pages) 500))
        (recur (:next-cursor body) pages)
        {:ids (mapv :evidence/id (mapcat :entries pages))
         :first-scanned (:scanned (first pages))}))))

(defn- first-page-ids [node q limit]
  (let [[status body] (evidence/query-evidence-response node (assoc q "limit" (str limit)))]
    (assert (= 200 status) (pr-str body))
    (mapv :evidence/id (:entries body))))

(defn- scan-path
  "Run F with the sidecar detached, so evidence reads take the store scan."
  [f]
  (let [ds @text/!ds]
    (reset! text/!ds nil)
    (try (f) (finally (reset! text/!ds ds)))))

(def ^:private queries
  [{}
   {"session-id" "s1"}
   {"session-id" "s1" "include-ephemeral" "false"}
   {"session-id" "s2" "author" "b"}
   {"author" "b" "tags" "alpha"}
   {"session-id" "s0" "tags" "alpha,beta"}
   {"type" "coordination" "claim-type" "observation"}
   {"tags" "beta" "include-ephemeral" "false"}
   {"subject-type" "session"}
   {"since" (at-hours 50) "before" (at-hours 120)}
   {"session-id" "s1" "fork-of" "n-0003"}
   {"session-id" "absent"}])

(defn- wait-for [ms pred]
  (let [deadline (+ (System/currentTimeMillis) ms)]
    (loop []
      (cond (pred) true
            (> (System/currentTimeMillis) deadline) false
            :else (do (Thread/sleep 20) (recur))))))

(deftest index-reads-match-the-store-scan
  (with-open [node (xtn/start-node)]
    (xt/execute-tx node [(into [:put-docs :evidence] (map fixture-doc (range n-docs)))])
    (text/init! {:path (temp-db)})
    (text/catch-up! node)

    (testing "the index does not serve reads before reconcile!"
      (is (false? (text/complete? 50))))

    ;; In the store, never offered to the index, and below the checkpoint:
    ;; the append that used to be lost for good.
    (let [lost {:xt/id "lost" :evidence/id "lost" :evidence/at (at-hours 3)
                :evidence/type :note :evidence/author "a"
                :evidence/session-id "s1" :evidence/tags [] :evidence/body {}}]
      (xt/execute-tx node [[:put-docs :evidence lost]])
      (let [r (text/reconcile! node)]
        (is (= 1 (:missing r)) "reconcile! finds the unindexed store doc")
        (is (text/complete?))))

    ;; The index says n-0003 is in session s1; the store says s0.
    (jdbc/execute! @text/!ds ["UPDATE ev_attr SET session = 's1' WHERE id = 'n-0003'"])

    (testing "backfill-docs! caches every stored row"
      (is (= (inc n-docs) (:filled (text/backfill-docs! node))))
      (is (zero? (:filled (text/backfill-docs! node))) "a repeat fills nothing"))
    ;; Two cache misses: these must still come from the store.
    (jdbc/execute! @text/!ds ["DELETE FROM ev_doc WHERE id IN ('n-0010', 'n-0301')"])

    (testing "each query returns the store scan's rows, in its order, across pages"
      (doseq [q queries
              limit [1 7]]
        (is (= (:ids (scan-path #(run-query node q limit)))
               (:ids (run-query node q limit)))
            (pr-str q limit)))
      (doseq [q queries]
        (is (= (:count (scan-path #(evidence/count-evidence node q)))
               (:count (evidence/count-evidence node q)))
            (str "count " (pr-str q)))))

    (testing "cached rows are the stored rows"
      (doseq [id ["n-0000" "n-0007" "n-0011" "lost" "n-0010"]]
        (is (= (scan-path #(evidence/fetch-by-id node id))
               (evidence/fetch-by-id node id))
            id))
      (is (= (dissoc (scan-path #(evidence/fetch-by-id node "n-0007")) :evidence/body)
             (dissoc (get (text/cached-docs ["n-0007"]) "n-0007") :evidence/body)))
      (is (not (contains? (text/cached-docs ["n-0010"]) "n-0010")) "the miss is real"))

    (testing "absolute facts, so the two paths cannot agree on a wrong answer"
      (let [ids (set (:ids (run-query node {"session-id" "s1"} 50)))]
        (is (contains? ids "lost") "reconciled doc is served")
        (is (not (contains? ids "n-0003")) "stale index row dropped by re-check")
        (is (= (count ids)
               (inc (count (filter #(= 1 (mod % 3)) (range n-docs))))))))

    (testing "the index path is the one taken"
      ;; include-ephemeral=false makes the scan read a whole page to find one.
      (let [q {"session-id" "s1" "include-ephemeral" "false"}]
        (is (= 1 (:first-scanned (run-query node q 1))))
        (is (< 1 (:first-scanned (scan-path #(run-query node q 1)))))))

    (testing "an acknowledged append is visible to the next read"
      (let [doc {:xt/id "fresh" :evidence/id "fresh" :evidence/at (at-hours 900)
                 :evidence/type :note :evidence/author "a"
                 :evidence/session-id "s1" :evidence/tags [] :evidence/body {}}]
        (xt/execute-tx node [[:put-docs :evidence doc]])
        (text/on-append! doc)
        (is (= ["fresh"] (first-page-ids node {"session-id" "s1"} 1)))
        (is (wait-for 5000 #(contains? (text/cached-docs ["fresh"]) "fresh"))
            "the writer reads the stored row back into the cache")
        (is (= (scan-path #(evidence/fetch-by-id node "fresh"))
               (get (text/cached-docs ["fresh"]) "fresh")))))

    (testing "a failed append stops index reads until it is repaired"
      (let [good @text/!ds
            doc {:xt/id "failed" :evidence/id "failed" :evidence/at (at-hours 901)
                 :evidence/type :note :evidence/author "a"
                 :evidence/session-id "s1" :evidence/tags [] :evidence/body {}}]
        (xt/execute-tx node [[:put-docs :evidence doc]])
        (reset! text/!ds (jdbc/get-datasource
                          {:dbtype "sqlite"
                           :dbname "/nonexistent-futon1b-dir/does-not-exist.db"}))
        (try
          (text/on-append! doc)
          (is (wait-for 5000 #(not (text/complete? 0))))
          (finally (reset! text/!ds good)))
        (is (false? (text/complete? 50)) "still held after the datasource returns")
        (is (= ["failed"] (first-page-ids node {"session-id" "s1"} 1))
            "the scan path still answers")
        (text/retry-failed! node)
        (is (text/complete?))
        (is (= 1 (:first-scanned (run-query node {"session-id" "s1"} 1))))
        (is (= ["failed"] (first-page-ids node {"session-id" "s1"} 1)))))))

(defn -main [& _]
  (let [result (run-tests 'test-evidence-index-window)]
    (shutdown-agents)
    (System/exit (if (zero? (+ (:fail result) (:error result))) 0 1))))
