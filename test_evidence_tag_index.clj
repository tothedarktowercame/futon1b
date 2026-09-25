(ns test-evidence-tag-index
  "Tag reads through the text sidecar's ev_tags index (futon1b-evidence
  tag-window / tag-count) against the store scan they replace.

  Run: clojure -M:node -m test-evidence-tag-index"
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
            "futon1b-tag-index-test-"
            (make-array java.nio.file.attribute.FileAttribute 0))
           .toFile .getAbsolutePath)
       "/fts5-evidence.db"))

(def ^:private base (Instant/parse "2026-09-01T00:00:00Z"))

(defn- at-hours [h] (str (.plusSeconds base (* 3600 h))))

(defn- fixture-doc [i]
  ;; Hourly, so the 15-minute tail below the checkpoint holds one or two rows
  ;; and everything else must come through the candidate path. Tags mix
  ;; keywords and strings: the store keeps both, and the index stores str.
  (cond-> {:xt/id (format "n-%04d" i)
           :evidence/id (format "n-%04d" i)
           :evidence/at (at-hours i)
           :evidence/type (if (even? i) :coordination :note)
           :evidence/claim-type (if (zero? (mod i 4)) :observation :claim)
           :evidence/author (if (zero? (mod i 3)) "b" "a")
           :evidence/session-id (str "s" (mod i 2))
           :evidence/ephemeral? (zero? (mod i 7))
           :evidence/tags (cond-> []
                            (zero? (mod i 3)) (conj (if (odd? i) :alpha "alpha"))
                            (zero? (mod i 5)) (conj :beta))
           :evidence/body {:i i}}
    (zero? (mod i 11)) (assoc :evidence/subject {:ref/type :session :ref/id (str "r" i)})
    (zero? (mod i 13)) (assoc :evidence/fork-of "n-0003")))

(def ^:private n-docs 600)

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
         :scanned (reduce + (map :scanned pages))}))))

(defn- scan-path
  "Run F with the sidecar detached, so evidence reads take the store scan."
  [f]
  (let [ds @text/!ds]
    (reset! text/!ds nil)
    (try (f) (finally (reset! text/!ds ds)))))

(def ^:private queries
  [{"tags" "alpha"}
   {"tags" "alpha" "include-ephemeral" "false"}
   {"tags" "alpha" "type" "coordination"}
   {"tags" "alpha" "claim-type" "observation"}
   {"tags" "alpha,beta"}
   {"tags" "beta" "author" "b"}
   {"tags" "alpha" "session-id" "s1"}
   {"tags" "alpha" "since" (at-hours 100) "before" (at-hours 400)}
   {"tags" "alpha" "subject-type" "session"}
   {"tags" "beta" "fork-of" "n-0003"}
   {"tags" "absent-tag"}])

(deftest tag-reads-match-the-store-scan
  (with-open [node (xtn/start-node)]
    (xt/execute-tx node [(into [:put-docs :evidence] (map fixture-doc (range n-docs)))])
    (text/init! {:path (temp-db)})
    (text/catch-up! node)
    (let [checkpoint (text/checkpoint-at)
          ck (Instant/parse checkpoint)
          ;; Written after the catch-up and never offered to the index: one
          ;; above the checkpoint, one below it but inside the tail margin
          ;; (an on-append! that lost to SQLITE_BUSY after the checkpoint
          ;; moved past its :at).
          late-new {:xt/id "late-new" :evidence/id "late-new"
                    :evidence/at (str (.plusSeconds ck 60)) :evidence/type :note
                    :evidence/author "a" :evidence/tags [:alpha] :evidence/body {}}
          late-margin {:xt/id "late-margin" :evidence/id "late-margin"
                       :evidence/at (str (.minusSeconds ck 300)) :evidence/type :note
                       :evidence/author "a" :evidence/tags ["alpha"] :evidence/body {}}]
      (xt/execute-tx node [[:put-docs :evidence late-new late-margin]])
      ;; The index says n-0001 is tagged alpha; the store says it is not.
      (jdbc/execute! @text/!ds ["INSERT INTO ev_tags(id, tag) VALUES (?,?)" "n-0001" ":alpha"])

      (testing "each query returns the store scan's rows, in its order, across pages"
        (doseq [q queries]
          (let [fast (run-query node q 7)
                scan (scan-path #(run-query node q 7))]
            (is (= (:ids scan) (:ids fast)) (pr-str q))
            (is (= (:count (scan-path #(evidence/count-evidence node q)))
                   (:count (evidence/count-evidence node q)))
                (str "count " (pr-str q))))))

      (testing "absolute facts, so the two paths cannot agree on a wrong answer"
        (let [ids (set (:ids (run-query node {"tags" "alpha"} 50)))
              expected (->> (range n-docs) (filter #(zero? (mod % 3)))
                            (map #(format "n-%04d" %)) set)]
          (is (contains? ids "late-new") "tail above the checkpoint")
          (is (contains? ids "late-margin") "unindexed row inside the margin")
          (is (not (contains? ids "n-0001")) "stale index row dropped by re-check")
          (is (= (into expected ["late-new" "late-margin"]) ids))
          (is (= (+ 2 (count expected))
                 (:count (evidence/count-evidence node {"tags" "alpha"})))))
        (let [ids (set (:ids (run-query node {"tags" "alpha" "include-ephemeral" "false"} 50)))]
          (is (seq ids))
          (is (not-any? #(zero? (mod (parse-long (subs % 2)) 7))
                        (filter #(re-matches #"n-\d+" %) ids))
              "ephemeral rows are excluded through the candidate path")))

      (testing "the candidate path is the one taken"
        (let [q {"tags" "beta"}]
          (is (< (:scanned (run-query node q 10))
                 (:scanned (scan-path #(run-query node q 10))))))))))

(defn -main [& _]
  (let [result (run-tests 'test-evidence-tag-index)]
    (shutdown-agents)
    (System/exit (if (zero? (+ (:fail result) (:error result))) 0 1))))
