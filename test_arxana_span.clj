(ns test-arxana-span
  (:require [clojure.test :refer [deftest is run-tests]]
            [futon1b-arxana :as a]
            [xtdb.node :as xtn]))
(def event "2026-08-27T12:00:00.000Z")
(def request {:type-revision [:span 1] :start-field :start :end-field :end
              :start 15 :end 25 :interval :half-open :valid-time "2026-09-01T00:00:00.000Z"})
(defn span [n content]
  {:id (str "arxana:clusion:sha256:span-" n) :type-revision [:span 1]
   :endpoints [] :asserted-content content :event-time event})
(deftest overlap
  (with-open [node (xtn/start-node)]
    (let [rs [(span 0 {:start 0 :end 15}) (span 1 {:start 25 :end 30})
              (span 2 {:start 10}) (span 3 {:start 10 :end 20})
              (span 4 {:start 24 :end 26}) (span 5 {:start 20 :end 20})]]
      (doseq [r rs]
        (a/conditional-command! node {:protocol/version "arxana.v1" :message/id (:id r)
                                      :operation :clusion/assert :body {:record r}}))
      (is (= (subvec rs 3 5) (:records (a/records-by-span-overlap node request))))
      ;; Limit is over the intersection, not the earlier non-overlapping rows.
      (let [r (a/records-by-span-overlap node (assoc request :limit 1))]
        (is (= [(rs 3)] (:records r)))
        (is (:truncated? r)))
      (is (empty? (:records (a/records-by-span-overlap node (assoc request :valid-time "2026-08-01T00:00:00.000Z")))))
      (is (= :half-open (:interval (a/records-by-span-overlap node (dissoc request :interval)))))
      (is (thrown? Exception (a/records-by-span-overlap node (assoc request :interval :closed))))
      (is (thrown? Exception (a/records-by-span-overlap node (assoc request :start 25))))
      (is (thrown? Exception (a/records-by-span-overlap node (assoc request :end-field "query text")))))))
(defn -main [& _]
  (let [r (run-tests 'test-arxana-span)]
    (shutdown-agents)
    (when (pos? (+ (:fail r) (:error r))) (System/exit 1))))
