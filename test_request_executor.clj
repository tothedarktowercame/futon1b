(ns test-request-executor
  (:require [clojure.test :refer [deftest is run-tests]]
            [futon1b-request-executor :as sut])
  (:import [java.util.concurrent CountDownLatch TimeUnit RejectedExecutionException]))

(deftest queue-and-worker-are-distinct-observations
  (let [executor (sut/bounded-executor 1 1)
        entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        observed (promise)]
    (try
      (.execute executor ^Runnable #(do (.countDown entered)
                                       (.await release 2 TimeUnit/SECONDS)))
      (is (.await entered 1 TimeUnit/SECONDS))
      (.execute executor ^Runnable #(deliver observed (sut/task-observation)))
      (is (= 1 (:workers/active (sut/snapshot executor))))
      (is (= 1 (:requests/queued (sut/snapshot executor))))
      (is (thrown? RejectedExecutionException (.execute executor ^Runnable (fn []))))
      (Thread/sleep 20)
      (.countDown release)
      (let [r (deref observed 1000 nil)]
        (is (integer? (:request-task/id r)))
        (is (>= (:queue-wait-ms r) 10)))
      (is (nil? (sut/task-observation)))
      (finally (.countDown release) (.shutdownNow executor)))))

(defn -main [& _]
  (let [r (run-tests 'test-request-executor)]
    (when (pos? (+ (:fail r) (:error r))) (System/exit 1))))
