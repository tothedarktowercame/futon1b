(ns futon1b-request-executor
  "Bounded HTTP worker admission with observable queue delay.
  A task is an HTTP exchange runnable, not a database transaction."
  (:import [java.util.concurrent ArrayBlockingQueue ThreadPoolExecutor
            ThreadPoolExecutor$AbortPolicy TimeUnit RejectedExecutionException]))

(defonce ^:private current-task (ThreadLocal.))
(defonce ^:private sequence-id (atom 0))

(defn task-observation [] (.get current-task))

(defn snapshot [^ThreadPoolExecutor executor]
  {:workers/total (.getMaximumPoolSize executor)
   :workers/active (.getActiveCount executor)
   :requests/queued (.size (.getQueue executor))
   :queue/remaining-capacity (.remainingCapacity (.getQueue executor))
   :requests/completed (.getCompletedTaskCount executor)})

(defn bounded-executor [threads queue-capacity]
  (proxy [ThreadPoolExecutor]
         [threads threads 0 TimeUnit/MILLISECONDS
          (ArrayBlockingQueue. queue-capacity) (ThreadPoolExecutor$AbortPolicy.)]
    (execute [task]
      (let [id (swap! sequence-id inc)
            enqueued (System/nanoTime)
            wrapped (reify Runnable
                      (run [_]
                        (let [observation {:request-task/id id
                                           :queue-wait-ms (quot (- (System/nanoTime) enqueued)
                                                               1000000)}]
                          (.set current-task observation)
                          (try (.run ^Runnable task)
                               (finally (.remove current-task))))))]
        (try (proxy-super execute wrapped)
             (catch RejectedExecutionException error
               (println "[futon1b-request-queue] rejected task-id=" id
                        "capacity-exhausted=true")
               (throw error)))))))
