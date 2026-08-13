;; futon1b-xt — shared XTDB 2 query helper.
;;
;; XTDB 2.1.0 errors with "Not all variables in expression are in scope"
;; when a query references a column on a table NO doc has ever been written
;; to (the table doesn't exist yet, so the column can't bind). On a fresh
;; operational-first store every first-touch read hits this — including the
;; hyperedge no-op guard's read-before-first-ever-write and /health. Treat
;; exactly that error as an empty result; everything else propagates.
(ns futon1b-xt
  (:require [xtdb.api :as xt])
  (:import [java.util.concurrent Semaphore]))

(def ^:private query-width 4)

(defonce ^:private query-permits
  ;; One process-wide budget for pgwire queries. Per-request futures otherwise
  ;; multiply the HTTP worker count (four requests x four hydrations = sixteen
  ;; concurrent XTDB queries), precisely the convoy that caused the 2026-07-22
  ;; brown-out. Fair acquisition keeps point reads and writes from starving.
  (Semaphore. query-width true))

(def ^:private cached-plan-retries
  "A pgwire cached plan is invalidated when the result type of its table
  changes underneath it. XTDB materializes hyperedge props as `prop$*`
  COLUMNS, so ingesting documents with previously unseen prop keys widens the
  table and invalidates plans — continuously, while a backlog is indexing.
  Re-executing re-prepares, so this is retryable rather than fatal; it was
  fatal here only because we rethrew it. Found 2026-08-13, when it blocked
  every boot after ~9.5k new pattern/clause records were written."
  5)

(defn safe-q
  [node form]
  (.acquire query-permits)
  (try
    (loop [attempt 1]
      (let [r (try
                (xt/q node form)
                (catch Exception e
                  (let [msg (str (.getMessage e))]
                    (cond
                      (re-find #"(?i)not all variables in expression are in scope|table not found"
                               msg)
                      []

                      (and (re-find #"(?i)cached plan must not change result type" msg)
                           (< attempt cached-plan-retries))
                      ::retry-cached-plan

                      :else (throw e)))))]
        (if (identical? r ::retry-cached-plan)
          (do (Thread/sleep (* 200 attempt))
              (recur (inc attempt)))
          r)))
    (finally
      (.release query-permits))))

(defn q1 [node form]
  (first (safe-q node form)))

(defn present? [node table id]
  (seq (safe-q node (list '-> (list 'from table '[xt/id])
                          (list 'where (list '= 'xt/id id))))))
