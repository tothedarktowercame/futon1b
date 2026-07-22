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

(defn safe-q
  [node form]
  (.acquire query-permits)
  (try
    (try
      (xt/q node form)
      (catch Exception e
        (if (re-find #"(?i)not all variables in expression are in scope|table not found"
                     (str (.getMessage e)))
          []
          (throw e))))
    (finally
      (.release query-permits))))

(defn q1 [node form]
  (first (safe-q node form)))

(defn present? [node table id]
  (seq (safe-q node (list '-> (list 'from table '[xt/id])
                          (list 'where (list '= 'xt/id id))))))
