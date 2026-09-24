;; The write log: a rescued or failed put leaves a record that can be read
;; from outside the JVM after the fact.
;;
;; The rescue ladder (migration.ingest/put-doc-with-rescue!, README "F4")
;; already records every stringification and, since 2026-09-24, every failed
;; put attempt with the store's own message -- but only into in-memory shape
;; logs (defonce atoms in futon1b-graph and futon1b-evidence) that no route
;; exposed and no file received. On 2026-09-23 `unknown object type: class
;; clojure.lang.Ratio` fired six times over ten hours and nothing outside the
;; process named it.
;;
;; This namespace does two things and changes no write contract:
;;   1. attach-file!  -- watch the shape logs and append each new entry as one
;;      EDN map per line to a file beside the store (write-log.edn). The file
;;      survives restarts and is greppable without touching the JVM.
;;   2. entries       -- the merged, time-ordered tail of the in-memory logs,
;;      served read-only by GET /api/alpha/write-log.
;; A clean put records nothing, so an idle store leaves the file untouched.
(ns futon1b-write-log
  (:require [clojure.java.io :as io]))

(defonce ^:private !file (atom nil))
(def ^:private file-lock (Object.))

(defn file-path
  "Where the durable record lives for a store directory."
  [store-dir]
  (str store-dir "/write-log.edn"))

(defn- append-lines! [path entries]
  (when (seq entries)
    (locking file-lock
      (io/make-parents path)
      (with-open [w (io/writer path :append true)]
        (doseq [e entries]
          (.write w (pr-str e))
          (.write w "\n"))))))

(defn- new-entries
  "Entries present in NEW that were not in OLD. The logs are append-only
  vectors capped by dropping the oldest, so growth is the common case and an
  equal count with a different last element is the capped case."
  [old new]
  (cond
    (> (count new) (count old)) (subvec new (count old))
    (and (seq new) (not= (peek new) (peek old))) [(peek new)]
    :else []))

(defn attach-file!
  "Append every new entry of each LOG atom to PATH. Idempotent per log
  (re-attaching replaces the watch). Returns the path."
  [path logs]
  (reset! !file path)
  (doseq [log logs]
    (add-watch log ::file
               (fn [_ _ old new]
                 (try (append-lines! path (new-entries old new))
                      (catch Throwable t
                        (println "[write-log] append failed:" (.getMessage t)))))))
  path)

(defn detach-file! [logs]
  (doseq [log logs] (remove-watch log ::file))
  (reset! !file nil))

(defn entries
  "Merged tail of the in-memory logs, oldest first, at most LIMIT entries.
  Optional KIND filters on :kind (:shape or :put-failed)."
  [logs {:keys [limit kind]}]
  (let [all (cond->> (sort-by :at (mapcat deref logs))
              kind (filter #(= kind (:kind %))))
        n (count all)
        limit (or limit 50)]
    {:count n
     :file @!file
     :entries (vec (drop (max 0 (- n limit)) all))}))
