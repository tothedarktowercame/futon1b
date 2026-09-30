(ns test-sidecars
  "The index of sidecars stays complete (futon1b-sidecars).

   S1: every SQLite table a serving namespace creates is declared by exactly
       one registry entry, and every declared table is created somewhere —
       so a new sidecar cannot land without an entry, and a removed one
       cannot leave a stale entry behind.
   S2: every symbol an entry names (ns, hooks, repair fns, gate fn) resolves,
       so a rename cannot leave the registry pointing at nothing.
   S3: health-snapshot answers for every entry on a process with no
       datasource and no node — /health must never throw on a cold server,
       and an entry that is not serving must say so, not vanish.

   Run: clojure -M:node -m test-sidecars
   Bad case: add \"CREATE TABLE IF NOT EXISTS zz_probe (k TEXT)\" to any
   futon1b_*.clj ddl vector; S1 names zz_probe as undeclared."
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [futon1b-sidecars :as sidecars]))

(def ^:private failures (atom 0))

(defn- check! [label ok? & [detail]]
  (println (if ok? "  PASS" "  FAIL") label (if (and (not ok?) detail) (pr-str detail) ""))
  (when-not ok? (swap! failures inc)))

(def ^:private table-re
  #"CREATE (?:VIRTUAL )?TABLE (?:IF NOT EXISTS )?([A-Za-z_][A-Za-z_0-9]*)")

(defn created-tables
  "Table name -> files, over the serving namespaces (futon1b_*.clj). Tests
   and probes create scratch tables of their own and are not sidecars."
  []
  (->> (.listFiles (io/file "."))
       (filter #(re-matches #"futon1b_.*\.clj" (.getName ^java.io.File %)))
       (mapcat (fn [f] (for [[_ t] (re-seq table-re (slurp f))] [t (.getName ^java.io.File f)])))
       (reduce (fn [m [t f]] (update m t (fnil conj #{}) f)) {})))

(defn- named-symbols [entry]
  (->> [(:ns entry)
        (:declares entry)
        (get-in entry [:gate :fn])
        (vals (:maintained-by entry))]
       flatten
       (filter symbol?)))

(defn -main [& _]
  (println "S1: declared tables == created tables")
  (let [created (created-tables)
        declared (->> sidecars/registry
                      (mapcat (fn [e] (for [t (keys (:tables e))] [t (:sidecar/id e)])))
                      (reduce (fn [m [t id]] (update m t (fnil conj []) id)) {}))
        undeclared (set/difference (set (keys created)) (set (keys declared)))
        phantom (set/difference (set (keys declared)) (set (keys created)))
        doubled (into {} (filter #(> (count (val %)) 1)) declared)]
    (check! "the scan finds the known tables (guards a broken regex)"
            (every? (set (keys created)) ["ev_fts" "hx_edge" "scope"])
            (keys created))
    (check! "every created table is declared" (empty? undeclared)
            (select-keys created undeclared))
    (check! "every declared table is created" (empty? phantom) phantom)
    (check! "no table is declared by two entries" (empty? doubled) doubled))

  (println "S2: every named symbol resolves")
  (doseq [e sidecars/registry
          s (named-symbols e)]
    (check! (str (:sidecar/id e) " " s)
            (if (namespace s)
              (some? (try (requiring-resolve s) (catch Throwable _ nil)))
              (some? (find-ns s)))))

  (println "S3: health-snapshot on a cold process")
  (let [snap (try (sidecars/health-snapshot) (catch Throwable t t))]
    (check! "does not throw" (map? snap) snap)
    (when (map? snap)
      (check! "one line per entry"
              (= (set (map :sidecar/id sidecars/registry)) (set (keys snap)))
              (keys snap))
      (check! "every line says whether it serves, and why"
              (every? #(and (contains? % :serving?) (keyword? (:why %))) (vals snap))
              snap)
      (check! "no datasource: the SQLite sidecars report not serving"
              (and (false? (get-in snap [:evidence-text :serving?]))
                   (false? (get-in snap [:hyperedges :serving?]))
                   (false? (get-in snap [:entities :serving?])))
              (select-keys snap [:evidence-text :hyperedges :entities]))))

  (println (if (zero? @failures) "ALL PASS" (str @failures " FAILED")))
  (shutdown-agents)
  (System/exit (if (zero? @failures) 0 1)))
