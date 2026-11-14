(ns test-hx-write-paths
  "Guard: every futon1b source file that writes the `hyperedges` table goes
   through the hyperedge sidecar's hooks.

   Why (2026-11-14, holes/E-xtdb-plan-cache-heap.md): Arxana's commands put
   hyperedge docs straight into XTDB and never reached the sidecar index;
   nothing noticed, because the index was assumed to be repaired by a history
   replay that had in fact stopped completing. Since then a failed hook is
   journaled and repaired by point read (futon1b_hxindex.clj, \"Failure
   journal\"), which only works if every write path calls a hook.

   The check is per file and textual: a file with a hyperedges put/delete/
   erase/patch must mention hx/on-put!, hx/on-put-batch! or hx/on-delete!.
   It does not see writes whose table is a variable (graph/retract-documents!
   uses `[:delete-docs table id]` and calls hx/on-delete! itself); keep those
   few generic paths hooked by hand.

   Run: clojure -M:node -m test-hx-write-paths"
  (:require [clojure.java.io :as io]
            [clojure.string :as str]))

(def ^:private write-re
  #"(?:put-docs|delete-docs|erase-docs|patch-docs)\s*(?:\{[^}]*:(?:into|from)\s+:hyperedges|:hyperedges)")

(def ^:private hook-re #"hx/on-put!|hx/on-put-batch!|hx/on-delete!")

(def ^:private exempt
  ;; the index itself
  #{"futon1b_hxindex.clj"})

(defn unhooked-writers
  "Names of FILES (name -> text) that write hyperedges without a hook."
  [files]
  (sort (for [[nm text] files
              :when (and (not (exempt nm))
                         (re-find write-re text)
                         (not (re-find hook-re text)))]
          nm)))

(defn- serving-sources []
  ;; serving code and operator scripts; the root-level probe_*/p2_*/s*_*
  ;; files are throwaway-node experiments, not paths into the live store
  (let [root (io/file ".")]
    (into {}
          (for [f (concat (.listFiles root) (.listFiles (io/file root "scripts")))
                :let [nm (.getName ^java.io.File f)]
                :when (and (.isFile ^java.io.File f)
                           (str/ends-with? nm ".clj")
                           (or (str/starts-with? nm "futon1b_")
                               (= "scripts" (.getName (.getParentFile (.getAbsoluteFile ^java.io.File f))))))]
            [nm (slurp f)]))))

(defn -main [& _]
  (let [planted {"planted.clj" "(xt/execute-tx node [[:put-docs :hyperedges doc]])"
                 "planted_into.clj" "[:put-docs {:into :hyperedges :valid-from t} doc]"
                 "hooked.clj" "[:put-docs :hyperedges d] (hx/on-put! d)"}
        caught (unhooked-writers planted)
        files (serving-sources)
        offenders (unhooked-writers files)
        checked (count (filter #(re-find write-re (val %)) files))]
    (println "  bad case: planted unhooked writers are caught"
             (if (= ["planted.clj" "planted_into.clj"] caught) "PASS" (str "FAIL " (pr-str caught))))
    (println (str "  " checked " file(s) write hyperedges; unhooked: ")
             (if (empty? offenders) "none PASS" (str (pr-str offenders) " FAIL")))
    (shutdown-agents)
    (System/exit (if (and (= ["planted.clj" "planted_into.clj"] caught)
                          (empty? offenders)
                          (pos? checked))
                   0 1))))
