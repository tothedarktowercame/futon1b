;; Ingest mark7z IATC argument graphs into an XTDB 2.2.0-rc0 store
;; (M-xtdb-22x-benchmarking, D1 seed). Idempotent: deterministic :xt/id per
;; doc, so re-running over a growing graphs dir supersedes (bitemporal
;; versioning) rather than duplicating.
;;
;;   clojure -M:node -m ingest-graphs --graphs <dir> --store <dir>
;;
;; Tables: iatc_graphs (one row per proof graph), iatc_nodes (typed, with
;; source-anchored text — the future text-index workload), iatc_edges
;; (premise/conclusion refs — the graph-traversal workload).
(ns ingest-graphs
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [xtdb.node :as xtn]
            [xtdb.api :as xt]))

(defn open-store [dir]
  (xtn/start-node {:log     [:local {:path (str dir "/log")}]
                   :storage [:local {:path (str dir "/storage")}]}))

(defn kw->s [k] (cond (nil? k) nil (keyword? k) (name k) :else (str k)))

(defn- refs [gid v]
  (cond (nil? v) nil
        (sequential? v) (mapv #(str gid "#" (kw->s %)) v)
        :else [(str gid "#" (kw->s v))]))

(defn docs-for [g]
  (let [paper   (:paper/id g)
        gid     (:passage/id g)
        src     (:source g)
        nodes   (:nodes g)
        edges   (:edges g)]
    (concat
     [[:put-docs :iatc_graphs
       {:xt/id gid :paper paper
        :src-kind (kw->s (:kind src))
        :line-lo (first (:lines src)) :line-hi (second (:lines src))
        :n-nodes (count nodes) :n-edges (count edges)
        :n-holes (count (:holes g))}]]
     (for [n nodes]
       [:put-docs :iatc_nodes
        {:xt/id (str gid "#" (kw->s (:id n)))
         :graph gid :paper paper
         :node-id (kw->s (:id n))
         :kind (kw->s (:kind n))
         :text (:text n)
         :line-lo (some-> n :source :lines first)
         :line-hi (some-> n :source :lines second)}])
     (for [e edges]
       [:put-docs :iatc_edges
        {:xt/id (str gid "#" (kw->s (:id e)))
         :graph gid :paper paper
         :edge-id (kw->s (:id e))
         :kind (kw->s (:kind e))
         :relation (kw->s (:relation e))
         :premise-refs (refs gid (or (:premise e) (:premises e)))
         :conclusion-refs (refs gid (or (:conclusion e) (:conclusions e)))
         :warrant-kind (kw->s (some-> e :warrant :kind))}]))))

(defn graph-files [dir]
  (->> (file-seq (io/file dir))
       (filter #(.isFile ^java.io.File %))
       (filter #(re-matches #".*__p\d+\.edn" (.getName ^java.io.File %)))
       (sort-by #(.getName ^java.io.File %))))

(defn -main [& args]
  (let [opts (apply hash-map args)
        gdir (get opts "--graphs")
        sdir (get opts "--store")]
    (assert (and gdir sdir) "need --graphs <dir> --store <dir>")
    (with-open [node (open-store sdir)]
      (let [fs (graph-files gdir)
            t0 (System/nanoTime)]
        (println (format "ingesting %d graphs from %s" (count fs) gdir))
        (doseq [f fs]
          (let [g (edn/read-string (slurp f))
                ops (vec (docs-for g))]
            (xt/execute-tx node ops)
            (println (format "  %s: %d ops" (.getName ^java.io.File f) (count ops)))))
        (let [ms (/ (- (System/nanoTime) t0) 1e6)]
          (println (format "done: %d graphs in %.0f ms (%.1f ms/graph incl. tx)"
                           (count fs) ms (if (pos? (count fs)) (/ ms (count fs)) 0.0))))
        (doseq [t [:iatc_graphs :iatc_nodes :iatc_edges]]
          (println (format "  %-12s %s rows" (name t)
                           (-> (xt/q node (format "SELECT COUNT(*) c FROM %s" (name t)))
                               first :c))))))))
