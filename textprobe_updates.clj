;; textprobe_updates.clj — M-text-sidecar probe P1a (update-frequency profile).
;;
;; The ever-held vs current token divergence (#5637's stress metric) only
;; diverges for docs that get UPDATED. Graph docs carry :entity/seen-count and
;; :entity/first-seen / :entity/updated-at, so the update-prone population can
;; be profiled from the export snapshot offline — and the quiet-window history
;; sample drawn from it deterministically. See futon2/holes/M-text-sidecar.md.
;;
;; Usage: bb textprobe_updates.clj migration-export/graph-snapshot.edn textprobe/updates.edn

(require '[clojure.pprint :as pp])
(load-file (str (.getParent (java.io.File. *file*)) "/textprobe_stream.clj"))

(defn -main [in out]
  (let [state (volatile! {:seen-counts {}   ; seen-count -> n docs
                          :with-source {}   ; same, restricted to docs with :entity/source
                          :top []})         ; running top-N by seen-count (with source)
        top-n 40
        _ (stream-docs
           in
           (fn [doc]
             (when (:entity/id doc)
               (let [sc  (or (:entity/seen-count doc) 1)
                     src (:entity/source doc)]
                 (vswap! state
                         (fn [{:keys [seen-counts with-source top]}]
                           {:seen-counts (update seen-counts sc (fnil inc 0))
                            :with-source (if src (update with-source sc (fnil inc 0)) with-source)
                            :top (if src
                                   (->> (conj top {:id (:entity/external-id doc)
                                                   :name (:entity/name doc)
                                                   :type (:entity/type doc)
                                                   :seen-count sc
                                                   :source-chars (count src)})
                                        (sort-by :seen-count >)
                                        (take top-n)
                                        vec)
                                   top)}))))))
        {:keys [seen-counts with-source top]} @state
        dist (fn [m]
               (let [total (reduce + (vals m))
                     gt1   (reduce + (map (fn [[k v]] (if (> k 1) v 0)) m))]
                 {:total total
                  :updated (- total (get m 1 0))
                  :seen>1 gt1
                  :max-seen (when (seq m) (apply max (keys m)))
                  :histogram (into (sorted-map) m)}))
        report {:file in
                :all-entities (dist seen-counts)
                :entities-with-source (dist with-source)
                :top-updated-with-source top}]
    (spit out (with-out-str (pp/pprint report)))
    (println "entities:" (get-in report [:all-entities :total])
             "| updated (seen>1):" (get-in report [:all-entities :seen>1]))
    (println "with :entity/source:" (get-in report [:entities-with-source :total])
             "| updated:" (get-in report [:entities-with-source :seen>1]))
    (println "\nseen-count histogram (with source):")
    (doseq [[k v] (take 15 (get-in report [:entities-with-source :histogram]))]
      (println (format "  seen=%-4d n=%d" k v)))
    (println "\ntop updated entities with source:")
    (doseq [{:keys [seen-count source-chars name type]} (take 12 top)]
      (println (format "  seen=%-5d chars=%-6d %-28s %s" seen-count source-chars (str type) name)))))

(apply -main *command-line-args*)
