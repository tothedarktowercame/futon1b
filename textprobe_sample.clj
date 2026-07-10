;; textprobe_sample.clj — M-text-sidecar P1 (history-sample selection, offline).
;;
;; Builds the deterministic id sample for the quiet-window history extraction
;; (futon1a/scripts/textprobe-history-sample.clj) from the export snapshots —
;; no live-store access. Bands:
;;   :updated         — every entity with :entity/source and seen-count > 1
;;                      (the ever-held stress population; ~6.7k)
;;   :stable-sample   — hash-selected ~1k entities with source, seen-count = 1
;;                      (control: docs that should show no divergence)
;;   :evidence-sample — hash-selected ~500 evidence docs
;;                      (to confirm evidence is effectively append-only)
;; Selection is (mod (hash xt-id) k) — reproducible, no RNG.
;;
;; Usage: bb textprobe_sample.clj migration-export/graph-snapshot.edn \
;;           migration-export/evidence.edn textprobe/history-sample-ids.edn

(require '[clojure.pprint :as pp])
(load-file (str (.getParent (java.io.File. *file*)) "/textprobe_stream.clj"))

(def stable-mod 34)    ; ~34k stable entities / 34 ≈ 1k
(def evidence-mod 180) ; ~90k evidence docs / 180 ≈ 500

(defn -main [graph-file evidence-file out]
  (let [state (volatile! {:updated [] :stable-sample [] :evidence-sample []})]
    (stream-docs
     graph-file
     (fn [doc]
       (when (and (:entity/id doc) (:entity/source doc) (:xt/id doc))
         (let [sc (or (:entity/seen-count doc) 1)]
           (cond
             (> sc 1)
             (vswap! state update :updated conj (:xt/id doc))

             (zero? (mod (Math/abs (hash (:xt/id doc))) stable-mod))
             (vswap! state update :stable-sample conj (:xt/id doc)))))))
    (stream-docs
     evidence-file
     (fn [doc]
       (when (and (:evidence/id doc) (:xt/id doc)
                  (zero? (mod (Math/abs (hash (:xt/id doc))) evidence-mod)))
         (vswap! state update :evidence-sample conj (:xt/id doc)))))
    (let [{:keys [updated stable-sample evidence-sample]} @state
          ;; :xt/id values mix UUIDs and strings — order by printed form
          ordered #(vec (sort-by pr-str %))]
      (spit out (with-out-str
                  (pp/pprint {:bands {:updated (ordered updated)
                                      :stable-sample (ordered stable-sample)
                                      :evidence-sample (ordered evidence-sample)}
                              :selection {:stable-mod stable-mod
                                          :evidence-mod evidence-mod}
                              :sources {:graph graph-file :evidence evidence-file}})))
      (println "updated:" (count updated)
               "| stable-sample:" (count stable-sample)
               "| evidence-sample:" (count evidence-sample)
               "| total:" (+ (count updated) (count stable-sample) (count evidence-sample))))))

(apply -main *command-line-args*)
