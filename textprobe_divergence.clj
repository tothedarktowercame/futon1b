;; textprobe_divergence.clj — M-text-sidecar P1 (the headline measurement).
;;
;; Consumes history-versions.edn (from textprobe-history-standalone.clj or
;; the live-JVM side-file) and computes, per band, the ever-held vs current
;; token-set divergence that XTDB #5637's chalk notes estimate at ~1.5-2x
;; for typical edit patterns. Also reports how many re-seen docs actually
;; CHANGED their text (seen-count counts observations, not edits — this
;; closes that caveat) and the aggregate posting-inflation the "ever held"
;; index shape would pay on this corpus.
;;
;; Tokenizer: lowercase, split on non-alphanumeric, tokens >= 2 chars.
;; Deliberately the simplest baseline — analyzer choice is probe P2; the
;; divergence RATIO is far less analyzer-sensitive than the token sets.
;;
;; Usage: bb textprobe_divergence.clj history-versions.edn textprobe/divergence.edn

(require '[clojure.pprint :as pp]
         '[clojure.set :as set]
         '[clojure.string :as str])
(load-file (str (.getParent (java.io.File. *file*)) "/textprobe_stream.clj"))

(defn tokens [s]
  (into #{}
        (comp (remove str/blank?) (filter #(>= (count %) 2)))
        (str/split (str/lower-case s) #"[^\p{IsAlphabetic}\p{IsDigit}]+")))

(defn doc-tokens [fields]
  (transduce (map tokens) set/union #{} (vals fields)))

(defn percentile [sorted-v p]
  (when (seq sorted-v)
    (nth sorted-v (min (dec (count sorted-v))
                       (int (Math/floor (* p (count sorted-v))))))))

(defn -main [in out]
  (let [state (volatile! {})]
    (stream-docs
     in
     (fn [{:keys [band versions]}]
       (let [live (remove :deleted? versions)
             current (if-let [v (last live)] (doc-tokens (:fields v)) #{})
             ever (transduce (map #(doc-tokens (:fields %))) set/union #{} live)
             texts (map :fields live)
             changed? (> (count (distinct texts)) 1)
             ratio (when (pos? (count current))
                     (/ (count ever) (double (count current))))]
         (vswap! state update band
                 (fnil (fn [b]
                         (-> b
                             (update :docs inc)
                             (update :multi-version (if (> (count versions) 1) inc identity))
                             (update :text-changed (if changed? inc identity))
                             (update :sum-ever + (count ever))
                             (update :sum-current + (count current))
                             (update :ratios (if ratio #(conj % ratio) identity))
                             (update :empty-current (if (and (zero? (count current))
                                                             (pos? (count ever)))
                                                      inc identity))))
                       {:docs 0 :multi-version 0 :text-changed 0
                        :sum-ever 0 :sum-current 0 :ratios [] :empty-current 0})))))
    (let [report
          (into (sorted-map)
                (map (fn [[band {:keys [docs multi-version text-changed sum-ever
                                        sum-current ratios empty-current]}]]
                       (let [sr (vec (sort ratios))]
                         [band {:docs docs
                                :multi-version multi-version
                                :text-changed text-changed
                                :posting-inflation (when (pos? sum-current)
                                                     (/ sum-ever (double sum-current)))
                                :ever>current (count (filter #(> % 1.0) sr))
                                :ratio {:p50 (percentile sr 0.50)
                                        :p90 (percentile sr 0.90)
                                        :p99 (percentile sr 0.99)
                                        :max (last sr)}
                                :empty-current empty-current}])))
                @state)]
      (spit out (with-out-str (pp/pprint {:file in :bands report})))
      (doseq [[band r] report]
        (println (format "%-16s docs=%-6d multi-version=%-5d text-changed=%-5d inflation=%.3f ratio p50=%.2f p90=%.2f p99=%.2f max=%.2f (ever>current: %d, empty-current: %d)"
                         (str band) (:docs r) (:multi-version r) (:text-changed r)
                         (or (:posting-inflation r) 0.0)
                         (or (get-in r [:ratio :p50]) 0.0)
                         (or (get-in r [:ratio :p90]) 0.0)
                         (or (get-in r [:ratio :p99]) 0.0)
                         (or (get-in r [:ratio :max]) 0.0)
                         (:ever>current r) (:empty-current r)))))))

(apply -main *command-line-args*)
