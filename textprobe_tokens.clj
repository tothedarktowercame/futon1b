;; textprobe_tokens.clj — M-text-sidecar probe P1b (posting-list economics).
;;
;; Tokenizes the text-bearing fields (per the P1a census) of an export
;; snapshot and reports the numbers #5637's chalk notes reason about:
;; vocabulary size, posting-list (docs-per-token) skew, hot terms, and the
;; effect of English stop-word filtering. Tracks the candidate index fields
;; and the HUD screen-capture fields SEPARATELY, to give the include/exclude
;; decision a measured basis. Same baseline tokenizer as
;; textprobe_divergence.clj (lowercase, non-alphanumeric split, len >= 2).
;;
;; Usage: bb textprobe_tokens.clj <export.edn> <out.edn>

(require '[clojure.pprint :as pp]
         '[clojure.set :as set]
         '[clojure.string :as str])
(load-file (str (.getParent (java.io.File. *file*)) "/textprobe_stream.clj"))

(def index-paths
  [[:entity/source]
   [:entity/name]
   [:entity/props :anchor/passage]
   [:entity/props :anchor/heading]
   [:evidence/body :text]])

;; the two heaviest census paths — HUD screen captures, candidate EXCLUDE
(defn- hud-strings [doc]
  (for [arg (get-in doc [:evidence/body :args])
        :when (map? arg)
        s [(get-in arg [:buffer :visible :text])
           (get-in arg [:minibuffer :buffer :visible :text])]
        :when (string? s)]
    s))

(def stop-words
  ;; Lucene EnglishAnalyzer's default stop set (org.apache.lucene.analysis.en)
  #{"a" "an" "and" "are" "as" "at" "be" "but" "by" "for" "if" "in" "into"
    "is" "it" "no" "not" "of" "on" "or" "such" "that" "the" "their" "then"
    "there" "these" "they" "this" "to" "was" "will" "with"})

(defn tokens [s]
  (into #{}
        (comp (remove str/blank?) (filter #(>= (count %) 2)))
        (str/split (str/lower-case s) #"[^\p{IsAlphabetic}\p{IsDigit}]+")))

(defn- profile [postings]
  (let [counts (vec (sort (vals postings)))
        n (count counts)
        total (reduce + counts)
        pct (fn [p] (when (pos? n) (nth counts (min (dec n) (int (Math/floor (* p n)))))))
        stopped (filter (fn [[t _]] (stop-words t)) postings)]
    {:vocab n
     :postings total
     :docs-per-token {:p50 (pct 0.50) :p90 (pct 0.90) :p99 (pct 0.99)
                      :max (last counts)}
     :stop-word-share (when (pos? total)
                        (/ (reduce + (map second stopped)) (double total)))
     :hot-terms (->> postings (sort-by val >) (take 40)
                     (mapv (fn [[t c]] [t c])))}))

(defn -main [in out]
  (let [state (volatile! {:index {} :hud {} :index-docs 0 :hud-docs 0})]
    (stream-docs
     in
     (fn [doc]
       (let [index-toks (transduce (comp (keep #(get-in doc %))
                                         (filter string?)
                                         (map tokens))
                                   set/union #{} index-paths)
             hud-toks (transduce (map tokens) set/union #{} (hud-strings doc))]
         (when (seq index-toks)
           (vswap! state (fn [s] (-> s (update :index-docs inc)
                                     (update :index #(reduce (fn [m t] (update m t (fnil inc 0))) % index-toks))))))
         (when (seq hud-toks)
           (vswap! state (fn [s] (-> s (update :hud-docs inc)
                                     (update :hud #(reduce (fn [m t] (update m t (fnil inc 0))) % hud-toks)))))))))
    (let [{:keys [index hud index-docs hud-docs]} @state
          report {:file in
                  :index-fields (assoc (profile index) :docs index-docs)
                  :hud-fields (assoc (profile hud) :docs hud-docs)}]
      (spit out (with-out-str (pp/pprint report)))
      (doseq [[k r] (dissoc report :file)]
        (println (format "%-13s docs=%-6d vocab=%-7d postings=%-9d docs/token p50=%s p90=%s p99=%s max=%s stop-share=%.3f"
                         (str k) (:docs r) (:vocab r) (:postings r)
                         (str (get-in r [:docs-per-token :p50]))
                         (str (get-in r [:docs-per-token :p90]))
                         (str (get-in r [:docs-per-token :p99]))
                         (str (get-in r [:docs-per-token :max]))
                         (or (:stop-word-share r) 0.0)))
        (println "  hot:" (str/join " " (map first (take 12 (:hot-terms r)))))))))

(apply -main *command-line-args*)
