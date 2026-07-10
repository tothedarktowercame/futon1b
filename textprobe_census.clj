;; textprobe_census.clj — M-text-sidecar probe P1a (field census).
;;
;; Streams the migration-export EDN files doc-by-doc (no full load; the
;; evidence file is 150M) and reports every string-bearing field path with
;; doc counts, string counts, and char volumes — the census that decides
;; which fields are text-bearing for the sidecar index and the #5637
;; evidence packet. See futon2/holes/M-text-sidecar.md (P1a).
;;
;; Usage: bb textprobe_census.clj <export-file.edn> <out.edn>

(require '[clojure.edn :as edn]
         '[clojure.java.io :as io]
         '[clojure.pprint :as pp])

(def readers
  ;; Mirror the export's tagged literals (migration finding F2): keep
  ;; timestamps opaque — the census only cares about strings.
  {'futon1a/instant identity})

(defn- skip-ws
  "Consume EDN whitespace (incl. commas); return the next significant char
  as an int, or -1 at EOF."
  [^java.io.PushbackReader rdr]
  (loop []
    (let [c (.read rdr)]
      (if (and (pos? c)
               (or (Character/isWhitespace (char c)) (= c (int \,))))
        (recur)
        c))))

(defn stream-docs
  "Read a file whose top-level form is a vector of maps, calling (f doc)
  per element without materializing the vector. Returns the doc count."
  [path f]
  (with-open [rdr (java.io.PushbackReader. (io/reader path))]
    (let [c (skip-ws rdr)]
      (when-not (= c (int \[))
        (throw (ex-info "expected top-level [" {:path path :char c}))))
    (loop [n 0]
      (let [c (skip-ws rdr)]
        (if (or (neg? c) (= c (int \])))
          n
          (do (.unread rdr c)
              (f (edn/read {:readers readers :default (fn [_ v] v)} rdr))
              (recur (inc n))))))))

(defn- record-string [acc path ^String s]
  (update acc path
          (fn [{:keys [strings chars max-chars] :or {strings 0 chars 0 max-chars 0}}]
            {:strings   (inc strings)
             :chars     (+ chars (.length s))
             :max-chars (max max-chars (.length s))})))

(defn- walk-doc
  "Fold every string value in doc into acc, keyed by its key path.
  Vector elements share their parent path (tag-like fields)."
  [acc path v]
  (cond
    (string? v)     (record-string acc path v)
    (map? v)        (reduce-kv (fn [a k v'] (walk-doc a (conj path k) v')) acc v)
    (sequential? v) (reduce (fn [a v'] (walk-doc a (conj path '*) v')) acc v)
    :else           acc))

(defn -main [in out]
  (let [state (volatile! {:census {} :doc-fields {} :types {}})
        n     (stream-docs
               in
               (fn [doc]
                 (let [dtype (or (:evidence/type doc) (:entity/type doc)
                                 (:relation/type doc) :untyped)]
                   (vswap! state
                           (fn [{:keys [census doc-fields types]}]
                             (let [c' (walk-doc census [] doc)]
                               {:census c'
                                ;; count docs (not strings) per path: a path
                                ;; touched by this doc = grew its :strings
                                :doc-fields (reduce (fn [df p]
                                                      (if (= (get-in census [p :strings] 0)
                                                             (get-in c' [p :strings] 0))
                                                        df
                                                        (update df p (fnil inc 0))))
                                                    doc-fields (keys c'))
                                :types (update types dtype (fnil inc 0))}))))))
        {:keys [census doc-fields types]} @state
        report {:file in
                :docs n
                :doc-types (into (sorted-map-by (fn [a b] (compare [(get types b) a] [(get types a) b]))) types)
                :fields (->> census
                             (map (fn [[path m]]
                                    (assoc m :path path :docs (get doc-fields path 0))))
                             (sort-by :chars >)
                             vec)}]
    (io/make-parents out)
    (spit out (with-out-str (pp/pprint report)))
    (println "docs:" n)
    (println "doc types:" (count types) "| string paths:" (count (:fields report)))
    (println "\ntop paths by char volume:")
    (doseq [{:keys [path docs strings chars max-chars]} (take 25 (:fields report))]
      (println (format "  %-55s docs=%-7d strs=%-8d chars=%-11d max=%d"
                       (pr-str path) docs strings chars max-chars)))))

(apply -main *command-line-args*)
