;; zai-memory-1b — the XTQL-backed read fn for the Zai memory seam.
;;
;; This is the D-11.i seam fn (proven in seam_swap.clj against the synthetic
;; oracle) promoted to a reusable namespace so example scripts and any future
;; out-of-process bridge can share it. It mirrors the contract of
;; futon3c.peripheral.memory-backend/memory-search and returns the
;; M-custom-harness §12.3 envelope {:frame :query :items}.
;;
;; NB the futon3c serving JVM embeds futon1a (XTDB 1.24.0) under the SAME
;; Maven coordinates as XTDB 2 (com.xtdb/xtdb-core), so this namespace can
;; never be loaded in-process there — the seam must be bridged out-of-process
;; (see the migration excursion doc).
(ns zai-memory-1b
  (:require [clojure.string :as str]
            [xtdb.api :as xt]
            [xtdb.node :as xtn]))

(def frame-string "recorded, not necessarily current")

(defn- clamp-limit [limit]
  (let [n (if (or (nil? limit) (not (pos? limit))) 20 (int limit))]
    (max 1 (min 100 n))))

(defn- build-query-form
  "Construct the XTQL pipeline form (data) for the given filters, splicing the
  runtime filter values directly into the form. Tag filtering uses the unnest
  idiom with ANY-of as an `or` of equalities (plain XTQL only — Clojure
  set-as-predicate is not an XTQL expression). `since` compares :evidence/at
  lexicographically — live evidence timestamps are ISO-8601 UTC strings, so
  string >= is chronological."
  [{:keys [type author tags since]}]
  (let [clauses (reduce conj
                        ['(from :evidence [{:xt/id id} evidence/type
                                           evidence/author evidence/at evidence/tags])]
                        (concat
                         (when type
                           [(list 'where (list '= 'evidence/type type))])
                         (when author
                           [(list 'where (list '= 'evidence/author author))])
                         (when since
                           [(list 'where (list '>= 'evidence/at since))])
                         (when (seq tags)
                           [(list 'unnest '{:tag evidence/tags})
                            (list 'where (cons 'or (map #(list '= 'tag %) tags)))])
                         ['(return id evidence/type evidence/author evidence/at)]))]
    (cons '-> clauses)))

(defn- memory-search-text
  "D1: the optional :text filter routes through the FTS5 sidecar
  (candidate pre-filter + store re-check), then applies the structured
  filters the sidecar does not know (type/tags) over the re-checked
  docs. Envelope unchanged; items gain :score (BM25, lower = better)."
  [node {:keys [type author tags since limit text]}]
  (let [lim (clamp-limit limit)
        sidecar ((requiring-resolve 'futon1b-text/search)
                 node {:q (str text) :author author :since since
                       :limit (max lim 24)})
        items (->> (:results sidecar)
                   (filter (fn [{:keys [entry]}]
                             (and (or (nil? type) (= type (:evidence/type entry)))
                                  (or (empty? tags)
                                      (let [stored (set (map #(keyword (name %))
                                                             (:evidence/tags entry)))]
                                        (every? stored (map #(keyword (name %)) tags)))))))
                   (take lim)
                   (mapv (fn [{:keys [score entry]}]
                           {:id     (:evidence/id entry)
                            :at     (:evidence/at entry)
                            :author (:evidence/author entry)
                            :type   (:evidence/type entry)
                            :score  score})))]
    {:ok true
     :result {:frame frame-string
              :query (cond-> {:text (str text)}
                       type       (assoc :type type)
                       author     (assoc :author author)
                       since      (assoc :since since)
                       (seq tags) (assoc :tags tags)
                       :always    (assoc :limit lim))
              :items items}}))

(defn memory-search
  "XTQL-backed memory-search over an XTDB 2 evidence table.
  `node` is an open xtdb node. `opts`: {:type <kw> :author <str>
  :tags [<kw>...] :since <iso-string> :limit <int> :text <str>} — all
  optional. :text routes through the D1 FTS5 sidecar (see
  memory-search-text); existing structured-only calls are untouched.
  Returns the §12.3 envelope. Dedups by :id (unnest over multi-tag matches
  yields duplicate rows) so the limit counts unique documents."
  [node {:keys [type author tags since limit text] :as opts}]
  (if-not (str/blank? (str (or text "")))
    (memory-search-text node opts)
    (let [lim   (clamp-limit limit)
        form  (build-query-form opts)
        rows  (xt/q node form)
        items (->> (vals (reduce (fn [acc r]
                                   (if (contains? acc (:id r))
                                     acc
                                     (assoc acc (:id r)
                                            {:id     (:id r)
                                             :at     (:evidence/at r)
                                             :author (:evidence/author r)
                                             :type   (:evidence/type r)})))
                                 (array-map)
                                 rows))
                   vec
                   (take lim)
                   vec)]
    {:ok true
     :result {:frame frame-string
              :query (cond-> {}
                       type       (assoc :type type)
                       author     (assoc :author author)
                       since      (assoc :since since)
                       (seq tags) (assoc :tags tags)
                       :always    (assoc :limit lim))
              :items items}})))

(defn open-store
  "Open the migrated persistent futon1b store read/write (single process at a
  time — do not open while another JVM holds it)."
  [store-dir]
  (xtn/start-node {:log     [:local {:path (str store-dir "/log")}]
                   :storage [:local {:path (str store-dir "/storage")}]}))
