;; E-futon1b-foothold S4 — the D-11.i seam swap.
;; Re-implement memory-search's read fn against XTDB 2, returning the exact
;; M-custom-harness §12.3 envelope. The evidence seed (identical to s2_s3.clj)
;; is the oracle — expected counts are computed in plain Clojure from the same
;; structures, asserted against the seam-swap output.
;;
;; Run: cd /home/joe/code/futon1b && timeout 240 clojure -M:node -e '(load-file "seam_swap.clj")'
(require '[xtdb.node :as xtn] '[xtdb.api :as xt])

;; ---------------------------------------------------------------------------
;; Seed (reused verbatim from s2_s3.clj so the oracle counts are comparable).
;; ---------------------------------------------------------------------------
(def evidence
  (vec (for [i (range 30)]
         {:xt/id           (+ 1000 i)
          :evidence/type   :observation
          :evidence/author (str "agent-" (mod i 3))
          :evidence/at     (format "2026-07-04T%02d:30:00Z" (mod i 24))
          :evidence/tags   (nth [[:psr] [:pur] [:par] [:proof-path] [:psr :proof-path]]
                                (mod i 5))})))

(def expected-proof-path-ids
  (set (map :xt/id (filter #(some #{:proof-path} (:evidence/tags %)) evidence))))

;; ---------------------------------------------------------------------------
;; The seam-swap fn. Mirrors futon3c memory-search's contract.
;; The XTQL pipeline is built as data (a -> thread of clause forms) and handed
;; to xt/q, which evaluates it. Clauses are appended conditionally so that
;; only the filters actually supplied appear in :query and in the plan.
;; ---------------------------------------------------------------------------
(def ^:private frame-string "recorded, not necessarily current")

(defn- clamp-limit [limit]
  (let [n (if (or (nil? limit) (not (pos? limit))) 20 (int limit))]
    (max 1 (min 100 n))))

(defn- build-query-form
  "Construct the XTQL pipeline form (data) for the given filters, splicing the
  runtime filter values directly into the form (xt/q takes the form as data and
  does no symbol substitution). Tag filtering uses the unnest pipeline idiom
  proven in S3(c) with ANY-of as an `or` of equalities — plain XTQL only:
  Clojure's set-as-predicate/`some` are NOT XTQL expressions (review fix,
  claude-16: the original (where (some (set tag-set) tag)) threw
  \"set not applicable to types set\" at runtime).
  NB: when :tags has >1 element a doc matching several tags yields duplicate
  rows after unnest; memory-search-1b dedups by :id and applies the limit in
  Clojure so the limit counts unique documents (matching futon3c semantics)."
  [{:keys [type author tags]}]
  (let [clauses (reduce conj
                        ['(from :evidence [{:xt/id id} evidence/type
                                           evidence/author evidence/at evidence/tags])]
                        (concat
                         (when type
                           [(list 'where (list '= 'evidence/type type))])
                         (when author
                           [(list 'where (list '= 'evidence/author author))])
                         (when (seq tags)
                           [(list 'unnest '{:tag evidence/tags})
                            (list 'where (cons 'or (map #(list '= 'tag %) tags)))])
                         ['(return id evidence/type evidence/author evidence/at)]))]
    (cons '-> clauses)))

(defn memory-search-1b
  "XTDB 2 re-implementation of futon3c memory-search's read fn.
  `node` is an open xtdb.node. `opts`: {:type <kw> :author <str>
  :tags [<kw>...] :limit <int>} — all optional. Returns the §12.3 envelope."
  [node {:keys [type author tags limit]}]
  (let [lim    (clamp-limit limit)
        form   (build-query-form {:type type :author author :tags tags})
        rows   (xt/q node form)
        ;; Dedup by :id (unnest over multi-tag matches yields dup rows), then
        ;; apply the limit over unique documents.
        items  (->> (vals (reduce (fn [acc r]
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
                       (seq tags) (assoc :tags tags)
                       :always    (assoc :limit lim))
              :items items}}))

;; ---------------------------------------------------------------------------
;; Driver: seed the throwaway node, run the seam-swap, assert the contract.
;; ---------------------------------------------------------------------------
(with-open [node (xtn/start-node)]
  (xt/execute-tx node (vec (for [e evidence] [:put-docs :evidence e])))

  (let [a1       (memory-search-1b node {:tags [:proof-path] :limit 50})
        a1-res   (:result a1)
        a1-items (:items a1-res)
        a1-ids   (set (map :id a1-items))
        a1-exp   expected-proof-path-ids
        a2       (memory-search-1b node {:type :observation :author "agent-1" :limit 5})
        a2-res   (:result a2)
        a2-items (:items a2-res)
        a2-keys  (set (keys a2-res))
        a2-item-ok (every? #(and (contains? % :id) (contains? % :at)
                                 (contains? % :author) (contains? % :type))
                           a2-items)
        a3-hi-res   (:result (memory-search-1b node {:limit 500}))
        a3-hi       (count (:items a3-hi-res))
        a3-none-res (:result (memory-search-1b node {}))
        a3-none     (count (:items a3-none-res))]
    (println)
    (println "=== S4 seam-swap assertions ===")
    (println "A1 tags :proof-path  : expected" (count a1-exp)
             "got" (count a1-ids)
             (if (= a1-exp a1-ids) "PASS" "FAIL"))
    (println "A2 envelope shape    : keys" (sort a2-keys)
             "frame-ok" (= frame-string (:frame a2-res))
             "item-fields-ok" a2-item-ok
             (if (and (= #{:frame :query :items} a2-keys)
                      (= frame-string (:frame a2-res))
                      a2-item-ok) "PASS" "FAIL"))
    (println "A3 limit clamp       : limit-500->" a3-hi "(<=100)"
             "no-limit->" a3-none "(<=20)"
             (if (and (<= a3-hi 100) (<= a3-none 20)) "PASS" "FAIL"))
    (println "sample item          :"
             (first (:items (:result a1))))))
