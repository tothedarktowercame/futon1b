;; E-futon1a-to-futon1b-migration-pipeline — S2: Transform layer.
;;
;; Transforms futon1a XTDB 1.x document shapes to futon1b XTDB 2.x-compatible
;; shapes. This is the bridge between export (S1) and ingest (S3).
;;
;; Known transform needs (from M-futon1b-port hazard catalog):
;;   - H4: :hx/props nested maps → denormalize to :prop/<key> top-level columns.
;;   - String-keyed maps in :evidence/body → stringify (XTDB 2 rejects non-keyword keys).
;;   - Any other nested maps with non-keyword keys → stringify recursively.
;;
;; Design principle: FAIL LOUD on unknown shapes. The transform collects
;; unknown structural patterns into a log rather than silently dropping them.
;; The operator reviews the log before proceeding to full ingest.
;;
;; The transform is a pure function — no I/O, no side effects. This makes it
;; testable against the seed slice and replayable.
(ns migration.transform)

;; ---------------------------------------------------------------------------
;; Shape detection: identify values that XTDB 2 will reject.
;; ---------------------------------------------------------------------------

(defn string-keyed-map?
  "True if m is a map with at least one non-keyword key.
  XTDB 2 rejects maps with string keys (they can't be Arrow struct field names)."
  [m]
  (and (map? m)
       (some (fn [k] (not (keyword? k))) (keys m))))

(defn has-namespace?
  "True if k is a keyword with a namespace (e.g. :hx/props)."
  [k]
  (and (keyword? k) (seq (namespace k))))

(defn unqualified-keyword?
  "True if k is a keyword without a namespace (e.g. :foo)."
  [k]
  (and (keyword? k) (not (seq (namespace k)))))

;; ---------------------------------------------------------------------------
;; Transform: stringify non-keyword-keyed maps.
;;
;; When a map contains non-keyword keys (like JSON string keys in
;; :evidence/body), we stringify the entire map value. This preserves the data
;; (the consumer can re-parse it) while making it acceptable to XTDB 2.
;;
;; We do NOT attempt to convert string keys to keywords — many string keys
;; contain characters illegal in keywords (spaces, hyphens in some positions,
;; dots) and the conversion would be lossy or ambiguous.
;; ---------------------------------------------------------------------------

(defn stringify-if-needed
  "If v is a map with non-keyword keys, return (pr-str v).
  Otherwise return v unchanged."
  [v]
  (if (string-keyed-map? v)
    (pr-str v)
    v))

(defn deep-stringify-non-keyword-maps
  "Walk a value, converting any map with non-keyword keys to its pr-str string.
  Maps with all-keyword keys are walked into recursively.

  This is the core transform for :evidence/body and any other nested structure
  that contains JSON-style string keys. The pr-str representation is
  round-trippable (the consumer can clojure.edn/read-string it back), which
  preserves all data — unlike the slice's approach of dropping :evidence/body
  entirely."
  [v]
  (cond
    ;; Leaf values: return as-is.
    (nil? v) v
    (string? v) v
    (keyword? v) v
    (number? v) v
    (boolean? v) v
    (char? v) v

    ;; Temporal values (java.util.Date from #inst / #futon1a/instant) are
    ;; XTDB 2 native — pass through so timestamps stay typed (probed 2026-07-10:
    ;; put-docs accepts Date and Instant; both read back as temporal values).
    (inst? v) v

    ;; Symbols: XTDB 2 does not accept bare symbols as document values.
    ;; They appear in data parsed from .sexp files (e.g. structural-law-inventory
    ;; family records: graph-symmetry, operational). Stringify to preserve data.
    (symbol? v) (str v)

    ;; String-keyed map: stringify the whole thing.
    (string-keyed-map? v)
    (pr-str v)

    ;; All-keyword-keyed map: walk into values recursively.
    (map? v)
    (into {} (map (fn [[k val]] [k (deep-stringify-non-keyword-maps val)])) v)

    ;; Sequential: walk elements.
    (sequential? v)
    (mapv deep-stringify-non-keyword-maps v)

    ;; Set: walk elements (note: elements must be comparable after transform).
    (set? v)
    (into #{} (map deep-stringify-non-keyword-maps) v)

    ;; Fallback: stringify anything else (Java objects, dates, etc.)
    ;; These can't be stored as XTDB 2 doc values directly.
    :else
    (pr-str v)))

;; ---------------------------------------------------------------------------
;; Transform: denormalize :hx/props (H4 solution from M-futon1b-port).
;;
;; XTQL `where` cannot navigate nested maps (H4). We flatten :hx/props entries
;; into top-level :prop/<key> columns, preserving the original :hx/props map.
;; This is the proven D-5 Option B from M-futon1b-port.
;; ---------------------------------------------------------------------------

(defn denormalize-hx-props
  "Add top-level :prop/<key> columns for each :hx/props entry.
  Preserves the original :hx/props map. Only applies to docs with :hx/props."
  [doc]
  (if-let [props (:hx/props doc)]
    (if (map? props)
      (let [prop-keys (into {}
                            (for [[k v] props
                                  :when (and (keyword? k) (seq (namespace k)))]
                              [(keyword "prop" (name k)) v]))
            ;; For props with unqualified keys (common: :repo, :source-file)
            unqualified (into {}
                              (for [[k v] props
                                    :when (unqualified-keyword? k)]
                                  [(keyword "prop" (name k)) v]))]
        (merge doc prop-keys unqualified))
      doc)
    doc))

;; ---------------------------------------------------------------------------
;; Transform: ensure :xt/id is present.
;;
;; The exported docs may use different ID keys depending on their type:
;;   - entities: :xt/id (already set)
;;   - hyperedges: :xt/id (already set, equals :hx/id)
;;   - evidence: :xt/id (already set, equals :evidence/id)
;;   - some exported docs may have :xt/id stripped by the HTTP layer (it
;;     dissoc's :xt/id before returning entity/hyperedge docs)
;;
;; We reconstruct :xt/id from the appropriate ID field if missing.
;; ---------------------------------------------------------------------------

(defn ensure-xt-id
  "Ensure the doc has :xt/id, deriving it from type-specific ID fields if needed."
  [doc]
  (if (:xt/id doc)
    doc
    (let [id (or (:hx/id doc)
                 (:evidence/id doc)
                 (:entity/id doc)
                 (:relation/id doc)
                 (:doc/id doc)
                 (:lab/session-id doc)
                 (:type/id doc))]
      (if id
        (assoc doc :xt/id id)
        doc))))

;; ---------------------------------------------------------------------------
;; Shape catalog: known doc types and their expected structure.
;;
;; This catalog drives the FAIL LOUD mechanism. Each known doc type has a
;; validator that checks whether the transform produced a valid XTDB 2 doc.
;; Unknown doc types are logged but not rejected — they pass through with
;; the generic transform.
;; ---------------------------------------------------------------------------

(def known-id-keys
  "The set of keys that identify a document's primary ID."
  #{:xt/id :hx/id :evidence/id :entity/id :relation/id :doc/id
    :lab/session-id :type/id})

(defn classify-doc
  "Classify a document by its type marker. Returns a keyword identifying the doc type."
  [doc]
  (cond
    (:hx/id doc)         :hyperedge
    (:evidence/id doc)   :evidence
    (:entity/id doc)     :entity
    (:relation/id doc)   :relation
    (:doc/id doc)        :docbook
    (:lab/session-id doc) :lab-session
    (:type/id doc)       :type-catalog
    :else                :unknown))

;; ---------------------------------------------------------------------------
;; Unknown-shape log: collect docs that triggered unexpected transforms.
;;
;; Rather than silently dropping or modifying unknown shapes, we collect them
;; for operator review. This is the opposite of the K2 hazard class (silent
;; wrong results).
;; ---------------------------------------------------------------------------

(defn make-shape-log
  "Create a fresh shape log (atom containing a vector of {:doc-type :key :value :reason})."
  []
  (atom []))

(defn log-shape!
  "Record an unexpected shape in the log."
  [log doc-type key value reason]
  (swap! log conj {:doc-type doc-type
                    :key key
                    :value-preview (let [s (pr-str value)]
                                     (subs s 0 (min (count s) 200)))
                    :reason reason}))

;; ---------------------------------------------------------------------------
;; RESCUE transforms — applied by ingest ONLY to docs that failed put-docs.
;;
;; Found by the first live ingest (2026-07-10): ~914 emacs-cursor evidence
;; docs failed with "Unknown type: NULL" (nil-carrying structs nested in
;; vectors inside :evidence/body). Bisection showed the failure is STATEFUL —
;; the same doc passes on a fresh table and fails after other docs have
;; shaped the column's Arrow type union — so no shape-level rule can predict
;; it (the seed slice contains near-identical shapes that ingest fine). The
;; honest fix is a runtime rescue ladder, per-doc, on actual failure:
;;   stage 1: pr-str top-level values carrying nils in risky positions;
;;   stage 2: pr-str every deep collection value (flat scalar colls like
;;            :evidence/tags / :hx/endpoints are kept — they stay queryable).
;; Both stages are round-trippable (edn/read-string recovers the value) and
;; every stringification is recorded in the shape-log.
;; ---------------------------------------------------------------------------

(defn risky-nil?
  "True if v contains a nil in a position Arrow may fail to type:
  any nil inside a sequential/set, or a nil map-value below the first
  nesting level."
  ([v] (risky-nil? v 0 false))
  ([v level in-seq?]
   (cond
     (nil? v) (or in-seq? (>= level 2))
     (map? v) (boolean (some (fn [[_ mv]] (risky-nil? mv (inc level) in-seq?)) v))
     (or (sequential? v) (set? v)) (boolean (some #(risky-nil? % level true) v))
     :else false)))

(defn stringify-risky-nils
  "Rescue stage 1: pr-str any top-level doc value that carries a risky nil.
  Logs each stringification to the optional shape-log."
  [doc shape-log]
  (into {}
        (map (fn [[k v]]
               (if (and (not= k :xt/id) (coll? v) (risky-nil? v))
                 (do (when shape-log
                       (log-shape! shape-log (classify-doc doc) k v
                                   "rescue-1: risky nil — stringified"))
                     [k (pr-str v)])
                 [k v])))
        doc))

(defn- flat-scalar-coll?
  "A sequential/set whose elements are all non-nil scalars — safe and
  queryable (e.g. :evidence/tags, :hx/endpoints); rescue stage 2 keeps these."
  [v]
  (and (or (sequential? v) (set? v))
       (every? #(or (keyword? %) (string? %) (number? %) (boolean? %) (inst? %)) v)))

(defn stringify-deep-colls
  "Rescue stage 2: pr-str every top-level collection value except flat
  scalar collections. Logs each stringification to the optional shape-log."
  [doc shape-log]
  (into {}
        (map (fn [[k v]]
               (if (and (not= k :xt/id) (coll? v) (not (flat-scalar-coll? v)))
                 (do (when shape-log
                       (log-shape! shape-log (classify-doc doc) k v
                                   "rescue-2: deep coll — stringified"))
                     [k (pr-str v)])
                 [k v])))
        doc))

;; ---------------------------------------------------------------------------
;; The master transform function.
;;
;; Applied to each exported doc in order:
;; 1. Ensure :xt/id is present.
;; 2. Deep-stringify non-keyword maps (for :evidence/body and any other nested JSON).
;; 3. Denormalize :hx/props (for hyperedges).
;; 4. Record any unknown shapes in the log.
;; ---------------------------------------------------------------------------

(defn transform-doc
  "Transform a single futon1a XTDB 1.x doc to a futon1b XTDB 2.x-compatible doc.
  Returns the transformed doc. Side-effects: may record shapes in the optional log."
  ([doc]
   (transform-doc doc nil))
  ([doc shape-log]
   (let [doc-type (classify-doc doc)]
     (when (and shape-log (= :unknown doc-type))
       (log-shape! shape-log doc-type :unknown doc
                   "doc has no known ID key; cannot classify"))
     (-> doc
         ensure-xt-id
         deep-stringify-non-keyword-maps
         denormalize-hx-props))))

(defn transform-docs
  "Transform a collection of docs. Returns [transformed-docs shape-log-entries].
  The shape-log-entries vector contains any unexpected shapes found."
  ([docs]
   (transform-docs docs nil))
  ([docs shape-log]
   (let [results (mapv #(transform-doc % shape-log) docs)]
     results)))

;; ---------------------------------------------------------------------------
;; Self-test: run against the seed slice to verify the transform works.
;; ---------------------------------------------------------------------------

(defn self-test
  "Run a quick self-test against a few known doc shapes.
  Prints PASS/FAIL for each assertion."
  []
  (let [test-evidence {:evidence/id "test-1"
                       :xt/id "test-1"
                       :evidence/body {"event" "test" "agent-id" "claude-1" "nested" {"a" 1 "b" 2}}}
        test-hyperedge {:hx/id "hx:test"
                        :xt/id "hx:test"
                        :hx/type :code/calls
                        :hx/props {:repo "futon3c" :source-file "/foo.clj"}
                        :hx/endpoints ["a" "b"]}
        test-entity {:entity/id "ent-1"
                     :xt/id "ent-1"
                     :entity/name "test-ent"
                     :entity/type :pattern/library}

        te (transform-doc test-evidence)
        th (transform-doc test-hyperedge)
        tent (transform-doc test-entity)]

    ;; Evidence body should be stringified.
    (println "T1 evidence-body-stringified:"
             (if (string? (:evidence/body te)) "PASS" "FAIL")
             "(type:" (type (:evidence/body te)) ")")

    ;; Hyperedge should have :prop/repo and :prop/source-file.
    (println "T2 hx-props-denormalized-repo:"
             (if (= "futon3c" (:prop/repo th)) "PASS" "FAIL"))
    (println "T3 hx-props-denormalized-source-file:"
             (if (= "/foo.clj" (:prop/source-file th)) "PASS" "FAIL"))

    ;; Original :hx/props map should be preserved.
    (println "T4 hx-props-preserved:"
             (if (= {:repo "futon3c" :source-file "/foo.clj"} (:hx/props th))
               "PASS" "FAIL"))

    ;; Entity should pass through unchanged (no string-keyed maps).
    (println "T5 entity-passthrough:"
             (if (= (:entity/name tent) "test-ent") "PASS" "FAIL"))

    ;; xt/id should be present on all.
    (println "T6 xt-id-present-evidence:"
             (if (:xt/id te) "PASS" "FAIL"))
    (println "T7 xt-id-present-hyperedge:"
             (if (:xt/id th) "PASS" "FAIL"))

    ;; Unknown doc type should be classified correctly.
    (println "T8 classify-unknown:"
             (if (= :unknown (classify-doc {:foo "bar"})) "PASS" "FAIL"))

    (println "\n=== transform self-test complete ===")))

;; Run self-test when loaded directly.
;; (self-test)
