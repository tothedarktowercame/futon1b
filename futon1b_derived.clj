(ns futon1b-derived
  "Validated writes and point reads for the operational derived-record table."
  (:require [clojure.string :as str]
            [clojure.set :as set]
            [futon1b-gates :as gates]
            [futon1b-xt :as fxt]
            [migration.transform :as xf]
            [xtdb.api :as xt])
  (:import [java.security MessageDigest]
           [java.time Instant]))

(def batch-max 500)
(def list-max 1000)
(defonce !shape-log (xf/make-shape-log))

(defn- canonical [x]
  (cond
    (map? x) (into (sorted-map-by #(compare (pr-str %1) (pr-str %2)))
                   (map (fn [[k v]] [k (canonical v)])) x)
    (set? x) (vec (sort-by pr-str (map canonical x)))
    (sequential? x) (mapv canonical x)
    :else x))

(defn- sha256 [x]
  (let [digest (.digest (MessageDigest/getInstance "SHA-256")
                        (.getBytes (pr-str (canonical x)) "UTF-8"))]
    (apply str (map #(format "%02x" (bit-and 0xff %)) digest))))

(defn- normalize-family [family]
  (cond
    (keyword? family) family
    (and (string? family) (not (str/blank? family)))
    (keyword (str/replace family #"^:" ""))
    :else nil))

(defn- family-token [family]
  (subs (str family) 1))

(defn- required! [pred field value]
  (when-not pred
    (throw (gates/layered-error 4 :invalid-derived-record
                                {:field field :value value}))))

(defn build-derived-doc
  "Validate and normalize one caller payload. Returns the canonical document,
   its source-link documents, and parsed valid-from instant."
  [payload]
  (let [family (normalize-family (or (:derived/family payload) (:family payload)))
        writer (or (:derived/writer payload) (:writer payload))
        writer-version (or (:derived/writer-version payload) (:writer-version payload))
        source-ids (or (:derived/source-act-ids payload) (:source-act-ids payload))
        subject (or (:derived/subject payload) (:subject payload))
        at (or (:derived/at payload) (:at payload))
        body (if (contains? payload :derived/body) (:derived/body payload) (:body payload))]
    (required! (keyword? family) :derived/family family)
    (required! (and (string? writer) (not (str/blank? writer))) :derived/writer writer)
    (required! (and (string? writer-version) (not (str/blank? writer-version)))
               :derived/writer-version writer-version)
    (required! (and (vector? source-ids) (seq source-ids)
                    (every? #(and (string? %) (not (str/blank? %))) source-ids))
               :derived/source-act-ids source-ids)
    (required! (map? subject) :derived/subject subject)
    (required! (some? body) :derived/body body)
    (let [valid-from (try (Instant/parse at) (catch Throwable _ nil))]
      (required! valid-from :derived/at at)
      (let [source-ids (vec (sort (distinct source-ids)))
            stable-id (or (:derived/id payload) (:id payload)
                          (sha256 [family writer-version source-ids body]))
            id (if (str/starts-with? (str stable-id) "derived:")
                 (str stable-id)
                 (str "derived:" (family-token family) ":" stable-id))
            expected-prefix (str "derived:" (family-token family) ":")]
        (required! (str/starts-with? id expected-prefix) :derived/id id)
        (let [doc (xf/transform-doc
                   {:xt/id id :derived/id id :derived/family family
                    :derived/writer writer :derived/writer-version writer-version
                    :derived/source-act-ids source-ids :derived/subject subject
                    :derived/at at :derived/body body}
                   !shape-log {:log-stringify? true})
              links (mapv (fn [source-id]
                            {:xt/id (str "derived-source:" id ":" source-id)
                             :link/derived-id id :link/source-act-id source-id
                             :link/family family})
                          source-ids)]
          {:doc doc :links links :valid-from valid-from})))))

(defn fetch-by-id
  ([node id] (fetch-by-id node id nil))
  ([node id valid-at]
   (let [from (if valid-at
                (list 'from :derived {:bind '[*]
                                      :for-valid-time (list 'at 'p-at)})
                '(from :derived [*]))
         params (if valid-at '[p-id p-at] '[p-id])
         args (cond-> [id] valid-at (conj valid-at))]
     (some-> (fxt/q1 node (apply fxt/pq params
                                 (list '-> from '(where (= xt/id p-id))) args))
             (dissoc :xt/id)))))

(defn- link-by-id [node id valid-at]
  (let [from (list 'from :derived-source-links
                   {:bind '[*] :for-valid-time (list 'at 'p-at)})]
    (fxt/q1 node (fxt/pq '[p-id p-at]
                         (list '-> from '(where (= xt/id p-id))) id valid-at))))

(defn- comparable [doc] (dissoc doc :xt/id))

(defn- link-id [derived-id source-id]
  (str "derived-source:" derived-id ":" source-id))

(defn- classify-write [node {:keys [doc valid-from] :as prepared}]
  (let [stored (fetch-by-id node (:xt/id doc) valid-from)]
    (cond
      (nil? stored) (assoc prepared :action :write)
      (= (comparable doc) stored) (assoc prepared :action :no-op)
      (= (:derived/at stored) (:derived/at doc))
      (throw (gates/layered-error 1 :derived-id-conflict
                                  {:derived/id (:xt/id doc)
                                   :valid-time (str valid-from)}))
      :else (let [old (set (:derived/source-act-ids stored))
                  new (set (:derived/source-act-ids doc))]
              (assoc prepared :action :write
                     :dropped-link-ids
                     (mapv #(link-id (:xt/id doc) %) (sort (set/difference old new))))))))

(defn- ops-for [{:keys [doc links valid-from dropped-link-ids]}]
  (into (into [[:put-docs {:into :derived :valid-from valid-from} doc]]
              (map (fn [id]
                     [:delete-docs {:from :derived-source-links
                                    :valid-from valid-from} id]))
              dropped-link-ids)
        (map (fn [link]
               [:put-docs {:into :derived-source-links :valid-from valid-from} link]))
        links))

(defn- verify! [node {:keys [doc links valid-from dropped-link-ids]}]
  (let [stored (fetch-by-id node (:xt/id doc) valid-from)
        missing-links (keep #(when-not (= % (link-by-id node (:xt/id %) valid-from))
                               (:xt/id %)) links)
        stale-links (filterv #(link-by-id node % valid-from) dropped-link-ids)]
    (when-not (and (= (comparable doc) stored) (empty? missing-links)
                   (empty? stale-links))
      (throw (gates/layered-error 0 :postcommit-missing-derived
                                  {:derived/id (:xt/id doc)
                                   :missing-links (vec missing-links)
                                   :stale-links stale-links})))
    true))

(defn- rescued [prepared stage]
  (let [reshape (case stage
                  :rescue-1 #(xf/stringify-risky-nils % !shape-log)
                  :rescue-2 #(xf/stringify-deep-colls
                              (xf/stringify-risky-nils % nil) !shape-log))]
    (-> prepared (update :doc reshape) (update :links #(mapv reshape %)))))

(defn- commit-with-rescue! [node prepared]
  (loop [[[stage candidate] & more]
         [[:put prepared]
          [:rescue-1 (rescued prepared :rescue-1)]
          [:rescue-2 (rescued prepared :rescue-2)]]]
    (let [result (try
                   (xt/execute-tx node (ops-for candidate))
                   (verify! node candidate)
                   {:stage stage}
                   (catch Throwable error {:error error}))]
      (if-let [error (:error result)]
        (if (seq more) (recur more) (throw error))
        (:stage result)))))

(defn- execute-prepared! [node prepared]
  (let [stage (commit-with-rescue! node prepared)]
    (cond-> {:ok true :derived/id (get-in prepared [:doc :derived/id])}
      (not= :put stage) (assoc :rescue stage))))

(defn write-derived!
  "Write one derived row and all source links atomically. Identical replay at
   the same valid time is a no-op; a different row is a layer-1 conflict."
  [node payload]
  (let [{:keys [action] :as prepared}
        (classify-write node (build-derived-doc payload))]
    (if (= :no-op action)
      (do (verify! node prepared)
          {:ok true :derived/id (get-in prepared [:doc :derived/id]) :no-op? true})
      (execute-prepared! node prepared))))

(defn write-derived-batch!
  "Write at most 500 payloads. Valid new rows sharing valid-time use one
   transaction; invalid/conflicting rows fail independently. A failed group
   transaction is retried item-by-item through the single writer."
  [node payloads]
  (when (> (count payloads) batch-max)
    (throw (gates/layered-error 4 :batch-too-large
                                {:max batch-max :count (count payloads)})))
  (let [prepared (mapv (fn [index payload]
                         (try {:index index
                               :prepared (classify-write node (build-derived-doc payload))}
                              (catch Throwable error {:index index :error error :payload payload})))
                       (range) payloads)
        results (atom (vec (repeat (count payloads) nil)))]
    (doseq [{:keys [index error]} prepared
            :when error]
      (swap! results assoc index {:ok false :error (.getMessage ^Throwable error)
                                  :reason (or (get-in (ex-data error) [:error :reason])
                                              :invalid-derived-record)}))
    (doseq [{:keys [index prepared]} prepared
            :when (= :no-op (:action prepared))]
      (try
        (verify! node prepared)
        (swap! results assoc index {:ok true :derived/id (get-in prepared [:doc :derived/id])
                                    :no-op? true})
        (catch Throwable error
          (swap! results assoc index {:ok false :error (.getMessage error)}))))
    (doseq [[_ group] (group-by (comp :valid-from :prepared)
                                (filter #(= :write (get-in % [:prepared :action])) prepared))]
      (let [items (vec group)
            committed? (try
                         (xt/execute-tx node (into [] (mapcat (comp ops-for :prepared)) items))
                         (doseq [{:keys [prepared]} items] (verify! node prepared))
                         true
                         (catch Throwable _ false))]
        (doseq [{:keys [index prepared]} items]
          (if committed?
            (swap! results assoc index {:ok true :derived/id (get-in prepared [:doc :derived/id])})
            (try
              (swap! results assoc index (write-derived! node (nth payloads index)))
              (catch Throwable error
                (swap! results assoc index {:ok false :error (.getMessage error)
                                            :reason (or (get-in (ex-data error) [:error :reason])
                                                        :derived-write-failed)})))))))
    @results))

(defn list-derived
  "Return one current-version page ordered by derived id. Source lookup uses
   the scalar companion rows, then resolves each current derived document."
  [node {:keys [source-act-id family limit after]}]
  (let [family (some-> family normalize-family)
        limit (long (or limit 100))
        _ (required! (<= 1 limit list-max) :limit limit)
        after (or after "")
        rows (if source-act-id
               (let [links (fxt/safe-q
                            node (if family
                                   (fxt/pq '[p-source p-family p-after p-limit]
                                           '(-> (from :derived-source-links [link/derived-id link/source-act-id link/family])
                                                (where (and (= link/source-act-id p-source)
                                                            (= link/family p-family)
                                                            (> link/derived-id p-after)))
                                                (order-by link/derived-id)
                                                (limit p-limit))
                                           source-act-id family after (inc limit))
                                   (fxt/pq '[p-source p-after p-limit]
                                           '(-> (from :derived-source-links [link/derived-id link/source-act-id])
                                                (where (and (= link/source-act-id p-source)
                                                            (> link/derived-id p-after)))
                                                (order-by link/derived-id)
                                                (limit p-limit))
                                           source-act-id after (inc limit))))]
                 (keep #(fetch-by-id node (:link/derived-id %)) links))
               (fxt/safe-q
                node (if family
                       (fxt/pq '[p-family p-after p-limit]
                               '(-> (from :derived [*])
                                    (where (and (= derived/family p-family)
                                                (> xt/id p-after)))
                                    (order-by xt/id)
                                    (limit p-limit))
                               family after (inc limit))
                       (fxt/pq '[p-after p-limit]
                               '(-> (from :derived [*])
                                    (where (> xt/id p-after))
                                    (order-by xt/id)
                                    (limit p-limit))
                               after (inc limit)))))
        rows (cond->> rows family (filter #(= family (:derived/family %))))
        page (vec (take limit rows))]
    {:derived (mapv #(dissoc % :xt/id) page)
     :count (count page)
     :next-cursor (when (> (count rows) limit)
                    (:derived/id (peek page)))}))

(defn census
  "Exact current-version counts, optionally restricted to one family."
  [node family]
  (let [family (some-> family normalize-family)
        rows (fxt/safe-q node (if family
                                (fxt/pq '[p-family]
                                        '(-> (from :derived [derived/family])
                                             (where (= derived/family p-family)))
                                        family)
                                '(from :derived [derived/family])))
        counts (frequencies (map :derived/family rows))]
    {:ok true :total (reduce + 0 (vals counts)) :families counts}))
