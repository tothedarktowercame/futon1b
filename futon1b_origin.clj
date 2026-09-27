(ns futon1b-origin
  "Wire contract for write-time provenance. Origin is not an authorization grant."
  (:require [clojure.string :as str])
  (:import [java.time Instant]))

(def kinds #{:operator :agent :harness :unknown})
(def required #{:kind :actor :writer :attributed-author :authorization :recorded-at :basis})
(def allowed (into required [:source-id :surface]))
(defn normalize [origin]
  (when (map? origin)
    (into {} (map (fn [[k v]]
                   (let [k (if (string? k) (keyword k) k)]
                     [k (cond
                          (and (#{:kind :basis} k) (string? v)) (keyword v)
                          (and (= :authorization k) (= "unknown" v)) :unknown
                          :else v)]))) origin)))

(defn valid? [origin]
  (let [m (normalize origin)]
    (boolean
     (and m (every? allowed (keys m)) (every? #(contains? m %) required)
          (contains? kinds (:kind m)) (= :write-time (:basis m))
          ;; No grant is inferred from provenance. A grant reference is a string.
          (or (= :unknown (:authorization m))
              (and (string? (:authorization m)) (not (str/blank? (:authorization m)))))
          (every? #(and (string? (get m %)) (not (str/blank? (get m %))))
                  (filter #(contains? m %) [:actor :writer :attributed-author :source-id :surface]))
          (try (Instant/parse (:recorded-at m)) true (catch Exception _ false))))))
