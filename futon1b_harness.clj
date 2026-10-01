(ns futon1b-harness
  "Execution context, independent of origin and grants. Absence is preserved."
  (:require [clojure.string :as str]))

(def kinds #{:war-machine :zai :none :unknown})
(def allowed #{:kind :basis :execution-id :reason :source-ref})
(defn normalize [value]
  (when (map? value)
    (into {} (map (fn [[k v]]
                   (let [k (if (string? k) (keyword k) k)]
                     [k (if (and (#{:kind :basis} k) (string? v)) (keyword v) v)])))
          value)))
(defn- text? [v] (and (string? v) (not (str/blank? v))))
(defn refusal [value]
  (let [m (normalize value)]
    (cond
      (nil? m) :invalid-harness-map
      (not-every? allowed (keys m)) :unexpected-harness-key
      (not (contains? kinds (:kind m))) :unknown-harness-kind
      (not= :producer-context (:basis m)) :invalid-harness-basis
      (= :zai (:kind m)) :zai-harness-not-deployed
      (and (= :war-machine (:kind m)) (not (text? (:execution-id m)))) :missing-execution-id
      (and (= :unknown (:kind m)) (not (text? (:reason m)))) :missing-harness-reason
      (some #(and (contains? m %) (not (text? (get m %))))
            [:execution-id :reason :source-ref]) :invalid-harness-string
      :else nil)))
