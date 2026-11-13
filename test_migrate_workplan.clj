(ns test-migrate-workplan
  "Invented fixtures only. No running services or real records are accessed."
  (:require [clojure.test :refer [deftest is run-tests]]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [scripts.migrate-workplan :as m]
            [futon3c.arxana.records :as r]
            [futon1b-arxana :as a]
            [futon1b-arxana-index :as idx]
            [futon1b-xt :as fxt]
            [xtdb.api :as xt]
            [xtdb.node :as xtn]))

(defn cl [type eps content]
  (r/make-clusion {:type-revision type :endpoints eps :asserted-content content}))
(defn ep [role c] {:role role :kind :clusion :target {:clusion/id (:id c)}})
(defn evidence [c method content]
  (r/make-attestation {:subject-clusion-id (:id c) :producer "invented-reader"
                       :method {:id method :version 1} :basis :model
                       :standing :unverified :coverage :partial :content content
                       :source-refs [{:file "invented.md" :line 1}]}))
(defn verdict [c reason]
  (r/make-assessment {:subject-clusion-id (:id c) :actor "invented-reviewer"
                     :policy "invented-legacy-policy" :verdict :rejected
                     :reason reason :time "2026-01-01"}))
(def item (cl [:workplan/item 1] [] {:item/id "M-invented"}))
(def lexical (cl [:lexicon/entity 1] [] {:entity/slug "invented-word"}))
(def benefit (cl [:benefit-line 1] [] {:benefit/line "invented-benefit"}))
(def serves (cl [:workplan/serves 1] [(ep :item item) (ep :benefit-line benefit)] {}))
(def rule (cl [:workplan/entity-match-rule 1] [(ep :entity lexical)] {:rule/id "invented-rule"}))
(def catalog (cl [:workplan/catalog 1] [] {:catalog/id "entity-match-rules"}))
(def member (cl [:workplan/entity-rule-member 1] [(ep :catalog catalog) (ep :rule rule)] {}))
(def referent (cl [:workplan/referent 1] [(ep :entity lexical) (ep :target benefit)] {:referent/role "invented-relation"}))
(def reading (evidence item "invented-reading" {:pending/level 4 :sheet/one-line "Invented historical narrative"}))
(def disabled (assoc (evidence rule "invented-rules" {:forms [{:form "invented" :enabled false}]}) :basis :human :standing :verified))
(def mapping {(:id item) {:status :resolved :entity-id "fixture/mission/invented" :entity-type :mission/doc :external-id "M-invented"}})
(def source [item lexical benefit serves rule catalog member referent reading
             (r/make-attestation disabled)
             (evidence serves "invented-method-a" {:reason "Invented reason A"})
             (evidence serves "invented-method-b" {:reason "Invented reason B"})
             (verdict referent "Invented rejection A") (verdict referent "Invented rejection B")
             (r/make-assessment (assoc (verdict item "Invented acceptance") :verdict :accepted))
             (cl [:workplan/catalog 1] [] {:catalog/id "workplan/all-items" :items ["M-invented"]})])

(deftest historical-transform-preserves-meaning-and-identity
  (let [result (m/transform source mapping) rs (:records result)
        changed (first (filter #(= [:workplan/serves 2] (:type-revision %)) rs))
        atts (filter #(= (:id changed) (:subject-clusion-id %)) rs)
        anchors (filter #(= [:giraffe/legacy-reading 1] (:type-revision %)) rs)
        legacy (first (filter #(= :giraffe/legacy-reading (:attest/kind %)) rs))]
    (is (= (count source) (count (:manifest result))))
    (is (not-any? #(= [:workplan/item 1] (:type-revision %)) rs))
    (is (= "fixture/mission/invented" (get-in changed [:endpoints 0 :target :entity/id])))
    (is (not= (:id serves) (:id changed)))
    (is (= 2 (count atts)))
    (is (= #{"invented-method-a" "invented-method-b"} (set (map #(get-in % [:method :id]) atts))))
    (is (every? #(= :unverified (:standing %)) atts))
    (is (some #{lexical} rs))
    (is (some #{(r/make-attestation disabled)} rs))
    (is (= 2 (count (filter #(= :rejected (:verdict %)) rs))))
    (is (= 1 (count (filter #(= [:workplan/closure-claim 1] (:type-revision %)) rs))))
    (is (= 1 (count anchors)))
    (is (= (:content reading) (:content legacy)))
    (is (= (:source-refs reading) (:source-refs legacy)))
    (is (:legacy/reduction? legacy))
    (is (nil? (:time legacy)))
    (is (nil? (:document/digest (first anchors))))
    (is (= :unknown (get-in (first anchors) [:asserted-content :legacy/freshness])))
    (is (not-any? #(= "workplan/all-items" (get-in % [:asserted-content :catalog/id])) rs))
    (is (= (pr-str result) (pr-str (m/transform (reverse source) mapping))))))

(deftest absent-and-ambiguous-items-quarantine-dependent-history
  (doseq [reason [:missing :ambiguous :fixture-path-ambiguity]]
    (let [result (m/transform source {(:id item) {:status :quarantined :reason reason}})
          manifests (into {} (map (juxt :source-id identity)) (:manifest result))]
      (doseq [id [(:id item) (:id serves) (:id reading)]]
        (is (= :quarantined (:disposition (manifests id)))))
      (is (not-any? #(seq (m/entity-deps %)) (:records result)))
      (is (some #{lexical} (:records result))))))

(deftest append-is-atomic-idempotent-and-maintains-source-and-cache
  (let [dir (.toFile (java.nio.file.Files/createTempDirectory "invented-workplan-" (make-array java.nio.file.attribute.FileAttribute 0)))
        index (idx/open-index (str (io/file dir "index.db")))
        rs (:records (m/transform source mapping))
        entity {:xt/id "fixture/mission/invented" :entity/type :mission/doc
                :entity/external-id "M-invented" :entity/props {:mission/path "/invented/M-invented.md"}}]
    (with-open [node (xtn/start-node)]
      (xt/execute-tx node [[:put-docs :entities entity]])
      (let [first-run (m/append-batch! node index rs mapping (str dir))
            before (idx/snapshot index)
            again (m/append-batch! node index rs mapping (str dir))
            link (first (filter #(= [:workplan/serves 2] (:type-revision %)) rs))
            edge-query {:edge-type [:workplan/serves 2] :endpoint-role :item
                        :endpoint-id "fixture/mission/invented" :limit 100}]
        (is (= {(keyword "existing-identical") (count rs)}
               (:target-counts (m/dry-run! node {:records rs :counts {}}))))
        (is (= {:conflict 1} (:target-counts
                            (m/dry-run! node {:records [(assoc lexical :note "invented conflict")] :counts {}}))))
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Item mirrors are forbidden"
              (m/append-batch! node index [item] mapping (str dir))))
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Item mapping changed"
              (m/append-batch! node index rs (assoc-in mapping [(:id item) :entity-path] "/invented/wrong.md") (str dir))))
        (is (= (count rs) (:created first-run)))
        (is (= 0 (:created again)))
        (is (= (count rs) (:equal again)))
        (is (= (count rs) (a/source-sequence node) (idx/index-watermark index)))
        (is (= before (idx/snapshot index)))
        (is (= (count rs) (count (fxt/safe-q node '(from :arxana_index_events [*])))))
        (is (= (count rs) (count (fxt/safe-q node '(from :arxana_outcomes [*])))))
        (is (= #{(:id link)} (set (:edge-ids (idx/query-edges index node edge-query)))))
        (is (= #{(:id link)} (set (map :xt/id (a/clusions-by-type-and-role node :workplan/serves :item "fixture/mission/invented" 100)))))
        (is (= (set (map :id (a/evidence-for node (:id link))))
               (set (:attestation-ids (idx/attestations index node (:id link) 100)))))
        (let [bad (assoc lexical :note "invented immutable-content conflict")]
          (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Immutable record conflict"
                (m/append-batch! node index [bad] mapping (str dir)))))
        (let [new (cl [:lexicon/entity 1] [] {:entity/slug "invented-cas-race"})
              stale-ops (m/batch-ops [new] [] 0 {})]
          (is (thrown? Throwable (xt/execute-tx node stale-ops)))
          (is (nil? (a/clusion-by-id node (:id new))))
          (is (= (count rs) (a/source-sequence node))))
        (let [missing (evidence (cl [:lexicon/entity 1] [] {:entity/slug "absent"}) "invented" {})]
          (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Missing clusion dependency"
                (m/append-batch! node index [missing] mapping (str dir)))))
        (let [other (assoc entity :xt/id "fixture/mission/other" :entity/external-id "M-other")
              new (cl [:lexicon/entity 1] [] {:entity/slug "invented-multi-entity-guard"})
              entities {(:xt/id entity) entity (:xt/id other) other}
              counter (a/source-sequence node)]
          (xt/execute-tx node [[:put-docs :entities other]])
          (is (thrown? Throwable
                (xt/execute-tx node (m/batch-ops [new] [] counter
                                    (assoc-in entities [(:xt/id other) :entity/external-id] "M-wrong")))))
          (is (nil? (a/clusion-by-id node (:id new))))
          (xt/execute-tx node (m/batch-ops [new] [] counter entities))
          (is (= new (a/clusion-by-id node (:id new))))
          (is (= (inc counter) (a/source-sequence node))))
        (is (every? #(= :ABSENT %) (vals (:records (edn/read-string (slurp (:backup first-run)))))))))))

(deftest entity-preflight-does-not-exhaust-query-permits
  (let [active (atom 0) peak (atom 0)]
    (with-redefs [m/point-doc (fn [_ _ id]
                              (let [n (swap! active inc)]
                                (swap! peak max n)
                                (try (Thread/sleep 20) {:xt/id id}
                                     (finally (swap! active dec)))))]
      (is (= (into {} (for [id (range 12)] [id {:xt/id id}]))
             (m/point-docs nil :entities (range 12))))
      (is (<= @peak 4)))))

(let [result (run-tests 'test-migrate-workplan)]
  (shutdown-agents)
  (when (pos? (+ (:fail result) (:error result))) (System/exit 1)))
