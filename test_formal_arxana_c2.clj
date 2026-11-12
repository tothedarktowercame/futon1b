(ns test-formal-arxana-c2
  (:require [futon1b-arxana :as arxana]
            [futon1b-xt :as fxt]
            [xtdb.node :as xtn]))

(def !results (atom []))
(defn check! [label x]
  (swap! !results conj [label (boolean x)])
  (println label (if x "PASS" "FAIL")))
(defn command [mid op record]
  {:protocol/version "arxana.v1" :message/id mid :operation op
   :body {:record record}})
(defn thrown-reason [f]
  (try (f) nil (catch clojure.lang.ExceptionInfo e (get-in (ex-data e) [:error :reason]))))

(defn -main [& _]
  (with-open [node (xtn/start-node)]
    (let [claim {:id "arxana:clusion:sha256:claim1"
                 :type-revision [:interpretation 1]
                 :endpoints [] :asserted-content {}}
          evidence-a {:id "arxana:attestation:sha256:a1"
                      :subject-clusion-id (:id claim) :producer "one"}
          evidence-b {:id "arxana:attestation:sha256:a2"
                      :subject-clusion-id (:id claim) :producer "two"}
          accept {:id "arxana:record:sha256:accept"
                  :subject-clusion-id (:id claim) :verdict :accepted}
          reject {:id "arxana:record:sha256:reject"
                  :subject-clusion-id (:id claim) :verdict :rejected}]
      (arxana/conditional-command! node (command "c1" :clusion/assert claim))
      (check! "append_keeps" (= claim (arxana/clusion-by-id node (:id claim))))
      (let [dup (arxana/conditional-command! node (command "c1" :clusion/assert
                                                       (assoc claim :asserted-content {:changed true})))]
        (check! "message-id replay returns first outcome" (:duplicate? dup))
        (check! "message-id replay writes nothing new"
                (= claim (arxana/clusion-by-id node (:id claim)))))
      ;; review (claude-13): an equal record whose keys were inserted in
      ;; another order is the same record, not an immutability conflict
      (let [reordered (into {} (reverse (seq claim)))
            r (try (arxana/conditional-command! node (command "c-reordered" :clusion/assert reordered))
                   (catch clojure.lang.ExceptionInfo e (get-in (ex-data e) [:error :reason])))]
        (check! "equal record with reordered keys re-asserts without conflict"
                (and (map? r) (false? (get-in r [:ok :created?])))))
      (arxana/conditional-command! node (command "e1" :evidence/append evidence-a))
      (arxana/conditional-command! node (command "e2" :evidence/append evidence-b))
      (check! "attest_keeps_claims" (= claim (arxana/clusion-by-id node (:id claim))))
      (check! "two_interpreters_one_claim"
              (= #{(:id evidence-a) (:id evidence-b)}
                 (set (map :id (arxana/evidence-for node (:id claim))))))
      (arxana/conditional-command! node (command "a1" :assessment/append accept))
      (arxana/conditional-command! node (command "a2" :assessment/append reject))
      (check! "assess_keeps_claims" (= claim (arxana/clusion-by-id node (:id claim))))
      (check! "assess_keeps_evidence" (= 2 (count (arxana/evidence-for node (:id claim)))))
      (check! "conflicting_assessments_coexist"
              (= #{:accepted :rejected}
                 (set (map :verdict (arxana/assessments-for node (:id claim))))))
      (check! "unknown clusion refuses and failed validation writes nothing"
              (and (= :arxana-unknown-clusion
                      (thrown-reason #(arxana/conditional-command!
                                       node (command "bad-subject" :evidence/append
                                                     (assoc evidence-a :id "arxana:attestation:sha256:bad"
                                                            :subject-clusion-id "arxana:clusion:sha256:missing")))))
                   (nil? (arxana/outcome-by-message node "bad-subject"))))
      (check! "reserved route refuses non-arxana record identity"
              (= :arxana-invalid-record-id
                 (thrown-reason #(arxana/conditional-command!
                                  node (command "bad-id" :clusion/assert
                                                (assoc claim :id "plain:claim"))))))
      ;; --- Flat derived columns (M-repl-circus, 2026-11-12) ---
      (let [link {:id "arxana:clusion:sha256:link1"
                  :type-revision [:workplan/names 1]
                  :endpoints [{:role :item :kind :clusion
                               :target {:clusion/id (:id claim)}}
                              {:role :entity :kind :external-ref
                               :target {:ref/id "entity-abc"}}]
                  :asserted-content {}}
            rich-att {:id "arxana:attestation:sha256:a3"
                      :subject-clusion-id (:id claim)
                      :producer "giraffe" :basis :model :standing :unverified
                      :coverage :complete :method {:id "giraffe/state-group" :version 1}
                      :attest/kind :giraffe/state-group}
            flat-match? (fn [op record]
                          (let [table (case op :clusion/assert :arxana_clusions
                                               :evidence/append :arxana_evidence
                                               :assessment/append :arxana_assessments)
                                cols (arxana/flat-columns op record)
                                col-syms (mapv (fn [k] (symbol (namespace k) (name k))) (keys cols))
                                ks (into ['xt/id 'record/data] col-syms)
                                row (first (fxt/safe-q node
                                            (fxt/pq '[p-id]
                                                    (list '-> (list 'from table ks)
                                                          (list 'where (list '= 'xt/id 'p-id)))
                                                    (:id record))))]
                            (and row
                                 (= (select-keys row (keys cols)) cols)
                                 ;; record/data never carries the flat columns:
                                 (= (:record/data row)
                                    (arxana/canonical-record-data record)))))]
        (arxana/conditional-command! node (command "c2" :clusion/assert link))
        (arxana/conditional-command! node (command "e3" :evidence/append rich-att))
        (check! "clusion doc carries clusion/type and role/* columns, record/data untouched"
                (flat-match? :clusion/assert link))
        (check! "attestation doc carries attest/* columns"
                (flat-match? :evidence/append rich-att))
        (check! "assessment doc carries assess/verdict"
                (flat-match? :assessment/append accept))
        (check! "XTQL by type + role endpoint finds the clusion"
                (= [(:id link)]
                   (mapv :xt/id (arxana/clusions-by-type-and-role
                                 node :workplan/names :item (:id claim) 10))))
        (check! "a different endpoint does not match"
                (empty? (arxana/clusions-by-type-and-role
                         node :workplan/names :item "arxana:clusion:sha256:other" 10)))
        (check! "newest-evidence finds the rich attestation by kind"
                (= (:id rich-att)
                   (:xt/id (arxana/newest-evidence node (:id claim) :giraffe/state-group))))
        (check! "record id identical with and without flattening"
                (and (= (:id link) "arxana:clusion:sha256:link1")
                     (= link (arxana/clusion-by-id node (:id link)))))
        ;; Plant: a clusion WRITTEN with one role column dropped must fail
        ;; the shape check (the stored doc no longer matches the record).
        (let [orig-flat arxana/flat-columns
              planted-link (assoc link :id "arxana:clusion:sha256:link-planted")]
          (with-redefs [arxana/flat-columns
                        (fn [op record] (dissoc (orig-flat op record) :role/entity))]
            (arxana/conditional-command! node (command "c3" :clusion/assert planted-link)))
          (check! "plant: a dropped role column fails the shape check"
                  (not (flat-match? :clusion/assert planted-link)))))))
  (let [failed (remove second @!results)]
    (println (str (- (count @!results) (count failed)) "/" (count @!results) " PASS"))
    (shutdown-agents)
    (System/exit (if (seq failed) 1 0))))
