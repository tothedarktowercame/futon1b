(ns test-arxana-edge-join
  (:require [futon1b-arxana :as arxana]
            [futon1b-xt :as fxt]
            [xtdb.node :as xtn]))

(def !results (atom []))

(defn check! [label value]
  (swap! !results conj [label (boolean value)])
  (println label (if value "PASS" "FAIL")))

(defn- command [i record]
  {:protocol/version "arxana.v1"
   :message/id (str "edge-join-fixture-" i)
   :operation :clusion/assert
   :body {:record record}})

(defn- edge [id type endpoints]
  {:id (str "arxana:clusion:sha256:invented-" id)
   :type-revision type
   :endpoints (mapv (fn [[role target]]
                      {:role role :kind :external-ref :target {:ref/id target}})
                    endpoints)
   :asserted-content {}})

(def fixtures
  [(edge "p1" [:pattern/proposal 1] [[:turn "turn-a"] [:pattern "pattern-a"]])
   (edge "o1" [:pattern/occurrence 1] [[:turn "turn-a"] [:pattern "pattern-a"]])
   (edge "r1" [:memory/recall 1] [[:turn "turn-a"] [:memory "memory-a"]])
   ;; turn-b deliberately has only two of the three animal records.
   (edge "p2" [:pattern/proposal 1] [[:turn "turn-b"] [:pattern "pattern-b"]])
   (edge "o2" [:pattern/occurrence 1] [[:turn "turn-b"] [:pattern "pattern-b"]])
   ;; A second complete row witnesses limit + truncation.
   (edge "p3" [:pattern/proposal 1] [[:turn "turn-c"] [:pattern "pattern-c"]])
   (edge "o3" [:pattern/occurrence 1] [[:turn "turn-c"] [:pattern "pattern-c"]])
   (edge "r3" [:memory/recall 1] [[:turn "turn-c"] [:memory "memory-c"]])])

(def join-request
  {:query/type :edge-join
   :patterns [{:edge-type [:pattern/proposal 1]
               :bind {:turn "t" :pattern "p"}}
              {:edge-type [:pattern/occurrence 1]
               :bind {:turn "t" :pattern "p"}}
              {:edge-type [:memory/recall 1]
               :bind {:turn "t" :memory "m"}}]})

(defn- reason [f]
  (try (f) nil
       (catch clojure.lang.ExceptionInfo e
         (get-in (ex-data e) [:error :reason]))))

(defn -main [& _]
  (with-open [node (xtn/start-node)]
    (doseq [[i record] (map-indexed vector fixtures)]
      (arxana/conditional-command! node (command i record)))
    (let [result (arxana/edge-join node join-request)
          rows (:rows result)]
      (check! "edge-join: three animals return exactly complete turn+pattern rows"
              (and (= 2 (:row-count result))
                   (= #{["turn-a" "pattern-a" "memory-a"]
                        ["turn-c" "pattern-c" "memory-c"]}
                      (set (map (juxt :t :p :m) rows)))
                   (every? #(= 3 (count (:edges %))) rows)
                   (not-any? #(= "turn-b" (:t %)) rows)))
      (let [filtered (arxana/edge-join node
                                       (assoc join-request :where {"t" "turn-a"}))]
        (check! "edge-join: where constant filters a shared variable"
                (and (= 1 (:row-count filtered))
                     (= "turn-a" (get-in filtered [:rows 0 :t])))))
      (let [limited (arxana/edge-join node (assoc join-request :limit 1))]
        (check! "edge-join: limit uses a sentinel and reports truncation"
                (and (= 1 (:row-count limited)) (:truncated? limited))))
      ;; The bound must reach the query itself, not only the returned rows:
      ;; a two-pattern join has three matches (turns a, b, c), so a query
      ;; run with limit 1 must fetch at most the limit+1 sentinel.
      (let [fetched (atom nil)
            real fxt/timed-q]
        (with-redefs [fxt/timed-q (fn [& args]
                                    (let [rows (apply real args)]
                                      (reset! fetched (count rows))
                                      rows))]
          (arxana/edge-join node {:patterns (subvec (:patterns join-request) 0 2)
                                  :limit 1}))
        (check! "edge-join: the limit bounds the XTQL query, not just the rows"
                (= 2 @fetched)))
      (check! "edge-join: unknown role is a typed rejection"
              (= :edge-join-unknown-role
                 (reason #(arxana/edge-join
                           node {:patterns [{:edge-type [:pattern/proposal 1]
                                             :bind {:unknown "x"}}
                                            {:edge-type [:memory/recall 1]
                                             :bind {:turn "x"}}]}))))
      (check! "edge-join: five patterns are a typed rejection"
              (= :edge-join-pattern-count
                 (reason #(arxana/edge-join
                           node {:patterns (vec (repeat 5
                                                        {:edge-type [:memory/recall 1]
                                                         :bind {:turn "t"}}))}))))
      (check! "edge-join: submitted query text is a typed rejection"
              (= :edge-join-unknown-field
                 (reason #(arxana/edge-join
                           node (assoc join-request :xtql "(from :secrets [*])")))))))
  (let [failed (remove second @!results)]
    (println (str (- (count @!results) (count failed)) "/" (count @!results) " PASS"))
    (shutdown-agents)
    (System/exit (if (seq failed) 1 0))))
