(ns test-ent-serving
  "P2 tests: same-id write/hook ordering and indexed entity-read parity.

  Run: clojure -M:node -m test-ent-serving
  Bad case: clojure -M:node -m test-ent-serving --disable-write-locks"
  (:require [clojure.test :refer [deftest is run-tests testing use-fixtures]]
            [futon1b-entindex :as ent]
            [futon1b-graph :as graph]
            [futon1b-text :as text]
            [futon1b-xt :as fxt]
            [xtdb.node :as xtn])
  (:import [java.nio.file Files]))

(def ^:dynamic *node* nil)
(def ^:dynamic *disable-write-locks?* false)

(defn- temp-dir []
  (-> (Files/createTempDirectory
       "futon1b-ent-serving-test-"
       (make-array java.nio.file.attribute.FileAttribute 0))
      .toFile .getAbsolutePath))

(defn- entity [id t body]
  {:xt/id id :entity/id id :entity/name id :entity/type t
   :entity/props {:body body}})

(defn- stored [id]
  (first (fxt/hydrate-by-ids *node* :entities [id])))

(defn- indexed [id]
  (some #(when (= id (:xt/id %)) %)
        (ent/type-page {:type (:entity/type (stored id))})))

(deftest same-id-store-and-hook-order-is-atomic
  ;; Deterministically create the old race: pause the old hook after its store
  ;; write, let the new write finish, then let the old hook overwrite SQLite.
  ;; The stripe prevents the new STORE write from passing the paused old hook.
  (let [entered (promise)
        release-old (promise)
        original ent/on-put!
        hook (fn [doc]
               (if (= "old" (get-in doc [:entity/props :body]))
                 (future
                   (deliver entered true)
                   @release-old
                   (ent/index-doc! @text/!ds doc))
                 (original doc)))]
    (with-redefs [ent/on-put! hook]
      (binding [graph/*entity-write-locking?* (not *disable-write-locks?*)]
        (let [old-write (future (graph/put-verified!
                                 *node* :entities
                                 (entity "same-id-order" :probe/order "old")))]
          (is (true? (deref entered 5000 false)) "old write reached its paused hook")
          (let [new-write (future (graph/put-verified!
                                   *node* :entities
                                   (entity "same-id-order" :probe/order "new")))]
            ;; Without locking, the new write and hook finish before release.
            ;; With locking, it waits at the same stripe.
            (when *disable-write-locks?*
              (is (= :ok (deref new-write 5000 ::timeout))))
            (deliver release-old true)
            (is (= :ok @old-write))
            (is (= :ok @new-write))))))
    (is (= (stored "same-id-order") (indexed "same-id-order"))
        "ent_node body equals the store after deliberately reversed hooks")))

(deftest concurrent-same-id-rounds-end-at-the-store-body
  (binding [graph/*entity-write-locking?* (not *disable-write-locks?*)]
    (doseq [round (range 8)]
      (let [writers (doall
                     (for [thread (range 8)]
                       (future
                         (graph/put-verified!
                          *node* :entities
                          (entity "same-id-stress" :probe/stress
                                  [round thread])))))]
        (doseq [writer writers] @writer)
        (is (= (stored "same-id-stress") (indexed "same-id-stress"))
            (str "round " round " leaves store and sidecar equal"))))))

(deftest different-id-writes-reach-their-hooks-in-parallel
  (let [id-a "parallel-a"
        stripe-a (Math/floorMod (hash id-a) 256)
        id-b (first (remove #(= stripe-a (Math/floorMod (hash %) 256))
                            (map #(str "parallel-" %) (range 100))))
        entered (atom #{})
        both-entered (promise)
        release (promise)
        hook (fn [doc]
               (future
                 (when (= 2 (count (swap! entered conj (:xt/id doc))))
                   (deliver both-entered true))
                 @release
                 (ent/index-doc! @text/!ds doc)))]
    (with-redefs [ent/on-put! hook]
      (let [a (future (graph/put-verified! *node* :entities
                                           (entity id-a :probe/parallel "a")))
            b (future (graph/put-verified! *node* :entities
                                           (entity id-b :probe/parallel "b")))]
        (is (true? (deref both-entered 5000 false))
            "different stripes reach their hooks concurrently")
        (deliver release true)
        (is (= :ok @a))
        (is (= :ok @b))))))

(defn- without-index [f]
  (let [prior @ent/!reads-enabled]
    (reset! ent/!reads-enabled false)
    (try (f) (finally (reset! ent/!reads-enabled prior)))))

(defn- without-annotation [response]
  (dissoc response :ent-index))

(deftest indexed-entities-query-matches-scan
  (let [cases [{:type :probe/parity :limit 2}
               {:type :probe/parity :limit 2 :after "parity-01"}
               {:type :probe/parity :limit 3 :include-total? false}
               {:type :probe/other :limit 10 :include-total? true}]]
    (doseq [opts cases]
      (let [scan (without-index #(graph/entities-query *node* opts))
            indexed-response (graph/entities-query *node* opts)]
        (is (= scan (without-annotation indexed-response)) (pr-str opts))
        (is (= (ent/checkpoint) (get-in indexed-response [:ent-index :checkpoint])))))
    (testing "unordered requests may use the index's free stable ordering"
      (let [response (graph/entities-query
                      *node* {:type :probe/parity :limit 3 :ordered? false})]
        (is (= ["parity-00" "parity-01" "parity-02"]
               (mapv :entity/id (:entities response))))
        (is (= "parity-02" (:next-cursor response)))
        (is (= 5 (:count response)))))
    (testing "gate false executes the XTDB query function"
      (let [calls (atom 0)
            row (entity "captured" :probe/captured "body")
            query-fn (fn [_ _] (swap! calls inc) [row])]
        (without-index
         #(graph/entities-query ::capturing-node
                                {:type :probe/captured :limit 1}
                                query-fn))
        (is (pos? @calls))))))

(deftest indexed-entities-latest-matches-scan
  (doseq [opts [{:type :probe/parity :limit 3}
                {:type :pattern/library :limit 10}]]
    (let [scan (without-index #(graph/entities-latest *node* opts))
          started (System/nanoTime)
          indexed-response (graph/entities-latest *node* opts)
          elapsed-ms (/ (- (System/nanoTime) started) 1000000.0)]
      (println "  indexed entities-latest" (:type opts) "elapsed-ms" elapsed-ms)
      (is (= scan (without-annotation indexed-response)) (pr-str opts))
      (is (= (ent/checkpoint) (get-in indexed-response [:ent-index :checkpoint])))))
  (testing "indexed bodies avoid XTDB; pattern/library keeps only its relation read"
    (let [ordinary-calls (atom 0)
          relation-calls (atom 0)]
      (graph/entities-latest *node* {:type :probe/parity :limit 2}
                             (fn [& _] (swap! ordinary-calls inc) []))
      (graph/entities-latest *node* {:type :pattern/library :limit 2}
                             (fn [& _] (swap! relation-calls inc) []))
      (is (zero? @ordinary-calls))
      (is (= 1 @relation-calls)))))

(defn- seed! []
  (doseq [i (range 5)]
    (graph/put-verified! *node* :entities
                         (entity (format "parity-%02d" i) :probe/parity i)))
  (graph/put-verified! *node* :entities (entity "other-00" :probe/other 0))
  (doseq [i (range 3)]
    (graph/put-verified!
     *node* :entities
     (assoc (entity (format "library-%02d" i) :pattern/library i)
            :entity/name (if (< i 2) "same-name" "other-name"))))
  ;; fill! is explicit and leaves the gate closed; catch-up opens it.
  (ent/fill! *node* :chunk 10 :pause-ms 0)
  (ent/catch-up! *node* :force true)
  (is (ent/reads-usable?)))

(defn -main [& args]
  (let [dir (temp-dir)]
    (with-open [node (xtn/start-node)]
      (text/init! {:path (str dir "/fts5-evidence.db")})
      (ent/init!)
      (binding [*node* node
                *disable-write-locks?* (boolean (some #{"--disable-write-locks"} args))]
        (let [{:keys [fail error]} (run-tests 'test-ent-serving)]
          (shutdown-agents)
          (System/exit (if (zero? (+ fail error)) 0 1)))))))

(use-fixtures
 :once
 (fn [run]
   ;; Install all read fixtures and establish the gate before any test.
   ;; Same-id tests remain valid after the checkpoint because awaited hooks
   ;; keep the sidecar current.
   (seed!)
   (run)))
