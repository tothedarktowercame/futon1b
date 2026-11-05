(ns test-derived
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.test :refer [deftest is run-tests testing]]
            [futon1b-derived :as derived]
            [futon1b-gates :as gates]
            [futon1b-server :as server]
            [futon1b-text :as text]
            [futon1b-xt :as fxt]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.node :as xtn])
  (:import [java.net URI]
           [java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers
            HttpResponse$BodyHandlers]
           [java.nio.file Files]))

(defn- payload [id at body]
  {:derived/id id
   :derived/family :artifact-activation
   :derived/writer "futon3c.agency.artifact-activation"
   :derived/writer-version "test-v1"
   :derived/source-act-ids ["act:one" "act:two"]
   :derived/subject {:ref/type :act :ref/id "act:one"}
   :derived/at at
   :derived/body body})

(defn- request [client method base path body]
  (let [builder (-> (HttpRequest/newBuilder (URI/create (str base path)))
                    (.header "Content-Type" "application/edn")
                    (.header "x-penholder" "api"))
        request (if (= method :post)
                  (.POST builder (HttpRequest$BodyPublishers/ofString (pr-str body)))
                  (.GET builder))
        response (.send client (.build request) (HttpResponse$BodyHandlers/ofString))]
    [(.statusCode response) (edn/read-string (.body response))]))

(defn- source-links [node source-id]
  (fxt/safe-q node
              (fxt/pq '[p-source]
                      '(-> (from :derived-source-links [*])
                           (where (= link/source-act-id p-source)))
                      source-id)))

(deftest derived-routes-are-versioned-atomic-and-outside-evidence-search
  (gates/seed-mission-contract!)
  (with-open [node (xtn/start-node)]
    (let [dir (.toFile (Files/createTempDirectory
                        "derived-table" (make-array java.nio.file.attribute.FileAttribute 0)))
          _ (text/init! {:path (str dir "/sidecar.db")})
          srv (server/start-server! {:node node :port 0 :bind-host "127.0.0.1"})
          base (str "http://127.0.0.1:" (.getPort (.getAddress srv)))
          client (HttpClient/newHttpClient)
          id "derived:artifact-activation:one"
          at1 "2026-11-05T20:00:00Z"
          at2 "2026-11-05T21:00:00Z"
          original (payload id at1 {:marker "derived-only-marker" :n 1})]
      (try
        (testing "single POST, exact point GET, and scalar source links"
          (let [[status written] (request client :post base "/api/alpha/derived" original)
                [get-status stored] (request client :get base
                                             (str "/api/alpha/derived/" id) nil)]
            (is (= 201 status) (pr-str written))
            (is (= 200 get-status))
            (is (= (:doc (derived/build-derived-doc original))
                   (assoc stored :xt/id (:derived/id stored))))
            (is (= #{id} (set (map :link/derived-id (source-links node "act:one")))))
            (is (every? string? (map :link/source-act-id
                                     (source-links node "act:one"))))))

        (testing "same-time replay and conflict; later valid-time is a version"
          (is (= [200 true]
                 (let [[status body] (request client :post base "/api/alpha/derived" original)]
                   [status (:no-op? body)])))
          (is (= 409 (first (request client :post base "/api/alpha/derived"
                                    (assoc original :derived/body {:different true})))))
          (is (= 201 (first (request client :post base "/api/alpha/derived"
                                    (assoc (payload id at2 {:marker "derived-only-marker" :n 2})
                                           :derived/source-act-ids ["act:two" "act:three"])))))
          (is (empty? (filter #(= id (:link/derived-id %))
                              (source-links node "act:one"))))
          (is (= #{id} (set (map :link/derived-id
                                  (source-links node "act:three")))))
          (is (= 2 (count (fxt/safe-q
                           node
                           (fxt/pq '[p-id]
                                   '(-> (from :derived {:bind [*]
                                                       :for-valid-time :all-time})
                                        (where (= xt/id p-id)))
                                   id))))))

        (testing "batch isolates one invalid item and keeps each row with its links"
          (let [good-a (payload "derived:artifact-activation:batch-a" at1 {:n "a"})
                bad (dissoc (payload "derived:artifact-activation:bad" at1 {:n "bad"})
                            :derived/family)
                good-b (payload "derived:artifact-activation:batch-b" at1 {:n "b"})
                [status result] (request client :post base "/api/alpha/derived/batch"
                                         {:derived [good-a bad good-b]})]
            (is (= 200 status))
            (is (= [true false true] (mapv :ok (:results result))))
            (is (some? (derived/fetch-by-id node (:derived/id good-a))))
            (is (nil? (derived/fetch-by-id node (:derived/id bad))))
            (is (some? (derived/fetch-by-id node (:derived/id good-b))))
            (is (= #{(:derived/id good-a) (:derived/id good-b)}
                   (set (map :link/derived-id
                             (filter #(str/starts-with? (:link/derived-id %) "derived:artifact-activation:batch-")
                                     (source-links node "act:one"))))))))

        (testing "source lookup, family census, cap, and unknown parameters"
          (let [[status lookup] (request client :get base
                                         "/api/alpha/derived?source-act-id=act%3Athree" nil)
                [_ census] (request client :get base
                                    "/api/alpha/derived/census?family=artifact-activation" nil)]
            (is (= 200 status))
            (is (= [id] (mapv :derived/id (:derived lookup))))
            (is (pos? (:total census))))
          (is (= 400 (first (request client :get base
                                     "/api/alpha/derived?family=artifact-activation&wat=1" nil))))
          (is (= 400 (first (request client :get base
                                     "/api/alpha/derived?family=artifact-activation&limit=1001" nil)))))

        (testing "derived writes never enter evidence FTS"
          (is (empty? (:results (text/search node {:q "derived-only-marker"
                                                   :scope :all :limit 10}))))
          (is (zero? (first (vals (jdbc/execute-one!
                                   @text/!ds ["SELECT count(*) AS n FROM ev_attr"])))))
          (is (zero? (first (vals (jdbc/execute-one!
                                   @text/!ds ["SELECT count(*) AS n FROM ev_fts"])))))
          (is (empty? (fxt/safe-q node '(from :evidence [xt/id])))))
        (finally (.stop srv 0))))))

(deftest required-fields-are-validated
  (let [base (payload "derived:artifact-activation:validation"
                      "2026-11-05T20:00:00Z" {:ok true})]
    (doseq [field [:derived/family :derived/writer :derived/source-act-ids :derived/at]]
      (is (= :invalid-derived-record
             (try (derived/build-derived-doc (dissoc base field)) nil
                  (catch clojure.lang.ExceptionInfo error
                    (get-in (ex-data error) [:error :reason]))))
          (str field)))))

(deftest family-list-pages-2500-current-rows-once
  (gates/seed-mission-contract!)
  (with-open [node (xtn/start-node)]
    (let [at "2026-11-05T22:00:00Z"
          docs (mapv (fn [n]
                       (:doc (derived/build-derived-doc
                              {:derived/id (format "derived:paging:%04d" n)
                               :derived/family :paging
                               :derived/writer "test-derived"
                               :derived/writer-version "v1"
                               :derived/source-act-ids [(str "act:" n)]
                               :derived/subject {:n n} :derived/at at
                               :derived/body {:n n}})))
                     (range 2500))]
      (doseq [chunk (partition-all 500 docs)]
        (xt/execute-tx node (mapv (fn [doc]
                                    [:put-docs {:into :derived
                                                :valid-from (java.time.Instant/parse at)} doc])
                                  chunk)))
      (let [srv (server/start-server! {:node node :port 0 :bind-host "127.0.0.1"})
            base (str "http://127.0.0.1:" (.getPort (.getAddress srv)))
            client (HttpClient/newHttpClient)]
        (try
          (loop [after nil seen [] page-sizes []]
            (let [path (str "/api/alpha/derived?family=paging&limit=1000"
                            (when after (str "&after=" after)))
                  [status body] (request client :get base path nil)
                  ids (mapv :derived/id (:derived body))
                  all (into seen ids)
                  sizes (conj page-sizes (count ids))]
              (is (= 200 status))
              (if-let [cursor (:next-cursor body)]
                (recur cursor all sizes)
                (do (is (= [1000 1000 500] sizes))
                    (is (= 2500 (count all)))
                    (is (= 2500 (count (distinct all))))))))
          (finally (.stop srv 0)))))))

(defn -main [& _]
  (let [result (run-tests 'test-derived)]
    (shutdown-agents)
    (System/exit (if (zero? (+ (:fail result) (:error result))) 0 1))))
