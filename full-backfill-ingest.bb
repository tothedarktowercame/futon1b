#!/usr/bin/env bb
;; Streaming ingest of exported hyperedge EDN files -> POST /api/alpha/hyperedge (live :7073).
(require '[babashka.http-client :as http] '[clojure.edn :as edn] '[clojure.java.io :as io])
(import '[java.io PushbackReader])
(def dst "http://127.0.0.1:7073")
(def dir (first *command-line-args*))
(def files (->> (file-seq (io/file dir))
                (filter #(and (.isFile %) (re-find #"hyperedge" (.getName %)) (re-find #"\.edn$" (.getName %))
                              (not (re-find #"manifest|summary" (.getName %)))))))
(println "hyperedge files:" (mapv #(.getName %) files))
(defn post-doc! [hx]
  (let [payload (select-keys hx [:hx/id :hx/type :hx/endpoints :hx/labels :hx/props :hx/ends])
        resp (http/post (str dst "/api/alpha/hyperedge")
                        {:headers {"content-type" "application/edn" "x-penholder" "api"}
                         :body (pr-str payload) :timeout 60000 :throw false})
        r (try (edn/read-string (:body resp)) (catch Exception _ nil))]
    (cond (:no-op? r) :no-op (:ok r) :ok :else :fail)))
(def totals (atom {}))
(doseq [f files]
  (println "== file:" (.getName f) (str (.length f) " bytes"))
  (with-open [rdr (PushbackReader. (io/reader f))]
    (let [first-form (edn/read {:eof ::eof :default (fn [_ v] v)} rdr)
          docs (cond
                 (= ::eof first-form) []
                 (map? first-form) ;; stream of maps: read the rest lazily below
                 (cons first-form
                       (take-while #(not= ::eof %)
                                   (repeatedly #(edn/read {:eof ::eof :default (fn [_ v] v)} rdr))))
                 (sequential? first-form) (mapcat #(if (map? %) [%] (:hyperedges % [])) [first-form])
                 :else [])]
      (loop [ds docs n 0 acc {}]
        (if-let [d (first ds)]
          (let [docs* (if (and (map? d) (:hyperedges d)) (:hyperedges d) [d])
                acc* (reduce (fn [a doc] (update a (post-doc! doc) (fnil inc 0))) acc docs*)
                n* (+ n (count docs*))]
            (when (zero? (mod n* 2000)) (println "  ..." n* (pr-str acc*)))
            (recur (rest ds) n* acc*))
          (do (println "  file done:" n (pr-str acc))
              (swap! totals (partial merge-with +) acc)))))))
(println "TOTALS:" (pr-str @totals))
