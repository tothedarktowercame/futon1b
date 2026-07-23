#!/usr/bin/env bb
;; Per-type hyperedge backfill: census-driven, resumable, one type at a time.
(require '[babashka.http-client :as http] '[cheshire.core :as json] '[clojure.edn :as edn]
         '[babashka.process :as proc])
(def src "http://127.0.0.1:7071")
(def dst "http://127.0.0.1:7073")
(def types ["agency/contracts" "agent/id" "agent/sense-deliberate-act"
   "aif/predictive-coding-belief-update"
   "arxana/essay" "arxana/essay-section" "arxana/flight-organ-annotation"
   "builder/wm-gate-runner" "builder/wm-guardrails-core" "builder/wm-hole-counter"
   "builder/wm-input-sources-hygiene"
   "capability/ascent" "capability/produces"
   "cascade/cluster-member" "cascade/hole-target" "cascade/mission-pattern"
   "clock/clocked-on"
   "code/file-churn" "code/indentation" "code/ns-contains" "code/requires"
   "code/v05/author" "code/v05/authored" "code/v05/block-trailer"
   "code/v05/calls" "code/v05/commit" "code/v05/contains" "code/v05/coverage"
   "code/v05/edits" "code/v05/excursion-doc" "code/v05/mined-move"
   "code/v05/mission-cross-ref" "code/v05/mission-doc" "code/v05/namespace"
   "code/v05/pattern-slot" "code/v05/precedes" "code/v05/related-mission"
   "code/v05/replay-cursor" "code/v05/satisficing-signature" "code/v05/sorry"
   "code/v05/term-defines" "code/v05/test" "code/v05/var"
   "code/v05/vocabulary-use"
   "coord/type"
   "data/mission-scope-trees" "data/outings" "data/repl-traces"
   "edge/renamed-to" "edge/witness-stale"
   "essay/id"
   "mission-scope/nesting" "mission-scope/psr" "mission-scope/pur"
   "mission-scope/pxr"])
;; code/v05/watcher-event deliberately EXCLUDED (boundary doc: spam not migrated)
(defn census [base t]
  (try (-> (http/get (str base "/api/alpha/census?type=" (java.net.URLEncoder/encode t "UTF-8"))
                     {:headers {"accept" "application/edn"} :timeout 120000})
           :body edn/read-string :count)
       (catch Exception e (println "  census error" t (ex-message e)) nil)))
(defn post-doc! [hx]
  (let [payload {:hx/id (get hx "hx/id") :hx/type (get hx "hx/type")
                 :hx/endpoints (get hx "hx/endpoints") :hx/labels (get hx "hx/labels")
                 :hx/props (get hx "hx/props") :hx/ends (get hx "hx/ends")}]
    (loop [attempt 1]
      (let [res (try
                  (let [resp (http/post (str dst "/api/alpha/hyperedge")
                                        {:headers {"content-type" "application/edn" "x-penholder" "api"}
                                         :body (pr-str payload) :timeout 60000 :throw false})
                        r (try (edn/read-string (:body resp)) (catch Exception _ nil))]
                    (cond (:no-op? r) :no-op (:ok r) :ok :else :fail))
                  (catch Exception _ :conn-error))]
        (if (and (= res :conn-error) (< attempt 4))
          (do (Thread/sleep (* attempt 3000)) (recur (inc attempt)))
          res)))))
(doseq [t types]
  (let [n-src (census src t)]
    (cond
      (or (nil? n-src) (zero? n-src)) (println t ": src 0/absent — skip")
      :else
      (try
      (let [n-dst (or (census dst t) 0)]
        (if (>= n-dst n-src)
          (println t ": dst" n-dst ">= src" n-src "— already done, skip")
          (let [enc (java.net.URLEncoder/encode t "UTF-8")
                f (str "/tmp/claude-1000/hx-" (clojure.string/replace t #"/" "_") ".json")
                _ (println t ": fetching" n-src "docs...")
                curl (proc/shell {:out f :err :string :continue true}
                                 "curl" "-s" "--max-time" "2400"
                                 "-H" "accept: application/json"
                                 (str src "/api/alpha/hyperedges?type=" enc "&limit=" n-src))]
            (if (not= 0 (:exit curl))
              (println t ": FETCH FAILED exit" (:exit curl))
              (let [hxs (try (-> (slurp f) (json/parse-string false) (get "hyperedges"))
                             (catch Exception e (println t ": PARSE FAILED" (ex-message e)) nil))]
                (when hxs
                  (let [freq (frequencies (map post-doc! hxs))]
                    (println t ": ingested" (pr-str freq) "| dst census now" (census dst t))))
                (try (clojure.java.io/delete-file f) (catch Exception _ nil)))))))
      (catch Exception e (println t ": TYPE-LEVEL ERROR" (ex-message e))))))))
(println "=== PER-TYPE BACKFILL PASS COMPLETE ===")
