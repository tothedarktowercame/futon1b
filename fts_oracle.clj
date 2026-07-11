#!/usr/bin/env bb
;; D1 acceptance oracle (M-text-sidecar §Acceptance): on a defined subset,
;; an exhaustive local scan must agree exactly with the sidecar's answers.
;;
;; Subset: author=joe evidence since 2026-06-01 (the PZ1 corpus — ~1.4k
;; docs, small enough to scan exhaustively over HTTP).
;; Oracle tokenization mirrors FTS5 unicode61 for ASCII: lowercase, split
;; on non-alphanumeric. Query terms are chosen ASCII-only so the
;; approximation is exact. AND/OR composition matches the sidecar's
;; match-string semantics (default conjunction AND).
;;
;; Usage: bb fts_oracle.clj [base-url]
(require '[clojure.edn :as edn]
         '[clojure.string :as str]
         '[babashka.http-client :as http])

(def base (or (first *command-line-args*) "http://127.0.0.1:7074"))
(def subset-params "author=joe&since=2026-06-01")

(defn get-edn [url]
  (edn/read-string {:default (fn [_ v] v)}
                   (:body (http/get url {:timeout 120000}))))

(println "fetching subset…")
(def subset (:entries (get-edn (str base "/api/alpha/evidence?" subset-params))))
(println "subset docs:" (count subset))

(defn body-text [doc]
  (let [b (:evidence/body doc)]
    (if (string? b) b (pr-str b))))

(defn tokens [s]
  (set (remove str/blank? (str/split (str/lower-case s) #"[^a-z0-9]+"))))

(def doc-tokens
  (into {} (map (fn [d] [(:evidence/id d) (tokens (body-text d))])) subset))

;; queries: single terms across frequency bands + AND + OR compositions
(def queries
  [["substrate" :single]
   ["handoff" :single]
   ["peradam" :single]
   ["gflownets" :single]
   ["zabuton" :single]
   ["mission substrate" :and]          ; both terms required
   ["correction lexicon" :and]
   ["evidence backend futon1b" :and]
   ["peradam OR zabuton" :or]
   ["gflownets OR sunflower" :or]])

(defn oracle-hits [q kind]
  (let [terms (map str/lower-case (remove #{"OR" "AND"} (str/split q #"\s+")))]
    (set (keep (fn [[id toks]]
                 (when (case kind
                         :or (some toks terms)
                         (every? toks terms))
                   id))
               doc-tokens))))

(defn sidecar-hits [q]
  (let [enc (java.net.URLEncoder/encode (str q) "UTF-8")
        res (get-edn (str base "/api/alpha/evidence/text-search?q=" enc
                          "&" subset-params "&limit=2000"))]
    (set (map #(get-in % [:entry :evidence/id]) (:results res)))))

(def failures (atom 0))
(doseq [[q kind] queries]
  (let [o (oracle-hits q kind)
        s (sidecar-hits q)
        ok (= o s)]
    (when-not ok (swap! failures inc))
    (println (format "%-32s %-4s oracle=%-4d sidecar=%-4d %s"
                     (str "\"" q "\"") (name kind) (count o) (count s)
                     (if ok "AGREE" (str "DISAGREE  only-oracle=" (vec (take 3 (remove s o)))
                                         " only-sidecar=" (vec (take 3 (remove o s)))))))))
(println (if (zero? @failures)
           "ORACLE: ALL AGREE"
           (str "ORACLE: " @failures " DISAGREEMENTS")))
(System/exit (min @failures 1))
