#!/usr/bin/env bb
;; Replay evidence documents from a dump file into a store.
;;
;;     scripts/replay-evidence.bb <file.edn> [port]
;;
;; The file is {:entries [...]} as returned by GET /api/alpha/evidence.
;;
;; Order matters twice over (README-fts.md §7):
;;  - ASCENDING by (at, id), because the store enforces referential integrity
;;    on :in-reply-to and refuses a reply whose parent is absent. The API pages
;;    NEWEST-FIRST, so the naive order is exactly wrong and fails every chain.
;;  - the checkpoint is NOT touched here. on-append! indexes each doc as it
;;    lands but deliberately leaves the (at, id) checkpoint alone. One reset +
;;    one catch-up happen AFTER the whole insertion phase, never during --
;;    otherwise a sweep drains mid-insert and writes a basis asserting coverage
;;    it never scanned (§3).
;;
;; Identity is carried, so this is idempotent: re-running upserts nothing and
;; is refused per-document as `duplicate evidence id`, which counts as success.

(require '[clojure.edn :as edn] '[babashka.http-client :as http])

(def SRC (or (first *command-line-args*)
             (throw (ex-info "usage: replay-evidence.bb <file.edn> [port]" {}))))
(def PORT (or (second *command-line-args*) "7073"))
(def DST (str "http://127.0.0.1:" PORT "/api/alpha/evidence"))

(defn ->payload [e]
  (cond-> {:penholder "api"
           :id (:evidence/id e) :at (:evidence/at e)
           :type (:evidence/type e) :claim-type (:evidence/claim-type e)
           :author (:evidence/author e) :body (:evidence/body e)
           :tags (vec (or (:evidence/tags e) []))}
    (:evidence/subject e)     (assoc :subject (:evidence/subject e))
    (:evidence/session-id e)  (assoc :session-id (:evidence/session-id e))
    (:evidence/pattern-id e)  (assoc :pattern-id (:evidence/pattern-id e))
    (:evidence/in-reply-to e) (assoc :in-reply-to (:evidence/in-reply-to e))
    (:evidence/fork-of e)     (assoc :fork-of (:evidence/fork-of e))))

(let [raw (edn/read-string {:default (fn [_t v] v)} (slurp SRC))
      evs (->> (or (:entries raw) (:results raw) raw)
               (filter map?) (filter :evidence/id)
               (sort-by (juxt #(str (:evidence/at %)) #(str (:evidence/id %)))))
      t0 (System/currentTimeMillis)]
  (println (format "  %d documents, %s -> %s"
                   (count evs) (str (:evidence/at (first evs))) (str (:evidence/at (last evs)))))
  (let [res (doall
             (map-indexed
              (fn [i e]
                (when (and (pos? i) (zero? (mod i 250)))
                  (println (format "    %d/%d" i (count evs))) (flush))
                (let [r (http/post DST {:body (pr-str (->payload e))
                                        :headers {"content-type" "application/edn"}
                                        :throw false :timeout 120000})]
                  {:id (:evidence/id e) :status (:status r) :body (str (:body r))}))
              evs))
        ;; the server answers 201 Created on insert -- a `= 200` check reports
        ;; every success as a failure (done once; it cost 35 phantom failures)
        ok   (filter #(#{200 201} (:status %)) res)
        dup  (filter #(re-find #"duplicate" (:body %)) res)
        bad  (remove #(or (#{200 201} (:status %)) (re-find #"duplicate" (:body %))) res)]
    (println (format "  accepted %d | already-present %d | FAILED %d | %.1fs"
                     (count ok) (count dup) (count bad)
                     (/ (- (System/currentTimeMillis) t0) 1000.0)))
    (doseq [b (take 12 bad)]
      (println (format "    FAIL %s -> %s %s" (:id b) (:status b)
                       (subs (:body b) 0 (min 150 (count (:body b)))))))
    (when (seq bad) (System/exit 1))))
