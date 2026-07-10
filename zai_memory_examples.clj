;; zai_memory_examples.clj — XTQL-backed Zai memory seam, exercised against
;; the REAL migrated evidence corpus (56,301 docs in migration-store).
;;
;; Each example is a query the futon3c zai harness's memory_search tool would
;; issue, answered by futon1b/XTDB 2 instead of futon1a/XTDB 1, returning the
;; identical §12.3 envelope. E5 spot-checks parity: the same docs fetched from
;; the live futon1a HTTP API must agree field-by-field.
;;
;; Run: cd /home/joe/code/futon1b && \
;;      timeout 300 clojure -M:node -e '(load-file "zai_memory_examples.clj")'
(require '[zai-memory-1b :as zm]
         '[clojure.edn :as edn])

(defn- show [label {:keys [result]}]
  (println)
  (println (format "— %s" label))
  (println (format "  query: %s" (pr-str (:query result))))
  (println (format "  items: %d" (count (:items result))))
  (doseq [it (take 3 (:items result))]
    (println "   " (pr-str it))))

(with-open [node (zm/open-store "migration-store")]

  ;; E1: recall by tag (ANY-of; :invoke/:dev carry ~26.5k memories each).
  (show "E1 memory_search {:tags [:invoke-start] :limit 5}"
        (zm/memory-search node {:tags [:invoke-start] :limit 5}))

  ;; E2: a specific agent's memory trail.
  (show "E2 memory_search {:author \"claude-16\" :limit 5}"
        (zm/memory-search node {:author "claude-16" :limit 5}))

  ;; E3: coordination memories tagged :invoke.
  (show "E3 memory_search {:type :coordination :tags [:invoke] :limit 5}"
        (zm/memory-search node {:type :coordination :tags [:invoke] :limit 5}))

  ;; E4: recall since a date (string >= over ISO-8601 UTC = chronological).
  (show "E4 memory_search {:since \"2026-07-09\" :limit 5}"
        (zm/memory-search node {:since "2026-07-09" :limit 5}))

  ;; E5: KNOWN GAP, kept visible — sessionless evidence (e.g. mission-sync
  ;; records, author "mission-control/sync") carries no :evidence/session-id,
  ;; so the session-scoped export never saw it. 0 items here is the honest
  ;; reading until the quiet-window evidence id-drain leg runs (see
  ;; E-futon1a-to-futon1b-migration-pipeline.md, 2026-07-10 update).
  (show "E5 memory_search {:tags [:mission :sync]} — sessionless gap, expect 0"
        (zm/memory-search node {:tags [:mission :sync] :limit 5}))

  ;; E6: parity spot-check — same docs via live futon1a HTTP.
  (println)
  (println "— E6 parity spot-check vs live futon1a (:7071)")
  (let [items (:items (:result (zm/memory-search node {:tags [:invoke]
                                                       :limit 3})))
        fetch (fn [id]
                (let [url (str "http://localhost:7071/api/alpha/evidence/" id)
                      conn (.openConnection (java.net.URL. url))]
                  (.setRequestProperty conn "Accept" "application/edn")
                  (when (= 200 (.getResponseCode conn))
                    (edn/read-string (slurp (.getInputStream conn))))))]
    (doseq [{:keys [id at author type]} items]
      (let [src (fetch id)]
        (if-not src
          (println (format "  %s : SOURCE FETCH FAILED" id))
          (let [ok? (and (= at (:evidence/at src))
                         (= author (:evidence/author src))
                         (= type (:evidence/type src)))]
            (println (format "  %s : %s" id (if ok? "PARITY PASS" "PARITY FAIL")))
            (when-not ok?
              (println "    store:" (pr-str {:at at :author author :type type}))
              (println "    live: " (pr-str (select-keys src [:evidence/at
                                                              :evidence/author
                                                              :evidence/type])))))))))

  (println)
  (println "=== zai memory examples complete ==="))
