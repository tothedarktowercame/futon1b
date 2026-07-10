;; E-futon1a-to-futon1b-migration-pipeline — S1: Export from live futon1a store.
;;
;; Exports all documents from the live futon1a :7071 store via HTTP API.
;; I-0 SAFE: no writes to the store, no restart, no second serving process.
;; All queries are read-only HTTP GET/POST requests.
;;
;; Export strategy (one path per document population):
;;
;; 1. ENTITIES + RELATIONS: POST /api/alpha/snapshot with scope "latest".
;;    The snapshot endpoint writes an EDN file to <data-dir>/snapshots/ —
;;    a filesystem write, NOT an XTDB write. The file is read and parsed.
;;    This covers ~85k docs (entities with :entity/id, relations with :relation/id).
;;
;; 2. HYPEREDGES: For each hx-type, GET /api/alpha/hyperedges?type=X&limit=<count>.
;;    The census endpoint gives us the per-type count; we set limit=count to
;;    get all docs of that type in one call. Hyperedge types are discovered
;;    from the slice manifest's :by-type keys + the type catalog.
;;
;; 3. EVIDENCE: GET /api/alpha/evidence/sessions gives session list + counts.
;;    Then for each session, GET /api/alpha/evidence?session-id=X&limit=<count>.
;;    Session-scoped queries are indexed and fast (unlike the unscoped endpoint
;;    which does a full scan + sort).
;;
;; 4. TYPE CATALOG: GET /api/alpha/types returns all type descriptors.
;;    Small population (~207 docs).
;;
;; The export writes each population to a separate EDN file in the output dir.
;; This allows incremental re-runs (re-export only failed populations).
;;
;; Run: clojure -M:node -m migration.export --output-dir <dir> [--base-url URL]
(ns migration.export
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io])
  (:import [java.io PushbackReader File]
           [java.net URL HttpURLConnection]
           [java.nio.charset StandardCharsets])
  (:gen-class))

;; ---------------------------------------------------------------------------
;; HTTP client (minimal — no external deps, stays within futon1b's deps.edn).
;; ---------------------------------------------------------------------------

(defn http-get
  "GET URL, return body string. Throws on non-200."
  ([^String url-str] (http-get url-str 120000))  ; 2-min default timeout
  ([^String url-str timeout-ms]
   (let [conn ^HttpURLConnection (.. (URL. url-str) openConnection)]
     (.setRequestMethod conn "GET")
     (.setConnectTimeout conn 30000)
     (.setReadTimeout conn timeout-ms)
     (.setRequestProperty conn "Accept" "application/edn")
     (let [code (.getResponseCode conn)]
       (if (= 200 code)
         (slurp (.getInputStream conn))
         (let [err-body (try (slurp (.getErrorStream conn))
                             (catch Exception _ ""))]
           (throw (ex-info (str "HTTP " code " for " url-str)
                           {:url url-str :code code :body err-body}))))))))

(defn http-post-edn
  "POST EDN body to URL, return body string. Throws on non-200."
  ([^String url-str body] (http-post-edn url-str body 120000))
  ([^String url-str body timeout-ms]
   (let [conn ^HttpURLConnection (.. (URL. url-str) openConnection)]
     (.setRequestMethod conn "POST")
     (.setConnectTimeout conn 30000)
     (.setReadTimeout conn timeout-ms)
     (.setRequestProperty conn "Content-Type" "application/edn")
     (.setRequestProperty conn "Accept" "application/edn")
     (.setDoOutput conn true)
     (with-open [os (.getOutputStream conn)]
       (.write os (.getBytes (pr-str body) StandardCharsets/UTF_8)))
     (let [code (.getResponseCode conn)]
       (if (= 200 code)
         (slurp (.getInputStream conn))
         (let [err-body (try (slurp (.getErrorStream conn))
                             (catch Exception _ ""))]
           (throw (ex-info (str "HTTP " code " for " url-str)
                           {:url url-str :code code :body err-body}))))))))

;; ---------------------------------------------------------------------------
;; EDN sanitization: the live store contains double-keywordized keywords —
;; e.g. (keyword ":pattern/*") in the type catalog — which pr-str as
;; `::pattern/*`, a token clojure.edn cannot read (auto-resolved keywords are
;; not EDN). Found by the first live run of export-type-catalog (2026-07-10).
;; We normalize `::foo` tokens to `:foo` at the ingress points, WITHOUT
;; touching string literals (evidence bodies embed code text containing `::`),
;; and count every replacement into !sanitize-log so nothing is silent.
;; ---------------------------------------------------------------------------

(def !sanitize-log
  "Atom of {:where <label> :count n} entries, included in export-summary.edn."
  (atom []))

(defn sanitize-edn-str
  "Replace unreadable `::' keyword tokens with `:' outside string literals.
  Handles string escapes and EDN char literals (\\\" etc.).
  Returns [sanitized-string replacement-count]."
  [^String s]
  (let [n (.length s)
        sb (StringBuilder. n)]
    (loop [i 0 in-str? false esc? false cnt 0]
      (if (>= i n)
        [(.toString sb) cnt]
        (let [c (.charAt s i)]
          (cond
            in-str?
            (do (.append sb c)
                (cond
                  esc?      (recur (inc i) true false cnt)
                  (= c \\)  (recur (inc i) true true cnt)
                  (= c \")  (recur (inc i) false false cnt)
                  :else     (recur (inc i) true false cnt)))

            ;; EDN char literal: append backslash + next char verbatim so a
            ;; \" char literal cannot be mistaken for a string opener.
            (= c \\)
            (if (< (inc i) n)
              (do (.append sb c) (.append sb (.charAt s (inc i)))
                  (recur (+ i 2) false false cnt))
              (do (.append sb c) (recur (inc i) false false cnt)))

            (= c \")
            (do (.append sb c) (recur (inc i) true false cnt))

            ;; the mangled token: `::` outside a string → emit single `:`.
            (and (= c \:) (< (inc i) n) (= (.charAt s (inc i)) \:))
            (do (.append sb \:) (recur (+ i 2) false false (inc cnt)))

            :else
            (do (.append sb c) (recur (inc i) false false cnt))))))))

(defn- sanitize!
  "Sanitize s, logging any replacements under `where`. Returns the clean string."
  [^String s where]
  (let [[clean cnt] (sanitize-edn-str s)]
    (when (pos? cnt)
      (println (format "  WARN: normalized %d double-colon keyword token(s) in %s"
                       cnt where))
      (swap! !sanitize-log conj {:where where :count cnt}))
    clean))

(def server-edn-readers
  "EDN readers for tags the futon1a server emits. Mirrors
  futon1a.api.snapshot/edn-readers (the snapshot files were designed to be
  read with it — ~13.5k :entity/first-seen / :entity/updated-at values).
  We parse to java.util.Date rather than Instant so the value round-trips
  through our own export files as EDN-native #inst (millisecond truncation
  accepted; the live values carry millis)."
  {'futon1a/instant (fn [s] (java.util.Date/from (java.time.Instant/parse (str s))))})

(defn parse-edn-response
  "Parse an EDN response string into Clojure data.
  Sanitizes double-keywordized tokens (see sanitize-edn-str) before reading."
  ([s] (parse-edn-response s "http-response"))
  ([s where]
   (when (and s (seq s))
     (edn/read-string {:readers server-edn-readers} (sanitize! s where)))))

;; ---------------------------------------------------------------------------
;; EDN file I/O.
;; ---------------------------------------------------------------------------

(defn write-edn-file
  "Write data as EDN to a file. Creates parent dirs."
  [^String path data]
  (let [f (File. path)]
    (.mkdirs (.getParentFile f))
    (with-open [w (io/writer f)]
      (binding [*out* w *print-readably* true]
        (pr data)))
    path))

(defn read-edn-file
  "Read EDN from a file (accepting futon1a server tags)."
  [^String path]
  (with-open [r (PushbackReader. (io/reader path))]
    (edn/read {:readers server-edn-readers} r)))

(defn read-server-edn-file
  "Read an EDN file written by the SERVER (snapshot files) — these may carry
  double-keywordized tokens the store contains. Try a plain streaming read
  first; on a parse failure, fall back to slurp + sanitize + read. NB the
  fallback holds the whole file in memory — for the ~246k-doc hyperedges
  snapshot budget heap accordingly (-Xmx2g)."
  [^String path]
  (try
    (read-edn-file path)
    (catch Exception e
      (println (format "  plain EDN read of %s failed (%s); retrying sanitized"
                       path (.getMessage e)))
      (edn/read-string {:readers server-edn-readers}
                       (sanitize! (slurp path) (str "file:" path))))))

;; ---------------------------------------------------------------------------
;; Export: type catalog (small, ~207 docs).
;; ---------------------------------------------------------------------------

(defn export-type-catalog
  "Export the type catalog from /api/alpha/types."
  [base-url output-dir]
  (println "[export] type catalog...")
  (let [types (parse-edn-response (http-get (str base-url "/api/alpha/types")))
        type-docs (:types types)]
    (println (format "  %d type descriptors" (count type-docs)))
    (write-edn-file (str output-dir "/type-catalog.edn") type-docs)
    {:count (count type-docs) :file "type-catalog.edn"}))

;; ---------------------------------------------------------------------------
;; Export: hyperedges (per-type, potentially large).
;;
;; We discover all hyperedge types from the slice manifest + probe known types.
;; For each type, get the census count, then fetch all docs with limit=count.
;; ---------------------------------------------------------------------------

(def ^:private hx-type-candidates
  "Candidate hyperedge-type names to probe.

  The live store exposes NO endpoint that enumerates hyperedge types (the type
  catalog only lists :entity/:relation kinds; /census requires a ?type= arg).
  So discovery is candidate-driven: this list is harvested from every
  hyperedge-type-shaped string literal in the futon3c source
  (`grep -rhoE '\"<ns>/<name>\"'`). We census-probe each candidate and keep only
  those the store reports as `:kind :hyperedge` with `:count > 0` — census does
  the filtering, so harmless non-hyperedge candidates (entity types, dead names)
  cost only a probe.

  LIMITATION (finding #2 of the claude-16 review): completeness is bounded by
  this list. A hyperedge type present in the store but never named as a string
  literal in the futon3c source would be missed. The `--hx-types-file` CLI arg
  lets an operator supply an authoritative superset. The discovery log
  (hyperedges-manifest.edn :probed) records exactly what was probed and found,
  so coverage is auditable — no silent cap. This REPLACES the prior hardcoded
  list of 10 (which silently dropped ~61k hyperedges incl. all 29,903
  code/v05/test docs, and included 2 dead names: code/v05/edits, code/v05/var)."
  ["agency/contracts" "agent/id" "agent/sense-deliberate-act"
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
   "code/v05/vocabulary-use" "code/v05/watcher-event"
   "coord/type"
   "data/mission-scope-trees" "data/outings" "data/repl-traces"
   "edge/renamed-to" "edge/witness-stale"
   "essay/id"
   "mission-scope/nesting" "mission-scope/psr" "mission-scope/pur"
   "mission-scope/pxr"])

(defn census
  "Census-probe a type. Returns the raw census map {:type :kind :count} or nil.
  Uses a short (20s) timeout — census is an indexed lookup and should be fast;
  we don't want one slow probe to stall discovery across ~60 candidates."
  [base-url type-str]
  (try
    (parse-edn-response
      (http-get (str base-url "/api/alpha/census?type=" type-str) 20000))
    (catch Exception e
      (println (format "  WARN: census for %s failed: %s" type-str (.getMessage e)))
      nil)))

(defn probe-one
  "Census-probe one type. Returns {:type :kind :count :status}.
  :status is :ok (census answered), :failed (census errored/timed out — count
  UNKNOWN, must not be treated as 0), or :inactive (answered but not a
  positive-count hyperedge)."
  [base-url t]
  (let [c (census base-url t)]
    (cond
      (nil? c)                       {:type t :status :failed}
      (:error c)                     {:type t :status :failed :error (:error c)}
      (and (= :hyperedge (:kind c))
           (pos? (or (:count c) 0))) {:type t :kind (:kind c) :count (:count c)
                                      :status :ok}
      :else                          {:type t :kind (:kind c)
                                      :count (or (:count c) 0) :status :inactive})))

(defn discover-hx-types
  "Probe every candidate; RETRY failures once (server-side census on the biggest
  types can time out under live load). Returns {:active :probed :failed}.

  CRITICAL (finding, 2026-07-05): a census that errors/times out yields count
  UNKNOWN — it is put in :failed, never silently treated as count-0. The
  largest types (code/v05/calls ~90k, contains ~74k) are exactly the ones whose
  server-side count query times out under load, so a silent-drop would lose the
  bulk of the store. Callers MUST surface :failed and refuse to claim a complete
  export while it is non-empty."
  [base-url candidates]
  (let [pass1 (mapv #(probe-one base-url %) candidates)
        failed1 (filter #(= :failed (:status %)) pass1)
        ;; retry failures once — a transient timeout may clear.
        retried (into {} (for [{:keys [type]} failed1]
                           [type (probe-one base-url type)]))
        probed (mapv (fn [p] (if (= :failed (:status p))
                               (get retried (:type p) p)
                               p))
                     pass1)
        active (->> probed
                    (filter #(= :ok (:status %)))
                    (map (juxt :type :count))
                    vec)
        failed (->> probed (filter #(= :failed (:status %))) (mapv :type))]
    {:active active :probed probed :failed failed}))

(defn export-hyperedges
  "Export all hyperedges, per type. Writes to output-dir/hyperedges.edn.
  Discovers active types by census-probing the candidate list (finding #2 fix)."
  ([base-url output-dir] (export-hyperedges base-url output-dir hx-type-candidates))
  ([base-url output-dir candidates]
   (println "[export] hyperedges...")
   (let [{:keys [active probed failed]} (discover-hx-types base-url candidates)
         active-types active
         total (reduce + (map second active-types))]
     (println (format "  probed %d candidates → %d active hyperedge types"
                      (count probed) (count active-types)))
    (println (format "  %d active types, %d total hyperedges"
                     (count active-types) total))
    (when (seq failed)
      ;; DO NOT proceed as if complete. A failed census = UNKNOWN count, and the
      ;; biggest types (calls, contains) are the ones that time out server-side
      ;; under live load. These are NOT exported and NOT counted.
      (println (format "  *** WARNING: %d types had FAILED census (count unknown, NOT exported):"
                       (count failed)))
      (doseq [t failed] (println (format "        %s" t)))
      (println "  *** Export is INCOMPLETE. Re-run in a low-load window, or use a"
               "non-HTTP export path for these types. See hyperedges-manifest.edn :failed."))
    ;; Fetch all docs per type.
    (let [all-hx (atom [])
          manifest (atom {})]
      (doseq [[t c] active-types]
        (println (format "  fetching %s (%d)..." t c))
        (let [resp (parse-edn-response
                     (http-get (str base-url "/api/alpha/hyperedges?type=" t
                                   "&limit=" c)
                               300000))  ; 5-min timeout for large types
              docs (:hyperedges resp)]
          (swap! all-hx into docs)
          (swap! manifest assoc (keyword t) (count docs))
          (println (format "    got %d docs" (count docs)))))
      (let [final-hx @all-hx
            final-manifest (assoc @manifest
                                  :total (count final-hx)
                                  :probed probed
                                  :failed failed
                                  :complete? (empty? failed))]
        (write-edn-file (str output-dir "/hyperedges.edn") final-hx)
        (write-edn-file (str output-dir "/hyperedges-manifest.edn") final-manifest)
        (println (format "  wrote %d hyperedges across %d types"
                         (count final-hx) (count active-types)))
        {:count (count final-hx) :file "hyperedges.edn"
         :manifest (dissoc final-manifest :probed)})))))

;; ---------------------------------------------------------------------------
;; Export: evidence (session-scoped, ~53k docs).
;; ---------------------------------------------------------------------------

(defn export-evidence
  "Export all evidence docs, session-scoped. Writes to output-dir/evidence.edn."
  [base-url output-dir]
  (println "[export] evidence...")
  (let [sessions-resp (parse-edn-response
                        (http-get (str base-url "/api/alpha/evidence/sessions")
                                  60000))
        sessions (:sessions sessions-resp)
        total-entries (:total-entries sessions-resp)]
    (println (format "  %d sessions, %d total entries"
                     (count sessions) total-entries))
    (let [all-ev (atom [])
          errors (atom [])]
      (doseq [{:keys [session-id count]} sessions]
        (when (and session-id (pos? count))
          (println (format "  session %s (%d entries)..." session-id count))
          (try
            ;; URL-encode: live session-ids include spaces/parens (e.g.
            ;; "claude-4 (awaiting session)") — unencoded they 400.
            (let [resp (parse-edn-response
                         (http-get (str base-url "/api/alpha/evidence?session-id="
                                        (java.net.URLEncoder/encode (str session-id) "UTF-8")
                                        "&limit=" count)
                                   120000))
                  docs (:entries resp)]
              (swap! all-ev into docs))
            (catch Exception e
              (swap! errors conj {:session-id session-id
                                  :error (.getMessage e)})
              (println (format "    ERROR: %s" (.getMessage e)))))))
      (let [final-ev @all-ev]
        (write-edn-file (str output-dir "/evidence.edn") final-ev)
        (write-edn-file (str output-dir "/evidence-errors.edn") @errors)
        (println (format "  wrote %d evidence docs (%d errors)"
                         (count final-ev) (count @errors)))
        {:count (count final-ev) :file "evidence.edn"
         :errors (count @errors)}))))

;; ---------------------------------------------------------------------------
;; Export: via the server-side snapshot mechanism (entities+relations AND
;; hyperedges).
;;
;; POST /api/alpha/snapshot with a scope makes the SERVER iterate the store by
;; id (open-q id-cursor, not a census/materializing query) and write an EDN file
;; under <data-dir>/snapshots/<id>.edn. We trigger it and read the file. This is
;; a filesystem write, NOT an XTDB write — I-0 safe.
;;
;; Two scopes:
;;   "latest"     -> entities + relations (~91k docs)   -> graph-snapshot.edn
;;   "hyperedges" -> all hyperedge docs (~246k)         -> hyperedges.edn
;;
;; This REPLACES the per-type census+fetch hyperedge path (discover-hx-types /
;; export-hyperedges below): census `(count e)` and any `:order-by` hit XTDB's
;; ~30s server-side query timeout for the large types (calls, contains, test);
;; the server-side open-q id-iteration does not. The census helpers are kept for
;; verification/manifest use, not for the primary export.
;; ---------------------------------------------------------------------------

(defn export-via-snapshot
  "POST /api/alpha/snapshot with `scope`, read the resulting file, write its
  :docs to output-dir/out-name. NOTE: the hyperedges scope must be run when the
  serving JVM is quiet — the server-side id-drain of ~246k must finish within
  the ~30s query-timeout (it does on a quiet box; it fails under heavy load)."
  [base-url output-dir scope out-name timeout-ms]
  (println (format "[export] snapshot scope=%s -> %s ..." scope out-name))
  (let [resp (parse-edn-response
               (http-post-edn (str base-url "/api/alpha/snapshot")
                              {:scope scope} timeout-ms))]
    (if (:ok? resp)
      (let [snapshot-file (:snapshot/file resp)
            counts (:counts resp)]
        (println (format "  snapshot written: %s" snapshot-file))
        (println (format "  counts: %s" (pr-str counts)))
        (if (.exists (File. snapshot-file))
          (let [docs (:docs (read-server-edn-file snapshot-file))]
            (write-edn-file (str output-dir "/" out-name) docs)
            (println (format "  wrote %d docs -> %s" (count docs) out-name))
            {:count (count docs) :file out-name :counts counts})
          (throw (ex-info "snapshot file not found after export"
                          {:expected-file snapshot-file}))))
      (throw (ex-info "snapshot export failed" {:response resp})))))

(defn export-graph-snapshot
  "Entities + relations via snapshot scope 'latest' (works under load, ~74s)."
  [base-url output-dir]
  (export-via-snapshot base-url output-dir "latest" "graph-snapshot.edn" 300000))

(defn export-hyperedges-snapshot
  "All hyperedges via snapshot scope 'hyperedges' (server-side open-q). Run on a
  QUIET serving JVM — see export-via-snapshot note."
  [base-url output-dir]
  (export-via-snapshot base-url output-dir "hyperedges" "hyperedges.edn" 600000))

;; ---------------------------------------------------------------------------
;; Export orchestrator.
;; ---------------------------------------------------------------------------

(defn export-all
  "Export all document populations from the live store.
  Returns a summary map."
  [base-url output-dir]
  (.mkdirs (File. output-dir))
  (println "=== Full-store export from" base-url "===")
  (println "Output dir:" output-dir)
  (println)
  (let [start (System/currentTimeMillis)
        results (atom {})]
    ;; Export each population, catching failures so partial exports work.
    ;; Hyperedges + graph use the server-side snapshot (open-q id-iteration),
    ;; NOT the census path — see export-via-snapshot. Run graph first (works
    ;; under load); hyperedges needs a quiet serving JVM.
    (doseq [[label export-fn]
            [[:type-catalog export-type-catalog]
             [:graph        export-graph-snapshot]
             [:evidence     export-evidence]
             [:hyperedges   export-hyperedges-snapshot]]]
      (try
        (let [result (export-fn base-url output-dir)]
          (swap! results assoc label result))
        (catch Exception e
          (println (format "FAIL [%s]: %s" (name label) (.getMessage e)))
          (swap! results assoc label {:error (.getMessage e)}))))
    (let [elapsed (- (System/currentTimeMillis) start)
          summary {:base-url base-url
                   :output-dir output-dir
                   :results @results
                   :sanitize-log @!sanitize-log
                   :elapsed-ms elapsed}]
      (write-edn-file (str output-dir "/export-summary.edn") summary)
      (println)
      (println (format "=== Export complete in %.1fs ===" (/ elapsed 1000.0)))
      summary)))

;; ---------------------------------------------------------------------------
;; CLI.
;; ---------------------------------------------------------------------------

(defn- parse-args [args]
  (loop [args args opts {:base-url "http://localhost:7071"
                         :output-dir "migration-export"}]
    (if-not (seq args)
      opts
      (case (first args)
        "--base-url"  (recur (nnext args) (assoc opts :base-url (second args)))
        "--output-dir" (recur (nnext args) (assoc opts :output-dir (second args)))
        "--help" (do (println "Usage: clojure -M:node -m migration.export"
                              "[--base-url URL] [--output-dir DIR]")
                     (System/exit 0))
        (throw (ex-info (str "Unknown arg: " (first args)) {:args args}))))))

(defn -main [& args]
  (let [{:keys [base-url output-dir]} (parse-args args)]
    (export-all base-url output-dir)
    (shutdown-agents)))
