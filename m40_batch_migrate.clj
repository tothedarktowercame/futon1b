(ns futon1b-migrate-m40
  "MEM-migration step 1 (2026-11-12): batch evidence writer for the minus40
   (:7093) -> futon1b (:7073) copy, reusing futon1b-migrate-derived's
   write-batch! shape. Loaded into the running futon1b JVM via
   scripts/futon1b-eval.sh -f (a NEW file; no server/evidence/xt namespace
   is edited or reloaded).

   Per batch (the driver pages :7093's HTTP and writes each page as JSON):
   - prepare each doc with the HTTP path's pure parts (ev/build-evidence-doc
     + xf transform + the contract audit), NOT its per-doc existence queries;
   - ONE `_id IN` existence query per batch covers duplicate ids and the
     in-reply-to / fork-of parents (parents may be in the batch or already
     on :7073);
   - identical content skips; different content is refused and logged, never
     overwritten;
   - ONE xt/execute-tx per batch, each row [:put-docs {:into :evidence
     :valid-from <the doc's own :evidence/at>} doc] — valid time is the
     document's own time, which settles the valid-time question for evidence;
   - ONE readback IN query; then futon1b-text/on-append! per written doc
     (the text sidecar's catch-up runs forward by date, so back-dated docs
     reach the index only through this queue)."
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [futon1b-evidence :as ev]
            [futon1b-text :as ftext]
            [futon1b-xt :as fxt]
            [migration.transform :as xf]
            [xtdb.api :as xt])
  (:import [java.time Instant]))

(defonce ^:private !shape-log
  ;; the HTTP path's shape log lives in futon1b-evidence; a separate one here
  ;; keeps this migration's reshapes attributable.
  (xf/make-shape-log))

(defn- transform [doc]
  (xf/transform-doc doc !shape-log
                    {:log-stringify? true :log-except #{[:evidence/body]}}))

(defn- evidence-by-ids [node ids]
  (if (empty? ids) {}
      (let [sql (str "SELECT * FROM evidence WHERE _id IN ("
                     (str/join "," (repeat (count ids) "?")) ")")]
        (into {} (map (juxt :xt/id identity)
                      (fxt/safe-q node (into [sql] ids)))))))

(defn- same-doc? [a b]
  (= (dissoc a :xt/id) (dissoc b :xt/id)))

(defn write-batch!
  "Write one batch of evidence DOCS (maps with :evidence/* keys, as :7093's
   GET returns them). Returns {:written :skipped :conflicts :refused}."
  [node docs]
  (let [prepared
        (mapv (fn [payload]
                (let [{:keys [doc invalid invalid-status]} (ev/build-evidence-doc payload)]
                  (if invalid
                    {:refused {:id (or (:evidence/id payload) (:id payload))
                               :status (or invalid-status 400) :body invalid}}
                    (let [doc (transform doc)
                          assessment (try (#'ev/record-contract-audit! doc)
                                          (catch Throwable _ nil))]
                      (if (and (= :enforce (:mode assessment))
                               (seq (:violations assessment)))
                        {:refused {:id (:xt/id doc) :status 422
                                   :body {:error/code :evidence-contract-violation}}}
                        {:doc doc})))))
              docs)
        refused (vec (keep :refused prepared))
        built (vec (keep :doc prepared))
        ids (mapv :xt/id built)
        parents (vec (distinct (keep #(or (:evidence/in-reply-to %) (:evidence/fork-of %)) built)))
        known (evidence-by-ids node (distinct (concat ids parents)))
        in-batch (set ids)
        missing-parent (vec (keep (fn [d]
                                    (let [p (or (:evidence/in-reply-to d) (:evidence/fork-of d))]
                                      (when (and p (not (contains? known p))
                                                 (not (contains? in-batch p)))
                                        (:xt/id d))))
                                  built))
        conflicts (vec (keep (fn [d]
                               (when-let [stored (known (:xt/id d))]
                                 (when-not (same-doc? d stored)
                                   (:xt/id d))))
                             built))
        new (vec (remove (fn [d]
                           (or (contains? known (:xt/id d))
                               (some #(= (:xt/id d) %) missing-parent)))
                         built))]
    (doseq [id missing-parent]
      (println "[m40-migrate] parent missing, refused:" id))
    (doseq [id conflicts]
      (println "[m40-migrate] CONFLICT (different content, not overwritten):" id))
    (when (seq new)
      (xt/execute-tx
       node
       (mapv (fn [d]
               [:put-docs {:into :evidence
                           :valid-from (Instant/parse (str (:evidence/at d)))} d])
             new)))
    (let [after (evidence-by-ids node ids)
          mismatches (vec (keep (fn [d]
                                  (when-not (same-doc? d (get after (:xt/id d)))
                                    (:xt/id d)))
                                (remove #(contains? (set (concat missing-parent conflicts)) (:xt/id %))
                                        built)))]
      (doseq [id mismatches]
        (println "[m40-migrate] READBACK MISMATCH:" id))
      (doseq [d new]
        (ftext/on-append! (assoc d :xt/id (:xt/id d))))
      {:written (count new)
       :skipped (- (count built) (count new) (count missing-parent) (count conflicts))
       :conflicts conflicts
       :refused-parents missing-parent
       :refused refused
       :readback-mismatches mismatches})))

(defn write-batch-file!
  "Read a JSON array of evidence docs from PATH, write them as one batch.
   For the python driver over futon1b-eval.sh."
  [node path]
  (let [docs (json/parse-string (slurp (io/file path)) true)]
    (write-batch! node docs)))
