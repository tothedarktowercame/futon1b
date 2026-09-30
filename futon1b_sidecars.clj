(ns futon1b-sidecars
  "The index of sidecars: every derived index this server keeps beside XTDB,
  in one declaration (Joe, 2026-09-30: \"if we build sidecar indexes for
  everything we want to query, we better build an index of sidecars so we
  don't lose track of which ones exist\").

  Why this exists. XTDB 2.1.0 reads a whole table for any predicate that
  matches rows (TN-entities-route-cost-2026-09-26.md), so each slow read
  shape has been answered with its own sidecar, each with its own write
  hook, catch-up loop and serving gate. When a gate is false, reads fall
  back to scanning the store and the ONLY symptom is slowness — that is how
  the evidence index sat unused after a reload, and how 20 minutes of
  clock-decision refusals went unattributed. CANDIDATE-INDEX-CONTRACT.md §C4
  already required each index to be declared; only the text sidecar did,
  and only for itself.

  The rule. A derived index exists only if it is declared here:
  - every SQLite table a futon1b namespace creates is named by exactly one
    entry's :tables (test_sidecars.clj enforces this both ways);
  - an entry says what it is derived from, how it is maintained, what gate
    decides whether it serves, which routes it serves, and what a read does
    when the gate is false;
  - :stands-in-for says what upstream facility would make the entry
    unnecessary, so each one can be removed when that ships (xtdb/xtdb#3663,
    user-declared secondary indices — open as of 2026-09-30, not in 2.2.0).

  `health-snapshot` is what /health shows: per entry, serving or not and
  why. It must stay cheap — no XTDB query and no table count — because
  /health takes no permit."
  (:require [futon1b-text :as text]
            [futon1b-hxindex :as hx]
            [futon1b-scopeindex :as scope]
            [futon1b-graph :as graph]))

(def registry
  [{:sidecar/id :evidence-text
    :ns 'futon1b-text
    :kind :sqlite-candidate-index
    :derived-from {:xtdb-table :evidence}
    :tables {"ev_fts" :fts5-body-and-attrs
             "ev_fts_map" :ev_fts-rowid-by-id
             "ev_attr" :attribute-btree
             "ev_tags" :tag-junction
             "ev_doc" :body-cache
             "fts_meta" :checkpoint}
    :declares 'futon1b-text/projection
    :maintained-by {:write-hook 'futon1b-text/on-append!
                    :repair ['futon1b-text/catch-up! 'futon1b-text/reconcile!]
                    :cadence "on append; catch-up every FUTON1B_FTS_CATCHUP_MS (5 min)"}
    :gate {:fn 'futon1b-text/complete?
           :means "reconcile! has run since boot/reload, no failed ids, the append writer has caught up"}
    :serves ["GET /api/alpha/evidence (list windows: index-window, tag-window)"
             "GET /api/alpha/evidence/text-search"
             "GET /api/alpha/evidence/{id} body reads (ev_doc)"]
    :fallback "scan-window: whole-table XTDB read, ~2 s per call at rest"
    :stands-in-for "xtdb#3663 for the attribute columns; FTS has no upstream equivalent"}

   {:sidecar/id :hyperedges
    :ns 'futon1b-hxindex
    :kind :sqlite-candidate-index
    :derived-from {:xtdb-table :hyperedges}
    :tables {"hx_edge" :endpoint-rows
             "hx_node" :one-row-per-hyperedge
             "hx_meta" :checkpoint}
    :declares {:hx/type ["hx_edge.type" "hx_node.type"]
               :hx/endpoints ["hx_edge.endpoint" "hx_edge.pos"]}
    :maintained-by {:write-hook ['futon1b-hxindex/on-put! 'futon1b-hxindex/on-delete!]
                    :repair ['futon1b-hxindex/catch-up! 'futon1b-hxindex/rebuild!]
                    :cadence "on put/delete; catch-up every FUTON1B_HX_CATCHUP_MS (15 min)"}
    :gate {:fn 'futon1b-hxindex/reads-usable?
           :means "reads enabled, a checkpoint exists, no hook has failed since the last catch-up"}
    :serves ["GET /api/alpha/hyperedges?type=… (type-only, type+end, end prefix)"
             "GET /api/alpha/census?type=…"]
    :fallback "XTDB type scan; census refuses with a typed 503"
    :stands-in-for "xtdb#3663 (declared index on hx$type, and per-element on hx$endpoints)"}

   {:sidecar/id :scopes
    :ns 'futon1b-scopeindex
    :kind :sqlite-file-index
    :derived-from {:files "a superpod run's artifacts/{expo,marks,graphs}/"}
    :tables {"scope" :scope-spans
             "scope_meta" :per-run-watermark
             "mark" :s1-marks
             "gnode" :proof-graph-nodes}
    :declares 'futon1b-scopeindex/ddl
    :maintained-by {:repair ['futon1b-scopeindex/index-run!]
                    :cadence "on request, per run; re-indexes when the run's watermark moves"}
    :gate {:fn nil
           :means "none: the files are the source of truth and nothing is in XTDB, so there is no fallback"}
    :serves ["GET /api/alpha/scopes"]
    :fallback "none (404/empty when a run was never indexed)"
    :stands-in-for "nothing upstream; scopes are deliberately not ingested (design §2 option b)"}

   {:sidecar/id :memory-projection
    :ns 'futon1b-graph
    :kind :in-memory-projection
    :derived-from {:xtdb-table :entities :filter "memory components"}
    :tables {}
    :maintained-by {:write-hook ['futon1b-graph/with-memory-projection-mutation]
                    :repair ['futon1b-graph/initialize-memory-projection!
                             'futon1b-graph/refresh-memory-projection-component!]
                    :cadence "built on first read after boot/generation change; point refresh on write"}
    :gate {:fn 'futon1b-graph/memory-projection-snapshot
           :means "built, and built from the current projection generation"}
    :serves ["GET /api/alpha/memory/projection" "GET /api/alpha/memory/search"
             "POST /api/alpha/memory/assert (read side)"]
    :fallback "a full rebuild under the node lock (the 2026-09-28 capacity outage)"
    :stands-in-for "nothing upstream; this is an application projection, not an index"}

   {:sidecar/id :alias-warrants
    :ns 'futon1b-graph
    :kind :in-memory-cache
    :derived-from {:xtdb-table :entities :filter "alias scans by name/id"}
    :tables {}
    :maintained-by {:write-hook ['futon1b-graph/with-entity-mutation]
                    :cadence "issued on read; every :entities write retires all of them"}
    :gate {:fn 'futon1b-graph/alias-warrant-snapshot
           :means "a warrant serves only while its basis (boot, entity generation) is current"}
    :serves ["GET /api/alpha/entity/{name-or-id}"]
    :fallback "the alias scan"
    :stands-in-for "xtdb#3663 (declared index on the alias columns)"}])

(defn- status
  "Cheap serving status for one entry. Never throws: a status that cannot be
  read is reported as such, not hidden."
  [{:sidecar/keys [id]}]
  (try
    (case id
      :evidence-text
      (let [s (text/stats nil)
            complete (text/complete? 0)]
        {:serving? complete
         :why (cond
                (not (:ready s)) :no-datasource
                (nil? (get-in s [:appends :reconciled-at])) :not-reconciled-since-boot
                (pos? (get-in s [:appends :failed] 0)) :failed-ids-held
                (not complete) :append-writer-behind
                :else :complete)
         :appends (:appends s)
         :periodic? (:periodic? s)})

      :hyperedges
      (let [usable (hx/reads-usable?)
            cp (hx/checkpoint)]
        {:serving? usable
         :why (cond
                (not @hx/!reads-enabled) :disabled-by-FUTON1B_HX_READS
                (nil? cp) :no-datasource
                (nil? (:ts cp)) :never-filled
                (not usable) :hook-failed-since-last-catch-up
                :else :complete)
         :checkpoint cp
         :hook-failures (:hook-failures @hx/!stats)
         :last-catch-up (:last-catch-up @hx/!stats)})

      :scopes
      {:serving? (some? @text/!ds)
       :why (if @text/!ds :file-derived :no-datasource)
       :last-index-run (select-keys (:last-index-run @scope/!stats)
                                    [:run :rows :elapsed-ms :skipped])}

      :memory-projection
      (let [s (graph/memory-projection-snapshot)]
        (assoc s :serving? (:current? s)
                 :why (cond (not (:built? s)) :not-built
                            (not (:current? s)) :stale-generation-rebuilds-on-next-read
                            :else :current)))

      :alias-warrants
      ;; Serving means a warrant is current right now. Seen live 2026-09-30:
      ;; 2,598 issued, 0 honoured — every :entities write retires them all,
      ;; and entity writes outpace alias reads.
      (let [s (graph/alias-warrant-snapshot)]
        (assoc s
               :serving? (pos? (:entries s))
               :why (cond (pos? (:entries s)) :current-warrants-held
                          (zero? (:honoured s)) :never-honoured-since-boot
                          :else :all-retired-by-entity-writes))))
    (catch Throwable t
      {:serving? :unknown
       :why :status-threw
       :error (str (.getSimpleName (class t)) ": " (.getMessage t))})))

(defn health-snapshot
  "One line per declared sidecar for /health: {:sidecar/id {:serving? … :why …}}.
  A sidecar that is declared but not serving is exactly what this is for."
  []
  (into (sorted-map)
        (for [e registry]
          [(:sidecar/id e) (status e)])))

(defn declarations
  "The registry as data (symbols, not fns), with current status merged in."
  []
  (let [st (health-snapshot)]
    (mapv #(assoc % :status (get st (:sidecar/id %))) registry)))
