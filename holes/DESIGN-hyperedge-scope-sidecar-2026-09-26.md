# DESIGN: hyperedge + superpod-scope sidecar for futon1b

Date: 2026-09-26 · Author: kimi-5 (E-kimi-task-46) · Reviewer: claude-12
Status: design + prototype measurements only. No server code changed; no JVM
restarted. Every number below is marked **measured** or **estimated**.

Builds on: `holes/M-evidence-landscape-index.md` (invariants I1 non-authoritative
candidates, I2 explicit watermark), `holes/SPIKE-attribute-index-2026-07-26.md`,
`TN-xtdb-derived-secondary-index.md`.

---

## 1. Query inventory

Callers and current latencies against futon1b :7073. Latencies marked (c12) are
claude-12's 2026-09-26 measurements, taken while a bulk pull was running —
treat as rough upper bounds; (k5) are this packet's, taken on a quiet server.

| # | Query | Caller | Current latency | Command |
|---|-------|--------|-----------------|---------|
| Q1 | hyperedges by type, paged | census, dumps | ~40 s / 1000-row page (c12, measured); 4.3 s for `limit=10&include-total=false` (k5, measured) | `curl ':7073/api/alpha/hyperedges?type=code/v05/edits&limit=10&include-total=false'` |
| Q2 | by type + one endpoint | commit detail views | 35 s for 463 rows (c12, measured, contended); **2.6 s** (k5, measured, quiet) — still a scan, not a lookup | `curl ':7073/api/alpha/hyperedges?type=code/v05/edits&end=00033002792f22e8e20a6507ba785e11ecad68ef&limit=100&include-total=false'` |
| Q3 | by type + endpoint prefix (repo) | repo-level reports | not directly supported; requires paging + post-filter (estimated: minutes, same cost as Q1) | — |
| Q4 | count by type | census | 551,794 for `code/v05/edits`, 18,524 for `code/v05/commit` (c12, measured via `/api/alpha/census`); `include-total=true` runs an unbounded typed scan per SPIKE §2 | `curl ':7073/api/alpha/census?type=code/v05/edits'` |
| Q5 | mission-scope per binder | `futon6/scripts/mission_efe_scope_dump.py` (pages every `mission-scope/<binder>` with `include-total=true` on page 1) | most of an 18+ minute EFE publish (c12, measured) | `mission_efe_scope_dump.py` |
| Q6 | all scopes of one paper | `futon6/scripts/render_scope_view.py` | today: parse ~139 EDN files per paper (estimated ~100 ms from disk) | — |
| Q7 | scopes overlapping a line interval of a paper | `render_scope_margin.py` (the mark7 margin HTML) | as Q6, plus per-file filter (estimated ~100 ms) | — |
| Q8 | all scopes of a kind across papers | `expository_scope_audit.py` | today: parse all 1,111 files of a run (estimated ~1 s per run; ~10 min at 5,000 papers, estimated) | — |
| Q9 | join proof-graph node → S1 mark + S4 scopes over same span | planned margin/graph tooling | today: manual cross-referencing of `artifacts/graphs` + `artifacts/marks` + `artifacts/expo` (no index) | — |

The sidecar must serve Q1–Q5 from its hyperedge tables and Q6–Q9 from its scope
tables, in milliseconds, with candidates re-checked per I1 where the truth
lives in XTDB (Q1–Q5) and served file-derived with run provenance where the
truth is run files (Q6–Q9).

## 2. Where the superpod data should live

**Decision: (b) — index the run's files directly in the futon1b sidecar, with
the run directory as source of truth; do not ingest scopes into XTDB.**

Measured basis from the 8-paper run `mark7master-20260921` (/tmp/r7v):

- `artifacts/expo`: 1,111 files (measured, `ls | wc -l`), 772 scope rows parsed
  (measured by prototype). Per-file `:provenance {:generator "expository-json/v1"
  :model "mark4-70b"}` is already in the data (measured, sample file).
- Kind distribution (measured, prototype): connection 693, auxiliary-construction
  39, generalisation 16, difficulty-assessment 14, heuristic-plausibility 5,
  obstruction 5. (The packet's expository 277 / universal-property 35 belong to
  other artifact families not in `expo/*.edn`.)

Extrapolation to 5,000 papers (estimated, linear from 8 papers ≈ 277
paper-prefixes seen in filenames / 96 scopes per paper-prefix... using the
simpler ratio of 772 scopes per 8-paper run ≈ 96 scopes/paper):

- scopes ≈ 480,000 rows; expo passages ≈ 695,000 files (estimated).
- SQLite scope table + indexes at ~60–100 bytes/row (measured 0.2 MB for 772
  rows incl. all prototype overhead) → **30–60 MB** at 5,000 papers (estimated).

Cost of each option at 5,000 papers:

- **(a) Ingest into XTDB + sidecar index.** ~0.7–1.4M new hyperedge documents
  (estimated: 480k scopes + passages) into a store where Q1 already costs
  4–40 s per page for 551k rows (measured). Every scope row pays the full XTDB
  ingest and indexing cost to buy durability and query we already have from the
  files. Provenance must be re-encoded as props. Re-run replacement needs
  retraction transactions (the costliest path in the current API). Rejected:
  it makes the store's worst problem (typed-scan cost) strictly bigger to serve
  data whose truth is, and should remain, the run files.
- **(b) Sidecar indexes run files directly (chosen).** Cost: a file-walker +
  parser (~1.8 s for 152k EDN edges in the prototype, measured; expo parsing
  was below measurement noise). Provenance: keep `run`, `generator`, `model`
  columns straight from the file's `:provenance` — no re-encoding, no loss.
  Re-run of one paper = `DELETE FROM scope WHERE run=? AND paper=?` +
  re-parse that paper's files (measured prototype query shape; sub-ms delete,
  estimated <1 s re-parse). The sidecar's I1 re-check for scopes is
  "does the file row still parse to this", cheap and local.
- **(c) Per-run SQLite beside the run.** Fine for single-run tooling, but Q8
  and Q9 are cross-paper and will become cross-run ("all scopes of kind K",
  "compare run A and run B"); N SQLite files means N-way unions in Python.
  Rejected as primary; per-run files remain a valid export format.

The scope index is a new database section in the same sidecar file family as
`futon1b_text.clj`'s, but it is **file-derived, not XTDB-derived** — it is
governed by the maintenance contract in §4 with the run directory as its
rebuild oracle instead of the XTDB store.

## 3. Schema

Hyperedges (XTDB-derived; mirrors `ev_attr` style):

```sql
CREATE TABLE hx_edge (            -- one row per (hx, endpoint position)
  hx_id    TEXT NOT NULL,
  type     TEXT NOT NULL,
  pos      INTEGER NOT NULL,      -- 0..n endpoint position
  endpoint TEXT NOT NULL);
CREATE INDEX hx_type_end ON hx_edge(type, endpoint);        -- Q2, Q3 (prefix via range/GLOB)
CREATE INDEX hx_type_id  ON hx_edge(type, hx_id);           -- paging Q1 by hx_id cursor
CREATE TABLE hx_props (           -- scalar props the queries filter on
  hx_id TEXT PRIMARY KEY,
  type  TEXT NOT NULL,
  repo  TEXT,                     -- code/v05/* props
  phase INTEGER);
CREATE INDEX hx_props_type_repo ON hx_props(type, repo);    -- Q4-by-repo, Q3 joins
CREATE TABLE hx_meta (key TEXT PRIMARY KEY, value TEXT);    -- watermark: last tx-id, rebuild ts
```

Q3 (endpoint prefix) runs as `WHERE type=? AND endpoint >= ? AND endpoint < ?`
prefix range on `hx_type_end` — a B-tree range seek, no GLOB needed when the
prefix is literal (as `repo/` prefixes are).

Scopes (file-derived):

```sql
CREATE TABLE scope (
  run       TEXT NOT NULL,        -- e.g. mark7master-20260921
  paper     TEXT NOT NULL,        -- :paper/id
  passage   TEXT NOT NULL,        -- :passage/id
  scope_id  TEXT NOT NULL,        -- :id within passage
  kind      TEXT NOT NULL,
  l0 INTEGER NOT NULL, l1 INTEGER NOT NULL,   -- :source :lines
  generator TEXT, model TEXT);    -- :provenance, verbatim
CREATE INDEX scope_paper_lines ON scope(paper, l0, l1);     -- Q6, Q7, Q9
CREATE INDEX scope_kind        ON scope(kind, paper);       -- Q8
CREATE INDEX scope_run_paper   ON scope(run, paper);        -- re-run replacement
```

**Interval mechanism: plain B-tree on `(paper, l0, l1)`, not R*Tree.** Every
interval query in the inventory is equality on paper plus overlap on lines
(Q7: `paper=? AND l0<=:b AND l1>=:a`). Scopes per paper are ~96 (measured), so
after the paper equality the B-tree range on `l0` leaves tens of candidates.
R*Tree exists for many-dimensional spatial joins where no selective equality
prefix exists; here it would add a virtual-table dependency and worse
delete-during-rebuild behaviour for zero measured benefit (prototype Q7: 40
rows, <0.1 ms, measured).

Q9 is a SQL join of `scope` to itself (or to a future `mark` table with the
same shape: `mark(run, paper, mark_id, l0, l1, payload)`) on
`paper AND l0<=b AND l1>=a` — the prototype measured 6 rows in <0.1 ms.

## 4. Maintenance contract

In the terms M-evidence-landscape-index already uses:

- **I1 (non-authoritative candidates).** `hx_*` answers are candidates;
  point-hydrated and re-checked against XTDB before use, exactly as `ev_fts`
  today. `scope` answers are file-derived; the re-check is re-parsing the
  cited `(run, paper, passage)` file, and the row carries its file coordinates
  to make that cheap.
- **I2 (explicit watermark).** `hx_meta` carries `hx-watermark-tx = <xt tx-id>`
  — "this index reflects the store as of tx N". The scope side carries
  `scope-watermark = (run, dir-mtime, file-count)` per run, since the oracle
  is a directory, not a transaction log.
- **Writes.** Hyperedge rows are written **from the write log**
  (`futon1b_write_log.clj` already appends every accepted write as EDN lines to
  `write-log.edn` beside the store): a tailer advances the watermark past each
  entry and upserts `hx_edge`/`hx_props`. Writing at post time (inside the
  request path) is rejected: it couples request latency to SQLite and bypasses
  the serialization the write log already provides — the log is our userspace
  #5730-style ordering point.
- **Replacement and retraction.** Same `(type, hx_id)` re-posted → delete
  `hx_edge` rows for that hx_id, re-insert (positions may change). Retraction
  in XTDB terms (doc absent at re-check) → delete by hx_id. Scope re-run of
  one paper → `DELETE WHERE run=? AND paper=?` then re-parse; the whole
  statement is one SQLite transaction, so readers never see a half-replaced
  paper.
- **Rebuild oracle.** Full rebuild = walk the store by type (the same paged
  scan, done once, offline) or replay `write-log.edn` from line 0; verify by
  comparing `count(*)` per type against `/api/alpha/census` (Q4 numbers above
  are the current expected values). Scope rebuild = re-walk the run directory;
  verify by file count and per-kind counts. Rebuild is always safe because
  nothing authoritative is stored only in the sidecar.

## 5. Prototype numbers

Throwaway build: `/tmp/proto_sidecar.py` → `/tmp/hx_proto.db` (SQLite,
`journal_mode=OFF`, regex parse of EDN; not in any repo).

Sources: `/tmp/v05-cache/edits/page-*.edn` (160 pages ≈ 152,828 edges of the
551,794 total, ~28%, measured) and `/tmp/r7v/artifacts/expo/*.edn` (1,111
files → 772 scope rows, measured).

- **Build: 1.8 s total; 116.9 MB file** (measured). Nearly all of the size is
  the synthetic ~60–100-char hx_id repeated per endpoint row — real hx_ids are
  ~150 chars, so budget ~2×: **~250 MB for the 28% sample, ~0.9 GB for the
  full 551k-edge type** (estimated). An `hx_id → integer` interning table
  would cut this ~5×; worth doing, not required for correctness.
- Query times (measured, best of 5, warm cache):

| Query | Rows | Time |
|---|---|---|
| edits of one commit by sha endpoint (Q2) | 9 | **<0.1 ms** (vs 2.6–35 s live) |
| count edits grouped by repo (Q4-by-repo) | 13 repos | 4.9 ms |
| repo endpoint prefix (Q3) | 1 commit-sample | 0.2 ms |
| scopes of paper 0708.1921 overlapping lines [150,200] (Q7) | 40 | <0.1 ms |
| all scopes of paper (Q6) | 90 | 0.1 ms |
| count scopes kind=connection (Q8) | 693 | <0.1 ms |
| graph-node span join run+paper+[157,160] (Q9) | 6 | <0.1 ms |

Even at 100× rows (full edits type, 5,000-paper scope set) these stay in the
single-digit-ms range by B-tree selectivity (estimated); the queries all have
an equality prefix.

## 6. What should not be stored

**Commit→var edit edges (551,794 rows) should not have been ingested as
hyperedges at all.** They are derivable: `git log --name-only` for futon1b
returned 799 commit/file lines in 4 ms (measured; the packet's "~2 s for all
repos" is estimated and consistent). The var-level refinement (which vars in a
changed file) is a function of the commit and the repo checkout — pure,
recomputable, and already produced by
`futon3c/watcher/commit_ingest.clj` at HEAD. Storing 551k derived rows bought
us a census entry and cost: the single largest typed scan in the store (Q1,
40 s/page), the dominant term in Q2/Q4 pain, and a sidecar table approaching
1 GB (estimated) to index data git already indexes.

Recommendation: **the watcher should write `code/v05/commit→file` edges**
(one per changed file; 18,524 commits → order 50–100k edges, estimated — a
6–10× reduction) and var-level detail should be computed on demand from git
plus the file's var index, or cached in the sidecar only. Do not backfill-delete
the existing 551k in this packet; that is a retraction decision for Joe.
(💡 A `commit→file` edge type plus a `var-at-commit` resolver would also make
the v05 data robust to force-push/history edits, which HEAD-var capture is not.)

## 7. Implementation packets

1. **P1 — `hx_edge`/`hx_props` schema + write-log tailer.** One behaviour:
   tail `write-log.edn`, upsert hyperedge rows, advance `hx-watermark-tx`.
   Acceptance: post a new hyperedge, observe it in the sidecar with watermark
   ≥ its tx-id, without any store re-scan.
2. **P2 — candidate-backed Q2 (type+endpoint) route.** Serve
   `hyperedges?type&T&end=E` from `hx_type_end`, point-hydrate and re-check
   each candidate against XTDB (I1). Acceptance: the Q2 sha query above
   answers <100 ms and a planted stale candidate is caught by the re-check
   (the deliberate stale-read test from the mission acceptance bar).
3. **P3 — count/type and endpoint-prefix routes (Q4, Q3) + census fast path.**
   Acceptance: census for `code/v05/edits` answers from the sidecar in ms;
   prefix query returns identical membership to a paged scan on a small type.
4. **P4 — replacement/retraction coverage + rebuild oracle.** Retraction
   deletes, oracle compares per-type counts to census. Acceptance: retract a
   hyperedge, sidecar row gone; full rebuild reproduces counts exactly.
5. **P5 — scope sidecar (file-derived) + Q6–Q8.** Walk a run directory into
   `scope`, with per-run watermark. Acceptance: re-parse of /tmp/r7v
   reproduces the 772-row kind counts above; Q7 for a sample paper matches
   `render_scope_margin.py` output for the same interval.
6. **P6 — re-run replacement + Q9 join table.** `mark` table, scope replace
   transaction. Acceptance: touching one paper's files and re-indexing changes
   exactly that paper's rows; the graph-node→mark+scope join returns the same
   spans a manual /tmp/r7v cross-check finds.
7. **P7 (decision-gated on Joe) — watcher writes commit→file; retire or
   freeze commit→var ingest.** Acceptance: new commits produce file edges
   only; census growth of `code/v05/edits` stops.

Order rationale: P1–P2 retire the worst measured pain (Q2 scan) with the
smallest surface; P3–P4 complete the hyperedge contract before the scope work
adds a second data family; P5–P6 are independent of P3–P4 and could run in
parallel once P1 lands.
