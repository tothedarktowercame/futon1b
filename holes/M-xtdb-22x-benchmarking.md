# Mission: M-xtdb-22x-benchmarking — a #5637/#3663 benchmark on XTDB 2.2.x, from arXiv corpora

**Date:** 2026-08-05
**Status:** OPEN — authored same day as the Henderson call; nothing dispatched yet
**Owner:** claude (Fable session). Driver: Joe.
**Home:** futon1b/holes/ (beside the technotes and probe scripts this builds on)

Cross-refs:

- Call outcome: `futon7/data/outbox/staged/2026-07-26--james-henderson--xtdb-3663--jx5637/call-outcome-2026-08-05.md`
- `futon1b/TN-xtdb2-query-ceilings-and-ingest-memory-2026-08-02.md` — the 2.1.0
  findings this mission re-verifies on 2.2.x (§1 id-set hydration, §2
  fixture inversion, §5 sustained ingest)
- `futon1b/TN-xtdb-derived-secondary-index.md` — the one-facility/two-backends
  frame; the pgwire gap that 2.2.x reportedly addresses
- `futon3c/docs/xtdb2-memory-latency-2026-07-30.md` — memory observations,
  now partially reinterpreted by James's guidance (see HEAD)
- `futon2/holes/M-text-sidecar.md` — parent thread; D1 sidecar + textprobe
  evidence methodology this benchmark ports to public data
- https://github.com/xtdb/xtdb/issues/5637 · https://github.com/xtdb/xtdb/issues/3663
- Corpora: `storage/arxiv-paper-hg-gpu/` (superpod math.CT mining run),
  `storage/arxiv-manifest/arxiv_manifest.sqlite` (math-archive metadata harvest)
- Probe scripts to re-run: `futon1b/probe_in_predicate.clj`,
  `probe_id_batch_planning.clj`, `scale_probe_bisect.clj`

## HEAD — operator anchor

Joe, same day as the call (2026-08-05): James is "happy to collaborate on
3663 first, and what I'm proposing to do is develop a benchmark for 5637."
For initial development, "we could certainly afford to spend a week working
with the original mining run [arxiv-paper-hg-gpu], and see if it shows us
things about graph performance at that scale." Destination: "ultimately
we're going to want to ingest the whole Arxiv as a big database, and I
think CT is 1/100 of the scale of that." And: the arXiv manifest on the
laptop enables "a 'holographic' ingest with version histories and e.g.
indexing of abstracts, not relying on the superpod. Applying the 'scopes'
analysis to just abstracts is probably feasible for a Linode GPU job, then
we'd have a 'mini' version of the overall corpus to think about."

James's steers from the call, which this mission takes as given:

1. **2.2.x has had more effort put into auth** → plan a fresh migration
   rather than hardening 2.1.0; and re-verify the 2.1.0 findings on 2.2.x
   before citing them again.
2. **Running at full allocated memory is expected** — roughly half heap,
   half off-heap. Provision `-Xmx` at about half the intended footprint.
3. **Collaboration on #3663 first**; the benchmark is our proposed
   contribution vehicle for the #5637 side.

## Why a mission and not a task

The goal is clear (a benchmark JUXT can run) but the specification is not:
what 2.2.x changed (which reshapes the query ladder), how to obtain real
version histories (the manifest's harvest is single-version today — see
facts below), what the harness contract is, and which memory behaviours
survive correct provisioning are all genuine DERIVE questions. The probes
decide what D1 actually is.

## Facts on the ground (verified 2026-08-05)

- **Superpod CT run** (`storage/arxiv-paper-hg-gpu/`, generated 2026-02-24,
  source arxiv.math-ct): 9,916 entities, 22,721 relations, 30,556 LaTeX
  fragments, BGE-large embeddings; `hypergraphs.json` 337M; has its own
  `manifest.json`. It is a **snapshot** — no revision histories. Node/edge
  vocabulary per the superpod pipeline (post/term/expression/scope nodes;
  iatc/mention/discourse/scope/surface/categorical edges).
- **arXiv manifest** (`storage/arxiv-manifest/arxiv_manifest.sqlite`, 785M):
  `papers` table with **570,209 rows = 570,209 distinct papers** — the math
  archive at full breadth — each with title, abstract, authors, categories,
  dates, license, withdrawal flag. The schema is **already per-version**
  (`PRIMARY KEY (arxiv_id, version)`, `latest` flag) but the harvest to
  date holds **exactly one version per paper** (max version = 1; zero
  papers with >1 row; zero distinct-abstract pairs). So the holographic
  ingest is well-founded and the **version-history leg is a fillable
  harvest gap, not a schema change**.
- **Scale ladder:** math.CT = 4,616 manifest papers ⇒ CT ≈ **1/124 of the
  math archive**; whole arXiv (~2.9M papers) is roughly another 5× beyond
  math. Joe's "CT is 1/100 of the destination" is right at the
  math-archive rung.
- **Fixture-inversion governs the method** (TN 08-02 §2, proven twice on
  our own store): small-scale point values can invert relative to live
  scale. Therefore the benchmark's output is **shape-vs-scale curves, not
  point values**, and the CT week is instrument calibration, not number
  production.

## IDENTIFY

### The gap

1. **The #3663 collaboration needs shared, reproducible ground.** Our
   evidence so far (textprobe, sidecar oracle, request-journal timings) is
   real but private. A benchmark on public, redistributable data is the
   difference between "believe our numbers" and "run this yourself."
2. **The 2.1.0 findings are about to go stale.** Moving to 2.2.x
   invalidates-or-refreshes the XTQL id-set findings, the compile ceiling,
   and possibly the memory profile. Re-verification is the cheapest
   highest-value first step and gates everything downstream.
3. **No benchmark exists for the workload class** XTDB is distinctively
   for: text search composed with scalar filters and graph traversal over
   a *bitemporal* corpus (as-of text queries; ever-held index semantics).
   That is exactly the workload our memory system generates and exactly
   what #5637's design questions need numbers for.

### Deliverables

- **D1 — the harness.** Manifest-driven, corpus-pluggable, and
  scale-parameterized: ingest replay at controlled rates; a query ladder
  (point text lookup → text+scalar compose → text+graph traversal → as-of
  text query); an exhaustive-scan oracle on sampled query sets; a metric
  vocabulary carried over from the futon evidence (posting inflation,
  staleness bound, candidate false-positive rate under re-check, hydration
  cost, ingest profile including recovery-after-burst). Corpus adapters:
  superpod CT run now; holographic manifest corpus when D3 lands; Rob's
  mark-2 run and other categories as drop-ins.
- **D2 — CT-scale curves on 2.2.x.** The affordable week: same harness at
  ≥3 sub-scales within the CT corpus (e.g. 1k / 5k / 10k papers) on Zone,
  provisioned per James's guidance. Includes the ingest-profile leg — a
  controlled public re-run of TN 08-02 §5 (does the reclaim-bound
  non-recovery reproduce at correct provisioning?).
- **D3 — the holographic corpus.** Whole-math-arXiv metadata + abstracts
  ingested as a bitemporal store, with **real version histories** once the
  supplementary harvest (P2) fills them in — real wholesale-rewrite
  updates at 570k-paper breadth, giving the ever-held axis genuine data to
  replace our 5-month post-flattening lower bound.
- **D4 — the benchmark note.** A write-up to James / the #3663 thread that
  the collaboration can anchor to: harness, first curves, and what each
  curve says about the spec decisions. **Posting gated on Joe's read.**

### Follow-ons (named now, deliberately out of POC scope)

- **F1 — scopes-on-abstracts.** The superpod Stage-5/scopes analysis
  applied to abstracts only → a structural "mini" corpus at full
  math-arXiv breadth (hypergraph rungs of the ladder at 570k scale).
  Venue options: Linode GPU job, **or Zone itself (CPU-only, 256 GB)** —
  plausible split: the corpus-wide legs (BGE embeddings + pattern-based
  Stage-5 detection) are CPU-feasible on abstracts-only volume (~600 MB of
  text); an LLM leg, if wanted, runs as a quantized GLM *Air*-class MoE
  via llama.cpp (RAM-resident, low active-parameter count) but is
  throughput-bound on CPU, so it serves **sampled judgment / term
  discovery**, not corpus-wide labeling. HARD CAVEAT: Zone is also the
  benchmark test host — mining jobs and benchmark measurement runs must
  not share the box in time (the 07-30 measurements were confounded by
  exactly such a concurrent sweep); schedule in separate windows.
  Authored as its own leg when D3 exists to feed it.
- **F2 — the mark7 mining run (Rob / Superpod).** The latest pipeline is
  **mark7** (packaged handoff: `futon6/holes/mark7-rob-handoff.md`, awaiting
  a 20 h Superpod window); its cost pole is LLM stages (S3/S4/S7) on 8×A100
  — full-scale mark7 does NOT fit Zone. **But the pipeline is
  endpoint-agnostic** (`OPENAI_BASE_URL` + `--model`), so Zone + a
  quantized GLM-Air behind llama.cpp's OpenAI-compatible server can run
  the packaged smoke test and a **quality probe at N=25–100 citation-ranked
  papers** (days at CPU speed) — proving output quality *before* Rob is
  asked for anything, and incidentally giving a GLM-Air vs LLaMA
  model-sensitivity comparison. The benchmark harness treats corpora as
  manifest-shaped inputs precisely so mark7's output drops in without
  rework whenever it lands.
- **F3 — whole-arXiv beyond math**, and full-text ingest. The destination;
  needs its own harvest and storage planning.
- **F4 — production futon1b migration 2.1.0 → 2.2.x.** Motivated by the
  auth improvements; a separate, operator-gated infrastructure operation
  once 2.2.x is proven on Zone. When it happens, capture ever-held
  divergence immediately before and after — the "migrations reset
  ever-held accumulation" finding predicts the reset and the migration is
  a free data point.

### POC scope boundary

In scope: throwaway and test 2.2.x nodes on Zone; the probe re-runs; CT
corpus ingest replay and query ladder; synthetic rewrite replay for
harness development (labeled synthetic); the supplementary version harvest
for metadata+abstracts; D3 at metadata+abstract resolution. Out of scope:
full texts; GPU jobs; touching the production laptop store; any in-core
XTDB work. Follow-ons get authored when their boundary is hit, not left
implicit.

### Acceptance bar

- **D1:** one harness runs at ≥3 scales from a manifest-shaped input with
  no code change; oracle agreement on a named query set (the M-text-sidecar
  oracle discipline: sidecar/index answers vs exhaustive scan + re-check);
  every reported number reproducible from a script in this repo; a person
  outside our stack (i.e. JUXT) can run it from public data.
- **D2:** curves for each ladder rung with confounds stated per the TN
  house style; ingest profile reports the heap/off-heap split (so the
  numbers land in James's own vocabulary) and the post-burst recovery
  observation either reproduced or explicitly not.
- **D3:** version updates ingested as genuine bitemporal updates (not
  synthetic); ever-held divergence measured on real arXiv version
  histories and compared against the futon corpus findings (1.028
  aggregate / heavy-tail p99 3.80 / class concentration).
- **D4:** states corpus, method, and confounds so JUXT can discount
  appropriately; gated on Joe's read before anything is sent or posted.

## Constraints

- **Zone (256 GB) is the test host; the laptop store is production and
  untouched by this mission.** Provisioning per James: `-Xmx` ≈ half the
  intended footprint, cgroup headroom above that for page cache.
- XTDB 2.2.x consumed as a dependency; **verify the current 2.2.x release
  at P1 time** — no local xtdb checkout, consistent with M-text-sidecar.
- Substantial build legs follow the coding-handoff protocol (belled to
  Codex, small packets, discovery split from implementation, park on every
  dispatch). Probes P1/P2 are likely carve-out (b) — the context lives
  here and in the TNs.
- The 30-min-class job cap and bell-visibility constraints apply to any
  long ingest runs: prefer `systemd-run` / durable runners for multi-hour
  ingests, with progress observable from files, not just job state.

## MAP / DERIVE — probes, car first

Per car-of-sequence: run P1, observe, re-rank the rest. These are DERIVE —
their findings decide what D1 is.

- **P1 (the car) — 2.2.x delta probe.** Stand up a throwaway 2.2.x node
  and re-run the three probe scripts: does XTQL now have a list-membership
  predicate? Does an `or` of primary-key equalities still lose the index?
  Where is the compile ceiling? Then the auth surface: do the 2.1.0 gaps
  (embedded client credentials; the resurrecting `xtdb` template row / two
  `pg_user` rows after `ALTER USER`) still reproduce, per the bounded repro
  in TN-xtdb-derived-secondary-index? **Everything downstream reshapes on
  these answers**: a membership predicate changes the re-check rung of the
  ladder; the auth answer sets F4's urgency and mechanism.
- **P2 — version-harvest probe.** Determine the cheapest honest route to
  per-version metadata+abstracts for the math archive (OAI-PMH vs abs-page
  scraping vs the export API), its cost and etiquette at 570k-paper
  breadth, and extend the existing harvester (the sqlite schema already
  fits; `harvest_runs`/`download_queue` machinery exists). Output: a
  version-history coverage statement (what fraction of papers have >1
  version; distribution) — itself a #5637-relevant number.
- **P3 — harness logic-model** (logic-model-first, before any build): the
  ingest-replay contract (rate control, burst shapes, what gets recorded),
  the ladder rung definitions with their oracles, the metric definitions,
  and the corpus-adapter interface (what "manifest-shaped" means,
  concretely, so superpod / holographic / Rob's run all fit). One page,
  agreed here, before code.
- **P4 — provisioning matrix on Zone.** With P1's node warm: which 2.1.0
  memory behaviours persist at correct provisioning — the ~2 s list-scan
  floor, metaspace growth under query-shape variety, burst non-recovery?
  Whatever survives generous, correctly-split provisioning is structural
  and goes in D4; whatever vanishes was provisioning artefact and closes
  that chapter honestly.

## ARGUE (strategic, plain language)

The call converted a cold design discussion into a named collaboration
(#3663 first) with us holding the evidence pen. The highest-value thing we
can put on that table is a benchmark **JUXT can run themselves** on public
data — it converts our private credibility into shared infrastructure, and
it is the natural container for every finding we already have (the ladder
rungs are our production pain points; the metrics are our textprobe
vocabulary; the ingest leg is our outage, reproduced controlled). The CT
week is cheap because the harness, not the numbers, is the deliverable —
our own fixture-inversion finding says small-scale numbers must not be
trusted, so we build the instrument that measures the curve instead. The
holographic corpus is the strategic move: real version histories at
570k-paper breadth for metadata-only cost, which upgrades the weakest part
of our #5637 evidence (the flattened-history lower bound) into the
strongest part of the benchmark. The risk declined: a month of building
against unverified 2.2.x assumptions — hence P1 as the car.

## VERIFY / INSTANTIATE

Deferred until DERIVE reports, per exploratory-slices-are-DERIVE. When the
probes settle the design: ARGUE the committed harness design in a dated
section here, then INSTANTIATE with the usual gates (clj-kondo,
check-parens on any elisp/clj, tests, oracle-agreement from the acceptance
bar). Benchmark runs that produce cited numbers get the TN treatment:
environment stated, confounds listed, scripts named.

## Next steps (recorded 2026-08-05, post-first-ingest)

0. **XTQL `in` — build it ourselves?** (James via Joe, 2026-08-05 follow-up:
   most XTDB users use SQL and don't much care about XTQL — "if we want
   `in` we may have to build it.") Consequences: (a) the benchmark's
   client story stays **SQL-first** — which matches both the user base and
   our production workaround, and the ladder already is; (b) the R5
   finding converts from bug-report-and-wait into a **well-scoped first
   in-core contribution**: an XTQL list-membership predicate lowering onto
   the same plan SQL `IN` already uses (the planner machinery exists — the
   work is XTQL surface: parse/type-check `(in expr [literals…])` and
   lower it). Small, testable, directly ours to need, and pre-signaled as
   acceptable by the maintainer. NOTE: this changes the mission's "no
   local xtdb checkout" constraint — an in-core PR needs one; that
   boundary move happens only when this item is actually picked up.
   (c) Joe's XTQL-first motivation for the memory system is now partly a
   sustainability question — XTQL staying good depends on users like us
   contributing; worth saying plainly in the next Henderson exchange.

1. **rc0 auth repro** — the other half of P1: re-run the pgwire/authn
   bounded repro (TN-xtdb-derived-secondary-index §write-path) against
   2.2.0-rc0 to test James's "more effort into auth" concretely. Also look
   at the **Flight SQL server** rc0 starts by default — new client (and
   auth?) surface.
2. **Re-ingest cadence** — `ingest_graphs.clj` is idempotent; wire a
   periodic re-run (cron or a loop on Zone) so the bench store tracks the
   shards. Each re-ingest of a changed graph is a bitemporal version —
   deliberately part of the benchmark corpus's history.
3. **Full-extraction ingest** (Joe, 2026-08-05: ingest should include
   symbol grounding and the other pipeline features, not just S3). Facts
   established: all 100 papers' S1 anatomy is on Zone
   (`data/showcases/ct-anatomy/golden/fable-<pid>-dp-emacs.json` — marks
   with symbol typings, scopes, binders), and the enriched candidates
   carry `binder-context` / `enrichment` / `source-window` /
   `anchor-lines`. Widen the schema: `iatc_marks` (per-mark rows from
   dp-emacs JSON), `iatc_candidates` (enrichment layers), plus rung2
   reports and `:holes` as rows. That makes "browse a fully-ingested
   article" a query, and gives the text/scalar rungs realistic volume.
4. **Programmatic quality checks — seeded today as R6 in
   `query_probe.clj`**, integrity-as-queries over the first 10 graphs
   (n=10 is enough to start qualitative work; 100 would be overkill —
   Joe). First results: **2/30 edges dangle their premise ref; 0 dangling
   conclusions; 0 empty node texts; 1/69 nodes anchored outside its
   passage; 18/27 warrant-bearing edges are `missing-warrant` (3 edges
   carry no warrant at all)**. Next checks worth adding: rung2-soft-fail
   tally (every recent shard pass carries one — quantify), per-paper
   warrant-missing rates, node-text ⊆ source-window faithfulness (needs
   the candidates layer from step 3), and a mark6-overlap comparison
   where papers coincide. Instrument caveat, upstream-worthy: correlated
   `NOT EXISTS` over an array element (`premise_refs[1]`) returned
   silently wrong results on rc0 where the equivalent LEFT JOIN is
   correct — potential SQL-semantics bug to minimally reproduce and
   report.
5. **Browsing surface** — verified: **no Emacs mode for a fully-ingested
   article exists** (nothing consumes the dp-emacs JSON format; the name
   was aspiration). Best renderings to date were the iffy HTML showcases.
   With step 3 done, the cheapest good browser is probably a thin Emacs
   mode querying the bench store (pgwire or the new Flight SQL surface)
   rather than reviving the HTML path — house precedent:
   `futon4/dev/arxana-xtdb-browse.el`. Decision deferred; rendering is
   not on the benchmark's critical path.
6. **Pass discipline** (Joe's caution): the pipeline model assumes a full
   corpus pass per stage before the next refinement pass — the
   completeness ledger enforces exactly this, so *official* S4+/second-pass
   numbers wait for S3 × 100 to drain. But prefix work on the first ~10 is
   legitimate **DERIVE probing** when labeled as such: S7 box-typing is
   per-graph and can run on the prefix as a probe; the R6 checks above are
   corpus-independent integrity facts; neither pretends to be a pass.

## Log

- 2026-08-06 — **Bench store at full e2e corpus: 98 graphs / 772 nodes /
  419 edges** (ingest idempotent, 83 ms/graph warm). Ladder at this size,
  cold→warm over 3 repeats: R1 point 163→24 ms · R2 scalar 224→83 ms (608
  rows) · R3 text LIKE 185→45 ms (27 rows — the corpus is finally big
  enough for text queries to return anything) · R4 edge→node join 289→37 ms
  (409 rows) · SQL `IN` 131→63 ms.

  **R6 quality census across three corpus sizes — the useful result:**

  | check | n=10 | n=22 | n=98 |
  |---|---|---|---|
  | dangling premise refs | 2/30 (6.7%) | 2/77 (2.6%) | **3/419 (0.7%)** |
  | dangling conclusion refs | 0 | 0 | **0** |
  | empty node text | 0 | 0 | **1/772 (0.1%)** |
  | anchor outside passage | 1/69 | 1/179 | **1/772 (0.1%)** |
  | missing-warrant edges | 18/27 (67%) | 42/73 (58%) | **222/383 (58%)** |

  Two things this says. (1) **Structural defects are front-loaded, not a
  constant rate** — the dangling-ref rate falls ~10× as the corpus grows,
  i.e. the early graphs carried them and later ones did not; total
  structural defect load is ~0.4% of nodes+edges. (2) **The missing-warrant
  rate has stabilised at 58%** across a 4.5× corpus increase — stable
  enough to quote: *in mined category-theory proofs, 58% of
  warrant-bearing inference edges carry `missing-warrant`.* That is the
  quantified form of the Lakatos point (published mathematics deletes its
  scaffolding), and it is the number the three-arm exam in
  `futon6/holes/E-mining-qual-loop.md` §3 is designed to attribute —
  literature omission vs extraction failure.

- 2026-08-05 (evening) — **Bench node LIVE; P1's first rc0 datum in.**
  `futon1b/bench22x/` (deps.edn + `ingest_graphs.clj` + `query_probe.clj`,
  house `:node` pattern bumped to **2.2.0-rc0**) deployed to
  `zone:~/xtdb-bench`, store at `~/xtdb-bench/store`. First ingest: 10
  mark7z graphs → 10 `iatc_graphs` / 69 `iatc_nodes` / 30 `iatc_edges`
  rows, ~417 ms/graph incl. tx; idempotent by deterministic ids. First
  ladder probe (3 repeats, cold→warm): R1 point lookup 116→26 ms; R2
  scalar 72→32 ms; R4 edge→node join 172→34 ms; R3 LIKE scan ~45-60 ms
  (executes; term absent from first papers). **R5: `(in _id [...])` FAILS
  on 2.2.0-rc0 with the same "in not applicable to types utf8 and list"
  — the 08-02 §1 XTQL membership gap carries to 2.2.x**; SQL `IN` works
  (86→18 ms); `or`-form accepted (index behavior untestable at 69 rows).
  Notes: `text` is a reserved word in the SQL grammar (quote it); rc0
  starts a Flight SQL server by default (new surface, worth a look for
  the benchmark's client story). Re-ingest cadence: rerun ingest as
  shards produce; scale probes become meaningful ≥10k nodes.

- 2026-08-05 (afternoon) — **F2's quality-probe leg EXECUTED on Zone, same
  day.** GLM-4.5-Air UD-Q4_K_XL (64G) downloaded + served via fresh
  llama.cpp build (`:8090`, `--reasoning-budget 0`, ~4.2 tok/s CPU);
  futon6 synced from laptop (canonical at `~/code/futon6` on Zone);
  top-100 eprints staged (all local, no arXiv fetch). **mark7 smoke test
  PASS end-to-end**: emit_marks 1,871 marks on `math__0608040`, candidate
  extracted, IATC graph generated by GLM-Air and **gated PASS**
  (argcheck + substance) — grounded typed nodes with source-line anchors
  into HTT. Portability fix shipped both trees: hardcoded LLM timeouts →
  `FUTON6_LLM_TIMEOUT` env (defaults unchanged). **Top-100 run
  (`mark7z`, corpus `math-ct-top100`) LAUNCHED** via
  `linode_stepper --from S1 --reuse S0 STAGE --no-halt`, S1 running.
  Operational lessons: pkill/pgrep self-match (twice); Python
  block-buffering makes a live stepper's log lag — trust the ledger and
  process table, not the log tail. XTDB release fact for P1: latest 2.2.x
  is **v2.2.0-rc0** (2026-07-21).

- 2026-08-05 — mission authored (Fable session) from the Henderson call
  debrief. Corpus facts verified same day (manifest schema/counts; superpod
  manifest). Nothing dispatched; P1 is the car.
