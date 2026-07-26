# Spike: attribute-filter performance in the futon1b XTDB2 store

Date: 2026-07-26  
Scope: read-only investigation of the live `migration-store-21` service and the
futon1b serving code. No source or store mutations were made. Every endpoint probe
was bounded with `limit`; plan inspection used `EXPLAIN`, not `EXPLAIN ANALYZE`.

## Executive finding

This is not primarily a case of futon1b fetching every full document and then
filtering all five shapes in Clojure. Most scalar predicates are already expressed
as XTDB `where` predicates. The exception in the reported workload is evidence
`tags` (and several other evidence fields), which is deliberately post-filtered by
futon1b after reading ordered 1,000-row projections. However, an XTDB `where`
predicate is not evidence of a row-level secondary index: the live 2.1.0 plans show
`scan` operators with predicates for `evidence$at`, `evidence$author`, and
`hx$type`. XTDB issue
[#3663](https://github.com/xtdb/xtdb/issues/3663) describes the present mechanism
as per-column, page-level metadata which can prune chunks/pages but is
under-selective for unique or near-unique attributes. It remains open.

There are also two important futon1b amplification effects:

1. `GET /evidence?...&limit=1` still asks XTDB for ordered projected pages of
   1,000 until Clojure post-filters produce a match.
2. `GET /hyperedges?type=T&limit=N` defaults to `include-total=true`, so it runs a
   second, unbounded-in-cardinality typed scan solely to return an exact count.

The 35-second memory-projection reports have a different immediate cause. Current
projection reads normally use a maintained in-JVM index and take tens of
milliseconds. Any unrelated XTDB transaction changes the global node watermark,
causing a complete projection rebuild and point hydration; under writes that build
can retry five times and fail as `memory-projection-source-not-quiescent`.

Recommendation: fix those futon1b amplification effects first, then extend the
existing SQLite derived-index precedent into one general attribute/text sidecar,
starting with append-only evidence. Retain XTDB as truth and re-check candidate IDs.
In parallel, take the measurements below upstream to #3663/#5637 as evidence for a
native, log-maintained secondary-index facility shared by scalar and text index
implementations.

## Method and live state

The live server was PID 1991418, XTDB 2.1.0, with HTTP on 7073 and pgwire on
`127.0.0.1:40563`. Its command line contained `-Xmx4g` and
`-XX:MaxDirectMemorySize=1536m`. `psql` was not installed, so I used the already
installed PostgreSQL JDBC driver to make a read-only pgwire connection. XTDB 2.1
supports SQL `EXPLAIN`; XTQL also exposes an `explain?` query option in the
[official query reference](https://docs.xtdb.com/reference/main/xtql/queries#query-options).

The timings below are wall-clock client timings for bounded HTTP requests made at
12:50–12:51 BST, while production traffic continued. They are observations under
load, not isolated benchmarks. Historical observations cite exact
`journalctl --user -u futon1b-server.service` timestamps and elapsed values.

## A. Data layout and filter placement

### Evidence

`build-evidence-doc` writes table `:evidence` documents with `:xt/id` and the
columns `:evidence/id`, `:evidence/type`, `:evidence/claim-type`,
`:evidence/author`, `:evidence/at`, `:evidence/body`, and `:evidence/tags`, plus
optional subject, pattern, session, reply/fork, conjecture, and ephemeral columns
(`futon1b_evidence.clj:35-74`). In SQL these appear as `_id`,
`evidence$id`, `evidence$type`, `evidence$claim_type`, `evidence$author`,
`evidence$at`, etc.

The list endpoint reads a compact projection rather than full bodies
(`futon1b_evidence.clj:162-169`), orders it by `evidence/at DESC, xt/id DESC`,
and takes fixed 1,000-row keyset pages (`futon1b_evidence.clj:194-212`). It applies
the authoritative post-filters, selects the requested window, then hydrates only
the selected IDs with point reads (`futon1b_evidence.clj:219-254`).

Filter placement is:

| Evidence query parameter | XTDB `where` predicate | Clojure post-filter |
|---|---:|---:|
| `type`, `claim-type`, `author`, `session-id`, `fork-of` | yes | no |
| `since`, `before` | yes | yes, redundant contract check |
| `tags` | **no** | yes, all requested tags must be present |
| `subject-type`, `subject-id`, `pattern-id` | no | yes |
| `include-ephemeral=false` | no | yes |

The pushdown clauses are explicit at `futon1b_evidence.clj:171-192`; the
post-filters are at `futon1b_evidence.clj:261-284`. Therefore shape 1 is mixed:
`since` is pushed into XTDB, but `tags` is filter-after-page in futon1b. Shape 2's
`author` and `session-id` are XTDB predicates, although `limit=1` does not reduce
the internal ordered page below 1,000.

Point evidence reads are true `_id` predicates (`futon1b_evidence.clj:80-86`).

### Hyperedges

Hyperedges live in `:hyperedges` with `:xt/id`, `:hx/id`, `:hx/type`,
`:hx/endpoints`, `:hx/ends`, `:hx/labels`, and `:hx/props`. Migration also
denormalizes nested props into top-level `:prop/<key>` columns because XTQL
`where` cannot navigate the nested map (`migration/transform.clj:111-130`,
`migration/transform.clj:279-302`). SQL exposes names such as `_id`, `hx$type`,
`hx$endpoints`, `prop$repo`, and `prop$source_file`.

For shape 3, `type`, `repo`, and `source-file` become XTDB predicates. A bounded
query selects only `_id`, type, timestamp, repo, and source-file, orders/limits in
XTDB, then point-hydrates the selected IDs
(`futon1b_graph.clj:507-537`, `futon1b_graph.clj:581-608`). Thus this is not
Clojure filter-after-full-fetch. The expensive surprise is the default exact
total: unless `include-total=false`, latest, repo, or source-file is supplied, a
second query reads all `_id` values of that type and Clojure counts them
(`futon1b_graph.clj:609-637`).

Bounded type windows with `include-total=false` have a 32-entry process cache,
synchronously invalidated on hyperedge mutation
(`futon1b_graph.clj:639-662`).

### Entities

Entities live in `:entities` with `_id`, `entity$id`, `entity$name`,
`entity$type`, `entity$external_id`, `entity$source`, and `entity$props`; current
writes construct that shape at `futon1b_graph.clj:232-247`.

`GET /api/alpha/entity/<id>` does no application-level corpus filtering, but may
execute three sequential XTDB queries: `_id`, then `entity/name`, then
`entity/external-id` (`futon1b_graph.clj:121-133`). The fallback attribute
predicates are therefore exposed to the same scan/selectivity problem, and a miss
can pay for all three. The two-part external lookup pushes both `entity/source`
and `entity/external-id` (`futon1b_graph.clj:201-219`).

The separate typed entity collection does push `entity/type`, but sorts and takes
the HTTP limit only after XTDB has returned every matching document
(`futon1b_graph.clj:279-290`). That is another futon1b-side limit-pushdown
opportunity, though not one of the five named shapes.

### Memory projection

Shape 4 normally does not query the XTDB corpus. Startup builds a bounded current
index of at most 5,000 `memory/assert` edges, hydrates each edge and its evidence
entry by `_id`, and indexes components by endpoint in an atom
(`futon1b_graph.clj:758-829`). Successful memory-edge mutations synchronously
point-refresh it (`futon1b_graph.clj:831-852`).

The flaw is the read-time validity check. It compares the index's source watermark
with the node's **global** latest transaction/message coordinates and rebuilds on
any mismatch (`futon1b_graph.clj:670-680`, `futon1b_graph.clj:959-972`). An
unrelated evidence append therefore invalidates a projection that did not change.
During a rebuild, any transaction advancing the watermark makes the attempt
incoherent; after five attempts it returns 503. Explicit historical projection
requests correctly bypass this current-state index.

### Shared concurrency

Every XTDB query goes through one fair, process-wide semaphore of width four
(`futon1b_xt.clj:13-34`). This prevents the former 16-query hydration fan-out, but
also means long scans create a convoy in which otherwise selective or point
requests wait behind scan work. It explains why the same endpoint alternates
between milliseconds and seconds without changing its query shape.

## B. Plans and bounded measurements

### Plans

The following are faithful compact renderings of the live `EXPLAIN` output. They
model the actual projected XTQL queries, including the internal page size. They are
plans only; no rows were read by `EXPLAIN`.

**Shape 1, tags only (`limit=1` at HTTP; 1,000 internally):**

```text
top limit=1000
  project [_id evidence$at evidence$tags]
    order-by [evidence$at DESC, _id DESC]
      scan table=evidence columns=[evidence$at _id evidence$tags]
           predicates=[]
```

The requested tag is absent from the plan. Clojure checks it after this page.

**Shape 1 with `since=2026-07-26T11:30:00Z`:**

```text
top limit=1000
  project [_id evidence$at evidence$tags]
    order-by [evidence$at DESC, _id DESC]
      scan table=evidence columns=[evidence$at _id evidence$tags]
           predicates=[(>= evidence$at "2026-07-26T11:30:00Z")]
```

**Shape 2, `author=codex-2&limit=1`:**

```text
top limit=1000
  project [_id evidence$at evidence$author evidence$session_id]
    order-by [evidence$at DESC, _id DESC]
      scan table=evidence
           predicates=[(== evidence$author "codex-2")]
```

The session variant has the same shape with an equality predicate on
`evidence$session_id`.

**Shape 3, `type=memory/assert&limit=1000`:**

```text
top limit=1000
  project [_id hx$type prop$timestamp]
    order-by [_id ASC]
      scan table=hyperedges
           predicates=[(== hx$type "memory/assert")]
```

With the endpoint's default `include-total=true`, an additional plan is:

```text
project [_id hx$type]
  scan table=hyperedges
       predicates=[(== hx$type "memory/assert")]
```

There is no limit on that count input. Importantly, all four plans say `scan`;
the equality/range terms are scan predicates, not separate index seeks.

### Bounded differential timings

| Probe | Result |
|---|---:|
| `/health` | 2.69 ms |
| evidence `author=codex-2`, `limit=1` | 0.751 s |
| evidence `tags=turn-round`, `limit=1` | 1.695 s |
| same tag plus `since=11:30Z`, `limit=1` | 0.592 s, no match |
| hyperedges `memory/assert`, `limit=10`, `include-total=false` cold | 2.915 s |
| exact repeat of previous request | 0.008 s |
| hyperedges `code/v05/sorry`, `limit=10`, `include-total=false` cold | 2.058 s |
| hyperedges `code/v05/mission-doc`, `limit=10`, `include-total=false` cold | 2.382 s |
| hyperedges `memory/assert`, `limit=10`, default exact total | 3.769 s |
| entity `math/rewrite-orientation` | 0.202 s |
| current memory projection, one endpoint | 0.029 s then 0.034 s |

The `since` differential supports the source reading: time reduces the XTDB scan
input before futon1b's tag test. Similar 2–3 second cold times across distinct
hyperedge types at only ten returned rows are consistent with page/chunk scanning
plus point hydration rather than a selective row index. The 2.915 s to 8 ms exact
repeat demonstrates that the existing JVM cache is highly effective for repetitive
mission sweeps, but only when callers opt out of the exact total.

### Historical production evidence

These are representative exact journal records, not estimates:

* 12:03:32: evidence `limit=1000&since=...` completed in 2,935 ms.
* 12:03:44 and 12:03:58: `code/v05/mission-doc&limit=500` completed after the
  clients disconnected, in 28,443 and 22,223 ms.
* 12:04:54–12:05:53: four memory projections took 23,572, 19,622, 19,301, and
  20,068 ms, each ending 503 `memory-projection-source-not-quiescent`.
* 12:40:28: entity `math/construction-before-estimates` took 19,834 ms.
  Between 12:40:36 and 12:41:09, six other entity reads took 22,704–29,863 ms
  and their clients disconnected. Successful reads in the same interval also
  took 11,803–18,095 ms.
* After restart, the startup projection index reported 33 components, 90
  endpoints, two build attempts, and 23,889 ms at 12:47:03.
* At 12:48:11 a projection took 8,485 ms; the next at 12:48:22 took 35 ms.
  Evidence writes occurred in this interval. This is the rebuild cliff versus
  maintained-index lookup.
* At 12:48:34–12:48:36, the same session-id `limit=1` shape alternated among
  0–2 ms and 1,134–1,331 ms, consistent with semaphore/scan contention.
* The bounded probes above are independently present in the request journal at
  12:50:06–12:50:25, including the hyperedge cache repeat (2,915 ms then 6 ms).

## C. Native indexing status and the upstream case

There is no XTDB 2.1 configuration switch that creates a PostgreSQL-style
secondary index for these attributes. The relevant native work is explicitly
open:

* [#3663, “Maintaining user-specified secondary indices”](https://github.com/xtdb/xtdb/issues/3663)
  says XTDB currently approximates them with metadata on every column of each
  chunk/page. The issue calls this under-selective because it cannot identify the
  specific matching entities, and proposes letting users choose indexed columns
  and optimizing merge-task calculation. Status: open, `spec-reqd`.
* [#5637, “Text indexing”](https://github.com/xtdb/xtdb/issues/5637) is also open
  and its progress status is “not started.” It is still deciding capability level,
  requirements, Lucene scope, and data model.

The in-house work already supplies a strong design precedent and evidence packet:

* `futon1b_text.clj` embeds SQLite FTS5 in the serving JVM, with WAL and a
  rebuildable file beside the XTDB store (`futon1b_text.clj:1-21`,
  `futon1b_text.clj:36-60`).
* It uses the sidecar only to obtain candidate IDs, then point-fetches and
  re-checks every candidate against XTDB truth (`futon1b_text.clj:167-218`).
  Evidence is append-only, so refresh rides the append path with staleness bounded
  by one in-flight future.
* The D1 run indexed 94,430 evidence rows, passed a 10/10 scan oracle, and observed
  about four seconds of live-append staleness.
* The textprobe packet measured 131,807 histories. Aggregate ever-held posting
  inflation was only 1.028 for entities and 1.000 for evidence, but a rewrite-heavy
  tail reached p99 3.80 and max 90.5. Graph text had 788k postings; evidence prose
  1.45m; low-value HUD captures alone would add 2.19m. This argues for per-field
  inclusion/analyzers and store-truth re-check rather than one universal analyzer.
* `futon1bi` is the reusable extraction: a spec-driven, rebuildable candidate
  index with an oracle and freshness stamp. Its deferred work is operational
  wrapping/soak/tombstones, not the candidate/re-check core.

### What to propose upstream

A second isolated sidecar would solve today's attribute reads, but repetition of
the same architecture for text and scalar attributes is evidence that the missing
abstraction is native. The upstream proposal should be a **derived secondary-index
facility**, not one index format pretending text and scalar search are identical:

1. User-declared projections from tables/columns, including multivalued fields.
2. Log/transaction-driven maintenance, rebuild and compaction lifecycle, and an
   explicit indexed-through snapshot/watermark.
3. A candidate-ID contract integrated with XTDB's bitemporal resolver, so current
   and historical store truth remains authoritative.
4. Pluggable backends: hash/sorted/bitmap structures for scalar equality/range;
   Lucene or another inverted index for text positions, analysis, and ranking.
5. Per-field configuration and basic selectivity statistics for planning.

The new contribution to the existing #5637 packet is the scalar evidence: cold
bounded equality scans take roughly 2–3 seconds even for ten results; common
production type sweeps take 5–28 seconds; non-primary-key entity fallbacks reached
20–30 seconds; the same maintained result is 8–35 ms once indexed/cached. That
connects the text requirement to #3663 with an operationally observed workload,
rather than asking XTDB to special-case only full-text search.

## D. Options

Ratings refer to the five shapes from the request: (1) evidence tags/since, (2)
evidence author/session limit-one, (3) typed hyperedge sweeps, (4) memory
projection, (5) entity point/fallback.

| Option | Expected win by shape | Implementation cost | Staleness / consistency risk |
|---|---|---|---|
| Native XTDB secondary-index facility | 1 high with multivalue tag + time indexes; 2 high; 3 high; 4 medium (endpoint membership still needs a suitable multivalue index/materialization); 5 high | Very high and upstream-dependent; no existing 2.1 config. Requires engine/spec work and upgrade adoption. | Lowest if integrated with XTDB snapshots and bitemporal resolution. Until #3663 lands, unavailable rather than merely costly. |
| Generalize the existing SQLite sidecar | 1 high; 2 high; 3 high; 4 medium/high; 5 high | Medium. Reuse WAL, boot catch-up, candidate hydration, oracle, and freshness stamp. Add ordinary indexed tables plus tag/link tables; mutable graph docs require deletion/tombstone handling absent from append-only evidence POC. | Derived and rebuildable. Candidate + XTDB re-check prevents false positives from becoming wrong answers, but stale missing candidates can cause false negatives unless writes/retractions and indexed-through watermarks are handled. Evidence is the safest first slice. |
| Serving-JVM short-TTL cache | 1/2 medium only for identical hot requests; 3 high for repetitive mission sweeps (already observed 2.915 s → 8 ms); 4 low because it already has a maintained projection; 5 medium | Low. Existing bounded hyperedge cache is a template. Cache keys, maximum size, admission, and mutation invalidation need discipline. | TTL staleness or broad invalidation. Exact repeat workloads benefit; high-cardinality/ad-hoc filters do not. It masks scans and can shift the cold-start cliff rather than remove it. |
| futon1b maintained in-memory index maps | 1 high for tag/time candidate sets; 2 high; 3 high; 4 already high when not spuriously rebuilt; 5 high for name/external lookup | Medium. Build at boot and update synchronously on every write/retraction. Heap and boot cost grow with 400k edges/110k evidence; mutation coverage and process restarts need tests. | Can be exact for current state when mutation hooks are complete. Missed write paths cause false negatives. Historical queries must bypass. A global watermark check is safe but, as shape 4 shows, can destroy availability through needless rebuilds. |

## Recommended sequence

1. **Remove known amplification without a new index.** Make mission-scope callers
   use `include-total=false` when they do not consume an exact total, so the
   existing bounded cache is eligible. Push typed entity limits into XTDB. Consider
   reducing evidence's internal page size only after measuring match density; a
   smaller page helps selective filters but can add round trips for sparse tags.
2. **Repair projection coherence.** Give the memory projection a mutation-scoped
   generation (or table/type-specific revision), since memory-edge writes already
   synchronously refresh it. Do not compare it to the global node watermark on
   every read. Preserve the coherent startup/rebuild path and historical bypass.
3. **Generalize, do not duplicate, the SQLite precedent.** Keep one derived-index
   subsystem/file lifecycle and add B-tree tables for evidence scalar attributes
   plus a junction table for tags. Start there because evidence is append-only and
   the existing catch-up contract applies. Query candidates with a bound, point
   hydrate, and re-check XTDB. Add hyperedge/entity indexing only with explicit
   write, replacement, and retraction coverage plus a rebuild oracle.
4. **Use TTL caching only as a pressure valve.** It is justified for repeated
   mission sweeps and the live result proves its value, but it is not the
   attribute-index architecture.
5. **Engage upstream with a combined packet.** Add these plans and journal
   measurements to the textprobe findings, frame #5637 as one backend of the
   broader #3663 facility, and ask JUXT to validate the candidate-ID +
   snapshot-watermark contract before investing in an in-core implementation.

This sequence gives immediate futon1b wins, retains the proven “derived candidates,
store truth” safety boundary, and turns the apparent need for a second sidecar into
concrete requirements for a native XTDB2 facility.
