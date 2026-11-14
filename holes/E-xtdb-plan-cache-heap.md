# E-xtdb-plan-cache-heap — Zone's 4g heap filled with cached query plans

Opened 2026-10-28 (claude, REPL-slowness session). Status: mechanism
identified, size attribution not yet measured. Handoff below.

## Symptom

Agency REPL turns slowed on 2026-10-28. Cause: futon1b-zone (:7073) was in a
full-GC spiral after 8 days' uptime.

- `jstat -gcutil`: old gen 99.5%, ~2 full GCs/s, about 0.8 s of GC per second;
  28,963 full GCs (11,800 s) over the process lifetime.
- `/health`: heap 3,932 / 4,096 MB. All sidecars serving
  (`:evidence-text :serving? true`, 0 failed) — this is **not** the 10-14
  reconcile failure.
- Request log: 688 x 504 and 2,559 requests over 3 s in the last hour
  (4,505 total); 2,363 x 504 in the last 6 h, 3,040 in 24 h. The 504s were
  mostly `POST /api/alpha/evidence` failing with "query permit unavailable
  after 5000ms"; Agency waits on those appends during a turn.

Restarted 23:37Z with the heap raised to 8g (systemd drop-in
`~/.config/systemd/user/futon1b-zone.service.d/heap.conf`). Up in 106 s; 0
full GCs afterwards. **8g buys time; it does not change what fills the heap.**

## What filled the heap

Class histogram just before the restart
(`~/dumps/futon1b/histo-2026-10-28-pre-restart.txt`; heap dump alongside it,
`futon1b-2026-10-28-pre-restart.hprof.gz`, 1.4 GB compressed):

| instances | bytes | class |
|---|---|---|
| 4.1M | 954 MB | `byte[]` |
| 12.1M | 499 MB | `Object[]` |
| 10.1M | 322 MB | `arrow…pojo.FieldType` |
| 10.6M | 255 MB | `arrow…pojo.Field` (+ 255 MB of `UnmodifiableRandomAccessList`, 267 MB `ArrayList`) |
| 4.3M | 139 MB | `clojure.lang.Symbol` |
| 1.07M | 26 MB | `xtdb.metadata.PageMetadata$PageIndexKey` |
| 205k | 5 MB | `xtdb.operator.table$emit_rows_table…write_param_BANG_` closures |
| **4,102** | | `caffeine…BoundedLocalManualCache` |

Arrow `Field` trees and Clojure symbols are what XTDB's query plans are made of.
The 4,102 Caffeine caches are the giveaway: see below.

## Mechanism (XTDB 2.1.0, read from the jar)

- `xtdb.query/->query-source`: `plan-cache` is Caffeine, `maximumSize 4096`.
  Key = `[parsed-query (select-keys opts [:default-db :decorrelate? :explain?
  :explain-analyze? :arg-fields]) + :table-info]`.
- Each cached plan carries its **own** `emit-cache` (Caffeine,
  `maximumSize 16`), keyed on `:scan-fields` — the Arrow `Field`s of every
  column the query scans — plus stats. So one plan can hold up to 16 compiled
  operator trees, each with its own copy of the scan schema.
- `xtdb.query.SqlParser/parseStatement` caches parse trees by SQL string,
  also `maximumSize 4096`. The plan cache key holds the parse tree object, so
  a string evicted from the parse cache and seen again gets a **new** plan
  entry while the old one waits for eviction.

So 4,102 Caffeine caches = a plan cache at its 4096 cap, plus one emit cache
per plan. Both caches are bounded; the problem is that the bound, at the size
of futon1b's plans (wide `hyperedges`/`entities`/`evidence` schemas), is
bigger than a 4g heap can hold alongside the working set. The heap fills
when futon1b sends more than ~4096 distinct SQL strings, which it does:

### Where futon1b makes distinct SQL strings

Every distinct `IN (?, …)` arity is a distinct string, and so a distinct plan:

- `futon1b_evidence.clj` `rows-by-ids` — `SELECT * FROM evidence[ FOR …] WHERE
  _id IN (?×n)` for n above `fxt/equality-hydrate-max-ids`, up to 1,000 (HTTP
  page cap). Times every temporal `FOR` variant from `temporal-specs-for`.
- `futon1b_xt.clj:256` `hydrate-by-ids` — `SELECT * FROM <table> WHERE _id IN
  (?×chunk)`; the last chunk of every call has a new arity.
- `futon1b_graph.clj:1200` — `SELECT <cols> FROM hyperedges WHERE _id IN
  (?×chunk)`; arity × every distinct column list.
- `futon1b_text.clj:1035` — `SELECT * FROM evidence WHERE _id IN (?×n)`.

(The `futon1b_text`/`scopeindex` SQL against `ev_attr`, `ev_tags`, `ev_doc`
etc. is SQLite, not XTDB, and does not count.)

The 205k `emit_rows_table…write_param` closures fit: XTDB plans a
parameter list as a VALUES table, one closure set per parameter.

## What is NOT yet shown

1. **Retained size of the plan cache.** The histogram shows the cache was
   full; it does not show that the 4096 plans are what holds ~3.5 GB. The
   heap dump answers this: dominator tree / retained size under
   `xtdb.query.QuerySource` → `plan-cache`.
2. **Which strings fill it.** Top shapes in a full cache. Probe below; run it
   when `:plans` is near 4096 (a day or two of uptime).
3. Whether `PageMetadata$PageIndexKey` (1.07M) grows independently
   (`xtdb.metadata` cache-size is 128 entries, so probably bounded — check).

## Instruments left running

- `~/notes/futon1b-heap/plan-cache-probe.clj` — plans, emit entries, heap.
  `scripts/futon1b-eval.sh -f <file>`.
- `~/notes/futon1b-heap/plan-key-probe.clj` — which key component varies,
  and the top query shapes. At 2 min uptime: 63 plans, 63 distinct parsed
  queries, 1 distinct `:table-info`, so far the variation is all in the SQL text.
- `futon1b-heap-sampler.service` (systemd user unit, `--collect`) appends a
  row every 10 min to `~/notes/futon1b-heap/samples.tsv`: plans, emit
  entries, heap used, old-gen %, full GCs. Stop with
  `systemctl --user stop futon1b-heap-sampler`. If the
  plan count reaches ~4096 while the heap climbs in step, that confirms the
  mechanism without the dump.

## Handoff (split, per CLAUDE.md — one behaviour each, review between)

**H1 (discovery, no code change).** From the heap dump, report retained size
under the plan cache and under the parse cache, the top 10 retained-size
dominators overall, and per-plan average retained size. Tool: Eclipse MAT
headless (`ParseHeapDump.sh … org.eclipse.mat.api:suspects`) or any
dominator-capable analyser; decompress first (~5 GB). Do not run against the
live JVM. Acceptance: a table of retained MB by owner, with the method used.
Also run `plan-key-probe.clj` once the sampler shows `plans` > 3000 and
paste the top shapes.

**H2 (implementation, after H1 confirms).** Collapse IN-list arities in the
four call sites above to a fixed set of sizes (e.g. 1, 8, 40, 200, 1000),
padding with a repeated last id (a duplicate id in `IN` cannot add rows, and
the callers already rebuild order through `by-id`). One shared helper in
`futon1b_xt.clj`, used by all four. Acceptance: a unit test that the padded
query returns exactly the unpadded rows, including the duplicate-id case;
a test that the helper only ever emits the fixed arities; on Zone after a day,
`plans` from the probe well under 4096. Gates: clj-kondo, check-parens,
the futon1b test suite (note `^:slow` exclusion — see
`slow-tagged-tests-hide`).

**H3 (only if H2 is not enough).** Options, in order of preference: (a) an
`_id = ANY(?)` single-array-parameter form, if XTDB 2.1 plans it as a point
lookup and not a scan (the 2026-09-27 measurement found `IN` scans; test
`ANY` the same way first); (b) a periodic `(.invalidateAll plan-cache)` from
futon1b, which trades plan-compile CPU for heap; (c) keeping 8g as the
standing value and updating README "Memory requirements" to say why.

Whatever lands: if the heap returns to 4g, delete the drop-in; if 8g stays,
fold it into `deps.edn :server` and the README table instead of a drop-in.

## After the restart: boot catch-up never finished (fixed, cfc7c68)

The restart fixed the heap but not the latency. `/health` showed
`:evidence-text {:serving? false :why :not-reconciled-since-boot}` for 20
minutes: the boot `catch-up!` waited for an empty page, each page scan took
~30 s, and live appends landed during every scan, so the checkpoint trailed
the head by ~45 s and never drained. `reconcile!` (which runs after it on the
boot thread) never started, and every evidence read scanned the store.

Fix: `catch-up!` also drains after a short page (same proof as an empty one —
the scan reached the end of the store as of a snapshot taken after the basis).
Regression test `catch-up-drains-under-live-appends` in
`test_evidence_tag_index.clj` appends a doc during every scan; without the fix
it hits its 20-scan cap.

Getting the *running* boot loop out needed one more step, since a reload does
not replace a loop already executing the old fn: after
`(require 'futon1b-text :reload)`, `scan-after` was wrapped to return `[]`
exactly once and restore itself. The boot loop drained (954 indexed), and
`reconcile!` ran: 722,810 store docs, 2 missing, indexed. Sound because the
append hook had indexed every post-boot append (368/368, 0 failed) and
`reconcile!` compares every id regardless. Evidence POST latency afterwards:
median 84 ms, p90 221 ms (32 writes), against 5 s timeouts before.

Any future restart under load would have hit this; it is not specific to
the heap problem.

## Also changed on 2026-10-28

`scripts/restart-futon1b-detached.sh` took the old PID from
`pgrep -f '-m futon1b-server'`, which also matches minus40's scratch server
(:7093). The receipt said `old-pid=1395` (minus40) for a restart of 24011, so
its PID-change check compared the wrong process. It now reads the unit's
`MainPID`.

## Completion criteria

- H1: retained size of the plan cache and parse cache measured from the heap dump, with a table of retained MB by owner and per-plan average — check: a report under `/home/joe/notes/futon1b-heap/` or `/home/joe/code/futon1b/holes/` naming the method (Eclipse MAT or equivalent) **Not met.**
- H2: the four IN-list call sites (futon1b_evidence rows-by-ids, futon1b_xt hydrate-by-ids, futon1b_graph, futon1b_text) collapse arities to a fixed set via one shared helper, with a unit test that padded queries return exactly the unpadded rows — check: the helper in `/home/joe/code/futon1b/futon1b_xt.clj` and its tests **Not met.**
- After H2, a day of sampler data on Zone shows plan count well under 4096 with heap stable — check: `/home/joe/notes/futon1b-heap/samples.tsv` plans column **Not started.**
- Heap disposition settled: either the 8g drop-in is deleted (heap back to 4g) or 8g is folded into deps.edn :server and the README memory table — check: `~/.config/systemd/user/futon1b-zone.service.d/heap.conf` absent or README updated **Not met.**
- The boot catch-up fix holds under live appends — check: `catch-up-drains-under-live-appends` at `/home/joe/code/futon1b/test_evidence_tag_index.clj:179` passes **Met.**

## 2026-11-14: confirmed at 8g (claude-13, read-only)

Joe asked for the cause before any move to 16g. Same mechanism, now measured
over a full run at 8g (pid 3382662, up since 11-11 02:55Z):

- The sampler (`~/notes/futon1b-heap/samples.tsv`, every 10 min) shows the
  plan cache climbing steadily to its 4096 cap: 569 plans at 11-11 17:44Z,
  1,587 at 11-12 05:44, 2,813 at 11-13 17:44, 3,704 at 11-13 23:45 (the bulk
  dedup merges and retractions ran that evening), 4,026 at 11-14 15:05.
  Emit entries 10,281. Heap 7.8 GB; old gen 99.3%.
- Full GCs: 0 until plans passed ~2,800; 519 by 11-13 23:45; 6,805 by
  11-14 11:45; 12,081 by 15:05 (about 400 per 10 min). Entity POSTs then
  took 30 s+ and the futon3c watcher's writes timed out.
- Class histogram 11-14 ~15:20Z
  (`~/storage/futon1b-heap-2026-11-14/histo.txt`): 19.0M arrow `Field`
  (455 MB) + 11.8M `FieldType` (378 MB) + 19.0M `UnmodifiableRandomAccessList`
  (455 MB), 14.7M `Symbol` (472 MB), 7.0M `ProjectionSpec$Identity`,
  777k `emit_rows_table…write_param` closures: about twice the 10-28 counts at
  4g, i.e. the cache simply filled the bigger heap.

So 8g is not too small for futon1b's data; it is too small for a full plan
cache of this size (roughly 1.7 MB per plan at 4,000 plans). 16g would move
the cliff, not remove it: the cache would still reach 4096 plans and grow
again with any new SQL shapes. H2 above (fixed IN-list arities in the four
call sites) has not been done and remains the fix; H3(b) (periodic
`invalidateAll`) is the stop-gap. Rationale for 16g, if ever: only if, after
H2, the sampler shows plans staying well under 4096 while heap use still
climbs.

## 2026-11-14 ~15:50Z: measured live — the plan cache holds 4.2 GB, and most plans come from one as-of query (Claude Code seat, taking over claude-1's remit)

Joe chose to diagnose on the failing JVM before restarting it. At the start
futon1b (pid 3382662) was refusing connections on :7073 (curl exit 56), all
CPU was in GC threads, old gen stayed at 99.5% after each full GC (12,988 full
GCs, 12,550 s of full-GC time), plans 4,044.

**Heap dump** (before any change, for H1's dominator analysis later; no MAT on
Zone): `~/storage/futon1b-heap-2026-11-14/futon1b-live-2026-11-14T1550Z.hprof`
(13.1 GB, 78 s).

**Retained size of the plan cache, measured by removing it** (H1's question,
answered without the dump): `(.invalidateAll plan-cache)` then `System/gc`.
Old gen 7,906 MB → 3,665 MB; used 7,953 → 3,677 MB; 2 s. So the 4,044 plans
held about 4.2 GB, 54% of the heap, roughly 1.05 MB per plan, and futon1b's
working set without them is about 3.7 GB. Afterwards :7073 answered
`evidence?limit=1` in 10–15 ms. Record:
`~/notes/futon1b-heap/invalidate-experiment-2026-11-14.edn`.

**Which queries fill it.** `plan-key-probe.clj`: 3,921 of 4,044 plans have
zero arg-fields, so they are not the parameterised `_id IN (?…)` reads H2
targets. XTDB's parse caches (`xtdb.query.SqlParser` static
`singleQueryCache` 80 + `multiQueryCache` 2,857 strings, dumped to
`~/notes/futon1b-heap/sql-strings-2026-11-14T1555Z.txt`) by shape:

| strings | shape |
|---|---|
| 1,862 | XTQL hyperedges-by-endpoint, `fn [p-target p-limit p-type]`, with `:for-valid-time (at #xt/instant "…")` and `:for-system-time (at …)` **inlined**, valid = system instant in all 1,862 |
| 434 | `SELECT … FROM hyperedges WHERE _id IN ($1…)` (H2 site) |
| 192 | `SELECT … FROM entities WHERE _id IN (…)` |
| ~130 | transaction guards: `ASSERT [NOT EXISTS] … arxana_* / entities` with `IN`, `OR` chains or `VALUES` of varying length (migration/Arxana bulk loads) |
| 20 | `SELECT * FROM evidence WHERE _id IN (…)` |
| ~300 | everything else |

The 1,862 instants spread evenly over the whole run, 20–70 an hour from
11-11 02Z to 11-14 15Z, i.e. a steady per-turn leak, not the 11-13 bulk work.
(Parse-cache shares stand in for plan-cache shares: plan keys print as
`[N]`, so plans cannot be attributed to strings directly.)

**Where the as-of query comes from.**
- futon1b: `futon1b_graph.clj` `temporal-filter` / `hyperedge-from`
  (l.1123–1134) put the as-of instant into the query form as a literal, the
  practice `futon1b_xt.clj` `pq`'s docstring forbids. Six callers. Only
  as-of reads take this path (current reads go to the sidecar).
- futon3c: `agency/pattern_card_provider.clj` `default-card-query`, called
  by `refresh-cards!` with `system-as-of (str (Instant/now))` as both
  `valid-as-of` and `system-as-of`: one query for the session's
  `:pattern-card/selection` edges, plus one `:act/withdrawal` query per
  selection, on every card refresh. Present since b9924b391 (2026-09-28),
  so it probably also drove the 10-28 fill at 4g.

**Consequence for the plan.** H2 (fixed IN-list arities) addresses about a
quarter of the distinct strings. The main fix is to make the as-of instants
parameters in `hyperedge-from` (`(at p-valid-as-of)` with the instant passed
as an argument, if XTDB 2.1 accepts a parameter there; test in a test JVM
first), so every as-of read of one shape shares one plan. H2 then removes
most of the rest. The 16 GB question does not arise: the working set without
plans is ~3.7 GB.

**State after the stop-gap.** Plans restart from 0 (23 a minute later). At the
observed rate the cache reaches ~2,800 plans, where full GCs began on this
run, in about two to three days; repeat `invalidateAll` (or restart) before
then if the fix has not landed.
