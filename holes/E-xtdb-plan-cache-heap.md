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
