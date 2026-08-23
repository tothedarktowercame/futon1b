# TN-futon1b-gc-wedge-incident-2026-08-23

**Technical note — the :7073 store's four-day permit wedge and GC saturation,
2026-08-23.** Author: Claude (session 012zxpti, with Joe). Intended for
handoff: §5 lists the follow-ups that remain open, each with an owner-shaped
acceptance bar. Predecessors: `../TN-futon1b-memory-incident.md` (2026-07-22
brown-out), `../TN-futon1b-boot-incident-2026-08-13.md`. Discovery record:
`futon3c/holes/excursions/E-futon1b-gc-wedge.md` (ruling, histogram, stacks,
repair receipt).

## 1. Incident summary (times UTC)

`futon1b-zone.service` (PID 1082659, `-Xmx4g`, `migration-store-21`) had
been up ~4 days. Both `expensive-read` admission permits were held by two
`GET /api/alpha/evidence` reads that entered `with-expensive-read!` ~40 s
after boot and never returned — blocked in the pgwire socket read
(`sun.nio.ch.Net.poll` under `PgPreparedStatement.execute`). Every later
corpus read waited 3 s and shed with 503 `:expensive-read-busy`. Heap was
3.97/4 GB with all six G1 concurrent threads saturated; the live set was
~9.3 M Arrow `Field` objects — retained per-plan schema metadata from
thousands of distinct compiled query shapes.

Symptom to the rest of the stack: evidence reads timed out or were rejected;
point reads and writes still worked; `/health` (cheap) answered `{:ok true}`
throughout, which is why nothing alerted.

## 2. Root cause

Two independent defects, one shape:

1. **Unbounded query shapes.** `fetch-newest-projected-page` embedded each
   keyset cursor as literals in a fresh XTQL form, so every page compiled a
   new plan; XTDB retained each plan's Arrow field tree. With the
   `evidence/subject` union projected on every page, and post-filtered reads
   (`subject-type`/`subject-id`, `tags`) looping page after page inside one
   HTTP request (157,336 `type=coordination` rows for the wedged shape), the
   count of shapes grew without bound.
2. **No deadline on any store read.** `xt/q` → `plan-q` hands next.jdbc a
   fixed option map with no `:timeout`, and a pgwire socket read does not
   honour thread interruption. Once a read stalled, the `finally` that
   releases the permit could never run.

Not the cause: semaphore leak (permits were held by real threads);
`MALLOC_ARENA_MAX` (set to 2); `-Xmx` too small for the corpus (baseline
live set is ~976 MB — README "Memory requirements").

## 3. Repair (landed, live)

futon1b `4cd17bc`, `8aba53c`, `bf875b0`; regression
`clojure -M:node -m test-evidence-deadline`.

| # | Change | Where |
|---|--------|-------|
| 1 | Page query is one `(fn [p-type … p-cursor-at p-cursor-id p-limit] …)` XTQL form; filter values, cursor, limit are parameters (two variants: first page / after-cursor × present filters). `evidence/subject` projected only for subject filters. `/count` scan and hydration use the same path. | `futon1b_evidence.clj` `page-query`, `pushdown-params` |
| 2 | `timed-q`: node's own JDBC connection, `setNetworkTimeout` (timeout+5 s) + `setQueryTimeout`, 60 s default; expiry → HTTP **504** `:query-deadline-exceeded`, permit released. | `futon1b_xt.clj` |
| 3 | Whole-request scan ceiling 20,000 projected rows; past it the response carries `:incomplete true`, `:scan/max`, `:next-cursor`. Every response carries `:scanned`. | `bounded-window` |
| 4 | `/api/alpha/hyperedges` `limit>1000` → 400 (validated before taking a permit); only windows ≤1000 cached. | `futon1b_server.clj`, `futon1b_graph.clj` |
| 5 | Holder registry + `[futon1b-expensive-read] start/end` log lines; cheap `/health` reports `:permits/available`, `:permits/waiters`, `:holders` (id, sanitized shape, trace-id, thread, age-ms), `:oldest-holder-ms`, `:stats`, `:heap`, `:metaspace-used-mb`, `:gc`. | `futon1b_server.clj` |

Contract changes are in `../API-CONTRACT.md` §3 "Futon1b bounded-page
extension".

**Measured facts worth keeping:**
- XTDB 2.1.0 pgwire **does not act on pgjdbc's cancel** (`setQueryTimeout`).
  The effective deadline is the network timeout: 65 s for the 60 s default.
- Once the client socket drops, the server-side scan stops within ~3 s
  (process CPU → 0, no `xtdb.operator` threads).
- Post-repair, across 18 distinct-cursor pages: Arrow `Field` 1.51 M flat
  (was 9.3 M), `PageIndexKey` 361 k constant, post-GC heap **993 MB**.
- `DynamicClassLoader` still creeps ~130 per request (53,212 → 56,253 over
  18 requests); Metaspace 471 MB used at that point.

## 4. The restart — what actually happened

The repair needed a restart (a code reload cannot free two stuck JDBC calls).
While taking the before-census required by
`scripts/restart-futon1b-detached.sh` (`GET /health?deep=true`, which runs a
`safe-q` count over seven tables), the old JVM hit `OutOfMemoryError: Java
heap space` at 11:06:02 and `-XX:+ExitOnOutOfMemoryError` ended it. That
request was almost certainly the last straw on a 3.97/4 GB heap. systemd
restarted the unit at 11:06:13 (PID 3639998) against source already on disk
(`4cd17bc`, written 11:00:50), so the live service runs the repair;
`8aba53c` (hyperedge validation order, metaspace field) is on disk but not
live until the next restart.

Post-restart gate: `/health` up after ~35 s; `scripts/fts-status.py` store
159,492 = index 159,492 (delta +0); fresh evidence stamped 11:09–11:10;
permits turning over under live traffic (holders < 10 s old).

**Lesson:** do not issue `/health?deep=true` against a JVM above ~85 % heap.
Read pressure from the cheap `/health` fields added in this repair.

## 5. Open follow-ups

Each is one handoff. None is blocking; (a) is the one that can silently
mis-serve callers today.

### (a) Callers must honour `:incomplete` — then push subject filters down

The shape that wedged, `?type=coordination&subject-type=portfolio&subject-id=
global&include-ephemeral=false&limit=100`, now returns in ~15 s with
`:count 0 :scanned 20000 :incomplete true` plus a cursor. A caller that
treats `count < limit` as end-of-corpus will read "no such evidence".

- **Step 1 (futon3c):** find the issuer (`Futon1bBackend` / the portfolio
  poller) and make it loop on `:next-cursor` while `:incomplete` is true, with
  its own request budget. Acceptance: a test that feeds a stubbed
  `:incomplete` page and asserts continuation.
- **Step 2 (futon1b):** push `subject-type`/`subject-id` into the XTQL
  `where` as parameters on the nested map (e.g. `(= (. evidence/subject
  ref/type) p-subject-type)` — verify the accessor form XTDB 2.1.0 accepts;
  `evidence/subject` is a union, so test a doc whose subject is absent/non-map).
  Then `requires-post-filtering?` drops those keys and the scan ceiling is
  rarely reached. Acceptance: the wedged shape answers < 2 s with
  `:scanned` ≤ `limit`; `test-evidence-deadline` subject checks still pass.
- Same treatment is possible for `pattern-id` (scalar column); `tags`
  (set containment) may not be expressible and stays post-filtered.

### (b) Restart script cannot run in exactly the wedged state

`restart-futon1b-detached.sh` PRE-CHECK 2 aborts when `/health?deep=true`
does not answer within 20 s — which is precisely the condition a wedge
produces, and asking harder is what killed the JVM (§4). Options, Joe's call:

- an explicit opt-in env (`FUTON1B_RESTART_ALLOW_NO_CENSUS=1`) that records
  "before-census unavailable" loudly in the receipt, skips the shrink diff,
  and still runs PRE-CHECK 1 (log backlog), the PID-changed check, and the
  write-path check; or
- take the before-census from a source that does not need the JVM (the
  sidecar's `fts-status` store-row count covers `:evidence` only; the XTDB
  log/object store would need a new reader).

Acceptance: the script has a documented path for "store wedged, census
unavailable" that does not abort and does not touch the heap.

### (c) Metaspace / generated-class growth

`DynamicClassLoader` grows ~130 per request even with stable query text.
Likely sources: hydration's `IN (?,?,…)` has a distinct text per id-count
(up to 1,000 shapes), and XTDB compiles expression fns per plan. This is the
next thing that would force a restart.

- **Discovery first:** sample `jcmd <pid> GC.class_histogram | grep
  DynamicClassLoader` and `:metaspace-used-mb` from `/health` hourly for a
  day; correlate with `[futon1b-expensive-read]` shapes in the journal.
  Decide whether it plateaus (plan cache bounded) or is monotonic.
- **If monotonic:** pad hydration `IN` lists to fixed sizes (e.g. 16/64/256/
  1000 with NULL fill) so the text is one of four shapes; then re-measure.
- Alert threshold to add to vitality: metaspace > 1 GB or monotonic over
  6 h.

### (d) Migrate the remaining `safe-q` readers to `timed-q`

Still un-deadlined: `/api/alpha/entities`, `/entities/latest`,
`/hyperedges`, `/relations`, `/sessions`, `/health?deep=true`, graph
point-reads. On 2026-08-23 post-restart `/health` showed both permits held by
`/entities` and `/entities/latest` (ages 10–14 s; they returned). One route
per handoff; acceptance per route: a stalled read returns 504 within 65 s
and the holder entry disappears.

### (e) Vitality should poll cheap `/health`, not an HTML consumer

Alert on: `:oldest-holder-ms` > 60,000; both permits held for > 60 s;
`:stats :rejected` rising for 5 min; post-GC heap > 85 %; G1 concurrent CPU
continuously busy. All of these were true for four days with nothing firing.

## 6. How to tell the three failure modes apart next time

| Signal (cheap `/health`) | Overload | Hung JDBC | Leaked permit |
|---|---|---|---|
| `:holders` | many short-lived | 1–2 entries, age growing unbounded | `:permits/available` < 2 with `:holders []` |
| `:stats :timed-out` | ~0 | rising (post-repair) | 0 |
| `:stats :rejected` | rising | rising | rising |
| heap after GC | normal | may be high (retained plans) | normal |

A leaked permit (third column) has not been observed; the 2026-08-23 event
was the second column before the repair, and would now show as the second
column with `:timed-out` rising and the permits returning.
