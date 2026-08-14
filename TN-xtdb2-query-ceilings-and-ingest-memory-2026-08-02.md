# TN — XTDB 2.1.0: primary-key batch fetch, and an ingest memory profile

**Status: measurement, 2026-08-02 (rewritten end-of-day).** Workload-evidence note
for the JUXT / James Henderson call (Wed 2026-08-05). Companion to
`TN-xtdb-derived-secondary-index.md` and `holes/SPIKE-attribute-index-2026-07-26.md`;
staged context in
`futon7/data/outbox/staged/2026-07-26--james-henderson--xtdb-3663--jx5637/`.

Everything here came out of repairing a real outage in our store today, not from a
benchmark exercise. An earlier draft of this note led with a JVM method-size limit;
that turned out to be the least interesting of the findings and has been demoted to
§3. **The lead finding is §1, and the most useful finding is probably §2.**

Environments are kept explicit throughout, because §2 is precisely about how much
that matters:

- **(L)** the live futon1b store over HTTP — ~94k docs; the `code/v05/var` type alone
  is **34,480** docs; `xt/id` strings 60–70 chars.
- **(F6)** throwaway in-process node, 6,000 synthetic docs, ids ~26 chars.
- **(F40)** throwaway in-process node, 40,000 synthetic docs, ids shaped like the
  live ones (60–70 chars).

Repro scripts in this repo, all `clojure -M:node -m <ns>`: `probe_in_predicate.clj`,
`probe_id_batch_planning.clj`, `scale_probe_bisect.clj`.

---

## 1. Fetching N documents by primary key: XTQL cannot express it, and the workaround is not index-backed

The task is mundane: a projection pass has selected N document ids, and we now want
the full bodies in as few round trips as possible.

**No XTQL spelling works.** On **(F6)**, xtdb 2.1.0:

| surface | form | result |
|---|---|---|
| XTQL | `(in xt/id [...])` | `in not applicable to types utf8 and list` |
| XTQL | `(contains? #{...} xt/id)` | `contains? not applicable to types set and utf8` |
| XTQL | `(= xt/id (any [...]))` | `any not applicable to types list` |
| XTQL | `(or (= xt/id a) (= xt/id b) …)` | accepted — but see below |
| SQL | `SELECT _id FROM hyperedges WHERE _id IN (?, ?, ?)` | **works** |

**And the one accepted XTQL form is dramatically the wrong shape.** On **(F40)**,
fetching 50 documents by primary key three ways:

| shape | time |
|---|---:|
| A — 50 sequential point lookups | 6393 ms (127.9 ms/row) |
| B — one 50-clause XTQL `or` | 2196 ms |
| **C — one SQL `_id IN (?…)`** | **400 ms** |

SQL `IN` is **16× faster than point lookups** and **5.5× faster than the XTQL
disjunction**. It is clearly planned as index lookups; the disjunction is clearly not.

On **(L)** the gap is far worse than any fixture suggested. A single `(= xt/id x)`
costs ~50 ms — properly indexed. But **one 50-clause `or` of those same equalities
costs ~40 seconds**:

| window | via 50-clause `or` chunks | via per-doc point lookups |
|---|---:|---:|
| 50 rows | **40.4 s** | 17.9 s |
| 100 rows | **76.8 s** | 16.9 s |

Note the shapes as well as the magnitudes: the disjunction scales *linearly in
chunks* (~40 s each), while per-doc lookups go slightly **down** from 50 to 100 rows.
That is the signature of one form hitting the index and the other scanning.

A thread dump taken while one such request was in flight
(`holes/jstack-hydration-slowness-2026-08-02.txt`) shows it `runnable` inside
`next.jdbc.result_set/reduce_stmt` with **15.6 s CPU over 1288 s elapsed** — grinding,
not blocked.

**Questions for JUXT.**
1. Is a list-membership predicate planned for XTQL, or is dropping to the SQL surface
   the intended answer for id-set hydration? Our port to XTDB 2 was motivated by
   wanting to use XTQL, so "use SQL for this shape" is a real ergonomic cost.
2. Is a disjunction of primary-key equalities expected to lose the index? If that is
   inherent to how `or` is planned, it would be worth documenting — it is a natural
   thing to write and the penalty is three orders of magnitude on our data.
3. Is ~50 ms the expected cost of a single primary-key point lookup at ~94k docs?

## 2. Fixture size inverted the answer — twice, in opposite directions

This is the finding we would most want to pass on, because it cost us a day.

The same comparison — batched disjunction versus per-document lookups — gives
**opposite** answers depending on corpus size:

| environment | batched `or` | per-doc lookups | verdict |
|---|---|---|---|
| **(F6)** 6,000 docs | 5,000 rows in 15.5 s | — | batched looks ~3× **better** |
| **(F40)** 40,000 docs | 50 rows in 2.2 s | 50 rows in 6.4 s | batched still looks **better** |
| **(L)** 34,480-doc type | 50 rows in **40.4 s** | 50 rows in **17.9 s** | batched is **2.3× worse** |

We shipped the batched form on the strength of a 6,000-doc fixture. On live data it
was roughly **16× slower** than the code it replaced, and because it held an
admission permit for its whole duration it starved every other reader. We reverted it.

Note that **(F40)** — 40k docs, live-length ids, i.e. deliberately sized to the live
type — *still* did not reproduce the effect. Whatever makes the disjunction
catastrophic on **(L)** is not corpus size alone: candidates are real document
shape/width, bitemporal history depth, index state after sustained writes, or
concurrent load. We have not isolated it, and would welcome a steer on what to
instrument.

The practical rule we have adopted: **for this workload, offline measurement is not
merely imprecise, it can invert the ordering.** Only the live store is authoritative.

## 3. The disjunction also has a hard compile ceiling (previously the headline)

Secondary to §1, but real. On **(F6)**, one `or` clause per id:

| clauses | result |
|---:|---|
| 50, 100 | ok |
| 250 | `Syntax error compiling reify* at (0:0)` |
| 500 – 5000 | `xtdb.error.Fault` |

The `reify*` signature indicates the query expression is compiled to bytecode and the
generated method overruns the JVM's 64 KB limit. The ceiling is on **expression size,
not clause count**, so it moves with id length — our live ids are 2–3× the length of
the probe's.

One caution for other users: above the ceiling the error text varies, and in one run
it happened to match a tolerant `catch` in *our own* query wrapper, which converted it
to an empty result — HTTP 200 with silent data loss. That was our bug, not XTDB's, but
the variability is worth knowing about.

## 4. SQL `IN` in production: a 4× improvement, short of what the fixture promised

We put SQL `IN` on the evidence hydration path (replacing an N+1 of point reads) and
measured it live.

- **Before:** evidence scans up to **43.7 s**.
- **After:** **9.5 – 12.1 s**.
- On a 1,200-doc fixture, hydrating **1,000 ids in one `IN`** took **911 ms**, and the
  rows were verified **identical** to the N+1 oracle they replaced.

So SQL `IN` is a genuine ~4× win on live data and correct. But the fixture predicted
sub-second and live delivers ten seconds — §2 again. Part of the gap is that our page
size stays at 1,000 rows for essentially all real requests, so live `IN` lists are 20×
the size of the ones in §1's measurement; we have not yet separated that from any
non-linearity in `IN` itself.

## 5. Sustained ingest: reclaim-bound, and it did not recover

Offered as a workload profile rather than a bug report.

A watcher-cache invalidation re-enqueued a large document backlog, producing on **(L)**:

- **~245 `POST /api/alpha/hyperedge` per minute, sustained ~90 minutes** (~10k writes),
  each a small hyperedge doc.
- RSS **2.5 GB** at boot → 4.0 GB → **10.1 GB** at 87 minutes.
- cgroup `memory.high` 10 GB, `memory.max` 12 GB; `memory.current` pinned at **10 GB**.
- `memory.events`: `high = 18,538,302`. **`oom_kill = 0`** — throttled, never killed.
- `memory.pressure`: `full avg10=66.42 avg60=68.60 avg300=63.93` — roughly **two-thirds
  of wall-clock** stalled in reclaim.
- A single hyperedge POST degraded from ~230 ms to **10,446 ms**.

The shape worth discussing: the node did not fail, it degraded into a reclaim-bound
state it could not leave once the write burst pushed the working set past
`memory.high` — and it stayed there after the writes stopped. **What governs that
working set for a write-heavy small-document workload, and is there a supported way to
bound or shed it?**

**Correction to the previous draft.** That draft said reads "became unavailable behind
our own admission control", implying backpressure working as designed. That was wrong
and the real story is worth telling as a caution, because it is a trap around *any*
database:

We wrap expensive reads in a 2-permit semaphore, and `safe-q` wraps every query in a
second, inner 4-permit semaphore. The outer one was constructed **fair** but acquired
with Java's **untimed** `tryAcquire()`, which explicitly barges past the FIFO queue —
so the fairness flag was inert and there was no queueing at all: a scan arriving while
both permits were held failed instantly rather than waiting 200 ms. Separately, two of
our own measurement requests — the pathological §1 disjunctions — held both permits for
20+ minutes, at which point every other read shed. Diagnosing that as "the database is
unavailable" was wrong twice over. None of it was XTDB's behaviour.

---

## Caveats and provenance

- **(L)**, **(F6)** and **(F40)** are not comparable to each other; §2 is the whole
  point of saying so.
- §3's ceiling is about generated-expression size and should transfer between
  environments; its *timings* will not.
- §5 is one observation of one workload on one box (30 GB RAM, store cgroup capped at
  12 GB) with our coordination JVM alongside.
- §1 was verified against 2.1.0 specifically on 2026-08-02. If a membership predicate
  has landed since, that finding is stale — worth re-running `probe_in_predicate.clj`
  before the call.
- Numbers in §1 (F40), §3 and §4 (fixture) are reproducible from the scripts named
  above. Numbers in §1 (L), §4 (live) and §5 come from the futon1b request journal and
  systemd/cgroup counters, and are not reproducible on demand without recreating the
  load.
