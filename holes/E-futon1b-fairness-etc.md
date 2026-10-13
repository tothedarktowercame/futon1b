# E-futon1b-fairness-etc — admission fairness under mixed read workloads

**Status:** OPEN — observation and design directions recorded; no Futon1b
admission change has been selected or implemented.

**Opened:** 2026-10-13, at Joe's request after bounded War Machine CI met a
live `503 :expensive-read-busy` while substrate-2 itself remained healthy.

## 1. The incident

Futon2 CI's `run4-draft-mission-test` uses production mission discovery and
therefore reads mission entities from substrate-2.  At 19:56 UTC that read was
reported by Futon2 as `:substrate-unreachable`.  Direct inspection later showed
that description was false:

- `GET :7072/health` returned 200 with `:node-open? true`;
- `GET :7073/api/alpha/entities?type=mission&limit=1` returned
  `503 {:error :expensive-read-busy :retry-after-seconds 1}`;
- all four expensive-read permits were occupied by evidence reads, with two
  waiters;
- holders observed in the journal commonly lasted 10–50 seconds, including
  one evidence cursor read lasting 62 seconds;
- ten subsequent mission probes produced nine 200s and one 503.  Successful
  mission reads were normally about 30–50 ms; the rejected probe waited about
  3.4 seconds before admission shed it.

So this was not an unavailable substrate.  It was a short operational read
competing with long corpus reads in one admission class, followed by a client
that treated a typed overload response as permanent unreachability.

## 2. Immediate client repair

Futon2 commit `81da537c0` retries only the explicit
`503 :expensive-read-busy` response, honors `:retry-after-seconds`, and keeps a
bounded attempt count.  Other 503 responses still fail immediately.  This is
appropriate client behavior for the server contract, but it is not the full
Futon1b repair: clients should not need synchronized retry loops merely to get
a short mission read admitted.

## 3. Why “raise the permits to eight” is not yet an answer

More concurrency could help, but the number must follow a measurement rather
than become a substitute for admission design.

The server has two related limits:

1. the outer expensive-read semaphore in `futon1b_server.clj`, which is fair
   and currently waits three seconds before returning 503;
2. the inner XTDB query permits in `futon1b_xt.clj`, which protect the store
   from concurrent query memory and GC pressure.

Raising only the outer width can move contention inward.  Eight admitted
requests against four query permits may create a larger in-process queue while
increasing retained request state, heap pressure, and tail latency.  Raising
both widths can make throughput worse if the live limit is memory bandwidth,
GC, or XTDB's useful query concurrency.  The August GC-wedge incident is the
reason these bounds exist.

The question is therefore not just “do we need eight permits?” but:

- how many queries can make useful progress concurrently at the current heap;
- how many requests may wait, for how long, and with what memory cost;
- which request classes must remain responsive when corpus scans are active;
- whether queued work is fairly ordered or dominated by repeated callers.

## 4. Proposed shape

### A. Bounded queued admission

Keep a strict cap on executing store queries, but admit a bounded number of
waiters with an explicit deadline.  Report queue-full and deadline-expired as
different typed outcomes.  Eight waiters may be a reasonable first experiment;
it is not yet a production constant.

Required properties:

- FIFO within a class;
- maximum queue length and maximum wait time;
- cancellation removes a waiter promptly;
- no request holds a worker thread or large hydrated result while queued;
- retry advice reflects observed turnover rather than a fixed guess.

### B. Separate operational reads from corpus reads

A 30 ms mission registry read should not wait behind four 30-second evidence
scans.  Introduce explicit request classes, for example:

- `:operational` — bounded entity/identity/configuration reads needed for live
  decisions and readiness;
- `:interactive` — user-facing evidence and graph queries;
- `:bulk` — corpus scans, catch-up, indexing, and diagnostic enumeration.

Reserve a small amount of execution capacity for operational work, or use a
weighted scheduler with an aging rule so bulk work cannot starve either.  Do
not infer the class from URL names alone: routes should declare their cost
class and enforce size/window limits appropriate to it.

### C. Make overload observable as overload

The health route already exposes holders and permit counts.  Extend the model
with per-class:

- queued, admitted, completed, rejected, cancelled, and deadline-expired
  counts;
- queue wait and service-time distributions;
- current and oldest waiter;
- current and oldest holder;
- response status and query shape;
- heap/GC correlation during saturation windows.

Clients and downstream receipts must preserve `:expensive-read-busy` (or a
future more precise admission kind) rather than relabeling it
`:substrate-unreachable`.

### D. Reduce the work, not only schedule it

The journal showed evidence requests holding permits for tens of seconds.
Admission can protect availability, but those query shapes should also be
examined for:

- missing sidecar/index use;
- oversized result windows;
- count queries paired with full reads;
- repeated identical reads that could share a result or cursor;
- hydration that can be paged and interleaved;
- callers polling faster than the data can change.

The fastest mission query and the slowest evidence query should be measured
separately; a single aggregate latency hides the starvation mechanism.

## 5. Experiment before choosing widths

Replay a mixed workload with controlled arms:

| Arm | execution permits | queued waiters | scheduling |
|---|---:|---:|---|
| current control | 4 | semaphore's timed wait | one fair class |
| queue only | 4 | 8 | one fair class |
| classed queue | 4 | 8 | operational reservation / weighted bulk |
| wider execution | 6 or 8 | 8 | same classed policy |

For each arm measure throughput, p50/p95/p99 queue wait, service time by class,
503/504 counts, heap, GC time, and correctness under cancellation.  The
acceptance signal is not merely fewer 503s: operational reads remain bounded
without making bulk throughput collapse or recreating the GC wedge.

## 6. Concrete next questions

- Should mission/entity reads have a dedicated indexed route rather than use
  the general entities scan?
- What is the maximum memory retained by one queued HTTP exchange?
- Can the HTTP worker be released while admission waits, or must queuing move
  ahead of the current worker pool?
- Which existing routes are operational, interactive, and bulk?
- Is the inner four-query limit still optimal at the current 12 GB JVM and
  present XTDB version?
- What retry delay best matches live permit turnover?

## 7. Closure criteria

- [ ] Enumerate every route using `with-expensive-read!` and assign a declared
      request class and bounded input shape.
- [ ] Capture a reproducible mixed-workload baseline from the current policy.
- [ ] Compare queue-only, classed, and wider-execution arms using the measures
      above.
- [ ] Select and implement an admission policy with explicit queue and
      cancellation semantics.
- [ ] Prove with a load test that short operational reads remain bounded while
      long evidence scans run.
- [ ] Preserve typed overload through at least one real client and its
      operational receipt.
- [ ] Update `/health` and the runbook with per-class queue/holder signals and
      alert thresholds.

## Provenance

Observed on the Zone Futon1b service on 2026-10-13.  Relevant existing
accounts and code include:

- `docs/TN-futon1b-gc-wedge-incident-2026-08-23.md`;
- `TN-xtdb2-query-ceilings-and-ingest-memory-2026-08-02.md`;
- `TN-entities-speedups-2026-09-26.md`;
- `futon1b_server.clj` (`expensive-read-permit`,
  `with-expensive-read!`, and `/health` holder reporting);
- `futon1b_xt.clj` (inner query permits and query deadlines).

