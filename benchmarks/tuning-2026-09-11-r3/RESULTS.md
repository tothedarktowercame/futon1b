# Saturation control results

All six JVM cases completed (12 scenarios, 960 operations, zero operation
failures). Source/dependency hashes and receipt log hashes were checked after
execution; see `validation.json`. Together with r2 there are 1,920 measured
operations. The initial failed harness launch is retained separately and excluded.

| Repeat/config | Scenario | Point p95 ms | Queue p95 ms | Projection p95 ms | Scenario seconds | Process peak RSS MiB |
|---|---|---:|---:|---:|---:|---:|
| 1-baseline | foreground | 5076.4 | 5069 | 15731.4 | 15.80 | 1626.9 |
| 1-baseline | mixed | 3849.6 | 3844 | 18330.1 | 18.48 | 1626.9 |
| 1-workers-memory | foreground | 93.2 | 61 | 21592.3 | 21.67 | 1963.1 |
| 1-workers-memory | mixed | 32.2 | 18 | 13373.0 | 13.56 | 1963.1 |
| 1-workers | foreground | 73.5 | 48 | 20174.9 | 20.23 | 1641.0 |
| 1-workers | mixed | 35.8 | 18 | 21916.9 | 22.23 | 1641.0 |
| 2-baseline | mixed | 5218.6 | 5211 | 18043.8 | 18.25 | 1636.1 |
| 2-baseline | foreground | 3146.6 | 3142 | 12325.2 | 12.37 | 1636.1 |
| 2-workers-memory | mixed | 40.3 | 27 | 15755.5 | 15.96 | 2051.4 |
| 2-workers-memory | foreground | 12.4 | 7 | 12944.8 | 12.99 | 2051.4 |
| 2-workers | mixed | 41.9 | 22 | 14501.2 | 14.68 | 1619.3 |
| 2-workers | foreground | 12.6 | 9 | 12229.3 | 12.27 | 1619.3 |

## Interpretation

Four simultaneous projection requests occupy all four baseline request workers:
one builds while others wait on the existing projection lock. Short reads then
wait for a worker. Across both repetitions and both scenarios, baseline point-read
p95 was **3,147–5,219 ms**, almost entirely queue time (**3,142–5,211 ms**).
With eight workers and unchanged memory limits, it was **13–74 ms** (queue
**9–48 ms**). This is direct evidence of executor head-of-line blocking under
this constructed schedule, with spare workers helping short requests proceed.
It is not a diagnosis that every production stall has this cause.

Eight workers did not consistently improve projection latency or total scenario
time. Projection builds still serialize; additional workers do not remove that
lock or increase the four internal query permits. The mixed scenario produced a
source-watermark change during one build in each baseline repetition; the existing
bounded projection retry reached attempt 2 and succeeded. All other r3 builds
completed on attempt 1. No consistency check was disabled. These two events show
that unrelated-table writes can affect projection construction under this workload,
but they did not reproduce production's exhausted-retry failures.

With eight workers and doubled heap/direct-memory limits, point p95 was 12–93 ms.
That is not a consistent improvement over eight workers alone. Process peak RSS
was about 1.6 GiB for the 4-GiB-heap configurations and 1.9–2.0 GiB for the
8-GiB-heap configurations; this fixture provides no evidence that a larger memory
limit solves the production stalls. It also does not establish that production
has enough memory: production volume and competing services were not replicated.

**Candidate for the next controlled validation: eight request workers, retaining
the existing memory and query-permit limits.** This is a proposed experiment
setting, not a deployed change or a proven optimum. Do not double database permits
on the strength of this result: their count was not varied. A larger-memory trial
needs production-relevant volume plus GC pause, allocation and direct-memory
measurements; an HTTP-path trial needs the real admission wrapper and concurrency
representative of clients. FTS sidecar contention remains a separate untested cause.
The bounded retry and observability repairs remain necessary whichever setting wins.

Before interpreting a later serving trial, record worker queue/service time,
projection lock/quiescence/hydration time, source movement, and heap/GC observations
under the same trace identity. Queue length is useful, but is not an exact queue
position for an APM request unless that request's identity is actually observed.
Keep V2 retired and V3 held until the repair/deployment decision.

## Evidence and validation

`freeze.json` was committed in `57a8bcd` before this matrix. `*.receipt.json`
contains exact JVM commands, time bounds and exit codes. `*.json` case files
contain every request's outcome and timing. `*.log` contains actual projection
lock and source-watermark diagnostics. `*.resources.json` contains process RSS
and CPU accounting. `summary.json` is reproduced by `python3 summarize.py`.
See [experiment scope and reproduction](../tuning-2026-09-11/README.md) for the
bookmark, Apollo restriction/version mismatch, configuration pins and limitations.

The four JVM warnings per case concern module/native-access options and are
retained in the logs; no operation exception was hidden or counted as success.
The r3 Clojure probe passed clj-kondo (zero errors/warnings) and the workspace
Emacs parentheses check. Both matrices exercised the actual executor/query/
projection code in independent test JVMs. Post-run checks verified workload
counts, finite nonnegative timings, all receipt exits, source drift and log hashes.
No benchmark Java process remained after completion. No serving setting or loop
state was changed.

The full staged whitespace check reports three trailing-space lines in retained
raw stdout (two in `2-baseline.log`, one in `2-workers-memory.log`). They are
preserved byte-for-byte to keep receipt hashes valid; source/report checks pass.
Concurrent `println` calls also interleave some log records. The per-request JSON
receipts remain intact, but this exposes an observability repair to make before
relying on line-oriented production parsing: emit each structured event atomically.
