# Disposable store tuning experiment, 2026-09-11

Joe requested a pinned synthetic experiment to distinguish APM-only pressure
from competing database users, and to investigate increasing workers and memory.
This packet tests actual XTDB queries, writes and memory projection builds with
the bounded request executor. It does not start APM or a second HTTP service.
The measured results are in [the saturation report](../tuning-2026-09-11-r3/RESULTS.md).

## What was frozen

The read-only serving-store observation at 2026-09-11T02:19:03.732079Z is
retained in `bookmark.json`, `bookmark-readiness.edn`, and `bookmark-health.edn`.
Submitted and processed byte offsets both read **10972461711**. The two GETs
returned HTTP 200, in about 22 ms and 1 ms respectively. This is an as-observed
watermark bookmark, **not a backup, transactional snapshot, or freeze of other
writers**. No marker was written into the serving database.

Each test revision has a pre-execution `freeze.json`: tracked Clojure/deps
file hashes, resolved JAR hashes, runner hash, Java version, fixture definition,
configuration matrix and deadlines. Each invocation retains its exact command,
exit code, log hash, source-drift result, resource measurements and per-operation
results. The runner refuses to overwrite retained cases. It checks source hashes
before every case and after completion, and dependency hashes before the matrix.

The host has 16 cores/32 threads and about 249 GiB RAM (about 235 GiB available at
inspection). Serving futon1b uses heap 4 GiB, direct-memory limit 3 GiB and
metaspace limit 2 GiB. Its systemd unit has no CPU or memory maximum configured.
Those serving settings were not changed by this experiment.

## Why not Apollo yet

`/home/apollo/code/futon1b` is readable but not writable by Joe; the noninteractive
attempt to act as Apollo required a password. Its branch is
`apollo/xtdb-2.2.0`, ref `8ba7076c0705972b0afe241a75543b2fd1fdc88e`, with XTDB
2.2.0-rc0. The current Joe checkout uses XTDB 2.1.0. Apollo therefore was not an
identical production replica, and using it would confound engine version with
capacity tuning. No ownership or Git trust protection was bypassed.

Instead, test JVMs used this checkout's source and dependencies with fresh
disposable XTDB nodes. They were not given a production store path and did not
start the HTTP listener, FTS sidecar, agents or loops. This isolates database
contents, not host CPU, scheduling, disk or memory from other running processes.

## Retained revisions

1. Initial harness, preparation `3ac5b0d`, freeze `4de13b1`: first baseline exited
   1 after about 6.4 seconds because `q1` was called incorrectly. Its original
   source, log and receipt remain here; it is excluded from performance claims.
   The failed `/usr/bin/time` output contains an error line before its JSON object.
2. Corrected harness `508b535`, freeze `5adde8c`, results `6e624c6`:
   [r2](../tuning-2026-09-11-r2/) has six completed process cases, twelve scenarios
   and 960 operations, with no operation failures. The 5,000-entry/128-edge fixture
   showed considerable warm-up order effects and low memory use. It did not
   saturate all workers with projection builds at once.
3. Larger saturation control, prepared in `6e624c6`, frozen in `57a8bcd`:
   [r3](../tuning-2026-09-11-r3/) uses 20,000 entries, 2,048 memory assertion edges,
   and four projection requests at the beginning of the foreground sequence.
   This is a deliberately constructed saturation control, not a replay of an
   observed APM request schedule.

## Reproduction and limits

The committed result directories are evidence, not writable rerun destinations.
To repeat, create an explicitly new benchmark revision, preserve the workload,
and run its `run_matrix.py` once to freeze and once to execute. Review the new
freeze first. Each invocation has a 240-second deadline; each submitted future
has a 120-second wait bound. Deadline cleanup targets only the test process group.
There is no harness retry. Existing bounded retries *inside* projection building
remain intact, as do source-watermark checks, the projection lock, and the four
internal XTDB query permits.

The matrix compares 4 workers/4 GiB heap/3 GiB direct, 8/4/3, and 8/8/6; queue
capacity 16 and metaspace 2 GiB are fixed. Two repetitions reverse configuration
order and scenario order. Each scenario receives a fresh node; its JVM is shared
with the other scenario in the case, so JIT and other process warm-up persists.

Foreground is 60 point reads and four projection builds. Mixed adds 16 scans
and 16 synchronous writes to an unrelated table, interleaved with foreground
requests. Requests are submitted in waves of 16; the next wave waits for all of
the current wave. This limits arrival pressure and makes total elapsed time
sensitive to the slowest task. Point reads assert that the seeded document exists.
The fixed 1,024-character payload is highly compressible. These are not a
production-size corpus, realistic payload distribution, or a memory-capacity test.

This bypasses the HTTP transport and its expensive-read admission wrapper, but
uses the real request executor and query/projection implementation. There is no
claim to measure network clients, APM coordination, retries across services,
FTS SQLite contention, or the entire production admission path. In particular,
this does not settle the cause of all frame 200–218 failures.

Per-request queue/service/total times are retained. Reported p95 uses nearest
rank, per scenario, including failures in the denominator; only four projection
samples occur per scenario. Process maximum RSS covers both scenarios. Heap used
is an end-of-scenario observation, and GC counters are cumulative within the JVM,
not per-scenario deltas or evidence of peak heap pressure. Two repetitions are
exploratory evidence, not capacity certification.

V2 and V3 registry entries were rechecked during execution: both remained
`:coordinator/enabled? false` (recorded lifecycle `:draining`). No loop restart,
serving reload, service configuration edit or serving database write was made.
