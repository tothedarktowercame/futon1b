# TN — APM jit-all-open-v3 usage pause and restart (2026-09-12)

**Status: paused.** The APMv3 loop (`jit-queue:jit-all-open-v3` inside the
futon3c dev JVM) was durably stopped on 2026-09-12 ~02:08 UTC because both
Claude and Codex usage were exhausted; its solver/proctor seats are
Codex-typed, so it could only churn. This note is what to read when it is
time to restart. Written by zai-5 from live inspection; every path and id
below was verified against disk state on 2026-09-12.

## How it was stopped (reusable)

```bash
# form file, then:
cd /home/joe/code/futon3c && ./scripts/proof-eval.sh -f <file.clj>
# file contents:
(do (require 'futon3c.apm.jit-queue-coordinator)
    (futon3c.apm.jit-queue-coordinator/stop!
     "/home/joe/code/futon3c/data/apm-coordinators/registry.edn"
     "jit-queue:jit-all-open-v3"
     "<cause>"))
```

`stop!` durably disables the registry entry, cancels the in-JVM scheduler and
the `semantic-progress:jit-queue:jit-all-open-v3` watchdog, and returns
`:draining` while the in-flight tick settles (~1 min). No JVM restart needed.

## State at pause

- Registry `data/apm-coordinators/registry.edn`: entry
  `jit-queue:jit-all-open-v3` has `:coordinator/enabled? false`. All other
  coordinators (`jit-m94A03-retry-v1/v2`, `library-lane:t00J02`,
  `ftriangle-live-smoke-v1`, `jit-all-open-v2`, `jit-all-open-nontopology-v1`)
  were already disabled before the pause.
- Queue state `data/apm-campaigns/jit-all-open-v3/coordinator.edn` (version
  82084 at pause): 45 completed frames through f225, 7 voids
  (statement-refuted), 32 parked (see below), next-index 53. The last
  regulator completion ended `:batch-paused` — see the f225 hold.
- The watchdog file `coordinator.edn.watchdog.edn` still names f228/preflight
  with `:watchdog/status :watching`; it is disarmed by the stop and will be
  re-armed on resume.

## BEFORE resume: release the f225 store-read hold

The queue is `:batch-paused` holding a store-read hold from f225 (m93J03),
taken 2026-09-11 19:16 after two hyperedge reads to futon1b (:7073) measured
6.0–6.2 s (threshold 5 s; both returned; timeout is 30 s). The hold dispatched
a repair to `codex-16` that never completed. The hold entry lives in the
queue state under `:store-read/hold` with `:hold/id
bad82e9694d2ba65a5c0f9765f5bffd3863413fa38fcdeb273e0016bf05cdc` and
`:resume/status nil`.

Release path (futon3c, trusted operator action, requires the quiescent
stopped queue — which the pause provides):

```clojure
(require 'futon3c.apm.jit-queue-coordinator)
(futon3c.apm.jit-queue-coordinator/release-store-read-hold!
 {:registry-path "/home/joe/code/futon3c/data/apm-coordinators/registry.edn"
  :coordinator-id "jit-queue:jit-all-open-v3"
  :receipt "<verified repair receipt id>"})
```

Verify first that futon1b is healthy (below); the honest receipt basis is that
the latency was measured, diagnosed, and is now within threshold, not that an
agent "said so".

## Resuming

```bash
# write form file (never shell-quoted inline; see proof-eval.sh header):
(do (require 'futon3c.apm.durable-coordinator)
    (futon3c.apm.durable-coordinator/resume!
     "/home/joe/code/futon3c/data/apm-coordinators/registry.edn"
     "jit-queue:jit-all-open-v3"
     "codex usage restored"))
# then: cd /home/joe/code/futon3c && ./scripts/proof-eval.sh -f <file.clj>
```

`scripts/shunt.sh resume` is the broader path (topology + wm loops too); for
just the v3 queue the direct `resume!` above is the precise action. After
resume, verify: registry `enabled? true`, scheduler armed, and the queue
leaves `:batch-paused` on its next tick.

## futon1b state (the thing that tripped f225)

Diagnosis is complete and pre-existing in this repo: see
`TN-evidence-query-fixed-cost-2026-09-07.md` (~850 ms per-request engine
floor), `TN-futon1b-cost-profile-2026-09-08.md` (end= reads ~1.2–2.3 s cold;
48-entry response cache, cursor walks evict hot keys), and
`benchmarks/tuning-2026-09-11-r3/RESULTS.md` (projection builds serialize and
occupy request workers; 8-worker default deployed and live since 2026-09-11
02:33 — the f225 warnings came 17 h after it, consistent with r3's own
"projection builds still serialize" finding). At idle on 2026-09-12 the exact
URL answered ~1.3 s; metaspace sampler clean (165 MB, no monotonic growth).

Ranked fixes already written in the cost-profile TN: (1) flip `include-total`
default to false; (2) cache observability + point/cursor partitioning;
(3) prepared paths for evidence count/session filters. Doing rank 1 before
restart would remove most of the warning margin.

## Parked frames (32) — not futon1b's fault, for triage when capacity returns

Recent and representative (full list + rationales in the queue state):
- f200, f202 — futon3c thread-store "already has an active writer" races
  (successor round dispatched into a still-running seat).
- f206 — Student compile failure misclassified as apparatus fault; the proof
  itself was closed and banked (33c076fd).
- f208, f209 — the 2026-09-09 Codex quota outage (checkpoint forced; dead job
  pointer halted the whole campaign until cleared).
- f218 — closure checker bound (watchdog recorded 532 s vs 300 s max).
- f220 — directed mathematical-infrastructure transfer after 50 rounds.
- f169–f189 era — `:fresh-session-id-missing` apparatus fault, fixed by
  f4ae8d5a; kept as decided partials.

## Open observation for the apparatus

The f225 pause outlived its trigger: a measured, understood, self-recovering
latency blip routed the queue into a hold waiting on an agent that then lost
its capacity, and the hold had no time-bound release. A hold that
auto-releases when reads measure healthy again would have avoided the
deadlock. Worth deciding before restart, independently of the futon1b fixes.
