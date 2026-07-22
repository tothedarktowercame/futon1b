# TN-futon1b-memory-incident

**Technical note — the :7073 store's recurring memory instability, 2026-07-22
episode.** Author: claude-2. Date: 2026-07-22. Intended for handoff to a fixing
agent. Predecessor incident: 2026-07-13 OOM-zombie (see §History — different
proximate cause, same organ).

## Incident summary (2026-07-22, times BST)

Between ~13:20 and the 13:37 operator restart, `futon1b-server.service`
(Dionysus, `--store-dir migration-store-21 --port 7073`) entered a
**memory-pressure brown-out**: the service stayed `active (running)` and HTTP
stayed up, but requests stalled for seconds at a time. Journal showed a simple
evidence POST taking **5,328 ms** (13:23:42). Callers timing out produced a
string of futon3c boundary rejections:

```
[boundary] I-single-boundary VIOLATION: shape rejected — futon1b server unreachable
[zai-transcript] FATAL e-e65f354d…: ZAI transcript persistence was rejected
```

Observable damage: one live zai operator turn **aborted** ("z.ai invoke
failed: ZAIF decision persistence was rejected" — since fixed, see §Already
fixed); at least one zai transcript entry lost; unknown number of other
evidence writes (chat turns, marks) rejected or silently dropped in the
window. Zaif persistence ledger at the time: 87 ok / 1 rejected.

## State at incident vs baseline

| | at incident (~13:25) | post-restart (13:40) |
|---|---|---|
| cgroup memory | **6.6G, peak 7.0G** (= MemoryHigh) | 3.6G |
| cgroup headroom ("available") | **380.7M** | 3.3G |
| swap (cgroup, max 1G) | 770M, peak 945M | — |
| JVM RSS | 5.5G | — |
| /health latency | seconds/timeouts | 6.5 ms |
| service uptime | 2h59m (started 10:32 BST) | fresh |

Machine-level RAM was NOT the constraint (30G total, ~18G available). The
pressure was **cgroup-local**: `memory.high` reclaim throttling.

## Root cause, layered

1. **Budget arithmetic.** `deps.edn` `:server` alias runs `-Xmx4g
   -XX:MaxDirectMemorySize=1g -XX:MaxMetaspaceSize=1g` — worst-case JVM
   footprint ≈ 6–6.5G before thread stacks. The unit sets `MemoryHigh=7G`.
   The **page cache for the store files (XTDB migration-store-21 + the D1
   FTS5 sqlite sidecar) is charged to the same cgroup**, so a warm JVM plus a
   warm store cache *inevitably* reaches the 7G high-water mark. `memory.high`
   is a throttle, not a kill: at the line, the kernel forces reclaim on the
   service — evicting exactly the page cache the store needs — and request
   threads stall in reclaim/IO. Growth observed today: 3.6G fresh → 7.0G peak
   in ~3 hours of normal load.
2. **Realization spikes (the 07-13 finding, still open).** HTTP requests are
   served by a fixed pool of 4 threads (`futon1b_server.clj`, pool-2) running
   JDBC queries over the loopback pgwire self-connection; **large evidence
   query results are fully realized on-heap** on those threads. Under memory
   pressure these spikes both trigger reclaim and convoy behind it (4 threads
   → slow queries queue callers into timeouts). Today's load included repeated
   paged evidence sweeps (z3a scorer runs, tag queries, normal session
   traffic).
3. **Diagnosability bug (futon3c side).** The boundary reports a *timeout /
   connection failure* as `kind :shape` — "shape rejected — futon1b server
   unreachable". A transport failure masquerading as a validation failure cost
   real diagnosis time today (the zaif error read as if an entry were
   malformed). The violation taxonomy should separate `:unreachable`/
   `:timeout` from `:shape`.

## Diagnostic signature of a recurrence

`systemctl --user status futon1b-server` shows Memory near `high:` with
"available" in the low hundreds of MB; journal shows `[futon1b-request] end …
elapsed-ms=` in the thousands for trivial requests; futon3c server pane
accumulates `I-single-boundary VIOLATION … futon1b server unreachable` while
the service is nominally healthy. (Distinct from the 07-13 zombie: there, HTTP
was DEAD — socket open, accept backlog full, `HTTP-Dispatcher` thread killed
by OOM under the old `-Xmx1g`.)

## History

- **2026-07-12/13:** OOM-zombie under `-Xmx1g` (heap OOMs every ~8 min, HTTP
  dispatcher died 22:46; recurrence 07-13 11:27 when a restart went out
  without the heap bump). Fixed by the `:server` alias with `-Xmx4g` + unit
  `ExecStart -M:server`. pool-2 identified as the request pool realizing big
  results. Evening 07-13 episode = box overload (wedged native-comp, 3.5 days
  at 99.6% CPU) + 30s→120s futon3c timeout fix that was committed but not
  loaded. Details: memory note `futon1b-7073-oom-zombie-2026-07-13`; heap
  histogram `/tmp/futon1b-7073-histo-2026-07-13-recurrence.txt` (if still
  present).
- **2026-07-22 (this note):** brown-out at the `MemoryHigh` line — the 4G heap
  fix moved the failure from OOM-death to cgroup-throttle purgatory.

## Already fixed — do NOT redo

- **Zaif shadow persistence no longer kills turns** (futon3c `b5b5d02`,
  hot-reloaded 2026-07-22): rejection is counted + surfaced in follow-mode,
  swallowed. Regression-tested.
- `-Xmx4g` server alias + unit (07-13). futon3c evidence-backend timeout is
  120s via `FUTON1B_TIMEOUT_MS` (07-13, f62a3ef) — but note callers with
  their OWN short timeouts (elisp chat-turn emit: 1s) still fail fast.

## Candidate directions for the fixer (with the tradeoffs named)

1. **Fix the budget arithmetic** (cheapest, likely first): either raise
   `MemoryHigh` to ~10–12G and `MemoryMax` above it (box has 30G; check
   `futon-services.slice` for a parent cap), or lower `-Xmx` to what the
   heap actually needs (evidence: post-07-13 histograms; a fresh histogram
   under today's load would settle it). Leave real headroom between JVM
   worst-case and `MemoryHigh` for store page cache — that gap IS the
   store's read performance.
2. **Cap realization** (structural, the durable fix): server-side result
   limits / streaming on the evidence endpoints so a query can't realize
   unbounded results on a request thread (`&limit` discipline exists
   client-side; the server should enforce its own ceiling). Consider bumping
   the 4-thread pool only AFTER realization is capped (more threads × huge
   results = worse).
3. **Fix the violation taxonomy** (futon3c `evidence/boundary.clj`):
   `:unreachable`/`:timeout` distinct from `:shape`, so the next brown-out
   is legible in one journal line.
4. **Monitoring hook:** the vitality scanner (or the planned zaif-hud layer-1)
   should watch `memory.current / memory.high` ratio and request `elapsed-ms`,
   and alarm BEFORE the throttle line. Today the first symptom anyone saw was
   a dead operator turn.
5. **Write-path resilience for the marks channel:** the elisp chat-turn emit
   (1s timeout, silent failure) can lose operator ✘/✓ marks during
   brown-outs — the one channel whose whole design premise is precision 1.0.
   A tiny retry-once-on-timeout, or at minimum a visible failure message in
   the repl buffer, would close the silent-loss window.

## Acceptance bar for closing this note

(a) A week of normal operation with zero `I-single-boundary VIOLATION …
unreachable` lines; (b) trivial-request `elapsed-ms` p95 under 500 ms at
steady state; (c) cgroup memory stabilizes with ≥1.5G headroom below
`MemoryHigh` under warm load; (d) a recurrence, if any, is identifiable from
one journal line (taxonomy fix); (e) findings recorded back here.

## Context: why this matters right now

The Z3a zaif cohort (M-zaif-harness, futon2) is entering its pilot phase; the
study's judgment channel (operator marks) and its instrumentation (dual
decisions, transcripts) all ride this store. Store brown-outs during the
cohort window are silent data loss for a preregistered experiment.
