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

## Hardening pass deployed 2026-07-22 (Codex, 14:04–14:11 BST)

This pass addresses immediate capacity, bounded realization, write-path
availability, transport diagnosis, durable Emacs delivery, and monitoring as
one coherent change. It does **not** declare the incident closed: the week-long
steady-state acceptance observations below remain outstanding.

### Store/query invariants now enforced

- Evidence lists are cursor pages over a compact projection. `limit` defaults
  to 100, is restricted to 1–1000, and invalid/oversized values return 400.
  Full evidence bodies are hydrated only after the exact filtered page is
  selected. `:next-cursor {:at ... :id ...}` continues the newest-first keyset
  scan without gaps or duplicates. Futon3c follows these cursors when its
  backend protocol requires more than one page, including `-all`; it never
  requests one unbounded store-JVM response.
- All `futon1b-xt/safe-q` calls share a fair process-wide four-permit pgwire
  budget. Four hydration futures are therefore a global ceiling rather than a
  per-request multiplier.
- Corpus-scale HTTP reads share two admission permits. A third concurrent scan
  receives retryable 503 `:expensive-read-busy`, leaving two of the four HTTP
  workers for writes, point reads, and `/health`. The futon3c evidence client
  retries this response with bounded exponential backoff.
- Other list/search endpoints now receive a validated default limit of 100 and
  reject non-positive/invalid values; their current compatibility maximum is
  5000 because the mission-scope readers legitimately request that window.
  Evidence remains the stricter 1000-page protocol because it has a cursor.

### Failure taxonomy and marks durability

- Futon1b HTTP append errors are classified as `:store-timeout` versus
  `:store-unreachable`. The boundary now records these as I-evidence-per-turn
  `:kind :timeout` / `:kind :unreachable`, not `:shape`, and the futon3c HTTP
  compatibility route returns retryable 503 for transport/store failures.
- Emacs chat evidence is written first to a private disk outbox under
  `~/.local/state/futon3c/evidence-outbox/`, with one stable evidence id carried
  in both futon3c and direct-futon1b JSON spellings. A 2xx or duplicate-id 409
  acknowledges and removes it; transport/408/429/5xx keeps it for periodic
  replay; terminal 4xx moves it to `failed/` for inspection. Pending/failed
  counts are visible in chat headers and echo-area messages. Thus the existing
  one-second UI timeout no longer implies loss, and reply chains may reference
  an entry while it is pending because its identity is already fixed.
  `agent-chat.el` was hot-loaded into both live Emacs servers (`server` and
  `futon-ops`) after its tests passed; both reported zero pending/failed files.

### Capacity and monitoring deployed

- The installed unit is now `MemoryHigh=10G`, `MemoryMax=12G`; the parent
  `futon-services.slice` has no memory cap. `MemorySwapMax=1G` is unchanged.
- `futon1b-vitality.timer` is installed and enabled once per minute. It records
  bounded private JSONL at `~/.local/state/futon1b/vitality.jsonl`: cgroup
  current/high/max, anon/file/swapcached, `memory.events`, PSI, and timed
  `/health`. It alerts at 80% of `memory.high`, on a new `memory.events high`
  event, failed health, or health latency >=500 ms. The log rotates at 10 MiB.

### Evidence collected during deployment

- Before changing the live 7G high watermark, at ~14:00 BST the cgroup had
  reached **6.29 GB / 83.7%** after roughly 22 minutes: **5.53 GB anon** and
  **0.73 GB file**, with `memory.events high=0` and `/health` 53 ms. This
  confirms tight budget growth but weakens the earlier claim that page cache
  alone dominates it.
- Both futon1b restarts recovered `/health` on attempt 22 (~21 seconds, mostly
  XTDB replay/startup). After the final restart: high/max = 10/12 GiB, memory
  1.44 GB (13.4% of high), no swap, no high/max/OOM events, PSI zero.
- The 14:11 timer sample, roughly three minutes later, was already **4.90 GB
  (45.7% of high)**: **4.65 GB anon** and **0.23 GB file**, still with zero
  high events, zero PSI, and `/health` 115 ms. This rapid post-start increase
  is why the new capacity must remain a monitored hypothesis rather than being
  called a resolution; the 24-hour/one-week observations below are mandatory.
- A cold `GET /api/alpha/evidence?limit=2` took **5.79 s**. During it, ten
  `/health` probes completed in 1.9–44.6 ms (nine under 11 ms), demonstrating
  liveness isolation. The cold list latency itself is still unacceptable and
  remains an investigation below.
- Live oversized evidence limit returned 400 in 3 ms with the documented
  envelope. A two-entry response returned the documented next cursor.

### Validation completed

- Futon1b: clj-kondo 0 errors/warnings on changed Clojure; check-parens clean;
  `test-a1a2` **36/36 PASS** including pagination, limit rejection, two occupied
  scans + third 503, and a concurrent successful write; `test-a3a4a5`
  **50/50 PASS**.
- Futon3c focused Clojure: boundary + backend **16 tests / 66 assertions**, all
  pass. Emacs shared chat/session/identity suites: **24/24**, including timeout
  replay, duplicate acknowledgement, terminal-failure retention. Changed Lisp
  and Clojure are check-parens clean; clj-kondo reports 0 errors (the existing
  http-kit macro-resolution warnings were excluded in the focused invocation).
- Broad `futon3c.transport.http-test`: **98 tests / 489 assertions**, 7 failures,
  all outside this change: two stale `/health` evidence-count expectations,
  one pre-existing `server-sent-at-ms` expectation, and four stale AIF agenda
  expectations. The new retryable evidence-status assertion passed. Do not
  restore the removed unbounded health count to satisfy the stale test.

### Still open / evidence that requires time

1. Let the vitality timer collect at least a week of normal cohort load. Report
   warm steady-state max and p95 headroom, anon/file split, PSI, and any
   `memory.events high` deltas here. The acceptance target remains >=1.5G below
   `MemoryHigh`; the higher limit is capacity, not proof of stabilization.
2. Derive per-route p50/p95/p99 from `[futon1b-request]` logs. The timer measures
   liveness latency, not route latency. Specifically explain the 5.8 s cold
   two-entry page and the observed 20.4 s mission-doc hyperedge window; likely
   dimensions are XTDB compilation/warmup, ingestion overlap, and point-read
   hydration. Do not raise HTTP concurrency as a remedy.
3. Review `vitality.jsonl` after 24 hours for monotonic anon growth. Capture NMT,
   heap occupancy/histogram, and `memory.stat` together if ratio reaches 70%,
   so heap, metaspace/native, and file cache can be apportioned at the same
   instant.
4. Audit and reconcile the Emacs outbox during the pilot: pending must drain to
   zero; every file under `failed/` requires an operator decision. Add an HUD
   surface if chat-header visibility proves insufficient.
5. Update the seven stale broad HTTP assertions separately; they are test drift,
   not justification to restore removed behavior or rewrite current AIF facts.

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

## Second episode same day — heap OOM in HTTP dispatcher (14:35 BST)

The initial field diagnosis correctly captured the zombie signature and the
outbox interaction, but incorrectly named IOException as the dispatcher
killer and treated 5.9G cgroup use against a 10G high watermark as proof that
the incident was not memory-related. The preserved journal makes the direct
cause unambiguous:

```text
14:35:39 Exception in thread "HTTP-Dispatcher"
java.lang.OutOfMemoryError: Java heap space
  at ...ServerImpl$Dispatcher.lambda$run$0(ServerImpl.java:534)
  at java.util.Collection.toArray(Collection.java:418)
```

The cgroup still had capacity, but the JVM's separately bounded 4G heap did
not. The IOException lines were workers discovering that timed-out clients had
closed their sockets; they were aftermath, not what killed the dispatcher.

### Corrected causal chain

1. At 14:20:01 and 14:20:04 two
   `/api/alpha/hyperedges?end=...&limit=1` calls began. The endpoint branch
   still queried `(from :hyperedges [*])`, filtered endpoint membership in
   Clojure, and applied `limit` last. Thus the first hardening pass's bounded
   realization claim did not cover this branch.
2. Both scans occupied the two expensive-read permits for **936.7 s and
   941.7 s**. Point reads and writes were intended to remain available, but
   they now competed for the remaining global XTDB/query capacity.
3. The new outbox froze its caller's one-second interactive timeout into each
   disk record. Two Emacs processes could retry the same shared queue every 15
   seconds. Each abandoned Futon3c request continued server-side, including
   reference/read-back point queries, after Emacs had closed its connection.
4. JDK `HttpServer` was backed by `Executors/newFixedThreadPool(4)`, whose
   handoff queue is unbounded. It continued accepting abandoned connections
   faster than the four workers could retire them. The dispatcher eventually
   exhausted the Java heap while copying its selected connection set.
5. `OutOfMemoryError` is not caught by the dispatcher's `Exception` guard, so
   the accept thread vanished while the listener and JVM remained. The saved
   `/tmp/futon1b-stuck-threads.txt` and `Recv-Q 51`/backlog 50 are the expected
   terminal state.

The outbox was therefore an amplifier, but not the sole cause: the unbounded
endpoint realization and unbounded HTTP handoff queue were necessary parts of
the failure. A top-level IOException catch would not have prevented this OOM.

### Remediation deployed 15:18–15:23 BST

- Endpoint membership now uses XTQL `unnest` + `where`, orders/limits the
  projected ids inside XTDB, and hydrates only the selected window. The exact
  two formerly fatal queries completed concurrently in **2.98 s and 3.02 s**.
  Ten main and independent health probes stayed between 1–5 ms.
- The main HTTP executor is bounded at four running exchanges plus sixteen
  queued. Excess accepted work is rejected/closed rather than retained in an
  unbounded heap queue. A closed client is logged as `client-disconnected` and
  does not provoke an impossible second 500 write.
- The serving JVM now has `-XX:+ExitOnOutOfMemoryError`. If any other heap OOM
  escapes, the whole JVM exits and `Restart=on-failure` recovers it instead of
  preserving a socket-listening zombie. This is safer than attempting to
  continue inside a potentially corrupted/OOM JVM.
- Production exposes a separate one-worker liveness acceptor on `:7072`; the
  vitality sampler records both it and main-path `:7073/health`. After restart,
  both returned 200 on readiness attempt 19.
- Futon1b now enforces reply/fork existence itself. The Futon3c backend removes
  redundant client preflight reads, bounds an append attempt at 30 seconds, and
  the live compatibility handler single-flights a stable evidence id (a second
  in-flight request receives retryable 503). The live compatibility probe
  appended/read back in **0.67 s**; its duplicate returned 409 in **0.06 s**.
  The existing backend object in the live Futon3c JVM retains its old method
  body until the next normal JVM lifecycle, but the hot-loaded timeout vars and
  handler single-flight are active now. Futon3c was deliberately not restarted
  from its own Agency-routed session.
- Outbox records no longer persist the one-second UI timeout. Interactive
  delivery remains short, while replay is an asynchronous curl process with a
  75-second budget, a filesystem lease shared by both Emacs servers, and a
  persisted exponential `next-at` deadline so aligned timers cannot retry in a
  burst. Both Emacs servers were hot-loaded and their timers re-armed.
- All three parked records are now readable from Futon1b. One was acknowledged
  automatically as an already-stored duplicate. Two were redelivered under
  operator control, verified by direct GET, and retained privately under
  `evidence-outbox/acknowledged/` as incident audit artifacts. Live queue state
  is pending=0, failed=0.
- A forced-timeout end-to-end replay used stable id
  `emacs-hardening-replay-probe-20260722`: the interactive 1 ms attempt queued
  it, the asynchronous retry stored it by the second observation, and the
  third observation found pending=0 with direct GET=200. This exercises the
  normal timer/acknowledgement path rather than only the controlled recovery
  path. Both Emacs timers remain active; pending=0, failed=0.

### Validation and remaining observation

- A final post-commit Futon1b-only lifecycle at **15:32 BST** loaded the exact
  committed server code. Main `:7073/health` and independent `:7072/health`
  were both 200 on readiness attempt 17; systemd reported active/running, PID
  1058379, and `NRestarts=0`. Futon3c was not restarted from its Agency-routed
  session.
- Futon1b suites: **39/39** and **51/51**; these include finite executor
  rejection, independent liveness, server-side reference validation, and
  endpoint-limit pushdown.
- Futon3c backend/boundary: **17 tests / 68 assertions**. Emacs outbox suite:
  **16/16** after the cross-process deadline/status parsing tests; the combined
  chat/session/identity run is **26/26**. Broad HTTP: **99 tests / 492
  assertions**, the same seven pre-existing stale failures; the new stable-id
  single-flight test passed.
- Keep the week-long memory/latency acceptance run. Additionally watch bounded
  executor rejection/client-disconnect rates and both liveness surfaces. The
  first normal Futon3c restart must be followed by a compatibility append probe
  proving that the newly instantiated backend has dropped the legacy preflight
  GET. Investigate any recurrence of the transient zero-duration XTDB
  `RuntimeException` seen during controlled redelivery; request logs now retain
  the exception message for that purpose.

## Third episode — malformed EDN producer identified and traced (2026-07-23)

The later `[boundary] I-evidence-per-turn VIOLATION: persistence rejected`
lines were not a Futon1b availability failure. Main and independent health
remained 200, PSI was zero, and the Futon1b request journal recorded immediate
500 responses with:

```text
class=java.lang.RuntimeException message="Invalid token: :"
```

The requests initially carried no trace header, so the rejected body was gone
after parsing failed. The retry cadence nevertheless matched two durable Emacs
outbox records exactly. They identify the current producer as `codex-4`
`turn-commits` evidence in session
`019f8b63-a009-79e0-9a22-e7402848c822`:

- `emacs-7a24bbc7c4bf28eaafe208f93a3fbad8` (72 attempts when inspected);
- `emacs-68256967396ff3f9d4fc583a9e19541b` (4 attempts when inspected).

This is conclusive producer attribution, but not conclusive malformed-field
attribution. The preserved JSON records are valid, and reconstructing them
through the current checked-out normalizer produces readable EDN. The serving
Futon3c JVM predates several same-day source changes and retains its original
backend method body, so naming a specific value from the vanished wire body
would exceed the evidence.

At 22:38 BST both exact records were moved intact from the live pending queue
to its private `failed/` directory. Pending is zero and failed is two. This is
reversible containment, not acknowledgement or deletion: it stops the
deterministic retry stream while preserving both payloads for a controlled
post-deployment replay.

Futon3c commit `0367415` (`Trace and preflight evidence appends`) closes that
observability gap:

- every Futon1b append carries stable
  `x-trace-id: evidence-append:<evidence-id>`;
- the producer serializes once and proves that Futon1b's EDN reader can consume
  the exact wire representation before opening a socket;
- an unreadable payload is a terminal `:store-serialization`/HTTP 400, not a
  retryable store 503, so the outbox cannot amplify a deterministic producer
  defect;
- the boundary violation names trace id, evidence id, author, session, event,
  HTTP status, and structural invalid-EDN paths while never logging the
  evidence body;
- both success and failure compatibility responses expose the trace id.

Focused validation is 20 tests / 85 assertions for the backend and boundary,
plus the transport status test (4 assertions), all passing. `check-parens.el`
is clean and clj-kondo reports zero errors/warnings. The broad HTTP namespace
still has the same seven unrelated expectation drifts recorded above.

Deployment remains tied to the next safe Futon3c lifecycle: the existing
backend object cannot acquire a recompiled record method in place, and the
restart-safety invariant forbids restarting the Agency JVM from its own routed
agent session. After a separate-session restart, replay one parked record and
require either a 2xx/duplicate acknowledgement or a terminal diagnostic whose
trace id appears in both the boundary and Futon1b journals. Continue the
week-long vitality/latency monitoring independently.
