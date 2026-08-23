# futon1b — the XTDB 2 substrate store

The successor store to futon1a (XTDB 1.24 / RocksDB): one XTDB 2.1.0 node
owning the store, served over HTTP/EDN by `futon1b_server.clj` — the second
JVM (approved 2026-07-10; futon3c's I-0 becomes "one coordination JVM + one
store JVM"). The migration pipeline (`migration/`) moves futon1a data in;
the server carries the live operational surface.

- **Contract**: `API-CONTRACT.md` — the futon1a route-by-route spec the
  server implements (extracted at boundary commit `a71c399`).
- **Text index**: `CANDIDATE-INDEX-CONTRACT.md` (invariants C1–C6) and
  `README-fts.md` (how the index behaves, how to check it, and the checklist
  for store operations — **read before any replay, backfill or store swap**).
- **Plan**: `futon2/holes/E-futon1b-operational-switchover.md` (phases A–D)
  and `futon2/holes/E-futon1a-to-futon1b-migration-pipeline.md` (data leg,
  findings F1–F11).
- **Boundary**: `futon2/holes/BOUNDARY-futon1a-to-futon1b-2026-07-10.md`.

## Run

```
clojure -M:node -m futon1b-server --store-dir migration-store --port 7073
# lucy: --port 7074 (nginx owns :7073 there)
```

XTDB 2 local stores are single-process: while the server runs, no other JVM
may open the same `--store-dir`. Stores (`migration-store*/`,
`migration-export/`) are gitignored — each box builds its own from its own
futon1a.

## Deploy (systemd --user)

The persistent service runs from the tracked unit
`scripts/futon1b-server.service`, which uses **`-M:server`** — so the heap
sizing lives in `deps.edn :server` (version-controlled) and reproduces on
every box without a per-box `-J-Xmx` override. To install / reproduce
(e.g. on linode-chicago):

```
mkdir -p ~/.config/systemd/user
cp scripts/futon1b-server.service ~/.config/systemd/user/
rm -rf ~/.config/systemd/user/futon1b-server.service.d   # drop stale -J override
systemctl --user daemon-reload
systemctl --user enable --now futon1b-server
```

Memory is documented in **[Memory requirements](#memory-requirements)** below.
Day-to-day serving is modest and ran fine on a 3.8G box; `deps.edn :server` ships
a larger ceiling sized for *rebuilds*, so read that section before deploying to a
small host — the shipped value is a ceiling, not a minimum.

The two genuine per-box knobs in the unit are `--store-dir` (lucy
`switchover-store`, chicago `chicago-store`) and `--port` (lucy `7074`, chicago
`7074`, default elsewhere `7073`) — edit these in the copied unit before
`enable`.

## Memory requirements

**You do not need a big machine to run this.** Day-to-day serving is modest; the
large numbers in `:server` are sized for *rebuilds*, which are occasional. Size
for the workload you actually have.

| workload | heap | direct | evidence |
|---|---|---|---|
| dev / scripts / tests (`:node`) | `1g` | `1g` | |
| **day-to-day serving** | **~`1536m`** | **~`768m`** | ran the **full corpus on lucy's 3.8G box** from 2026-07-14; live set after GC only ~800–850M, so 1536m is ~44% headroom. Measured again on Zone 2026-08-17 serving 140,296 documents: **~976M live**. |
| heavy concurrent workload | `4g` | `768m`+ | the 2026-07-25/26 learning loop pinned G1 concurrent threads at 99.9% under 1536m (deep-health 8–25s, type sweeps 5–20s) while 15G sat free |
| **full-corpus rebuild / backfill** | `4g` | **`3g`** | steady-state Arrow allocation is already ~1.45G, so at a 1536m cap the rebuild scan **cannot obtain transient buffers at any page size** (2026-08-17) |

`deps.edn :server` currently ships **`4g` + `3g`** — the rebuild row, because
candidate-index backfill is a C3 lifecycle operation that *will* recur and it is
the case most likely to fail confusingly. That is a deliberate ceiling, **not a
minimum**.

Plus, in the systemd unit, **`Environment=MALLOC_ARENA_MAX=2`** — see below.
That one matters at *any* size.

### Sizing for a smaller box

`scripts/futon1b-server.service` runs `-M:server`, so whatever `deps.edn` says
reproduces on every box without a per-box override. Convenient, but it means the
shipped value follows the largest use case: **`4g` + `3g` is 7G of commitment,
which does not fit a 3.8G host like lucy.**

On a small box, run small and raise only when you rebuild:

1. serve day-to-day with roughly `1536m` heap + `768m` direct — proven on lucy;
2. when a full-corpus rebuild or candidate-index backfill is needed, raise the
   direct cap for that run (or do the rebuild on a larger host and copy the
   store), then drop back.

Either edit `:server` for that box or give it its own alias — the point is that a
tracked value is not automatically a *correct* value for your host, and nobody
re-checked the small boxes when it was raised for the large ones.

### How each number was arrived at (all measured, not guessed)

- **`1g` heap is too small.** ~150M over the live set → G1 death-spiral plus
  swap: the `pool-2` OOM cascade of **2026-07-12/13**, which took HTTP down while
  the listen socket stayed open (see `TN-futon1b-memory-incident.md`).
- **`1536m` heap is too small on a big corpus.** Right-sized 2026-07-14 from
  lucy's `gc.log` (full-corpus live set after GC only ~800–850M, so 1536m gave
  ~44% headroom on 3.8G) — but under the learning-loop workload of
  **2026-07-25/26** it pinned G1 concurrent threads at 99.9% (deep-health 8–25s,
  type sweeps 5–20s) while the box still had ~15G free. Hence `4g`.
- **`768m` → `1536m` → `3g` direct.** Steady-state Arrow allocation sits at
  ~1.45G, so at a 1536m cap a **full-corpus rebuild could not obtain transient
  buffers at any page size** (2026-08-17). Backfill, not tail maintenance, is what
  finds this ceiling — and candidate-index backfill is a C3 lifecycle operation
  that *will* recur. `4g`/`3g` also matches the half-heap/half-off-heap
  provisioning guidance from the Henderson call (2026-08-05).
- **`4g` is ample, not tight.** Measured on Zone 2026-08-17 while serving a
  140,296-document store: RSS 4.88G, heap committed 2.31G of which ~976M live.
  Raising it further buys nothing and lengthens GC.

### MALLOC_ARENA_MAX — the one that isn't a JVM flag

On 2026-08-17, with a full-corpus rebuild running, **glibc malloc arenas held
8.4G of 12.4G RSS and were invisible to `-XX:NativeMemoryTracking`** — NMT
accounted for 5.26G against a 10.96G cgroup anon total. Native allocation from
SQLite/zstd/netty JNI goes through glibc, not the JVM's tracked allocators, so it
looks like a leak the JVM cannot see. Setting `MALLOC_ARENA_MAX=2` cut RSS ~70%
and turned growth *negative*.

If a futon1b JVM's RSS climbs while `jcmd GC.heap_info` and
`VM.native_memory summary` both stay flat, this is why — check arena-shaped
anonymous mappings (~64M each) before hunting a heap leak.

### Cgroup sizing, for reference

Narrow `MemoryHigh`/`MemoryMax` deliberately: over `MemoryHigh` with anon-only
pages and swap exhausted, the kernel reclaims forever and pins PSI, which on
2026-08-17 made `systemd-oomd` kill *other* processes on the box rather than this
one. Zone's `futon1b-dionysus.service` uses `48G`/`50G`; Dionysus used `20G`/`21G`
after that incident. See `futon0/holes/…` and the oomd notes for the full story.

Tests: `clojure -M:node -m test-a1a2` (HTTP smoke suite against an
in-memory node; 26/26 as of `d171150`). `clojure -M:node -m test-query-classes`
measures that every read path keeps the JVM class count flat across distinct
query values (the 2026-08-23 metaspace incident — see `fxt/pq`). Gates for any Clojure change:
`clj-kondo` (0 errors) and `futon4/dev/check-parens.el`.

## Env

| Var | Default | Meaning |
|---|---|---|
| `FUTON1B_ALLOWED_PENHOLDERS` | `api,joe` | L3 write allow-list |
| `FUTON1B_COMPAT_PENHOLDER` | (unset) | fallback penholder when neither body `:penholder` nor `x-penholder` header is present; unset ⇒ such writes 403 |

futon3c-side (consumers of this server):

| Var | Default | Meaning |
|---|---|---|
| `FUTON3C_EVIDENCE_BACKEND` | (unset) | `futon1b` selects the HTTP/EDN evidence backend (wins over direct-xtdb) |
| `FUTON1B_URL` | `http://localhost:7074` | where the backend + watcher dual-write find this server |
| `FUTON1B_PENHOLDER` | falls back to `FUTON1A_PENHOLDER`, then `api` | `x-penholder` the backend sends |
| `FUTON1B_TIMEOUT_MS` | `120000` | http-kit client timeout for backend reads/appends — a limit=1000 evidence scan takes ~19s on lucy and crossed the old 30s ceiling under ingest load (2026-07-13) |
| `FUTON1A_PORT` | `7071` | **`0` disables embedded futon1a entirely** (B3 gate) |
| `FUTON1A_URL` | `http://localhost:7071` | where the stack's substrate HTTP clients point — set to this server for cutover |
| `FUTON1B_EMBED` | `0` (`1` on lucy) | run this server's node + HTTP **inside the futon3c JVM** (I-0 unification, 2026-07-14) instead of the `futon1b-server` unit. When `1`, that unit MUST be stopped — see the finding below |
| `FUTON1B_STORE_DIR` | `~/code/futon1b/switchover-store` | store the embedded node opens. **Per-box**; the default is lucy's. Pin it explicitly on every other box |
| `FUTON_SUBSTRATE_URL` | falls back to `FUTON1A_URL` | canonical substrate authority used by new graph clients; keep the legacy alias equal during migration |

## Findings (hard-won; read before touching the stack)

### XTDB 2.1.0 needs `-Djava.net.preferIPv4Stack=true` (2026-07-10)

2.1.0 submits transactions and queries over an **internal pgwire loopback
connection to "localhost"**. The node binds `127.0.0.1` (IPv4-mapped), but
dual-stack boxes that resolve localhost to `::1` first (lucy) get
`ConnectException: Connection refused` on **every** `xt/q`/`execute-tx` —
the node starts fine, then nothing works. The flag is baked into the
`:node` alias in `deps.edn`; any new alias or external launcher needs it
too. (Boxes that resolve localhost IPv4-first never see this, which is why
it survived the laptop unnoticed.)

### `[*]` projection works on 2.1.0 (broken on 2.0.0)

On 2.0.0, `(from :table [*])` bound nothing (workarounds all over the
migration-era code project the doc's own keys). On 2.1.0 it returns full
docs and composes with `where` — the evidence query layer relies on it,
with exact-match filters pushed down. Old key-projection workarounds are
correct but no longer necessary.

### Arrow column typing is stateful — the F4 rescue ladder (2026-07-10)

A doc can ingest fine on a fresh table and fail after other docs shape the
column's type union ("Unknown type: NULL") — no shape rule predicts it.
Every server write goes through `migration.ingest/put-doc-with-rescue!`
(batch → per-doc → stringify nil-risky values → stringify deep colls) and
is **verified by read-back** (2.0.0 batch puts could drop rows silently).
Backfilling into a store already shaped by live writes will rescue MORE
docs than a fresh-store migration — keep the shape logs.

### Doc-shape transforms (F1/F2/H4, `migration/transform.clj`)

Applied to every write, live and migrated: string-keyed maps (e.g. JSON
bodies) are stringified whole; bare symbols stringified; nested `:hx/props`
denormalized to `:prop/<key>` columns (H4 — XTDB 2 rejects what XTDB 1
tolerated). Read paths must fold `:prop/*` back into `:hx/props` for wire
compatibility (A4 does this for hyperedge reads).

### Fresh-store first-touch queries error (2026-07-10)

XTDB 2.1.0 throws "Not all variables in expression are in scope" when a
query's `where` references a column on a table **no doc has ever been
written to** — on an operational-first (empty) store that's every first
read, including the hyperedge no-op guard's read-before-first-write and
`/health`. All server query paths go through `futon1b-xt/safe-q`, which
maps exactly that error to an empty result. Use it for any new query code.

### Penholder on every write (2026-07-10)

All POST routes are L3-gated. The futon3c operational writers already send
`x-penholder` (from `FUTON1A_PENHOLDER`). The **dormant watcher dual-write
hook does not** — if it is ever revived against this server, set
`FUTON1B_COMPAT_PENHOLDER=api` or add the header at the hook.

### http-kit hands bodies back as streams (2026-07-10)

The futon3c backend must pass `:as :text` on every http-kit request (and
defensively slurp): without it, response bodies arrive as
`org.httpkit.BytesInputStream` and `edn/read-string` dies with a
ClassCastException that *looks* like a server-side problem.

### Protocol semantics live client-side, pushdown only narrows (B1 design)

`futon3c.evidence.futon1b-backend` re-applies the shared
`filter-and-sort-entries` locally on every -query/-count (with
`include-ephemeral=true` requested, so the local filter owns the
ephemeral default). The server's filters are a transfer optimization,
never the decider — keep it that way when adding params, or the three
backends drift.

### Boot recipe with futon1a fully absent (Gate 1, verified 2026-07-10)

```
FUTON1A_PORT=0 \
FUTON3C_EVIDENCE_BACKEND=futon1b \
FUTON1B_URL=http://localhost:7074 \
FUTON1A_URL=http://localhost:7074 \
make dev            # in futon3c; this server on :7074 first
```

The I-evidence-per-turn boot check probes this server's `/health` (live
reachability, stronger than the in-process backends get), and
`boundary/append!` → `verify-persisted` round-trips were verified with
futon1a absent. Lucy caveat: something already holds :6667 there — sort
the futon3c IRC port before a full `make dev`.

### The watchers speak JSON (found live, Phase C Gate 2, 2026-07-11)

futon3c's commit_ingest/file_ingest POST `application/json` with string
keys ("hx/type"); futon1a keywordizes JSON server-side so both formats
hit one code path. The v1 EDN-only server 500ed every watcher write —
**silently**, because the watchers post `:throw false` and only log
statuses they parse. The server is now Content-Type-aware both ways
(cheshire; nested keys keywordized like futon1a; JSON responses when
Accept/Content-Type asks). `test_json.clj` locks the watcher payload
shape. Lesson: "who actually calls this" beats "what the contract says
clients could do" — enumerate callers before triaging format support.

### Query layer at corpus scale (found live, 2026-07-11, post-backfill)

`[*]`-everything reads died at 94k evidence docs (count >60s; the
mission-control poll timed out perpetually). The pattern now: push every
exact/range filter into XTQL where (since/before compare lexicographically
on the ISO strings — the contract's own semantics); scan with a narrow
projection for counting/filtering; hydrate `[*]` only for the sorted
window (`:at`-cutoff second pass). Two traps: a where-column ABSENT from
the projection is an unbound var, which `safe-q` maps to a silently empty
result — projections must include every pushdown column; and hyperedge
repo/source-file filters should use the denormalized `:prop/*` columns.
Post-fix: the killer queries run 7-11s cold on the 94k corpus (client
timeout 30s for headroom).

The 2026-07-22 hardening pass makes boundedness a server invariant rather
than a caller convention. Evidence list responses are cursor pages (default
100, maximum 1000), reject invalid/oversized limits, order only compact
projections, and hydrate at most the selected page. `futon1b-xt/safe-q` owns a
fair process-wide four-query semaphore, so hydration concurrency cannot
multiply across HTTP requests. Corpus scans also share a two-permit admission
gate; contention returns retryable 503 and leaves workers for writes, point
reads, and liveness. See `API-CONTRACT.md` §3 for cursor fields.

After the second 2026-07-22 episode, the JDK HTTP handoff is also bounded to
four running requests plus sixteen queued exchanges. Endpoint membership
queries push `unnest`/`where` and `limit` into XTDB before hydrating full
hyperedges. The serving JVM uses `ExitOnOutOfMemoryError`, allowing systemd's
`Restart=on-failure` to recover instead of retaining a socket-listening zombie,
and production exposes an independent liveness acceptor on port 7072.

### Heap OOM stops ingestion silently-ish; process survives (2026-07-11)

After ~2h of serving-day load (`-Xmx1g`), the node hit
`java.lang.OutOfMemoryError: Java heap space` during log-record
processing. XTDB's response: **ingestion stops permanently and the node
is marked unhealthy, but the JVM keeps running** — so systemd
`Restart=on-failure` never fires, writes 500, and every read that stamps
a current-time basis fails with "system-time (…) is after the latest
completed tx (…)". That error message is the tell: reads demand a basis
newer than the frozen indexer can serve. Remedy per XTDB docs: restart
the node (`systemctl --user restart futon1b-server`); log replay from
the last flushed block took ~60s on the 328k-hyperedge/94k-evidence
store and no data was lost (the append that collided with the OOM was
simply rejected, not half-applied). Open question: a watchdog that
restarts on the unhealthy marker, or a modest `-Xmx` bump once Phase E
frees the second JVM's budget.

**Recurred same day — diagnosis sharpened (Joe pushed back, rightly).**
Incident 1 was airtight heap OOM (`Java heap space`, ingestion stopped;
systemd exit accounting: **2.4G memory peak, 1.1G swap peak** on a
1g-heap JVM). Incident 2: an `OutOfMemoryError` WAS logged (13:44:37,
kind unstated, kotlinx DefaultExecutor) but the user-visible slowness
started ~5min earlier — futon3c's 30s client timeouts at 13:39 were the
run-up, not a dead server: GC grinding near the 1g ceiling (one core
pegged) compounded by box swap pressure (that JVM: **1.9G peak, 511M
swap peak**), each GC cycle paging swapped heap back from disk. The
logged OOM was the end of that spiral, not the start.

Unit now runs `-J-Xmx1536m -J-XX:MaxDirectMemorySize=768m
-J-XX:+ExitOnOutOfMemoryError
-J-Xlog:gc:file=…/gc.log:time,uptime:filecount=2,filesize=10m`
(CLI `-J` opts land after alias jvm-opts, so they win).
**ExitOnOutOfMemoryError closes the zombie mode from incident 1** — the
process now dies on OOM so systemd `Restart=on-failure` actually fires;
the GC log makes the next incident diagnosable in one look (calm at
boot: ~330M used of 559M committed, 12-37ms pauses). If it recurs at
1.5g, stop bumping — treat as Phase E pressure + box-swap problem.

**FTS full-build heap pressure (2026-07-11 evening):** the initial index
build's page scans (compound-keyset order-by over the whole evidence
table, ~94 pages) heat XTDB's caches over ALL rows repeatedly; by ~70%
through, the live set filled the 1536m heap — GC log showed the spiral
plainly (398 Full GCs, 1535M→1535M(1536M), 2.7s pauses; ExitOnOOM never
fired because allocation kept "succeeding" by a hair). Per the
stop-bumping rule: no bigger heap. Remedy = the checkpoint ratchet —
restart with a fresh heap and the build resumes from (at, id), needing
only the tail; repeat if needed, progress is monotone. This is a
ONE-TIME-per-box cost (steady state is the append path); a proper fix
(streaming id+at pass, or cache budget tuning for build mode) only pays
if boxes multiply. GC log at futon1b/gc.log is the diagnostic of record.

**Replay-census caveat:** for ~2min after a restart, `/health` reports
counts from the partially-replayed log (saw 322k hyperedges vs the true
328k) — do not read a low census right after boot as data loss; wait
for replay and re-check (newest-entry GETs are the better probe).

### Layered error envelope

Gate failures return futon1a's envelope
`{:error {:layer N :reason kw :context map}}` with L4=400, L3=403, L2=500,
L1=409, L0=503. The three evidence required-field errors are plain
`{:error "evidence/type required"}` 400s (contract wart, preserved).

## File map

| File | Role |
|---|---|
| `futon1b_server.clj` | HTTP server: routing, hyperedge upsert (stable ids, no-op guard, verified put) |
| `futon1b_gates.clj` | A2: penholder L3, canonical-id L4 (`:mission/doc` contract seeded at boot), error envelope |
| `futon1b_evidence.clj` | A1: evidence write/query/count/sessions/chain |
| `futon1b_graph.clj` | A3/A4/A5: entities, relations, hyperedge reads, census, type registry |
| `futon1b_xt.clj` | `safe-q` — fresh-store-tolerant query helper (see findings) |
| `zai_memory_1b.clj` | Zai memory seam (`memory-search`, `open-store`) |
| `migration/` | export / transform / ingest (rescue ladder) / verify — the data leg |
| `test_a1a2.clj`, `test_a3a4a5.clj` | HTTP smoke suites (26/26, 31/31) |
| `textprobe*.clj` | standalone history extractors for private store copies (M-text-sidecar) |
| `p1_*/p2*/parity_*/s0-s2*` | the original port-era probes and parity harness (E-futon1b-foothold) |

### Required pre-restart gate

Before restarting the substrate, run:

```sh
python3 scripts/pre-restart-check.py --store migration-store-21
```

Exit 0 means the byte-offset backlog is within the calibrated 32 MiB boot
budget; any non-zero exit blocks the restart. Every reported coordinate is
explicitly labelled as bytes: XTDB's submitted and processed message IDs are
byte offsets into `log/LOG`, not message counts. Override the budget only with
an operator-approved measured value via `--max-backlog-bytes`.

At boot, the memory projection waits for submitted and processed byte offsets
to become quiescent, then builds exactly once. The wait is bounded at 10
minutes by default and fails loudly instead of turning a crash into a hang;
set `FUTON1B_PROJECTION_QUIESCENCE_TIMEOUT_MS` only to an operator-approved
measured bound. The former `FUTON1B_PROJECTION_BUILD_ATTEMPTS` retry workaround
is no longer used.

### Unfiltered GET /api/alpha/evidence can kill the JVM (2026-07-13)

`?limit=1` with NO filters crashed the server twice in a row (systemd restart
counter 2→3 territory): `limit` does not bound the underlying `[*]` scan, so
an unfiltered list on the 94k-doc store materializes far too much before the
limit applies (empty reply, then process death). Filtered forms
(`since=`/`author=`/`before=` + limit) return promptly and safely, as do
/count and /text-search (FTS pre-filters). Until the scan is bounded
server-side, treat the unfiltered list as forbidden; page with time windows.
Found while wiring the futon1bi soak harness (claude-1).

### Under `FUTON1B_EMBED`, the systemd unit is the FAULT, not the fix (2026-07-16)

Since the I-0 unification each box is in ONE of two states, and the launcher's
health check has to know which. Get it backwards and the advice is actively
self-defeating:

| `FUTON1B_EMBED` | `futon1b-server` unit | who serves :7074 |
|---|---|---|
| `1` (lucy) | **must be STOPPED** | the futon3c JVM, in-process |
| `0` (chicago, laptop) | **must be RUNNING** | the unit |

XTDB 2 stores are single-process, so under `EMBED=1` a running unit both holds
the store lock and owns :7074 — the embedding JVM then dies on a bare
`Address already in use` from `HttpServer/create` (`futon1b_server.clj:421`).
That bind happens AFTER `zm/open-store`, so the failure costs a full store open
first and names neither the port nor the holder. `dev-linode-env`'s check was
written pre-unification and stayed unconditional: it alarmed on the CORRECT
state (unit stopped) and told the operator to START the unit — which causes the
very collision it warned about. Fixed in futon3c: the check is now embed-gated
on both linode launchers, and `start-futon1b-embedded!` preflights the port
before the store open, naming the holder via `ss`.

**Two traps for whoever unifies the next box:**

1. **The store default is lucy's.** `FUTON1B_STORE_DIR` defaults to
   `switchover-store`. Flip `FUTON1B_EMBED=1` elsewhere without pinning it and
   XTDB 2 does not fail loudly — it CREATES an empty `switchover-store` on that
   box and serves a blank evidence store that looks fine. Strictly nastier than
   the bind error. `dev-linode2-env` now pins `chicago-store`.
2. **The launchers run `set -euo pipefail`.** A guard reading `${FUTON1B_EMBED}`
   aborts the whole script if the var is merely unset, so export it (defaulted)
   before any check reads it.

**Laptop side is still open** (`futon3c/scripts/dev-laptop-env`). Its check is
unconditional but currently CORRECT, because the laptop is unified-exempt: the
unit serves :7073 (not :7074) against `migration-store-21`. Before embedding
there, a laptop agent must (a) gate the check on `FUTON1B_EMBED` the way
`dev-linode-env` does, (b) pin `FUTON1B_STORE_DIR` to `migration-store-21`, and
(c) set `FUTON1B_PORT=7073` — `dev-laptop-env` points `FUTON1B_URL` at 7073 but
never sets `FUTON1B_PORT`, which `bootstrap.clj` defaults to 7074, so the embed
would bind 7074 while the stack talks to 7073. The futon3c regression test
(`futon3c.dev.bootstrap-test/futon1b-health-check-is-embed-aware`) deliberately
covers only the two linode launchers; add the laptop to that list as part of
the same change.
