# TN: a derived secondary-index facility for XTDB2

Technote, 2026-07-26. Status: parked for Joe's headspace — this frames a
possible upstream proposal; nothing here is committed work. Written by
claude-3 after the attribute-index spike; grounded in
`holes/SPIKE-attribute-index-2026-07-26.md` (live EXPLAIN plans + production
journal timings) and the textprobe/futon1bi packet from the #5637 work.

## The observation that prompted this

We now have — or are about to have — **two sidecars solving the same
missing abstraction**:

1. `futon1b_text.clj`: SQLite FTS5 beside the XTDB store, because XTDB2 has
   no text indexing ([#5637], open, "not started").
2. The planned attribute sidecar (spike recommendation 3): SQLite B-tree +
   tag junction tables beside the same store, because XTDB2's `where`
   predicates are *scan predicates over per-column page metadata*, not index
   seeks ([#3663], open, `spec-reqd` — the issue itself calls the metadata
   approach under-selective for unique/near-unique attributes).

Same architecture both times: a derived, rebuildable candidate index whose
answers are **never authoritative** — candidates are point-hydrated and
re-checked against XTDB truth. When one abstraction grows two independent
implementations in one small codebase, the abstraction wants to be native.

## Why #3663 is the frame and #5637 is a backend

Text search and scalar filtering look like different features, and #5637 is
currently scoped as if text were special (Lucene scope, analyzers, data
model all open questions). But everything *around* the index is identical
in both of our implementations:

- declared projection from tables/columns (what gets indexed);
- log/append-driven maintenance with an explicit indexed-through watermark;
- rebuild + compaction lifecycle (our fts5 file is disposable by design);
- a candidate-ID contract: the index proposes, the bitemporal store
  disposes;
- per-field configuration (textprobe's inflation tail — p99 3.80, max 90.5
  on rewrite-heavy histories, and 2.19m postings from low-value HUD
  captures alone — is the argument that per-field inclusion/analyzers are a
  requirement, not a nicety).

Only the innermost structure differs: inverted index with
analysis/positions/ranking for text; hash/sorted/bitmap for scalar
equality/range. That is a **pluggable backend**, not a separate facility.
Proposing "text indexing" and "secondary indices" as one derived-index
facility with two backends gives JUXT one spec to review instead of two,
and gives us one lifecycle to operate instead of N sidecars.

## The evidence packet (what makes this more than an opinion)

From the spike, all reproducible on `migration-store-21` (~400k hyperedges,
~110k evidence, ~46k entities):

- Live `EXPLAIN` on XTDB 2.1.0 shows `scan` operators with predicates for
  `evidence$author`, `evidence$at`, `hx$type` — pushdown without seeks.
- Cold bounded equality reads: 2–3s for ten rows (`hx$type` sweeps across
  three distinct types: 2.9s / 2.1s / 2.4s cold; 8ms on exact repeat via
  the JVM cache).
- Production journal: typed sweeps 5–28s; non-PK entity fallback reads
  20–30s with client disconnects; the same maintained-index lookups serve
  at 8–35ms.
- Text side (textprobe, 131,807 histories): posting inflation aggregate
  1.028 entities / 1.000 evidence, but p99 3.80 on the rewrite-heavy tail;
  graph text 788k postings, evidence prose 1.45m.
- The candidate+recheck contract is proven in production shape: the fts5 D1
  run indexed 94,430 evidence rows, 10/10 scan-oracle, ~4s live-append
  staleness. `futon1bi` is the reusable extraction (spec-driven, oracle,
  freshness stamp; deferred work is operational wrapping, not the core).

The new contribution relative to the existing #5637 packet is the *scalar*
half: an operationally observed workload where the absence of #3663 costs
seconds-to-tens-of-seconds on bounded queries, with the same store serving
equivalent lookups in milliseconds when any maintained structure exists.

## The proposal sketch (five requirements)

A native **derived secondary-index facility**:

1. **Declared projections** — user names table/columns to index, including
   multivalued fields (tags, endpoints).
2. **Log-maintained** — driven by the transaction log with an explicit
   indexed-through snapshot/watermark; rebuild and compaction are lifecycle
   operations, not emergencies.
3. **Candidate-ID contract** — index answers are candidate sets resolved
   through XTDB's bitemporal machinery; store truth stays authoritative.
   (This is the load-bearing safety property; it is what let us ship fts5
   without consistency anxiety, and it is what makes staleness a bounded
   performance question instead of a correctness one.)
4. **Pluggable backends** — scalar structures and inverted-text indexes
   under one lifecycle; #5637 becomes the text backend.
5. **Per-field configuration + selectivity statistics** — opt-in per
   column/analyzer, with enough stats for the planner to prefer a seek over
   a scan when one exists.

## What to ask JUXT (when there is headspace)

- Validate the candidate-ID + snapshot-watermark contract *before* anyone
  invests in in-core structures — it is the piece that determines whether
  derived indexes can be non-authoritative (cheap to trust) or must be
  transactional (expensive to build).
- Whether #3663's "user-specified secondary indices" spec work is open to
  the two-backend framing, i.e. folding #5637's requirements in rather than
  running the specs separately.
- Whether an out-of-core reference implementation (our generalized sidecar,
  requirement-shaped) would be useful to them as a de-risking prototype —
  we are building it for ourselves anyway (spike recommendation 3,
  evidence-first because append-only).

## Relation to in-house work

- Near-term store fixes (amplification + projection coherence) are separate
  and dispatched — they need no new index and land regardless.
- The sidecar generalization (one derived-index subsystem, evidence scalar
  attributes + tag junction first) proceeds on our side and doubles as the
  reference implementation above.
- The D2 packet (#5637) remains Joe-gated; this technote is the material
  that would extend it into the #3663 frame.

## Write-path authorization gap

The same missing-facility problem has a write-side consequence. XTDB 2.1.0
must expose its pgwire server because the ordinary Clojure `xt/q` and
`xt/execute-tx` API reaches the node through
`Xtdb.createConnectionBuilder`; `xtdb.node.impl/createConnectionBuilder`
delegates to the loopback pgwire `DataSource`. The direct Java
`Xtdb.executeTx` method bypasses pgwire, but futon1b does not use that API.
Consequently a pgwire password gate must authenticate both futon1b and
external diagnostic clients.

The apparent user-table solution is not safe to ship in XTDB 2.1.0:

- `xtdb/authn.clj:17-29` verifies a password by selecting
  `SELECT passwd ... FROM pg_user WHERE username = ?` and taking the first
  row. Rules in `xtdb/authn.clj:31-55` can choose trust or password by user
  and remote address, but have no deny/read-only method. Remote address
  cannot distinguish local clients here.
- `xtdb/pgwire.clj:72-73` constructs the node's own connection builder with
  only localhost and port. `xtdb.api.ServerConfig` has host, ports, thread
  count, and TLS settings but no internal-client username/password.
- The built-in `xtdb` account is recreated as a template row with the known
  password `xtdb` on every node start
  (`xtdb/information_schema.clj:334-359`). `ALTER USER` writes another
  live-index row (`xtdb/indexer.clj:519-580`) rather than replacing that
  template across restart.

A bounded local reproduction confirmed the failure rather than merely
inferring it. A node was bootstrapped with pgwire disabled, its built-in
password changed through direct `Xtdb.executeTx`, then reopened with a
catch-all `{:method :password}` rule. On the first opening, no-password
connections were refused and the generated password could run `EXPLAIN`
and application writes. After close/reopen, authentication with the same
secret failed. A trusted diagnostic query then returned two `pg_user` rows
for username `xtdb`: one hash verified the generated secret and the other
verified the known default. Password authentication chooses one with no
ordering. Reapplying `ALTER USER` before every pgwire start still failed
the restart acceptance test. This can both lock out futon1b and retain an
impersonable default credential, so it is not a real gate.

This is a second native request beside the derived-index facility:
**pgwire write authorization**. Minimally, XTDB needs either:

1. server rules that can deny writes (and ideally distinguish read-only
   from read-write) independently of authentication, including a way for
   the embedded node client to carry credentials; or
2. a persistent user-table implementation in which the bootstrap account
   is replaced deterministically, plus internal-client credential fields
   in node/server configuration.

The strongest available mitigation today is OS isolation, not an XTDB
configuration illusion: run futon1b under a dedicated Unix account, pin
the pgwire port, and install an owner-aware loopback firewall rule that
permits that account only. Merely binding to `127.0.0.1`, or a firewall
rule without process-owner separation, does not protect against other
local processes running as Joe. This operational change needs Joe/root
coordination and should be tested against the server's own loopback calls
before activation.

A tx-count tripwire was considered but is not a small sound patch in the
current server. There is no durable server write log, and write
transactions are issued at several independent call sites in
`futon1b_graph.clj`, `futon1b_server.clj`, and the shared rescue-ingest
helpers used by `futon1b_evidence.clj`.
Comparing `xt.txs` with an in-memory counter would false-alarm after
restart and can miss or misclassify multi-operation transactions. A useful
tripwire therefore requires first centralizing transaction submission and
persisting an application-issued transaction ledger; it should not be
presented as authorization.

[#3663]: https://github.com/xtdb/xtdb/issues/3663
[#5637]: https://github.com/xtdb/xtdb/issues/5637
