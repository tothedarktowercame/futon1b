# TN: a current-time hyperedge-type candidate index

Technote, 2026-08-23. Status: design and costing only; no query or write path
was changed. This is follow-up 6 from
`TN-futon1b-hyperedge-walk-latency-2026-08-23.md` section 5 and should be read
with `TN-futon1b-sql-in-hyperedge-hydration-2026-08-23.md` and
`TN-xtdb-derived-secondary-index.md`.

## Recommendation

**Build a narrower version:** an in-process, current-time candidate index from
normalized `hx/type` to ordered hyperedge IDs. Use it only for bounded,
type-only reads with neither `valid-as-of` nor `system-as-of`. Point-read and
recheck every candidate against XTDB. Route historical reads, endpoint reads,
unbounded reads, and any read whose freshness coordinate cannot be certified
through the existing XTDB query.

Do **not** put a vector of IDs in one XTDB document per type. That turns every
single-edge mutation into a read/replace of a potentially large document,
creates a hot lost-update boundary, and retains each replaced vector in XTDB's
bitemporal history. A row per `(type, edge-id)` does not help either: looking
up its non-key type repeats the scan defect. Fixed bucket documents reduce the
rewrite size but add a second durable index protocol without solving temporal
or watermark ownership. The broader log-maintained sidecar proposed in
`TN-xtdb-derived-secondary-index.md` remains the right general facility; it is
more machinery than the cold cascade path currently warrants.

Do not add `entity/type` to the first increment. A type index only produces
entity IDs; it does not fix their hydration. The live plan in the SQL-IN note
shows three entity IDs still scan 49,196 rows in 1.718 s, while one point read
costs 0.278 s. A useful entity index therefore needs a separately costed
projection (or a native secondary index), not a copy of the hyperedge change.

## Evidence and scope

The committed live measurements are:

| operation | evidence | cost |
|---|---|---:|
| current `hx/type` page selection | parent note section 4 | about 1.1 s scan floor per page |
| hydrate 100 hyperedges, four point reads in flight | SQL-IN note | 1.880--2.283 s warm |
| `_id = ?`, hyperedges | SQL-IN addendum | 1 page, 1 row, 0.045 s |
| `_id IN (?)`, hyperedges | SQL-IN addendum | 2,962 pages, 508,013 rows, 2.491 s |
| current corpus | dispatch measurement | 509,498 hyperedges, 258 types |

At 20 pages, eliminating only the repeated type scan removes roughly 22 s
from the cold graph walk. It does not remove hydration, so the predicted cold
floor for the longest 20-page walk remains roughly 38--46 s if it runs alone;
the graph's two-worker schedule and differing page counts make that figure a
cost component, not a prediction of endpoint wall time. Applied to the
measured 27.2 s cold graph, the optimistic lower bound is about 5.2 s, but the
implementation acceptance bar should be empirical: same live request, cold
cache, complete rows, before and after. Warm repeat remains approximately
0.29 s and should not materially improve.

I attempted to refresh the corpus cardinalities directly as required, using:

```text
python3 /tmp/pgprobe.py "SELECT COUNT(*) AS n, COUNT(DISTINCT CAST(hx\$type AS VARCHAR)) AS types, AVG(CHAR_LENGTH(_id)) AS avg_id_chars, MAX(CHAR_LENGTH(_id)) AS max_id_chars FROM hyperedges" 60
```

On 2026-08-23 the connection to `127.0.0.1:34257` failed immediately with
`ConnectionRefusedError: [Errno 111] Connection refused`. No fixture number was
substituted. All quantities below therefore state their assumptions and use
the packet's 509,498/258 live measurement.

## Proposed representation and read contract

Keep one atom per node containing:

```clojure
{:revision 17
 :source-offsets {<log-partition> <processed-byte-offset>, ...}
 :built-at "..."
 :by-type {:mission-scope/pattern <ordered-set-of-string-ids> ...}
 :type-by-id {"<edge-id>" :mission-scope/pattern ...}}
```

`by-type` makes selection proportional to the requested page rather than the
corpus. `type-by-id` makes a type-changing put and an id-only retraction remove
the old membership without a corpus search. The first implementation should
use an immutable sorted set per type for simple atomic publication and stable
`after` paging. If profiling shows its object overhead is unacceptable, replace
each set with an immutable sorted ID vector plus a small per-type delta; do not
trade stable ordering or atomic publication for memory.

For an eligible request, seek after the cursor in the ordered set, take at
most `limit`, then perform the existing `_id = ?` point reads. Recheck that
each returned document has the requested normalized `hx/type`, is current,
and satisfies `repo`/`source-file`; omit stale candidates and continue taking
candidates until the page is full or the set is exhausted. This continuation
is necessary: filtering only the first `limit` candidates silently shortens a
page. Exact totals are not provided by this candidate path when an additional
filter is present. `include-total=true` should initially use the old path;
`include-total=false`, which is what the cascade walker uses, gets the index.

The index is a candidate accelerator, never authority. Rechecking eliminates
false positives, but it cannot recover false negatives. Freshness gating below
is therefore mandatory rather than an optimization.

## Bitemporality and freshness

This index represents one thing only: the current valid-time/current
system-time view. Any `valid-as-of` or `system-as-of` request bypasses it. A
future general index would need interval-bearing memberships keyed by both
valid and system ranges; pretending that current membership answers those
queries would be a correctness bug.

Bootstrap before the HTTP listener accepts traffic:

1. Wait until every submitted log byte offset has been processed.
2. Capture the **per-partition processed byte-offset map**, stripped of
   volatile observation timestamps.
3. Scan only `[xt/id hx/type]`, build private maps, and capture the processed
   offsets again.
4. Publish atomically only when the two offset maps are equal; retry boundedly
   otherwise. If the bound is exhausted, leave the index unavailable and
   serve the old scan path.

This deliberately does not copy `node-watermark`'s whole-map comparison in
`futon1b_graph.clj:1012-1023`. The nearby comment records 295 rebuild failures
on a quiet store because a changing `:system-time` made logically identical
progress coordinates unequal. Nor is a single maximum offset sufficient: an
advance in one partition can be hidden by a larger unchanged offset in
another. Compare the normalized per-partition byte coordinates.

After every verified hyperedge put/retract, update membership and publish the
processed offset coordinate inside the same application mutation lock. A
current read serves the index only when its recorded coordinates equal the
node's processed coordinates. A direct pgwire write, a missed application
path, or replay advancement changes that coordinate; the next read must mark
the projection stale and fall back to the authoritative scan while a bounded
rebuild is scheduled. This is the property that prevents an externally added
edge becoming an invisible false negative. Offset/status read failure also
falls back; it must never be interpreted as equality.

Concurrent writes after a successful coordinate check may linearize after the
read. The mutation lock must cover the check and candidate snapshot so the
application does not publish a store mutation without either updating or
invalidating the projection.

## Restart and disagreement

The projection is disposable and is rebuilt on each restart. It adds one
narrow current scan and transfer of 509,498 `(id,type)` pairs; it does not scan
or hydrate full documents and does not replay the 6.3 GB log itself. Its live
build time remains unmeasured because pgwire was unavailable. The implementation
packet should measure this separately from the existing 44 s restart and set a
bounded startup budget. Failure or timeout must start the service with the
index unavailable, not with a partial index certified current.

At runtime, disagreement is handled asymmetrically:

- stale extra IDs are removed by point recheck and trigger a repair/rebuild;
- a freshness-coordinate mismatch may imply missing IDs, so the entire indexed
  answer is rejected and the scan path is used;
- periodic sampled comparison can diagnose drift but cannot certify absence,
  and therefore cannot replace the coordinate gate.

Expose revision, build duration, membership count, type count, source offsets,
fallback count/reason, stale candidates, and rebuild failures on a watched
status surface. This makes the next incident visible without an unwatched HTML
page.

## Write-path cost

The existing upsert already performs a no-op read and a verified read-back
(`futon1b_server.clj:124-177`); general retraction resolves type before delete,
executes and verifies the transaction, then invalidates caches
(`futon1b_graph.clj:211-280`). The proposed synchronous work is one old-type
lookup in `type-by-id`, removal/insertion in one or two type sets, one map
update, and one status-coordinate capture. It introduces no XTDB transaction.

For a balanced immutable set the CPU work is `O(log n_type)` plus structural
allocation. With 971 members, that is about ten comparisons; even with a type
of 100,000 members it is about seventeen. The expected local cost is below a
millisecond, but that is an estimate, not a measurement. Acceptance should be
p50/p95 over at least 100 no-op-excluded puts and retractions on a disposable
type, reporting the existing transaction/read-back time separately; require
less than 5 ms p95 incremental projection time and no measurable regression
beyond noise in end-to-end p95. Do not weaken verified writes to meet it.

## Resident-memory cost

There is one membership and one reverse entry per current hyperedge. A Java or
Clojure object-rich implementation should be budgeted at **160--280 bytes per
edge across both maps**, including a string reference/object, tree/hash nodes,
map structure, and alignment. At 509,498 edges that is approximately
78--136 MiB, plus type/set roots and transient rebuild duplication. Atomic
rebuild temporarily holds old and new projections, so reserve **160--275 MiB
peak**. These are engineering bounds, not heap measurements; ID length could
not be refreshed while pgwire was down.

On the 4 GiB heap this steady-state estimate is 1.9--3.3%, and peak is
3.9--6.7%. It is comparable to, but separate from, the query cache's documented
115 MiB worst case. Given today's GC/OOM incident, implementation must measure
retained size with a class histogram before and after live build and reject the
feature (fall back to scans) if steady retained growth exceeds **160 MiB** or
bootstrap peak exceeds **300 MiB**. Interning IDs or keywords dynamically is
not an acceptable way to hide the cost.

## Decision and implementation acceptance bar

Build only the current, bounded, `include-total=false`, type-only candidate
index. It directly removes the approximately 1.1 s repeated scan from the
cold pages that dominate the cascade while retaining XTDB point reads as the
truth boundary. Do not initially serve historical, endpoint, exact-total,
unbounded, or entity-type queries.

The implementation is worth landing only if the live-store packet proves all
of the following:

1. identical complete ordered pages and cursors against a forced-scan oracle,
   including type-changing puts, direct-write freshness mismatch, retracts,
   repo/source filters, and empty types;
2. no index use for either temporal coordinate or an unverified watermark;
3. cold type selection below 50 ms/page and lower cold cascade wall time,
   with the actual number reported even if 5.2 s is not reached;
4. startup build duration reported, retained growth at most 160 MiB and peak
   at most 300 MiB;
5. less than 5 ms p95 synchronous projection overhead, without removing the
   current no-op or read-back checks.

If freshness cannot be proven from normalized per-partition processed offsets
for every write surface, do not ship an in-memory answer. In that case the
general log-maintained derived-index facility is the next honest design, not a
cache that can silently omit new edges.
