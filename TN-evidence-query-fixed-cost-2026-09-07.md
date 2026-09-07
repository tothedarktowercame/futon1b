# TN — /api/alpha/evidence carries a ~850ms fixed cost per request

**Status: measurement, 2026-09-07.** Written while tracing why the APM campaign's
promotion phase was losing throughput. The contention turned out to originate
here rather than in the campaign, so the numbers are recorded in the repo that
owns the endpoint.

## The finding

A query that matches nothing and scans nothing still costs ~850ms.

```
GET /api/alpha/evidence?session-id=no-such-session-xyz&since=…&before=…&limit=1000
  :count 0  :scanned 0            853ms
```

`:scanned 0` is the important part. `bounded-window` did no projection scanning
and no hydration, and the request still took most of a second. The cost is
per-request engine overhead — plan construction, temporal resolution, Arrow
setup — not data volume.

Measured against the same store, same minute:

| request | rows | elapsed |
|---|---|---|
| `evidence?session-id=<absent>&limit=1000` | 0 (`:scanned 0`) | 853ms |
| `evidence?session-id=<absent>&limit=1` | 0 | 849–1273ms |
| `evidence?session-id=<present>&limit=100` | 100 (`:scanned 100`) | 2176–2576ms |
| `evidence?session-id=<present>&limit=1000` | 640 (`:scanned 640`) | 5253–5377ms |
| `evidence?session-id=<present>&limit=100`, page 2 by cursor | 100 | 3293ms |
| `evidence?limit=1` (no session filter) | 1 (`:scanned 1`) | 3664ms |
| `evidence/count?session-id=<absent>` | — | 1242ms |
| `hyperedges?type=memory&limit=1&include-total=false` | 1 | 3ms |
| `/health` | — | 3–4ms |

Two things follow.

**The session-id pushdown works.** It is absent from
`requires-post-filtering?`, so it is a where-column rather than a post-filter,
and the numbers agree: filtering to an absent session costs 853ms while the
same query with no session filter at all costs 3664ms to return a single row.
Pruning is happening. The floor is underneath it.

**A comparable endpoint on the same substrate answers in 3ms.** Whatever the
~850ms is, `hyperedges` does not pay it. That is the gap worth closing.

## Why it mattered downstream

futon1b's permit pool is two, and the APM promotion's visibility reads share it
with `futon3c.inbox-zero.infer-adapters/fetch-session-evidence`. The promotion's
per-read bound is 5000ms of **wall clock including queue wait**, so a co-tenant
holding a permit for 5.3s can fail a read, which fails a candidate, which fails
a publication, which parks a frame. That is f188's
`:memory-snapshot-visibility-not-obtained`.

Tap sampling of the campaign (futon3c `scripts/apm-tap.sh probes`) shows the
shape from the other side — this is queueing, not slow service, which is why the
median is near zero while the tail is seconds:

```
f189 promote-solver            n=55 min=0  p50=1    p90=1323 max=1791 saturated=27/55
f189 memory-cascade-operation  n=24 min=89 p50=1265 p90=2319 max=4424 saturated=22/24
```

## What was NOT the fix

Lowering the sweeper's `limit=1000` to 100. It is not a page size: the caller
kept only the first page and never read `:next-cursor`, so a lower limit
silently truncates, and the consumer derives an activity window from the first
and last entry — a dropped tail reports a window the session never ended at.
Fixed in futon3c `19b98d41` by following the cursor when one is offered.

Paging unconditionally is also the wrong lever *because* of the fixed cost:
seven pages of 100 pay the ~850ms floor seven times, roughly four times the
gate occupancy for identical rows. On a two-permit pool that is worse, not
better.

## The actual lever

Make the floor smaller. At 3ms — what `hyperedges` already achieves — none of
the contention above exists at any limit, cadence, or fan-out, and the retry
and cursor mitigations layered on top of it become unnecessary.

Not attempted here: this note records the measurement and stops at the boundary
of the query engine.
