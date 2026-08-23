# TN-futon1b-hyperedge-walk-latency-2026-08-23

**Technical note — `GET /api/alpha/cascade-real/graph` takes 42–56 s because a
typed hyperedge walk costs ~2 s per 100-row page, and nothing amortises it.**
Author: Claude Fable 5 (with Joe), 2026-08-23. Predecessors:
`TN-futon1b-gc-wedge-incident-2026-08-23.md` (same route family, admission
permits), `../TN-futon1b-memory-incident.md`, memory note "fetch-entity miss
path" (27 s 404s, fixed `ee8f41e`). §5 lists follow-ups with acceptance bars.

## 1. Symptom

```
curl -s -o /dev/null -w 'HTTP %{http_code}  %{time_total}s %{size_download}B\n' \
  http://localhost:7070/api/alpha/cascade-real/graph
HTTP 200  42.167477s  252137B
```

Reproduces every call: 41.5 s, 42.2 s (Joe), 56.2 s (this session, under
concurrent traffic). The response is 252 KB. Nothing errors, nothing sheds,
`/health` is `{:ok true}` throughout. The route is the structure behind the
pipeline-pattern-cascade body; at this latency it is unusable interactively
and at risk of any 30–60 s client timeout.

## 2. Where the time goes

`cascade-real-graph` (`futon3c/src/futon3c/logic/cascade_real_live.clj:457-462`)
makes seven `fetch-edges` calls **sequentially**, each a full typed walk via
`substrate/hyperedges-by-type` paged at `substrate-page-size` = 100
(`futon3c/src/futon3c/substrate/client.clj:84`).

| type | rows | pages | first page |
|---|---:|---:|---:|
| mission-scope/pattern | 971 | 10 | 1.84 s |
| code/v05/mined-move | 177 | 2 | 2.78 s |
| held/on-mission | 124 | 2 | 2.73 s |
| cascade/cluster-member | 117 | 2 | 1.91 s |
| clock/clocked-on | 67 | 1 | 1.86 s |
| cascade/hole-target | 0 | 1 | 0.76 s |
| mine/meme | 0 | 1 | 1.24 s |

20 sequential page requests × ~2 s ≈ 41 s. That is the whole budget.

Direct measurement against :7073 (`/api/alpha/hyperedges?type=…&limit=N&include-total=false`):

| request | time |
|---|---:|
| `mission-scope/pattern` limit=1 | 1.17 s |
| `mission-scope/pattern` limit=10 | 1.38 s |
| `mission-scope/pattern` limit=100 (cold) | 3.09 s, 239 KB |
| same, immediate repeat (cache hit) | 0.006 s |
| `cascade/hole-target` (0 rows) | 1.11 s |
| same, +1 s | 0.0006 s |
| same, +2 s / +3 s / +5 s | 1.13 / 1.04 / 0.92 s |

## 3. Root causes — four, stacked

**(a) ~1.1 s fixed floor per page: the type-window scan is unindexed.**
`hyperedges-query-uncached` (`futon1b_graph.clj:749`) selects the window with
`(from :hyperedges …) (where (= hx/type p-type)) (order-by xt/id) (limit p-limit)`.
XTDB 2 has no secondary index on `hx/type`, so this is a scan of the whole
hyperedges table regardless of `limit` — `limit=1` and an empty type both pay
it. This is the same class of cost as the 27 s `fetch-entity` miss path: a
predicate the store cannot serve from an index.

**(b) ~2 s per full page: per-document hydration, 100 point lookups at
4-way concurrency.** `hydrate-hyperedge-window` (`futon1b_graph.clj:736`)
issues one `(where (= xt/id p-id))` query per row, `partition-all 4`. This is
deliberate (comment at :705 — the batched variadic-`or` form regressed badly on
2026-08-02 because a disjunction of id equalities is not index-backed in
XTQL, and XTQL has no `IN`; SQL `WHERE _id IN (?,?)` does work, per
`probe_in_predicate.clj`). So a page is 1 scan + 25 serial batches of 4 point
reads ≈ 1.1 s + 1.9 s.

**(c) The materialised-window cache is reset every ~1–2 s.** `hyperedges-query`
caches bounded `include-total=false` windows and does hit (6 ms on repeat).
But `invalidate-hyperedge-query-cache!` is called on **every** hyperedge write
or retract (`futon1b_server.clj:137,171`) and resets the *entire* cache, not
the written type's entries. With agents writing `clock/clocked-on`,
`memory/assert`, etc. continuously, the measured cache lifetime is 1–2 s — far
shorter than a 40 s walk. Net effect: the cache is correct and useless for
this workload.

**(d) Client does 20 sequential round-trips and discards >90 % of the bytes.**
The seven walks are independent but run one after another; and
`mission-scope/pattern` alone is 971 full hydrated documents (2.66 MB) of which
the graph keeps `{mission, pattern, ident, state, relation}` — roughly an
order of magnitude more payload than needed (Joe's observation). The endpoint
has no projection parameter; that is a futon1b API change, not a cascade one.

## 4. XTDB-specific findings (for the #5637 / secondary-index thread)

- Filter-by-type over a single wide table is a full scan; every "list the
  edges of type T" in the stack pays ~1 s at the current ~300k-doc scale and
  grows linearly. `TN-xtdb-derived-secondary-index.md` already argues for a
  derived index; this is the live cost of not having one.
- The missing XTQL set-membership predicate forces N point lookups where one
  `_id IN (…)` would do. Worth re-probing with the SQL surface now that the
  re-check bridge in `mathse-xtdb-benchmark/recheck.clj` uses exactly that
  form (`WHERE _id IN (…)`, 50–110 µs/candidate warm there — on a much
  smaller table).
- Whole-cache invalidation on any write is a design choice, not an XTDB
  limit; XTDB's system-time makes per-type or `system-as-of`-keyed caching
  trivially safe.

## 5. Follow-ups (one behaviour each; review between)

1. **Scope cache invalidation to the written `hx/type`.** Key the cache by
   type; on write, drop only that type's windows. Acceptance: second
   `cascade-real/graph` call within 60 s completes in < 5 s while unrelated
   writes continue; existing cache-correctness tests pass.
2. **Parallelise the seven walks in `cascade-real-graph`.** `pmap`/futures
   over the fetch calls, bounded by the :7073 permit count (2). Acceptance:
   cold graph ≤ max single walk (~20 s for mission-scope/pattern) not the sum.
3. **Projection parameter on `/api/alpha/hyperedges`** (`fields=` or
   `props=`) so callers can skip hydration of `:hx/props` they don't read.
   Acceptance: `mission-scope/pattern` walk payload < 300 KB; contract §4
   updated; hydration skipped entirely when requested fields are all in
   `hyperedge-window-cols`.
4. **Re-probe batched hydration via SQL `_id IN`** on the live store, against
   the 2026-08-02 measurement that retired the XTQL `or` form. Discovery only;
   report ms per 100 ids at 4-way vs one `IN (100)`.
5. **Raise the page size for this caller** only after 1–3: the comment at
   `client.clj:74-80` records the budget reason for 100 (timeouts at 250).
6. **Longer term:** type-keyed derived index (`TN-xtdb-derived-secondary-index.md`)
   removes the 1.1 s floor for every typed read in the stack, not just this one.

Not in scope here: the `/api/alpha/hyperedges?end` holder seen in `/health`
during measurement (701 ms, pool-2-thread-1) is the ordinary permit traffic,
not this incident.
