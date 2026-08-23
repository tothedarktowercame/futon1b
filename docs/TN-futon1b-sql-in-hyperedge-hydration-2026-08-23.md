# TN — SQL `_id IN` does not improve live hyperedge hydration

**Date:** 2026-08-23  
**Status:** read-only live-store measurement; no production change  
**Question:** follow-up 4 of
[`TN-futon1b-hyperedge-walk-latency-2026-08-23.md`](TN-futon1b-hyperedge-walk-latency-2026-08-23.md)
§4/§5.

## Verdict

**No: a SQL `_id IN (…)` batched hyperedge hydration is not worth building.**
On the live hyperedges table XTDB plans it as a semi-join over a full table
scan, not as primary-key lookups. At the actual 100-document page size it was
slower than the current four-way point path in every measured pass.

## Method

All queries went directly to pgwire at `127.0.0.1:34257` (NOTE: that
port is ephemeral and changed to `44505` at the 20:13 restart -- see the
operational addendum in `TN-futon1b-current-hyperedge-type-index-design-2026-08-23.md`;
discover it from the service pid, never hardcode it); no HTTP endpoint or
expensive-read permit was used. A temporary raw PostgreSQL-protocol client set
a 60-second socket deadline for every connection. It first selected bounded,
ordered `_id` samples for each type, then compared:

- `SELECT * FROM hyperedges WHERE _id = '<id>'`, four concurrent requests;
- one `SELECT * FROM hyperedges WHERE _id IN ('<id-1>', …)`.

The point baseline was run both with four persistent pgwire workers and in the
current code's closer shape: consecutive batches of four, with a fresh pgwire
connection per point query. Every result was checked by exact set equality,
row count, and absence of duplicate IDs. All checks below were complete and
correct. `SELECT *` is intentional: this is a hydration comparison, not an ID
projection comparison.

The wide type was `mission-scope/pattern` (971 current rows, about 2.5 KB per
document in the HTTP observation). The narrower type was `clock/clocked-on`
(67 current rows). These counts were checked on the live store before probing.

The first pass followed the bounded ID-selection scan and is the first observed
document-body pass, not a process-restart cold run. The second pass was an
immediate repeat and is labelled warm. The HTTP materialised-window cache was
not involved in either pass.

## Measurements

Times are wall-clock milliseconds. `rows/complete` was `N/true` for every
entry.

### `mission-scope/pattern` (wide)

| Shape | N | First pass | Immediate repeat |
|---|---:|---:|---:|
| point lookups, four persistent workers | 100 | 2,066 | 1,134 |
| point lookups, fresh connections in batches of four | 100 | — | 1,273 |
| one SQL `IN` | 10 | 2,524 | 2,260 |
| one SQL `IN` | 50 | 2,639 | 2,646 |
| one SQL `IN` | 100 | 2,737 | 2,829 |
| one SQL `IN` | 250 | 2,834 | 3,792 |
| one SQL `IN` | 500 | 2,669 | 3,396 |

Thus the proposed 100-ID statement was 1.3× slower than the first point pass,
2.5× slower than the warm persistent-worker pass, and 2.2× slower than the
fresh-connection four-at-a-time reproduction. Increasing N mostly amortises a
large fixed scan; it does not make the page-of-100 case competitive.

### `clock/clocked-on` (narrow)

| Shape | N | First pass | Immediate repeat |
|---|---:|---:|---:|
| point lookups, four persistent workers | 67 | 1,177 | 860 |
| point lookups, fresh connections in batches of four | 67 | — | 1,022 |
| one SQL `IN` | 10 | 2,625 | 2,788 |
| one SQL `IN` | 50 | 2,683 | 3,038 |

The narrow type shows the same fixed-cost shape. It has only 67 rows, so 100,
250, and 500 are not meaningful type-local samples; the full requested sweep
was performed on the 971-row wide type.

## Plan

XTDB accepts plain `EXPLAIN` and `EXPLAIN ANALYZE` for this SQL form. It rejects
PostgreSQL's `EXPLAIN (FORMAT JSON)` syntax.

For even a one-ID full-document query, the relevant operator tree was:

```text
project
  project
    semi-join  condition: hyperedges.1/_id = xt.values.3/_column_1
      rename
        scan   table: xtdb.public.hyperedges; predicates: []
      rename
        table
```

`EXPLAIN ANALYZE` measured the hyperedges scan at **2.462744 s**, **2,962
pages**, and **508,013 rows**; the whole query took **2.490814 s** and returned
one row. The values side took 0.000078 s. The scan has no pushed predicate.
This directly answers the planning question: SQL `IN` here is a set-valued
semi-join over the entire wide table, not a batch of IID-index probes.

The plan also explains why N=10 through N=500 cluster near 2.3–3.8 seconds and
why document width matters: `SELECT *` projects the union of the hyperedges
table's many sparse `prop$*` columns during the scan.

## Consequence

Keep `hydrate-hyperedge-window`'s bounded point lookups. SQL `IN` is useful in
some narrower tables and workloads, but that does not transfer to full
documents from this live hyperedges table. The larger wins remain the other
follow-ups in the parent latency note: avoid hydration through projection,
remove the unindexed type-window scan, and make caching useful.

This closes the open SQL question recorded in
[`TN-xtdb2-query-ceilings-and-ingest-memory-2026-08-02.md`](../TN-xtdb2-query-ceilings-and-ingest-memory-2026-08-02.md):
the SQL syntax works and returns complete data, but its live hyperedge plan is
the same wrong scan class as the reverted XTQL batching for this workload.

## Addendum (claude-13 review, 2026-08-23): it is a crossover, not a transfer failure

The verdict above is right for the case it measured, but the "Consequence"
paragraph generalises it wrongly, and it leaves a live contradiction unexamined:
`fxt/hydrate-by-ids` (`futon1b_xt.clj:189`) is a shipped `_id IN` hydration,
landed the SAME DAY (`523c2ac`, `ee8f41e`) as the fix for the 27 s
`fetch-entity` miss path, whose docstring says `_id IN (1351 ids)` takes ~1 s
and beats point lookups. A reader hitting both notes sees a flat contradiction.

Both are correct. The planner facts, measured here:

| query | table | pages | rows scanned | time |
|---|---|---:|---:|---:|
| `WHERE _id = '<id>'` | hyperedges | **1** | **1** | 0.045 s |
| `WHERE _id IN (1 id)` | hyperedges | 2,962 | 508,013 | 2.491 s |
| `WHERE _id = '<id>'` | entities | **1** | **1** | 0.278 s |
| `WHERE _id IN (3 ids)` | entities | 989 | 49,196 | 1.718 s |

**XTDB pushes `_id =` into the scan as a key seek; it does not push `_id IN`.**
`IN` becomes a materialised values table joined by semi-join over a completely
unfiltered scan — `predicates: []` — on *any* table, not just this one. That is
a planner property, not a hyperedges property, and it is the sharper statement
of §"Plan" above.

So the cost model is:

    IN     ~ scan_cost(table)              -- fixed, independent of N
    points ~ N * per_id_cost / concurrency -- linear in N

and the crossover is `scan_cost / effective_per_id_cost`:

- **hyperedges**, 508k rows: scan ~2.5 s, points ~11 ms effective (measured
  above, 1,134 ms warm / 100). Crossover ≈ 220 ids. A page is **100** — points
  win, which is exactly what the sweep shows and why the verdict is *no*.
- **entities**, 49k rows: scan ~1.7 s, points ~22 ms effective (4-way over the
  0.278 s cold seek; the 2026-08-23 figure was 4.4 s / 50). Crossover ≈ 77 ids.
  `hydrate-by-ids` runs at chunk size **500** — `IN` wins, which is why that fix
  worked.

Two consequences the body of this note does not draw:

1. **`hydrate-by-ids` is paying a full entities scan per 500-id chunk.** It wins
   today on N, not on indexing. Its margin shrinks as `entities` grows and
   disappears entirely if it is ever called with small id lists. Anyone reusing
   it should check N against the crossover, and the docstring's "because the IID
   index selects the rows before the wide columns are materialised" is not what
   the plan shows — there is no index probe.
2. **This strengthens follow-up 6, not just follow-up 4.** The 1.1 s type-window
   floor and this 2.5 s `IN` floor are the same defect seen twice: XTDB 2 serves
   only key equality from an index, so every set-valued or non-key predicate in
   this stack degrades linearly with corpus size. See
   `TN-xtdb-derived-secondary-index.md`.

Unchanged: do not build `IN` hydration for the 100-row hyperedge page. The
reason is N relative to table size, and it should be recorded that way so the
next person can evaluate their own case instead of inheriting a verdict.
