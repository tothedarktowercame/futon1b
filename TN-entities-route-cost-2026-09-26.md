# TN — where `/api/alpha/entities?type=…` spends its 20 s (measured inside the JVM)

**Status: measurement, 2026-09-26 09:47–09:52Z**, taken through the new
Drawbridge (`futon1b_drawbridge.clj`, :6769) in the serving JVM five minutes
after the restart at 09:43Z, futon1b `f68716b`, store `migration-store-21`
(entities 54,498 current rows). Read-only. The question came from
`futon2/holes/labs/wm-contract/proof2/packets/WM-MISSION-READ-COST-D.md`
(claude-3), which timed the route from outside at ~9.4 s at rest and 23–42 s
under load and could not say which statement held the time. Forms in
`/tmp/f1b-time.clj`, `/tmp/f1b-time2.clj`, `/tmp/f1b-time3.clj` at writing.

## The route's three statements, `type=mission&limit=1` (`futon1b_graph.clj:591-635`)

| statement | as the route runs it | measured (3 runs) |
|---|---|---|
| window | `(from :entities [xt/id entity/type]) (where (= entity/type ?)) (order-by xt/id) (limit 1)` | 7,007 / 5,990 / 6,099 ms |
| hydrate | `SELECT * FROM entities WHERE _id IN (?)`, one id | 8,420 / 8,835 / 9,327 ms |
| count | `(from :entities [xt/id entity/type]) (where (= entity/type ?))`, 331 rows | 5,829 / 6,021 / 5,921 ms |

Sum ≈ 21 s; the route from outside in the same minute: 20.8 s.

## What each one is paying for

| probe | ms |
|---|---|
| `(from :entities [xt/id]) (limit 1)`, no predicate | 106 |
| the window **without** `order-by`, limit 1 | 109 |
| type scan, `agent-view`, **14 rows** | 5,782 |
| type scan, `mission`, 331 rows | ~6,000 |
| type scan, a type with 0 rows (claude-3, from outside) | 97 |
| all-time versions of `mission` (491) | 6,210 |
| point read `(where (= xt/id ?))`, `[xt/id]` | 88 |
| `SELECT _id FROM entities WHERE _id = ?` | 128 |
| `SELECT * FROM entities WHERE _id = ?` (784-char doc) | 291 |
| `SELECT * FROM entities WHERE _id IN (?)`, the same id | 7,965 |

1. **A type predicate that matches anything costs ~6 s flat.** 14 rows and 331
   rows cost the same; 0 rows costs 0.1 s. So the scan reads the whole table
   whenever the type is present at all, and only an absent type is pruned.
   Both the window (because `order-by` needs every matching row before
   `limit`) and the count pay it. Without `order-by` the limit-1 window is 0.1 s.
2. **`_id IN (?)` does not use the id index; `_id = ?` does.** The same id, the
   same `SELECT *`: 8 s against 0.3 s. `hydrate-by-ids` (`futon1b_xt.clj:210-240`)
   is built on `IN`, so hydrating one row costs a table scan. The docstring's
   "~1 s for 1,351 ids by IN" (2026-08-23) is not what the store does today.
3. **The count is a third full scan** (`:622-626`, `bb3c3c5`, 2026-08-23),
   on every call regardless of `limit`.

Under the load of a WM click these three become the 25–42 s claude-3 measured.

## Not established here

Why the type scan reads the whole table when the type has rows (page
metadata, the 2.1.0 planner, or the arrow layout of `entity/type`) — the
answer is in XTDB, not in this repo's queries. What is in this repo's queries
is the three-scans-per-call shape above; its owner decides what to do with
it. For the record, the 2026-09-08 cost profile measured a type-only read at
p50 2.3 s and the 2026-08-23 note at ~0.7 s + ~1 s; the shape has not changed
since `081e34e`, so the growth is in the scan's cost per call, not in the
number of scans.
