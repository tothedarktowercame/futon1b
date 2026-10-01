# TN — candidate speed-ups for `/api/alpha/entities?type=…`, measured

**Status: measurement, 2026-09-26 09:54–10:15Z.** Read-only, no code change,
no write to the store, no restart. Taken through the Drawbridge (:6769,
`README-drawbridge.md`) in the serving JVM (`futon1b-zone.service`, started
**09:43:13Z**, store `migration-store-21`, entities 54,498 current rows),
futon1b `059bb79`. Every probe is one statement through
`futon1b-xt/safe-q` / `futon1b-xt/timed-q` on `@futon1b-server/!node`, with
values as `futon1b-xt/pq` parameters or JDBC binds, timed three times; the
table gives all three runs and the current form is the first row.

Continuation of `TN-entities-route-cost-2026-09-26.md` (claude-8), which
established *where* the 20 s goes; this one measures *what else the same three
statements could be*. The question came from
`futon2/holes/labs/wm-contract/proof2/packets/WM-MISSION-READ-COST-D.md`.

**Conditions.** Serve-path traffic ran throughout: at 09:54Z `/health` showed
an `/api/alpha/entities?limit&type` holder 22.5 s old and 1 of 4 expensive-read
permits taken, and `requests/completed` went 200 → 4,401 between 09:54Z and
10:15Z. No WM click was in the air at either end
(`GET localhost:7070/api/alpha/wm/click` → `running? false` at 09:54Z and
10:15Z; last click ended 03:51Z). Heap 870 → 902 MB of 4,096 MB, metaspace
171 → 177 MB.

**The route today, same minute as the last table**, `type=mission&limit=1` by
curl from outside: **19.04 / 20.06 / 20.27 s** (10:14–10:15Z).

---

## 1. Hydrate — 1, 10 and 100 mission ids

Ids taken from an unordered `limit 100` window (2.08 s, one call). Each row is
one full-document read of exactly those ids.

| form | 1 id | 10 ids | 100 ids | per id (100) |
|---|---|---|---|---|
| **(a) current** `SELECT * … WHERE _id IN (?,…)` (`futon1b_xt.clj:210-237`) | 9379 / 8198 / 8765 ms | 7770 / 6518 / 6568 ms | 7378 / 8277 / 7248 ms | 74 ms |
| (b) `SELECT * … WHERE _id = ?`, one statement per id, sequential | 165 / 168 / 178 ms | 1799 / 1720 / 1804 ms | 18049 / 20034 / 23209 ms | 200 ms |
| (c) `WHERE _id = ? OR _id = ? …` | 196 ms | **cancelled at 60 s** (also at 2 and 3 ids) | not run | — |
| (d) `WHERE _id = ANY(?)`, Clojure vector as the parameter | 9392 / 8912 / 8750 ms | 8820 / 8291 / 10596 ms | 11124 / 9482 / 8969 ms | 95 ms |
| (d′) `WHERE _id = ANY(?)`, `java.lang.String[]` as the parameter | **refused** | — | — | — |
| (e) XTQL `(-> (from :entities [*]) (where (= xt/id p-id)))`, one per id | 278 / 237 / 231 ms | 2611 / 2314 / 2278 ms | 25466 / 24469 / 23132 ms | 245 ms |

- **`IN` is a scan at every N, not only at N=1.** 1 id and 100 ids cost the
  same 7–9 s, while equality is 0.2 s per id at every N. The plan does not
  change with N; what changes is that per-id equality eventually loses to it —
  the crossover is around **40 ids** (40 × 0.2 s ≈ 8 s).
- **`OR` is the worst of the three.** One id is just `_id = ?` (196 ms); at two
  ids it already exceeds the 60 s read deadline
  (`futon1b_xt.clj:101`, `org.postgresql.util.PSQLException: query cancelled
  during execution`).
- **`ANY(?)` with a real SQL array is not accepted by XTDB 2.1.0's pgwire**:
  `:xtdb.pgwire/unsupported-param-type`, `:param-oid 1015` (varchar array),
  binary format. Passing a Clojure vector instead does work — next.jdbc sends
  it as one value and XTDB reads it as a list — but it costs the same scan as
  `IN`, plus a little.
- **(e) is not a speed-up over (b)**: the XTQL point read with `[*]` is 245 ms
  against SQL's 200 ms, i.e. the wide projection is not what costs, the
  statement count is.

## 2. Window — the ordered id page

| form | limit 1 | limit 100 |
|---|---|---|
| **(a) current** XTQL `[xt/id entity/type]` + `(where (= entity/type p-type))` + `(order-by xt/id)` + `(limit p-limit)` (`futon1b_graph.clj:609-620`) | 5723 / 6982 / 5528 ms | 6615 / 5560 / 5283 ms |
| (b) same without `order-by`, then `sort` the returned ids in Clojure | 131 / 156 / 105 ms | 2092 / 2138 / 2745 ms |
| (c) SQL `SELECT _id FROM entities WHERE entity$type = ? ORDER BY _id LIMIT ?` | 5259 / 5579 / 6317 ms | 6939 / 5721 / 5237 ms |
| (c′) the same SQL **without** `ORDER BY` | — | 1954 / 1901 / 2403 ms |
| (d) cursor path, `(> xt/id p-after)` **with** `order-by`, limit 100 | — | 5687 / 5655 / 6412 ms |
| (d′) cursor path, `(> xt/id p-after)` **without** `order-by`, limit 100 | — | 3489 / 3423 / 3748 ms |

- **The SQL planner treats this shape exactly as XTQL does** — 5.6 s ordered,
  2.0 s unordered, matching (a) and (b) within noise. Nothing is gained by
  spelling the window in SQL.
- **`order-by` is what costs**, as claude-8 found: it forces every matching row
  to be produced before `limit` applies. Dropping it takes limit-1 from 5.7 s
  to 0.13 s. Dropping it does **not** make the window free at limit 100
  (2.1 s) or on the cursor path (3.5 s) — the scan still has to find that many
  matching rows.

**What `:next-cursor` would lose without `order-by`** (measured, not inferred):
three consecutive unordered limit-100 windows returned the **same 100 ids in
the same order**, and that set is **not** the first 100 by `xt/id`. So the
unordered page is a stable-but-arbitrary slice of the type, not a prefix of the
id ordering. `entities-query` sets `:next-cursor` to `(peek window-ids)` and the
next page asks for `(> xt/id p-after)` (`futon1b_graph.clj:610, 630-635`): if the
page is an arbitrary slice, that id has no relation to what remains, so the
next page **skips every unread row below it and re-reads rows above it that
were already returned** — sorting the N ids in Clojure fixes the order *within*
a page and changes nothing about which rows the next page asks for. A walk
would both drop and duplicate rows, and the "same order" above is only observed
within one basis; nothing in the query promises it across a write. The
paging contract the docstring states ("`:next-cursor` resumes the stable
`xt/id` ordering") and the two-page test (`test_temporal.clj:453-460`) are what
would have to change.

## 3. Count

| form | 3 runs | rows |
|---|---|---|
| **(a) current** materialise `[xt/id entity/type]`, `count` in Clojure (`futon1b_graph.clj:622-626`) | 5124 / 5170 / 5541 ms | 331 |
| (b) SQL `SELECT COUNT(*) AS n FROM entities WHERE entity$type = ?` | 5917 / 6297 / 5348 ms | 331 |
| (c) XTQL `(aggregate {:n (row-count)})` over the same `where` | 5984 / 5462 / 5989 ms | 331 |

All three cost the same ~5.5 s: pushing the count into SQL or into an XTQL
aggregate does not avoid the scan (§4). Counting in Clojure is, if anything,
marginally the cheapest of the three.

**(d) Who reads `:count`.** Every caller of the route in futon2 and futon3c
destructures `:entities` (and `:next-cursor`); **none reads `:count`**:

| caller | reads |
|---|---|
| `futon2/src/futon2/aif/substrate.clj:84-91` `entities-by-type` (the path all of futon2's WM reads take) | `:entities` |
| `futon2/src/futon2/aif/adapters/interest_network.clj:37` | `:entities` |
| `futon3c/src/futon3c/watcher/multi.clj:687` `fetch-pattern-entity-ids` | `:entities`, `:next-cursor` |
| `futon3c/src/futon3c/scripts/mission_scope_ingest.clj:463` | `:entities` |
| `futon3c/scripts/pattern_store_census.py:53` | regex over `:entity/external-id` in the body |
| `futon3c/holes/labs/M-apm-demonstration/analysis/memory_shape.py:44` | `entities` |

The only readers of `:count` are futon1b's own tests —
`test_temporal.clj:385, 419, 434, 459-460`. So the cheapest count measured
here is the one not taken; that is a decision about the route's contract, not
a query change.

## 4. The scan the three statements share

| probe | 3 runs | rows |
|---|---|---|
| `(-> (from :entities [xt/id entity/type]) (where (= entity/type p-type)))` | 6129 / 6137 / 5165 ms | 331 |
| the same with `:for-valid-time :all-time` | 5584 / 5789 / 5508 ms | 491 |
| `[xt/id]`-only bind: `(-> (from :entities [xt/id {:entity/type p-type}]))` | 5346 / 5238 / 5819 ms | 331 |

All three are ~5.3–6.1 s. Narrowing the projection to the id alone does not
help, and all-time history costs no more than the current-time read despite
returning 491 rows instead of 331. **The scan's cost is XTDB's**, not this
repo's phrasing of it; what this repo controls is how many scans a call makes.

## What one call would cost

`type=mission&limit=1`, cheapest measured statement for each of the three
parts, against the route's measured 19.0–20.3 s today:

| part | current (median) | cheapest measured | what it changes |
|---|---|---|---|
| window | 5723 ms | **131 ms** (b, no `order-by`) | the cursor contract, §2 |
| hydrate | 8765 ms | **168 ms** (b, `_id = ?` for one id) | nothing at 1 id; loses to `IN` above ~40 ids |
| count | 5170 ms | **0 ms** (not taken) | `:count` disappears from the body; no caller outside futon1b reads it |
| **total** | **19.7 s** | **0.30 s** | |

Two intermediate points from the same numbers, for a change that keeps the
paging contract:

- keep `order-by`, drop the count, `_id = ?` at limit 1: 5723 + 168 = **5.9 s**;
- `limit=100`: unordered window 2138 + `IN` hydrate 7378 + no count = **9.5 s**,
  against 5560 + 7378 + 5170 = **18.1 s** as it runs now.

No recommendation beyond these numbers. The route is its owner's to change, and
a change is its own packet with a warrant.

---

**Not established here.** Why a type predicate that matches any row reads the
whole table (claude-8's §"Not established" — the answer is inside XTDB, not in
these queries); whether the `IN` scan and the type scan share a plan; whether
any of these numbers hold on a quiescent JVM (serve-path traffic ran throughout,
and the route's own 19–20 s here is above claude-3's ~9.4 s quiescent
measurement at 04:30Z, so treat every figure as an under-load figure).

**Reading the eval log.** Three of these probes are logged as
`clojure.lang.Compiler$CompilerException: Syntax error macroexpanding at (1:1)`
in `/tmp/futon1b-eval.log` at 09:58–09:59Z. That is not what happened: the
statement hit the 60 s deadline, and the handler walking the cause chain
NPE'd on the end of the chain (`(iterate #(.getCause %) t)` without a
`take-while some?`). The masking is the one `README-drawbridge.md` warns
about, arriving one level further in than expected.

---

## Addendum, 10:50–10:52Z: unit 3 live, units 1–2 not

The three changes measured above landed as `5393517` (`include-total=false`),
`b343d34` (`ordered=false`) and `c95a8f8` (equality hydrate below 40 ids).
Only the third is in the serving JVM. `(require 'futon1b-xt :reload)` through
:6769 was safe — `futon1b_xt.clj` was at its committed state, and the
namespace's semaphore and network-timeout executor are both `defonce`, so a
reload leaves them alone — and it is enough on its own, because every caller
reaches `hydrate-by-ids` through its var.

`GET /api/alpha/entities?type=mission&limit=1`, three curls each, no click in
the air, no other permit holder:

| | 3 runs |
|---|---|
| before the reload | 18.12 / 19.88 / 19.71 s |
| after `(require 'futon1b-xt :reload)` | 10.88 / 11.99 / 10.71 s |
| the same, with `&include-total=false&ordered=false` | 10.21 s, and the body still carries `:count 331` |

The ~8 s removed is the `_id IN (?)` scan for the single row, as §1 predicted
(8.8 s → 0.17 s). The third row is the check that the flags are still inert:
the route's own code is the pre-`5393517` version, and an older route ignores
unknown query params rather than refusing them — which is also why futon2 can
pass the flags before this JVM has them.

**`futon1b-graph` and `futon1b-server` were NOT reloaded, and should not be
until the shared checkout is quiescent.** Both files currently carry another
agent's uncommitted work in progress (the hyperedge sidecar read path), and
`futon1b_hxindex.clj:407` — which `futon1b-graph` loads — does not compile at
this moment (`defonce` with a docstring, three args). `require :reload`
resolves through the live checkout, so reloading either namespace now would
install half-finished code into the serving JVM, and reloading `futon1b-graph`
would fail part-way through redefining it. Nothing was restarted. The flags go
live on the next reload of those two namespaces from a clean tree, or on the
next restart, whichever comes first.

### Correction to the paragraph above: a reload will not make the flags live

The shared checkout went clean at 10:57Z (`c2e7cb5`, `07256fa` — the
`defonce` docstring is now a comment), so I looked at what reloading the other
two namespaces would actually do, and the answer is: not this.

`start-server!` registers each route by VALUE, not by var —
`(.createContext server "/api/alpha/entities" (handler entities-route))`
(`futon1b_server.clj:1195`), and `handler` (`:412`) closes the fn it is given
into a `reify HttpHandler`. So `(require 'futon1b-server :reload)` redefines
the `entities-route` var while the live context keeps calling the closure
captured at startup. The flags are parsed in that route fn, so they would stay
inert. (`(require 'futon1b-graph :reload)` does take effect — `entities-route`
calls `graph/entities-query` through its var — but on its own it changes
nothing observable: with no `:include-total?`/`:ordered?` in the opts map both
default to true, which is today's behaviour.)

Unit 3 reached the running server only because `hydrate-by-ids` is called
through its var from `futon1b-graph`, which is not how the routes are wired.

**So `5393517` and `b343d34` need a restart, not a reload.** Not done here —
restarting futon1b is not this packet's to do; it is scheduled by whoever owns
the pause. `README-drawbridge.md`'s "route implementations reload in place"
holds for what a route CALLS, not for the route fn itself.
