# M-xtdb-fast: make XTQL fast enough to browse the GCC

**Status:** STUB — IDENTIFY, to follow on from futon3c M-repl-circus (Joe,
2026-11-12). Opened by claude-13 to hold the idea and the day's
measurements; no work scheduled yet.

## The idea (Joe, 2026-11-12)

> So here's a cool idea, which is the zone maps could exactly be run through
> our pattern cascade so we could make it fast by using this whole thing I've
> always thought we could use other things to make other things fast so like,
> thought about using pattern cascades for proof creation or whatever but
> yeah, we could make a little zone map setup but anyway, let's save that on
> the make XTDB fast mission that's going to follow on later from from this
> work

Joe went to XTDB 2 to use XTQL ("I went to XTDB 2 because I wanted to use
XTQL"). The working arrangement today: XTQL defines every view; a SQLite
sidecar materialises the views a page must answer fast ("pages ... are like
"cached views" in an SQL database", Joe). This mission asks how far XTQL
itself can be made fast, and whether the pattern cascade can decide what to
summarise or materialise, the way zone maps summarise pages so a scan can
skip them.

## What was measured on 2026-11-12 (live futon1b, XTDB 2.1.0, 8 GB heap)

- **`_id = ?` uses the id index; `_id IN (…)` and `OR` do not.** Evidence
  table (~860k rows): `_id = ?` 0.3 s; `_id IN (one id)` 23 s; `IN (two)`
  28 s; `_id = ? OR _id = ?` 94 s. The September measurements (fxt
  `equality-hydrate-max-ids`) found the same; the text search's hydration
  still used IN until futon1b `fcb0e63`.
- **No predicate on a field inside a nested map.** `(hx$props).mission = ?`
  fails with an XTDB internal error; XTQL cannot express it either. Hence the
  flat `prop/*` columns for hyperedges and the flat Arxana columns
  (futon1b `5ea3be93f`).
- **A flat column still scans.** `prop$mission = ?` over 1.7M hyperedges:
  32 rows in 10 s. XTDB 2 is columnar, with page-level zone maps but no
  general secondary indexes (XTDB forum; Biff built its own outside XTDB).
- **Sidecars are what is fast.** The same mission-scope read served from the
  hx_node doc projection: 80 ms (futon1b `b37dbc3`).
- **Four query permits were the bottleneck under load** (now eight, futon1b
  `a59a349`); a permit held by a 14 s scan starves every other reader.

## Leads

- **Zone maps through the cascade** (Joe's idea above): let the pattern
  cascade choose which columns and value ranges to summarise, or which views
  to materialise, from the queries the Circus actually runs.
- **A newer XTDB.** XTDB's May 2026 post describes a 40x speed-up for a class
  of temporal queries (https://xtdb.com/blog/40x-query-perf). We run 2.1.0;
  test a newer release on a copy of the store, including whether `_id IN`
  now uses the id index.
- **Layout.** Lookups by `_id` are fast, so records keyed by what pages ask
  for ("needs of X") can be fetched directly; smaller single-type tables
  prune better than one mixed 1.7M-row table.
- **The cache as a view.** Each sidecar view declared by its XTQL query, with
  the rebuild-equality check as its test, so the cache can be dropped for any
  view XTDB becomes fast enough to answer directly.

Related: futon3c `holes/missions/M-repl-circus.md` §DERIVE; futon3c
`holes/labs/M-repl-circus/GCC-xtql-2026-11-12.md`,
`GCC-item-page-xtql-2026-11-12.md`; futon1b `E-xtdb-plan-cache-heap.md`,
`M-xtdb-22x-benchmarking.md`, `DESIGN-hyperedge-scope-sidecar-2026-09-26.md`.
