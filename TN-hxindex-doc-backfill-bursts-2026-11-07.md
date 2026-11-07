# TN: allocator-refusal bursts from the hx doc backfill (2026-11-07)

Owner of the fix: claude-19 (belled by claude-13, who found the symptom while on
M-formal-arxana; Joe saw it as "Clock decision not recorded: the evidence store
refused it (store-rejected: futon1b rejected the append)").

## Symptom

Every ~15.5 min, 50-190 requests failed within 1-5 s, almost all
`POST /api/alpha/hyperedge`, plus a few `POST /api/alpha/evidence` and
`GET /api/alpha/hyperedges`, each with

    class=xtdb.error.Fault message="Unable to allocate buffer of size N due to
    memory limit. Current allocation: 161302160"

3,003 such refusals on 2026-11-07 between 00:00 and 18:00. Heap was ~2-3 GB of
8 GB throughout: not a JVM heap problem.

## Where the limit is

The JVM runs `-XX:MaxDirectMemorySize=3g`. XTDB 2.1.0 gives its Arrow root
allocator a limit of 90% of that (2,899,102,924 bytes) and the memory cache a
child limit of 50% (1.5 GiB). Read live through the Drawbridge:

    scripts/futon1b-eval.sh '(let [a (:allocator @futon1b-server/!node)]
      (mapv (fn [c] [(.getName c) (.getLimit c) (.getAllocatedMemory c)
                     (.getPeakMemoryAllocation c)])
            (cons a (.getChildAllocators a))))'

Children: `database/xtdb` (log-processor, indexer, buffer-pool, metadata-mgr,
compactor, live-index), `query-source` (one `BoundQuery/openCursor` child per
running query), `pgwire`, `memory-cache`. The "Current allocation" in the
error is the *requesting* allocator's own total (here the memory cache's
~161 MB); the limit that was hit was the ROOT's, exhausted by someone else.
`query-source` had peaked at 2,890,402,680 bytes: one query had taken the
whole budget.

## Cause

Each burst coincided with a line

    [hxindex] periodic catch-up failed: PSQLException: ERROR: Unable to
    allocate buffer of size 524288 due to memory limit. Current allocation:
    2735795640

i.e. the hyperedge sidecar's periodic catch-up (`futon1b-hxindex/catch-up!`,
every 15 min) ran a query that reserved ~2.73 GB and then failed itself. The
query was the Fix C doc backfill added in 130b13c (2026-11-06 22:01):

    SELECT * FROM hyperedges WHERE _id > ? ORDER BY _id LIMIT 2000

XTDB 2.1.0 cannot seek on `_id` order, so this reads every current row's full
document and sorts all ~1.5M of them to hand back 2,000. It ran *first* in
every catch-up, before the checkpoint legs, so from the 00:01 restart that
loaded Fix C until the fix below all 63 periodic runs died there; the sidecar
checkpoint stayed at 2026-11-06T20:58:10Z (last successful repair 21:00). The
hooks kept writing, so the index itself was not stale, but the repair path
was dead and every run cost a burst of refused writes.

## Measurements on the live store (claude-19, 18:00-18:25Z)

| query | time | query-source peak |
|---|---|---|
| `SELECT _id ... ORDER BY _id LIMIT 20000` (narrow, sorted) | 7.6 s | 445 MB |
| `SELECT * ... WHERE _id >= lo AND _id <= hi` (20k docs, no sort) | 49 s | 588 MB |
| XTQL `[*]` where `hx/type = :pattern/clause` (9,466 rows) | 47 s | 429 MB |
| `SELECT * ... WHERE _id > ?` returning 0 rows | 83 s | 386 MB |
| one checkpoint-leg page (`changed-page`, 1000 rows) | 4.5 s | 340 MB |

A full-document scan costs 50-80 s whatever the filter; type filters do not
prune (every type is spread over every hash-trie page). `_id IN (...)` is also
a full scan (futon1b_xt.clj, `equality-hydrate-max-ids`).

## Fix (649d2a1)

`backfill-hx-docs!` now walks `(after, hi]` id ranges with no ORDER BY. Bounds
come from the sidecar's own sorted `hx_node` ids (SQLite, free); each range
is one filtered scan streamed into `hx_node` in 2,000-row batches; the cursor
is saved in `hx_meta` (`hx-doc-backfill-after`) after every range; each run
stops after a 4-minute budget (at least one range); the final range is
open-ended so an id the sidecar never saw is still covered; the marker that
gates type-only reads is set only when that last range has been written. A
backfill failure is recorded in `!stats :last-doc-backfill-error` and logged
instead of thrown, so the checkpoint legs run regardless. Loaded into the
serving JVM with `(require 'futon1b-hxindex :reload)` at 18:26:26Z; a manual
catch-up was kicked at 18:26:46Z and the periodic one continues after it.

## Not fixed here: checkpoint-leg throughput

The legs repair every changed id by `repair-ids!`, which hydrates with
`_id IN (500 ids)` statements: two full-document scans (~50-80 s each) per
1,000-row page, so roughly 1,000 rows per 2-3 min, about 7 rows/s. The
hyperedge write rate is ~9/s (5,000-5,700 `POST /api/alpha/hyperedge` per
10 min), almost all `:code/v05/edits` and `:code/v05/watcher-event` from the
futon3c watcher (`futon3c.watcher.multi`, `commit_ingest`): 1.1M of the 1.5M
current hyperedges are those two types. At that rate the legs cannot
converge on a 22-hour backlog, and yesterday's runs (1,345-3,647 ids per
15 min) kept up only because the rate was lower. Either the hydration needs
a shape that is not a scan per 500 ids, or the watcher needs to write less.

## Who wrote the 9/s

futon3c JVM (pid 521296), watcher namespaces above. Not the agent count.
