# TN — entity miss regression, 2026-11-11

**Status:** read-only discovery against the serving futon1b JVM and
`migration-store-21`, 2026-11-11 02:25–02:27Z. No namespace was reloaded, no
data was written, and no process was restarted or signalled. Drawbridge reads
used `scripts/futon1b-eval.sh` on :6769 as documented in
`README-drawbridge.md`.

## Finding

The August repair has not been reverted in source. The narrow alias query is
still present, but the current 62,799-row table makes that query a 17–20 s
scan. A cold absent name therefore spends nearly all its time in
`scan-alias-ids`; an honoured absent warrant skips that scan and hydration,
but every request still performs the initial wide `xt/id` lookup, which is why
the repeat is still a roughly 1–3 s request.

## 1. Cold-miss decomposition

I timed the same parameterised XTQL statements that the route calls, inside
the serving JVM. The subject was the absent string
`codex4-probe-1794363805168-never`.

| part | live time | rows | source |
|---|---:|---:|---|
| `xt/id` read, `(from :entities [*])` | 612 ms | 0 | `futon1b_graph.clj:394-397` |
| narrow name/external-id scan | 17,723 ms | 0 | `futon1b_graph.clj:323-337` |
| hydration | 0.047 ms | 0 | `futon1b_graph.clj:355-366`; an empty id vector does not query |

The complete function, on a second absent subject, took **20,066 ms cold**.
Its newly issued warrant had `:warrant/ids []`. The immediately repeated calls
took **1,592 ms** and **1,112 ms**, both with `:warrant/reused? true`.
Variation from claude-13's 21.4–23.4 s cold and 2.8–2.9 s repeated HTTP
measurements is ordinary live-store/load variation; the attribution is not
ambiguous. The scan dominates cold misses and the id read is the only store
operation on an honoured absent warrant.

The code order is `xt/id`, UUID short-circuit, then warranted alias lookup
(`futon1b_graph.clj:388-402`). On a cold alias miss there are no ids to
hydrate. On a warrant hit, `entities-by-alias-warranted` calls hydration with
the warrant's empty id vector and returns `[]` without querying
(`futon1b_graph.clj:354-362`).

## 2. Growth since August

| date | current `:entities` rows | evidence |
|---|---:|---|
| 2026-08-23 | about **5,900** | `futon3c/holes/excursions/E-fetch-entity-miss-path.md:15-25` |
| 2026-11-11 | **62,799** | live narrow `[xt/id]` count |

The live SQLite entities sidecar also reports **62,799** rows, and its grouped
type counts sum to that value. Thus the corpus is about 10.6 times August's
size. The source query remains the narrow repair from `ee8f41e`; growth plus
the store's present scan cost explains the regression without a source
rollback. For context, the September cost note already recorded 54,498 rows
and showed that scan cost had risen despite unchanged query shapes
(`TN-entities-route-cost-2026-09-26.md`).

## 3. What the entities sidecar can answer

The sidecar is complete for **whole current entity documents**, not indexed
for aliases. `ent_node` has only `id`, `type`, and Transit `doc` columns, with
indexes on the primary-key `id` and `(type,id)`
(`futon1b_entindex.clj:48-52`). The complete encoded document necessarily
contains `entity/name` and `entity/external-id` when the source document has
them (`futon1b_entindex.clj:14-15,84-97`), and the gate currently reports
complete: store count = sidecar count = 62,799, no hook failures.

It therefore *could* find aliases only by selecting and decoding every blob;
it cannot cheaply answer “ids whose name or external-id equals v.” Its public
reads are type page and type count, both backed by the `type` index
(`futon1b_entindex.clj:414-436`). The sidecar declaration correspondingly
lists only `entity/type` and document and serves only the plural typed routes
(`futon1b_sidecars.clj:78-94`). The alias route instead has a separate
in-memory warrant declaration whose fallback is the XTDB scan
(`futon1b_sidecars.clj:129-141`). No alias columns/API exist for
`futon1b_graph.clj:323-380` to call.

## 4. Why an honoured miss is still slow

`fetch-entity-warranted` unconditionally attempts the `(from :entities [*])`
`xt/id` read before consulting alias warrants (`futon1b_graph.clj:394-401`).
An absent warrant contains no ids, so its reuse has no scan or hydration cost;
the 1.59/1.11 s repeats measured here are the wide id miss. This also explains
the made-up UUID result: UUID shape skips alias lookup but cannot skip that
first read.

## 5. Issuers, frequency, and interference

Repository callers that can intentionally issue misses include:

* flight ingest's fetch-before-merge (`futon3c/watcher/flight_ingest.clj:62-72`);
* mission-scope ingest's name/id resolver (`futon3c/scripts/mission_scope_ingest.clj:107-118`), which explicitly says absent exact ids are common and caches nil for one run;
* APM memory cascade pattern fetch (`futon3c/apm/conductor.clj:432-441`);
* review/wiring/honest-hole scripts found at
  `futon3c/scripts/review_codex_lane_attachments.clj:104`,
  `wire_math_memory_patterns.clj:190`, and `o5_honest_holes.py:160`.

There is no per-route request counter. Accessible repo-local logs contain no
entity requests, and this seat cannot read the service journal. The best live
aggregate is the alias-warrant counter: after 59.1 hours of JVM uptime it
showed **7,398 issued** and **1,276 honoured**, approximately 125 issuances and
22 reuses per hour. These count all alias fallbacks, hits as well as misses,
and include the probes above, so they are an upper bound rather than a miss
rate. The global HTTP executor showed 822,214 completed requests, but cannot
attribute them by route (`futon1b_request_executor.clj:12-17`).

The entity route does **not** enter `with-expensive-read!`; it calls the graph
directly at `futon1b_server.clj:1102-1130`. Therefore a 20 s alias scan is not
visible as an expensive-read holder and does not consume one of that
semaphore's four permits (`futon1b_server.clj:709-718,799-827`). It does,
however, occupy one of eight HTTP workers and one of four process-wide XTDB
query permits while each statement runs. Four concurrent scans can exhaust
the query permits; other reads wait at most five seconds and then fail with
`:timeout/phase :permit-acquire` (`futon1b_xt.clj:20-33,52-83`). So one miss
does not serialize all reads, but a small burst can delay or reject unrelated
reads, while also evading the scan admission accounting.

## Proposed fix and pinning test

Extend `ent_node` with nullable `name` and `external_id` columns and indexes on
each; populate them in `index-doc!`, fill/repair, and a schema migration. Add a
gated exact-alias lookup that returns deterministically ordered ids. When
`ent/reads-usable?` is true, `scan-alias-ids` should use it; only a closed
sidecar gate should fall back to the existing XTDB scan. This preserves the
sidecar completeness invariant rather than treating the cache as authority
when its gate is closed.

The acceptance test should use the real temporary SQLite sidecar, insert a
recorded-size **62,799-row** corpus through the actual indexing path, assert
name and external-id hits plus an absent alias, and time the absent lookup
under a conservative local threshold (for example 250 ms). A graph-level test
should make the sidecar gate serving, make any XTDB alias scan throw, and show
that a cold alias miss still returns absent; a companion closed-gate test
should show the existing scan is invoked. Keep correctness assertions primary
and report timing separately enough that a slow CI host produces a diagnostic,
not a silently weakened invariant.

Because this changes sidecar DDL/migration plus two already-loaded namespaces,
deployment should be a **futon1b JVM restart from master**, which runs sidecar
initialisation/migration and loads `futon1b-entindex` and `futon1b-graph`
coherently. A graph-only Drawbridge reload would leave the live sidecar schema
and entindex Vars old. Restarting futon1b is Joe's call.

## Implementation and deployment (2026-11-11)

Implemented after review: `ent_node` now has nullable `name` and `external_id`
columns and `(name,id)` / `(external_id,id)` indexes. Every hook, fill, and
repair upsert writes them. `init!` detects the old DDL, adds missing columns,
decodes the existing complete documents once, backfills both aliases in a
transaction, creates the indexes, and is idempotent. Exact id and alias reads
use this sidecar only while `reads-usable?` holds; otherwise the unchanged XTDB
reads run. Alias ordering and warrants are unchanged.

The recorded-shape acceptance corpus is
`test-data/entity-sidecar-samples.edn`, sampled read-only from the live store
and expanded to the live recorded size of 62,799 rows through the real
`index-doc!` path. On this host, its absent alias lookup took **0.78–1.19 ms**
(250 ms diagnostic threshold). Creating an old-schema 62,799-row sidecar and
running `init!` took **0.64–1.03 s wall** (0.63–1.03 s reported). This copy has
the live row count and real body shapes; Joe should expect roughly one second
of added first-boot work, while treating the restart receipt as authoritative.

Deploy only from canonical master, at Joe's quiet moment:

```sh
cd /home/joe/code/futon1b
systemd-run --user --unit=futon1b-restart --collect \
  /home/joe/code/futon1b/scripts/restart-futon1b-detached.sh
tail -f /tmp/futon1b-restart-receipt.txt
```

Wait for the receipt's final `FINISHED` line and successful result. The normal
restart script deliberately performs deep pre/post censuses; this note's
read-only discovery rule against `/health?deep=true` does not replace that
operator-owned restart safety protocol. On first boot, entity sidecar
initialisation performs the one-time column backfill and index creation before
serving indexed aliases. Subsequent boots detect both columns and skip the
body scan.

Post-restart, time one unique non-UUID cold miss and one known hit without
requesting deep health:

```sh
curl -sS -o /dev/null -w 'cold-miss status=%{http_code} time=%{time_total}\n' \
  http://127.0.0.1:7073/api/alpha/entity/post-restart-unique-absent
curl -sS -o /dev/null -w 'hit status=%{http_code} time=%{time_total}\n' \
  http://127.0.0.1:7073/api/alpha/entity/futon-theory%2Fmission-dependency
curl -sS http://127.0.0.1:7073/health
```

Expect 404 then 200, both using sidecar point/index reads, and `/health` to
show the entities sidecar `{:serving? true :why :complete}`. If the gate is
not complete, the code intentionally falls back to XTDB and the cold miss may
again take the corpus-scan time; investigate the checkpoint/hook failure
rather than bypassing the gate.
