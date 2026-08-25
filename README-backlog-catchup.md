# README-backlog-catchup — refilling a derived surface at scale

**What this is.** The record of the 2026-08-25 mission-scope backlog catch-up on
Zone: ~4,650 → ~18,250 `mission-scope/*` hyperedges across 577 scope trees in
5 hours, and the *method* — how to tell a store that lost data from one that
is merely behind, and how to drive a per-document ingest hard without taking
the substrate down. Written so the next "we seem to be missing X from XTDB"
starts here instead of from a hypothesis.

## 1. Diagnose before you reingest

The report that started this: *"scope census returns 32% of its former
population; futon1b OOMed on 08-23 (`/health?deep=true`); maybe the restart
lost the lane."* Plausible, and wrong. Reingest into a store that dropped rows
behaves differently from reingest into one that never had them, so the first
job is to tell those apart — and XTDB 2 can answer that itself.

**Bitemporal as-of counts.** `/api/alpha/hyperedges` accepts
`system-as-of=<instant>`; with `limit=1&include-total=true` the `:count` is the
true population at that system time. Sample around the suspected event:

```
for ts in 2026-08-22T00:00:00Z 2026-08-23T10:30:00Z 2026-08-23T12:00:00Z; do
  curl -s "localhost:7073/api/alpha/hyperedges?type=mission-scope/loose-section&limit=1&include-total=true&system-as-of=$ts" | grep -o ':count [0-9]*'
done
```

A monotone series across the event (here 2617 → 2617 → 2682) means nothing was
lost. That is also what the mechanics predict: XTDB's local log
(`<store>/log/LOG`) is append-only; a JVM kill cannot truncate committed
transactions, and restart replays the log. What *can* lose data is the
restart pointing at a different `--store-dir` — check `store-guard`'s boot
line ("futon1b-server up ... (store migration-store-21)") in the journal.

**Compare against the declared population, per unit.** For scopes the source
of truth is `futon6/data/mission-scope-trees/*.json`
(`scope-count-by-binder-type` per mission). `futon3c/scripts`-adjacent helper
used here: `/tmp/scope-skiplist.py` (kept inline in §5) pages every type out
of the store, counts per `(mission, binder)`, and diffs against the trees. It
gave: 577 trees, 68 current, 509 to run — and, crucially, *which* were
missing: trees dated June/July that had never landed. Missing-since-always is
a backlog; missing-since-Tuesday is a loss.

## 2. How the store got behind (the actual timeline)

Reconstructed from `system-as-of` counts, git history and
`futon3c/holes/technotes/TN-futon1a-sweep-2026-08-02.md` §3:

| when | what | loose-section count |
|---|---|---:|
| 2026-07-10 | futon1a → futon1b migration into `migration-store-21`. `migration/export.clj`'s type list carries only `mission-scope/{nesting,psr,pur,pxr}` — **none of the 14 structural binders** (`loose-section`, `eightfold-phase`, …). futon1a held 14,482 of them. | **0** on 07-11 |
| 07-11 → 07-28 | The only refill path is `futon3c.watcher.scope-reingest`, triggered when a doc *lands* (edit/save). Unchanged docs never come back. ~50 rows/day. | 392 (07-15), 934 (07-25) |
| 07-22 | futon3c `0fe209c0` bounds whole-type reads at 1,000 rows and — correctly — fails closed on truncation. | |
| ~07-28 → 08-02 | Population crosses 1,000. **Every** reingest throws `futon1b hyperedge result truncated`; the surface freezes. | **1002**, flat |
| 08-02 | TN-futon1a-sweep finds it, raises the cap to 5,000, notes a full backfill will cross 5,000 and *needs a cursor first*. | |
| 08-02 → 08-23 | Trickle resumes (doc-land only). Cursor pagination lands on Zone 08-23 (`eb0f4ac8`). The backfill is never scheduled. | 1227 (08-04) → 2617 (08-23) |
| 08-23 | `/health?deep=true` realizes every `xt/id` on-heap to count tables → OOM under `-XX:+ExitOnOutOfMemoryError`. Unrelated to scopes; became the red herring. | 2617 before and after |
| 08-25 | This catch-up. | 10,878 |

Three lessons fall out. (a) A migration of a *derived* surface must either
carry it or immediately schedule its regeneration — "the watcher will refill
it" only covers documents that change. (b) A fail-closed guard is right, but a
guard that throws into a log nobody reads is a silent freeze; the population
count is the liveness signal to alarm on. (c) The prerequisite (pagination)
shipped three weeks after the diagnosis, and the backfill it unblocked was
never run — a fixed blocker is not a done job.

## 3. Making the ingest fast enough to catch up

The per-document reingest (`scripts/mission-scope-reingest.sh`: python
detect → one `-main --binder` call per binder → `--true-up`) took **3 m 50 s**
per mission. 509 missions × 4 min ≈ 34 h. Where the time went, and what fixed
it, in the order it was found:

1. **Whole-type reads per call.** Each `-main` re-read all 15
   `mission-scope/*` types (~28 hydrated pages × ~1 s; 13 s per 1,000 rows
   uncached) to find *one mission's* rows. Fix: a `?mission=` pushdown on
   futon1b's hyperedges route (futon1b `470ceb6`, mirrors the existing
   `repo`/`source-file` filters on the denormalized `prop/mission` column —
   note the column must be bound in the window `from` or XTDB rejects the
   `where` and `safe-q` returns `[]`, which the test caught) and
   `mission-scope-hyperedges` in the ingest (futon3c `10af2d28`). → **1 m 50 s**.
   The remainder is the write path itself: ~50 `relations/batch` at ~1.3 s and
   ~600 entity GET/POSTs per mission — inherent per-scope cost.
2. **Drawbridge serializes.** Four shell workers evaluating into the serving
   JVM produced **max 1 in-flight request** at futon1b (count `start`/`end`
   lines in its journal). Throughput 1.5× for 4× workers. Fix: one worker JVM
   per shard, calling `scope-reingest/reingest-now!` in-process
   (`scripts/mission_scope_backlog_worker.clj`, `mission-scope-backlog-jvm.sh`;
   the JVM loads the ingest ns in ~1 s from the cpcache). → in-flight peaked
   at 13, ~3.5 missions/min with 8 workers.
3. **futon1b is the ceiling.** 4 request threads, queue 16, `AbortPolicy`, 4
   query permits, 2 expensive-read permits (set after the July memory
   incidents). At 8 workers the pool queues (entity GET 145 → 241 ms) and a
   dropped exchange left one worker parked forever because the ingest's
   HttpClient had no deadline — now 120 s (`FUTON3C_SUBSTRATE_HTTP_TIMEOUT_S`);
   a timed-out request fails that mission loudly and the next pass picks it
   up. Going past 8 workers means widening the pool *and* restarting futon1b:
   an operator decision, not a batch-job default.

Net: 34 h → **5 h wall clock** (median 143 s/mission, p90 676 s, max 49 min).
Machine load never exceeded 6.5 on 32 threads; the box was never the limit.

## 4. Side effects to expect while it runs

- Every write invalidates futon1b's hyperedge-window cache, so *other* readers
  pay uncached hydration under a queued pool. `GET :7070/api/alpha/cascade-real/graph`
  (5 s per-page deadline) returned sections as `:failed` in `:section-status`
  in 5 of 6 samples during the run — HTTP 200, well-formed, honest in the
  payload, but consumers that read only `:counts` saw zeros. Anything that
  regenerates artefacts from that endpoint must fail closed on
  `:section-status`, and should not run during a backlog.
- Do not run `/health?deep=true` against futon1b older than `608c9d5`.

## 5. Runbook (Zone, futon3c ≥ `61c2bf40`)

```
# 0. is it loss or backlog?  (§1) — as-of counts + tree diff
python3 /tmp/scope-skiplist.py /tmp/scope-backlog-missions.txt   # writes <mission>\t<binder:store/tree ...>

# 1. shard the remainder across N worker JVMs (excludes missions already logged ok)
cd ~/code/futon3c
WORKER=scripts/mission-scope-backlog-jvm.sh \
  scripts/mission-scope-backlog-parallel.sh 8 /tmp/scope-backlog-missions.txt <tag>
#    units: scope-backlog-<tag>-w<i>   logs: /tmp/scope-backlog-<tag>-w<i>.log
#    lines: "[backlog] i/N ok|FAIL|SKIP <mission> (Ns)"

# 2. watch
systemctl --user list-units 'scope-backlog-*'
grep -h "FAIL\|SKIP" /tmp/scope-backlog-<tag>-w*.log
curl -s localhost:7073/health | grep -o ':permits/[a-z]* [0-9]*\|:heap {[^}]*}'

# 3. rerun anything that failed: same command, new tag (ok missions are skipped)
# 4. final census: python3 /tmp/scope-skiplist.py /tmp/final.txt
```

Reading the final diff: the store legitimately **exceeds** the trees on
`source-material`, `mission-scope-in/out`, `pattern` (the per-binder ingest
writes W2′ enrichments beyond the detector's declared counts), so "current"
means no binder with store < tree. After this run: 3 deficits of 578 — one
tree whose doc no longer exists (`E-arxana-interaction-map`), two where phase
headings of the form `IDENTIFY — 2026-05-30 …` land only their `head` (ingest
canonicalization edge case, open).

Gotchas met on the way: tree `path`s can be relative to `/home/joe/code`
(17 of 578); `grep` in a `tee` pipeline block-buffers unless
`--line-buffered`; `clojure -M -e … <arg>` treats the arg as a script to load
(pass shards via `-J-Dscope.shard=`); the census regex misses `relates-to`
rows (harmless, they read as 0/N).

`scope-skiplist.py` lives at `/tmp` on Zone as of writing; if it is gone, it is
~60 lines: page each `mission-scope/<binder>` with `limit=1000&include-total=false`
following `:next-cursor` via `after=`, count `(:mission, binder)` from each
record's `:mission "…"`, diff against every tree's `scope-count-by-binder-type`.

## 6. Coordination notes

The whole thing ran from a laptop session against Zone over ssh; the code
changes were made and committed *on Zone's lineage* (it was 45/37 commits
ahead of origin — a local Codex handoff against the laptop checkout produced a
pagination commit that Zone already had). Long waits used a durable
`scripts/bg.py` task that ssh-polls Zone and releases an Agency park
(`POST /api/alpha/park/complete`) when no `scope-backlog-*` unit is running —
the Monitor tool does not wake an Agency seat.
