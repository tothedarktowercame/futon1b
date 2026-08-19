# README-fts — the evidence text index, and how to operate on it safely

Companion to [`CANDIDATE-INDEX-CONTRACT.md`](CANDIDATE-INDEX-CONTRACT.md).
The contract states the invariants (C1–C6) the index must uphold. This document
is the operational half: how the mechanism actually behaves, how to check it,
and how to run a store operation without silently invalidating it.

Written 2026-08-18 after two incidents on Zone in one day, both of the same
family: **a check that passed over the population it looked at, reported as a
statement about the store.** The numbers quoted below are from those incidents
and are real.

---

## 1. The two write paths

Evidence reaches the index by two different mechanisms with different guarantees.

| | `on-append!` | `catch-up!` |
|---|---|---|
| when | every evidence write | periodic sweep, and at boot |
| style | fire-and-forget future | single-flight, keyset pagination |
| advances checkpoint? | **no — deliberately** | yes, after every completed page |
| may drop writes? | **yes** (best effort) | no — it is the repair path |
| latency | ~instant (searchable in seconds) | up to one sweep interval |

`on-append!` gives you live search: a document written now is findable now. It
is explicitly *best effort*. If it fails — see the `SQLITE_BUSY` case in §6 —
the write still lands in the store, and the index is repaired later by
`catch-up!`.

### Why `on-append!` must not advance the checkpoint

This looks like a missed optimisation and is not. If a live append moved the
checkpoint past territory an interrupted rebuild had never scanned, a restart
would resume *after* the unscanned region and skip it silently, forever. So
appends index eagerly and leave the frontier alone. The cost is that the
checkpoint lags live writes; the benefit is that a rebuild is always resumable
without holes.

---

## 2. The two watermarks — the distinction that matters most

The sidecar keeps two assertions in `fts_meta`, and conflating them is the
single most expensive mistake available here.

**Checkpoint** (`last-at`, `last-id`) — *everything up to this `(at, id)` has
been indexed.* Advances after each completed page. This is what makes a long
rebuild resumable.

**Basis** (`basis-tx`, `basis-tx-ids`, `basis-captured-at`) — *this index
reflects the store as of these transaction coordinates.* Written **only when a
scan drains**, i.e. when looking for more work returns nothing. Per C2/C6 it is
a proof of coverage, not a note that a scan happened to stop.

They advance on different schedules. A basis is a claim about the store; a
checkpoint is a claim about a scan. **A true statement about a scan can be
promoted into a false statement about a store**, and that is exactly what
happened on 2026-08-17 (§7).

---

## 3. The behind-frontier hazard

`catch-up!` scans **forward** by `(at, id)`. Everything follows from that.

A normal append lands at the tail, ahead of the checkpoint, and is covered by
the next sweep. **A backfill or replay does not.** Replaying documents dated
last week inserts them *behind* the frontier, where no forward scan will ever
look again.

The failure is silent and self-certifying:

1. Bulk insert lands 7,249 documents dated 08-11…08-17, behind a checkpoint
   already at 08-17T17:48.
2. A periodic sweep runs, scans forward from 17:48, finds nothing above it.
3. It concludes it has drained, and writes `basis-tx`.
4. The store now asserts coverage over six days it never scanned.

The index was in fact complete, because `on-append!` had indexed each document
as it landed. But "complete because a best-effort mechanism happened not to
fail" is not the claim `basis-tx` makes. Had the append path dropped ten
documents, the false basis would have made that gap **invisible and permanent**.

> **Rule.** Any write landing behind the checkpoint invalidates the basis.
> Clear it and re-earn it. The store does not enforce this today — it is an
> operator obligation (see §8, open question).

---

## 4. Sidecar layout

Lives beside the XTDB store at `<store-dir>/fts5-evidence.db`.

| object | what | note |
|---|---|---|
| `ev_fts` | fts5 virtual table, `tokenize='unicode61'` | `id/author/at/session` UNINDEXED, `body` indexed |
| `ev_attr` | btree attributes (type, claim-type, subject, pattern, author+at…) | powers **facet filters** |
| `ev_tags` | tag junction | |
| `fts_meta(k,v)` | checkpoint + basis | the two watermarks from §2 |

No stemming, by design: it keeps the scan+re-check oracle exact (C5 — narrowing
produces candidates, the re-check decides).

**The sidecar is derived data. Deleting it and rebuilding is always safe.**
That is the property that makes everything here recoverable.

### `ev_fts` and `ev_attr` can disagree — check both

They are separate populations and a store can have one without the other. On
2026-08-18 the restored store showed:

```
ev_fts_docsize   145,770     body index complete
ev_attr               40     facets essentially EMPTY
```

`init!` had created the new-schema tables on an old-schema store, but only
*new* writes populate them. Free-text search worked perfectly; **filtering by
type/claim-type/tags silently returned almost nothing**, and no forward
catch-up would ever fix it, because the missing rows are all behind the
checkpoint (§3).

A schema upgrade is not a data backfill. After one, **rebuild** (§9).

---

## 5. Checking health

```bash
python3 scripts/fts-status.py [port]   # default 7073
```

It prints the index's numbers *next to the store's*, because the bug class this
exists to catch is an index that looks healthy on its own terms while
disagreeing with the store.

What the fields mean, and what is actually reassuring:

| field | window | read it as |
|---|---|---|
| store rows vs index rows | now | the only cross-check that matters; a small positive delta is live-write lag |
| `tx-lag` | now | index behind store in transaction terms; should trend to ~0 |
| `age-ms` | now | time since the last basis capture — confirms the sweep is running |
| `indexed` | **last catch-up run only** | overwritten every run; not a lifetime total |
| `errors` | **cumulative since process start** | never reset. A non-zero count with a repaired document is normal |
| `ready` | now | **the sidecar is attached. NOT a coverage claim** (`:ready (some? ds)`) |
| `basis captured-at` | now | present = a proven drain. **Absent is honest**; a stale one is a lie |
| `recheck-rejections` | cumulative | C5 re-check disagreeing with candidates |
| `periodic?` | now | the repair path is scheduled |

> **`indexed` and `errors` are not comparable, and `ready` is not coverage.**
> On 2026-08-19 `{:indexed 29, :errors 121, :ready true}` was read as "the
> index is dropping 121 of 150 writes". There is no population of 150: 29 is
> one sweep's total and 121 is 28.5 hours of cumulative append failures. At
> that moment index and store were level at 150,428, and checking all 125
> logged failures individually found **every one present in both `ev_fts` and
> `ev_attr`** — zero writes dropped. An `on-append!` failure never loses a
> write: the store already has the document, the checkpoint is deliberately
> not advanced, and the next catch-up re-indexes it (§1).
>
> 87 of those 125 errors landed in a single hour — a 2,384-document bulk
> replay contending with a catch-up scan for sqlite's single writer. Steady
> state is ~1.7% of live appends, all repaired.

> **`tx-lag 0` means "the index has caught up with everything the store has".
> It does not mean the store is receiving anything.** A perfectly healthy index
> over a dead write path reports `tx-lag 0`, `errors 0`, `periodic? true`. To
> tell them apart you must write something and read it back — or search for an
> id known to be absent, which distinguishes "index responds" from "index is
> current" without writing a permanent row into an append-only store.

Direct sidecar inspection (read-only):

```bash
python3 scripts/fts-probe.py <store-dir> [evidence-id]
```

---

## 6. Known operational hazards

**`SQLITE_BUSY: database is locked`.** A replay writing concurrently with
`on-append!` will lose the race for some documents. The store write still
succeeds; only the index entry is dropped, and `catch-up!` repairs it. Verified
2026-08-18: document `emacs-403ad…` failed to index with `SQLITE_BUSY`, and the
next sweep restored it to both `ev_fts` and `ev_attr`. **Do not treat an
`errors` count from a replay as data loss** — check the store, then check
whether the sweep repaired the index.

**A hydrated search response cannot be lexically scraped.** Evidence bodies
legitimately contain the same tokens the envelope uses, so splitting or
regexing the payload silently miscounts. Measured 2026-08-19: a `limit=10`
query with `hydrate=true` produced **310 occurrences of `:score `** — 300 of
them inside bodies — and 410 documents in the corpus contain the token
`evidence/id`. Chunk-splitting therefore over-segments while per-chunk id
extraction under-yields, which presents as *missing data* rather than as a
parsing error. Two agents lost rounds to this in one day, in opposite
directions.

Use the top-level **`:ids`** vector, which carries result identities in rank
order outside the body text, or parse the response as EDN. Do not regex it —
including for `:ids` itself: a regex for `:ids \[...\]` can match an
occurrence inside a body and return nothing, which is how this note's own
verification first failed.

**Evidence writes return `201`, not `200`.** A success check written as
`status == 200` will report every successful insert as a failure. (Done, and it
briefly produced 35 phantom failures.)

**Evidence is append-only and cannot be retracted.** `retractable-tables` is
`#{:entities :hyperedges :relations}`; the evidence route is `GET/POST only`.
Test writes are permanent — prefer a read-only probe (§5).

**The store is single-process.** XTDB 2 local stores permit exactly one open
process per store directory. Any operation involving a second server needs a
different store dir and port, and the live service must be stopped before
anything opens its store.

**Duplicate ids are refused, not upserted.** This makes re-running a replay safe
by refusal. Expect `duplicate evidence id` on a re-run and treat it as success.

---

## 7. Store operations — the checklist

Derived from what actually went wrong. **A checklist must be derived from what
the operation can touch, not from what you happen to be thinking about.**

### Before

- [ ] **Table census** (`GET /health?deep=true`) — record *every* population,
      not just the one you are working on. This is the check whose absence
      caused the 2026-08-17 incident.
- [ ] Establish what the operation covers. Grep the script for the endpoints it
      actually calls. "Union of both sites" was true of `:evidence` and false of
      `entities`, `hyperedges`, `relations` and `patterns`.
- [ ] Retain a rollback: rename stores aside, never delete.
- [ ] Note that an evidence write outage is **lossy** — no spool, no retry.

### Replaying evidence

- [ ] **Preserve `:id` and `:at`.** Without `:id` the server mints a fresh UUID
      (a duplicate, not the same evidence); without `:at` it stamps
      `Instant/now` and destroys the diachronic record.
- [ ] **Replay in ascending `(at, id)` — causal order.** The API pages
      newest-first, so a naive replay sends replies before their parents and
      every chat chain fails `reply-not-found`. This is referential integrity
      working correctly.
- [ ] Accept `201`; treat `duplicate evidence id` as already-present.

### After

- [ ] **Table census again, and diff it against the before.** Every population.
- [ ] Reset the checkpoint to a boundary covering the inserts, clear the basis,
      and let one real scan drain (§3, §9).
- [ ] Verify with a query that could not have been answered before the
      operation — not merely that counts moved.
- [ ] Confirm the **write path** works: new evidence appearing with timestamps
      after the operation. Easiest check to forget; everything else can pass
      without it.

### Choosing a direction

Given two stores, prefer **replaying the missing evidence into the live store**
over **swapping the live store for a derived one**. A replay touches one table
and is reversible per-document. A swap moves *every* table at once, including
the ones your verification does not cover. On 2026-08-17 the swap was chosen
because it was ~3 hours faster and its index was already drained — a speed
argument that only weighed the population that had been modelled.

---

## 8. Open question — should the store enforce this?

The rules above are discipline: they work, and they depend on whoever runs the
next replay remembering them.

> **Should a write landing behind the checkpoint invalidate the basis?**

The store can detect it — it knows the incoming `:at` and its own frontier.
Clearing the basis automatically would make the false claim structurally
impossible, at the cost of a rebuild after any backfill. A weaker, cheaper
variant: keep the basis but record that a behind-frontier write occurred, so a
consumer can see the claim is provisional.

Either is better than today's behaviour — silently keeping a basis that a bulk
insert has invalidated — which is the one option that should not survive.

**Undecided (Joe, 2026-08-17).** Not yet implemented.

---

## 9. Rebuilding

The sidecar is derived; a rebuild is always safe. Required after a schema
upgrade on an existing store (§4), after any backfill (§3), or whenever the
basis is suspect.

Run with the service **stopped** so nothing races the edit:

```python
# reset-checkpoint.py -- set the checkpoint back and clear the basis
BOUNDARY = "2026-08-10T00:00:00.000000000Z"   # or epoch for a full rebuild
c.execute("update fts_meta set v = ? where k = ?", (BOUNDARY, "last-at"))
c.execute("update fts_meta set v = ? where k = ?", ("", "last-id"))
for k in ("basis-tx", "basis-tx-ids", "basis-captured-at"):
    c.execute("delete from fts_meta where k = ?", (k,))
```

Restart, then let one scan cross the whole post-boundary region. Watch the
watermark climb and `scan-after` return empty; the basis is then **earned**.

Costs, measured on Zone (147k documents, 256 GB box): a bounded re-scan from a
week-old boundary is ~10 minutes; a full rebuild from zero is ~2.5 hours. Both
run **live** — search is degraded and the basis absent while it works, which is
honest and visible rather than silent.

---

## 9a. When a full rebuild will not run (2026-08-18, OPEN)

**On a long-lived store, a full evidence re-scan currently fails.** Attempted on
Zone's restored store (145,789 documents, written to continuously since March):

```
[fts] catch-up failed: Unconsumed nodes:
      [ArrowFieldNode [length=72103, nullCount=0],
       ArrowFieldNode [length=0,     nullCount=0]]
```

What was ruled out, and how:

| hypothesis | test | result |
|---|---|---|
| no lower bound → unbounded scan | re-ran with `--boundary 2026-01-01` | same error |
| page size too large | POST uses page 1000; periodic uses **200** | same error, both |
| transient / load-related | four attempts across 12 minutes | identical every time |
| **one corrupt stored batch** | probe from a *later* boundary | **REFUTED — see below** |

It is a **scan-volume ceiling, not a bad record.** The first three attempts all
reported `length=72103` and I read that constant as one specific corrupt Arrow
batch. That was wrong. Probing from a June boundary gave `length=71826` — the
number *tracks the scan*, and a fixed bad batch would give a fixed number.

The decisive experiment is the range, not the record:

| scan range | documents | result |
|---|---|---|
| 2026-08-10 → now | ~15,000 | **succeeds**, drains, basis earned |
| 2026-06-01 → now | ~112,000 | fails instantly |

So a scan over a large range dies assembling an oversized Arrow batch, while a
bounded recent range completes normally. `catch-up!` has **no upper-bound
parameter** — it always scans `[boundary, now]` — so you cannot simply walk the
corpus in chunks. That is what makes §9b the practical route.

Note what still works, because it bounds the problem precisely:

- **incremental tail catch-up is fine** — `{:indexed 13}` immediately after,
  basis re-earned, checkpoint advancing;
- `on-append!` indexes new writes normally, so live search is unaffected;
- every other query surface (hyperedges, entities, stats) serves normally.

Only the full historical scan fails. **Consequence: `ev_attr`/`ev_tags` cannot
be backfilled on this store by rebuilding, so facet filters stay near-empty for
history (§4) while free-text search remains complete.**

> **If you attempt this, restore the checkpoint afterwards.** A parked
> checkpoint makes the periodic sweep fail every 5 minutes and never re-earn a
> basis — strictly worse than not trying. Record `last-at`/`last-id` before you
> start; `fts-rebuild.py` prints them under `--- before ---`.

---

## 9b. Transplanting a sidecar (the way round the ceiling)

Because §9a blocks a full rescan, the practical way to get full facet coverage
is to **take an already-built sidecar from another store** and repair the
difference with one bounded catch-up. Done on Zone 2026-08-18; it took minutes
where a fresh re-ingest would have taken most of a day.

**Why it is sound.** C1: *"The index yields candidate document ids only. It
over-approximates and never under-approximates: no index state — stale,
corrupt, or rebuilt mid-flight — can produce a wrong answer, only a slower or
less complete one."* A donor indexing a **superset** of your evidence is
therefore safe: its surplus ids become candidates the store-side re-check
rejects. The direction that is *not* safe is a donor missing ids you have —
those documents silently become unfindable.

**So measure coverage first, both ways:**

```bash
scripts/sidecar-coverage.py <live-store-dir> <donor-store-dir>
```

On Zone this reported 2,385 surplus (harmless) and **547 missing** — all dated
`2026-08-10` or later, i.e. inside the window §9a proves is scannable. That is
what made the repair a single bounded catch-up rather than a blocker.

**Procedure** (`scripts/transplant-sidecar.sh`):

1. Copy the donor sidecar in beside the live one — the donor is static, so this
   costs no outage.
2. Set the staged copy's checkpoint back to a boundary covering the missing ids,
   and clear its basis. Edit the **staged** file, which nothing has open.
3. Swap the files, then run one catch-up. It covers the gap and re-earns the
   basis by a proven drain.

Result on Zone: `ev_attr` 57 → **148,183**, facet filters working from
2026-04 onward, `{:indexed 8157}`, basis earned, `recheck-rejections 0`.

### Two hazards this exposed

**`sudo` breaks `systemctl --user`.** Running the script under `sudo` gave
`Failed to connect to bus: No medium found`, and the stop/start silently did
not happen while the script reported an outage duration. Run file operations
with `sudo` if you must, but issue `systemctl --user` **as the owning user**.

**A restart may not be needed — and renaming a live SQLite db is still risky.**
`next.jdbc` opens the sidecar *by path per operation* rather than holding one
handle, so the swap took effect without a restart. But `-wal` and `-shm` are
named after the db file and were **not** renamed with it, so the old WAL briefly
paired with the new database. SQLite rejected it on salt mismatch and
`pragma integrity_check` returned `ok` — the safe outcome, but not one to rely
on. **Rename or remove `-wal`/`-shm` alongside the db**, and integrity-check
afterwards.

---

## 9c. Uniting two stores' evidence — the correct order of operations

The operation that went wrong on 2026-08-17 and was done correctly on 08-18.
**Goal:** the live store gains the evidence a second store has and it lacks.

| | what it touches |
|---|---|
| ❌ swap the live store's directory for the other one | **every table** — entities, hyperedges, relations, patterns all move with it |
| ✅ replay the other store's missing evidence *into* the live store | **`:evidence` only** |

The wrong version is faster and looks equivalent. It is not: the donor's
non-evidence tables are whatever they were when it was built, so swapping
silently reverts all of them. That is the whole of the 08-17 incident.

### The order, and why each step cannot move

1. **Census the live store** (`/health?deep=true`). Record *every* table. This
   is the before-picture that makes step 8 meaningful.
2. **Start a server on the donor, on its own port.** XTDB 2 local stores are
   single-process, so a store that is not being served cannot be read at all.
   The live service is untouched.
3. **Compute the delta by id**, bounded at a date both stores share. Check that
   assumption rather than trusting it.
4. **Replay delta → live, ascending `(at, id)`, preserving `:id` and `:at`.**
   Ascending because the store refuses a reply whose `:in-reply-to` parent is
   absent, and the API pages newest-first. **Do not touch the checkpoint here.**
5. **Stop the donor server.**
6. **Only now: reset the live checkpoint** to a boundary below the earliest
   replayed document, and clear the basis.
7. **One catch-up.** It drains across the whole region and re-earns the basis.
8. **Census again and diff.** Non-evidence tables must be unchanged, or grown
   only by live traffic.

**Why 6 cannot come before 4.** Resetting first, then inserting, lets a sweep
drain *while* documents are still landing behind it — you get a basis asserting
coverage of a region that was still being written. Insertion phase first, then
one reset, then one scan.

**Why 6 cannot be skipped.** The replayed documents are dated in the past, so
they land *behind* the checkpoint, and `catch-up!` only scans forward (§3). This
is not theoretical — it was observed live during this very operation:

```
basis-captured-at  2026-08-18T09:12:59      <- written by a periodic sweep
last-at            2026-08-18T09:04:08      <- ...that scanned forward from here
                                            while 2,384 documents dated
                                            08-10..08-17 sat behind it
```

The sweep was honest and its conclusion was false. The reset in step 6 is what
converts that into a real proof.

### Result (2026-08-18)

2,384 documents, `accepted 2384 | FAILED 0` in 185s; store 146,125 → 148,509;
catch-up `{:indexed 10878}`, basis earned; index and store exactly level at
148,522 (`delta +0`); spot-check 8/8 retrievable by id. Census confirmed every
non-evidence table intact and *above* its pre-operation count, i.e. grown by
live traffic and not disturbed:

| table | before | after |
|---|---|---|
| hyperedges | 470,363 | 470,987 |
| entities | 48,415 | 48,533 |
| relations | 49,483 | 49,497 |

Both sites' authors now answer in one result set (`codex-8` 9,334, `claude-9`
2,454, `mission-control/sync` 1,808, `codex-5` 643).

> **Footgun met on the way:** `pkill -f "store-dir <name>"` matches the shell
> command line running it, killing your own session. Harmless here, but use the
> pid from `pgrep` and check it first.

---

## 10. Incident log

**2026-08-17 — the drain that was never performed.** A 7,249-document replay
landed behind the checkpoint; a later sweep scanned forward, found nothing, and
wrote a `basis-tx` asserting coverage over six days it had never looked at. The
index was complete by luck (`on-append!` had not failed), not by proof. Fixed by
resetting to the divergence boundary and re-earning the basis.
Written up: `zone.hyperreal.enterprises/2026-08-17-drain-never-performed.html`

**2026-08-17 — the swap that reverted four tables.** The same merge was then
made live by swapping the store directory. The replay had covered `:evidence`
**only**, so `entities`, `hyperedges`, `relations` and `patterns` silently
reverted to a two-week-old snapshot — the pattern corpus going from a deduped
1,348 rows back to 4,655, and a day of agent work disappearing from the store
(though not from git). Every item on the verification checklist passed, because
every item was about evidence.

Rolled back 2026-08-18 in a 42-second outage, with the 35 intervening evidence
documents dumped beforehand and replayed after — lossless. **The lesson is §7:
a store swap moves every table, so the checklist must cover every table.**

**2026-08-18 — the union, done correctly.** The 08-17 goal finally met by the
other direction: 2,384 documents that existed only in the merged store replayed
*into* the live store, touching `:evidence` and nothing else. `FAILED 0`; index
and store level at 148,522; every other table intact. The false basis appeared
again mid-operation (a sweep drained at 09:12:59 over a 09:04:08 checkpoint while
the replayed documents sat behind it) and was cleared by the step-6 reset rather
than shipped. Procedure: §9c.

**2026-08-18 — the empty facet table.** The restored store had the new sidecar
schema but no historical attribute rows (`ev_attr` = 40 against 145,770 body
rows). Free-text search was complete; facet filters were silently near-empty.
See §4.
