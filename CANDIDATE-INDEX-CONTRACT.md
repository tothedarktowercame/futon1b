# The Candidate-Index Contract

**Status:** DERIVE draft, 2026-08-17 (M-evidence-landscape-index, deliverable 1).
This is the artefact that must survive migration to the eventual in-XT
facility (#3663). Implementation is written to this document, not the other
way round. Written first, per the mission's IDENTIFY.

**Upstream convergence map** — each term names the #3663 design element it is
the userspace image of, so the eventual migration is a swap of the candidate
and basis *sources*, with the contract unchanged:

| term | upstream element (#3663, jarohen 2026-08-09) |
|---|---|
| C1 | D1 "the index proposes, the store disposes"; INV2 IID-granular |
| C2 | the frontier; OQ2 (lag as an operational metric); #5730 sequencing |
| C3 | D4 (index = pure function of the permanent L0 sequence) |
| C4 | D2 declared indexes / per-column declaration |
| C5 | D5 probe → union → push down → resolve-and-recheck |
| C6 | D5's "step 3 is the entire cost of the index running behind" |

---

## C1 — Candidates, never answers

The index yields **candidate document ids only**. It over-approximates and
never under-approximates: no index state — stale, corrupt, or rebuilt
mid-flight — can produce a wrong answer, only a slower or less complete one.
Every candidate is re-checked against **the store's copy** of every
filterable field before use; the index's own columns are never consulted for
the final answer. Candidates are id-granular (one `xt/id` per entry), never
row- or page-granular.

*Test obligation:* a deliberate stale-read test must exist — the index is
made to assert something the store rejects, and the re-check demonstrably
drops it (mission acceptance bar, item 2).

## C2 — Explicit, queryable basis

The index carries a machine-readable **basis**: *"this reflects the store as
of these coordinates."* Concretely:

- `checkpoint` — the `(at, id)` keyset high-water mark of the last completed
  scan (existing `fts_meta` semantics, unchanged);
- `basis-tx` — the node's `:latest-completed-txs` coordinates, captured from
  `xt/status` **immediately before** the scan that drained to that
  checkpoint began (see C6 for why before, not after);
- `basis-captured-at` — wall-clock instant of that capture.

Every search response carries the basis. The stats surface computes
**staleness as a distance** — live `xt/status` coordinates minus `basis-tx`,
plus wall-clock age — never as a map-equality check. (Whole-map watermark
equality is known-too-strict: it fails spuriously against a quiet store —
Dionysus, 2026-08-14, 295 consecutive false rebuilds. Staleness is a number,
not a match.)

*Test obligation:* the watermark is queryable and a staleness window can be
stated in numbers (mission acceptance bar, item 3).

## C3 — Derived and rebuildable

The index is derived data. Deleting its file and rebuilding from a store
scan is always safe and yields an equivalent index (deterministic up to
FTS-internal layout). No write path treats the index as a source of truth;
no read path treats it as authoritative. The store's bitemporal machinery
remains the only authority on what exists and what matches.

## C4 — Declared projection

The indexed field set is an **explicit declaration** — a table in the
implementation of *store field → index column(s) → indexed-as* — not
"whatever happened to get written." A field absent from the declaration is
not queryable via the index (callers fall back to store-side predicates and
pay store-side costs). Extending the declaration is a lifecycle operation:
declare, then rebuild or backfill; never index a field ad hoc.

Rationale: per-field inclusion is what keeps the index proportionate
(textprobe: one low-value high-churn field class alone would have added
~2.19m postings), and it mirrors upstream `CREATE INDEX` declaration.

## C5 — Composition narrows candidates; the re-check decides

Content predicates (FTS) and attribute predicates (scalar columns, tag
membership) may compose freely **inside the candidate layer** to narrow the
candidate set — that is the entire point of unifying the two sidecars. But
composition changes nothing about authority: the re-check applies the same
predicates against the store's copy regardless of what the index already
claimed, and only the re-check's verdict counts. Hydration of surviving
candidates uses the store's SQL `IN` path (the one affordable id-set shape,
TN 2026-08-02 §1).

## C6 — Conservative basis advance

The live append hook may enrich the index at any time but **never advances
the basis** — neither the checkpoint nor `basis-tx`. Only a completed
catch-up scan does, and the `basis-tx` it commits is the one captured
*before* that scan started: everything committed to the store before the
capture is guaranteed scanned, so the basis under-claims coverage rather
than over-claiming it. An interrupted scan advances nothing. (This promotes
the existing `on-append!`/`catch-up!` checkpoint split — adopted after the
70-silent-misses incident — from implementation habit to contract term.)

The costs this buys are bounded and visible: re-indexing overlap on the next
catch-up (upsert dedupes), and a basis that lags live appends by at most one
catch-up interval — a number C2 makes queryable.

---

## Consumer obligations (operator-as-sensor interface)

The mission's ARGUE-2 section (`holes/M-evidence-landscape-index.md`)
binds six interface commitments, IC-1..IC-6, between this index and its
War Machine consumers. Two are contract-grade and restated here:

- **The response envelope is part of the contract** (IC-3): results with
  scores, `checked` vs `count`, and the basis form a typed observation
  vector; the stats surface additionally publishes the C4 declaration
  (`:projection`) and a named **residual** — the channels and fields this
  index structurally cannot see. Changing the envelope is a contract
  change.
- **Absence from the index is *unmeasured*, never *didn't happen***
  (IC-2): a consumer reading candidate sets as observations must carry
  the residual and the basis alongside every finding it derives.

## What this contract does NOT cover

Authoritative reads from the index. Ranking semantics beyond BM25
pass-through. Semantic/vector retrieval. Mutable-table indexing
(hyperedges/entities: tombstones and retraction coverage are a follow-on
with their own contract obligations). The extraction of typed operator
assertions from retrieved turns (a propose-and-confirm surface, deliberately
out of scope — the index makes assertions *retrievable*, a human confirms
what they *mean*).
