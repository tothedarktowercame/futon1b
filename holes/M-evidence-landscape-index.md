# Mission: M-evidence-landscape-index — a derived candidate-index sidecar for the Evidence Landscape, built to the shape of the eventual XTDB facility

**Status:** IDENTIFY **accepted by Joe 2026-08-17**; MAP + DERIVE + ARGUE +
VERIFY completed same day (sections below, per
`futon4/holes/mission-lifecycle.md`). **Stopped at the INSTANTIATE
boundary** — build to be orchestrated via Agency/Codex per the
coding-handoff protocol.
~~**Gate:** operator-acceptance — the VERIFY §V2 amendment to acceptance-bar
item 4 (composed query, not bare content retrieval).~~ **CLEARED by Joe in
conversation, 2026-08-17** ("Going towards the 'composed query' definitely
has my nod"), with one condition attached: the build must adhere to an
agreed interface with the War Machine work — see ARGUE-2 below, written to
discharge exactly that condition before INSTANTIATE.
**Gate:** operator-decision — posting the drafted #3663 reply
(`futon7/.../reply-3663-draft-2026-08-17.md`) is Joe's send; deliberately
deferred until the build gives it a demonstrable touchpoint.
Sibling of `M-xtdb-22x-benchmarking.md` (same store, same two upstream issues).

---

## HEAD — operator anchor

> "Build the railroad track while we run the sidecar." — Joe, 2026-08-15

And, from the same conversation, the thing that motivates it:

> "I am in the outside world, and I am currently the stack's best source of
> information about it."

The Evidence Landscape already persists every operator turn to XTDB. What it
cannot do is let anyone *ask* it anything that isn't a structured-field filter.
So the stack's highest-bandwidth sensor writes to a store that cannot be
interrogated about what it saw.

## Why a mission and not a task

"Add an index" would be a task. Three things make this a mission:

1. **It has an upstream-convergence constraint.** The eventual in-XT facility
   has a now-known shape (a second producer with a serialization point,
   #5730). Building to converge on that shape is a design decision that must
   be made before implementation, not after.
2. **It has a specification role.** A running sidecar with production timings
   is the strongest available contribution to #3663 — a contract someone can
   critique now, rather than a feature request.
3. **Its use case is not "search".** It is operator-assertion mining, which
   determines which query shapes matter and which don't.

## Facts on the ground (verified 2026-08-15)

**Our store.** ~94k docs live over HTTP; `code/v05/var` alone is 34,480;
`xt/id` strings 60–70 chars. futon1b is **not** an external-source database.

**The query ceilings** (`TN-xtdb2-query-ceilings-and-ingest-memory-2026-08-02.md`,
from repairing a real outage, not a benchmark exercise):

- Fetching N documents by primary key **cannot be expressed in XTQL**: `in`,
  `contains?` and `any` all reject on type grounds.
- The one accepted XTQL form is the wrong shape. On a 40k fixture: 50
  sequential point lookups 6393 ms; one 50-clause `or` 2196 ms; **SQL
  `_id IN (?…)` 400 ms** — 16× faster than point lookups, 5.5× faster than the
  disjunction.
- On the **live** store the gap is far worse: a single `(= xt/id x)` costs
  ~50 ms and is properly indexed, but **one 50-clause `or` of those same
  equalities costs ~40 seconds.**

**Two sidecars, one missing abstraction**
(`TN-xtdb-derived-secondary-index.md`, parked):

1. `futon1b_text.clj` — SQLite FTS5 beside the store, because XTDB2 has no
   text indexing (#5637, open, "not started").
2. The planned attribute sidecar — SQLite B-tree + tag junction tables,
   because XTDB2 `where` predicates are scan predicates over per-column page
   metadata, not index seeks (#3663, open, `spec-reqd`; the issue itself calls
   the metadata approach under-selective for unique/near-unique attributes).

Same architecture both times: **a derived, rebuildable candidate index whose
answers are never authoritative** — candidates point-hydrated and re-checked
against XTDB truth. Arriving at it twice independently is the evidence that it
is one abstraction rather than two features.

**Upstream state.** #3663 open (`spec-reqd`); #5637 open, not started; **#5733
open and in-progress** — DDL cannot target external-source databases because
`Database.submitTxBlocking` rejects every locally-submitted client transaction
(the tx-id sequence belongs to the CDC source; a deliberate sequencing
invariant, not a permission check). Its first part, **#5730 — a serialization
point for a second producer against external-source transactions** — is
implemented via PR #5731.

**The timeline moved, and its owner said so** (James Henderson, 2026-08-09,
logged at
`futon7/data/outbox/staged/2026-07-26--james-henderson--xtdb-3663--jx5637/correspondence-2026-08-09.md`):
a solution in XT is incidentally blocked by #5733, which requires non-trivial
changes *after 2.2 is released*.

**We are nevertheless not blocked.** #5733's client-submit ban applies to
external-source databases. futon1b is not one. The invariant that stops the
upstream fix shipping does not stop a userspace derived index.

## IDENTIFY

### The gap

The Evidence Landscape can be filtered by `session-id`, `author`, `type` and
`claim-type` — all structured fields. It cannot be queried by **content**, and
it cannot support **projection-then-fetch** (select N candidate ids, hydrate
their bodies) at any affordable cost. Both are prerequisites for the motivating
use case, and neither is reachable by tuning.

### Motivating use case (which fixes the query shapes)

Mining **operator assertions about the outside world** from the turn stream.
On 2026-08-15 alone, five conclusions reached from repository artifacts were
inverted by facts the operator supplied in passing — a cold EOI already sent, a
verbal £5,000 agreement, futon4 in daily use, the devmaps superseded rather
than abandoned, and a set of call notes that existed while the work proceeded
from a derived draft. None of those were reachable by more careful reading,
because they were not in what was being read.

Those facts are already persisted as turns. They are not retrievable.

### Deliverables

1. **The candidate-index contract** — the artefact that must survive migration
   to the in-XT facility. Written first, implementation second.
2. **Sidecar implementation** over SQLite (FTS5 for content, B-tree + junction
   tables for attributes), unified behind that one contract rather than as two
   independent sidecars.
3. **Watermark discipline** — see invariant I2.
4. **A #3663 contribution**: the contract, the production EXPLAIN plans, and
   the live timings above, offered as a running reference implementation for
   the upstream design to be checked against.

### The two invariants (this is the railroad-shape adoption)

- **I1 — non-authoritative candidates.** The index returns candidates only;
  every candidate is point-hydrated and re-checked against XTDB truth before
  use. Already true of both existing sidecars; now stated as a contract term.
- **I2 — explicit tx-id watermark.** The index carries *"this reflects the
  store as of tx N"* rather than being merely rebuildable. This is the half we
  are currently missing, and #5730 is what tells us it is the right half:
  a derived index **is** a second producer, and a second producer needs
  deterministic ordering against the primary stream.

I2 buys three things immediately, none of which depend on upstream: staleness
becomes expressible rather than a vibe; the re-check step gets a defined window
instead of an unbounded one; and when the railroad lands, the semantics we have
been running are already the semantics it enforces.

### Anti-gold-plating discipline

**Adopt the invariants, not the API.** Two things, not a framework.
Anticipating the eventual call signature would mean building against a design
that has not been made yet — which is the failure mode this approach exists to
avoid. If a third invariant is proposed, it needs an upstream artefact to point
at, as I2 points at #5730.

### Scope out

Authoritative reads from the sidecar. Any write path that makes the index the
source of truth. A second JVM. Upstream patches to XTDB itself. Semantic or
vector retrieval (a separate question from index-backed lookup).

### Acceptance bar

- A **named query that is currently unaffordable becomes affordable**, measured
  on the live store, reported as before/after with the same methodology as the
  query-ceilings technote.
- A **deliberate stale-read test** demonstrates I1 holding: the index returns a
  candidate that XTDB truth rejects, and the re-check catches it.
- The **watermark is queryable** and a staleness window can be stated in
  numbers rather than adjectives.
- At least one operator assertion from the 2026-08-15 session is **retrieved
  from the landscape by content**, having been unreachable before.

## Constraints

- **No third JVM** (operator standing constraint); futon1b serves standalone on
  :7073 and is not to be restarted casually.
- RSS ≈ 2× `-Xmx` is **expected**, per the Henderson call — by design, not a
  leak signature.
- The 2.2.x migration (`M-xtdb-22x-benchmarking.md`) is a sibling, not a
  dependency; this mission must not block on it.

## Relationship to other missions

- **`M-xtdb-22x-benchmarking`** — sibling; shares the two upstream issues and
  the arXiv/SO corpora.
- **`futon0/holes/M-capability-levels.md`** — consumer. 32 capabilities sit at
  `:scale :tbd` and 37 at `:position` unset; most are blocked on facts only the
  operator holds, which is what this index makes retrievable.
- **`futon7/README-conversion.md`** — consumer; the engagement chain's
  operator-attested provenance state has the same shape.
- **`futon3c` M-apm-demonstration** — parallel bookkeeping; the same
  attempt-schema is intended to carry human and agent subjects alike.

## Source material

`TN-xtdb2-query-ceilings-and-ingest-memory-2026-08-02.md` ·
`TN-xtdb-derived-secondary-index.md` ·
`holes/SPIKE-attribute-index-2026-07-26.md` · `holes/E-SO-to-XT.md` ·
`futon1b_text.clj` · XTDB #3663, #5637, #5733, #5730/#5731 ·
the 2026-08-09 correspondence record in futon7.

## ARGUE (strategic, plain language)

We keep the best record of our own work that we have ever kept, and we cannot
ask it anything. The store answers questions about *which session* and *which
author*, never about *what was said* — so the one channel that reliably brings
outside-world information into the stack writes to a place nobody can search.

The fix is not to wait. The upstream facility now has a known shape and a
timeline its own maintainer has said will slip past 2.2, and the reason it is
blocked does not apply to our store. So we build the thing we need, in
userspace, deliberately shaped like the thing that is coming — non-authoritative
candidates, an explicit watermark — and the migration becomes a swap rather
than a rewrite.

The by-product is worth as much as the artefact. A running index with
production timings is a specification the upstream design can be checked
against, offered at the moment that design is still cheap to change, by the
only party with a 94k-document store and a documented outage behind the
numbers.

## VERIFY / INSTANTIATE

Deferred until the IDENTIFY above is accepted. The acceptance bar names four
testable conditions; VERIFY should add nothing to them and subtract nothing
from them.

---
---

# Lifecycle phases (post-acceptance, 2026-08-17)

IDENTIFY accepted by Joe 2026-08-17 ("I accept IDENTIFY, but we should work
forward in ./futon4/holes/mission-lifecycle.md terms. We can stop when we get
to INSTANTIATE."). The sections above are the accepted IDENTIFY-era record
and are not edited; everything below accretes.

## MAP (2026-08-17, research only — facts, not decisions)

### Infrastructure inventory

- **FTS5 sidecar** (`futon1b_text.clj`, 405 lines) — the donor for the
  unified index. Already provides: candidate query with over-fetch
  (max(50, 4k)) + BM25 ranking; **re-check against the store's copy** in
  waves of 4 under the process-wide 4-permit budget, short-circuiting at k
  survivors; `(at, id)` keyset checkpoint in `fts_meta`; `on-append!`
  fire-and-forget that deliberately never advances the checkpoint;
  single-flight `catch-up!` (page 1000 boot / 200 periodic); periodic repair
  loop (5 min default); **attributable** error stats (`:last-error` carries
  id+at+message, not just a count); WAL; busy_timeout 10 s; `:index-as-of`
  already returned on every search response (time-valued).
- **Watermark source exists in-process:** `xt/status` →
  `:latest-completed-txs` + `:latest-submitted/processed-msg-ids`
  (`futon1b_graph.clj:835` `node-watermark`), already HTTP-exposed via the
  restart-readiness route (`futon1b_server.clj:621`). **Design lesson
  attached to it** (Dionysus 2026-08-14, `futon1b_graph.clj:811-818`):
  whole-map watermark *equality* is too strict — 295 consecutive false
  rebuild failures against a quiet store. Staleness must be a distance.
- **Hydration:** `hydrate-projected` (`futon1b_evidence.clj:101`) — one
  parameterised SQL `IN` per page, ≤1,000 ids, order restored client-side;
  the N+1 oracle is retained for tests.
- **Evidence write path:** `build-evidence-doc` — single append-only doc;
  columns id/type/claim-type/author/at/body/tags/subject/pattern-id/
  session-id/in-reply-to/fork-of/conjecture?/ephemeral?. `on-append!` is
  wired on the server's evidence POST path.
- **Filter placement today** (spike 07-26, unchanged): type/claim-type/
  author/session-id/fork-of are XTDB scan predicates; **tags, subject-type/
  subject-id, pattern-id, include-ephemeral are Clojure post-filters over
  ordered 1,000-row pages** — the unaffordable shapes.
- **Live service:** `migration-store-21`, :7073; the real sidecar file is
  `migration-store-21/fts5-evidence.db` (316 MB). NB the repo-root
  `fts5-evidence.db` (32 KB, 0 rows, dated 08-04) is a stale test artifact —
  do not measure against it.

### Data inventory (probed live, 2026-08-17 ~08:20, all bounded)

- Index: **140,233 rows** (store has grown ~48% since the 94,430-row D1
  run); checkpoint fresh (06:49 same morning); periodic loop on; 3 recorded
  append errors, last = SQLITE_BUSY 2026-08-16 (repaired by catch-up —
  the attributable-error surface works).
- **Turn-capture shape** (point-read of a live 08-15 doc): author `joe`,
  type `:coordination`, **claim-type `:question`**, tags
  `[:claude :chat :turn :user]`, session-id, subject
  `{:ref/type :session}`, body map `{:event "chat-turn" :text …}` with
  mission props. All operator turns share claim-type and tags — so
  *"turns where Joe asserted something about the outside world"* is not
  expressible by structured filter; it needs content × attribute compose
  (and, later, propose-and-confirm extraction — out of scope here).

### Surprises (recorded before DERIVE, as the phase requires)

1. **Bare content retrieval of the 08-15 assertions ALREADY WORKS.**
   `text-search?q=Michaela` returns Joe's 08-15 15:52:30 turn in
   milliseconds. IDENTIFY's "it cannot be queried by content" was true of
   the *structured endpoint*, not of the landscape: the FTS sidecar covers
   content, filtered by author/session/since/before/ephemeral only. The
   real gap is **(a) attribute-side candidates (tags/claim-type/type/
   subject/pattern-id), (b) content × attribute composition, (c) the I2
   basis.** Consequence for the acceptance bar → VERIFY §V2.
2. Candidate-layer cost is negligible at this scale (spikes, read-only on
   the live 316 MB file): FTS MATCH over 140k rows **2 ms**; MATCH +
   attribute filter **2–15 ms**; building a full 140k-row attribute
   projection table + index **185 ms**. The binding cost is entirely in the
   XTDB re-check/hydration — machinery that already exists and is proven.
3. The half-built I2 already has the right bones: `:index-as-of` on every
   response, attributable `:last-error`, checkpoint-never-advanced-by-
   append. The contract largely *promotes existing discipline to terms*.

### Ready vs missing

| ready (no new code) | missing (the work) |
|---|---|
| FTS5 candidates + BM25, re-check loop, wave hydration | `ev_attr` scalar table + `ev_tags` junction, same file |
| catch-up/append/periodic lifecycle + single-flight | composed candidate SQL (MATCH ∩ attr ∩ tags), attr-only mode |
| `(at,id)` checkpoint in `fts_meta` | `basis-tx` capture (xt/status) + commit-on-drain |
| SQL `IN` hydrator + N+1 oracle | re-check extended to claim-type/type/tags/subject |
| `xt/status` watermark source; restart-readiness route | staleness-as-distance in stats; basis in responses |
| attributable error stats | stale-read test; before/after measurement; backfill run |
| penholder gating pattern for ops endpoints | contract doc → **DONE 08-17: `CANDIDATE-INDEX-CONTRACT.md`** |

### MAP questions, answered

- **Q1 — is a tx-basis observable from userspace?** YES:
  `(xt/status node)` in-process (the sidecar shares the JVM);
  `:latest-completed-txs` is the tx coordinate; already serialized on the
  restart-readiness route. Comparison discipline: distance, never equality.
- **Q2 — how much of I1/I2 does the FTS sidecar already satisfy?** I1
  fully (store's copy decides; index columns never authoritative). I2
  half: rebuildable checkpoint + time-valued `:index-as-of`, but no
  tx-anchored basis and no staleness number.
- **Q3 — which query shapes does the mining use case need?**
  (a) content × tags/claim-type/author (the sensor sweep);
  (b) attribute-only candidates → SQL `IN` hydrate (projection-then-fetch);
  (c) session/time windows (already served).
- **Q4 — are the 2026-08-15 assertions in the landscape?** YES, verified
  by live retrieval.
- **Q5 — is evidence-only scope sufficient?** YES: turns are evidence;
  evidence is append-only (no tombstones needed); hyperedges/entities stay
  out per IDENTIFY's scope-out.
- **Q6 — one sqlite file or two?** Deferred to DERIVE (decision D-1 below).

## DERIVE (2026-08-17)

The contract is the primary design artefact: **`CANDIDATE-INDEX-CONTRACT.md`**
(repo root, beside the TNs) — terms C1–C6 with the upstream-convergence map.
This section holds the implementation design against that contract.

### Declared projection (contract C4)

| store field | index home | indexed-as |
|---|---|---|
| `:evidence/body` (text render) | `ev_fts.body` | FTS5 unicode61, no stemming (unchanged) |
| `:evidence/author`, `:at`, `:session-id` | `ev_fts` UNINDEXED cols (existing) + `ev_attr` | B-tree |
| `:evidence/type`, `:claim-type` | `ev_attr.type`, `ev_attr.claim_type` | B-tree, composite with `at` |
| `:evidence/tags` (multivalued) | `ev_tags(tag, id)` junction | PK (tag, id) + index (id) |
| `:evidence/subject` (`:ref/type`, `:ref/id`) | `ev_attr.subject_type`, `.subject_id` | composite B-tree |
| `:evidence/pattern-id` | `ev_attr.pattern_id` | B-tree |
| `:evidence/ephemeral?`, `:conjecture?` | `ev_attr` int flags | none (re-check-only assist) |
| NOT declared: `:in-reply-to`, `:fork-of`, `:id` | — | fall back to store predicates |

Values stored verbatim as `(str v)` (keywords keep the leading colon, e.g.
`":question"`) — representation is index-internal; the re-check reads the
store's typed copy, so no coercion subtleties can leak into answers (C1).

### Schema delta (one file, one lifecycle — see D-1)

```sql
CREATE TABLE IF NOT EXISTS ev_attr (
  id TEXT PRIMARY KEY, type TEXT, claim_type TEXT, author TEXT, at TEXT,
  session TEXT, subject_type TEXT, subject_id TEXT, pattern_id TEXT,
  ephemeral INTEGER, conjecture INTEGER);
CREATE INDEX IF NOT EXISTS ev_attr_claim_at ON ev_attr(claim_type, at);
CREATE INDEX IF NOT EXISTS ev_attr_type_at  ON ev_attr(type, at);
CREATE INDEX IF NOT EXISTS ev_attr_auth_at  ON ev_attr(author, at);
CREATE INDEX IF NOT EXISTS ev_attr_subject  ON ev_attr(subject_type, subject_id);
CREATE INDEX IF NOT EXISTS ev_attr_pattern  ON ev_attr(pattern_id);
CREATE TABLE IF NOT EXISTS ev_tags (
  id TEXT, tag TEXT, PRIMARY KEY (tag, id)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ev_tags_id ON ev_tags(id);
-- fts_meta gains keys: "basis-tx" (pr-str of :latest-completed-txs),
--                      "basis-captured-at"
```

### Maintenance (extends, does not replace)

`index-batch!` writes all three tables **in the one existing transaction**
per batch (delete-by-id + insert for each) — atomicity is what keeps
`ev_fts`/`ev_attr`/`ev_tags` mutually consistent, so a candidate join can
never straddle a half-indexed doc. `on-append!`, `catch-up!`, the periodic
loop, single-flight, and error attribution are all unchanged in shape.
`catch-up!` gains the C6 basis step: capture `(node-watermark node)` before
the first page; on drain (empty page), `meta-set!` basis-tx +
basis-captured-at alongside the existing checkpoint write. Interrupted scan
⇒ nothing advances (existing behaviour, now covering basis too).

**Backfill** for the new tables = the existing lifecycle op: delete
checkpoint (penholder-gated POST, exists) → full catch-up (~141 pages at
1,000/page against the live store; run off-peak; the D1 full build is the
precedent).

### Query path

- **Candidates** (one SQL statement, three composable fragments):
  `FROM ev_fts f JOIN ev_attr a USING (id)` +
  `WHERE ev_fts MATCH ?` (omitted in attribute-only mode → `FROM ev_attr a`) +
  scalar predicates on `a.*` +
  one `EXISTS (SELECT 1 FROM ev_tags t WHERE t.id = a.id AND t.tag = ?)`
  per requested tag (AND semantics, matching the HTTP contract's
  all-tags-present). Order: `bm25(ev_fts)` when MATCH present, else
  `a.at DESC, a.id DESC` keyset. Over-fetch unchanged (max(50, 4k)).
- **Re-check** (C5): `passes-recheck?` extended to claim-type/type/tags/
  subject/pattern-id, read from the store's doc; `recheck-cols` widened
  accordingly; wave machinery unchanged.
- **Hydration**: surviving ids through the existing SQL `IN` hydrator when
  bodies are wanted (hydrate=true default preserved).

### HTTP surface

Extend `GET /api/alpha/evidence/text-search` (name kept; `q` becomes
optional — absent `q` = attribute-only candidates): new params `type`,
`claim-type`, `tags` (repeatable), `subject-type`, `subject-id`,
`pattern-id`. Response gains `:index-basis {:checkpoint [at id]
:basis-tx … :basis-captured-at …}` (keeping `:index-as-of` for existing
callers). `?stats=true` gains `:basis` and `:staleness {:tx-lag <n>
:age-ms <n>}` computed live against `xt/status` — tx-lag as max coordinate
delta across the per-db map, **distance not equality**.

### Decisions (IF/HOWEVER/THEN/BECAUSE)

- **D-1 one file.** IF a second sqlite file would isolate the new tables,
  HOWEVER two files mean two checkpoints and two bases — recreating the
  "two sidecars, one missing abstraction" defect this mission exists to
  close, THEN all three tables live in `fts5-evidence.db` under one
  checkpoint/basis and one batch transaction, BECAUSE the contract's unit
  is *the index, singular* (TN-derived-secondary-index).
- **D-2 basis captured before the scan, committed on drain.** IF capturing
  after the drain would be fresher, HOWEVER a scan reads the live store
  while appends continue, so only the *pre-scan* status is provably
  covered by a completed drain, THEN capture-before/commit-after, BECAUSE
  C6 requires the basis to under-claim, never over-claim — same invariant
  that keeps `on-append!` off the checkpoint.
- **D-3 staleness is a distance.** IF map-equality would be simpler,
  HOWEVER equality fails spuriously on a quiet store (Dionysus 08-14, 295
  consecutive false failures), THEN staleness = coordinate delta + age,
  BECAUSE the contract needs a *number* an operator can put a threshold on.
- **D-4 evidence-only.** IF hyperedges/entities have the same query pain,
  HOWEVER they are mutable and need tombstone/retraction coverage the
  append-only design gets for free, THEN this mission indexes evidence
  only, BECAUSE scope-out already says so and the contract's
  "What this does NOT cover" holds the boundary explicitly.
- **D-5 extend the existing endpoint.** IF a new `/evidence/search` route
  would be cleaner, HOWEVER every existing caller (zaif retrieve arm, ops
  probes) already speaks text-search, THEN extend text-search with
  optional `q` + new params, BECAUSE one candidate surface per contract
  beats two surfaces per convenience.

### PSR

- Pattern chosen: math-informal/hybrid-certification
- Candidates: hybrid-certification, estimate-by-bounding
- Rationale: the flexiarg's IF/HOWEVER is this design verbatim — "the
  numerical step identifies candidates; the exact step certifies them...
  the method must detect [certification failure] and fall back rather than
  produce a wrong answer." The FTS/attr layer is the cheap locator, the
  XTDB re-check the exact certifier, C1 the fall-back guarantee.
- Confidence: high — the pattern is already proven in this codebase
  (futon1b_text.clj D1 run, 10/10 oracle).
- Library note: no dedicated `derived-candidate-index` pattern exists in
  futon3/library; after INSTANTIATE's PUR this mission is a candidate
  donor for one.

### Stale-read test design (acceptance item 2)

Doctor the index directly (test-only sqlite UPDATE of one `ev_attr.author`
or `ev_tags` row to a value the store contradicts), then query with the
doctored value as filter: the candidate layer MUST surface it, the re-check
MUST drop it, and the response MUST show `checked > count`. This tests C1
end-to-end rather than simulating it, and the doctored row is repaired by
the next catch-up (which is itself worth asserting — C3 in action).

## ARGUE (2026-08-17, lifecycle sense — post-DERIVE synthesis)

*(The IDENTIFY-era "ARGUE (strategic, plain language)" section above stands
as strategic context; this section is the design-level argument.)*

- **Pattern cross-reference:** hybrid-certification (PSR above) governs the
  whole read path. The C6 conservative-advance term is an instance of the
  house verify-before-write discipline applied to metadata: never record
  coverage you have not witnessed. The declared-projection term (C4) is the
  per-field-inclusion lesson from textprobe promoted to structure.
- **Theoretical coherence:** IDENTIFY's two invariants survive DERIVE
  unchanged and gained four siblings (C3–C6) — but every added term names
  an existing artefact (upstream design element or a dated house incident),
  honouring the anti-gold-plating rule: adopt invariants with evidence
  behind them, never anticipate an API.
- **Trade-offs accepted:** (1) recall on the newest appends lags by at most
  one catch-up interval — bounded, and C2 makes the bound queryable rather
  than vibes; (2) the 3-table batch transaction lengthens sqlite's
  single-writer hold — the known SQLITE_BUSY pressure point (one live
  occurrence 08-16); mitigated by the existing small periodic page (200)
  and busy_timeout, and measured at VERIFY; (3) keyword/string
  representation in the index is lossy — deliberately irrelevant, because
  the re-check reads typed store truth (C1); (4) no ranking work, no
  semantic retrieval — scope-out holds.
- **Generalization:** the contract is store-agnostic (nothing in C1–C6
  names sqlite or FTS5); the same terms should hold for the mutable-table
  follow-on (plus tombstone obligations) and for the eventual in-XT
  facility (the convergence map in the contract file is the migration
  plan).
- **Plain language:** We keep a complete record of every working
  conversation, but we can only ask it "who said something, when" — not
  "what was said about X." The fix is a small, disposable side-index that
  proposes likely matches fast, while the real database always gets the
  final word on every answer, so the side-index can be stale or wrong
  without ever lying to us. The side-index also stamps every answer with
  exactly how up-to-date it was, as a number. We are building it in the
  same shape the database's own maintainers have said their future
  built-in version will take, so when that arrives we swap ours out
  without changing anything that depends on it.

## ARGUE-2 (2026-08-17, operator-as-sensor interface — pattern cross-check, at Joe's direction)

**Prompt (Joe, 2026-08-17):** *"my one concern is that we're building
towards an agreed interface with the War Machine work. I.e., right now it
only treats the Operator as someone to nag, not someone who can learn
things or whose signals are thought about in a meaningful way. Basically,
everything I assert is something to do with the outside world, just
filtered into language and typed or spoken into a terminal. The question is
what we'll use that for."*

### What the index is FOR, stated as WM architecture

The WM's operator lane today is **engagement without observation**:
lifecycle gates surface to the operator as actions (the NAG side of
`war-machine/advanceability`), but nothing the operator says back enters
the system as an observation that updates belief.
`aif/status-gated-belief-update` names precisely this split — *engagement
status* (does the agent act on the instance) versus *observation
availability* (does the agent see the instance's state) — and the operator
currently has the first without the second. **This index is the
observation side of the operator lane.** The sensor framing from the
claude-6 session ("you're a sensor, and you're only instrumented as a
controller") is that same statement pre-formalisation.

That is the answer to "what we'll use that for," and it is what the
implementational shape must not foreclose. The pattern survey below
(war-machine, war-room, capability, aif sublibraries) converts it into
named interface commitments — the social exotype of the operator channel,
written before implementation per `wr-3-social-exotype-before-implementation`.

### Interface commitments — binding on THIS build (INSTANTIATE gates)

- **IC-1 — Voice separation is expressible at the candidate layer, and
  certified at the re-check.** Operator-voice sweeps (`author=joe` ∧ tags
  `[:user]` ∧ content) compose in one candidate query, and the re-check
  reads the store's copy of author and tags. Per
  `capability/hinge-capability-extraction`: agent-view and operator-turn
  are *different evidence types*, and without voice separation agent prose
  contaminates every operator reading. The amended acceptance-bar item 4
  IS the IC-1 demonstration.
- **IC-2 — Coverage travels with every answer; the residual is a named
  field.** Every response already carries the basis (contract C2); the
  stats surface additionally carries the **declared projection** (what
  this channel indexes) and a **named residual** (what it structurally
  cannot see): assertions on unlogged surfaces — calls, email, speech;
  the Henderson 2026-08-09 message invisible for six days is the incident
  — plus pre-capture history and undeclared fields. Per
  `war-machine/half-blind-observation` (a half-covered observation
  surface with unacknowledged gaps is worse than none) and
  `capability/the-instrument-selects-what-you-cultivate` (name the
  residual as a field; blind spots read as absences). Consumers MUST
  treat absent-from-index as *unmeasured*, never as *didn't happen*.
- **IC-3 — The response envelope is a committed observation vector, not
  incidental JSON.** `{results+scores, checked vs count, index-basis,
  and the stats projection/residual}` is the typed shape a WM consumer
  reads (`aif/structured-observation-vector`); it versions with the
  contract, and changing it is a contract change.
- **IC-4 — Failure is observable enough to change consumer behaviour.**
  Staleness (`:tx-lag`, `:age-ms`) and the re-check rejection rate
  (`checked − count`, plus a cumulative counter in stats) are exposed as
  numbers so a consumer *can* gate on them — e.g. refuse or flag a
  belief-update sweep against a basis older than threshold. Per
  `war-machine/operational-not-decorative`: an unconsumed staleness
  number is decoration; the commitment here is to expose it in gateable
  form, and the consumer-side commitment (IC-5/6) is to gate on it.

### Interface commitments — binding on the CONSUMER (follow-on; this build must not preclude them)

*(Committed home, specified 2026-08-17:
`futon5a/holes/excursions/E-wm-operator-observations.md` — blocked on this
mission's core, takes up IC-5/IC-6 plus the sweep/propose/confirm loop.)*

- **IC-5 — Mined assertions become provenance, never statuses.** A
  retrieved operator assertion enters the stack as an
  **operator-attested, dated** provenance record via propose-and-confirm
  (the third provenance state of
  `capability/derive-the-claim-from-the-evidence`); the index never
  writes a capability status, and substrate-vs-curated disagreements are
  *surfaced*, not silently resolved. `aif/no-self-certification` gives
  the deeper reason: operator assertions are exactly the
  `:independent? true` evidence class — the out-of-band witness that a
  verdict apparatus cannot manufacture about itself — and the voice tags
  of IC-1 are the birth-tags that make that independence checkable. An
  agent turn must never be readable as operator attestation.
- **IC-6 — Precision is the consumer's, features are ours.** The index
  exposes channel features (transport, voice, staleness, score) and bakes
  in no weights; the WM's per-channel precision registry
  (`aif/evidence-precision-registry`) weights operator-assertion evidence
  there, auditable, not here, implicit.

### Adherence and capture checks

- **Inhabitation** (`wr-4-inhabit-before-building`,
  `war-machine/inhabitation-threshold`): no new surface is built. The
  first consumers are surfaces already inhabited — the extended
  text-search endpoint (D-5), the WM loop's sweep, the capability
  star-map derivation (32 `:scale :tbd` entries blocked on facts only the
  operator holds), and the operator's own recall queries.
- **State-capture check** (`war-machine/state-capture` — name the
  external sorry this stack work closes): (1) the operator-assertion
  blindness that inverted five conclusions on 2026-08-15 — an outside-
  world grounding failure, not an infrastructure itch; (2) the #3663
  collaboration deliverable to JUXT (the drafted reply + reference
  implementation). Both external; not capture.
- **Cultivation risk, named now** (`the-instrument-selects-what-you-
  cultivate`, second harm): once operator-assertion mining exists, the
  stack will preferentially cultivate what the instrument sees — *typed
  turns*. The residual field (IC-2) is the guard: unlogged channels stay
  visibly unmeasured rather than quietly devalued, and the ten-minute
  logging habit (the `correspondence-2026-08-09.md` move) remains a
  practice, not a nag.

### What ARGUE-2 changes in DERIVE (addendum, accreted not rewritten)

- **D-6 (IC-2/IC-3):** `?stats=true` additionally returns `:projection`
  (the C4 declaration table, machine-readable) and `:residual` (static
  declared list of not-covered channels/fields), plus a cumulative
  `:recheck-rejections` counter (IC-4).
- **D-7 (IC-1):** the named acceptance query for amended bar item 4 is
  the voice-separated sensor sweep: `author=joe ∧ tags=[:user] ∧ content
  MATCH <t>` — one candidate query, re-check certified.
- **Packet impact:** P-query gains the D-6 fields and a voice-compose
  test; no schema change (D-6 is computed/static, not stored). P-before
  unchanged.

## VERIFY (2026-08-17, pre-INSTANTIATE checks)

- **V1 — Structural verification: no wiring diagram, recorded why.** One
  process, one repo, no new ports or inter-component interfaces — the
  design extends an existing in-JVM subsystem. An exotype diagram would
  draw one box.
- **V2 — Completion-criteria pre-check** (each IDENTIFY bar item against
  the design):
  1. *Named unaffordable query → affordable.* Covered: candidates for the
     before/after are (a) attribute-only `tags=X` (today a post-filtered
     1,000-row page walk — seconds to timeout on sparse tags) and (b) the
     composed sensor sweep `author=joe ∧ tags=[:user] ∧ content MATCH t`
     (today inexpressible in one query). **Before-leg measurements must be
     captured against the live store before the build lands** (TN 08-02
     methodology; journal-quoted; run off-peak, bounded, background).
  2. *Stale-read test.* Covered by the DERIVE test design (doctored index
     row; `checked > count`; repaired by next catch-up).
  3. *Queryable watermark, numeric staleness.* Covered by C2/C6 + the
     stats surface (`:tx-lag`, `:age-ms`).
  4. *An 08-15 assertion retrieved by content, "having been unreachable
     before."* **AMENDED — operator gate.** MAP surprise 1: bare content
     retrieval already works, so the item as written is already true and
     proves nothing. Proposed reading: *retrieved by a **content ×
     attribute composed** query (e.g. the sensor sweep in item 1b) that is
     demonstrably inexpressible-or-unaffordable today, with the before-leg
     evidence recorded.* This strengthens the bar; it does not lower it.
     **Accepted by Joe 2026-08-17** (gate cleared, see top of file); the
     named query is fixed by ARGUE-2 §D-7 (the voice-separated sensor
     sweep).
- **V3 — Spikes run (2026-08-17, read-only against the live 316 MB
  sidecar file):** FTS MATCH over 140,233 rows: 2 ms. MATCH + attribute
  predicate: 2–15 ms. Full 140k-row attribute projection built + indexed:
  185 ms. Conclusion: the candidate layer is not the risk; the design's
  cost model (all real cost in re-check/hydration, already-proven paths)
  is confirmed. Watermark source verified live: `xt/status` keys present
  and serialized on the restart-readiness route.
- **V4 — Risks carried into INSTANTIATE** (each with its check):
  1. 3-table batch write-lock duration vs the on-append! path — measure
     batch wall-clock at page 200 and 1,000 during backfill; SQLITE_BUSY
     count before/after (baseline: 3 errors lifetime).
  2. Backfill duration (~141 keyset pages live) — run off-peak,
     single-flight already enforced; progress observable via stats.
  3. `ev_fts` schema is CREATE-IF-NOT-EXISTS with fixed columns — adding
     nothing to it; new tables are additive, so a **rollback is DROP TABLE
     + delete two meta keys**, no FTS rebuild needed.
  4. Before-leg measurement discipline — must be captured BEFORE the
     attribute tables exist, else the "before" is unmeasurable (the build
     itself would have to be held back; sequence it first in the packet).
- **V5 — Decision log:** no DERIVE revisions arose during VERIFY. The one
  bar amendment is V2.4 (operator-gated). Evidence emission to the
  landscape (per lifecycle conventions) happens at INSTANTIATE with the
  measurement runs.

## INSTANTIATE — NOT ENTERED (stop-gate 2026-08-17)

Per Joe's direction, work stops here; the build moves to Agency/Codex
orchestration. Packet boundaries proposed for the handoff (small, one
behaviour each, discovery already done — this document and the contract ARE
the discovery):

1. **P-before** — capture the before-leg timings (V2.1) live; no code.
2. **P-schema+batch** — `ev_attr`/`ev_tags` DDL + 3-table `index-batch!` +
   basis capture/commit in `catch-up!`; gates: clj-kondo, batch-time
   measurement (V4.1).
3. **P-query** — composed candidate SQL + extended re-check + endpoint
   params + basis/staleness in responses + the ARGUE-2 D-6 stats fields
   (`:projection`, `:residual`, `:recheck-rejections`); gates: clj-kondo,
   oracle agreement (scan oracle on a named query set), stale-read test,
   voice-compose test (IC-1: the D-7 sensor sweep returns operator turns
   only, certified at re-check).
4. **P-backfill+after** — penholder backfill on the live store, off-peak;
   after-leg timings; acceptance-bar run including the amended item 4.

Each packet bells back with a summary + shas; review per the handoff
protocol (read diff, re-run verify step, state what was checked).
