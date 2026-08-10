# E-SO-to-XT — the StackExchange-family benchmark, from original contents

**Opened:** 2026-08-10 (Fable session, Joe driving)
**Parent:** `M-xtdb-22x-benchmarking.md` — this excursion specifies the
SE rung of that mission's ladder, which is probably the **fastest
large-size benchmark we can do something interesting with**.
**Principle (Joe, 2026-08-10):** *what we advertise is a repro with the
original contents.* No futon tooling in the critical path — regenerating an
artifact may require our pipeline; using the benchmark must not. The SE rung
satisfies this maximally: raw public dump + one ingest script + queries, no
LLM anywhere, therefore **byte-level reproducible** (a strictly stronger
class than the invariant-level reproducibility that is the honest ceiling
for the arXiv mining artifacts — see `futon6/REPRODUCING.md`).

## 1. Why SE, and why the duplicate workload

SE data is not bitemporal by *storage* but it is fully **time-delineated**:
every post, every edit, every closure event carries a timestamp, and
`PostHistory.xml` contains the full text of every revision. That makes one
question askable that no static-snapshot benchmark can pose honestly
(Joe's formulation): **"what are search terms that would have found this
duplicate before the question was posted?"**

For every question Q closed as a duplicate of P, a human moderator has
certified that P answered Q's information need. The dumps therefore carry
**free human relevance judgments at scale**. But evaluating "would search
have found P" against today's corpus measures leakage, through at least
four channels:

1. Q's title gets `[duplicate]` edited into it at closure;
2. P is often edited *after* Q's closure, sometimes absorbing Q's phrasing;
3. P's best answer may postdate Q entirely;
4. mass retagging rewrites the tag vocabulary years later.

Existing duplicate-detection benchmarks (CQADupStack and kin) hand-scrub a
static snapshot. A bitemporal store closes every channel **by query
modifier**: index as of Q's `CreationDate`. The as-of formulation is not
temporal flavour — it is the validity condition of the evaluation. As far
as we know nobody has published the bitemporal formulation; that is a
citable novelty for the #5637 thread, not just a workload.

It is also the sharpest forcing function available for #5637's design
space: a Lucene sidecar has no valid-time — as-of text search means either
per-snapshot indexes (combinatorial) or search-then-post-filter (wasteful,
and this benchmark would price exactly how wasteful — see W4). A text index
native to the store can respect temporality structurally. The workload
converts "should text indexing live inside XTDB" from a preference into a
benchmark delta.

## 2. The object (availability verified 2026-08-10)

`archive.org/details/stackexchange`, CC BY-SA 4.0, per-site 7z archives.
Most recent dump visible at check time: 2024-04-02 — **confirm the latest
dump date at build time and pin it; the dump date is the corpus id** (the
H35 lesson transposed: a directory is not a corpus, a dated archive item
is).

| site | archive size | role |
|---|---|---|
| mathoverflow.net | 487 MB | **first rung** — research-level math, thematic sibling of the arXiv CT corpus, MathJax-dense (analyzer stress), tractable everywhere |
| physics.stackexchange.com | 696 MB | second — prior local familiarity (futon6 processed 114K QA pairs; that tooling stays OFF the advertised path) |
| math.stackexchange.com | 3.4 GB | scale rung — dupe graph in the tens of thousands |
| stackoverflow.com | 21.4 GB (Posts alone) | load rung only, if ever |

**Attribution is satisfied by construction:** CC BY-SA requires linking the
source posts and authors; benchmark documents carry their post ids, URLs
and author ids as *fields*, so the ingested store is its own attribution
manifest.

## 3. The ingest mapping (this is the real work)

Per site, three files matter for v1: `Posts.xml`, `PostHistory.xml`,
`PostLinks.xml`. Everything else (Users, Votes, Comments, Tags) is later.

- **`se_posts`** — one document per post, **versioned**: `PostHistory` rows
  are per-field edit *events* (`PostHistoryTypeId` 1–3 initial
  title/body/tags, 4–6 edits, 7–9 rollbacks, 10/11 close/reopen). The
  ingest must **fold** these into full document states, one version per
  revision, written with **valid-time backdated to the revision's
  `CreationDate`**. This fold is the one genuinely fiddly component:
  rollbacks restore prior states; close/reopen events change status without
  changing text. It is deterministic and unit-testable (fold a post's
  events, compare the final state against the post's row in `Posts.xml` —
  a conservation check in the replay-harness spirit, runnable over every
  post in the dump).
- **`se_postlinks`** — edges with `CreationDate`; `LinkTypeId=3` is the
  duplicate relation (ground truth), `1` is "linked" (useful negative-ish
  pool).
- Streaming SAX parsing regardless of site (mandatory at SO scale,
  harmless at MO scale). Deps: XTDB + an XML parser. Nothing else.

Tooling home: `futon1b/bench22x/` beside `ingest_graphs.clj` /
`query_probe.clj`, same deps.edn pattern, same `--store <dir>` contract.

## 4. Workloads

- **W1 — as-of duplicate retrieval (the advertised centrepiece).** For each
  dupe pair (Q→P): query with Q's *ask-time* title+body terms against the
  corpus **as of Q's `CreationDate`**; measure P's rank. Recall@k / MRR
  over the full dupe set. Ground truth derived **in-store** from
  `se_postlinks` — reproduced by query, not shipped as a labels file
  anyone has to trust.
- **W2 — abductive term mining (extension, ours to play with).** Find the
  minimal ask-time query that ranks P top-k. Output = the vocabulary gap
  between duplicate pairs, itself a scientific object (why askers fail to
  find their duplicates). Deterministic if the search strategy is pinned,
  but expensive; not in the advertised core.
- **W3 — the ladder at scale.** The existing rungs (point lookup, scalar
  filter, text LIKE scan, graph join, id-set membership) re-run per site
  size. Output is **shape-vs-scale curves, never point values**
  (fixture-inversion discipline, mission doc §Facts: small-scale points
  can invert at live scale).
- **W4 — the price of temporality.** As-of text scan vs present-time text
  scan, same terms, same site. The delta is the measured cost of doing
  temporal text search *without* native index support — the number that
  frames #5637's data-model discussion.

## 5. Work list

| id | what | effort guess |
|---|---|---|
| D1 | dump fetch + checksum + pin (site, dump date) | hours |
| D2 | streaming ingest with the PostHistory fold + fold conservation test | 1–2 days; the core |
| D3 | dupe ground-truth extraction queries (in-store) | hours |
| D4 | W1/W3/W4 harness + metrics | a day |
| D5 | REPRODUCING-SE (byte-level claims; two downloads, one command) | hours |

MO end-to-end is plausibly a weekend of wall-clock, most of it download
and unattended ingest.

## 6. Priors and traps (house ledgers)

- **`DEFECT-bitemporal-as-of-two-routes.md`**: two futon1b *application*
  faults masqueraded as "the store ignores as-of" and were nearly reported
  upstream as an XTDB bug. The benchmark therefore queries the node
  **directly** (bench22x pattern) — no app layer in the path, which the
  original-contents principle demands anyway. Before trusting any as-of
  result, verify the route honors as-of with a known-answer probe.
- **Closure-event encoding drifted over SE's history** (the duplicate
  close reason is recorded differently pre/post ~2013). Verify the dupe
  extraction on both eras or scope the ground truth to post-2013 closures
  and say so.
- **Body HTML policy must be pinned**: dumps store rendered HTML. Strip to
  text, keep raw, or carry both as separate fields — the choice changes
  what the text workload measures. (MathJax `$...$` survives in the text
  either way — the analyzer stress case comes free.)
- **Provisioning per James's steer**: `-Xmx` ≈ half intended footprint;
  full-allocated-memory operation is expected, not a leak
  (RSS ≈ 2×Xmx).
- **2.2.x before results**: bench22x pins 2.2.0-rc0 today; the mission
  says migrate and re-verify before citing numbers. Build against rc0 if
  it unblocks D2, but W1–W4 numbers quoted anywhere outside this repo
  should come from 2.2.x.

## 7. Relation to the other rungs

The arXiv side supplies what SE cannot: typed argument structure
(hybrid text+graph queries over the mined fixture; CC0-selected corpus =
`futon6/holes/math-ct-cc0.ids.txt`, 78 papers, redistributable with zero
obligations) and, eventually, whole-field scale with version histories via
the manifest re-harvest. SE supplies what arXiv cannot: **real revision
histories today**, human relevance judgments at scale, and a workload
where bitemporality is the validity condition rather than a feature. The
two rungs together are the #5637 story: one proves the harness and the
hybrid queries, the other proves the reason text indexing belongs *inside*
a bitemporal store.
