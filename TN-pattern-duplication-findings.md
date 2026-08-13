# TN — `pattern/library` duplication in the substrate: six copies per pattern, and why nobody could see them

**Status: measurement, 2026-08-13.** Written in response to an agent on Zone
reporting that `iiching/exotype-157`, `exotic/dual-track-generation` "and hundreds
of other patterns" appear several times over in the substrate, and asking whether
that is expected behaviour (bitemporal revisions) or a defect. Companion to
`TN-futon1b-boot-incident-2026-08-13.md`, whose defect 1 (`respond!`, commit
`5f77c95`) turns out to be the reason this duplication could not be inspected —
see §4.

**Answer: not revisions.** XTDB 2 revisions share one `xt/id` and version along
valid-time; a census counts them once. These do not share an `xt/id` — each copy is
a physically distinct document with its own primary key. The duplication is real,
it is **inherited from the futon1a era**, and the current futon1b write path would
not reproduce it.

Environments, kept explicit because they disagree:

- **(L)** laptop — `futon1b-server --store-dir migration-store-21 --port 7073`.
- **(Z)** Zone (`zone-joe`, ams) — same server, same store *name*, **different and
  diverged store contents**.
- **(X)** `migration-export-full/graph-snapshot.edn` (2026-07-12, 91,809 docs) —
  the futon1a export that the current store was migrated from. This is the only
  surface on which the duplicate documents can currently be enumerated at all; see
  §4 for why.

---

## 1. The counts

| | (L) laptop | (Z) Zone |
|---|---|---|
| `GET /api/alpha/census?entity-type=pattern/library` | **4655** | **5876** |
| `GET /api/alpha/entities/latest?type=pattern/library&limit=5000` (distinct, sigiled) | **821** | **821** |

For reference: `futon3a/resources/notions/patterns-index.tsv` carries 1354 patterns;
(X) holds 1135 distinct `pattern/library` entity names.

Two things fall out immediately. The stores hold 4–5× more rows than there are
patterns; and (L) and (Z) hold *different amounts* of the same 821-name library, so
the two substrates have diverged by roughly one extra full ingest pass (5876 − 4655
= 1221) without either gaining a pattern.

## 2. The multiplicity is not a smear — it is exactly 1 or exactly 6

Scanning (X) for `:entity/type :pattern/library` gives 4635 documents over 1135
distinct `:entity/name` values, and the histogram is perfectly bimodal:

```
435 names × 1 copy
700 names × 6 copies          435 + (700 × 6) = 4635 ✓
```

Within each 6-group the documents decompose by shape as:

| shape | count | per name |
|---|---|---|
| legacy futon1a doc — `:xt/id` a real `#uuid`, full `lower-label`/`seen-count`/`first-seen`/`updated-at`, `:entity/external-id` = the pattern id | 700 | 1 |
| lean doc — `:xt/id` a **string**, `:entity/external-id` = the pattern id | 650 | ~1 |
| lean doc — `:xt/id` a **string**, `:entity/external-id` = the human **title** | 2850 | ~4 |

`iiching/exotype-157`, verbatim from (X), abridged:

```clj
{:entity/id #uuid "0030a6e9-…" :entity/name "iiching/exotype-157"
 :entity/external-id "iiching/exotype-157" :entity/lower-label "iiching/exotype-157"
 :entity/seen-count 18 :entity/updated-at #inst "2026-01-31T18:01:50.941-00:00"
 :xt/id #uuid "0030a6e9-…"}                                          ; legacy
{:entity/id "0030a6e9-…" … :entity/external-id "iiching/exotype-157"
 :xt/id "0030a6e9-…"}                                                ; same uuid, STRING key
{:entity/id "b58f52d0-…" … :entity/external-id "Exotype 157 (0x9D)" :xt/id "b58f52d0-…"}
{:entity/id "cacd6c0d-…" … :entity/external-id "Exotype 157 (0x9D)" :xt/id "cacd6c0d-…"}
{:entity/id "3671c43d-…" … :entity/external-id "Exotype 157 (0x9D)" :xt/id "3671c43d-…"}
{:entity/id "bf3a3a6b-…" … :entity/external-id "Exotype 157 (0x9D)" :xt/id "bf3a3a6b-…"}
```

Note the second row: same UUID *value* as the legacy doc, but keyed as a string
rather than a `#uuid`. In XTDB 2 those are different primary keys, so the "same"
entity occupies two rows before any real duplication is counted.

`exotic/dual-track-generation` is identical in form, with the title
`"Dual-Track Generated Patterns"` in the external-id slot.

## 3. Which patterns, and therefore when

The duplicated set is exactly the old bulk-generated corpora; the clean set is
exactly what was ingested later.

| duplicated (×6) | | untouched (×1) | |
|---|---|---|---|
| `iiching` | 257 | `math-informal` | 36 |
| `iching` | 64 | `futon-theory` | 32 |
| `p4ng` | 57 | `ukrns` | 21 |
| `vsatlas` | 33 | `storage` | 21 |
| `or` | 21 | `writing-coherence` | 21 |
| `devmap-coherence` | 20 | `f6` | 20 |
| `musn` | 17 | `equity` | 16 |
| `eight-gates` | 16 | `plos-npt-with-small-n` | 16 |
| `liberation` | 16 | `peeragogy` | 16 |
| `agent` | 14 | `math-formalization` | 15 |

**Provenance.** The title-shaped copies match `futon3/scripts/pattern_sync.clj:493`
(`ensure-pattern!`) exactly, which POSTs `/entity` with `:name` = the pattern id and
`:external-id` = the **title**, supplying no `:id`. Replaying it against a futon1
server whose ensure-by-name dedupe was not holding minted a fresh UUID per pattern
per run. futon3's `scripts/` contains several such writers with different
external-id conventions (`ingest_patterns.sh` posts `{name, type, props}` with no
external-id at all), which accounts for the two lean shapes.

**This is frozen history, not an ongoing process.**

- futon1b's migration preserved `xt/id` verbatim, so it inherited the duplicates
  rather than creating them.
- futon1b's current write path *does* dedupe: `ensure-entity-id`
  (`futon1b_graph.clj:221-231`) resolves requested `:id` → existing by
  `:entity/name` (preferring matching type, then smallest id) → fresh UUID. A
  replay today would land on the existing row.
- (L) has gained ~20 `pattern/library` rows since the 2026-07-12 migration
  (4635 → 4655). Zone runs no pattern-ingest cron or timer (`crontab -l`; two
  systemd user timers, both APM).

## 4. Why this was invisible: `GET /api/alpha/entities` 500s on any timestamped type

`GET /api/alpha/entities?type=pattern/library` returns **500** at every limit,
including `limit=1`:

```
{:error {:reason :exception, :message "No reader function for tag xt/zdt"}}
```

**This is the `respond!` defect already diagnosed and fixed earlier the same day**
— commit `5f77c95` "respond!: stop round-tripping EDN responses through the reader"
(2026-08-13 11:25 UTC), written up as defect 1 of
`TN-futon1b-boot-incident-2026-08-13.md`. `entities-route`
(`futon1b_server.clj:506-513`) hands `respond!` an already-`pr-str`'d string and
`respond!` read it back through `edn/read-string`; XTDB 2 returns the legacy
`:entity/updated-at` / `:entity/first-seen` values as `ZonedDateTime`, which
`pr-str` writes as `#xt/zdt "…"`, for which `clojure.edn/read-string` has no reader.
The boot-incident TN reports the sibling tags (`#xt/instant`, `#object[…]`) on the
same mechanism; `#xt/zdt` on the raw entity route is the same defect seen from the
pattern side.

Three things this note adds on top of that write-up:

1. **The measurements here were taken against an unrestarted server.** The (L) JVM
   has been up since **2026-08-10 10:32**, three days before the fix commit, so it
   still carries the old `respond!`. Any store whose server predates `5f77c95`
   still shows these 500s. Confirmed on (L):

   | type | result |
   |---|---|
   | `pattern/library` | 500 `xt/zdt` |
   | `pattern/component` | 500 `xt/zdt` |
   | `mission/doc` | 200 |
   | `agent/registered` | 200 (empty) |

2. **The residual matters for exactly this job.** `5f77c95` writes a string body
   straight out on the EDN path, but the JSON path still parses in order to encode
   — as its docstring says. So even on a fixed server,
   `Accept: application/json` on `/api/alpha/entities?type=pattern/library` will
   still 500. Enumerate the duplicates over **EDN**, not JSON.

3. **`entities/latest` survives only by accident.** `public-entity`
   (`futon1b_graph.clj:135-144`) projects to `{:id :name :type :external-id :source
   (:props) (:media/sha256)}`, dropping the timestamp fields before they reach
   `respond!`. That is why the deduped read kept working throughout and the raw
   read did not — and therefore why the duplication was only ever visible as a
   census number that disagreed with a listing.

## 5. What the duplication actually costs

The canonical read path is **unaffected**. `entities-latest`
(`futon1b_graph.clj:254-277`) groups by `:entity/name` and takes the
lexicographically smallest `:entity/id`, so pattern-library consumers see a clean
821. What is degraded:

1. **Counts lie.** Census reports 4655 (L) / 5876 (Z) for ~1135 real patterns. Any
   dashboard, ratio, or coverage denominator built on the census is wrong by 4–5×.
2. **Edges scatter across copies.** (X) holds 1780 `pattern/has-sigil` relations
   over **1506 distinct `:relation/src` ids** for 821 pattern names — sigils are
   attached to *different copies* of the same pattern. `resolve-rel-endpoint`
   (`futon1b_graph.clj:304-322`) resolves a name to the smallest-id copy, so which
   copy an edge lands on depends on write order and on whether the caller passed a
   name or an id.
3. **Store divergence is silent.** (L) and (Z) disagree by 1221 rows on the same
   library and nothing surfaces that.

## 6. Suggested sequencing (not yet done)

Two separate pieces of work, in this order:

1. **Restart the servers onto `5f77c95` or later**, and enumerate over the EDN path
   (§4). No data change; it is simply the precondition for step 2, since on a
   pre-fix server there is no way to list the duplicate rows at all. If the
   enumeration is wanted over JSON, the residual call-site fix (~30 routes passing
   the map instead of `pr-str`) has to land first.
2. **Merge-and-retract pass.** For each duplicated name: elect the legacy
   `#uuid`-keyed document as canonical, repoint `pattern/has-sigil` (and any other
   relation whose `src`/`dst` is a non-canonical copy) onto it, then retract the
   extras via the gated `POST /api/alpha/documents/retract`. Must be scoped and run
   per store — (L) and (Z) have diverged, so this needs its own discovery pass on
   each rather than a single fix replayed.

## 7. Repro

```bash
# counts, both stores
curl -s "http://127.0.0.1:7073/api/alpha/census?entity-type=pattern/library"
curl -s -H 'Accept: application/json' \
  "http://127.0.0.1:7073/api/alpha/entities/latest?type=pattern/library&limit=5000" \
  | python3 -c 'import sys,json; print(len(json.load(sys.stdin)["entities"]))'

# the xt/zdt failure (pre-5f77c95 server; and post-fix too if you ask for JSON)
curl -s "http://127.0.0.1:7073/api/alpha/entities?type=pattern/library&limit=1"

# the multiplicity histogram: brace-depth scan over migration-export-full/graph-snapshot.edn,
# select docs containing ":entity/type :pattern/library", group by :entity/name,
# then histogram the group sizes and cross-tabulate by
# (has :entity/lower-label?, :xt/id is #uuid?, :entity/external-id == :entity/name?).
```
