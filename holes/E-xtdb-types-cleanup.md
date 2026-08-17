# E-xtdb-types-cleanup

**Created 2026-08-17** at Joe's request: *"I think we should make a separate
E-xtdb-types-cleanup note; it is worth doing the cleanup and logging our work in
a note, even if short."*

**Outcome: the cleanup was NOT performed, because there is no code path that can
perform it.** The inventory and the safety check are done and recorded below, so
the work is not lost; what remains is a decision about which of two paths to
take. Prevention is arguably the more important half.

---

## 1. What was found

Discovered while diffing the type catalogues of two co-located stores on Zone —
the Zone site (`:7073`) and the frozen Dionysus store (`:7083`).

| | Zone site | Dionysus |
|---|---|---|
| types | 250 | 222 |
| shared | 222 | 222 |
| site-only | **28** | 0 |

**(a) 28 probe types, Zone site only.** `:h1`, `:h1/probe`, `:h1/probe2`,
`:h1/repro`, `:idempotency`, `:idempotency/probe`, `:probe`, `:probe/batchver`,
`:probe/c`, `:probe/clean`, `:probe/clean-newtype-1`, `:probe/n`, … — residue of
the H1/H3 hypothesis probes recorded in
`futon3c/holes/excursions/E-apm-A3-ingest-efficiency.md` (2026-08-13/14; "brand-new
type 3,000 ms vs existing 2,995 ms" *was* a type-registration probe).

**(b) 6 names registered under BOTH kinds** — entity and relation:
`:arxana`, `:diagram`, `:learning-loop`, `:model`, `:pattern`, `:pattern/*`.

**(c) Glob patterns registered as types**, on **both** sites (so pre-dating
2026-08-14): `:pattern/*`, `:devmap/*`, `:me/*`, `:prototype/*`. A literal `*` in
a type id is a wildcard passed where a concrete type was expected.

## 2. Root cause

`register-types!` (`futon1b_graph.clj:60`) validates exactly `(keyword? type-id)`:

```clojure
(doseq [{:keys [kind type-id]} kind-type-pairs
        :when (keyword? type-id)
        td [(type-doc {:type-id type-id :kind kind})
            (when-let [p (infer-parent type-id)] (type-doc {:type-id p :kind kind}))]
        :when (and td (not (fxt/present? node :type-catalog (:xt/id td))))]
  (put-verified! node :type-catalog td))
```

Called from the entity write path (`:280`, `:326`) and relation write path
(`:433`, `:504`). Consequences:

- **no vocabulary control** — any keyword becomes a type, so `:pattern/*` and
  `:probe/n` are as legitimate as `:capability`;
- **no notion of provisional** — a diagnostic write is indistinguishable from a
  real one, permanently;
- the parent hierarchy is **inferred from the keyword namespace** by
  `infer-parent`, not authored, so a junk type also mints a junk parent.

This is the same shape as the `#uuid`-string identity defect in
`E-apm-A3-ingest-efficiency.md`: **a write path that validates shape but not
identity, so malformed values round-trip silently and become
indistinguishable from real data.** Neither fails loudly.

## 3. Safety check — DONE

All candidate junk types have **zero users** (Zone site, `/api/alpha/census`):

```
:probe 0 · :h1 0 · :idempotency 0 · :probe/clean 0
:pattern/* 0 · :devmap/* 0 · :me/* 0 · :prototype/* 0
```

Census was validated against known-populated types first, so the zeros are
trustworthy rather than an artifact of a broken instrument:
`:capability` 35 · `:demo` 27 · `:commit` 39 · `:concept` 184 · `:claim` 1.
(`:pattern` also returns 0 — the pattern corpus lives under `pattern/library`
and `pattern/clause`, consistent with the 2026-08-14 re-ingest.)

So deletion would strand nothing.

## 4. Why the cleanup could not be performed

**There is no code path in futon1b that removes a type-catalog document.**

| path | what it does | can it delete a type? |
|---|---|---|
| `POST /documents/retract` | `retract-documents!`, atomic + read-back verified | **No** — `retractable-tables` is `#{:entities :hyperedges :relations}` |
| `POST /types/merge` | `types-mutate!` sets `:type/aliases` on the doc | No — aliasing, not consolidation |
| `POST /types/parent` | sets `:type/parent` | No |
| `register-types!` | `put-verified!` only | No |

The catalogue is **append-only by construction**.

Direct store access was considered and rejected. pgwire is reachable
(`127.0.0.1:35127` on Zone, loopback) but writing there bypasses
`with-memory-projection-mutation` (the mutation/projection serialization lock)
and the post-commit read-back that `retract-documents!` exists to provide —
whose docstring notes XTDB's *"silent-drop failure mode"* is precisely what the
read-back guards against. There is also a recorded precedent for refusing
partial cleanups of this kind: *"codex-3 correctly refused to implement that
partial cleanup"* (`retractable-tables` docstring; see
`zone.hyperreal.enterprises/2026-08-13-relations-retraction.html`).

## 5. What doing it would take — two halves

**Half 1 — removal (the smaller half).** Add `:type-catalog` to
`retractable-tables`. The docstring for the `:relations` addition (2026-08-13) is
the model to follow: the deletion body is already table-generic
(`[:delete-docs table id]`) and the read-back check is generic, so the change is
small. The question it must answer, which `:relations` had to answer too: **what
happens if a type doc is retracted while entities of that type exist?** For the
28 probe types the count is 0, so the first use is safe — but the guard belongs
in the code, not in the caller's diligence.

**Half 2 — prevention (the load-bearing half).** Removing 28 rows without
changing `register-types!` means they return with the next probe. Options, not
decided here:

- **reject** ids containing `*` outright — a glob is never a type;
- **reserve a provisional namespace** (e.g. `:probe/…`, `:h1/…`) that registers
  but is excluded from `/types` by default and is retractable;
- **an allowed-set or a declared schema**, which is the largest change and cuts
  against the current accretive design — noting that accretion is *why* the
  vocabulary is rich (85 relation types arrived from futon5a's AIF work, the
  pattern library, the Interest Network, Arxana, APM without anyone gatekeeping).

That trade-off — accretion buys richness and costs hygiene — is the real
decision, and it is Joe's.

## 6. Not done

No writes were made to either store. The 28 probe types, 6 kind-collisions and
4 glob types are all still present as of 2026-08-17.

## Provenance

Found 2026-08-17 while surveying the evidence landscape for
`futon0/holes/M-what-is-it-who-is-it-for.md` §2 (MAP). Facts established by
reading `futon1b_graph.clj` (`register-types!` `:60`, `retractable-tables`
`:146`, `retract-documents!` `:176`, `types-mutate!` `:90`) and
`futon1b_server.clj` (`types-route` `:634`), and by calling `/api/alpha/types`
and `/api/alpha/census` on both `:7073` and `:7083`.
