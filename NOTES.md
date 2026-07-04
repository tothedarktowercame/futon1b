# E-futon1b-foothold — S2/S3 notes (2026-07-04)

Seed: 100 synthetic hyperedges (hx/ schema, endpoint pool e-1..e-30, 5
types) + 30 evidence docs, constructed in `s2_s3.clj`; the seed data is the
oracle — expected counts computed in plain Clojure from the same
structures, asserted against XTQL results. XTDB 2.0.0 in-process node via
`clojure -M:node` (Arrow `--add-opens` in the alias).

Provenance: S1 + the correct unnest idiom + this file by claude-16; the
**silent-no-filter discovery** below by zai-8 (dispatch 3), whose jar
archaeology (XtqlQuery nested classes, unnest as parsed binding) pointed
toward the answer before its round budget ran out.

## S3(a) — endpoint membership (the core question)

Datalog (futon1a, ~34 sites):

    {:find [(pull e [*])] :in [eid] :where [[e :hx/endpoints eid]]}

XTQL:

    (-> (from :hyperedges [xt/id hx/type hx/endpoints])
        (unnest {:ep hx/endpoints})
        (where (= ep "e-7"))
        (return xt/id))

Result: expected 21, got 21 — **PASS**.

**Verdict: reads cleanly, with one loud caveat.** The pipeline form is
legible — arguably more explicit than Datalog's implicit set-membership
unification. BUT: the naive translation attempt (`unnest` guessed as an
inline clause) **returned all rows unfiltered rather than erroring** —
a silent wrong answer, found by zai-8. Migration consequence: every one of
the ~34 membership sites needs a correctness assertion against known data,
not just a compiles-and-runs check. Budget the port accordingly.

## S3(b) — by-type with limit

Datalog: `[?h :hx/type :code/calls]` + `:limit`. XTQL:

    (-> (from :hyperedges [xt/id hx/type])
        (where (= hx/type :code/calls))
        (limit 10))

Expected-total 20, got 20; limited 10. **PASS. Reads cleanly, no caveats.**

## S3(c) — evidence by tag

Same unnest idiom over `:evidence/tags` (keyword elements). Expected 12,
got 12. **PASS** — same verdict and caveat as (a).

## Remaining

S4: re-implement `memory-search`'s read fn against this node, returning
the identical M-custom-harness §12.3 envelope — the D-11.i seam-swap
proof. Then the excursion closes and its verdicts feed the A2 trigger.

## S4 — seam swap (2026-07-04)

`seam_swap.clj`: `memory-search-1b` re-implements the futon3c memory-search
read fn against the XTDB 2 node. Assertions (seed-as-oracle): tag ANY-of
count 12/12 PASS; envelope shape-identical (`:frame :query :items`, item
fields `:id :at :author :type`) PASS; limit clamps (500→≤100, none→20)
PASS.

**Verdict: the D-11.i seam swaps.** Nothing above the read fn changes.

**Second silent-hazard datum (zai-8's build, claude-16's fix):** the tag
ANY-of was first written with Clojure idioms (`some` + set-as-predicate)
inside XTQL `where` — XTQL is its own expression language, and this threw
"set not applicable to types set" at runtime (loud, at least, unlike the
unnest case). Correct form: `unnest` + `(where (or (= tag :a) (= tag :b)))`.
Port lesson reinforced: XTQL looks like Clojure but is not Clojure — every
translated predicate needs a runtime test.

Provenance: fn structure + assertions by zai-8 (dispatch 4); tag-predicate
fix + this section by claude-16 (review). zai-8's final run was not
re-executed after its last edit — the reviewer's run caught the failure.

## Hazard catalog (XTQL translation traps)

Every known silent-wrong-result or late-failure mode encountered during
the foothold and P1. Anyone touching a translation site should read this
first — these are the trap shapes the assertion discipline exists to catch.

**H1 — naive unnest formulation (silent wrong results).**
A wrong `unnest` clause returns all rows unfiltered instead of erroring.
Found: foothold S3(a), zai-8 dispatch 3. Correct form:
`(-> (from …) (unnest {:ep hx/endpoints}) (where (= ep X)) (return xt/id))`.
The pipeline order matters: `unnest` before `where` on the unnested var.

**H2 — Clojure-isms inside XTQL where (runtime error).**
Using Clojure functions (`some`, set-as-predicate, `empty?`) inside XTQL
`where` fails at runtime — XTQL is its own expression language, not
Clojure. Found: foothold S4 (`some` + set-predicate); P1 A5 (`empty?`).
Fix: express predicates in XTQL only (`or` of `=` for ANY-of), or move
the check to Clojure-side after the query.

**H3 — `from` silently ignores extra positional forms (silent wrong
results).** `(from :hyperedges [xt/id hx/type] (where (= hx/type :code/calls))`
returns ALL rows unfiltered — the extra `(where …)` form after the column
vector is silently dropped, no error. Found: P1 A2 (zai-9). Fix: `where`
must be a threaded pipeline stage: `(-> (from :hyperedges [xt/id hx/type])
(where (= hx/type :code/calls)))`. This is arguably nastier than H1
because the form looks structurally plausible and the failure is invisible
without an oracle. The manifest count assertion caught it.

**H4 — XTQL `where` cannot navigate nested maps/structs (runtime error).**
XTQL stores nested maps (e.g. `:hx/props {:repo "x" :source-file "y"}`)
as Arrow struct types. Inside `where`, `get`, `get-in`, and `.get` all
fail with "not applicable to types struct." The `{parent [child]}`
nested bind spec also fails on qualified keywords ("Attribute in bind
spec must be keyword"). Found: P2b (zai-7, dispatched by zai-9). Fix:
denormalize — flatten nested map keys into top-level `:prop/<key>`
columns at ingest time. This pushes the filter into the query layer,
which futon1a itself never achieved (it does props filtering in Clojure
via `prop-matches?` + `get-in` on materialized entities, never in
Datalog). The denormalization is strictly better for query-layer
filtering and is a port-design decision (see mission doc D-5).

**General lesson (foothold + P1 + P2):** XTQL looks like Clojure but is not
Clojure, and it looks like Datalog but is not Datalog. Every translated
site needs a known-data correctness assertion, not a compiles-and-runs
check. The seed-as-oracle / manifest-as-oracle pattern is the discipline.
