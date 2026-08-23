# Sigil rejoin — items needing a human (2026-08-23)

> **REVISED 2026-08-23 (Joe + a Dionysus-local agent's analysis). Do NOT apply the original instruction
> "decide the slug or retire the sigil" row by row — applied naively it retires 46 valid sigils.**
>
> Resolution, by category:
>
> | category | n | resolution |
> |---|---|---|
> | Ambiguous `or/` vs `p4ng/` duplicates | 15 | **`or/`** is the library these belong to (Joe). |
> | Unmatched: title truncated at ` -> ` or parenthetical abbreviated (`(I2)` for `(Invariant 2)`, also I0/I3/I4/A5) | 15 | auto-join after normalising both sides: truncate at ` -> `, drop the parenthetical. Matches the `@title` lines in `futon3/library/**/*.flexiarg`. |
> | Unmatched: namespace pick (`futon-theory/` vs `storage/`) | 2 | human picks. |
> | Unmatched: **devmap prototypes** (P0–P12 in `futon4/docs/VSATARCS.md`; ids `f<N>/p<M>` per `futon3/resources/sigils/rationale-examples.edn`) | 46 | **re-route, not retire**: relation type `:prototype/has-sigil`, src = the `devmap/prototype` entity. Zone has 73 `devmap/prototype` entities (names `f2/p7` …) still keyed by `#uuid` literals — that population was never re-ingested to slugs, so its own `prototype/has-sigil` rows may still join; check before writing. Devmaps are a recognised use of the flexiarg format, not non-patterns (Joe). |
> | True orphan: "Meme Layer (ANN Search)" | 1 | retire (zero hits anywhere in the corpus). |
>
> The consumable output is a sibling file, `docs/sigil-rejoin-resolution-2026-08-23.edn`, in the shape the
> apply script reads (see "After adjudication" at the bottom). The tables below are the raw input, kept for
> audit.

Source: `docs/sigil-rejoin-2026-08-23.edn` (codex-10, `f56ecd9`). The 326 exact-title matches were written
by `scripts/sigil-rejoin-apply.py` (`4f5d192`). Source-file hits are `grep -F` of the title over
futon3b/futon3c/futon3 library, futon4/docs, futon5.

## Ambiguous — duplicate pattern titles (15)

| sigil | title | candidate slugs |
|---|---|---|
| `sigil|ante|也` | Overlay Bridges | `or/overlay-bridges`, `p4ng/overlay-bridges` |
| `sigil|ante|了` | Readiness Windows | `or/readiness-windows`, `p4ng/readiness-windows` |
| `sigil|ante|支` | Overlay Bridges | `or/overlay-bridges`, `p4ng/overlay-bridges` |
| `sigil|kasi|田` | Identifier Garden | `or/identifier-garden`, `p4ng/identifier-garden` |
| `sigil|li|工` | License Laddering | `or/license-laddering`, `p4ng/license-laddering` |
| `sigil|lukin|介` | Attribution-Forward Review | `or/attribution-forward-review`, `p4ng/attribution-forward` |
| `sigil|ma|只` | DREAM Audit (Per Release) | `or/dream-audit-per-release`, `p4ng/dream-audit` |
| `sigil|ma|旦` | Readiness Windows | `or/readiness-windows`, `p4ng/readiness-windows` |
| `sigil|ni|友` | Maintainer Care | `or/maintainer-care`, `p4ng/maintainer-care` |
| `sigil|ni|史` | Retirement Shelf | `or/retirement-shelf`, `p4ng/retirement-shelf` |
| `sigil|palisa|工` | Quality-by-Design Crowd | `or/quality-by-design-crowd`, `p4ng/quality-by-design` |
| `sigil|pali|亏` | Peripheral-to-Core Pathways | `or/peripheral-to-core-pathways`, `p4ng/peripheral-to-core` |
| `sigil|pata|人` | Citizen Co-Learning Track | `or/citizen-co-learning-track`, `p4ng/citizen-colearning` |
| `sigil|seli|石` | Friction-as-Feature | `or/friction-as-feature`, `p4ng/friction-as-feature` |
| `sigil|weka|节` | Tune Audits (Quarterly) | `or/tune-audits-quarterly`, `p4ng/tune-audits` |

## Unmatched — sigil title not in the library (64)

| sigil | sigil title | nearest slugs (difflib, not a join) | source-file hits |
|---|---|---|---|
| `sigil|alasa|正` | Pilot Storage (fulab, MMCA) | `cascades/on-the-fly-cascade`, `hdm/deep-storage-to-active-graph`, `storage/deterministic-substrate` | `futon4/docs/VSATARCS.md` |
| `sigil|ala|乃` | HDM - Deep Storage | `hdm/bootstrap-kernel`, `hdm/deep-storage-to-active-graph`, `stack-coherence/readme-devmap-sync` | `futon5/resources/exotype-xenotype-lift.edn` |
| `sigil|ala|久` | Graph Storage Layer | `exotic/tri-store-lineage`, `hdm/deep-storage-to-active-graph`, `vsatlas/askew-layer` | `futon4/docs/VSATARCS.md` |
| `sigil|ala|也` | Literary Interface | `futon-theory/interface-loop`, `portal/first-class-query-interface`, `storage/canonical-interface` | `futon4/docs/VSATARCS.md`<br>`futon5/resources/exotype-xenotype-lift.edn` |
| `sigil|ala|叉` | StackExchange Import | `agent/state-is-hypothesis`, `blues/quick-change`, `stack-coherence/evidence-ledger` | `futon4/docs/VSATARCS.md`<br>`futon5/resources/exotype-xenotype-lift.edn` |
| `sigil|ala|双` | Design Pattern Templates | `or3/design-around-the-red-line`, `p4ng/pattern-the-play`, `p4ng/self-patterning-mandate` | `futon4/docs/VSATARCS.md` |
| `sigil|ala|无` | Agent Protocol | `fulab/clock-out`, `fulab/pattern-propose`, `futon-theory/event-protocol` | `futon4/docs/VSATARCS.md`<br>`futon5/resources/exotype-xenotype-lift.edn` |
| `sigil|ale|及` | HTTP API (Canonical Interface) | `portal/first-class-query-interface`, `storage/canonical-interface`, `vsatlas/peeragogical-infrastructures` | `futon4/docs/VSATARCS.md` |
| `sigil|ale|无` | All-or-Nothing Integrity (I2) | `futon-theory/all-or-nothing`, `storage/all-or-nothing-startup`, `storage/startup-integrity-gate` | — |
| `sigil|awen|功` | Local Gain Persistence | `futon-theory/local-gain-persistence`, `paramitas/persistence`, `storage/canonical-interface` | — |
| `sigil|e|小` | Reusable Mathematical Models | `music/authentic-cadence`, `t3/implementation-modes`, `vsatlas/festival-model` | `futon4/docs/VSATARCS.md` |
| `sigil|ilo|下` | Proof-Theoretic Digest/Compression | `f6/proof-as-social-process`, `fulab/proof-commit`, `vsatlatarium/vapor-as-affect` | `futon4/docs/VSATARCS.md` |
| `sigil|ilo|平` | Stabilization & Hub Demonstration | `agent/escalation-cost-vs-risk`, `math-formalization/notation-semantics-traps`, `sidecar/validation-enforcement-gate` | `futon4/docs/VSATARCS.md` |
| `sigil|ilo|才` | F0-F2 Interface (Agent Perception) | `math-formalization/tactic-algebra-interference`, `math-informal/find-the-right-abstraction`, `p4ng/candidate-move-generation` | `futon4/docs/VSATARCS.md` |
| `sigil|insa|习` | Interactive Tutorials | `futon-theory/interface-loop`, `t3/intervention-triad`, `vsatlas/interpretive-views` | `futon4/docs/VSATARCS.md` |
| `sigil|ken|入` | Open-World Ingest | `mojo/up`, `or/overlay-bridges`, `p4ng/overlay-bridges`, `storage/open-world-continuity` | `futon4/docs/VSATARCS.md`<br>`futon5/resources/exotype-xenotype-lift.edn` |
| `sigil|kiwen|本` | Xenotype CT Attachment | `futon-theory/xenotype-portability`, `iiching/exotype-174`, `iiching/exotype-206` | `futon4/docs/VSATARCS.md` |
| `sigil|kulupu|人` | Coordination Protocol | `agent/coordination-has-cost`, `coordination/cross-validation-protocol`, `war-machine/coordination-bottleneck` | — |
| `sigil|lili|少` | Minimum Viable Events (A5) | `blues/dominant-sevenths`, `futon-theory/minimum-viable-events`, `vsatlas/minimal-linking-pilot` | — |
| `sigil|linja|双` | Mission Dependency | `futon-theory/mission-dependency`, `vsatelier/decision-provenance`, `vsatelier/projection-independence` | — |
| `sigil|lipu|业` | Mission Queue & Supervisor | `fulab/session-resume`, `futon-theory/mission-lifecycle`, `popiii/discussion` | `futon4/docs/VSATARCS.md` |
| `sigil|lipu|义` | NLP Interface | `futon-theory/interface-loop`, `portal/first-class-query-interface`, `storage/canonical-interface` | `futon4/docs/VSATARCS.md`<br>`futon5/resources/exotype-xenotype-lift.edn` |
| `sigil|lipu|忆` | Sidecar Audit Trail | `fulab/changelog-trail`, `or/dream-audit-per-release`, `p4ng/dream-audit`, `war-machine/ideal-actual-gap` | `futon4/docs/VSATARCS.md`<br>`futon5/src/nonstarter/demo.clj`<br>`futon5/docs/nonstarter-adapter-contract.md` |
| `sigil|lipu|文` | F0-F4 Interface (Hypertext Navigation) | `musn/declare-scope`, `vsat/embedded-annotation`, `vsatlas/interpretation-offering` | — |
| `sigil|li|另` | Transfer Milestone | `iching/hexagram-63-jiji`, `math-informal/try-a-simpler-case`, `t3/transformation-space` | `futon4/docs/VSATARCS.md`<br>`futon5/resources/exotype-xenotype-lift.edn` |
| `sigil|lon|乡` | Predictive Coding Microcycle | `forward-model/price-your-own-moves`, `or/citizen-co-learning-track`, `p4ng/citizen-colearning`, `xed-roads/live-recording-bridge` | `futon4/docs/VSATARCS.md`<br>`futon5/resources/exotype-xenotype-lift.edn` |
| `sigil|lon|亏` | Core Infrastructure | `transition/persistence-conditions`, `vsatlas/peeragogical-infrastructures`, `vsatlas/pluriversal-infrastructure` | `futon4/docs/VSATARCS.md` |
| `sigil|lukin|习` | Portal Query Layer | `vsatlas/askew-layer`, `vsatlas/offer-ladder`, `war-machine/spatial-over-tabular` | `futon4/docs/VSATARCS.md`<br>`futon5/src/nonstarter/demo.clj` |
| `sigil|lukin|入` | F0-F7 Interface (Transparency/Simulation) | `fulab/pattern-dep`, `hdm/opacity-to-transparency`, `peripherals/surface-earns-inhabitation` | — |
| `sigil|lukin|示` | Rapid Debugging (I4) | `exotic/hybrid-execution-semantics`, `futon-theory/rapid-debugging`, `storage/rapid-debugging` | — |
| `sigil|lukin|见` | Query Workflows |  | `futon4/docs/VSATARCS.md` |
| `sigil|moku|乡` | Policy Layer Harness | `mojo/up`, `p4ng/proportional-load-sharing`, `storage/error-layer-hierarchy` | `futon4/docs/VSATARCS.md` |
| `sigil|monsi|已` | Informal Argument Support | `devmap-coherence/ifr-f6-upekkha`, `math-informal/local-to-global`, `math-informal/the-diagonal-argument` | `futon4/docs/VSATARCS.md` |
| `sigil|nanpa|计` | Counter-Ratchet (I2) | `futon-theory/counter-ratchet`, `futon-theory/interface-loop`, `or3/count-every-card-back` | — |
| `sigil|nanpa|门` | Progress Signal | `futon-theory/progress-signal`, `iching/hexagram-35-jin`, `realtime/loop-success-signals` | — |
| `sigil|nasin|习` | Scenario Simulations | `coordination/par-as-obligation`, `p4ng/boundary-oscillation`, `peripherals/surface-earns-inhabitation` | `futon4/docs/VSATARCS.md` |
| `sigil|nasin|口` | Mission Interface Signature | `futon-theory/mission-interface-signature`, `futon-theory/mission-lifecycle`, `or/friction-as-feature`, `p4ng/friction-as-feature` | — |
| `sigil|nasin|止` | Compass GFE Alignment | `math-strategy/compose-independent-lemmas`, `stack-coherence/commit-intent-alignment`, `vsatlas/cooperative-learning-environment` | `futon4/docs/VSATARCS.md` |
| `sigil|nimi|乡` | Curriculum & Behaviour Cards | `aif/currency-before-merge`, `enrichment/churn-as-signal` | `futon4/docs/VSATARCS.md` |
| `sigil|palisa|门` | Error Hierarchy (I3) | `futon-theory/error-hierarchy`, `storage/error-layer-hierarchy`, `t3/adaptive-delivery-spine` | — |
| `sigil|pana|予` | Futon3a→Futon0 Telemetry Bridge (Raw) | `math-strategy/convention-bridge`, `or3/ask-first-then-bring-the-expert`, `stack-coherence/futon-bridge-health` | — |
| `sigil|pini|久` | Self-Documenting Foundations | `agent/scope-before-action`, `f6/learning-event-detection`, `p4ng/self-patterning-mandate` | `futon4/docs/VSATARCS.md` |
| `sigil|pi|王` | Mission Scoping | `coordination/intent-to-mission-binding`, `futon-theory/mission-dependency`, `futon-theory/mission-scoping` | — |
| `sigil|poka|本` | Durability First (I0) | `futon-theory/durability-first`, `storage/durability-first`, `storage/durability-throughput-gate` | — |
| `sigil|poki|文` | Graph-Memory Schema & Mirroring | `storage/graph-memory-contract`, `storage/persistence-speed-mirroring`, `storage/reproducible-mirroring` | `futon4/docs/VSATARCS.md` |
| `sigil|pona|正` | F0-F3 Interface (Verification Visibility) | `peripherals/surface-earns-inhabitation`, `t3/intervention-triad`, `t4r/legacy` | — |
| `sigil|sewi|六` | Hunger & Precision Dynamics | `aif/evidence-precision-registry`, `ants/hunger-precision-coupling`, `exotic/hybrid-execution-semantics` | `futon4/docs/VSATARCS.md`<br>`futon5/resources/exotype-xenotype-lift.edn` |
| `sigil|sike|予` | Library Evidence Feedback | `library-coherence/library-evidence-ledger`, `stack-coherence/evidence-ledger`, `stack-coherence/maturity-evidence-audit` | `futon4/docs/VSATARCS.md` |
| `sigil|sike|己` | MetaCA Demonstrator | `coordination/artifact-registration`, `p4ng/candidate-move-generation`, `storage/graph-memory-contract` | `futon4/docs/VSATARCS.md` |
| `sigil|sike|马` | Mission Lifecycle | `futon-theory/baldwin-cycle`, `futon-theory/mission-dependency`, `futon-theory/mission-lifecycle` | `futon3/library/futon-theory/INDEX.md` |
| `sigil|sina|止` | Invariant Enforcement | `sidecar/validation-enforcement-gate`, `storage/invariants-vs-repair`, `workflow-coherence/wip-cap` | `futon4/docs/VSATARCS.md` |
| `sigil|sina|田` | Reasoning Map | `ai4ci/project-summary-core`, `iching/hexagram-10-lu`, `storage/all-or-nothing-startup` | `futon4/docs/VSATARCS.md` |
| `sigil|sitelen|双` | Curry-Howard Operational | `futon-theory/curry-howard-operational`, `math-informal/structural-characterization`, `war-machine/half-blind-observation` | — |
| `sigil|sona|久` | Scholium Mode | `exotic/tri-store-lineage`, `futon-theory/symbolic-geodesic`, `vsatlas/festival-model` | `futon4/docs/VSATARCS.md`<br>`futon5/resources/exotype-xenotype-lift.edn` |
| `sigil|sona|忆` | Meme Layer (ANN Search) | `enrichment/layered-ingestion`, `storage/error-layer-hierarchy`, `vsatlas/three-layer-architecture` | — |
| `sigil|suno|日` | Morning Review Protocol | `futon-theory/event-protocol`, `futon-theory/stop-the-line`, `meta/sigil-differentiation` | `futon4/docs/VSATARCS.md` |
| `sigil|suno|白` | HDM - Opacity | `hdm/hyperreal-capital`, `hdm/opacity-to-transparency`, `or/five-p-rhythm-for-impact`, `p4ng/five-p-rhythm` | `futon5/resources/exotype-xenotype-lift.edn` |
| `sigil|tan|已` | Pivot Stream & Analyzer | `vsatlas/non-destructive-relational-layers`, `vsatlas/open-ecosystem-mandate`, `vsatlas/stewardship-layer` | `futon4/docs/VSATARCS.md` |
| `sigil|tenpo|己` | Hyperreal Enterprises as Seed Institution | `p4ng/agent-pattern-triad`, `popiii/patterns-as-reflexive-infrastructure`, `system-coherence/treat-content-refresh-as-second-order-when-conditions-bind` | — |
| `sigil|toki|乃` | Cross-Domain Reasoning Patterns | `ants/hunger-precision-coupling`, `eight-gates/press-mechanism`, `realtime/learn-as-you-go` | `futon4/docs/VSATARCS.md` |
| `sigil|walo|四` | Observation Layer Hardening | `p4ng/proportional-load-sharing`, `software-design/observer-pattern`, `storage/error-layer-hierarchy` | `futon4/docs/VSATARCS.md`<br>`futon5/resources/exotype-xenotype-lift.edn` |
| `sigil|wan|一` | Single Source of Truth (I1) | `agency/single-routing-authority`, `exotic/live-sync-source-truth`, `futon-theory/single-source-of-truth` | — |
| `sigil|wawa|习` | Viriya Metrics | `storage/graph-memory-contract` | `futon4/docs/VSATARCS.md` |
| `sigil|wawa|二` | Proof Hooks (futon1/futon2 Export) |  | `futon4/docs/VSATARCS.md` |

## After adjudication

Write `docs/sigil-rejoin-resolution-2026-08-23.edn`:

```clojure
{:resolved-at "…" :by "<agent/human>" :input-sha "<sha of sigil-rejoin-2026-08-23.edn>"
 :rows [;; pattern sigils — same relation type the apply script already writes
        {:sigil-id "sigil|ante|也" :pattern-id "or/overlay-bridges"
         :relation-type :pattern/has-sigil :by :human :note "or/ not p4ng/"}
        {:sigil-id "sigil|ala|乃" :pattern-id "hdm/deep-storage-to-active-graph"
         :relation-type :pattern/has-sigil :by :title-normalised :note "truncated at ->"}
        ;; devmap prototypes — different relation type AND different src population
        {:sigil-id "sigil|…" :prototype-id "f2/p7"           ; the devmap/prototype :entity/name
         :relation-type :prototype/has-sigil :by :human :note "P7 Query Workflows"}
        ;; retirements are explicit, never implied by absence
        {:sigil-id "sigil|…" :retire true :note "Meme Layer (ANN Search): no corpus hits"}]}
```

Every sigil-id from the ambiguous + unmatched buckets (79) must appear exactly once. Then extend
`scripts/sigil-rejoin-apply.py` to read this file in addition to `:matched` (it is idempotent, so the 326
already written are no-ops), resolving `:prototype-id` against `GET /api/alpha/entities?type=devmap/prototype`
by `:entity/name`. Retirement rows are reported, not executed, by the apply script.
