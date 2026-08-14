# DEFECT: bitemporal `as-of` fails on two routes, for two different reasons

**Filed:** 2026-07-31 by claude-2
**Component:** futon1b (application layer) — **NOT XTDB**
**Severity:** Defect 1 is silent-wrong-answer; Defect 2 is fail-closed
**Found via:** M-memory-retrieval white paper V2 planning; blocks the frozen
chronological benchmark (Experiment 0)

## Why this note exists

Two agents independently observed that `system-as-of` had no effect on
`GET /api/alpha/evidence` and generalised to "the store ignores as-of". That
generalisation was wrong and was about to be carried into a conversation with
XTDB's maintainers as an XTDB bug. Tracing to source shows **two distinct
futon1b faults**, one of which masquerades as the other. Neither is XTDB's.

## Defect 1 — `GET /api/alpha/evidence` silently drops `as-of`

**Behaviour.** The parameter is accepted, ignored, and no error is raised. The
caller receives present-time data believing it is historical.

```bash
curl -s 'http://127.0.0.1:7073/api/alpha/evidence?type=pattern-outcome&limit=3' \
  -o /tmp/a.edn -w '%{size_download}\n'
# 17441

curl -s 'http://127.0.0.1:7073/api/alpha/evidence?type=pattern-outcome&limit=3&system-as-of=2020-01-01T00:00:00Z' \
  -o /tmp/b.edn -w '%{size_download}\n'
# 17441   <- byte-identical; correct answer is ZERO rows (store did not exist)
```

**Cause.** `evidence-route` (`futon1b_server.clj:382–426`) dispatches to
`ev/query-evidence-response`, passing `(query-params ex)` unfiltered.
`futon1b_evidence.clj` contains **no** occurrence of `as-of`, `basis`, or
`snapshot`. The parameter is dropped in the handler and never reaches XTDB.

Compare `hyperedges-route` (`:529–534`) and `memory-projection-route`
(`:592–596`), which both parse `valid-as-of` / `system-as-of` correctly. The
evidence route is the odd one out.

**Why it matters.** `dispatch_with_recall.clj:376–388` and
`substrate/client.clj:59–113` construct as-of requests. Any caller that routes
a temporal query through the evidence endpoint gets silently wrong data. A
silent wrong answer is worse than an error.

**Suggested fix.** Either parse and honour the parameters as the sibling
routes do, or reject the request with an explicit
`:unsupported-temporal-parameter` error. Silently ignoring is the one option
that should be off the table.

## Defect 2 — `POST /api/alpha/memory/projection` implements `as-of`, then refuses to return it

**Behaviour.** As-of genuinely reaches the engine — but every in-range query
fails closed on a result bound.

```bash
# baseline, no as-of
curl -s -X POST 'http://127.0.0.1:7073/api/alpha/memory/projection' \
  -H 'Content-Type: application/edn' -H 'Accept: application/edn' \
  -d '{:endpoints ["math-formalization/tactic-algebra-interference"] :limit 10}'
# 200, 22973 bytes, 10 memories

# out-of-range as-of -> correctly empty (this proves the plumbing works)
… -d '{… :limit 10 :system-as-of "2020-01-01T00:00:00Z"}'
# 200, 571 bytes, 0 memories   ✅

# any in-range as-of, at limit 3, 5 or 10 -> always 400
… -d '{… :limit 10 :system-as-of "2026-07-29T00:00:00Z"}'
# 400 {:error {:layer 4, :reason :memory-projection-result-bound-exceeded,
#              :context {:endpoint-count 1, :per-endpoint-limit 10, :maximum 10}}}
```

**Cause.** `futon1b_graph.clj:942–949`:

```clojure
(list 'limit (inc raw-limit))
…
(when (> (count selected+) raw-limit)
  (throw (gates/layered-error
          4 :memory-projection-result-bound-exceeded
          {:endpoint-count endpoint-count
           :per-endpoint-limit limit
           :maximum raw-limit})))
```

This is a truncation guard: fetch one more than asked, and if you got it,
refuse rather than silently truncate. Reasonable for current-time queries.

Under a bitemporal query the underlying scan returns **every historical
version** of each edge, so the row count exceeds `raw-limit` essentially
always, and the guard fires unconditionally. It cannot distinguish "too many
distinct results" from "the same results, many versions". Lowering the limit
does not help — it lowers the threshold in step (verified at 3, 5, 10).

The guard predates bitemporal use of this route and is blind to version
history.

**Suggested fix.** Make the bound version-aware: deduplicate by entity id (or
count distinct entities) *before* the bound check, so the guard measures
distinct results rather than rows. Alternatively apply the guard only when no
temporal basis is supplied.

**Why it matters.** This route is the one `dispatch_with_recall` uses, and it
is the only path by which dispatch-time graph state could be reconstructed
without per-problem snapshots. Fixing the guard may unblock the frozen
benchmark; it is a one-file change in our own code.

## What is NOT established

Stated explicitly so nobody over-reads this note:

- **No XTDB bug is demonstrated.** Both faults are in futon1b handlers with
  named line numbers.
- **XTDB history retention is untested.** That the parameter reaches the
  engine does not establish that enough history is retained to reconstruct,
  say, 2026-07-25 state.
- **valid-time vs system-time semantics are untested** for this use case.

The last two are legitimate questions for the XTDB maintainers *if they still
stand after Defect 2 is fixed* — but they should be asked as questions, on
evidence gathered after the fix, not as bug reports now.

## Repro environment

futon1b embedded in the futon3c JVM (I-0), store on `127.0.0.1:7073`,
2026-07-31. Read-only; no writes performed.
