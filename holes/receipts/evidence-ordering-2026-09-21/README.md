# Evidence reader global ordering — 2026-09-21

Author: codex-15. Commission: invoke-1790014276427-23047-b5c5972d.
Baseline / implementation parent: `14621cf328f0c8f439fd6f0c240e8a13b691b625`.
No serving eval, load, restart, emitter/schema change, or deep-health call.

## Diagnosis

The live GET reproduced the reported 1,000 rows / 103 adjacent ordering
violations; `live-order-summary.json` retains the request, cursor and first bad
pair without copying evidence bodies. Read-only process inspection found the
serving futon1b process rooted at `/home/joe/code/futon1b`, using XTDB 2.1.0.
Loaded Clojure var identity was not inspected: reproducing against the actual
parent source in a separate process establishes that source/dependency code is
sufficient to cause the defect, without needing a loaded-code divergence.

The failure is in **projection ordering**, not hydration. At 110,000 real stored
records, the parent's compact projection has 63 ordering violations. Hydration
preserves precisely its projected ID order. Smaller probes with 2,600 and
26,000 records both passed, which is why a 1,001-record regression would miss it.

The installed `xtdb-core-2.1.0.jar`, `xtdb/operator/order_by.clj`, explains the
threshold: `*block-size*` defaults to 102400. `sorted-idxs` sorts one relation
correctly. External merge `mk-rel-comparator` constructs a comparator between
**different** left/right relations, then uses Java `Comparator.reversed` for
`:desc`. That reverses the row-index arguments, so each index is applied to the
wrong relation. Reversing arguments is valid for the same-relation comparator,
but does not negate the cross-relation comparison. The global merged result is
wrong before the SQL/XTQL limit. This is not a timestamp-format discrepancy.

Read the retained dependency source without loading a JVM:

```sh
unzip -p ~/.m2/repository/com/xtdb/xtdb-core/2.1.0/xtdb-core-2.1.0.jar \
  xtdb/operator/order_by.clj
```

## Reader change

`page-query` emits the parameterized compact cursor-bounded projection with
**no database ORDER BY or LIMIT**. `fetch-newest-projected-page` reduces the
entire projection into a persistent ordered set of the greatest K `(at,id)`
keys, retaining at most K+1 rows during an insertion, then returns those keys
descending. Thus selection is global before the limit/cursor. This is not
sorting an already-limited page, narrowing the date window, or suppressing an
ordering detector. No XTDB global var or dependency is patched.

`futon1b-xt/timed-reduce-q` exposes the existing JDBC realization under the
same permit, deadline, transaction and close/finally handling. Its initial
accumulator is persistent/reusable across cached-plan retries. `timed-q`
continues to return a vector using this implementation. Full evidence bodies
are still hydrated only for the chosen bounded window. Post-filters and the
existing logical scan ceiling / incomplete cursor remain intact.

Cost: each compact projection is streamed globally, with O(N log K) comparison
work and O(K) retained rows. The former external sort also had to consider the
global input. This trades the broken database sort for reader-side comparisons;
it does not promise a production latency improvement. Existing query deadlines
still apply. The serving query has not been rerun against the fix because the
owner will review and reload it.

## Real-store failing / passing regression

Namespace: `test-evidence-ordering`, test tagged `^:slow`.
Command: `clojure -M:node -m test-evidence-ordering`.

One ephemeral real XTDB node and real HTTP server on loopback port 0, no stubs.
110,000 documents inserted in 110 batches, interleaved across 20 days, with
repeated timestamps and different IDs; 55,000 match `context-retrieval`.
The HTTP request uses exactly `tags + since + before + limit=1000` from the
commission. Every continuation uses both returned cursor fields. The independent
oracle is the entire bounded matching fixture identity set sorted by `(at,id)`.
The test separately checks projection order, hydration ID preservation, full
first-page size, termination, strict concatenated order, uniqueness, set equality
and ordered identity equality.

The identical test file was copied into a clean archive of the parent:

```sh
mkdir -p /tmp/evidence-ordering-15/parent
git archive 14621cf328f0c8f439fd6f0c240e8a13b691b625 | \
  tar -x -C /tmp/evidence-ordering-15/parent
cp test_evidence_ordering.clj /tmp/evidence-ordering-15/parent/
(cd /tmp/evidence-ordering-15/parent && clojure -M:node -m test-evidence-ordering)
# Then, in the changed canonical checkout:
clojure -M:node -m test-evidence-ordering
```

Actual parent output (`parent-regression.log`, exit 1):

```text
projection violations: 63 hydration preserves IDs: true
returned: 50184 expected: 55000 violations: 1 unique: 49842
Ran 1 tests containing 59 assertions.
5 failures, 0 errors.
```

Actual fixed output (`fixed-regression.log`, exit 0):

```text
projection violations: 0 hydration preserves IDs: true
returned: 55000 expected: 55000 violations: 0 unique: 55000
Ran 1 tests containing 64 assertions.
0 failures, 0 errors.
```

The assertion-count difference is the per-HTTP-page status assertion: the broken
reader terminates prematurely in 51 pages; the fixed reader uses 56, including
its final empty page. No test expectation changed between these runs.

## Gates and existing-suite limitations

- `clj-kondo --lint futon1b_xt.clj futon1b_evidence.clj test_evidence_ordering.clj test_evidence_deadline.clj`: zero errors/warnings.
- `emacs -Q --batch -l /home/joe/code/futon4/dev/check-parens.el --eval '(arxana-check-parens-cli)' -- --no-defaults futon1b_xt.clj futon1b_evidence.clj test_evidence_ordering.clj test_evidence_deadline.clj`: OK.
- `test-evidence-ordering`: green as above.
- `test-evidence-deadline`: **not green before or after**. Both parent and fixed
  runs report 38 passing checks and the same failing `/health` assertion expecting
  two permits, while the server's existing semaphore has four. Both then throw
  the same 32128-byte Arrow live-index allocator leak at node close after the
  deadline probe. Raw parent/fixed logs retained. Only this namespace's projection
  query-shape assertions were adapted for the removed database limit parameter;
  the unrelated failing assertion and timeout probe were not weakened.
- `test-a1a2`: **partial, not a passing namespace run**. Thirty checks pass,
  including real 1,000-row hydration equivalence. Its existing admission test
  holds two scans, synchronously sends a third expecting rejection, and only
  releases the first two after that response. With four actual permits the third
  enters the same blocked stub, causing a deadlock. The isolated test process was
  terminated; its partial log is retained. No production process was signalled.
  This test file is unchanged; the deadlock is outside the reader invariant.

No all-suite-green claim is made. The new requested acceptance regression and
static gates pass; the above existing test defects remain explicit review risks.

## Reload order for the owner

After review, from the serving JVM's own master classpath:

```clojure
(require 'futon1b-xt :reload)
(require 'futon1b-evidence :reload)
```

No server namespace reload is required: it calls evidence vars. No reload was
performed by this task. Reissue the originally failing GET after the owner reloads;
the current live response alone cannot certify the undeployed fix.
