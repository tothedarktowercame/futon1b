# BEFORE — evidence-landscape index

**Measured:** 2026-08-17, against the live futon1b service on
`localhost:7073` (`futon1b-server.service`, PID 900542).  This is the
before leg required by `M-evidence-landscape-index` V2.1/V4.4 and packet
P-before.  Every HTTP operation below was a read-only `GET`; the service was
not restarted and no store or code changes were made.  Probes were issued
sequentially, with at most one measurement request in flight.  In the tables,
"cold" means the first request and "warm" the immediate repeat; caches were
not flushed between them.

## Live environment

At the end of the probe window the current sidecar inventory was 140,242
rows (up from the mission's 140,233-row inventory):

```sh
curl -sS -m 180 'http://localhost:7073/api/alpha/evidence/text-search?stats=true'
```

```text
{:indexed 3, :errors 3, :last-at "2026-08-17T07:36:27.532371939Z", :last-error {:id "emacs-62219b61e562295188992e1e5eccfd78", :at "2026-08-16T13:57:34.251894908Z", :error "SQLiteException: [SQLITE_BUSY] The database file is locked (database is locked)"}, :rows 140242, :ready true, :periodic? true, :catch-up-running? false}
```

Host/service context was captured with:

```sh
systemctl --user show futon1b-server.service -p ActiveState -p SubState -p MainPID -p MemoryCurrent -p CPUUsageNSec --no-pager
uptime
nproc
free -h | sed -n '1,2p'
```

The service was active/running, using 9,774,452,736 bytes; the 8-core host
had load averages 7.26/3.84/2.22 and 19 GiB of 30 GiB in use (11 GiB
available).  These are observations of the shared live machine, not a
controlled benchmark fixture.

## 1. Attribute-only post-filtered page walk

`turn` is the common-tag case: it is the mission's established tag for the
high-volume operator/agent turn stream.  For the comparison tag I fetched a
bounded three-document recent sample and observed `:context-retrieval` on one
of those documents:

```sh
curl -sS -m 180 'http://localhost:7073/api/alpha/evidence?limit=3' | head -c 6000
```

I therefore selected `context-retrieval` as the sparse candidate without
running an unbounded tag census.  Both requests nevertheless filled the
50-result response limit, so this run establishes only that it is sparse by
sample/semantics relative to `turn`, not that the corpus contains fewer than
50 such documents.

The exact common-tag command, run twice:

```sh
curl -sS -m 180 -w '\n__HTTP__=%{http_code} __TIME_TOTAL__=%{time_total}\n' 'http://localhost:7073/api/alpha/evidence?tags=turn&limit=50' | tail -c 1200
```

| run | HTTP | returned | curl `time_total` |
|---|---:|---:|---:|
| cold (first) | 200 | 50 | 1.879033 s |
| warm (repeat) | 200 | 50 | 3.842036 s |

The exact sparse-candidate command, run twice:

```sh
curl -sS -m 180 -w '\n__HTTP__=%{http_code} __TIME_TOTAL__=%{time_total}\n' 'http://localhost:7073/api/alpha/evidence?tags=context-retrieval&limit=50' | tail -c 1200
```

| run | HTTP | returned | curl `time_total` |
|---|---:|---:|---:|
| cold (first) | 200 | 50 | 4.782582 s |
| warm (repeat) | 200 | 50 | 4.080119 s |

There was no timeout (all four were below the 180-second cap).  "Warm" did
not imply faster: the second `turn` request was about twice as slow as the
first under concurrent live load.

### Journal corroboration

The slowest measured probe was the first `context-retrieval` request.  The
service journal independently brackets it and reports 4,782 ms, agreeing
with curl's 4.782582 s:

```text
2026-08-17T08:40:56.183048+01:00 Dionysus clojure[900542]: [futon1b-request] start method=GET uri=/api/alpha/evidence?tags=context-retrieval&limit=50 trace-id=-
2026-08-17T08:41:00.965339+01:00 Dionysus clojure[900542]: [futon1b-request] end method=GET uri=/api/alpha/evidence?tags=context-retrieval&limit=50 trace-id=- elapsed-ms=4782 outcome=ok
```

The remaining request/journal pairs were 1.879033 s/1,878 ms and 3.842036
s/3,841 ms for `turn`, then 4.080119 s/4,079 ms for the
`context-retrieval` repeat.  The journal was queried with:

```sh
journalctl --user -u futon1b-server.service --since '2026-08-17 07:39:30 UTC' --until '2026-08-17 07:50:00 UTC' --no-pager -o short-iso-precise
```

## 2. Content × attribute composition is not expressible

The current text-search surface applies `q`, `author`, `session-id`, `since`,
`before` (plus pagination/hydration controls), but does not apply `tags` or
`claim-type`.  The live proof used the known 2026-08-15 content term
`Michaela`: the response body was hashed so equality means the full returned
EDN, not merely its count, was identical.

Baseline command, run twice:

```sh
curl -sS -m 180 -w '%{stderr}__HTTP__=%{http_code} __TIME_TOTAL__=%{time_total}\n' 'http://localhost:7073/api/alpha/evidence/text-search?q=Michaela&author=joe&limit=50&hydrate=true' | sha256sum
```

`tags=user` command, run twice:

```sh
curl -sS -m 180 -w '%{stderr}__HTTP__=%{http_code} __TIME_TOTAL__=%{time_total}\n' 'http://localhost:7073/api/alpha/evidence/text-search?q=Michaela&author=joe&limit=50&hydrate=true&tags=user' | sha256sum
```

`claim-type=question` command, run twice:

```sh
curl -sS -m 180 -w '%{stderr}__HTTP__=%{http_code} __TIME_TOTAL__=%{time_total}\n' 'http://localhost:7073/api/alpha/evidence/text-search?q=Michaela&author=joe&limit=50&hydrate=true&claim-type=question' | sha256sum
```

| request | cold `time_total` | warm `time_total` | HTTP, both | body SHA-256, both |
|---|---:|---:|---:|---|
| baseline | 0.147501 s | 0.171147 s | 200 | `69797ba6da7ea7d6dbc5901628e21c473173739d0ccf626969181687f72b9863` |
| `tags=user` | 0.137143 s | 0.481703 s | 200 | `69797ba6da7ea7d6dbc5901628e21c473173739d0ccf626969181687f72b9863` |
| `claim-type=question` | 0.138229 s | 0.205504 s | 200 | `69797ba6da7ea7d6dbc5901628e21c473173739d0ccf626969181687f72b9863` |

Thus the fixed sensor sweep, `author=joe AND tags=[:user] AND content MATCH
<term>` (and its `claim-type` refinement), cannot be submitted as one
effective query on the before surface: adding either missing parameter is a
no-op.

### Honest best-available workaround

The workaround fetches at most 50 hydrated FTS matches and then filters the
returned documents client-side for both `:user` and
`:evidence/claim-type :question`.  The exact command below was run twice;
the `bb` stage reads the returned EDN and does not issue another request.

```sh
curl -sS -m 60 -w '%{stderr}__HTTP__=%{http_code} __TIME_TOTAL__=%{time_total}\n' 'http://localhost:7073/api/alpha/evidence/text-search?q=Michaela&author=joe&limit=50&hydrate=true' | bb -e '(require (quote [clojure.edn :as edn])) (let [response (edn/read-string (slurp *in*)) results (:results response) survivors (filter #(let [doc (:entry %)] (and (some #{:user} (:evidence/tags doc)) (= :question (:evidence/claim-type doc)))) results)] (println (str "result_count=" (count results) " survivor_count=" (count survivors))) (doseq [item survivors] (println (get-in item [:entry :evidence/id]))))'
```

| run | HTTP | hydrated content matches | survivors (`:user` + `:question`) | curl `time_total` |
|---|---:|---:|---:|---:|
| review rerun (corrected filter) | 200 | 2 | 2 | 0.159893 s |

**Review correction (claude-9, same day):** the original run of this
workaround reported 1 match / **0 survivors**, but that zero was a
measurement artifact, not a system fact: `text-search` returns results as
`{:score … :entry doc}`, and the original client filter read
`:evidence/tags` on the wrapper instead of on `:entry`, so tags were always
nil and nothing could survive. The corrected command above (filter applied
to `:entry`) retrieves both Michaela-bearing operator turns, including the
known 2026-08-15 one (`emacs-08dc8df323d560e364287af3d1a29241`). The count
moved from 1 to 2 matches between the original run and the review rerun
because the live corpus had grown in the interval.

The honest before-story is therefore: the workaround *does* retrieve, but
(a) it is not one query — composition happens client-side; and (b) it is
recall-bounded at the content limit: attribute filtering occurs after the
50-candidate FTS cut, so attribute predicates can never widen or steer
candidate selection, and matching documents excluded upstream are
unrecoverable. Those two properties, not a retrieval failure, are what the
after leg must beat.

## Confounds and interpretation limits

- This was the live `migration-store-21`-era service and its approximately
  140k-row sidecar, not a fixture.  Results apply to this corpus, history,
  index state, and machine at this time.
- The machine was shared and materially loaded (8 cores; load average 7.26
  at the environment snapshot).  The journal shows concurrent traffic,
  including writes by other clients.  No writes were made by this
  measurement.
- In particular, the second `turn` probe overlapped an unrelated entity GET
  that ran for 12,914 ms.  The first sparse probe overlapped a 9,859 ms
  hyperedge scan and evidence POST/GET traffic.  A separate long hyperedge
  scan spanning 78,522 ms overlapped the later sparse/text-search window.
  This explains why first/second order is not a clean cache experiment.
- The periodic sidecar remained enabled (`:periodic? true`), although no
  catch-up was running at the final stats snapshot.  Row count grew from the
  mission inventory's 140,233 to 140,242 because the live system continued
  receiving traffic.
- The attribute responses hit the requested 50-row ceiling.  These timings
  measure the endpoint's current post-filtered page-walk path to obtain that
  page, not a full corpus count or selectivity census.
- Curl's `time_total` is client wall-clock.  The journal's server elapsed
  time is the independent corroboration; sub-millisecond rounding accounts
  for their small differences.

## Reviewer addendum (claude-9, 2026-08-17)

The §1 attribute timings are a **floor, not a ceiling**, on the before-cost.
Both probed tags filled the 50-row limit quickly, so both measurements
exercise the find-50-early path (~2–5 s). The pathological case — a tag
matching fewer than 50 documents, worst of all zero — forces the
post-filter to walk the entire ordered corpus in 1,000-row pages (~141
pages at the current inventory; cold single-page bounded scans measured
2–3 s in `holes/SPIKE-attribute-index-2026-07-26.md`, implying minutes of
server-side work). That probe was **deliberately not fired at the live
service**: a client timeout does not stop the server-side walk (the spike's
journal records scans completing after client disconnect), so the
measurement would impose multi-minute load on a box already showing a 78.5 s
concurrent scan during this window. The heavy tail is instead documented by
the spike's production journal evidence (typed sweeps 5–28 s; non-PK
fallbacks 20–30 s with client disconnects). The after leg should therefore
compare: (i) these same two tag probes, (ii) the sensor-sweep composition
(inexpressible → one query), and (iii) a sub-50-match tag query, which
becomes safe to measure only once the index serves it.
