# TN — futon1b live query cost and contention profile

**Status:** measurement only, 2026-09-08. No source, store, or service state was
changed. Measurements used the live `migration-store-21` service on port 7073
(PID 3291517, started 2026-09-08 02:04:57 UTC). Joe's target is milliseconds.

## Reproduction

The latency samples below use wall-clock milliseconds from curl. Run from
`/home/joe/code/futon1b`.

```bash
date -u +%FT%TZ
curl -sS http://127.0.0.1:7073/health
ps -p 3291517 -o pid,lstart,etime,%cpu,%mem,rss,vsz,cmd

python3 -c 'import subprocess,statistics,json,datetime
base="http://127.0.0.1:7073/api/alpha/hyperedges?end=m1&type=memory/assert&limit=1"
def r(url,n=8):
 xs=[float(subprocess.check_output(["curl","-sS","-o","/dev/null","-w","%{time_total}",url]))*1000 for _ in range(n)]
 return {"raw_ms":[round(x,3) for x in xs],"p50_ms":round(statistics.median(xs),3),"min_ms":round(min(xs),3),"max_ms":round(max(xs),3)}
print(json.dumps({"at":datetime.datetime.now(datetime.timezone.utc).isoformat(),"default":r(base),"true":r(base+"&include-total=true"),"false":r(base+"&include-total=false")},indent=2))'

python3 -c 'import subprocess,statistics,json,datetime
base="http://127.0.0.1:7073/api/alpha/hyperedges?type=memory/assert&limit=1"
def r(s):
 xs=[float(subprocess.check_output(["curl","-sS","-o","/dev/null","-w","%{time_total}",base+s]))*1000 for _ in range(6)]
 return {"raw_ms":[round(x,3) for x in xs],"p50_ms":round(statistics.median(xs),3)}
print(json.dumps({"at":datetime.datetime.now(datetime.timezone.utc).isoformat(),"default":r(""),"true":r("&include-total=true"),"false":r("&include-total=false")},indent=2))'
```

At 07:59:46 UTC the end+type default measured 1207.483–1328.356 ms
(p50 1294.199), explicit true 1227.926–1330.676 ms (p50 1267.861), and
explicit false 0.179–0.693 ms (p50 0.287). At 08:00:20 UTC type-only default
was p50 2341.534 ms, explicit true p50 2030.578 ms, and false paid 1191.834 ms
first, then 0.316–0.764 ms (six-call p50 0.489).

The endpoint census command was:

```bash
for spec in 'health|/health' 'evidence|/api/alpha/evidence?session-id=no-such-session-cost-profile&limit=1' 'evidence-count|/api/alpha/evidence/count?session-id=no-such-session-cost-profile' 'evidence-sessions|/api/alpha/evidence/sessions?limit=1' 'entities|/api/alpha/entities?type=agent-view&limit=1' 'entities-latest|/api/alpha/entities/latest?type=agent-view&limit=1' 'relations|/api/alpha/relations?type=no-such-relation-cost-profile&limit=1' 'census|/api/alpha/census?type=no-such-type-cost-profile' 'memory-search|/api/alpha/memory/search?type=no-such-memory-cost-profile&limit=1' 'text-search|/api/alpha/evidence/text-search?q=no-such-token-cost-profile&limit=1'; do
 n=${spec%%|*}; u=${spec#*|}; printf '%s ' "$n"
 curl --max-time 20 -sS -o /dev/null -w '%{http_code} %{time_total}\n' "http://127.0.0.1:7073$u"
done
```

At 08:01 UTC all returned HTTP 200. Times were health 0.723 ms; evidence
841.378; evidence count 936.181; evidence sessions 1407.695; entities 3562.520;
entities/latest 2374.360; empty relations 7.479; census 910.051; memory search
875.060; and text search 4.500. This is one observation per endpoint, useful
for locating costs but not a latency distribution.

## 1. The include-total default

The route defaults the parameter to true (`futon1b_server.clj:837-860`).
For type-only queries, true executes a second XTDB query over every matching
id and counts its materialized result (`futon1b_graph.clj:926-934`), in
addition to the bounded window query (`futon1b_graph.clj:884-925`).

For end queries there is a more important correction to the initial hypothesis.
That branch does not compute a total; it returns the bounded result count
(`futon1b_graph.clj:846-882`). True is slow there because only explicit false
is eligible for the response cache (`futon1b_graph.clj:1031-1049`). Thus
include-total currently controls both count semantics and cache eligibility.

Caller inventory:

```bash
rg -n 'api/alpha/hyperedges|/hyperedges\?' --glob '*.{clj,cljs,bb,el,sh}'  /home/joe/code/futon0 /home/joe/code/futon1 /home/joe/code/futon1b  /home/joe/code/futon2 /home/joe/code/futon3a /home/joe/code/futon3b  /home/joe/code/futon3c /home/joe/code/futon4 /home/joe/code/futon5  | rg -v '/(target|\.lake|node_modules)/'
rg -n 'include-total' --glob '*.{clj,cljs,bb,el,sh}'  /home/joe/code/futon0 /home/joe/code/futon1 /home/joe/code/futon1b  /home/joe/code/futon2 /home/joe/code/futon3a /home/joe/code/futon3b  /home/joe/code/futon3c /home/joe/code/futon4 /home/joe/code/futon5  | rg -v '/(target|\.lake|node_modules)/'
```

The first scan found 60 source/test occurrences. The second found explicit
false in futon2's substrate and 18 current futon3c sites; no production caller
explicitly requested true. Several scripts omit it. No caller in this scan
uses an exact-total-specific field; consumers use rows, returned count, or
cursor.

**Rank 1 proposal:** default to false, retaining explicit true for exact totals.
Expected warm end+type p50: 1294.199 → 0.287 ms; type-only: 2341.534 → 0.489 ms.
Adding false to omissions is a cross-repo compatibility step, not the primary fix.

## 2. First-call cache cost, key, and eviction

This is a materialized response cache, not merely an XTDB plan cache. Its key
is `[node opts]`, so exact values of type, end, limit, cursor, fields, and
temporal basis distinguish entries (`futon1b_graph.clj:1037-1049`). XTDB
plans use parameterized filter shapes (`futon1b_graph.clj:887-904`), but the
first response still must be materialized. Capacity is 48 FIFO entries, with
a documented worst case of about 115 MB; writes invalidate entries
(`futon1b_graph.clj:976-1029`).

```bash
python3 -c 'import subprocess,statistics,datetime
base="http://127.0.0.1:7073/api/alpha/hyperedges?limit=1&include-total=false&type="
def get(t): return float(subprocess.check_output(["curl","-sS","-o","/dev/null","-w","%{time_total}",base+t]))*1000
pre=get("memory/assert"); xs=[get("no-such-cache-profile-%02d"%i) for i in range(49)]; post=get("memory/assert")
print("AT",datetime.datetime.now(datetime.timezone.utc).isoformat()); print("pre_ms",round(pre,3)); print("49_distinct_ms",[round(x,3) for x in xs]); print("median_ms",round(statistics.median(xs),3),"post_ms",round(post,3))'
```

At 08:03:41 UTC the hot control took 0.738 ms. Forty-nine distinct exact option
sets took median 995.658 ms; revisiting the evicted control took 1043.786 ms.
The millisecond claim therefore fails as soon as the exact-key working set
exceeds 48. A 20-page walk consumes 20 keys because each cursor differs, as
the source comment notes.

This reconciles `TN-evidence-query-fixed-cost-2026-09-07.md:1-45`: evidence
has no equivalent materialized cache and therefore pays its roughly 850 ms
engine floor every time. Hyperedges looked categorically faster because the
sampled exact key was warm.

**Rank 2 proposal:** expose cache hits, misses, evictions, and retained bytes;
then partition hot point/type windows from cursor walks. Expected benefit is
keeping the observed 0.738 ms hot key from regressing to 1043.786 ms at 49
shapes. Do not simply raise capacity: the measured source bound is 2.4 MB per
entry and 115 MB total.

**Rank 3 proposal:** add prepared/cached paths for evidence count and session
filters. Target <5 ms, down from the measured 841–1408 ms empty/filter costs.

## 3. The global two-permit gate

One fair `Semaphore(2)` covers the JVM, not APM or a route
(`futon1b_server.clj:499-504`). Admission waits 3000 ms then returns retryable
503 (`futon1b_server.clj:505-520,575-610`). It protects the 4 GB heap and
pgwire/Arrow work from concurrent scans. The holder registry followed an
incident where two blocked reads held permits for four days
(`futon1b_server.clj:521-528`; `docs/TN-futon1b-gc-wedge-incident-2026-08-23.md:15-58`).

Permit routes are evidence query/count/sessions (`futon1b_server.clj:655-688`),
entities/latest and entities (`:742-760`), relations (`:787-800`),
hyperedges (`:837-860`), census (`:863-871`), memory search (`:892-901`),
temporal memory projection (`:911-932`), and hydrated text search
(`:935-1005`). Point reads, ordinary projection, types, writes, and health
do not take one.

At 07:59:18 UTC normal health showed 2 available, 0 waiters, no holders,
oldest age 0 ms, 4238 admitted, 4232 completed, 9 rejected, 6 errored, and
0 timed out. Reproduce with the first command block.

The contention probe:

```bash
python3 -c 'import subprocess,time,threading,urllib.request,datetime,re
url="http://127.0.0.1:7073/api/alpha/hyperedges?type=memory/assert&limit=1&include-total=true"
start=time.monotonic(); results=[]
def one(i):
 p=subprocess.run(["curl","-sS","-o","/dev/null","-w","%{http_code} %{time_total}",url],capture_output=True,text=True); results.append((i,round((time.monotonic()-start)*1000,1),p.stdout))
ts=[threading.Thread(target=one,args=(i,)) for i in range(4)]
for t in ts:t.start()
samples=[]
while any(t.is_alive() for t in ts):
 try:
  s=urllib.request.urlopen("http://127.0.0.1:7073/health",timeout=1).read().decode(); samples.append((round((time.monotonic()-start)*1000,1),int(re.search(r":permits/available (\d+)",s).group(1)),int(re.search(r":permits/waiters (\d+)",s).group(1)),int(re.search(r":oldest-holder-ms (\d+)",s).group(1))))
 except Exception as e:samples.append((round((time.monotonic()-start)*1000,1),"ERR",str(e)))
 time.sleep(.1)
for t in ts:t.join()
print("AT",datetime.datetime.now(datetime.timezone.utc).isoformat()); print("RESULTS",sorted(results)); print("SAMPLES",samples)'
```

At 08:02:26 UTC the first pair completed in 2.823–2.824 s and the queued pair
in 5.419 s. Health then observed both permits held and holder age rising from
1 to 2532 ms. More importantly, health timed out twice over the first 2.1 s:
two executing requests plus two permit waiters occupied all HTTP workers.
Admission limits scans but does not prevent waiters exhausting workers.

Two simultaneous 1.25 s holders imply an ideal 2.50 s lower bound for the next
pair, below futon3c's 30 s client deadline, but service above the 3 s permit
wait can reject the next arrival. Tail service and worker occupancy matter.

**Rank 4 proposal:** keep two permits until a controlled load test measures
heap, direct memory, GC, and pgwire progress at three and four. First remove
blocking permit waits from HTTP workers or shed before blocking. Expected
effect: health remains near 0.723 ms under four-reader contention instead of
exceeding its 1000 ms probe timeout. Raising permits alone would discard the
protection introduced after the GC/pgwire wedge.

## 4. Post-restart cold window

At the first sample the process uptime was 5 h 54 min. Health showed heap
724/4096 MB, metaspace 156 MB, and GC counts 13,328 young, 3,010 concurrent,
5 old. The eviction experiment establishes a cold *query-key* cost without
restart: unseen exact keys cost median 995.658 ms, while hot keys are below
1 ms.

It cannot establish a 30–45 minute post-restart duration. Historical service
logs are unavailable to this account:

```bash
systemctl status futon1b --no-pager
journalctl -u futon1b --since '2026-09-08 02:00:00'   --until '2026-09-08 03:15:00' --no-pager
```

The first says the unit is not found; the second exposes no entries. Without
restart telemetry, the 02:30 and 02:47 incidents cannot be attributed to cold
caches rather than contention. This needs a restart experiment scheduled by Joe.

**Rank 5 proposal:** at the next scheduled restart, sample health, cache
misses, holders/waiters, HTTP-worker occupancy, heap/direct memory, GC, and a
fixed read-only panel once per minute. Warm only production keys with false;
warming 49 arbitrary keys demonstrably evicts a 48-entry cache. Success is
p50 <5 ms for the hot panel and continuously available health; duration must
be measured rather than assumed.

## Ranked conclusion

The engine demonstrates 0.287–0.738 ms hot bounded responses. Between that and
consistent millisecond service are: the true default that disables caching;
a 48-entry exact-value cache with roughly 996–1192 ms misses; uncached
841–3563 ms surfaces; permit waiters consuming HTTP workers; and an unmeasured
restart interval. Defaulting totals off has the clearest measured effect and
does not weaken the two-scan safety bound. No proposal was implemented here.
