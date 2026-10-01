# PROTO: Neo4j sidecar prototype, measured — 2026-09-26

Excursion E-kimi-task-47, reviewer claude-12. Companion to kimi-5's SQLite-sidecar note
(E-kimi-task-46): same source data, same six queries, so the numbers compare directly.
Throwaway prototype; no futon1b server code touched. Everything lives under
`/tmp/neo4j-proto/` (left in place; server **stopped**).

## 1. Setup

- Neo4j **Community 5.26.0** (LTS), tarball `neo4j-community-5.26.0-unix.tar.gz` from
  dist.neo4j.org. SHA-256 verified against the published `.sha256` file:
  `ad8ac3398606145502b8f489530bbd39333707ae4668156af16b6086b5b037d7` (measured: match).
  Runs on the system Java 21.0.11.
- Unpacked to `/tmp/neo4j-proto/neo4j`. Nothing under `~/code`, no sudo, no system install.
- Config lines appended to `conf/neo4j.conf` (everything else default):

  ```
  server.default_listen_address=127.0.0.1
  server.bolt.listen_address=127.0.0.1:7687
  server.http.listen_address=127.0.0.1:7474
  server.memory.heap.initial_size=1g
  server.memory.heap.max_size=4g
  server.memory.pagecache.size=4g
  ```

  Ports 7687/7474 were free (checked with `ss -tlnp` first). Initial password set with
  `bin/neo4j-admin dbms set-initial-password` (password only in `/tmp/neo4j-proto` scripts,
  not committed).
- Access: HTTP transactional endpoint `/db/neo4j/tx/commit` via Python urllib. No pip
  installs. Scripts: `/tmp/neo4j-proto/parse.py`, `load.py`, `queries.py`.
- Load method: batched `UNWIND` (5,000 rows/batch) from CSVs produced by a regex EDN parse
  (`neo4j-admin database import` not used — it requires the DBMS offline and the HTTP path
  was already fast).
- **Load time: 6.7 s wall total** (measured): constraints+indexes 0.7 s, commits 1.6 s,
  edits (MERGE commit, MERGE var, CREATE rel) 3.7 s, superpod 0.4 s. ProofNode reload after
  a parse fix: 0.8 s.
- **Store size on disk: 24 MB** in `data/databases` after load (measured; a transient 515 MB
  of transaction logs accumulated during the batched load was checkpointed away).
- **RSS of the server after load: ~1.4–1.6 GB** (measured; dominated by JVM heap floor +
  page-cache mapping, not by this tiny dataset).

### Indexes created

```
CREATE CONSTRAINT commit_sha   FOR (c:Commit)    REQUIRE c.sha IS UNIQUE
CREATE CONSTRAINT var_id       FOR (v:Var)       REQUIRE v.id IS UNIQUE
CREATE CONSTRAINT paper_id     FOR (p:Paper)     REQUIRE p.id IS UNIQUE
CREATE CONSTRAINT passage_id   FOR (p:Passage)   REQUIRE p.id IS UNIQUE
CREATE CONSTRAINT scope_id     FOR (s:Scope)     REQUIRE s.id IS UNIQUE
CREATE CONSTRAINT proofnode_id FOR (n:ProofNode) REQUIRE n.id IS UNIQUE
CREATE INDEX scope_kind        FOR (s:Scope)  ON (s.kind)
CREATE INDEX scope_la          FOR (s:Scope)  ON (s.la)
CREATE INDEX commit_repo       FOR (c:Commit) ON (c.repo)
```

(Unique constraints back Q1/Q3/Q4 lookups; `scope_kind` backs Q5; `scope_la` was created
for Q4/Q6 but the planner preferred label scans at this size — the dataset is too small for
range indexes to matter.)

## 2. Data loaded and row counts (checked against the sources)

Model: `(:Commit {sha,ts,repo,subject})-[:EDITS]->(:Var {id})`;
`(:Paper)-[:IN_PAPER]-(:Passage {la,lb})-[:IN_PASSAGE]-(:Scope|ProofNode {kind,la,lb})`.

| Node/rel   | Loaded  | Independent count from source files | How |
|---|---|---|---|
| Commit     | 18,479  | 18,476 | `grep -o ':hx/endpoints \["[0-9a-f]\{40\}"\]' /tmp/v05-cache/commit/page-*.edn \| wc -l` → 18,476. The extra **3** are placeholder Commit nodes MERGEd for 396 edit rows whose shas lie outside the 19 cached commit pages (flagged by the parser). |
| EDITS rel  | 160,000 | 160,000 | `grep -oh '"dir:'` gives 320,000 = 2× (the string appears in both `:hx/id` and `:hx/endpoints`); /2 = 160,000. Also exactly 1,000 × 160 pages. |
| Var        | 30,923  | — | Distinct var ids across edit edges (set size during parse; no simpler grep cross-check). |
| Paper      | 13      | 12 expo + 1 graphs-only | `grep -h ':paper/id' expo/*.edn \| sort -u \| wc -l` = 12. |
| Passage    | 399     | 277 expo + 122 graphs | file counts (277 + 243 graph files, of which 122 non-rung2 define passages). |
| Scope      | 857     | 857 | `grep -oh ':kind :[a-z-]*' expo/*.edn` = 1,134 minus 277 passage-level `:source :kind` = 857. |
| ProofNode  | 1,274   | 1,274 | `grep ':id :n[0-9]*'` over non-rung2 graph files = 1,274. (All-files grep gives 2,810; the excess 1,536 are node *references* inside `.rung2.edn` semcheck reports, not node definitions. My first parse regex silently missed nothing here — the rung2 files were the trap.) |

Note: the packet estimated 1,111 expo files; the directory actually holds **277** (measured).
The superpod side of this prototype is therefore small; extrapolation in §5 covers the
intended scale.

## 3. Query table

Sample parameters: sha `304beb6bb43f51ceae0f297cdb40a3741a8e1214` (the commit with the most
edits, 1,196 vars); paper `0705.0102`, lines [226, 240]; kind `connection`; proof node
`0705.0102:proof0:L349-366#n1`. Times are HTTP round-trip including JSON encode/decode from
Python (perceived client latency), in milliseconds. **Cold** = first run after a
`neo4j restart` (page cache cleared); **warm** = immediate repeat. All numbers **measured**,
single runs (no averaging).

| # | Query | Cypher (abridged) | Cold ms | Warm ms | Rows |
|---|---|---|---|---|---|
| Q1 | one commit's edits | `MATCH (c:Commit {sha:$sha})-[:EDITS]->(v) RETURN v.id` | 195.4 | 25.4 | 1,196 |
| Q2 | edit counts per repo | `MATCH (c:Commit)-[:EDITS]->(v) RETURN c.repo, count(*)` | 127.6 | 103.4 | 14 |
| Q3 | commits sharing vars (two-hop) | `MATCH (c:Commit {sha:$sha})-[:EDITS]->(v)<-[:EDITS]-(o:Commit) WHERE o.sha<>$sha RETURN o.sha, count(DISTINCT v) ORDER BY 2 DESC LIMIT 25` | 186.5 | 14.5 | 25 |
| Q4 | scopes of paper P overlapping [a,b] | `MATCH (p:Paper {id:$pid})<-[:IN_PAPER]-(pa)<-[:IN_PASSAGE]-(s:Scope) WHERE s.la<=$b AND s.lb>=$a RETURN s...` | 145.9 | 11.8 | 7 |
| Q5 | all scopes of kind K | `MATCH (s:Scope {kind:$kind}) RETURN s.id LIMIT 1000` | 58.6 | 15.8 | 693 |
| Q6 | proof node → scopes over same span | `MATCH (n:ProofNode {id:$nid}) MATCH (s:Scope) WHERE s.la<=n.lb AND s.lb>=n.la AND s.id STARTS WITH $paper RETURN s.id` | 235.5* | 12.6 | 0 |

\*Q6 cold re-measured after the ProofNode reload (235.5 ms); the first cold run before the
reload was 91.4 ms. 0 rows is correct: no expo scope overlaps proof-node n1's line 350 in
paper 0705.0102 (scopes for that paper span lines 224–256).

Reading: at this scale everything is tens of milliseconds warm; cold penalty is ~5–10× and
is mostly JVM/classloading and page-cache priming, not disk (24 MB store). Q3 (the query
where a graph store should shine) is 14.5 ms warm for a 1,196-var commit — the equivalent
SQL self-join number belongs in kimi-5's note for the head-to-head.

## 4. What Neo4j cannot do that the futon1b design depends on

1. **Bitemporality (valid-time / system-time as-of reads).** Neo4j has no time dimensions
   at all; properties are current-value only. *Workaround:* model time explicitly — either
   timestamp properties on every node/rel plus application-side filtering (loses "as-of"
   semantics for anything you forgot to stamp; queries become `WHERE t.valid_from <= $t AND
   ...` everywhere, and indexes don't compose well on two time dimensions), or
   time-slice nodes (one node per version, `:SUPERSEDES` chains — the classic graph
   versioning pattern; costs ~2× nodes and makes every traversal version-aware). Either way
   the bitemporal *guarantee* (immutable history, as-of correctness) moves from the store
   into application code. Cost: ongoing, and unaudited.
2. **Candidate-ID re-check against XTDB.** Not a Neo4j capability gap per se — it's a
   cross-store join: sidecar answers must be validated against XTDB before use, which is a
   network hop and a consistency window. Same cost as the SQLite sidecar; no worse, no
   better. *Workaround:* none needed beyond what the futon1b design already specifies.
3. **Rebuild from the write log.** XTDB's log is the source of truth and the sidecar must
   be a deterministic, disposable projection. Neo4j supports this fine operationally (drop
   db, replay — our whole load took 6.7 s), *but* Community edition has no online backup,
   no CDC, and no incremental subscription API — you re-derive by polling XTDB yourself,
   exactly as SQLite would. Cost: equal to SQLite; the difference is you also own a second
   JVM's failure modes while doing it.
4. **Second server process.** This is the real structural cost vs SQLite. Measured here:
   1.4–1.6 GB RSS idle-ish after load (floor set by JVM + page cache), a systemd/unit
   lifecycle to manage, ports/firewall to babysit, password management, version upgrades,
   and a cold-start penalty (our restart→ready was ~12 s plus ~5–10× query slowdown until
   warm). SQLite's comparable footprint is a file and a library call. *Workaround:* none;
   it's intrinsic to client-server stores.

Net: Neo4j buys native two-hop traversal ergonomics (Q3 reads better in Cypher than as a
SQL self-join) at the price of a permanent second process and zero help with bitemporality —
the thing the futon1b design most depends on. For this workload the graph model is a
convenience, not a capability.

## 5. Extrapolation (estimated)

- **Scopes ×600 (5,000 papers):** ~514k scopes, ~400k passages, ~760k proof nodes —
  still small. Warm query times should be roughly flat for indexed lookups (Q1/Q4/Q5);
  Q4/Q6 span-overlap filters scale linearly with scopes-per-paper unless a range index
  bites, so expect low-single-digit-ms growth. Store ≈ a few hundred MB (estimated from
  24 MB at 857 scopes, dominated by the 160k EDITS rels; scope data adds ~1–2 KB/scope).
- **All 551,794 v05 edits (3.45×):** EDITS rels 160k → 552k, commits ~18.5k → ~? (full
  history), vars maybe 60–100k. Load scales ~linearly: ≈ 15–20 s via batched UNWIND
  (estimated; offline `neo4j-admin import` would be ~5 s but needs downtime). Store
  ≈ 60–80 MB. Q3 cost scales with the max vars-per-commit × commits-per-var — the 1,196-var
  commit already stresses it; expect warm Q3 in the 30–80 ms range (estimated). RSS
  unchanged in practice (heap-bounded).
- Neither extrapolation changes the conclusion: at 10× this size the numbers are still
  comfortably interactive, and the costs in §4 are all flat or worse.

## Constraint compliance

- Read-only against futon1b :7073 — never contacted.
- Server **stopped** (`bin/neo4j stop`, verified); `/tmp/neo4j-proto` left in place.
- This note is the only file committed, with an explicit path.

## Review: side by side with the SQLite prototype (claude-12, 2026-09-26)

Same source files; SQLite numbers from `/tmp/hx_proto.db` (DESIGN-hyperedge-scope-sidecar
§5), warm, measured. SQLite two-hop measured by claude-12 on a copy with two indexes added
(`(hx_id, pos)`, `(endpoint, pos, hx_id)`; 0.35 s to build) — the prototype had no index
on `hx_id`, and without one the query did not finish in 2 minutes.

| Query | Neo4j warm (cold) | SQLite warm |
|---|---|---|
| one commit's edits | 25 ms (195) | <0.1 ms |
| edits per repo | 103 ms (128) | 4.9 ms |
| two-hop: commits sharing vars with 304beb6 (1,196 vars) | 14.5 ms (187) | 2.7 ms |
| scopes of a paper overlapping a span | 12 ms (146) | <0.1 ms |
| scopes of one kind | 16 ms (59) | <0.1 ms |
| load | 6.7 s over HTTP | 1.8 s |
| on disk / resident | 24 MB / 1.4–1.6 GB server RSS | 117 MB file / in-process |

Loaded rows differ: Neo4j loaded all 160,000 edits and 857 scopes, matching the sources;
the SQLite prototype loaded 152,828 edits and 772 scopes (its parser dropped kinds with
`/`, and ~7k edit rows). 304beb6 has 1,196 edits in Neo4j and 1,195 in SQLite. The loss
does not move the times by more than it moves the row counts.

Expo file count: 277 top-level `expo/*.edn`; the packet's 1,111 counted `.attempts/` too.
Q6 returned 0 rows for its one sample, so it shows the query runs, not that the join finds
anything; SQLite's Q9 sample returned 6.

Reading: at this data size SQLite answers every query faster, including the two-hop query
a graph store is built for, once the index exists. Neo4j adds a second server to run and
does not provide as-of reads, the candidate re-check against XTDB, or rebuild from the
store — all of which the futon1b sidecar needs. Neither result says anything about
queries of 3+ hops or variable-length paths, which neither prototype ran.
