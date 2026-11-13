# Mission: M-notes-into-memory — ~/notes into memory that FUTON can share

**Date:** 2026-11-13
**Status:** HEAD (operator anchor and findings recorded; IDENTIFY not started)
**Owner:** claude-9 with Joe
**Home:** futon1b/holes/ (minus40's store is futon1b since 2026-11-12; a futon1c refactor is a
possibility, below)

Cross-refs:

- minus40 (`~/code/minus40`, now `github.com/tothedarktowercame/minus40`, private, pushed
  2026-11-13): memory files with history, as-of queries, near-duplicate checks; Python hooks
  and MCP server plus some Clojure; store = futon1b :7073 via `FUTON_MEMORY_BASE` (commits
  0253525, 3aaf95b).
- `futon3c/holes/missions/M-ways-of-working.md` (claude-5): ways of working need probes that
  can fail; "agent artifacts do not live in ~/code" and "durable notes have a backup" are
  candidate ways.
- `~/notes/claude-9/cleanup/CODE-CLEANUP-PLAN.md`: the ~/code inventory of the same day; it
  leaves ~/notes to this mission.
- Workspace `~/code/CLAUDE.md`, "Scratch work goes in /tmp, which is in RAM" (2026-11-13):
  durable notes go to `~/notes/<agent>/` for now, which is the rule this mission replaces.

## HEAD — operator anchor

Joe, 2026-11-13: "minus40 is PKM software for FUTON, for which '~/notes' is a preceding
simulacrum."

Earlier the same night: "~/notes/ is a liability … and actually should be broken down into
Minus40°DB memories (which are shared with other agents and subject to further optimizations)
but that is a mission-scale project not a one-off clean up." And: "I'd see it as a futon1b
feature or possibly futon1c (if that doesn't exist yet), possibly warranted also by a XTDB
2.2.0 switchover which is otherwise unrelated but still interesting/relevant."

## Findings so far (claude-9, 2026-11-13)

**1. ~/notes is several kinds of thing, and only one kind is memory** (Joe: "the key finding").
1,361 top-level entries, 24 GB:

| Kind | Examples | Proper home |
|---|---|---|
| Facts and decisions | rulings, findings, "X works like Y", per-agent notes | minus40 memories (shared, deduplicated, with history) |
| Packets (dispatch texts) | `packets/` (311, 127 MB) | a record store linked to Agency jobs and commits; they are records of work sent, not memories |
| Ledgers, plans, drafts | `euclid-overnight-ledger.md`, `claude-9/expository/*`, `claude-21/gold/*` | the repo of the work they describe, with a memory pointing at them |
| Run output and logs | 189 `*-analysis.json`, 261 top-level `*.log`, `admit-*`, `acceptance-click-*.out` | `~/storage/`, or delete |
| Per-agent working directories | 21 (`claude-21` 7.9 GB, `codex-1` 329 MB, `claude-7`, `codex-7`, …) | a mixture of the above; sorted item by item |

**2. No backup.** ~/notes is not a git repository, and none of the timed backups
(`zone-git-backup` via futon-clean-worker, `evidence-backup.sh`, `zone-site-backup`) covers it.
Its only copy is on Zone's disk. (The ~/code inventory found the same gap for minus40 itself,
closed the same night by pushing it to GitHub.)

**3. Used only by agents.** Written by Claude agents and by Codex agents working to Claude
packets; read by agents. Joe does not use it. So nothing in it is reachable through Joe's views
(terminal, web), and nothing in it reaches other agents except by path in a packet.

## What's already felt to be true

- minus40 already does the hard part for memories: history, as-of queries, near-duplicate
  detection across projects, a SessionStart brief. Notes moved into it gain those for free.
- The typology means the mission is a sorting problem with five destinations, not a bulk
  import.

## Anti-glibness discipline

- A note "moved into minus40" counts only if a memory can be found by `m40_find` from a fresh
  session and states something the note said, with a pointer back to the source document if
  there is one.
- No bulk conversion: a converter that turns 1,361 files into 1,361 memories would recreate the
  simulacrum inside the store. Every memory must pass minus40's near-duplicate check.
- Nothing is deleted from ~/notes until its kind has a home that is backed up.

## Working-economy position

Underwrites: shared memory across agents (today an agent's notes are invisible to the next
agent unless a packet names the path); Joe's ability to see what agents know; the ways-of-working
probes. Underwritten by: futon1b (store), minus40 (client), the backups.

## Carried-forward tensions

- **futon1b feature or futon1c?** minus40's client code (Python plus some Clojure) could fold
  into futon1b, or a futon1c could own PKM over futon1b's store. Possibly coupled to the XTDB
  2.2.0 switchover (futon1b `M-xtdb-22x-benchmarking.md`), otherwise unrelated.
- **Packets** need a store of records, not of memories. Agency's job ledger holds job ids and
  results but not the packet texts; where the texts live is open.
- **Documents vs memories.** Plans and ledgers belong in repos; deciding which repo, for notes
  that span several, is a judgment per item.
- **Agent-private vs shared.** Some notes are an agent's scratch; whether they become shared
  memory or are dropped needs a rule.
- **The interim rule.** CLAUDE.md now sends durable notes to `~/notes/<agent>/`; that continues
  to grow the liability until this mission gives a better home.

## First measurement (for IDENTIFY)

Sample 50 ~/notes items at random (seeded), classify each into the five kinds, and for the
"facts and decisions" ones extract candidate memories and count how many minus40's near-duplicate
check already holds. That gives the size of the real work: how many new memories ~/notes
contains, against how much is records, documents and logs.

## Step 0 (done)

- minus40 pushed to `tothedarktowercame/minus40` (private), 128 commits, bytecode caches
  untracked (2026-11-13). Not yet pushed on every commit; continuous push is open.

## Provenance

HEAD written by claude-9 on 2026-11-13 from the night's conversation with Joe (the ~/code
cleanup, the /tmp rule, minus40's backup) and a read-only survey of ~/notes. Operator quotes
are verbatim.
