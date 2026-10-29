# TN-slow-repl-turns-2026-10-28

**Technical note: Agency REPL turns were slow because futon1b's 4g heap
was full of XTDB query plans. Restarting it exposed a boot catch-up that
never finished. Separately, the graph Emacs was spending CPU on per-buffer
park polling.**
Author: claude (terminal session, with codex-3 and codex-12). Date: 2026-10-28/29.
Companions: `holes/E-xtdb-plan-cache-heap.md` (the open heap work, with
H1/H2 handoffs), `TN-futon1b-memory-incident.md` (an earlier memory
incident in a running node), `TN-futon1b-boot-incident-2026-08-13.md` (an
earlier boot incident).

## Symptom

Joe: turns sent to Claude on the REPL were slow all day, whatever the
compaction or pouch state. Agency itself (:7070) answered in 30 ms. The
slowness was in what a turn waits on: evidence appends to futon1b (:7073).

## How to find it next time (the checks that led here, in order)

1. `top`: a futon1b JVM at ~2000% CPU is GC, not work.
2. `jstat -gcutil <pid> 1000 3`: old gen at 99%, with full GC count rising
   every second, means the heap is full.
3. `curl localhost:7073/health`: `:heap`, `:holders` (permits held, with
   `:age-ms`), `:evidence-text :serving?`.
4. `journalctl --user -u futon1b-zone --since -1h | grep -c status=504`:
   the evidence POSTs failing with "query permit unavailable after 5000ms".
5. After any futon1b restart: `:evidence-text :serving?` must become
   `true` within minutes. If it stays `false` with
   `:not-reconciled-since-boot`, see part 2.

## Part 1: heap full of XTDB query plans (open)

Evidence before the restart (8 days' uptime):

| | |
|---|---|
| heap | 3,932 / 4,096 MB, old gen 99.5% |
| full GCs | ~2/s; 28,963 in all, 11,800 s |
| 504s | 688 in the last hour, 3,040 in 24 h |
| requests over 3 s | 2,559 of 4,505 in the last hour |

The histogram showed 10.6M Arrow `Field` and 10.1M `FieldType` objects,
4.3M `clojure.lang.Symbol`, and **4,102 Caffeine caches**. XTDB 2.1.0 caches
up to 4096 query plans (`xtdb.query/->query-source`). Each plan carries its
own emit cache of up to 16 compiled operator trees, keyed on the Arrow
schema of the columns it scans. So 4,102 caches means the plan cache was
full. futon1b sends more than 4096 distinct SQL strings because
`_id IN (?, …)` lists of every length are different strings (four call
sites, listed in the E- doc).

Not yet shown: that those plans are what holds ~3.5 GB. The pre-restart
heap dump is kept for that (`~/dumps/futon1b/`).

Done:
- Heap raised to 8g by a systemd drop-in,
  `~/.config/systemd/user/futon1b-zone.service.d/heap.conf`. This buys time
  and fixes nothing.
- `futon1b-heap-sampler.service` logs plan count against heap every 10 min
  to `~/notes/futon1b-heap/samples.tsv`. If plans climb towards 4096 while
  the heap climbs in step, the mechanism is confirmed. Probes:
  `~/notes/futon1b-heap/plan-cache-probe.clj` and `plan-key-probe.clj`, run
  with `scripts/futon1b-eval.sh -f`.

Open: H1 (measure retained size from the dump) and H2 (round IN lists up to
a few fixed lengths) in `holes/E-xtdb-plan-cache-heap.md`.

## Part 2: boot catch-up never finished (fixed, cfc7c68)

After the restart the heap was healthy, but evidence reads still scanned
the store for 20 minutes. The boot `catch-up!` only stopped on an *empty*
page. Each page scan took ~30 s and live appends landed during every scan,
so the checkpoint trailed the head by ~45 s and never caught up.
`reconcile!`, which runs after catch-up on the boot thread, never started.

Fix: catch-up also stops after a *short* page, which proves the same
thing: the scan reached the end of the store as of a snapshot taken after
the basis. The regression test `catch-up-drains-under-live-appends`
(`test_evidence_tag_index.clj`) adds a doc during every scan; without the
fix it hits its cap.

Freeing the loop that was already running: reloading the code does not
replace a loop already executing the old function. After
`(require 'futon1b-text :reload)`, `scan-after` was wrapped to return `[]`
exactly once and then restore itself. The boot loop drained and
`reconcile!` ran (722,810 docs, 2 missing, indexed). This is safe because
the append hook had indexed every append since boot, and `reconcile!`
compares every id anyway.

After the fix: evidence POST median 84 ms, p90 221 ms; tag/session list
reads 2 ms.

**Any restart under load would have hit this.** It is a boot defect, not
part of the heap problem.

Also fixed (fa16f09): `scripts/restart-futon1b-detached.sh` recorded the
wrong old PID, because its `pgrep` also matched minus40's scratch server on
:7093. It now reads the unit's `MainPID`.

## Part 3: Emacs park polling (fixed, futon3c)

This did not cause the slow turns, but it made the graph Emacs laggy.
`agent-repl-park--poll-once` sent two requests (`parked/ready`,
`followups/ready`) per REPL buffer every 3 s. With 20 buffers that is ~13
requests/s, with 240 pending 20 s request deadlines at any moment. A CPU
profile put 68% of Emacs's time in redisplay (433 redisplays/s) and 11%
in GC (0.27 s per pause).

How it was measured, without loading anything into the daemon:
- CPU profile: `profiler-cpu-start`, wait, then aggregate
  `(profiler-cpu-log)` via emacsclient.
- Redisplay rate: count `pre-redisplay-functions` calls for 10 s.
- Who is making requests: group `timer-list` closures by their `url`.

Fix: one `POST /api/alpha/ready/batch` per tick for all seats (futon3c
`4f6bfc3e5` server, `1c3b0640d` client, `eea3db17b` review fix). Each seat
gets exactly what the two GETs return, so busy gate, leases and ACKs are
unchanged. The client falls back to per-buffer polling on a 404. After: 9
pending poll requests with 21 buffers, and 254 redisplays/s. The remainder
appears to be process output from streaming turns, plus the 4/s
`loop-lag--tick` and the session-mode turn-tags refresh timers.

Review finding (eea3db17b): url.el reports a 404 as
`(:error (error http 404))`, so the fallback check placed behind
`(plist-get status :error)` never ran. A client loaded before the Agency
route existed delivered no parks or followups (about 00:20–00:26 on
10-29; queued items were not lost). Test fakes of url.el should pass url.el's real
error status for 4xx/5xx.

Two traps from the same session:
- A stale `emacs/agent-repl-park.elc` made a rerun test old code. Delete
  `.elc` files after byte-compile checks.
- The live Emacs had the new client before anyone was asked to load it.
  Check `(fboundp …)` in the daemon before assuming a change is not yet live.
