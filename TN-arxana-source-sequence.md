# TN — Arxana source sequence moves during boot replay, 2026-11-17

**Status:** outage and fix, 2026-11-17 23:03–23:13Z. futon1b-zone (:7073,
`migration-store-21`) failed to restart three times, and was down from 23:04:07Z
until about 23:11Z. Fixed by waiting for XTDB log replay before the boot-time
Arxana index rebuild. The fix was not committed when this note was written.

## Symptom

`restart-futon1b-detached.sh` passed both pre-checks (backlog 14,297 bytes,
`restart_safe=true`; full census), restarted the unit, and then waited out its
300 s budget with `STORE DID NOT RETURN`. The journal showed every start dying
about 8 s after JVM launch:

```
Execution error (ExceptionInfo) at futon1b-arxana-index/rebuild! (futon1b_arxana_index.clj:116).
source moved during rebuild
```

systemd restarted it twice, then stopped with "Start request repeated too
quickly". Both error reports carried the same data,
`{:before 44271, :after 44353}`.

## Cause

Since `a84ddc1` (M-formal-arxana C3a), `start-server!` opens the Arxana index
and calls `arxana-index/rebuild!` straight after `zm/open-store`.
`rebuild!` reads `arxana/source-sequence` before and after collecting source
records, and refuses if they differ. That check is correct: a rebuild over a
moving source would write an index inconsistent with its own watermark.

`source-sequence` reads the `arxana_index_state` row, which advances in the same
transaction as every indexed Arxana record. A freshly opened XTDB 2.1 node
replays, in the background, the log written since its last flushed block. Until
that replay reaches the log head, queries see the sequence climbing, here by 82
in the few seconds the scan took. No client was writing: :7073 was not yet
listening and `store-guard` had confirmed no other futon1b process held the
store. Both attempts replayed the same log tail and failed with the same numbers.

The restart script's backlog pre-check does not cover this. It measures the
*old* node's processed offset against the log size, and that is zero lag.
What the new node has to replay depends on the last flushed block, not on that
offset.

## Fix

`futon1b_server.clj`, `await-log-replay!` (line 1651), called just before the
rebuild (line 1691). It polls `xt/status` every 500 ms until, for every
database, each partition's `:latest-processed-msg-ids` entry has reached its
`:latest-submitted-msg-ids` entry. It throws after 10 minutes. In XTDB 2.1.0
these ids are vectors per log partition, for example
`{"xtdb" [42021698470]}`, not plain numbers; checked on an in-memory node before
the restart. `rebuild!`'s own "source moved" check is unchanged.

The other caller, the `:rebuild` query on the Arxana route (line 1146), runs only
on a serving node, after replay. A concurrent write there still makes the
rebuild refuse, which is the intended behaviour.

## Verification

- check-parens OK; clj-kondo 0 errors, 0 warnings on `futon1b_server.clj`.
- `await-log-replay!` returned at once on a live in-memory node.
- After `systemctl --user reset-failed futon1b-zone` and `start`, the journal
  printed `[startup] log replay complete {:latest-submitted-msg-ids {"xtdb"
  [42021698470]}, :latest-processed-msg-ids {"xtdb" [42021698470]}}` at
  23:10:55Z. The memory projection then built in 124 s, `/health` reported
  `:node-open? true`, and the entities and evidence-text sidecars reported
  `:serving? true`.
- The futon1b test suite was not run.

## Follow-up (same day)

Both open items from the first version of this note are now done:

- `await-log-replay!` is called directly after `zm/open-store` in
  `start-server!`, before the Arxana rebuild, the memory projection and the
  sidecar catch-ups. Every boot step that reads the store now sees it after
  replay, not only the Arxana rebuild.
- `restart-futon1b-detached.sh` waits up to `FUTON1B_RESTART_UP_BUDGET_S`
  (default 600 s, was a fixed 300 s) for `/health`. It stops early, with a
  separate message, if systemd has put the unit into the `failed` state. A slow
  healthy boot gets more time, and a crash loop is reported as soon as systemd
  gives up instead of after the full wait.

Neither change has been exercised by a real restart yet. The next restart
should log `[startup] log replay complete` before `[memory-projection]`, and
the receipt should report `STORE UP`.
