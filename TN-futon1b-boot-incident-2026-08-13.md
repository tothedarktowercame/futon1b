# TN-futon1b-boot-incident-2026-08-13

**Technical note — the :7073 store could not boot for 36 minutes. Three
latent defects, all in our code, all exposed by an operator restart.**
Author: claude-2. Date: 2026-08-13. Companion to
`TN-futon1b-memory-incident.md` (different organ: that one is memory
pressure in a *running* node; this one is a *booting* node that never gets
to run).

## Incident summary (2026-08-13, times UTC)

At **10:45** the operator restarted `c7-futon1b.service` to load a one-line
change. It did not come back. Five start attempts over ~36 minutes failed
in three distinct ways before the store served again at **11:21**.

No data was lost. Evidence count was **140,520** before the restart and
**140,520** after. Nothing was corrupted; nothing needed rebuilding.

**The restart was the trigger, not the cause.** All three defects were
already present and would have detonated on the next restart for any
reason — a reboot, an OOM, a deploy. What the restart did was convert three
latent faults into an outage, and it was avoidable: nobody had checked that
the service *could* restart, and the fix being loaded did not need to ship
that hour.

## Baseline vs incident

| | before | during |
|---|---|---|
| evidence count | 140,520 | unreachable |
| `agency/loud-failure` entity read | **500** | 500 → **200 after fix 1** |
| `hyperedges?type=pattern/library` | **500** | 500 → **200 after fix 1** |
| service state | active (serving) | failed ×5 |
| peak RSS | — | 14.1 G (of 249 G) |

Note the first two were **already 500 while the service was up**. That was
the symptom that started the investigation; the restart was an attempt to
fix it.

## Root cause, layered

### Fix 1 — `respond!` round-tripped every response through the EDN reader

`futon1b_server.clj`. Every route calls `(respond! ex 200 (pr-str {...}))`,
and `respond!` did:

```clojure
(let [v (if (string? body) (edn/read-string body) body)] ...)
```

So each response was serialized and immediately re-parsed. A no-op when it
works; a 500 when `pr-str` emits something the reader cannot read back —
`#xt/instant` ("No reader function for tag xt/instant"), `#object[...]`, and
at least one shape reporting "Map literal must contain an even number of
forms".

**Consequence: correctly STORED documents became unreadable, and the reader
threw before the caller's value was ever sent — so it also masked whatever
the real error was.** Three `agency/*` entities and the whole
`pattern/library` type index were affected. They were intact the entire
time; only the response path was broken.

A string body is now written straight out on the EDN path. The JSON path
still parses because it needs a data structure to encode — that is the
residual. The full fix is for callers to pass the map and drop their own
`pr-str` (~30 call sites), not attempted.

### Fix 2 — the boot gate allowed 5 attempts against an indexing backlog

`futon1b_graph.clj`. `build-memory-projection` reads a watermark, builds,
re-reads, and requires the two to be **equal**; a moving watermark burns an
attempt, and the fifth throws
`:memory-projection-source-not-quiescent`.

That is fine when the store is caught up and fatal when it is not. ~185 MB
of a 4.29 GB `log/LOG` was still being indexed, so every attempt saw a
different watermark. Five attempts is **~56 seconds**; the backlog needed
about **six and a half minutes**. The node could never reach the state its
own boot gate demanded.

A *running* node tolerates a moving watermark — this gate only runs at boot,
which is why the store had been serving happily all day with the condition
latent underneath it.

`FUTON1B_PROJECTION_BUILD_ATTEMPTS` now overrides the count; default stays 5.

### Fix 3 — `safe-q` rethrew a retryable pgwire conflict

`futon1b_xt.clj`. XTDB materializes hyperedge props as `prop$*` **columns**,
so ingesting documents with previously unseen prop keys widens the table and
invalidates any cached plan against it — continuously, while a backlog is
indexing. A half-finished migration had written ~9,466 `pattern/clause`
records with new prop keys.

`xtdb.error.Conflict: cached plan must not change result type` is the
standard signal to re-execute, which re-prepares. We rethrew it, so a
transient condition became fatal in a startup query (76 prepared columns).
Now retried, bounded at 5 attempts with linear backoff so a genuinely
persistent conflict still surfaces.

## The diagnostic trap, and it cost the most time

**`latest-submitted-msg-ids` and `latest-processed-msg-ids` are BYTE OFFSETS
into `log/LOG`, not message counts.**

```
log/LOG on disk            4,290,616,815 bytes
latest-submitted-msg-ids   4,285,952,253
latest-processed-msg-ids   4,100,532,086
```

Read as counts, the gap looks like **185 million messages** against an
observed drain rate of ~17k/min — i.e. ~180 hours, "unrecoverable, consider
rebuilding from scratch". Read correctly it is **185 MB of log**, which
drained in about six minutes once the node was allowed to run.

That single misreading turned a routine backlog into an apparent
catastrophe and drove an unnecessary escalation. If a future reader takes
one thing from this note, take that: **check whether the counter is a count
or an offset before extrapolating from it.** The store directory tells you
in one command — `du -sb log`.

## Falsified along the way — do not re-diagnose these

- **"Rich prose props corrupt readback."** No. Evidence rows, hyperedge
  props and entity props all round-trip unbalanced braces and bare dates at
  200, as do `{:a 1 :b`, `{:unbalanced`, `[:vec :missing`, `#{:set` and
  inline dates. Prose was innocent; the response path was not. This
  diagnosis was stated confidently in a job report and repeated by the
  operator before anyone tested it.
- **Memory pressure / cgroup limits** — the failure mode of
  `TN-futon1b-memory-incident.md`. Not this. Peak RSS 14.1 G against 249 G
  available, `MemoryHigh=infinity`, `MemoryMax=infinity`, `oom_kill=0`.
  Raising the heap from 8 G to 64 G changed *which* error we hit, not
  whether we hit one. The historical notes were a red herring here because
  they were written on a much smaller box.
- **The multi_watcher flooding the log.** It was running throughout, and it
  is a quiet writer: 19 events in 25 minutes, `:phase :idle`, no errors. It
  was stopped during recovery as a precaution and restored afterwards.

## Diagnostic signature of a recurrence

Service `failed` shortly after start, and the journal shows one of:

- `futon1b-gates/layered-error … :memory-projection-source-not-quiescent`
  with `:source-watermark` ≠ `:observed-watermark` → indexing backlog;
  raise `FUTON1B_PROJECTION_BUILD_ATTEMPTS` and let it run. Check the gap
  with `du -sb migration-store-21/log` against the reported offsets.
- `xtdb.error.Conflict: cached plan must not change result type` → the
  `prop$*` column set is moving; expected during/after a bulk ingest of
  documents with new prop keys. Should now self-heal via `safe-q` retry.
- A 500 on a *read* of a document you have good reason to believe exists,
  with a **reader** message (`No reader function for tag …`, `Map literal
  must contain …`) → suspect the response path before suspecting the data.

## Already fixed — do NOT redo

- `respond!` no longer round-trips EDN responses (commit `4f4c13f`).
- `FUTON1B_PROJECTION_BUILD_ATTEMPTS` override (commit `e74c143`).
- `safe-q` retries the cached-plan conflict (commit `2279e94`).

## Still open

1. **`respond!`'s JSON path still parses.** A JSON-requested response whose
   EDN does not read back will still fail. The real fix is callers passing
   maps instead of `pr-str`, ~30 sites.
2. **The boot gate is a retry loop, not a wait.** Retrying an expensive
   projection build 500 times is a blunt instrument; waiting for indexing
   quiescence and then building once would be correct. Not attempted on a
   downed substrate.
3. **The half-migration remains.** ~1,093 pattern records and ~9,466
   `pattern/clause` records coexist with 284 legacy `code/v05/pattern-slot`
   records, and some entity ids changed from UUID to qualified name. That
   reconciliation is separate work; it is what generated the new prop keys
   behind fix 3.
4. **Diagnostic litter in the store:** `e-probe-serialization-claude2-…`,
   `hx-probe-serialization-…`, `hx-probe-shape-1..6`, and entity
   `probe/rich-props`, written while falsifying the prose hypothesis. They
   contributed `prop$probe` to the column set. Retract them.
5. **No pre-restart check exists.** There is no way to ask a running node
   "would you survive a restart?" A cheap version — compare `du -sb log`
   against the processed offset and warn if the gap exceeds what 5 attempts
   can cover — would have turned this outage into a one-line warning.

## Acceptance bar for closing this note

- A deliberate restart of `c7-futon1b.service` under normal load comes back
  without env overrides, and item 5 exists so that the next person knows
  before they try.
