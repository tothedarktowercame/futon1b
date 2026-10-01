# Drawbridge in futon1b (:6769)

The serving JVM (`futon1b-zone.service`, :7073) carries a Drawbridge endpoint on
**127.0.0.1:6769** since 2026-09-26 (`futon1b_drawbridge.clj`, commit `f68716b`).
It is a port of futon3c's `:6768` endpoint, and **futon3c's
[README-drawbridge.md](../futon3c/README-drawbridge.md) is the manual**: the two
profiles, the eval contract, the reload-safety contract, the Emacs/CIDER
connection, the "every runtime exception reads as a syntax error" note, and the
raw curl forms all apply here unchanged. This file only lists what differs.

| | futon3c | futon1b |
|---|---|---|
| port | 6768 | **6769** |
| token | `futon3c/.admintoken`, `FUTON3C_ADMIN_TOKEN` | `futon1b/.admintoken` (same value), `FUTON1B_ADMIN_TOKEN` |
| port variable | `FUTON3C_DRAWBRIDGE_PORT` | `FUTON1B_DRAWBRIDGE_PORT` (0 disables; set in the zone unit) |
| CLI | `scripts/proof-eval.sh` | `scripts/futon1b-eval.sh` (same forms: a string, `-f file`, `-` for stdin) |
| eval log | `/tmp/futon3c-eval.log` | `/tmp/futon1b-eval.log` |
| started by | dev bootstrap | `-main` only; `start-server!` (tests, embedded nodes) never starts it |
| sandbox ns | `repl.eval-sandbox` | `futon1b-drawbridge.eval-sandbox` |

## What you reach

- the open node: `@futon1b-server/!node`
- the query helpers: `futon1b-xt/safe-q`, `futon1b-xt/timed-q` (JDBC deadline),
  `futon1b-xt/pq` (**always** parameterise; see its docstring for why inlined
  literals cost metaspace), `futon1b-xt/hydrate-by-ids`
- the route implementations: `futon1b-graph/entities-query`, `futon1b-evidence`, …

```
cd ~/code/futon1b
scripts/futon1b-eval.sh '(count (futon1b-xt/safe-q @futon1b-server/!node (futon1b-xt/pq (quote [p-type]) (quote (-> (from :entities [xt/id]) (where (= entity/type p-type)))) "mission")))'
scripts/futon1b-eval.sh -f /tmp/form.clj
```

Timing a statement where it runs is what this was installed for; the first
such measurement is `TN-entities-route-cost-2026-09-26.md`.

## What is different about this JVM

- **It owns the store.** XTDB 2 local stores are single-process; nothing you
  evaluate here may open `--store-dir`, and a wedged eval thread holding the
  request executor's permits is a wedged server. Prefer read-only forms; the
  same `refresh` and `shutdown-agents` refusals as futon3c apply.
- **Reload-safety is narrower.** `(require 'futon1b-graph :reload)` redefines
  route implementations in place and the running `HttpServer` picks them up
  through their Vars. Anything touching the node, the executors, the
  semaphores, or `start-server!` needs `scripts/restart-futon1b-detached.sh`
  (run through `systemd-run`, see the script header), never a reload.
- **Heap is shared with the store.** A form that materialises a whole table
  (`[*]` under a type predicate, an all-time history) competes with the
  serving path for the 4 GB heap; the eval timeout is 5 minutes, and a client
  that gives up earlier leaves the thread running.
