# Eight request workers deployed, 2026-09-11

Joe authorized bringing futon1b back with eight workers after the synthetic
saturation experiment. Commit `a1ebfa1` changes the default HTTP request pool
from four to eight. Queue capacity remains 16, database permits remain four,
and heap/direct-memory limits remain unchanged.

Validation before restart: clj-kondo zero warnings/errors; workspace Emacs
parentheses check OK; server namespace compiled under `:server`; executor test
passed 1 test/7 assertions; source diff whitespace check passed.

The canonical detached restart script ran as
`futon1b-restart-eight-workers-20260911.service`, using its full-census mode.
It passed the backlog check and restarted `futon1b-zone.service` from PID
764140 to 2166403. HTTP returned after about 60 seconds. All seven reported
table counts matched before and after. See the unchanged `restart-receipt.txt`.
The initial read-only write probe saw no post-restart timestamp; its result
is retained rather than rewritten. `fresh-writes.txt` records the follow-up.

`health.edn`, captured from the independent port 7072, confirms eight request
workers, zero queued requests, four available database permits, a 4096 MiB
maximum heap and an open node. These values were parsed and asserted with EDN.

The read-only `timeout 60 python3 scripts/fts-status.py` check exited zero:
store evidence 250160, index rows 250158, delta +2; sidecar attached, periodic
catch-up active, zero indexing errors, last-error none. This is a small remaining
index lag, not a claim of complete search coverage. No forced rebuild or
synthetic evidence write was performed.

`apm-hold.edn` rechecks both V2 and V3 as disabled after the restart. Neither
APM loop was resumed. No futon3c restart was performed.
