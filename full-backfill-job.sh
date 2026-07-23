#!/bin/bash
# Full hyperedge backfill: laptop futon1a (temp, read-only) -> live futon1b :7073.
# Runs detached under systemd-run (survives session). Log = logs/full-backfill.log.
set -uo pipefail
cd /home/joe/code/futon1b
LOG=logs/full-backfill.log
exec >> "$LOG" 2>&1
echo "=== FULL BACKFILL START $(date -u +%FT%TZ) ==="

# 1. Temp read-only futon1a on :7071 (child process; dies with this unit)
(cd /home/joe/code/futon1a && \
 FUTON1A_DATA_DIR=/home/joe/code/storage/futon1a/default \
 FUTON1A_PORT=7071 FUTON1A_ALLOW_EMPTY_PENHOLDERS=true \
 exec clojure -M -m futon1a.system) &
F1A_PID=$!
trap 'kill $F1A_PID 2>/dev/null' EXIT
for i in $(seq 1 60); do
  curl -s -m 3 http://127.0.0.1:7071/health > /dev/null && break; sleep 5
done
curl -s -m 5 http://127.0.0.1:7071/health > /dev/null || { echo "FATAL: futon1a did not come up"; exit 1; }
echo "futon1a up (pid $F1A_PID)"

# 2. Export (proven S1 pipeline; hyperedges via server-side snapshot)
clojure -M:node -m migration.export --output-dir migration-export-full --base-url http://127.0.0.1:7071 \
  || { echo "FATAL: export failed"; exit 1; }
echo "export done $(date -u +%FT%TZ)"

# 3. Ingest hyperedges into the LIVE :7073 via POST (server does transform; idempotent no-op guard)
bb full-backfill-ingest.bb migration-export-full
echo "=== FULL BACKFILL END $(date -u +%FT%TZ) ==="
