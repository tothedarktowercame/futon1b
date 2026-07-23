#!/bin/bash
set -uo pipefail
cd /home/joe/code/futon1b
exec >> logs/full-backfill.log 2>&1
echo "=== PER-TYPE BACKFILL START $(date -u +%FT%TZ) ==="
(cd /home/joe/code/futon1a && \
 FUTON1A_DATA_DIR=/home/joe/code/storage/futon1a/default \
 FUTON1A_PORT=7071 FUTON1A_ALLOW_EMPTY_PENHOLDERS=true \
 exec clojure -M -m futon1a.system) &
F1A_PID=$!
trap 'kill $F1A_PID 2>/dev/null' EXIT
for i in $(seq 1 60); do curl -s -m 3 http://127.0.0.1:7071/health > /dev/null && break; sleep 5; done
curl -s -m 5 http://127.0.0.1:7071/health > /dev/null || { echo "FATAL: futon1a did not come up"; exit 1; }
echo "futon1a up (pid $F1A_PID)"
bb hx-backfill-per-type.bb
echo "=== PER-TYPE BACKFILL END $(date -u +%FT%TZ) ==="
