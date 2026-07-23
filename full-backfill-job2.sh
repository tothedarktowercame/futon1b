#!/bin/bash
# Hyperedges-only retry: temp futon1a -> 60-min snapshot export -> HTTP ingest to live :7073.
set -uo pipefail
cd /home/joe/code/futon1b
LOG=logs/full-backfill.log
exec >> "$LOG" 2>&1
echo "=== HYPEREDGES RETRY START $(date -u +%FT%TZ) ==="
(cd /home/joe/code/futon1a && \
 FUTON1A_DATA_DIR=/home/joe/code/storage/futon1a/default \
 FUTON1A_PORT=7071 FUTON1A_ALLOW_EMPTY_PENHOLDERS=true \
 exec clojure -M -m futon1a.system) &
F1A_PID=$!
trap 'kill $F1A_PID 2>/dev/null' EXIT
for i in $(seq 1 60); do curl -s -m 3 http://127.0.0.1:7071/health > /dev/null && break; sleep 5; done
curl -s -m 5 http://127.0.0.1:7071/health > /dev/null || { echo "FATAL: futon1a did not come up"; exit 1; }
echo "futon1a up (pid $F1A_PID)"
clojure -M:node -e "
(require '[migration.export :as ex])
(println (ex/export-via-snapshot \"http://127.0.0.1:7071\" \"migration-export-full\" \"hyperedges\" \"hyperedges.edn\" 3600000))
" || { echo "FATAL: hyperedges export failed"; exit 1; }
echo "hyperedges export done $(date -u +%FT%TZ)"
bb full-backfill-ingest.bb migration-export-full
echo "=== HYPEREDGES RETRY END $(date -u +%FT%TZ) ==="
