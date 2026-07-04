#!/usr/bin/env bash
# M-futon1b-port P3 — parity harness.
#
# Runs both parity sides (XTDB 1.x Datalog + 2.x XTQL) over the identical
# slice, captures their pipe-delimited output, and compares line-by-line
# with strict equality. Any mismatch is a parity failure.
#
# The two sides CANNOT share a classpath (1.x and 2.x are the same Maven
# coordinate at different versions), so they run as separate JVMs.
#
# Run: cd /home/joe/code/futon1b && bash p3_parity_harness.sh
set -euo pipefail

cd "$(dirname "$0")"

OUT_DIR="/tmp/parity-p3"
mkdir -p "$OUT_DIR"

echo "=== P3 PARITY HARNESS ==="
echo "Running 2.x side (XTQL)..."
clojure -M:node -m parity-2x > "$OUT_DIR/out-2x.txt" 2>/dev/null

echo "Running 1.x side (Datalog)..."
(cd parity-1x && clojure -M:node -m parity-1x) > "$OUT_DIR/out-1x.txt" 2>/dev/null

echo ""
echo "=== 2.x output ==="
cat "$OUT_DIR/out-2x.txt"
echo ""
echo "=== 1.x output ==="
cat "$OUT_DIR/out-1x.txt"
echo ""

# Compare by strict line-by-line equality.
if diff "$OUT_DIR/out-1x.txt" "$OUT_DIR/out-2x.txt" > /dev/null; then
    LINE_COUNT=$(wc -l < "$OUT_DIR/out-2x.txt")
    # Subtract the DONE line from the query count.
    QUERY_LINES=$((LINE_COUNT - 1))
    echo "=== PARITY RESULT: PASS ==="
    echo "All $QUERY_LINES query lines are byte-identical between 1.x and 2.x."
else
    echo "=== PARITY RESULT: FAIL ==="
    echo "Differences found:"
    diff "$OUT_DIR/out-1x.txt" "$OUT_DIR/out-2x.txt" || true
    exit 1
fi
