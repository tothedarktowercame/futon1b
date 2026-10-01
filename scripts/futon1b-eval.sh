#!/usr/bin/env bash
# futon1b-eval.sh — evaluate Clojure inside the running futon1b JVM (:6769).
#   scripts/futon1b-eval.sh '(+ 1 2)'
#   scripts/futon1b-eval.sh -f /tmp/form.clj
#   scripts/futon1b-eval.sh - < /tmp/form.clj
# The node is @futon1b-server/!node; the query helpers are in futon1b-xt.
# Refuses tools.namespace refresh and shutdown-agents (see futon1b_drawbridge.clj).
set -euo pipefail
PORT="${FUTON1B_DRAWBRIDGE_PORT:-6769}"
HOST="${FUTON1B_DRAWBRIDGE_HOST:-127.0.0.1}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [ -n "${FUTON1B_ADMIN_TOKEN:-}" ]; then TOKEN="$FUTON1B_ADMIN_TOKEN"
elif [ -f "$DIR/.admintoken" ]; then TOKEN="$(tr -d '[:space:]' < "$DIR/.admintoken")"
else echo "no token: set FUTON1B_ADMIN_TOKEN or create $DIR/.admintoken" >&2; exit 2; fi
case "${1:-}" in
  "") if [ -t 0 ]; then sed -n 2,7p "$0" >&2; exit 2; fi; CODE="$(cat)" ;;
  -f|--file) CODE="$(cat "$2")" ;;
  -) CODE="$(cat)" ;;
  *) if [ -f "$1" ]; then CODE="$(cat "$1")"; else CODE="$1"; fi ;;
esac
printf '%s' "$CODE" | curl -s -H "x-admin-token: $TOKEN" -H "Content-Type: text/plain" \
  --data-binary @- "http://${HOST}:${PORT}/eval"
echo
