#!/usr/bin/env bash
# Restart futon1b-zone.service from OUTSIDE any agent's cgroup, with the
# README-fts §7 store-operation checklist BUILT IN rather than written down.
#
# Why detached: same reason as futon3c's restart-fdev-detached.sh -- an agent
# turn can live inside the cgroup being restarted, so `systemctl --user restart`
# would kill the only observer of whether the service came back.
#
# Why the checks are here and not in a document: README-fts.md was written after
# an incident where a checklist was run, passed, and four tables were reverted --
# because every check in it was about :evidence. A rule that lives in notes gets
# read after the operation. This script takes the census of EVERY table before
# and after, and diffs every row.
#
#   systemd-run --user --unit=futon1b-restart --collect \
#     /home/joe/code/futon1b/scripts/restart-futon1b-detached.sh
#
# If the store is wedged and a deep census would itself add unsafe heap
# pressure, explicitly opt into a blind restart:
#
#   systemd-run --user --unit=futon1b-restart --collect \
#     --setenv=FUTON1B_RESTART_ALLOW_NO_CENSUS=1 \
#     /home/joe/code/futon1b/scripts/restart-futon1b-detached.sh
#
# This skips BOTH deep-health census calls and their shrink diff. The receipt
# marks the missing evidence loudly. Backlog, PID-change, and write-path checks
# still run. Without the exact value 1, the censused path is unchanged.
#
# Then read /tmp/futon1b-restart-receipt.txt once :7073 answers again.
set -uo pipefail

R=/tmp/futon1b-restart-receipt.txt
REPO=/home/joe/code/futon1b
URL=http://127.0.0.1:7073
UNIT=futon1b-zone.service
: > "$R"
say(){ echo "$(date -u +%H:%M:%SZ) $*" >> "$R"; }
census(){ curl -sf --max-time 20 "$URL/health?deep=true" 2>/dev/null; }

say "cgroup=$(cat /proc/self/cgroup)"
say "unit=$UNIT  repo=$REPO  head=$(git -C "$REPO" rev-parse --short HEAD 2>/dev/null)"

# ---- PRE-CHECK 1: XTDB byte-offset backlog (fail closed) --------------------
say "PRE-CHECK 1: XTDB log backlog must be under the safe ceiling"
PRE=$(cd "$REPO" && timeout 120 python3 scripts/pre-restart-check.py 2>&1)
PRE_RC=$?
echo "$PRE" | sed 's/^/    /' >> "$R"
if [ "$PRE_RC" -ne 0 ] || ! echo "$PRE" | grep -q 'restart_safe=true'; then
  say "  *** BACKLOG NOT SAFE (exit $PRE_RC) - ABORTING, nothing was touched ***"
  say "RESULT=aborted"; exit 3
fi
say "  restart_safe=true"

# ---- PRE-CHECK 2: table census, EVERY population ---------------------------
say "PRE-CHECK 2: table census before"
if [ "${FUTON1B_RESTART_ALLOW_NO_CENSUS:-}" = "1" ]; then
  BEFORE=""
  say "  *** BEFORE-CENSUS UNAVAILABLE: BLIND RESTART EXPLICITLY OPTED IN ***"
  say "  *** /health?deep=true was NOT requested; shrink diff will be skipped ***"
  say "  CENSUS_MODE=blind-no-census"
else
  BEFORE=$(census)
  if [ -z "$BEFORE" ]; then
    say "  *** store did not answer /health?deep=true - ABORTING ***"
    say "RESULT=aborted"; exit 3
  fi
  echo "    $BEFORE" >> "$R"
fi

# ---- RESTART ---------------------------------------------------------------
OLD_PID=$(pgrep -f -- '-m futon1b-server' | while read -r p; do
           [ "$(cat /proc/$p/comm 2>/dev/null)" = "java" ] && echo "$p"; done | head -1)
RESTART_AT=$(date -u +%FT%TZ)
say "restarting $UNIT (t0=$RESTART_AT, old-pid=${OLD_PID:-none})"
systemctl --user restart "$UNIT"
RC=$?
say "  systemctl returned $RC"
if [ "$RC" -ne 0 ]; then
  say "  journalctl --user -u ${UNIT%.service} -n 40 --no-pager:"
  journalctl --user -u "${UNIT%.service}" -n 40 --no-pager >> "$R" 2>&1
  say "RESULT=failed"; exit 1
fi

# ---- WAIT ------------------------------------------------------------------
UP=""
for i in $(seq 1 150); do
  if curl -sf -o /dev/null --max-time 3 "$URL/health"; then
    UP=$((i*2)); say "STORE UP after ${UP}s"; break
  fi
  sleep 2
done
NEW_PID=$(systemctl --user show "$UNIT" -p MainPID --value 2>/dev/null)
if [ -n "$UP" ] && [ -n "$OLD_PID" ] && [ "$NEW_PID" = "$OLD_PID" ]; then
  say "  *** THE PROCESS DID NOT CHANGE (pid $OLD_PID before and after) ***"
  say "  The health check answered, but it was answered by the OLD process."
  say "  Every check after this point would pass against a store that never"
  say "  restarted -- which is exactly how this script read RESULT=ok on"
  say "  2026-08-19 while a second JVM crash-looped 38 times beside it."
  say "RESULT=failed-not-restarted"; exit 1
fi
say "  main pid ${OLD_PID:-none} -> ${NEW_PID:-unknown}"
if [ -z "$UP" ]; then
  say "STORE DID NOT RETURN within 300s"
  say "  journalctl --user -u ${UNIT%.service} -n 40 --no-pager:"
  journalctl --user -u "${UNIT%.service}" -n 40 --no-pager >> "$R" 2>&1
  say "RESULT=failed"; exit 1
fi

# ---- POST-CHECK 1: census again, diff EVERY row ----------------------------
say "POST-CHECK 1: table census after, every row diffed"
if [ "${FUTON1B_RESTART_ALLOW_NO_CENSUS:-}" = "1" ]; then
  say "  *** SKIPPED: no before-census exists; no shrink claim is possible ***"
else
  AFTER=$(census)
  echo "    $AFTER" >> "$R"
  DIFF=$(BEFORE="$BEFORE" AFTER="$AFTER" python3 - <<'PY'
import os, re
def tables(s):
    m = re.search(r':tables\s*\{(.*?)\}', s, re.S)
    return dict((k, int(v)) for k, v in re.findall(r':([\w-]+)\s+(\d+)', m.group(1))) if m else {}
b, a = tables(os.environ["BEFORE"]), tables(os.environ["AFTER"])
lost = []
for k in sorted(set(b) | set(a)):
    x, y = b.get(k), a.get(k)
    if x != y:
        d = (y or 0) - (x or 0)
        print(f"    {k}: {x} -> {y} ({d:+d})")
        if d < 0:
            lost.append(k)
print("LOST" if lost else "NO-TABLE-SHRANK")
PY
  )
  echo "$DIFF" | grep -v '^LOST$\|^NO-TABLE-SHRANK$' >> "$R"
  if echo "$DIFF" | grep -q '^LOST$'; then
    say "  *** A TABLE SHRANK ACROSS THE RESTART - investigate before writing anything ***"
    say "RESULT=degraded"; exit 2
  fi
  say "  no table shrank"
fi

# ---- POST-CHECK 2: the write path, which is the easiest one to forget ------
# READ-ONLY BY CONSTRUCTION. README-fts §6: evidence is append-only and cannot
# be retracted, so a POST probe would leave permanent test data in the store on
# every restart -- §6 says prefer a read-only probe. Live agents write to this
# store continuously, so "is there evidence stamped AFTER the restart?" answers
# §7's write-path question without adding anything to the corpus.
say "POST-CHECK 2: write path (read-only)"
FRESH=$(RESTART_AT="$RESTART_AT" URL="$URL" python3 "$REPO/scripts/_restart_fresh_writes.py" 2>&1)
echo "$FRESH" | grep -v 'FRESH-WRITES-SEEN\|NO-FRESH-WRITES\|PROBE-ERROR' | sed 's/^/    /' >> "$R"
if echo "$FRESH" | grep -q 'FRESH-WRITES-SEEN'; then
  say "  write path OK - evidence is landing with post-restart timestamps"
else
  say "  WRITE PATH UNCONFIRMED - no evidence stamped after the restart yet."
  say "  This is NOT proof of failure: it means no agent has written since restart."
  say "  Re-check with: curl -s '$URL/api/alpha/evidence?limit=5' | head -c 400"
  say "RESULT=ok-write-unconfirmed"; exit 0
fi

say "RESULT=ok"
exit 0
