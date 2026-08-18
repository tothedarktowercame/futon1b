#!/usr/bin/env bash
# Transplant the merged store's fully-built FTS sidecar onto the live store.
#
# Use when a full rescan will not run (README-fts.md §9a) but another store
# already holds a fully-built index over a superset of your evidence.
#
# Why it is sound: the sidecar is DERIVED data, and C1 has the index yield
# candidates only -- it over-approximates and never under-approximates, so a
# donor's surplus ids are rejected by the store-side re-check rather than
# producing wrong answers. Ids the donor LACKS are the danger: those documents
# become unfindable. Run sidecar-coverage.py first and choose a boundary that
# covers every missing id.
#
# The donor is static (no server on it), so everything is staged BEFORE the
# outage. The outage is stop + renames + start.
set -uo pipefail

usage() {
  cat <<'U'
usage: transplant-sidecar.sh <live-store-dir> <donor-store-dir> <boundary> [unit]

  live-store-dir   store whose sidecar is to be replaced (the running one)
  donor-store-dir  store whose sidecar is already fully built
  boundary         ISO8601 to reset the checkpoint to -- must be early enough
                   to cover every id the donor lacks, and recent enough that
                   the scan completes (README-fts.md §9a)
  unit             systemd --user unit, default c7-futon1b.service

Run sidecar-coverage.py FIRST. A donor missing ids you have makes those
documents unfindable; surplus ids are harmless (C1). Do NOT run under sudo --
systemctl --user needs the owning user's bus (README-fts.md §9b).
U
  exit 1
}
[ $# -ge 3 ] || usage
LIVE_DIR=$1; DONOR_DIR=$2; BOUNDARY=$3; UNIT=${4:-c7-futon1b.service}

LIVE=$LIVE_DIR/fts5-evidence.db
DONOR=$DONOR_DIR/fts5-evidence.db
NEW=$LIVE.new
BAK=$LIVE.bak-$(date +%Y-%m-%d-%H%M%S)
D=$LIVE_DIR

echo "=== 0. preflight ==="
[ -f "$DONOR" ] || { echo "FATAL: donor sidecar missing"; exit 1; }
[ -f "$LIVE" ]  || { echo "FATAL: live sidecar missing"; exit 1; }
[ -e "$BAK" ]   && { echo "FATAL: $BAK exists -- refusing to clobber"; exit 1; }
echo "  donor : $(du -h "$DONOR" | cut -f1)"
echo "  live  : $(du -h "$LIVE"  | cut -f1)"
echo "  free  : $(df -h "$D" | awk 'NR>1{print $4}')"

echo "=== 1. stage a copy of the donor (service still up, no outage) ==="
rm -f "$NEW"
cp "$DONOR" "$NEW" || { echo "FATAL: copy failed"; exit 1; }
chown joe:joe "$NEW" 2>/dev/null
echo "  staged $(du -h "$NEW" | cut -f1) at $(basename "$NEW")"

echo "=== 2. set the checkpoint on the STAGED file (nothing has it open) ==="
python3 - "$NEW" "$BOUNDARY" <<'PY'
import sqlite3, sys
db, boundary = sys.argv[1], sys.argv[2]
c = sqlite3.connect(db, timeout=30)
before = dict(c.execute("select k, v from fts_meta").fetchall())
print("    donor checkpoint was: %s" % before.get("last-at"))
c.execute("insert or replace into fts_meta (k,v) values ('last-at', ?)", (boundary,))
c.execute("insert or replace into fts_meta (k,v) values ('last-id', '')")
for k in ("basis-tx", "basis-tx-ids", "basis-captured-at"):
    c.execute("delete from fts_meta where k = ?", (k,))
c.commit()
after = dict(c.execute("select k, v from fts_meta").fetchall())
assert after.get("last-at") == boundary, "boundary not set: %r" % after.get("last-at")
assert "basis-tx" not in after, "basis not cleared"
print("    staged checkpoint  : %s" % after["last-at"])
print("    ev_fts=%d ev_attr=%d ev_tags=%d" % tuple(
    c.execute("select (select count(*) from ev_fts_docsize),"
              "       (select count(*) from ev_attr),"
              "       (select count(*) from ev_tags)").fetchone()))
c.close()
PY
[ $? -eq 0 ] || { echo "FATAL: staging edit failed"; rm -f "$NEW"; exit 1; }

echo "=== 3. swap (outage begins) ==="
T0=$(date +%s)
systemctl --user stop "$UNIT"
for i in $(seq 1 60); do ss -ltn | grep -q ":${PORT:-7073} " || break; sleep 1; done
mv "$LIVE" "$BAK" || { echo "FATAL: could not set live sidecar aside"; systemctl --user start "$UNIT"; exit 1; }
# -wal/-shm are named after the db file. Left behind, the OLD write-ahead log
# pairs with the NEW database; SQLite rejects it on salt mismatch, but do not
# rely on that -- move them aside with their database.
for ext in -wal -shm; do
  [ -e "$LIVE$ext" ] && mv "$LIVE$ext" "$BAK$ext"
done
mv "$NEW" "$LIVE" || { echo "FATAL: could not move staged into place -- RESTORING"; mv "$BAK" "$LIVE"; systemctl --user start "$UNIT"; exit 1; }
systemctl --user start "$UNIT"
for i in $(seq 1 180); do
  curl -s -m 5 -o /dev/null "http://127.0.0.1:${PORT:-7073}/health" 2>/dev/null && break
  sleep 2
done
T1=$(date +%s)
echo "  OUTAGE: $((T1-T0))s"
echo "  health: $(curl -s -m 10 http://127.0.0.1:${PORT:-7073}/health 2>/dev/null | cut -c1-80)"
echo "  previous sidecar retained at $(basename "$BAK")"

echo "=== 4. integrity check (the -wal pairing above is why) ==="
python3 - "$LIVE" <<'PY'
import sqlite3, sys
c = sqlite3.connect("file:%s?mode=ro" % sys.argv[1], uri=True)
r = c.execute("pragma integrity_check").fetchone()[0]
print("  integrity_check: %s" % r)
c.close()
sys.exit(0 if r == "ok" else 1)
PY
[ $? -eq 0 ] || echo "  WARNING: integrity check did NOT return ok -- consider restoring $BAK"

echo "=== 5. bounded catch-up: covers the ids the donor lacked, re-earns the basis ==="
curl -s -m 30 -X POST "http://127.0.0.1:${PORT:-7073}/api/alpha/evidence/text-search" \
  -H 'content-type: application/edn' --data '{:penholder "api" :op :catch-up}' | cut -c1-80
echo
echo "=== done -- monitor with fts-status.py ==="
