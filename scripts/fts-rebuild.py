#!/usr/bin/env python3
"""Start a full, deterministic rebuild of the futon1b FTS index.

    scripts/fts-rebuild.py <store-dir> [port] [--boundary ISO8601]

catch-up! documents that "with no checkpoint this is the full deterministic
rebuild", so a rebuild is: clear the checkpoint, clear the basis, then ask the
server to catch up. It runs in the BACKGROUND and LIVE -- no service stop, and
therefore no write outage. That matters: an evidence write outage is lossy
(no spool, no retry), so stopping the service to edit fts_meta would cost real
data for no benefit.

While it runs, search is degraded and the basis is absent. That is honest and
visible, per C2/C6 -- an absent basis is a refusal to claim coverage, not a
fault. on-append! keeps indexing new writes at the tail throughout.

With --boundary, resets to that (at, id) instead of the beginning -- the
cheaper repair after a bounded backfill. See README-fts.md §3 and §9.
"""
import os
import sqlite3
import sys
import urllib.request

args = [a for a in sys.argv[1:] if not a.startswith("--")]
if not args:
    sys.exit(__doc__)
store_dir = args[0]
port = args[1] if len(args) > 1 else "7073"

boundary = None
if "--boundary" in sys.argv:
    boundary = sys.argv[sys.argv.index("--boundary") + 1]

db = os.path.join(store_dir, "fts5-evidence.db")
if not os.path.exists(db):
    sys.exit("  no sidecar at %s" % db)

base = "http://127.0.0.1:%s/api/alpha/evidence/text-search" % port


def stats():
    with urllib.request.urlopen(base + "?stats=true", timeout=200) as r:
        return r.read().decode("utf-8", "replace")


# A catch-up already in flight holds its own in-memory cursor and would write
# it back after our edit, silently restoring the checkpoint and cancelling the
# rebuild. Single-flight makes this detectable rather than a race we lose.
before = stats()
if ":skipped :already-running" in before:
    sys.exit("  a catch-up is already running -- let it finish, then re-run")

c = sqlite3.connect(db, timeout=30)
print("  --- before ---")
for k, v in c.execute("select k, substr(v,1,50) from fts_meta order by k"):
    print("    %-20s %s" % (k, v))

if boundary:
    # INSERT OR REPLACE, not UPDATE: if a previous run deleted the checkpoint
    # rows, an UPDATE matches nothing and silently leaves no boundary at all --
    # which is the unbounded-scan case this flag exists to avoid.
    c.execute("insert or replace into fts_meta (k, v) values ('last-at', ?)",
              (boundary,))
    c.execute("insert or replace into fts_meta (k, v) values ('last-id', '')")
    print("  reset checkpoint to boundary %s" % boundary)
else:
    for k in ("last-at", "last-id"):
        c.execute("delete from fts_meta where k = ?", (k,))
    print("  cleared checkpoint (full rebuild from the beginning)")

for k in ("basis-tx", "basis-tx-ids", "basis-captured-at"):
    c.execute("delete from fts_meta where k = ?", (k,))
print("  cleared basis (it will be re-earned by a proven drain)")
c.commit()

after = dict(c.execute("select k, v from fts_meta").fetchall())
c.close()
if "basis-tx" in after:
    sys.exit("  RESET FAILED -- basis still present: %s" % sorted(after))
if boundary and after.get("last-at") != boundary:
    sys.exit("  RESET FAILED -- boundary not set (last-at=%r). Without a lower "
             "bound catch-up! scans unbounded and dies on an Arrow batch."
             % after.get("last-at"))
if not boundary and "last-at" in after:
    sys.exit("  RESET FAILED -- checkpoint still present: %s" % sorted(after))
print("  --- after ---")
for k, v in sorted(after.items()):
    print("    %-20s %s" % (k, str(v)[:50]))

req = urllib.request.Request(
    base,
    data=b'{:penholder "api" :op :catch-up}',
    headers={"content-type": "application/edn"},
    method="POST")
try:
    with urllib.request.urlopen(req, timeout=60) as r:
        print("  started: HTTP %s %s" % (r.status, r.read().decode()[:120]))
except Exception as e:                                    # noqa: BLE001
    print("  could not POST catch-up (%s)" % e)
    print("  the periodic sweep will start the rebuild on its next pass anyway")

print("  monitor with: scripts/fts-status.py %s" % port)
