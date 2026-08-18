#!/usr/bin/env python3
"""Does sidecar B cover every evidence id indexed in sidecar A?

    sidecar-coverage.py <A-store-dir> <B-store-dir>

Answers the only question that matters for a sidecar transplant: if B is a
superset of A, B can replace A's index and the C5 store-side re-check will
reject B's extra rows as candidates that do not exist here. If B is missing
ids that A has, those documents become unsearchable and a transplant is wrong.
"""
import os
import sqlite3
import sys

if len(sys.argv) < 3:
    sys.exit(__doc__)


def ids(store_dir):
    db = os.path.join(store_dir, "fts5-evidence.db")
    c = sqlite3.connect("file:%s?mode=ro" % db, uri=True)
    out = {r[0] for r in c.execute("select id from ev_fts")}
    attr = c.execute("select count(*) from ev_attr").fetchone()[0]
    c.close()
    return out, attr


a, a_attr = ids(sys.argv[1])
b, b_attr = ids(sys.argv[2])

print("  A (live)   : %7d indexed, ev_attr %7d" % (len(a), a_attr))
print("  B (donor)  : %7d indexed, ev_attr %7d" % (len(b), b_attr))
missing = a - b
extra = b - a
print("  ---")
print("  in A but NOT in B : %7d   <- these would LOSE their index entry" % len(missing))
print("  in B but not in A : %7d   <- harmless; C5 re-check rejects them" % len(extra))
if missing:
    print("  sample missing:")
    for i in sorted(missing)[:8]:
        print("    %s" % i)
    print("  VERDICT: transplant is NOT safe as-is (%d would go unsearchable)"
          % len(missing))
else:
    print("  VERDICT: B is a strict superset -- transplant is safe")
