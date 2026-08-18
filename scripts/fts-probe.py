#!/usr/bin/env python3
"""Inspect a futon1b FTS sidecar directly: schema, watermarks, populations.

    scripts/fts-probe.py <store-dir> [evidence-id]

Opens the sidecar READ-ONLY, so it is safe to run against a live store.

Why this exists separately from fts-status.py: the served stats can look
healthy while `ev_fts` (body) and `ev_attr` (facets) disagree about how much of
the corpus they cover. Only a direct look shows that. See README-fts.md §4.
"""
import os
import sqlite3
import sys

if len(sys.argv) < 2:
    sys.exit(__doc__)

store_dir = sys.argv[1]
want = sys.argv[2] if len(sys.argv) > 2 else None
db = os.path.join(store_dir, "fts5-evidence.db")
if not os.path.exists(db):
    sys.exit("  no sidecar at %s" % db)

c = sqlite3.connect("file:%s?mode=ro" % db, uri=True)

print("  --- ev_fts schema ---")
for (sql,) in c.execute("select sql from sqlite_master where name='ev_fts'"):
    print("   ", (sql or "").replace("\n", " ")[:220])

print("  --- fts_meta (checkpoint + basis) ---")
try:
    for k, v in c.execute("select k, substr(v,1,70) from fts_meta order by k"):
        print("    %-20s %s" % (k, v))
except sqlite3.Error as e:
    print("    error: %s" % e)
print("    (last-at/last-id = checkpoint; basis-* = proven drain. README-fts.md §2)")

print("  --- populations ---")
counts = {}
for t in ("ev_fts_docsize", "ev_attr", "ev_tags"):
    try:
        counts[t] = c.execute("select count(*) from %s" % t).fetchone()[0]
        print("    %-20s %d" % (t, counts[t]))
    except sqlite3.Error as e:
        print("    %-20s (%s)" % (t, e))

body, attr = counts.get("ev_fts_docsize"), counts.get("ev_attr")
if body and attr is not None and attr < body * 0.9:
    print("    WARNING: ev_attr covers %.1f%% of ev_fts -- facet filters will be"
          % (100.0 * attr / body))
    print("             silently near-empty. A schema upgrade is not a data")
    print("             backfill; rebuild. See README-fts.md §4 and §9.")

if want:
    print("  --- lookup %s ---" % want)
    for t in ("ev_fts", "ev_attr"):
        try:
            n = c.execute("select count(*) from %s where id = ?" % t,
                          (want,)).fetchone()[0]
            print("    %-20s %d" % (t, n))
        except sqlite3.Error as e:
            print("    %-20s (%s)" % (t, e))
c.close()
