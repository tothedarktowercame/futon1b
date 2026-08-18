#!/usr/bin/env python3
"""Report futon1b FTS index health against the store it indexes.

    scripts/fts-status.py [port]        # default 7073

Prints the index's own numbers next to the store's, because the whole class of
bug this exists to catch is an index that looks healthy on its own terms while
disagreeing with the store. See README-fts.md §5 for how to read the output --
in particular, `tx-lag 0` means the index has caught up with everything the
store HAS, not that the store is receiving anything.
"""
import re
import sys
import urllib.request

port = sys.argv[1] if len(sys.argv) > 1 else "7073"
base = "http://127.0.0.1:%s/api/alpha/evidence" % port


def get(url, timeout=200):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def field(key, blob):
    m = re.search(r":%s ([^,}\]]+)" % re.escape(key), blob)
    return m.group(1).strip() if m else "?"


try:
    stats = get(base + "/text-search?stats=true")
    count = get(base + "/count")
except Exception as e:                                    # noqa: BLE001
    sys.exit("  could not reach :%s -- %s" % (port, e))

store_rows, index_rows = field("count", count), field("rows", stats)

print("  store evidence rows : %s" % store_rows)
print("  index rows          : %s" % index_rows)
try:
    delta = int(store_rows) - int(index_rows)
    # A negative delta is NOT a fault: C1 has the index over-approximate and
    # never under-approximate, so surplus candidates are rejected by the
    # store-side re-check. Expected after a sidecar transplant (README-fts §9b).
    print("  delta               : %+d %s" % (
        delta,
        "(index behind -- catch-up should close this)" if delta > 0
        else "(level)" if delta == 0
        else "(index over-approximates -- fine per C1; re-check rejects surplus)"))
except ValueError:
    pass

print("  ---")
for k in ("indexed", "errors", "ready", "periodic?", "recheck-rejections"):
    print("  %-19s : %s" % (k, field(k, stats)))

m = re.search(r":staleness \{([^}]*)\}", stats)
if m:
    print("  staleness           : %s" % m.group(1))

m = re.search(r':captured-at "([^"]+)"', stats)
print("  basis captured-at   : %s"
      % (m.group(1) if m else "ABSENT -- no proven drain (honest, not broken)"))

m = re.search(r":last-error \{(.*?)\}", stats, re.S)
print("  last-error          : %s"
      % (m.group(1)[:200].replace("\n", " ") if m else "none"))
if m:
    print("    (`errors` is cumulative since boot, not current state --")
    print("     check whether catch-up! has since repaired the document)")
