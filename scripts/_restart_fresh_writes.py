#!/usr/bin/env python3
"""Read-only write-path confirmation for the futon1b restart script.

README-fts §7 asks: is new evidence appearing with timestamps after the
operation? §6 forbids the obvious answer -- evidence is append-only, so a POST
probe pollutes the corpus permanently on every restart. Live agents write
continuously, so reading is sufficient.
"""
import os, re, sys, urllib.request

url = os.environ["URL"]
since = os.environ["RESTART_AT"]
try:
    body = urllib.request.urlopen(url + "/api/alpha/evidence?limit=25", timeout=25).read().decode()
except Exception as exc:                                    # noqa: BLE001
    print("PROBE-ERROR", exc)
    sys.exit(0)

stamps = re.findall(r':evidence/at\s+#[\w/]*\s*"([^"]+)"', body)
if not stamps:
    stamps = re.findall(r':evidence/at\s+"([^"]+)"', body)
newer = [s for s in stamps if s > since]
print(f"read {len(stamps)} timestamps, {len(newer)} stamped after {since}")
print("FRESH-WRITES-SEEN" if newer else "NO-FRESH-WRITES")
