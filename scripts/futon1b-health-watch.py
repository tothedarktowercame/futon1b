#!/usr/bin/env python3
"""futon1b-health-watch — early warnings into Joe's Emacs *Warnings* buffer.

Joe, 2026-11-14: "could I get Emacs *Warnings* sent through if futon1b starts
showing signs of being poorly, so that I can respond before we hit a cliff?"
That day futon1b thrashed in GC for hours (plan cache 4.2 GB of 8 GB) before
anyone looked; see holes/E-xtdb-plan-cache-heap.md. Each signal below was an
early sign that day, and the thresholds come from what was measured.

One run takes cheap probes, compares them with the previous run (state file),
and calls `display-warning` with type `futon1b` in the Emacs at socket
`server`. A condition must hold on REQUIRED consecutive runs before it warns,
re-warns at most every REWARN_S while it persists, and sends one "recovered"
note when it clears. Meant to run every 2 min from futon1b-health-watch.timer.

  scripts/futon1b-health-watch.py            # one run
  scripts/futon1b-health-watch.py --dry-run  # print what it would send
  scripts/futon1b-health-watch.py --test     # send one labelled test warning
"""
import json
import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
EVAL = HERE / "futon1b-eval.sh"
URL = os.environ.get("FUTON1B_URL", "http://127.0.0.1:7073")
STATE_DIR = Path(os.environ.get("FUTON1B_HEALTH_DIR",
                                Path.home() / "storage" / "futon1b-health"))
STATE = STATE_DIR / "state.json"
LOG = STATE_DIR / "runs.tsv"
EMACS_SOCKET = os.environ.get("FUTON1B_HEALTH_EMACS_SOCKET", "server")
REWARN_S = 3600
NOTE = "Record: futon1b holes/E-xtdb-plan-cache-heap.md; rules: futon1b README."

# Read-only probe. Never call /health?deep=true (it can OOM a full heap).
PROBE = r"""
(let [node @futon1b-server/!node
      f (doto (.getDeclaredField (class node) "system") (.setAccessible true))
      sys (.get f node)
      qs (get sys :xtdb.query/query-source)
      mca (some (fn [^org.apache.arrow.memory.BufferAllocator c]
                  (when (= "memory-cache" (.getName c)) c))
                (.getChildAllocators ^org.apache.arrow.memory.BufferAllocator
                                     (:allocator (:xtdb/allocator sys))))
      st @futon1b-hxindex/!stats
      ds @futon1b-text/!ds]
  {:mcache-used (some-> mca .getAllocatedMemory)
   :mcache-limit (some-> mca .getLimit)
   :plans (.estimatedSize ^com.github.benmanes.caffeine.cache.Cache (:plan-cache qs))
   :permits (futon1b-xt/query-permits-available)
   :reads-usable (futon1b-hxindex/reads-usable?)
   :dirty (when ds (futon1b-hxindex/dirty-count ds))
   :hook-failures (or (:hook-failures st) 0)
   :audit-caught-up (get-in st [:last-catch-up :caught-up?])
   :audit-at (get-in st [:last-catch-up :at])})
"""


def sh(args, timeout):
    try:
        r = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        return r.returncode, r.stdout
    except (subprocess.TimeoutExpired, OSError):
        return -1, ""


def http_probe():
    t0 = time.time()
    try:
        with urllib.request.urlopen(f"{URL}/api/alpha/evidence?limit=1", timeout=20) as r:
            return r.status, time.time() - t0
    except Exception as e:  # refused, timeout, 5xx
        return getattr(e, "code", None) or str(e)[:80], time.time() - t0


def edn_field(text, key):
    """Read one scalar field from the probe's EDN answer."""
    import re
    m = re.search(r":" + re.escape(key) + r" (\S+?)[,}]", text)
    if not m:
        return None
    v = m.group(1)
    if v == "nil":
        return None
    if v in ("true", "false"):
        return v == "true"
    try:
        return int(v)
    except ValueError:
        return v.strip('"')


def collect():
    s = {"ts": time.time()}
    s["http"], s["http_s"] = http_probe()
    rc, pid = sh(["systemctl", "--user", "show", "futon1b-zone", "-p", "MainPID", "--value"], 10)
    s["pid"] = pid.strip() if rc == 0 else None
    if s["pid"] and s["pid"] != "0":
        rc, out = sh(["jstat", "-gcutil", s["pid"]], 20)
        cols = out.strip().splitlines()[-1].split() if rc == 0 and out.strip() else []
        s["fgc"] = int(cols[8]) if len(cols) > 8 and cols[8].isdigit() else None
    rc, out = sh([str(EVAL), PROBE], 45)
    s["eval_ok"] = rc == 0 and ":ok true" in out
    for k in ("mcache-used", "mcache-limit", "plans", "permits", "reads-usable", "dirty", "hook-failures",
              "audit-caught-up", "audit-at"):
        s[k] = edn_field(out, k) if s["eval_ok"] else None
    return s


def conditions(s, prev):
    """[(key, level, required-consecutive, message)] for what is wrong now."""
    out = []
    if s["http"] != 200:
        out.append(("http-down", ":error", 2,
                    f":7073 is not answering a one-row evidence read ({s['http']}, {s['http_s']:.0f} s). "
                    "Check `systemctl --user status futon1b-zone` and jstat GC before restarting."))
    elif s["http_s"] > 3:
        out.append(("http-slow", ":warning", 2,
                    f"a one-row evidence read took {s['http_s']:.1f} s (normally under 0.1 s). "
                    "Look at GC (jstat -gcutil) and query permits."))
    if s.get("fgc") is not None and prev.get("fgc") is not None and s.get("pid") == prev.get("pid"):
        d = s["fgc"] - prev["fgc"]
        if d >= 5:
            out.append(("full-gc", ":error", 1,
                        f"{d} full GCs since the last check (total {s['fgc']}). On 11-14 this "
                        "preceded hours of GC thrash. Check the plan count and old gen now."))
        elif d > 0:
            out.append(("full-gc", ":warning", 1,
                        f"{d} full GC(s) since the last check (total {s['fgc']}); there were none "
                        "between the 11-14 fix and this warning."))
    p = s.get("plans")
    if isinstance(p, int) and p >= 3500:
        out.append(("plans", ":error", 1,
                    f"XTDB plan cache at {p} of 4096 plans (~1 MB each). Full GCs began near 2,800 "
                    "on 11-14. Find the new query shape (sql-strings dump) or invalidateAll."))
    elif isinstance(p, int) and p >= 2500:
        out.append(("plans", ":warning", 1,
                    f"XTDB plan cache at {p} plans and rising; full GCs began near 2,800 on 11-14. "
                    "Some query is putting values in its text."))
    # XTDB's in-memory Arrow page cache frees a page only when its last
    # reader closes it. On 11-14 (19:23) it filled with 926 pages whose
    # readers were gone (suspected: queries cancelled at the deadline), and
    # every read then failed "Unable to allocate buffer". In normal use
    # the cache drains between queries, so a high level that does not
    # come down means leaked pages; a restart is the only clean fix.
    u, lim = s.get("mcache-used"), s.get("mcache-limit")
    if isinstance(u, int) and isinstance(lim, int) and lim > 0:
        frac = u / lim
        gb = f"{u / 1e9:.2f} of {lim / 1e9:.2f} GB"
        if frac >= 0.85:
            out.append(("mcache", ":error", 1,
                        f"XTDB memory cache at {gb} ({frac:.0%}). At 100% every read fails "
                        "with 'Unable to allocate buffer' (11-14 19:23). Pages are held by "
                        "readers that never closed; plan a futon1b restart now."))
        elif frac >= 0.5:
            out.append(("mcache", ":warning", 3,
                        f"XTDB memory cache has stayed at {gb} ({frac:.0%}) for 3 checks; "
                        "it normally drains between queries, so pages are probably leaking."))
    if s.get("eval_ok") is False and s["http"] == 200:
        out.append(("drawbridge", ":warning", 2,
                    "futon1b answers HTTP but its :6769 drawbridge did not answer the health probe."))
    if s.get("reads-usable") is False:
        out.append(("hx-reads-off", ":warning", 2,
                    f"hyperedge index reads are off (journaled ids: {s.get('dirty')}); typed reads are "
                    "scanning (~10 s each). repair-dirty! runs every minute; if this persists, "
                    "check @futon1b-hxindex/!stats :last-repair."))
    if isinstance(s.get("hook-failures"), int) and s["hook-failures"] > 0:
        out.append(("hx-unjournaled", ":error", 1,
                    f"{s['hook-failures']} hyperedge index failure(s) could not even be journaled "
                    "(the sidecar SQLite file failing?). Index reads stay off until an audit completes."))
    if s.get("audit-caught-up") is False:
        out.append(("hx-audit-behind", ":warning", 3,
                    f"the hyperedge history audit (last run {s.get('audit-at')}) did not reach the "
                    "present within its budget; on 11-13 this grew until no run could finish."))
    if s.get("permits") == 0:
        out.append(("permits", ":warning", 2,
                    "futon1b has no free query permits: requests are queueing behind long reads."))
    return out


def emacs_string(text):
    return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'


def send(level, text, dry):
    form = f"(display-warning 'futon1b {emacs_string(text)} {level})"
    if dry:
        print("WOULD SEND", form)
        return True
    rc, _ = sh(["emacsclient", "-s", EMACS_SOCKET, "--eval", form], 15)
    return rc == 0


def main():
    dry = "--dry-run" in sys.argv
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    if "--test" in sys.argv:
        ok = send(":warning", "futon1b-health-watch test: warnings reach this buffer. "
                  "Nothing is wrong; ignore this one.", dry)
        print("sent" if ok else "emacsclient failed")
        return
    state = json.loads(STATE.read_text()) if STATE.exists() else {}
    prev = state.get("prev", {})
    conds_state = state.get("conds", {})
    s = collect()
    now = s["ts"]
    active = {}
    for key, level, required, msg in conditions(s, prev):
        c = conds_state.get(key, {"streak": 0, "warned_at": None})
        c["streak"] += 1
        if c["streak"] >= required and (c["warned_at"] is None or now - c["warned_at"] >= REWARN_S):
            if send(level, f"futon1b: {msg} {NOTE}", dry):
                c["warned_at"] = now
        active[key] = c
    for key, c in conds_state.items():
        if key not in active and c.get("warned_at"):
            send(":warning", f"futon1b recovered: {key} has cleared.", dry)
    if not dry:
        STATE.write_text(json.dumps({"prev": s, "conds": active}))
        new = not LOG.exists()
        with LOG.open("a") as f:
            if new:
                f.write("ts\thttp\thttp_s\tfgc\tplans\tpermits\treads_usable\tdirty\thook_failures\tactive\tmcache_mb\n")
            f.write("\t".join(str(x) for x in [
                time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)), s["http"], f"{s['http_s']:.2f}",
                s.get("fgc"), s.get("plans"), s.get("permits"), s.get("reads-usable"),
                s.get("dirty"), s.get("hook-failures"), ",".join(sorted(active)) or "-",
                (s["mcache-used"] // 1000000) if isinstance(s.get("mcache-used"), int) else None]) + "\n")
    else:
        print(json.dumps(s, indent=1, default=str))
        print("active:", sorted(active))


if __name__ == "__main__":
    main()
