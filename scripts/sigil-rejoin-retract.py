#!/usr/bin/env python3
"""Retract only orphaned pattern/has-sigil rows through the public API.

Dry-run is the default. --apply first retracts one row and proves its exact
relation/id disappeared with a one-row count drop before batching the rest.
"""

import argparse
import datetime as dt
import json
import os
import re
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request


BASE = "http://127.0.0.1:7073"
RECEIPT = "docs/sigil-rejoin-retract-zone-2026-08-23.edn"
EXPECTED_DEAD = 1780
EXPECTED_KEPT = 326
UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
UUID_LITERAL = re.compile(r'^#uuid "[0-9a-f-]{36}"$')
CONTROL_TYPES = ["pattern/has-clause", "pattern/has-premise", "pattern/has-conclusion"]


class Keyword(str):
    pass


def request(method, path, body=None, params=None, attempts=30):
    url = BASE + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={"Accept": "application/json", "Content-Type": "application/json",
                 "x-penholder": "api"})
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(req, timeout=180) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as error:
            raw = error.read().decode("utf-8", "replace")
            if error.code == 503 and method == "GET" and attempt + 1 < attempts:
                time.sleep(min(5, int(error.headers.get("Retry-After", "1"))))
                continue
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                payload = {"raw": raw}
            return error.code, payload
    raise AssertionError("unreachable")


def get(path, params):
    status, payload = request("GET", path, params=params)
    if status != 200:
        raise RuntimeError(f"GET {path} failed: HTTP {status}: {payload}")
    return payload


def relation_rows():
    payload = get("/api/alpha/relations",
                  {"type": "pattern/has-sigil", "limit": 5000})
    rows = payload.get("relations", [])
    if len(rows) != payload.get("count"):
        raise RuntimeError(
            f"relation response truncated: rows={len(rows)} count={payload.get('count')}")
    return rows


def pattern_ids():
    payload = get("/api/alpha/entities",
                  {"type": "pattern/library", "limit": 2000})
    rows = payload.get("entities", [])
    if len(rows) != payload.get("count"):
        raise RuntimeError(
            f"pattern response truncated: rows={len(rows)} count={payload.get('count')}")
    return {row["entity/id"] for row in rows}


def latest_join():
    return get("/api/alpha/entities/latest",
               {"type": "pattern/library", "limit": 1})["sigil-join"]


def control_counts():
    return {relation_type: get("/api/alpha/relations",
                               {"type": relation_type, "limit": 1})["count"]
            for relation_type in CONTROL_TYPES}


def shape(row):
    src, dst = row.get("relation/src", ""), row.get("relation/dst", "")
    if UUID_LITERAL.fullmatch(src) and UUID_LITERAL.fullmatch(dst):
        return "uuid-literal->uuid-literal"
    if UUID.fullmatch(src) and isinstance(dst, str) and dst.startswith("sigil|"):
        return "bare-uuid->sigil-key"
    if UUID.fullmatch(src) and UUID.fullmatch(dst):
        return "bare-uuid->bare-uuid"
    return "other"


def classify(rows, live_patterns):
    dead = [row for row in rows if row.get("relation/src") not in live_patterns]
    kept = [row for row in rows if row.get("relation/src") in live_patterns]
    shapes = {}
    for row in dead:
        key = shape(row)
        shapes[key] = shapes.get(key, 0) + 1
    return dead, kept, shapes


def retract(rows):
    documents = [{"table": "relations", "id": row["relation/id"]} for row in rows]
    return request("POST", "/api/alpha/documents/retract", {"documents": documents})


def edn(value):
    if isinstance(value, Keyword):
        return ":" + value
    if value is None:
        return "nil"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        return "[" + " ".join(edn(item) for item in value) + "]"
    if isinstance(value, dict):
        return "{" + " ".join(f":{key} {edn(item)}" for key, item in value.items()) + "}"
    raise TypeError(type(value))


def atomic_write(path, value):
    absolute = os.path.abspath(path)
    os.makedirs(os.path.dirname(absolute), exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".sigil-rejoin-retract-",
                                     dir=os.path.dirname(absolute), text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(edn(value) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, absolute)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def snapshot(live_patterns):
    rows = relation_rows()
    dead, kept, shapes = classify(rows, live_patterns)
    return {"rows": len(rows), "dead": len(dead), "kept": len(kept),
            "shapes": shapes, "sigil-join": latest_join(),
            "controls": control_counts()}, dead


def main():
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    parser.add_argument("--receipt", default=RECEIPT)
    args = parser.parse_args()

    live_patterns = pattern_ids()
    before, dead = snapshot(live_patterns)
    expected_shapes = {"uuid-literal->uuid-literal": 820,
                       "bare-uuid->sigil-key": 818,
                       "bare-uuid->bare-uuid": 142}
    completed = (before["rows"] == EXPECTED_KEPT and before["dead"] == 0 and
                 before["kept"] == EXPECTED_KEPT and before["shapes"] == {} and
                 before["sigil-join"].get("matched") == EXPECTED_KEPT and
                 before["sigil-join"].get("relation-srcs") == EXPECTED_KEPT)
    if completed:
        print(json.dumps({"mode": "already-applied", "selected": 0,
                          "retracted": 0, "rows": before["rows"],
                          "sigil-join": before["sigil-join"]},
                         ensure_ascii=False, sort_keys=True))
        return
    if (before["rows"] != EXPECTED_DEAD + EXPECTED_KEPT or
            before["dead"] != EXPECTED_DEAD or before["kept"] != EXPECTED_KEPT or
            before["shapes"] != expected_shapes or
            before["sigil-join"].get("matched") != EXPECTED_KEPT):
        raise RuntimeError(f"selection gate failed; nothing retracted: {before}")

    preview = {"mode": "apply" if args.apply else "dry-run", "selected": len(dead),
               "kept": before["kept"], "shapes": before["shapes"],
               "rows": before["rows"], "sigil-join": before["sigil-join"]}
    if not args.apply:
        print(json.dumps(preview, ensure_ascii=False, sort_keys=True))
        return

    failed = []
    retracted = 0
    id_form = 'exact :relation/id response string (for example #uuid "…")'

    # Establish the public ID form on exactly one row before bulk mutation.
    probe = dead[0]
    status, payload = retract([probe])
    if status != 200 or payload.get("count") != 1:
        failed.append({"id": probe.get("relation/id"), "status": status,
                       "error": payload})
    else:
        after_probe = relation_rows()
        after_ids = {row["relation/id"] for row in after_probe}
        if (probe["relation/id"] in after_ids or
                len(after_probe) != before["rows"] - 1):
            failed.append({"id": probe["relation/id"], "status": "verify-failed",
                           "error": {"rows": len(after_probe),
                                     "id-still-present": probe["relation/id"] in after_ids}})
        elif latest_join().get("matched") != EXPECTED_KEPT:
            failed.append({"id": probe["relation/id"], "status": "join-changed",
                           "error": latest_join()})
        else:
            retracted = 1

    if failed:
        after, _ = snapshot(live_patterns)
    else:
        for start in range(1, len(dead), 200):
            batch = dead[start:start + 200]
            status, payload = retract(batch)
            if status == 200 and payload.get("count") == len(batch):
                retracted += len(batch)
            else:
                failed.extend({"id": row["relation/id"], "status": status,
                               "error": payload} for row in batch)
        after, remaining = snapshot(live_patterns)
        if remaining:
            failed.extend({"id": row["relation/id"], "status": "still-present",
                           "error": "post-apply selection"} for row in remaining)
        if after["controls"] != before["controls"]:
            failed.append({"status": "control-count-changed",
                           "error": {"before": before["controls"],
                                     "after": after["controls"]}})
        if (after["rows"] != EXPECTED_KEPT or after["kept"] != EXPECTED_KEPT or
                after["dead"] != 0 or
                after["sigil-join"].get("matched") != EXPECTED_KEPT or
                after["sigil-join"].get("relation-srcs") != EXPECTED_KEPT):
            failed.append({"status": "postcondition-failed", "error": after})

    receipt = {"applied-at": dt.datetime.now(dt.timezone.utc).isoformat()
               .replace("+00:00", "Z"),
               "host": "zone", "selected": len(dead), "retracted": retracted,
               "failed": failed, "id-form": id_form,
               "shape-counts": before["shapes"],
               "verify": {"before": before, "after": after}}
    atomic_write(args.receipt, receipt)
    print(json.dumps(receipt, ensure_ascii=False, sort_keys=True))
    if failed or retracted != EXPECTED_DEAD:
        raise RuntimeError(f"retraction incomplete: retracted={retracted} failed={len(failed)}")


if __name__ == "__main__":
    main()
