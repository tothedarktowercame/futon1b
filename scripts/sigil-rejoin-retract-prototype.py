#!/usr/bin/env python3
"""Retract prototype/has-sigil rows whose destination sigil is absent."""

import argparse
import datetime as dt
import json
import os
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = "http://127.0.0.1:7073"
RECEIPT = "docs/sigil-rejoin-retract-prototype-zone-2026-08-23.edn"
EXPECTED_DEAD = 142
EXPECTED_KEPT = 47


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
            if method == "GET" and error.code == 503 and attempt + 1 < attempts:
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


def complete_rows(path, key, params):
    payload = get(path, params)
    rows = payload.get(key, [])
    if len(rows) != payload.get("count"):
        raise RuntimeError(f"truncated {path}: rows={len(rows)} count={payload.get('count')}")
    return rows


def entity_ids(entity_type):
    return {row["entity/id"] for row in complete_rows(
        "/api/alpha/entities", "entities", {"type": entity_type, "limit": 5000})}


def relation_rows(relation_type, limit=5000):
    return complete_rows("/api/alpha/relations", "relations",
                         {"type": relation_type, "limit": limit})


def count(path, params):
    return get(path, params)["count"]


def join_state():
    return get("/api/alpha/entities/latest",
               {"type": "pattern/library", "limit": 1})["sigil-join"]


def snapshot(sigil_ids, prototype_ids):
    rows = relation_rows("prototype/has-sigil")
    dead = [row for row in rows if row.get("relation/dst") not in sigil_ids]
    kept = [row for row in rows if row.get("relation/dst") in sigil_ids]
    return ({"rows": len(rows), "dead": len(dead), "kept": len(kept),
             "remaining-dsts-live": all(row.get("relation/dst") in sigil_ids
                                         for row in kept),
             "remaining-srcs-live": all(row.get("relation/src") in prototype_ids
                                         for row in kept),
             "controls": {
                 "pattern/has-sigil": len(relation_rows("pattern/has-sigil")),
                 "pattern/library": count(
                     "/api/alpha/entities", {"type": "pattern/library", "limit": 1}),
                 "sigil-join": join_state()}}, dead)


def retract(rows):
    documents = [{"table": "relations", "id": row["relation/id"]} for row in rows]
    return request("POST", "/api/alpha/documents/retract", {"documents": documents})


def edn(value):
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
    fd, temporary = tempfile.mkstemp(prefix=".sigil-rejoin-prototype-",
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


def main():
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    parser.add_argument("--receipt", default=RECEIPT)
    args = parser.parse_args()

    sigil_ids = entity_ids("pattern/sigil")
    prototype_ids = entity_ids("devmap/prototype")
    before, dead = snapshot(sigil_ids, prototype_ids)
    completed = (before["rows"] == EXPECTED_KEPT and before["dead"] == 0 and
                 before["kept"] == EXPECTED_KEPT and
                 before["remaining-dsts-live"] and before["remaining-srcs-live"])
    if completed:
        print(json.dumps({"mode": "already-applied", "selected": 0,
                          "retracted": 0, "state": before},
                         ensure_ascii=False, sort_keys=True))
        return
    if (len(sigil_ids) != 405 or before["rows"] != EXPECTED_DEAD + EXPECTED_KEPT or
            before["dead"] != EXPECTED_DEAD or before["kept"] != EXPECTED_KEPT or
            not before["remaining-srcs-live"]):
        raise RuntimeError(f"selection gate failed; nothing retracted: "
                           f"sigils={len(sigil_ids)} state={before}")

    preview = {"mode": "apply" if args.apply else "dry-run",
               "sigils": len(sigil_ids), "selected": len(dead), "state": before}
    if not args.apply:
        print(json.dumps(preview, ensure_ascii=False, sort_keys=True))
        return

    failed = []
    retracted = 0
    probe = dead[0]
    status, payload = retract([probe])
    if status != 200 or payload.get("count") != 1:
        failed.append({"id": probe.get("relation/id"), "status": status,
                       "error": payload})
    else:
        probe_rows = relation_rows("prototype/has-sigil")
        probe_ids = {row["relation/id"] for row in probe_rows}
        if probe["relation/id"] in probe_ids or len(probe_rows) != before["rows"] - 1:
            failed.append({"id": probe["relation/id"], "status": "verify-failed",
                           "error": {"rows": len(probe_rows),
                                     "id-still-present": probe["relation/id"] in probe_ids}})
        else:
            retracted = 1

    if not failed:
        for start in range(1, len(dead), 200):
            batch = dead[start:start + 200]
            status, payload = retract(batch)
            if status == 200 and payload.get("count") == len(batch):
                retracted += len(batch)
            else:
                failed.extend({"id": row["relation/id"], "status": status,
                               "error": payload} for row in batch)

    after, remaining = snapshot(sigil_ids, prototype_ids)
    if remaining:
        failed.extend({"id": row["relation/id"], "status": "still-present"}
                      for row in remaining)
    if (after["rows"] != EXPECTED_KEPT or after["dead"] != 0 or
            after["kept"] != EXPECTED_KEPT or not after["remaining-dsts-live"] or
            not after["remaining-srcs-live"] or after["controls"] != before["controls"]):
        failed.append({"status": "postcondition-failed", "error": after})

    receipt = {
        "applied-at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "host": "zone", "selected": len(dead), "retracted": retracted,
        "failed": failed,
        "id-form": 'exact :relation/id response string (for example #uuid "…")',
        "verify": {"before": before, "after": after}}
    atomic_write(args.receipt, receipt)
    print(json.dumps(receipt, ensure_ascii=False, sort_keys=True))
    if failed or retracted != EXPECTED_DEAD:
        raise RuntimeError("apply did not satisfy the receipt")


if __name__ == "__main__":
    main()
