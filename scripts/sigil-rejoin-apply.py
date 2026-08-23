#!/usr/bin/env python3
"""Apply the reviewed exact-title sigil rejoin through the public HTTP API."""

import argparse
import datetime as dt
import hashlib
import json
import os
import subprocess
import tempfile
import urllib.parse
import urllib.request
import urllib.error


BASE = "http://127.0.0.1:7073"
INPUT = "docs/sigil-rejoin-2026-08-23.edn"
RECEIPT = "docs/sigil-rejoin-apply-zone-2026-08-23.edn"


class Keyword(str):
    pass


def request(method, path, body=None, params=None):
    url = BASE + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Accept": "application/json",
                                          "Content-Type": "application/json",
                                          "x-penholder": "api"})
    try:
        with urllib.request.urlopen(req, timeout=180) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        raw = error.read().decode("utf-8", "replace")
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = {"raw": raw}
        return error.code, payload


def matched_pairs(path):
    expression = (
        "(require '[cheshire.core :as json]) "
        "(print (json/generate-string (:matched "
        f"(clojure.edn/read-string (slurp {json.dumps(path)})))))"
    )
    result = subprocess.run(["bb", "-e", expression], check=True,
                            text=True, capture_output=True)
    pairs = json.loads(result.stdout)
    if len(pairs) != 326 or any(pair.get("by") != "title-exact" for pair in pairs):
        raise RuntimeError("input is not the reviewed 326-pair exact-title bucket")
    return pairs


def relation_rows():
    status, payload = request("GET", "/api/alpha/relations",
                              params={"type": "pattern/has-sigil", "limit": 5000})
    if status != 200:
        raise RuntimeError(f"relation read failed: HTTP {status}: {payload}")
    rows = payload["relations"]
    if len(rows) != payload["count"]:
        raise RuntimeError("relation response was truncated")
    return rows


def latest(limit):
    status, payload = request("GET", "/api/alpha/entities/latest",
                              params={"type": "pattern/library", "limit": limit})
    if status != 200:
        raise RuntimeError(f"latest read failed: HTTP {status}: {payload}")
    return payload


def relation_body(pair):
    return {"type": "pattern/has-sigil", "src": pair["pattern-id"],
            "dst": pair["sigil-id"],
            "provenance": {"note": "sigil-rejoin-2026-08-23 title-exact",
                           "source": "docs/sigil-rejoin-2026-08-23.edn"}}


def edn(value, indent=0):
    if isinstance(value, Keyword): return ":" + value
    if value is None: return "nil"
    if value is True: return "true"
    if value is False: return "false"
    if isinstance(value, (int, float)): return str(value)
    if isinstance(value, str): return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        return "[" + " ".join(edn(item, indent + 1) for item in value) + "]"
    if isinstance(value, dict):
        return "{" + " ".join(f":{key} {edn(item, indent + 1)}"
                                for key, item in value.items()) + "}"
    raise TypeError(type(value))


def atomic_write(path, value):
    absolute = os.path.abspath(path)
    os.makedirs(os.path.dirname(absolute), exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".sigil-rejoin-apply-",
                                     dir=os.path.dirname(absolute), text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(edn(value) + "\n")
            stream.flush(); os.fsync(stream.fileno())
        os.replace(temporary, absolute)
    finally:
        if os.path.exists(temporary): os.unlink(temporary)


def main():
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", default=True)
    mode.add_argument("--apply", action="store_true")
    parser.add_argument("--input", default=INPUT)
    parser.add_argument("--receipt", default=RECEIPT)
    args = parser.parse_args()
    pairs = matched_pairs(args.input)
    before_latest = latest(1)
    before_rows = relation_rows()
    before_ids = {row["relation/id"] for row in before_rows}
    preview = {"mode": "apply" if args.apply else "dry-run", "attempted": len(pairs),
               "before": {"sigil-join": before_latest["sigil-join"],
                          "relations": len(before_rows)},
               "chunks": [1, 100, 100, 100, 25]}
    if not args.apply:
        print(json.dumps(preview, ensure_ascii=False))
        return

    failed = []
    returned_ids = []
    batches = [pairs[:1]] + [pairs[i:i + 100] for i in range(1, len(pairs), 100)]
    for batch_index, batch in enumerate(batches):
        status, payload = request("POST", "/api/alpha/relations/batch",
                                  {"relations": [relation_body(pair) for pair in batch]})
        if status != 200:
            for pair in batch:
                failed.append({"pattern-id": pair["pattern-id"],
                               "sigil-id": pair["sigil-id"], "status": status,
                               "error": json.dumps(payload, ensure_ascii=False)})
            continue
        returned = payload.get("relations", [])
        returned_ids.extend(item["id"] for item in returned)
        if batch_index == 0:
            first = returned[0] if len(returned) == 1 else {}
            expected = pairs[0]
            if (first.get("src-id") != expected["pattern-id"] or
                    first.get("dst-id") != expected["sigil-id"]):
                raise RuntimeError(f"first relation resolved incorrectly: {first}")

    after_rows = relation_rows()
    after_ids = {row["relation/id"] for row in after_rows}
    after_latest = latest(2000)
    sigiled = sum(1 for entity in after_latest["entities"] if entity.get("sigiled?"))
    written_ids = set(returned_ids) - before_ids
    noop_ids = set(returned_ids) & before_ids
    receipt = {
        "applied-at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "host": "zone",
        "input-sha": subprocess.check_output(
            ["git", "rev-parse", f"HEAD:{args.input}"], text=True).strip(),
        "attempted": len(pairs), "written": len(written_ids),
        "no-op": len(noop_ids), "failed": failed,
        "verify": {"before": {"sigil-join": before_latest["sigil-join"]},
                   "after": {"sigil-join": after_latest["sigil-join"]},
                   "relation-count": len(after_rows), "sigiled-entities": sigiled}}
    atomic_write(args.receipt, receipt)
    if failed or len(returned_ids) != len(pairs):
        raise RuntimeError(f"batch failures: {len(failed)}; returned {len(returned_ids)}")
    if (after_latest["sigil-join"].get("patterns") != 1372 or
            after_latest["sigil-join"].get("matched") != 326 or
            len(after_rows) != 2106 or sigiled != 326):
        raise RuntimeError(f"postcondition failed: {receipt['verify']}")
    if not before_ids.issubset(after_ids):
        raise RuntimeError("existing relation disappeared")
    print(json.dumps(receipt, ensure_ascii=False))


if __name__ == "__main__":
    main()
