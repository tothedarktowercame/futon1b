#!/usr/bin/env python3
"""Apply a reviewed sigil-rejoin resolution through the public HTTP API."""

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
INPUT = "docs/sigil-rejoin-resolution-2026-08-23.edn"
RECEIPT = "docs/sigil-rejoin-resolution-apply-zone-2026-08-23.edn"


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


def resolution(path):
    expression = (
        "(require '[cheshire.core :as json]) "
        "(let [x (clojure.edn/read-string "
        f"(slurp {json.dumps(path)}))] "
        "(print (json/generate-string "
        "(select-keys x [:method :or-p4ng-ruling :counts :rows]))))"
    )
    result = subprocess.run(["bb", "-e", expression], check=True,
                            text=True, capture_output=True)
    document = json.loads(result.stdout)
    rows = document.get("rows", [])
    counts = document.get("counts", {})
    if (len(rows) != 79 or counts.get("total") != 79 or
            counts.get("pattern") != 32 or counts.get("prototype") != 47 or
            counts.get("retire") != 0):
        raise RuntimeError("input is not the reviewed 79-row resolution")
    for row in rows:
        pattern = row.get("pattern-id")
        prototype = row.get("prototype-id")
        if bool(pattern) == bool(prototype):
            raise RuntimeError(f"row must name exactly one endpoint: {row}")
        expected = ("pattern/has-sigil" if pattern else
                    "prototype/has-sigil")
        if row.get("relation-type") != expected:
            raise RuntimeError(f"row relation type disagrees with endpoint: {row}")
    return document


def relation_rows(relation_type):
    status, payload = request("GET", "/api/alpha/relations",
                              params={"type": relation_type, "limit": 5000})
    if status != 200:
        raise RuntimeError(f"relation read failed: HTTP {status}: {payload}")
    rows = payload["relations"]
    if len(rows) != payload["count"]:
        raise RuntimeError("relation response was truncated")
    return rows


def prototypes():
    status, payload = request("GET", "/api/alpha/entities",
                              params={"type": "devmap/prototype", "limit": 100})
    if status != 200:
        raise RuntimeError(f"prototype read failed: HTTP {status}: {payload}")
    rows = payload["entities"]
    if len(rows) != payload["count"] or len(rows) != 73:
        raise RuntimeError(f"prototype response was incomplete: {payload.get('count')}")
    by_name = {row["entity/name"]: row for row in rows}
    if len(by_name) != len(rows):
        raise RuntimeError("prototype entity names are not unique")
    return by_name


def latest(limit):
    status, payload = request("GET", "/api/alpha/entities/latest",
                              params={"type": "pattern/library", "limit": limit})
    if status != 200:
        raise RuntimeError(f"latest read failed: HTTP {status}: {payload}")
    return payload


def relation_body(row):
    return {"type": row["relation-type"],
            # Prototype names deliberately exercise the contract's name
            # resolution; pattern ids are already canonical entity names.
            "src": row.get("pattern-id") or row.get("prototype-id"),
            "dst": row["sigil-id"],
            "provenance": {
                "note": "sigil-rejoin-resolution-2026-08-23",
                "source": INPUT}}


def relation_shape(rows):
    def shape(value):
        if isinstance(value, str) and value.startswith('#uuid "'):
            return "#uuid-literal-string"
        return type(value).__name__
    return ({"src": shape(rows[0].get("relation/src")),
             "dst": shape(rows[0].get("relation/dst"))} if rows else None)


def expected_src(row, prototype_entities):
    if row.get("pattern-id"):
        return row["pattern-id"]
    return prototype_entities[row["prototype-id"]]["entity/id"]


def existing_relation(row, existing, prototype_entities):
    src = expected_src(row, prototype_entities)
    return next((relation for relation in existing
                 if relation.get("relation/src") == src and
                 relation.get("relation/dst") == row["sigil-id"] and
                 relation.get("relation/provenance", {}).get("note") ==
                 "sigil-rejoin-resolution-2026-08-23" and
                 relation.get("relation/provenance", {}).get("source") == INPUT),
                None)


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
    document = resolution(args.input)
    rows = document["rows"]
    pattern_rows = [row for row in rows if row.get("pattern-id")]
    prototype_rows = [row for row in rows if row.get("prototype-id")]
    prototype_entities = prototypes()
    missing = sorted({row["prototype-id"] for row in prototype_rows} -
                     set(prototype_entities))
    if missing:
        raise RuntimeError(f"prototype names did not resolve: {missing}")
    before_latest = latest(1)
    before_by_type = {kind: relation_rows(kind) for kind in
                      ("pattern/has-sigil", "prototype/has-sigil")}
    preview = {"mode": "apply" if args.apply else "dry-run", "attempted": len(rows),
               "before": {"sigil-join": before_latest["sigil-join"],
                          "relations": {kind: len(found) for kind, found in
                                        before_by_type.items()},
                          "prototype-relation-shape":
                          relation_shape(before_by_type["prototype/has-sigil"])},
               "planned": {"pattern/has-sigil": len(pattern_rows),
                           "prototype/has-sigil": len(prototype_rows)},
               "chunks": {"prototype/has-sigil": [1, 46],
                          "pattern/has-sigil": [32]}}
    if not args.apply:
        print(json.dumps(preview, ensure_ascii=False))
        return

    failed = {"pattern/has-sigil": [], "prototype/has-sigil": []}
    returned = {"pattern/has-sigil": [], "prototype/has-sigil": []}
    pending = {"pattern/has-sigil": [], "prototype/has-sigil": []}
    for row in rows:
        relation_type = row["relation-type"]
        if found := existing_relation(row, before_by_type[relation_type],
                                      prototype_entities):
            returned[relation_type].append({"id": found["relation/id"]})
        else:
            pending[relation_type].append(row)
    first_prototype = existing_relation(prototype_rows[0],
                                        before_by_type["prototype/has-sigil"],
                                        prototype_entities)
    prototype_batches = ([pending["prototype/has-sigil"]] if first_prototype
                         else [pending["prototype/has-sigil"][:1],
                               pending["prototype/has-sigil"][1:]])
    batches = ([("prototype/has-sigil", batch)
                for batch in prototype_batches] +
               [("pattern/has-sigil", pending["pattern/has-sigil"])])
    for relation_type, batch in batches:
        if not batch:
            continue
        status, payload = request("POST", "/api/alpha/relations/batch",
                                  {"relations": [relation_body(row) for row in batch]})
        if status != 200:
            for row in batch:
                failed[relation_type].append({
                    "source-id": row.get("pattern-id") or row.get("prototype-id"),
                    "sigil-id": row["sigil-id"], "status": status,
                    "error": json.dumps(payload, ensure_ascii=False)})
            continue
        batch_returned = payload.get("relations", [])
        returned[relation_type].extend(batch_returned)
        if first_prototype is None and relation_type == "prototype/has-sigil":
            first_prototype = batch_returned[0] if len(batch_returned) == 1 else {}
            expected = prototype_rows[0]
            expected_src = prototype_entities[expected["prototype-id"]]["entity/id"]
            if (first_prototype.get("src-id") != expected_src or
                    first_prototype.get("dst-id") != expected["sigil-id"]):
                raise RuntimeError(
                    f"first relation resolved incorrectly: {first_prototype}")

    after_by_type = {kind: relation_rows(kind) for kind in before_by_type}
    after_latest = latest(1)
    first_id = first_prototype.get("id") or first_prototype.get("relation/id")
    first_stored = next((row for row in after_by_type["prototype/has-sigil"]
                         if row["relation/id"] == first_id), None)
    expected_first_src = prototype_entities[prototype_rows[0]["prototype-id"]]["entity/id"]
    if not first_stored or first_stored.get("relation/src") != expected_first_src:
        raise RuntimeError("first prototype relation does not join to its entity id")
    outcomes = {}
    for relation_type in before_by_type:
        before_ids = {row["relation/id"] for row in before_by_type[relation_type]}
        returned_ids = {row["id"] for row in returned[relation_type]}
        outcomes[relation_type] = {
            "attempted": len(pattern_rows if relation_type == "pattern/has-sigil"
                             else prototype_rows),
            "written": len(returned_ids - before_ids),
            "no-op": len(returned_ids & before_ids),
            "failed": failed[relation_type]}
        if not before_ids.issubset({row["relation/id"] for row in
                                    after_by_type[relation_type]}):
            raise RuntimeError(f"existing {relation_type} relation disappeared")

    receipt = {
        "applied-at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "host": "zone",
        "input-sha": subprocess.check_output(
            ["git", "rev-parse", f"HEAD:{args.input}"], text=True).strip(),
        "resolution": {"method": document["method"],
                       "or-p4ng-ruling": document["or-p4ng-ruling"]},
        "attempted": len(rows), "by-type": outcomes,
        "failed": sum((items for items in failed.values()), []),
        "verify": {"before": {"sigil-join": before_latest["sigil-join"]},
                   "after": {"sigil-join": after_latest["sigil-join"]},
                   "relation-counts": {
                       kind: {"before": len(before_by_type[kind]),
                              "after": len(after_by_type[kind])}
                       for kind in before_by_type},
                   "prototype-relation-shape-before":
                   relation_shape(before_by_type["prototype/has-sigil"]),
                   "first-prototype": {
                       "prototype-id": prototype_rows[0]["prototype-id"],
                       "entity-id": expected_first_src,
                       "stored-src": first_stored["relation/src"]}}}
    atomic_write(args.receipt, receipt)
    failed_count = sum(len(items) for items in failed.values())
    if failed_count or sum(len(items) for items in returned.values()) != len(rows):
        raise RuntimeError(f"batch failures: {failed_count}; returned "
                           f"{sum(len(items) for items in returned.values())}")
    for relation_type, outcome in outcomes.items():
        if len(after_by_type[relation_type]) != (len(before_by_type[relation_type]) +
                                                 outcome["written"]):
            raise RuntimeError(f"unexpected {relation_type} count: {receipt['verify']}")
    print(json.dumps(receipt, ensure_ascii=False))


if __name__ == "__main__":
    main()
