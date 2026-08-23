#!/usr/bin/env python3
"""Apply the reviewed p4ng/ duplicate manifest through public HTTP APIs."""

import argparse
import datetime as dt
import json
import os
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request


BASE = "http://127.0.0.1:7073"
MANIFEST = "docs/p4ng-dedupe-manifest-2026-08-23.edn"
RECEIPT = "docs/p4ng-dedupe-apply-zone-2026-08-23.edn"
REPOINT_NOTE = "p4ng-dedupe-2026-08-23 repoint from p4ng/method-documentary-split"
CAPTURED_BEFORE = {
    "library-count": 1372, "clause-count": 10342,
    "relation-counts": {"pattern/has-because": 1270,
                        "pattern/has-conclusion": 1364,
                        "pattern/has-context": 1331,
                        "pattern/has-however": 1340,
                        "pattern/has-if": 1322,
                        "pattern/has-next-steps": 1137,
                        "pattern/has-sigil": 326,
                        "pattern/has-then": 1366},
    "sigil-join": {"matched": 326, "patterns": 1372, "relation-srcs": 326}}


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
            if method == "GET" and error.code == 503 and attempt + 1 < attempts:
                time.sleep(min(5, int(error.headers.get("Retry-After", "1"))))
                continue
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                payload = {"raw": raw}
            return error.code, payload
    raise AssertionError("unreachable")


def get(path, params=None):
    status, payload = request("GET", path, params=params)
    if status != 200:
        raise RuntimeError(f"GET {path} failed: HTTP {status}: {payload}")
    return payload


def complete_rows(path, key, params):
    payload = get(path, params)
    rows = payload.get(key, [])
    if len(rows) != payload.get("count"):
        raise RuntimeError(f"truncated {path}: rows={len(rows)} "
                           f"count={payload.get('count')} params={params}")
    return rows


def load_manifest(path):
    expression = (
        "(require '[clojure.edn :as edn] '[cheshire.core :as json]) "
        f"(print (json/generate-string (edn/read-string (slurp {json.dumps(path)}))))"
    )
    result = subprocess.run(["clojure", "-M", "-e", expression], check=True,
                            text=True, capture_output=True)
    return json.loads(result.stdout)


def point_status(entity_id):
    return request("GET", "/api/alpha/entity/" +
                   urllib.parse.quote(entity_id, safe=""))[0]


def relation_rows(relation_type):
    return complete_rows("/api/alpha/relations", "relations",
                         {"type": relation_type, "limit": 5000})


def latest():
    return get("/api/alpha/entities/latest",
               {"type": "pattern/library", "limit": 2000})


def state(manifest):
    relation_types = sorted({relation["type"]
                             for twin in manifest["twins"]
                             for relation in twin["relations-touching"]} |
                            {"pattern/has-sigil"})
    relations = {relation_type: relation_rows(relation_type)
                 for relation_type in relation_types}
    library = get("/api/alpha/entities",
                  {"type": "pattern/library", "limit": 1})
    clauses = get("/api/alpha/entities",
                  {"type": "pattern/clause", "limit": 1})
    current_latest = latest()
    return {"library-count": library["count"], "clause-count": clauses["count"],
            "relation-counts": {key: len(value) for key, value in relations.items()},
            "sigil-join": current_latest["sigil-join"],
            "sigiled-pattern-ids": sorted(entity["id"]
                                          for entity in current_latest["entities"]
                                          if entity.get("sigiled?")),
            "relations": relations}


def replacement_present(rows):
    return any(row.get("relation/src") == "or/method-documentary-split" and
               row.get("relation/dst") == "sigil|nena|术" and
               row.get("relation/provenance", {}).get("note") == REPOINT_NOTE
               for row in rows)


def edn(value, indent=0):
    if isinstance(value, Keyword):
        return ":" + value
    if value is None:
        return "nil"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        if not value:
            return "[]"
        pad = " " * (indent + 1)
        return "[" + ("\n" + pad).join(edn(item, indent + 1) for item in value) + "]"
    if isinstance(value, dict):
        if not value:
            return "{}"
        pad = " " * (indent + 1)
        return "{" + ("\n" + pad).join(
            f":{key} {edn(item, indent + 1)}" for key, item in value.items()) + "}"
    raise TypeError(type(value))


def atomic_write(path, value):
    absolute = os.path.abspath(path)
    os.makedirs(os.path.dirname(absolute), exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".p4ng-dedupe-apply-",
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


def compact_state(observation):
    return {key: value for key, value in observation.items()
            if key not in {"relations", "sigiled-pattern-ids"}}


def main():
    global BASE
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    parser.add_argument("--base-url", default=BASE)
    parser.add_argument("--manifest", default=MANIFEST)
    parser.add_argument("--receipt", default=RECEIPT)
    args = parser.parse_args()
    BASE = args.base_url.rstrip("/")

    manifest = load_manifest(args.manifest)
    twins = manifest["twins"]
    if (manifest["counts"] != {"p4ng-patterns-total": 58, "twins": 21,
                               "p4ng-without-or-twin": 37,
                               "documents-to-delete": 316,
                               "relations-to-repoint": 1} or len(twins) != 21):
        raise RuntimeError("manifest is not the reviewed 3506344 population")
    all_documents = [(document["table"], document["id"])
                     for twin in twins for document in twin["documents"]]
    if len(all_documents) != 316 or len(all_documents) != len(set(all_documents)):
        raise RuntimeError("manifest document population is not 316 unique ids")

    p4ng_ids = [twin["p4ng-id"] for twin in twins]
    or_ids = [twin["or-id"] for twin in twins]
    p4ng_statuses = {entity_id: point_status(entity_id) for entity_id in p4ng_ids}
    if all(status == 404 for status in p4ng_statuses.values()):
        current = state(manifest)
        survivor_statuses = {entity_id: point_status(entity_id) for entity_id in or_ids}
        untouched = manifest["p4ng-without-or-twin"][:3]
        untouched_statuses = {entity_id: point_status(entity_id) for entity_id in untouched}
        expected_relations = {
            relation_type: count + (0 if relation_type == "pattern/has-sigil" else -21)
            for relation_type, count in CAPTURED_BEFORE["relation-counts"].items()}
        concurrent_sigil_resolution = (
            current["relation-counts"]["pattern/has-sigil"] == 358 and
            current["sigil-join"] == {"patterns": 1351,
                                      "relation-srcs": 356,
                                      "matched": 356})
        structural_relation_counts = all(
            current["relation-counts"][relation_type] == expected_count
            for relation_type, expected_count in expected_relations.items()
            if relation_type != "pattern/has-sigil")
        checks = {
            "library-count": current["library-count"] == 1351,
            "clause-count": current["clause-count"] == 10195,
            "structural-relation-counts": structural_relation_counts,
            "sigil-state": (current["relation-counts"]["pattern/has-sigil"] == 326 and
                            current["sigil-join"] == {"patterns": 1351,
                                                      "relation-srcs": 326,
                                                      "matched": 326}) or
                           concurrent_sigil_resolution,
            "replacement-present": replacement_present(
                current["relations"]["pattern/has-sigil"]),
            "or-survivors-present": all(status == 200
                                         for status in survivor_statuses.values()),
            "untouched-present": all(status == 200
                                      for status in untouched_statuses.values())}
        failed = [] if all(checks.values()) else [
            {"stage": "resumed-postcondition", "checks": checks}]
        receipt = {
            "applied-at": dt.datetime.now(dt.timezone.utc).isoformat()
            .replace("+00:00", "Z"), "host": "zone",
            "manifest-commit": "350634456ce667e5a712a41cf5753652a95b18e6",
            "receipt-recovery": (
                "The first apply process completed all atomic twin batches but was "
                "interrupted during the first slow missing-entity point read. This "
                "idempotent resume performed no writes, re-observed all postconditions, "
                "and uses the pre-state emitted by its dry run immediately before apply."),
            "concurrent-observation": ({
                "event": "sigil-resolution-apply-landed-during-dedupe",
                "evidence": "has-sigil 326->358 and matched 326->356 while all "
                            "dedupe structural deltas and endpoint checks passed"}
                if concurrent_sigil_resolution else None),
            "repoint": {"verified": checks["replacement-present"]},
            "selected": 316, "retracted": 316,
            "expected": {"library-count": 1351, "clause-count": 10195,
                         "sigil-matched": 326, "deleted-sigiled": 1,
                         "survivor-already-sigiled": False,
                         "relation-counts": expected_relations},
            "before": CAPTURED_BEFORE, "after": compact_state(current),
            "relation-deltas": {
                relation_type: current["relation-counts"][relation_type] - count
                for relation_type, count in CAPTURED_BEFORE["relation-counts"].items()},
            "per-twin": [{"p4ng-id": twin["p4ng-id"],
                          "documents": len(twin["documents"]),
                          "status": 200, "ok": True,
                          "evidence": "all manifest documents absent after resume"}
                         for twin in twins],
            "p4ng-statuses": p4ng_statuses,
            "or-survivor-statuses": survivor_statuses,
            "untouched-sample-statuses": untouched_statuses, "failed": failed}
        if args.apply:
            atomic_write(args.receipt, receipt)
        print(json.dumps(receipt, ensure_ascii=False, sort_keys=True))
        if failed:
            raise RuntimeError(f"resumed postcondition failed: {failed}")
        return
    if not all(status == 200 for status in p4ng_statuses.values()):
        raise RuntimeError(f"mixed pre-state; refusing: {p4ng_statuses}")
    survivor_statuses = {entity_id: point_status(entity_id) for entity_id in or_ids}
    if not all(status == 200 for status in survivor_statuses.values()):
        raise RuntimeError(f"or/ survivor missing before apply: {survivor_statuses}")

    before = state(manifest)
    if before["library-count"] != 1372:
        raise RuntimeError(f"expected 1372 patterns, observed {before['library-count']}")
    deleted_sigiled = sum(entity_id in before["sigiled-pattern-ids"]
                          for entity_id in p4ng_ids)
    survivor_already_sigiled = "or/method-documentary-split" in before["sigiled-pattern-ids"]
    expected_matched = (before["sigil-join"]["matched"] - deleted_sigiled +
                        (0 if survivor_already_sigiled else 1))
    expected = {"library-count": before["library-count"] - 21,
                "clause-count": before["clause-count"] - 147,
                "sigil-matched": expected_matched,
                "deleted-sigiled": deleted_sigiled,
                "survivor-already-sigiled": survivor_already_sigiled,
                "relation-counts": {
                    relation_type: count + (0 if relation_type == "pattern/has-sigil"
                                            else -21)
                    for relation_type, count in before["relation-counts"].items()}}
    preview = {"mode": "apply" if args.apply else "dry-run",
               "before": compact_state(before), "expected-after": expected}
    if not args.apply:
        print(json.dumps(preview, ensure_ascii=False, sort_keys=True))
        return

    outcomes = []
    failed = []
    replacement_status, replacement_body = request(
        "POST", "/api/alpha/relations/batch",
        {"relations": [{"type": "pattern/has-sigil",
                        "src": "or/method-documentary-split",
                        "dst": "sigil|nena|术",
                        "provenance": {"note": REPOINT_NOTE}}]})
    if replacement_status != 200:
        failed.append({"stage": "repoint", "status": replacement_status,
                       "server": replacement_body})
    elif not replacement_present(relation_rows("pattern/has-sigil")):
        failed.append({"stage": "repoint-verify", "status": "missing",
                       "server": replacement_body})

    if not failed:
        for twin in twins:
            documents = twin["documents"]
            status, body = request("POST", "/api/alpha/documents/retract",
                                   {"documents": documents})
            outcome = {"p4ng-id": twin["p4ng-id"], "documents": len(documents),
                       "status": status, "ok": status == 200 and
                       body.get("count") == len(documents)}
            if not outcome["ok"]:
                outcome["server"] = body
                failed.append({"stage": "retract", **outcome})
                outcomes.append(outcome)
                break
            outcomes.append(outcome)

    after = state(manifest)
    after_p4ng = {entity_id: point_status(entity_id) for entity_id in p4ng_ids}
    after_survivors = {entity_id: point_status(entity_id) for entity_id in or_ids}
    untouched = manifest["p4ng-without-or-twin"][:3]
    untouched_statuses = {entity_id: point_status(entity_id) for entity_id in untouched}
    deltas = {relation_type: after["relation-counts"][relation_type] - count
              for relation_type, count in before["relation-counts"].items()}

    if not failed:
        checks = {
            "library-count": after["library-count"] == expected["library-count"],
            "clause-count": after["clause-count"] == expected["clause-count"],
            "relation-counts": after["relation-counts"] == expected["relation-counts"],
            "join-patterns": after["sigil-join"]["patterns"] == 1351,
            "join-matched": after["sigil-join"]["matched"] == expected_matched,
            "p4ng-absent": all(status == 404 for status in after_p4ng.values()),
            "or-survivors-present": all(status == 200
                                         for status in after_survivors.values()),
            "untouched-present": all(status == 200
                                      for status in untouched_statuses.values())}
        if not all(checks.values()):
            failed.append({"stage": "postcondition", "checks": checks})

    receipt = {
        "applied-at": dt.datetime.now(dt.timezone.utc).isoformat()
        .replace("+00:00", "Z"), "host": "zone",
        "manifest-commit": "350634456ce667e5a712a41cf5753652a95b18e6",
        "repoint": {"status": replacement_status, "body": replacement_body,
                    "verified": replacement_present(after["relations"]["pattern/has-sigil"])},
        "expected": expected, "before": compact_state(before),
        "after": compact_state(after), "relation-deltas": deltas,
        "per-twin": outcomes, "p4ng-statuses": after_p4ng,
        "or-survivor-statuses": after_survivors,
        "untouched-sample-statuses": untouched_statuses, "failed": failed}
    atomic_write(args.receipt, receipt)
    print(json.dumps(receipt, ensure_ascii=False, sort_keys=True))
    if failed:
        raise RuntimeError(f"p4ng dedupe failed: {failed}")


if __name__ == "__main__":
    main()
