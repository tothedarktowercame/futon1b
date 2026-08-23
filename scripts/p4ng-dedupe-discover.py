#!/usr/bin/env python3
"""Build a read-only deletion manifest for p4ng/ duplicates of or/ patterns."""

import argparse
import datetime as dt
import json
import os
import re
import tempfile
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request


BASE = "http://127.0.0.1:7073"
OUTPUT = "docs/p4ng-dedupe-manifest-2026-08-23.edn"


class Keyword(str):
    pass


def request_json(path, params=None, attempts=30):
    url = BASE + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(request, timeout=180) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            if error.code == 503 and attempt + 1 < attempts:
                error.read()
                time.sleep(min(5, int(error.headers.get("Retry-After", "1"))))
                continue
            raise RuntimeError(f"GET {url} failed: HTTP {error.code}: "
                               f"{error.read().decode('utf-8', 'replace')}") from error
    raise AssertionError("unreachable")


def request_text(path):
    request = urllib.request.Request(BASE + path,
                                     headers={"Accept": "application/edn"})
    with urllib.request.urlopen(request, timeout=180) as response:
        return response.read().decode("utf-8")


def complete_rows(path, key, params):
    payload = request_json(path, params)
    rows = payload.get(key, [])
    if len(rows) != payload.get("count"):
        raise RuntimeError(f"truncated {path}: rows={len(rows)} "
                           f"count={payload.get('count')} params={params}")
    return rows


def pattern_relation_types():
    # The live catalog currently contains one historical `:type/id :` entry,
    # which makes the whole response invalid EDN/JSON. Parse each flat type doc
    # independently and retain only catalogued pattern/* relations.
    raw = request_text("/api/alpha/types")
    found = set()
    for document in re.findall(r"\{[^{}]*\}", raw):
        type_id = re.search(r":type/id\s+:?([^\s,}]+)", document)
        kind = re.search(r":type/kind\s+:?([^\s,}]+)", document)
        if (type_id and kind and kind.group(1) == "relation" and
                type_id.group(1).startswith("pattern/")):
            found.add(type_id.group(1))
    if "pattern/has-sigil" not in found:
        raise RuntimeError(f"pattern relation catalog incomplete: {sorted(found)}")
    return sorted(found)


def normalized_title(entity):
    value = unicodedata.normalize("NFKC", entity.get("entity/source", "")).casefold()
    return " ".join(re.sub(r"[^\w]+", " ", value).split())


def twins(patterns):
    by_id = {entity["entity/id"]: entity for entity in patterns}
    or_patterns = [entity for entity in patterns
                   if entity["entity/id"].startswith("or/")]
    by_title = {}
    for entity in or_patterns:
        by_title.setdefault(normalized_title(entity), []).append(entity["entity/id"])
    matched, unmatched = [], []
    for entity in sorted((item for item in patterns
                          if item["entity/id"].startswith("p4ng/")),
                         key=lambda item: item["entity/id"]):
        p4ng_id = entity["entity/id"]
        tail_twin = "or/" + p4ng_id.removeprefix("p4ng/")
        if tail_twin in by_id:
            matched.append((entity, by_id[tail_twin], Keyword("slug-tail")))
            continue
        candidates = by_title.get(normalized_title(entity), [])
        if len(candidates) == 1:
            matched.append((entity, by_id[candidates[0]], Keyword("title")))
        elif candidates:
            raise RuntimeError(f"ambiguous title twin for {p4ng_id}: {candidates}")
        else:
            unmatched.append(p4ng_id)
    return matched, unmatched


def entity(endpoint):
    path = "/api/alpha/entity/" + urllib.parse.quote(endpoint, safe="")
    request = urllib.request.Request(BASE + path,
                                     headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.load(response).get("entity")
    except urllib.error.HTTPError as error:
        if error.code == 404:
            error.read()
            return None
        raise


def relation_manifest(row, or_id):
    relation_type = row["relation/type"]
    action = "repoint-to-or" if relation_type == "pattern/has-sigil" else "delete"
    return {"table": Keyword("relations"), "id": row["relation/id"],
            "type": Keyword(relation_type), "src": row.get("relation/src"),
            "dst": row.get("relation/dst"), "action": Keyword(action),
            **({"repoint-src": or_id} if action == "repoint-to-or" else {})}


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
        parts = [f":{key} {edn(item, indent + 1)}" for key, item in value.items()]
        return "{" + ("\n" + pad).join(parts) + "}"
    raise TypeError(type(value))


def atomic_write(path, value):
    absolute = os.path.abspath(path)
    os.makedirs(os.path.dirname(absolute), exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".p4ng-dedupe-",
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
    global BASE
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default=BASE)
    parser.add_argument("--output", default=OUTPUT)
    args = parser.parse_args()
    BASE = args.base_url.rstrip("/")

    patterns = complete_rows("/api/alpha/entities", "entities",
                             {"type": "pattern/library", "limit": 2000})
    paired, unmatched = twins(patterns)
    p4ng_total = sum(entity_["entity/id"].startswith("p4ng/") for entity_ in patterns)
    if len(paired) != 21:
        raise RuntimeError(f"expected 21 p4ng/or twins, observed {len(paired)}; "
                           f"unmatched={unmatched}")

    relation_types = pattern_relation_types()
    relations = []
    for relation_type in relation_types:
        relations.extend(complete_rows(
            "/api/alpha/relations", "relations",
            {"type": relation_type, "limit": 5000}))
    relation_ids = [row["relation/id"] for row in relations]
    if len(relation_ids) != len(set(relation_ids)):
        raise RuntimeError("relation catalog queries returned duplicate relation ids")

    twin_records = []
    documents_to_delete = set()
    relations_to_repoint = 0
    for p4ng, or_pattern, twin_by in paired:
        p4ng_id, or_id = p4ng["entity/id"], or_pattern["entity/id"]
        touching = [row for row in relations
                    if p4ng_id in (row.get("relation/src"), row.get("relation/dst"))]
        touching_records = [relation_manifest(row, or_id) for row in touching]
        relations_to_repoint += sum(record["action"] == "repoint-to-or"
                                    for record in touching_records)

        documents = [{"table": Keyword("entities"), "id": p4ng_id}]
        documents.extend({"table": Keyword("relations"), "id": row["relation/id"]}
                         for row in touching)

        # Non-sigil outgoing endpoints are clause candidates. Delete an entity
        # only when no relation other than this p4ng pattern references it and
        # the surviving or/ twin does not point at it.
        candidates = {row.get("relation/dst") for row in touching
                      if row.get("relation/src") == p4ng_id and
                      row.get("relation/type") != "pattern/has-sigil"}
        for candidate in sorted(item for item in candidates if item):
            references = [row for row in relations
                          if candidate in (row.get("relation/src"), row.get("relation/dst"))]
            shared_with_or = any(or_id in (row.get("relation/src"),
                                           row.get("relation/dst"))
                                 for row in references)
            foreign = [row for row in references
                       if p4ng_id not in (row.get("relation/src"),
                                         row.get("relation/dst"))]
            observed_entity = entity(candidate)
            if observed_entity and not shared_with_or and not foreign:
                documents.append({"table": Keyword("entities"), "id": candidate})

        unique_documents = []
        seen = set()
        for document in documents:
            key = (document["table"], document["id"])
            if key not in seen:
                seen.add(key)
                unique_documents.append(document)
                documents_to_delete.add(key)
        twin_records.append({"p4ng-id": p4ng_id, "or-id": or_id,
                             "twin-by": twin_by, "documents": unique_documents,
                             "relations-touching": touching_records})

    manifest = {
        "generated-at": dt.datetime.now(dt.timezone.utc).isoformat()
        .replace("+00:00", "Z"),
        "host": "zone", "twins": twin_records,
        "p4ng-without-or-twin": unmatched,
        "counts": {"p4ng-patterns-total": p4ng_total, "twins": len(paired),
                   "p4ng-without-or-twin": len(unmatched),
                   "documents-to-delete": len(documents_to_delete),
                   "relations-to-repoint": relations_to_repoint},
        "method": (
            "Read-only live API enumeration. Patterns are twins first by identical "
            "slug tail (p4ng/X to or/X), then by a unique NFKC/casefolded title when "
            "the slug changed. Every catalogued pattern/* relation type was fetched "
            "with limit 5000 and required rows=count. Each twin manifest includes its "
            "pattern entity and every relation touching its p4ng id. has-sigil rows are "
            "marked repoint-to-or; all other touching relations are marked delete. A "
            "non-sigil outgoing entity is included only if it exists and no relation "
            "outside that p4ng pattern references it, which also proves the or/ survivor "
            "does not share it. No mutation endpoint was called.")}
    atomic_write(args.output, manifest)
    print(json.dumps({"counts": manifest["counts"], "without": unmatched,
                      "relation-types": relation_types, "output": args.output},
                     ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
