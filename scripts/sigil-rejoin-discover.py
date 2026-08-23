#!/usr/bin/env python3
"""Read-only reconstruction of pattern/library -> pattern/sigil title joins."""

import argparse
import datetime as dt
import difflib
import json
import os
import re
import tempfile
import unicodedata
import urllib.parse
import urllib.request
from collections import defaultdict


class Keyword(str):
    pass


def get_json(base, path, params):
    url = base.rstrip("/") + path + "?" + urllib.parse.urlencode(params)
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=120) as response:
        if response.status != 200:
            raise RuntimeError(f"GET {url} returned HTTP {response.status}")
        return json.load(response)


def normalize_title(value):
    value = unicodedata.normalize("NFKC", value).casefold()
    value = re.sub(r"[^\w]+", " ", value, flags=re.UNICODE)
    return " ".join(value.split())


def sigil_title(entity):
    source = entity.get("entity/source", "")
    title, separator, _ = source.partition(" -> ")
    if not separator:
        raise ValueError(f"sigil source lacks ' -> ' delimiter: {source!r}")
    return title.strip()


def edn(value, indent=0):
    if isinstance(value, Keyword):
        return ":" + value
    if value is None:
        return "nil"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, list):
        if not value:
            return "[]"
        pad = " " * (indent + 1)
        return "[" + ("\n" + pad).join(edn(x, indent + 1) for x in value) + "]"
    if isinstance(value, dict):
        if not value:
            return "{}"
        pad = " " * (indent + 1)
        parts = [f":{k} {edn(v, indent + 1)}" for k, v in value.items()]
        return "{" + ("\n" + pad).join(parts) + "}"
    raise TypeError(type(value))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:7073")
    parser.add_argument("--output", default="docs/sigil-rejoin-2026-08-23.edn")
    args = parser.parse_args()

    patterns_response = get_json(args.base_url, "/api/alpha/entities", {"type": "pattern/library", "limit": 2000})
    sigils_response = get_json(args.base_url, "/api/alpha/entities", {"type": "pattern/sigil", "limit": 5000})
    relations_response = get_json(args.base_url, "/api/alpha/relations", {"type": "pattern/has-sigil", "limit": 5000})
    patterns = patterns_response["entities"]
    sigils = sigils_response["entities"]
    relations = relations_response["relations"]

    if len(patterns) != patterns_response["count"] or len(sigils) != sigils_response["count"] or len(relations) != relations_response["count"]:
        raise RuntimeError("endpoint response was paginated/truncated despite the bounded limit")

    exact = defaultdict(list)
    normalized = defaultdict(list)
    title_by_slug = {}
    for pattern in patterns:
        slug, title = pattern["entity/id"], pattern["entity/source"].strip()
        title_by_slug[slug] = title
        exact[title].append(slug)
        normalized[normalize_title(title)].append(slug)

    matched, ambiguous, unmatched = [], [], []
    extra_normalized = 0
    normalized_ambiguities = 0
    normalized_titles = list(normalized)
    for sigil in sorted(sigils, key=lambda x: x["entity/id"]):
        sid, title = sigil["entity/id"], sigil_title(sigil)
        candidates = sorted(exact.get(title, []))
        if len(candidates) == 1:
            matched.append({"pattern-id": candidates[0], "sigil-id": sid, "by": Keyword("title-exact")})
        elif len(candidates) > 1:
            ambiguous.append({"sigil-id": sid, "candidates": candidates, "title": title, "by": Keyword("title-exact")})
        else:
            norm = normalize_title(title)
            candidates = sorted(normalized.get(norm, []))
            if len(candidates) == 1:
                extra_normalized += 1
                matched.append({"pattern-id": candidates[0], "sigil-id": sid, "by": Keyword("title-normalized")})
            elif len(candidates) > 1:
                normalized_ambiguities += 1
                ambiguous.append({"sigil-id": sid, "candidates": candidates, "title": title, "by": Keyword("title-normalized")})
            else:
                nearest_norms = difflib.get_close_matches(norm, normalized_titles, n=3, cutoff=0.45)
                nearest = []
                for nearest_norm in nearest_norms:
                    nearest.extend(normalized[nearest_norm])
                unmatched.append({"sigil-id": sid, "title-prefix": title, "nearest": sorted(dict.fromkeys(nearest))})

    bucket_ids = [x["sigil-id"] for x in matched + ambiguous + unmatched]
    expected_ids = [x["entity/id"] for x in sigils]
    if len(bucket_ids) != len(set(bucket_ids)) or set(bucket_ids) != set(expected_ids):
        raise RuntimeError("sigil partition invariant failed")

    srcs = {relation["relation/src"] for relation in relations}
    dsts = {relation["relation/dst"] for relation in relations}
    attached_patterns = {item["pattern-id"] for item in matched}
    method = (
        "The script takes each sigil title as the exact, trimmed prefix before the literal ' -> ' in :entity/source. "
        "It first joins that title byte-for-byte to trimmed pattern :entity/source titles; a unique title is :title-exact and duplicate-title hits are ambiguous. "
        "As a second pass it applies Unicode NFKC, case-folding, replacement of punctuation/non-word runs with one space, and whitespace collapse; unique hits are high-confidence :title-normalized matches and duplicate hits remain ambiguous. "
        f"The normalized pass recovered {extra_normalized} additional sigils and introduced {normalized_ambiguities} additional ambiguities. "
        "For still-unmatched sigils, :nearest lists up to three pattern-title groups selected by Python difflib.SequenceMatcher similarity over the same normalized titles (cutoff 0.45); nearest values are suggestions only and are not joins."
    )
    receipt = {
        "generated-at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "store": "migration-store-21", "host": "zone",
        "counts": {"patterns": len(patterns), "sigils": len(sigils), "has-sigil-relations": len(relations),
                   "distinct-rel-srcs": len(srcs), "distinct-rel-dsts": len(dsts)},
        "matched": matched, "ambiguous": ambiguous, "unmatched-sigils": unmatched,
        "patterns-without-sigil-count": len(patterns) - len(attached_patterns), "method": method,
    }
    output = os.path.abspath(args.output)
    os.makedirs(os.path.dirname(output), exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".sigil-rejoin-", dir=os.path.dirname(output), text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(edn(receipt) + "\n")
            stream.flush(); os.fsync(stream.fileno())
        os.replace(temporary, output)
    finally:
        if os.path.exists(temporary): os.unlink(temporary)
    print(json.dumps({"counts": receipt["counts"], "matched": len(matched), "ambiguous": len(ambiguous), "unmatched": len(unmatched), "output": output}))


if __name__ == "__main__":
    main()
