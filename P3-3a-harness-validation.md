# P3-3a evidence execution-harness storage contract

Implementation: `ec24da6` on futon1b master. No futon3c changes or producer stamping.

Validation: clj-kondo, check-parens passed on the three touched Clojure files.
`clojure -M:node -m test-evidence-harness` ran only harness and origin namespaces:
4 tests, 66 assertions, no failures/errors. Disposable real XTDB nodes exercised
write-evidence!, fetch-by-id and LIST: all three currently valid kinds, absence,
origin preservation, refusal mutations, string enum normalization and namespaced
precedence. Zai remains a recognized but refused enum member.

Authorized live reload via scripts/futon1b-eval.sh succeeded for futon1b-harness
then futon1b-evidence; resource resolution returned
`file:/home/joe/code/futon1b/futon1b_harness.clj`. No restart, node recreation or
server namespace reload. Cheap /health returned ok true, deep false.

Live session: `p3-3a-harness-contract-20260927-codex5`.

- `p3-3a-none-d7278a43-5958-4049-8b24-5b5041e0ac99`: POST 201,
  rescue ok; GET 200 with harness `{kind: none, basis: producer-context}`.
- `p3-3a-absent-9771414a-9d94-4633-9dd5-c64841392352`: POST 201,
  rescue ok; GET 200 with no evidence/harness key.
- Zai request `p3-3a-zai-76979e93-3e68-4127-a4de-2c7352ba3d44`:
  POST 400, error/code invalid-harness, reason zai-harness-not-deployed;
  not a stored evidence id.

The first attempted none probe omitted x-penholder and was rejected 403
missing-penholder before writing; subsequent probes used the standard api header.
Raw successful probe responses are locally retained at /tmp/p3-3a-live-probes.json.

## Futon3c stripping answer

**Yes, the args-map path strips it.** `evidence/boundary.clj:177–203` converts
partial namespaced inputs into args maps; `evidence/store.clj:90–111` destructures
and rebuilds a fixed key list without harness. Add that preservation in P3-3b.
The complete valid EvidenceEntry path passes its map through, including extra keys
(the shape at social/shapes.clj:326 is open). `evidence/futon1b_backend.clj:203–210`
serializes the entire validated map with pr-str, and :447 uses that serializer;
the backend itself does not strip the field. Boundary coercion at :124 likewise
updates known values without selecting a restricted key set. Thus a blanket
“futon3c preserves it” would be false despite backend preservation.

Secondary harness-kind filtering is deferred: no candidate index, and adding the
nested map to common filter projection requires a bounded-query cost assessment.
No benchmark established a safe cost in this packet. API-CONTRACT explicitly says
the parameter is unsupported; callers must not assume it narrows LIST results.
