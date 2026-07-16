# futon1a substrate HTTP API — route-by-route contract spec

This is the contract `futon1b_server.clj` must implement for the operational
switchover (E-futon1b-operational-switchover in futon2/holes). Extracted from
futon1a source 2026-07-10 at boundary commit `a71c399`. File:line references
are into futon1a at that commit.

Sources: `src/futon1a/http/app.clj` (dispatch), `src/futon1a/api/routes.clj`
(handlers), `src/futon1a/compat/futon1_write.clj` (doc builders / id minting),
`src/futon1a/core/pipeline.clj` (gates), `src/futon1a/api/snapshot.clj`,
`src/futon1a/auth/penholder.clj`, `src/futon1a/ingest/open_world.clj`,
`src/futon1a/model/validation.clj`, `src/futon1a/core/invariants.clj`,
`src/futon1a/core/xtdb.clj`, `src/futon1a/system.clj`.

---

## 0. Transport, encoding, routing mechanics

**Request body parsing** (`app.clj:83-88` `parse-body`):
- If request `Content-Type` contains `application/json` → cheshire
  `json/decode` with **keywordized keys** (`app.clj:73-81`). JSON key
  `"evidence/id"` becomes the namespaced keyword `:evidence/id`, so JSON and
  EDN clients hit the same code paths.
- Otherwise → **EDN** via `edn/read-string` with `*data-readers*`
  (`app.clj:60-71`). Empty/absent body → `{}`. A non-map body is wrapped as
  `{:_body v}`.

**Response serialization** (`app.clj:47-58` `wants-json?`/`response`):
- If the request's `Accept` **or** `Content-Type` header contains
  `application/json` → JSON via cheshire (`{:escape-non-ascii true}`),
  `Content-Type: application/json; charset=utf-8` (`app.clj:40-45`). Note:
  EDN keywords serialize as strings without the leading colon
  (`:pattern/library` → `"pattern/library"`); map keys like `:evidence/id` →
  `"evidence/id"`.
- Otherwise → **EDN** `pr-str`, `Content-Type: application/edn;
  charset=utf-8` (`app.clj:33-38`). EDN is the default and canonical format.
- No transit anywhere.

**Query params**: parsed with `ring.util.codec/form-decode` on the raw query
string (`app.clj:213`) — a map of **string keys → string values**.

**`/api/alpha` aliasing** (`app.clj:109-131`): `alpha-strip` returns the
suffix after `/api/alpha` (`"/api/alpha/types"` → `"/types"`, bare
`/api/alpha` → `"/"`); `api-strip` does the same for `/api` **excluding**
`/api/alpha` (for Arxana's `arxana-store`, which normalizes its base URL to
`/api`). Dispatch is one big `cond` in `ring-handler` (`app.clj:210-1042`);
some routes match the literal full `uri` (all evidence GETs, hyperedge GETs,
census, patterns/activation, POST `/api/alpha/entity`, POST
`/api/alpha/relation`, POST `/api/alpha/evidence`), others match
`alpha-uri`/`api-uri` (hyperedge POST, snapshot, types, entity reads). Net
effect: everything below is reachable under `/api/alpha/...`; `/entity`,
`/relation`, `/hyperedge`, `/relations/batch`, `/snapshot`,
`/snapshot/restore` also work under bare `/api/...`; `/types*`,
`/entity/{id}` (GET) and `/write` also exist at the root. **Match order
matters**: `/api/alpha/evidence/sessions` and `.../chain` are matched before
the generic `/api/alpha/evidence/{id}` prefix.

**Penholder resolution for compat writes** (`app.clj:176-182`
`compat-penholder`): body `:penholder` → `x-penholder` header (trimmed,
non-empty) → server default `:compat/penholder` (env
`FUTON1A_COMPAT_PENHOLDER`, `system.clj:190`). Exceptions: `/write`,
`/ingest`, `/types/parent`, `/types/merge` build the request from the body
only (`base-req`, `app.clj:217-221`) — for those, `:penholder` **must be in
the body**; there is no header fallback.

**Error envelope** (uniform): layered `ex-info`s are mapped by
`errors/error->response` (`errors.clj:8-25`) to
`{:error {:layer <0|1|2|3|4> :reason <kw> :context <map>}}` with status from
the layer: **L4=400** (model/validation, `validation.clj:8-13`), **L3=403**
(penholder, `penholder.clj:9-14`), **L2=500** (integrity,
`invariants.clj:9-14`), **L1=409** (identity, `identity.clj:9-12`),
**L0=503** (durability, `xtdb.clj:14-19`). Unexpected exceptions → `500
{:error {:reason :exception :message ...}}` (`routes.clj:45-49`). Unknown
GET → `404 {:error {:reason :not-found :path :method}}`; other methods →
`405` (`app.clj:90-101`).

---

## 1. The write pipeline (shared by all writes)

Every mutation goes through `pipeline/run-write!` (`pipeline.clj:52-150`) or
`pipeline/run-open-world!` (`pipeline.clj:243-301`), strictly L4→L3→L2→L1→L0:

1. **L4 — model validation (400)**: `expansion/expansion-gate!` (no-op unless
   `:expansion` supplied); `mv/validate-model!` (`validation.clj:15-32`) —
   model must be a map containing every `:required-keys` key; tx-ops must be
   a vector of vectors.
2. **L4 — canonical-id gate** (`pipeline.clj:77-78` →
   `open-world/gate-entity-id!`, `open_world.clj:28-58`): runs only when the
   `:model` map has `:entity/type`. Looks up a registered descriptor for that
   type (`registry/get-model`); if the descriptor carries `:id-pattern`, the
   value of `:id-field` (default `:entity/id`) is classified
   (`open_world.clj:11-26`): matches `:id-pattern` → accept; matches any
   `:queue` regex → write proceeds but is recorded on the gate queue
   (`gate-queue/record!`, visible at `GET /meta/model/queue`,
   `routes.clj:102-107`); otherwise → recorded + **thrown as L4 400
   `:non-canonical-id`**. The `:mission/doc` contract is seeded at boot
   (`system.clj:135`). No descriptor / no `:id-pattern` → no-op.
3. **L3 — authorization (403)**: `auth/authorize!` (`penholder.clj:21-37`) —
   penholder must be a non-blank string **and** a member of
   `allowed-penholders` (env `FUTON1A_ALLOWED_PENHOLDERS`, comma-separated;
   `system.clj:188`). Errors: `:missing-penholder`, `:forbidden`.
4. **L2 — integrity (500)**: `ent/validate-entity` / `ent/validate-relation`
   (`entity.clj:16-29`) when the model carries entity/relation keys (requires
   `:entity/id`+`:entity/type`, or
   `:relation/id`+`:relation/from`+`:relation/to`).
5. **L1 — identity uniqueness (409)**: only when an `:identity` map is
   supplied (none of the routes below use it except `/write` callers that
   pass one).
6. **Type auto-registration**: `types/tx-ops-for-docs`
   (`type_registry.clj:47-65`) prepends idempotent puts of type docs for
   every `:entity/type` / `:relation/type` / `:hx/type` seen in the put docs
   — this is how `/types` gets populated.
7. **Counter-ratchet** (`invariants.clj:115-147`): simulates before/after
   counts of protected classes `:entity :relation :descriptor :docbook` over
   the tx's ids; any decrease throws L2 500 `:counter-ratchet` unless the
   class is in `:allow-drop-classes`. (Hyperedges/evidence are **not**
   protected — deletes pass.)
8. **L0 — durable write (503)**: `xt/durable-write-tx!` (`xtdb.clj:137-153`)
   — submit tx, `tx-sync!`, then `verify-materialized!` (`xtdb.clj:40-66`):
   every put doc must have `:xt/id` and be readable back, else 503
   `:postcommit-missing-entities`. A proof-path is built and appended to the
   proof log; success returns `{:ok? true :tx-id <str> :path/id <str> :path
   <proof-path>}` — handlers surface `:tx-id` and `:path/id`.

There is **no sanitize function** in futon1a. The closest things:
`normalize-type` (string→keyword, strips leading `:`; `app.clj:161-168`,
`routes.clj:747-754`, `futon1_write.clj:11-18`), `normalize-text`
(trim/blank→nil), and JSON output escaping (`:escape-non-ascii`). A
replacement need only replicate the normalizations noted per-route below.

---

## 2. Health

### GET /health (also /healthz)
`app.clj:223-239` → `routes/health` (`routes.clj:60-66`) →
`health/health-report` (`health.clj:22-41`).
- No params. Runs an `:xtdb` check (`xtdb/status node`).
- **200** when all checks ok, **503** when degraded. Body:
  `{:status :ok|:degraded :counts {} :checks {:xtdb {:ok? true :status
  <xtdb-status-map>}} :last-error nil}` (`/healthz` variant's check returns
  just `{:ok? true}`).

---

## 3. Evidence

### POST /api/alpha/evidence
`app.clj:798-806` → `routes/compat-write-evidence` (`routes.clj:761-831`).
- Penholder: `compat-penholder` (body/header/default). Gates: full
  `run-write!` with `:model {}` (so **no** L4 canonical-id gate, no L2 entity
  checks — effectively L3 penholder + L0 durability + counter-ratchet no-op)
  and `:claim {:op :compat/evidence-write}`.
- Body fields (each accepted with or without the `evidence/` namespace,
  namespaced wins):
  - required: `:evidence/type` (normalized to keyword; missing → **400**
    `{:error "evidence/type required"}`), `:evidence/claim-type` (same,
    `"evidence/claim-type required"`), `:evidence/author`
    (`"evidence/author required"`). These three 400s are plain
    `{:error <string>}`, not the layered envelope.
  - optional: `:evidence/id` (default: random UUID string), `:evidence/at`
    (default: `Instant/now` ISO string), `:evidence/body` (default `{}`),
    `:evidence/subject` (map, conventionally `{:ref/type kw :ref/id str}`),
    `:evidence/tags` (stored as `(vec …)`, default `[]`),
    `:evidence/pattern-id`, `:evidence/session-id`, `:evidence/in-reply-to`,
    `:evidence/fork-of`, `:evidence/conjecture?` (coerced `boolean`),
    `:evidence/ephemeral?` (coerced `boolean`) — the last two only stored
    when present (`routes.clj:813-816`).
- **Ephemeral handling**: `:evidence/ephemeral? true` is *stored as a flag
  only*. Nothing in the read paths filters on it; there is no TTL/skip logic
  anywhere in futon1a. (futon3c's backend-side `filter-and-sort-entries`
  excludes ephemeral by default — that filtering lives client-side of this
  API today.)
- Duplicate-id guard: if the id already exists → **409** `{:error "duplicate
  evidence id" :evidence/id <id>}` (`routes.clj:791-792`). Evidence is
  append-only, not upsert.
- Success: **201** `{:ok true :evidence/id <id> :entry
  <stored-doc-minus-:xt/id> :tx-id <str> :path/id <str>}`
  (`routes.clj:826-831`). Doc is stored with `:xt/id` = `:evidence/id`.

### GET /api/alpha/evidence/{id}
`app.clj:1029-1036`. Direct entity lookup (id = raw URI suffix, **not**
URL-decoded here).
- **200** with the doc minus `:xt/id` (top-level evidence map, no envelope)
  when found and it has `:evidence/id`; else **404** `{:error "not found"
  :evidence/id <id>}`.

### GET /api/alpha/evidence/{id}/chain
`app.clj:1011-1027`. Follows `:evidence/in-reply-to` links (cycle-safe) from
`{id}` back to the root. Always **200** `{:chain [<root> ... <id-entry>]}`
(oldest first; empty chain if id unknown).

### GET /api/alpha/evidence?…
`app.clj:808-867` (inline in app.clj). Query params (**exact HTTP strings**):
- `type` — evidence type; normalized to keyword, exact match on
  `:evidence/type`.
- `claim-type` — keyword match on `:evidence/claim-type`.
- `subject-type`, `subject-id` — post-filter on
  `[:evidence/subject :ref/type]` (keyword) / `[:evidence/subject :ref/id]`
  (string).
- `session-id` — exact match on `:evidence/session-id`.
- `author` — exact match on `:evidence/author`.
- `since` — post-filter: `(compare (str (:evidence/at e)) since) >= 0`, i.e.
  lexicographic ISO-8601 string comparison, **inclusive**.
- `tags` — comma-separated list, **AND** semantics; each requested tag string
  must equal the `name` of some stored tag (tolerant of stored
  keyword/symbol/string) (`app.clj:852-861`).
- `limit` — int; applied after sort; unparseable → ignored (unbounded).
- Sort: newest first by `:evidence/at`.
- **200** `{:entries [<doc minus :xt/id> ...] :count <returned-count>}`.
- **NOT supported by futon1a today**: `before`, `tag` (singular),
  `include-ephemeral` — silently ignored if sent. futon1b SHOULD add
  `before` + `include-ephemeral` (the futon3c EvidenceBackend protocol grew
  them 2026-07-10) — flagged as a deliberate contract extension.

### GET /api/alpha/evidence/count
**Does not exist in futon1a** (falls through to the `{id}` handler → 404).
futon1b SHOULD add it (protocol `-count`, `{:count <n>}`, same query params
as the evidence query minus `limit`) — deliberate contract extension.

### GET /api/alpha/evidence/sessions
`app.clj:886-947`. Params: `since` (inclusive, restricts *entries*
considered — a session whose latest entry is older simply disappears),
`author` (exact match), `limit` (int, caps session count).
- **200**:
```clojure
{:window-since <since-or-nil>
 :author-filter <author-or-nil>
 :total-sessions <n>
 :total-entries <n>          ; entries after since/author filtering
 :sessions [{:session-id s :count n
             :types [<str> ...]     ; distinct, sorted, (str kw) e.g. ":claim"
             :authors [<str> ...]
             :first-at <iso-str> :latest-at <iso-str>} ...]}
```
- Sessions sorted newest `:latest-at` first. Note `:types`/`:authors` values
  are `(str kw)` so keywords appear **with** the leading colon here (unlike
  the entry stream).

---

## 4. Hyperedges

### POST /api/alpha/hyperedge (alias POST /api/hyperedge)
`app.clj:344-365` → `routes/compat-upsert-hyperedge` (`routes.clj:1056-1128`)
using `f1w/upsert-hyperedge-doc` (`futon1_write.clj:230-287`).
- Penholder: `compat-penholder`. Gates: `run-write!` with `:model {}` → L3
  penholder + counter-ratchet (hyperedges unprotected) + L0
  verify-materialized; `:claim {:op :compat/futon1-hyperedge}`.
- Body:
  - `:hx/type` (or `:type`) — **required**, string or keyword, normalized to
    keyword. Missing → thrown ex-info without `:error` layer map → **500**
    `{:error {:reason :exception :message "hx/type required"}}` (not 400 — a
    replacement may preserve this wart or fix it).
  - `:hx/endpoints` (or `:endpoints`) — **required non-empty sequence**. Each
    endpoint: a string entity-id, or a rich map — `normalize-endpoint`
    (`futon1_write.clj:185-222`) resolves `:entity-id`/`:id`/`:name` (name →
    entity lookup)/`:article`/`:entity {…}`, plus optional `:role` (keyword)
    and `:passage` (string). Unresolvable endpoint → 500 "unresolvable
    endpoint".
  - optional: `:props` or `:hx/props` (map — stored verbatim under
    `:hx/props`, keys exactly as sent, no normalization),
    `:hx/content`/`:content` (map), `:hx/labels`/`:labels` (normalized to
    keywords), `:hx/confidence` (number), `:hx/id`/`:id` (overrides minted
    id).
  - `:hx/valid-time` (or `:valid-time`) — optional; epoch-millis number,
    numeric string, or ISO-8601 instant; coerced to `java.util.Date`
    (`routes.clj:1031-1054`); unparseable degrades to nil (current-time
    put). Stamped as the 3rd element of the put/delete op; **never stored in
    the doc**. (futon1b v1 has no valid-time — replays stay primary-only
    until cutover, per the dual-write design.)
  - `:hx/op` (or `:op`) `"retract"` — delete instead of put
    (`routes.clj:1091, 1102-1104`).
- **Id minting — stable**: unless `:hx/id` supplied, `hx-id = "hx:" +
  <type-sans-colon> + ":" + (str/join "." (sort endpoint-ids))`
  (`stable-hyperedge-id`, `futon1_write.clj:224-228`). Sorted endpoints ⇒
  same type+endpoint-set always maps to the same id (idempotent upsert).
  Plain readable string, not a digest.
- Stored doc: `{:xt/id hx-id :hx/id hx-id :hx/type kw :hx/endpoints
  [<entity-id-str> ...] :hx/ends [{:entity-id .. :role? :passage?} ...]}`
  plus `:hx/props` / `:hx/content` / `:hx/labels` / `:hx/confidence` when
  present (`futon1_write.clj:269-277`).
- **No-op write guard** (`routes.clj:1096-1101`, 2026-07-10): for a **plain
  put only** (not retract, no explicit valid-time), if the freshly built doc
  equals the currently stored doc, the pipeline is skipped (no tx, no proof
  path) and the response is **200** `{:profile "default" :hyperedge
  <public-shape> :no-op? true}` — **no `:tx-id`/`:path/id`**.
- Normal success: **200** `{:profile <x-profile-header-or-"default">
  :hyperedge {:hx/id .. :hx/type .. :hx/endpoints .. :hx/ends .. (+opt keys)}
  :tx-id .. :path/id ..}`; retract adds `:retracted? true`.

### GET /api/alpha/hyperedge/{id}
`app.clj:380-384` → `routes/hyperedge-by-id` (`routes.clj:1130-1139`). The
whole URI tail after `/api/alpha/hyperedge/` is URL-decoded and used as the
id — ids like `hx:code/v05/contains:a.b` (containing `:` and `/`) work
because the tail is taken wholesale.
- **200**: the hyperedge doc minus `:xt/id` (top-level map, no envelope),
  only if the doc exists **and** has `:hx/id`; otherwise **404** `{:error
  "not found" :hx/id <id>}`.

### GET /api/alpha/hyperedges?type=…&end=…&limit=…
`app.clj:387-403`. Requires `type` **or** `end`, else **400** `{:error "type
or end parameter required"}`. `limit` = int (unparseable ignored). Extra
params with `type`: `repo`, `source-file`.
- **type branch** → `routes/hyperedges-by-type` (`routes.clj:1150-1189`):
  index-only id lookup on `:hx/type` (15s query timeout), ids sorted by
  `str`, docs pulled lazily; `repo`/`source-file` filter against `:hx/props`
  under **both** keyword and string keys (`:repo`/`"repo"`)
  (`routes.clj:1171-1174`). Response **200** `{:hyperedges [<doc minus
  :xt/id> ...] :count <n>}` — `:count` is the **true total** for the type
  when unfiltered (even if `limit` truncated `:hyperedges`), but the
  returned-docs count when repo/source-file filters are applied.
- **end branch** → `routes/hyperedges-by-end` (`routes.clj:1215-1244`): if
  `end` is UUID-shaped, it is resolved via `:entity/id` → `:entity/name`
  first (`routes.clj:1195-1213`); then exact match against the flat
  `:hx/endpoints` vector. Sorted by `:hx/id`, limit applied post-sort.
  **200** `{:hyperedges [...] :count <returned-count>}`.

---

## 5. Entities

### POST /api/alpha/entity (alias POST /api/entity)
`app.clj:285-306` → `routes/compat-ensure-entity` (`routes.clj:615-638`)
using `f1w/ensure-entity-doc` (`futon1_write.clj:76-108`).
- Penholder: `compat-penholder`. Body: **required** `:name`, `:type` (missing
  → L4 **400** `{:error {:layer 4 :reason :missing-required ...}}`); optional
  `:external-id`, `:source`, `:id`, `:props` (map → stored as
  `:entity/props`).
- Id minting ("ensure" semantics, stable-by-name): requested `:id` → else the
  **existing** entity with the same `:entity/name` (preferring matching
  `:entity/type`, then lexicographically smallest `:entity/id` —
  dedup-tolerant, `futon1_write.clj:34-60`) → else a fresh random UUID
  string. Doc: `{:xt/id id :entity/id id :entity/name name (+ :entity/type
  :entity/external-id :entity/source :entity/props)}`.
- **Gates — this is the route where everything fires**: `run-write!` with
  `:model` = the entity doc and `:required-keys #{:entity/id :entity/name
  :entity/type}` ⇒ L4 validate **and the L4 canonical-id gate** (fires
  because model has `:entity/type`; e.g. a `:mission/doc` with non-canonical
  id gets 400 `:non-canonical-id` or is queued), L3 penholder, L2
  `validate-entity`, counter-ratchet (`:entity` protected), L0
  verify-materialized. `:claim {:op :compat/futon1-entity}`.
- Success **200**: `{:profile "default" :entity {:id .. :name .. :type <kw>
  :external-id <or nil> :source <or nil> (:props ..)} :tx-id .. :path/id ..}`.

### GET /api/alpha/entity/{id} (alias GET /api/entity/{id})
`app.clj:596-609` → `routes/compat-entity` (`routes.clj:326-341`) →
`f1g/fetch-entity` (`futon1_graph.clj:62-100`): lookup by `:xt/id`, then
fallback by `:entity/name`, then by `:entity/external-id` (deterministic
smallest-id pick on duplicates). Id path segment is URL-decoded.
- **200** `{:profile <x-profile|"default"> :entity {:id :name :type (+
  :external-id :source :props :media/sha256 when present)}}`; **404**
  `{:error "Entity not found" :profile .. :entity-id <id>}`.

### GET /api/alpha/entity?source=…&external-id=…
`app.clj:583-594` → `routes/entity-by-external` (`routes.clj:276-324`). Both
params required (else L4 400). **200** `{:entity <raw-doc>}`; **404** layered
`:not-found`; multiple identity docs → L1 **409** `:external-id-ambiguous`.

### GET /api/alpha/entities/latest?type=…&limit=…
`app.clj:611-618` → `routes/compat-entities-latest` (`routes.clj:343-357`).
- `type` **required** (string like `"pattern/library"`; missing → L4 400).
  `limit` parsed with `Long/parseLong` **unguarded** (bad value → 500);
  default effectively 1 (`max 1 (or limit 1)`).
- Special case: `type=pattern/library` only returns patterns that have a
  `:pattern/has-sigil` relation to a `:pattern/sigil` entity. De-dupes by
  `:entity/name`, sorts by name, takes `limit`, normalizes.
- **200** `{:profile .. :type "pattern/library" :entities
  [<normalized-entity> ...]}` (`:type` echoed as string without colon).

---

## 6. Relations

### POST /api/alpha/relation (alias POST /api/relation)
`app.clj:308-329` → `routes/compat-upsert-relation` (`routes.clj:640-670`)
using `f1w/upsert-relation-doc` (`futon1_write.clj:141-181`).
- Penholder: `compat-penholder`. Body: **required** `:type`, `:src`, `:dst`
  (missing → L4 400 `:missing-required`); optional `:provenance` (map),
  `:props` (map — if `:props` present and `:provenance` absent it is
  normalized to `:provenance {:note (:label props) :props props}`,
  `routes.clj:646-653`), `:id`.
- Endpoint resolution (`futon1_write.clj:110-122`): src/dst may be an entity
  **name**, an `:entity/id`, a raw UUID string, or a map with
  `:id`/`:entity/id`/`:name`. Unresolvable → 500 "relation requires
  resolvable src/dst and type".
- **Id minting** (stable, idempotent): `"rel|" src-id "|" type-sans-colon
  "|" dst-id "|" (note or "") "|" (order or "")` (`stable-relation-id`,
  `futon1_write.clj:130-139`), where note/order come from `:provenance`.
- Stored doc has **both** key spellings: `:relation/src`+`:relation/dst` and
  `:relation/from`+`:relation/to`, plus `:relation/id`, `:relation/type`,
  `:relation/provenance` (`futon1_write.clj:165-173`).
- Gates: `run-write!` with `:model (select-keys doc [:relation/id
  :relation/from :relation/to])`, same `:required-keys` ⇒ L4 validate + L3
  penholder + **L2 `validate-relation`** + counter-ratchet (`:relation`
  protected) + L0. No canonical-id gate. `:claim {:op
  :compat/futon1-relation}`.
- Success **200** `{:profile .. :relation {:id :type :relation/type :src-id
  :dst-id :provenance} :tx-id .. :path/id ..}` (public `:type` prefers the
  keyword derived from provenance `:note`).

(Batch variant `POST /api/alpha/relations/batch` — `app.clj:332-341`,
`routes.clj:672-745` — takes `{:relations [...]}`, same per-item shape,
single tx via `run-open-world!`; endpoints must already exist in the store
or L2 500 `:missing-endpoint`; returns `{:profile :count :relations :tx-id
:path/id}`.)

**Futon1b port (2026-07-16, `futon1b-graph/write-relations-batch!`):** the
batch variant is now served on :7073. Deviations, mirroring the single
route's: success envelope carries `:rescue` (map of rescued ids → stage)
instead of `:tx-id`/`:path/id`; the whole batch validates and resolves
endpoints before the first write, commits in one `execute-tx`, and every
doc is verified by read-back (XTDB 2 batch puts can drop rows silently) —
absent docs escalate through the per-doc rescue ladder, L0 503 if
unrescuable. §6 batch endpoint-existence is enforced for resolved ids
(stricter than the single route's uuid pass-through). Empty or non-map
`:relations` → L4 400 `:invalid-relations-batch`.

---

## 6a. Backend-neutral graph extensions (2026-07-13)

The authoritative Futon1b substrate additionally exposes three semantic
operations needed by consumers that formerly dereferenced Futon1a's embedded
XTDB node. These routes expose graph meanings rather than XTDB query forms:

- `GET /api/alpha/entities?type=…&limit=…` returns raw typed entity documents
  as `{:entities […] :count n}` so legacy top-level domain fields and newer
  `:entity/props` fields remain interpretable.
- `GET /api/alpha/relations?type=…|types=a,b&from=…&to=…&limit=…&hydrate=true`
  returns matching relations. `hydrate=true` adds the referenced entity
  documents once, avoiding N+1 endpoint lookups. Filters are conjunctive;
  `types` is the sole disjunction.
- `POST /api/alpha/graph/inhabited` with `{:bindings [{:kind :entity|:hyperedge
  :type … :endpoint-types […]} …]}` returns the bindings in order with an
  authoritative `:inhabited?` boolean. Hyperedge endpoint constraints preserve
  the former existential Datalog semantics.

Graph writes continue through the existing gated entity, relation, and
hyperedge routes. There is deliberately no raw transaction or evict endpoint.

## 7. Census

### GET /api/alpha/census?type=… | ?entity-type=…
`app.clj:407-411` → `routes/census` (`routes.clj:162-185`).
- Exactly one of `type` (an `:hx/type`) or `entity-type` (an
  `:entity/type`); `type` wins if both. Neither → **400** `{:error "census
  requires ?type=<hx-type> or ?entity-type=<type>"}`.
- Bound-type indexed count pushdown (`routes.clj:154-160`) — no doc
  materialization. **200** `{:type <param-string-as-sent> :kind
  :hyperedge|:entity :count <n>}`.

---

## 8. Types

### GET /api/alpha/types (also bare GET /types)
`app.clj:655-657, 698-700` → `routes/list-types` (`routes.clj:145-153`).
- **200** `{:types [<type-doc> ...]}` — every doc with `:type/kind`, pulled
  full (includes `:xt/id`, `:type/id` keyword, `:type/kind`
  `:entity|:relation|:intent`, optional `:type/parent`, `:type/aliases`),
  sorted by `kind|id`.

### POST /api/alpha/types/parent (also bare /types/parent)
`app.clj:659-661` → `routes/types-parent` (`routes.clj:187-219`). Body-only
penholder (**no header fallback**). Body: `:penholder`, `:type/id`,
`:type/kind` required (L4 400 if missing); `:type/parent` optional (nil
clears). **200** `{:tx-id .. :path/id ..}`.

### POST /api/alpha/types/merge (also bare /types/merge)
`app.clj:663-665` → `routes/types-merge` (`routes.clj:221-257`). Body:
`:penholder`, `:type/id`, `:type/kind`, `:type/aliases` (must be sequential,
else L4 400 `:invalid-type-aliases`). **200** `{:tx-id .. :path/id ..}`.

---

## 9. Snapshot

### POST /api/alpha/snapshot (alias POST /api/snapshot)
`app.clj:414-428` → `routes/snapshot-save` (`routes.clj:962-978`) →
`snapshot/export-graph-snapshot` (`snapshot.clj:111-183`).
- **No penholder** (filesystem export only). Body: `:scope` (string or
  keyword; default `"all"`), `:label` (optional).
- **Valid scopes** (`snapshot.clj:122-126`): `"all"` (entities+relations,
  snapshot-id `snap-<epoch-ms>`), `"latest"` (same doc set, fixed id
  `latest` — overwrites), `"hyperedges"` (all `:hx/id` docs via lazy
  `open-q`, fixed id `hyperedges`), **`"evidence"`** (all `:evidence/id`
  docs via lazy `open-q`, fixed id `evidence`; F5 sessionless-inclusive).
  Invalid scope → L4 **400** `:invalid-snapshot-scope`.
- Writes `<data-dir>/snapshots/<id>.edn` containing `{:snapshot/id
  :snapshot/scope :snapshot/label :snapshot/created-at <ms> :counts :docs
  [...]}`; `java.time.Instant` values serialized as `#futon1a/instant "..."`
  tagged literals (`snapshot.clj:39-52`).
- **200** `{:ok? true :snapshot/id <id> :snapshot/file <path> :counts
  {...}}` — counts per scope: all/latest `{:docs :ids :entities
  :relations}`, hyperedges `{:docs :ids :hyperedges}`, evidence `{:docs :ids
  :evidence :sessionless}`.

### POST /api/alpha/snapshot/restore
`app.clj:430-454` → `routes.clj:980-1004` →
`snapshot/restore-graph-snapshot!` (`snapshot.clj:202-268`). Penholder via
`compat-penholder`. Body: `:scope`, `:snapshot/id` (defaults to `"latest"`
when scope=latest; required otherwise). Re-ingests all docs through
`run-open-world!`. **200** `{:ok? true :tx-id :path/id :counts
:snapshot/id}`.

---

## 10. Patterns activation

### GET /api/alpha/patterns/activation
`app.clj:955-1009` (inline). Params: `since` (inclusive, lexicographic vs
`(str :evidence/at)`), `limit` (int, caps pattern count).
- Scans **all** evidence, keeps entries whose `:evidence/body` has `event ==
  "context-retrieval"` (checks both keyword and string key), explodes each
  body's `:results` into per-pattern activations, groups by result `:id`.
- **200**:
```clojure
{:window-since <since|nil>
 :total-retrievals <n>            ; matching evidence entries
 :pattern-count <n>
 :patterns [{:id <pattern-id> :title <str|nil> :count <n>
             :avg-score <double|nil> :last-fired <iso-str>
             :activations [{:id :title :score :at :session-id
                            :evidence-id :agent-id   ; = :evidence/author
                            :query <first-240-chars>} ...]} ...]}  ; newest first
```
- Patterns sorted by `:count` desc.

---

## 11. Gotchas a replacement must reproduce (or knowingly fix)

1. **EDN-first**: futon3c clients that send no headers get EDN back; JSON
   only on `Accept`/`Content-Type: application/json`. JSON output loses the
   keyword colon (`:claim` → `"claim"`) except in `/evidence/sessions`
   `:types`/`:authors` which keep it (stringified with `(str kw)`).
2. **Hyperedge idempotency lives in the id, versions in XTDB**: same
   type+sorted endpoints → same `hx:` id; changed `:hx/props`/`:hx/content`
   → new version of the same doc; identical doc → the no-op guard skips the
   tx and returns `:no-op? true` without `:tx-id`.
3. `x-penholder` header works for all `compat-*` writes but **not**
   `/write`, `/ingest`, `/types/*` (body `:penholder` only there).
4. Missing `hx/type` / bad endpoints on POST /hyperedge return **500** (raw
   ex-info), not 400.
5. `GET /api/alpha/evidence/count` and evidence params
   `before`/`tag`/`include-ephemeral` do **not exist** in futon1a; ephemeral
   is write-side metadata only. futon1b adds count/before/include-ephemeral
   as deliberate extensions (the futon3c EvidenceBackend protocol needs
   them).
6. Evidence GET-by-id ids are not URL-decoded; hyperedge GET-by-id ids are.
7. `entities/latest` `limit` uses `Long/parseLong` unguarded (bad value →
   500); all other numeric params swallow parse errors.
8. Hyperedge scan queries carry a 15s XTDB `:timeout`; census must stay a
   bound-type count-pushdown (a full census scan times out at ~470k docs).
