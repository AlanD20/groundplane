# ADR 0013: Deterministic etcd records and indexes

- Status: Proposed
- Date: 2026-08-20

## Context

Groundplane's single-host MVP keeps desired state, durable Controller records,
the task journal, Agent configuration, encrypted secrets, components,
connectors, and runners in one dedicated etcd instance. Stable ids are the
references; slugs and names are mutable labels with scoped uniqueness. The
human API addresses records by id and lists flat collections using cursor
pagination and owner filters.

ADR 0008 selects the official etcd v3 client behind
`internal/infra/etcd.Store`. The Store now exposes linearizable revisions,
ordered prefix reads, revision-aware watches, and atomic transactions that
compare modification revisions and apply puts/deletes. It deliberately does
not decide domain keys, ownership, JSON schemas, pagination, or conflict
mapping.

Without one repository contract, independently implemented repositories could
make incompatible choices about all of those matters. In particular:

- checking a slug and then creating a record would race;
- listing an index and fetching records at different revisions could produce a
  view that never existed;
- starting a watch after a list without its revision could lose a mutation;
- deleting a record separately from its indexes or ciphertext could leave
  dangling state; and
- an opaque cursor without a pinned revision could duplicate or skip records
  while writes continue.

This ADR proposes the complete storage mechanics. It does not accept product
choices that the authoritative documents currently leave inconsistent or
undefined. A typed repository may implement only the owner-approved choices
listed below; unresolved resource-specific sections remain implementation
gates even while an approved independent repository slice proceeds.

## Owner-approved choices

On 2026-08-20 the owner approved the concrete recommendations that were stated
before the detailed unresolved-choice recommendations later in this document
were written. That approval resolves only the following choices:

- normalized, flat, stable-id primary records are the desired-state
  persistence unit; the embedded `internal/core.Environment` shape is an
  assembled domain view, not a second persisted source of truth;
- backing-project slugs are platform-global;
- zone, volume, script, release-group, and component-kind uniqueness indexes
  are required in addition to the previously locked indexes;
- latest observations are separate from desired records, and component
  `healthy` plus runner/Agent `online` are projections of observed state;
- every collection uses id-ascending order;
- destructive actions create a Task and tombstone atomically, expose the
  resource until finalization, block mutations and descendant creation, and
  process contained descendants in stable-id postorder;
- pagination defaults to 50, accepts 1 through 200, binds an opaque cursor to
  one fixed revision and exact query, and expires with `cursor.expired` after
  the deployment's 24-hour compaction window;
- the error code names proposed in section 10 and retryable
  `storage.unavailable` HTTP 503 mapping are approved; and
- persistence conflicts and expired cursors are HTTP 409, malformed requests
  are HTTP 400, decoded validation is HTTP 422, and the closed internal Kind
  catalog is the accepted target error interface;
- atomic mutations are limited to 96 aggregate transaction operations, a
  1 MiB serialized request, and 256 KiB per record; bounded deletion may
  checkpoint stable-id batches under its tombstone; and
- mutating HTTP intents use the approved Idempotency-Key grammar,
  same-transaction marker, replay/mismatch behavior, and 90-day terminal
  retention; ADR 0019 accepts the exact canonical intent encoding and
  protected digest envelope; and
- an unknown write outcome requires durable idempotency evidence rather than
  inference from later key state.
- deletion tombstones use
  `/v1/runtime/deletions/<target-kind>/<stable-id>`; and
- Tasks use `/v1/tasks/<task_id>`, the operation and active-operation indexes,
  20-digit event sequence keys, and stable event-identity deduplication keys
  listed in section 6. The Controller-owned sequence, identical-replay versus
  mismatched-payload behavior, atomic event/summary/deduplication CAS, 32 KiB
  event and 1,000-event caps, 90-day terminal retention, no nonterminal
  pruning, and daily 100-key pruning batches are approved.

The approval cannot retroactively supply values or behavior that had not yet
been presented. The detailed recommendations under “Unresolved owner choices”
therefore still require explicit approval before this ADR can become Accepted
or typed repositories can be implemented.

## Accepted constraints

The eventual decision must preserve these existing contracts:

- etcd is a dedicated, single-node host-level dependency in the MVP. The
  configured key prefix isolates Groundplane's logical keyspace.
- ids are globally stable, ULID-style, kind-prefixed identifiers and are never
  derived from slugs or names.
- the API addresses entities by id. The CLI resolves mutable slugs/names within
  their owner scope unless `--id` is used.
- tenant slugs are globally unique; tenant-project slugs are unique within a
  tenant; environment slugs are unique within a project; service names and
  attach names are unique within an environment.
- backing-service is a facade over one backing project, its `main`
  environment, and its adapter service. It is not duplicate persisted state.
- routers, host status, the platform component overview, and activity are projections, not
  independent desired resources.
- secrets and generated credentials are encrypted before they enter etcd.
  Agent channel tokens are encrypted at rest and indexed only by SHA-256
  digest; short-lived runner registration tokens are never persisted.
- reusable Secret resources have an explicit project or platform owner.
  Effective lookup checks the project first and falls back to the platform
  only when the project record is absent; values never merge.
- service `runtime_intent` is durable Controller-owned operational state with
  values `running`, `stopped`, or `absent`; it is not Blueprint input.
- destructive actions are Tasks. An attach record is removed only as part of
  successful detach sequencing.
- every task state transition and activity event is durable; high-volume log
  output is not persisted.
- rendered Compose, env files, Corefiles, Caddyfiles, and task render plans are
  derived and are never stored as sources of truth.
- there are no compatibility readers, dual writes, or aliases for superseded
  storage schemas. A schema change is an explicit migration and clean cutover.

## Proposed decision

### 1. Logical keyspace

All paths below are logical Store keys. The Store prepends the configured
Controller key prefix. Version `v1` is the storage-schema version, not the REST
API version.

```text
/v1/records/...       id-addressed primary records
/v1/singletons/...    one record per declared owner
/v1/indexes/...       transactional lookup and membership indexes
/v1/secret-values/... encrypted, non-listable value records
/v1/observed/...      latest Agent-reported observations
/v1/runtime/...       durable queue, assignment, lock, and bootstrap records
```

Primary records are flat by collection:

```text
/v1/records/<collection>/<stable-id>
```

Flat primaries make the API's id lookup one exact read and keep record paths
unchanged across label renames. Ownership is represented in the record and in
transactional indexes, never by moving a primary record.

Stable ids are raw key segments after validation against their registered kind
prefix. Every other dynamic segment is encoded as `~` followed by unpadded
base64url of its UTF-8 bytes. This includes slugs, names, component kinds,
workspace names, operation ids, and token digests. Encoding is performed
exactly once; decoded values are validated before use. Empty dynamic values
are invalid unless a table below names a literal sentinel.

The literal owner sentinel `platform/-` denotes the singleton platform owner,
and `global/-` denotes a collection with no owner scope. All other owner scopes
are `<owner-kind>/<stable-owner-id>`, for example `tenant/tnt_...`,
`project/prj_...`, or `environment/env_...`.

### 2. Record and singleton layout

| State | Primary key | Required ownership indexes |
| --- | --- | --- |
| tenant | `/v1/records/tenants/<tenant-id>` | none |
| project | `/v1/records/projects/<project-id>` | `projects/by-owner/tenant/<tenant-id>/<project-id>` or `projects/by-owner/platform/-/<project-id>` for backing projects; backing slugs are platform-global |
| environment | `/v1/records/environments/<environment-id>` | `environments/by-owner/project/<project-id>/<environment-id>` |
| Environment Compose projection | `/v1/records/environment-compose-projections/<environment-id>` | none; atomically versioned with the Blueprint head |
| zone | `/v1/records/zones/<zone-id>` | `zones/by-owner/environment/<environment-id>/<zone-id>` |
| service | `/v1/records/services/<service-id>` | `services/by-owner/environment/<environment-id>/<service-id>` |
| environment entry | `/v1/records/entries/<entry-id>` | `entries/by-owner/environment/<environment-id>/<entry-id>` |
| attach | `/v1/records/attaches/<attach-id>` | by environment, each consuming service, backing service, backing project, and each granted Attach (ADR 0031) |
| route | `/v1/records/routes/<route-id>` | `routes/by-owner/environment/<environment-id>/<route-id>` |
| volume | `/v1/records/volumes/<volume-id>` | `volumes/by-owner/environment/<environment-id>/<volume-id>` |
| script | `/v1/records/scripts/<script-id>` | `scripts/by-owner/environment/<environment-id>/<script-id>` |
| release group | `/v1/records/release-groups/<release-group-id>` | `release-groups/by-owner/environment/<environment-id>/<release-group-id>` |
| component | `/v1/records/components/<component-id>` | by environment owner or `platform/-` |
| connector | `/v1/records/connectors/<connector-id>` | `connectors/by-owner/environment/<environment-id>/<connector-id>` |
| secret metadata | `/v1/records/secrets/<secret-id>` | `secrets/by-owner/project/<project-id>/<secret-id>` or `secrets/by-owner/platform/-/<secret-id>` plus one scoped encoded-key index for fallback resolution |
| runner | `/v1/records/runners/<runner-id>` | exactly one tenant or project owner index |
| Agent | `/v1/records/agents/<agent-id>` | `agents/by-owner/platform/-/<agent-id>` |
| release record | `/v1/records/releases/<deployment-id>` | by service and environment |
| recovery point | `/v1/records/recovery-points/<recovery-point-id>` | by environment and source id |

Task materialization source references are stored inside the Task record as a
closed Controller-only union. They bind a step and Environment to an immutable
Blueprint file, Entry value generation, or versioned generated-environment
input list; they never contain plaintext or enter the Agent projection.

Task records intentionally do not inherit the generic `/v1/records` layout.
Their exact owner-approved primary, index, event, and deduplication keys are
listed in decision-ledger item 5.

The exact secondary-index form is:

```text
/v1/indexes/<collection>/<index-name>/<scope-kind>/<scope-id>/<record-id>
```

Examples:

```text
/v1/indexes/attaches/by-backing-service/service/<service-id>/<attach-id>
/v1/indexes/attaches/by-granted-attach/attach/<grant-attach-id>/<attach-id>
/v1/indexes/releases/by-service/service/<service-id>/<deployment-id>
```

Each membership-index value is the exact target stable-id bytes. The key owns
the collection, index, scope, and record identity; the value is independently
checked against its final key segment and never duplicates the primary payload.
ADR 0029 records this correction to the originally documented JSON wrapper.

Singletons use their natural stable owner as the final segment:

```text
/v1/singletons/backup-policies/<environment-id>
/v1/singletons/agent-configs/<agent-id>
```

The following are projections and therefore have no primary record:

- backing-service: joins the project, `main` environment, and adapter service;
- router: reads ingress components for one environment;
- host and core overview: read health and component/Agent records;
- activity: reads Tasks and task events using the owner-proposed keys and
  workspace indexes in decision-ledger item 5.

### 3. Unique label indexes

A unique index is an exact key whose value identifies its primary record:

```text
/v1/indexes/<collection>/by-<label>/<scope-kind>/<scope-id>/<encoded-label>
```

The required accepted indexes are:

```text
tenants/by-slug/global/-/<slug>                         -> tenant id
projects/by-slug/tenant/<tenant-id>/<slug>             -> project id
environments/by-name/project/<project-id>/<name>       -> environment id
services/by-name/environment/<environment-id>/<name>   -> service id
attaches/by-name/environment/<environment-id>/<name>   -> attach id
```

These additional indexes are required because their labels are referenced by
desired state or operator surfaces:

```text
projects/by-slug/platform/-/<slug>                     -> backing project id
zones/by-name/environment/<environment-id>/<name>      -> zone id
volumes/by-name/environment/<environment-id>/<name>    -> volume id
scripts/by-name/environment/<environment-id>/<name>    -> script id
release-groups/by-name/environment/<environment-id>/<name> -> release-group id
components/by-kind/<owner-kind>/<owner-id>/<kind>      -> component id
```

No uniqueness key is invented for entries, routes, connectors, runners,
recovery points, releases, Tasks, or Agents. Their accepted identity is the
stable id unless an owner contract adds a distinct label rule.

### 4. JSON encoding and storage-schema versioning

Every primary, singleton, observed, runtime, and journal value is UTF-8 JSON
with no insignificant whitespace. A primary envelope is:

```json
{"schema":1,"kind":"service","data":{"id":"svc_..."}}
```

Rules:

- `schema` and `kind` are required. Schema `1` is the payload schema inside
  keyspace `v1`; both must match the repository and key collection.
- `data` uses snake_case field names and the repository's typed persistence
  DTO. Public API DTOs and `internal/core` aggregates are not used as implicit
  persistence schemas.
- ids and owner ids inside `data` must match the primary key and owner indexes.
- times are UTC RFC3339Nano strings. Durations are explicit strings or integer
  units named in the field; floating-point timestamps are forbidden.
- maps are emitted with lexicographically sorted keys. Byte strings and
  ciphertext are unpadded base64url strings in explicitly named fields.
- decoders reject unknown envelope fields, missing required fields, duplicate
  JSON object keys, unknown schema versions, and trailing data.
- malformed durable data is `internal`; it is never silently defaulted,
  repaired during a read, or exposed as operator validation failure.
- ordinary CAS uses etcd revisions, not byte comparison. Canonical encoding
  exists for reproducible snapshots, diagnostics, hashes explicitly defined
  over records, and deterministic tests.

A breaking payload change creates `/v2` plus one explicit offline or
startup-gated migration. The runtime reads and writes exactly one version. It
does not dual-write, fall back to an old decoder, or retain aliases after a
successful cutover.

### 5. Secret and generated-value layout

Metadata remains listable under `/records`; ciphertext never does. The
proposed encrypted keys are:

```text
/v1/secret-values/entries/<entry-id>/<value-generation-id>
/v1/secret-values/secrets/<secret-id>
/v1/secret-values/connectors/<connector-id>/<encoded-field-name>
/v1/secret-values/attach-facts/<attach-id>
/v1/secret-values/backup-identities/<environment-id>/<zero-padded-era>
```

Plain Entry values use the parallel immutable keyspace:

```text
/v1/entry-values/plain/<entry-id>/<value-generation-id>
```

`value-generation-id` is a `cfg_...` id and is write-once. Replaying an exact
record is idempotent; reusing the id with different bytes is a state conflict.
Plain generations carry their content digest. Secret generations contain only
the accepted Controller-key envelope metadata, ciphertext digest, and
ciphertext. Both bind the Environment and Entry ids and a canonical creation
timestamp. A Task references one exact generation, so later Entry edits cannot
change retry or restart materialization bytes.

The value envelope identifies schema, kind, controller-key generation if that
concept is later added, and one age ciphertext. It never contains plaintext or
a plaintext-derived key segment. Secret metadata and ciphertext are created,
renamed where applicable, and deleted in one transaction.

The accepted durable per-Agent channel-token records are:

```text
/v1/singletons/local-agent/-
/v1/runtime/agent-channel-tokens/<agent-id>
/v1/indexes/agent-channel-tokens/by-digest/global/-/~<base64url-sha256>
```

The singleton value is canonical
`{"schema":1,"record_id":"<agent-id>"}` and is the transactional authority
that enforces the one-Agent MVP even when two different ids race creation.
The per-Agent value contains schema, Agent id, the application-level encrypted
32-byte token, and creation/update timestamps. The digest index contains the
Agent id and uses `~` plus unpadded base64url of SHA-256 over the raw token
bytes as its final segment, without a second encoding pass. The plaintext or
encoded token is never an etcd key or plaintext value. `POST /agents` creates
the Agent, singleton pointer, Agent config, encrypted token record,
and digest index in one transaction; Agent removal revokes the digest index
before deleting the remaining records. The credential is durable rather than
consumed during `Connect`, allowing the Controller to rematerialize the runtime
token file after restart or Controller-owned container replacement. ADR 0011
owns the accepted channel and materialization contract.

Runner registration tokens are passed to the registration task and discarded;
they have no durable key.

### 6. Task, event, observed, and runtime layout

Task records use their ULID id as the ordered collection key. The
owner-approved Task layout is:

```text
/v1/tasks/<task_id>
/v1/indexes/tasks/operation/<operation_id>/<task_id>
/v1/indexes/tasks/active-operation/<operation_id>
/v1/runtime/task-events/<task_id>/<20-digit-zero-padded-sequence>
/v1/runtime/task-event-dedup/<task_id>/<step_id>/<attempt>/<ordinal>
/v1/runtime/task-queue/<task_id>
/v1/runtime/assignments/<agent_id>/<task_id>
```

Events are append-only within a Task. An event value contains its schema, Task
id, sequence, event kind, timestamp, step id where applicable, and typed
payload. The Task record and its next event sequence are updated atomically
with each event. Log output is excluded. Decision-ledger item 5 owns approval
of these keys, sequence allocation, deduplication, and retention as one
contract; no alternate Task primary or event-key layout exists. Both Task
index values are canonical `{"schema":1,"record_id":"<task-id>"}` JSON.

Queue and assignment records are durable until terminal task acknowledgement:

```text
/v1/runtime/task-queue/<task-id>
/v1/runtime/assignments/<agent-id>/<task-id>
/v1/runtime/locks/<encoded-lock-scope>/<encoded-lock-id>
```

Lock values carry the owning task id and its expected revision. They are
claimed/released transactionally; process-local mutexes are not the durable
serialization authority.

Latest observations are kept separately from desired records so an Agent
report cannot mutate intent:

```text
/v1/observed/agents/<agent-id>
/v1/observed/environments/<environment-id>/<resource-kind>/<stable-id>
```

Observed values carry the reporting Agent id, observed-at timestamp, and typed
state. Component `healthy` and runner/Agent `online` fields are projections of
observed records, not writes to desired records.

### 7. Atomic create, update, rename, and delete

All repository writes use one etcd transaction.

**Create** compares modification revision `0` for the primary, every unique
index, and every membership index, then puts all of them. The stable id is
generated once before the transaction. A retry reuses that id.

**Update** compares the primary's read modification revision. It writes the
new record and any changed secondary indexes in the same transaction.

**Rename** additionally compares the new unique key at revision `0`, compares
the old unique key at the revision that was read, deletes the old key, creates
the new key, and updates the primary. No reference is rewritten because all
references use ids.

**Delete finalization** compares the primary and every index/ciphertext
revision captured by the repository, then deletes them together. A missing
secondary record is corruption, not a successful partial cleanup. Records
with required dependents are not deleted until the owning task has applied the
accepted dependent/cascade policy.

**Unknown transaction outcome** requires explicit idempotency evidence. A
write that may be retried after a timeout includes an operation marker and
protected intent evidence in the same transaction. Accepted ADR 0019 defines
the versioned canonical intent and root-age-protected digest envelope. A linearizable
reread may declare success only when that marker proves the same intent
committed. Without such a marker, later valid writes make key-state inference
ambiguous; the repository must return an unknown/transient result for caller
reconciliation rather than guessing that a different value means the
transaction failed. A retry never generates a replacement id.

Large mutations that exceed etcd's transaction/request limits cannot be split
silently. Blueprint replacement and cascading deletion require an accepted,
bounded multi-transaction state machine before implementation.

### 8. List ordering and cursor encoding

Every list is served from one owner or secondary-index prefix. Index keys end
in a stable id, so ULID lexical order is chronological without a separate
timestamp index. The universal order is id ascending.

The first page performs a linearizable, key-ascending range read and captures
revision `R`. It then reads every referenced primary at the same revision `R`.
Subsequent pages use the same `R` and start strictly after the previous index
key. Filtering selects a dedicated index; repositories do not fetch the whole
database and filter in memory.

`next_cursor` is unpadded base64url of canonical JSON:

```json
{"v":1,"revision":42,"last_id":"svc_...","query":"<sha256>"}
```

`query` is SHA-256 over the canonical collection, owner, filters, ordering,
and page-limit tuple. Decoding validates the version, positive revision,
maximum length, exact query digest, and a canonical kind-specific `last_id`.
The repository reconstructs its private prefix key internally. A cursor is
opaque, exposes no etcd key layout, grants no authority, and can never select
a key outside its request.

The Store provides bounded range-at-revision and same-revision multi-get
operations. Paginated repositories must use those operations; there is no
unbounded `List` compatibility path.

### 9. Watch and resume

A consumer establishes a gap-free view as follows:

1. range-read its complete record/singleton scope and capture `ReadRevision`;
2. build its in-memory projection from that snapshot;
3. call `Watch` with `startRevision = ReadRevision + 1`;
4. apply events in delivered revision order, ignoring an event whose
   modification revision is not newer than the already applied revision for
   that key.

Repository consumers watch primary/singleton prefixes, not secondary indexes.
An etcd transaction is already fully visible when any event from its revision
is handled, so reconciliation rereads the affected aggregate rather than
assuming one flattened event contains the full transaction.

Watch errors are mandatory input. On transport failure, the consumer resumes
from the last fully applied revision plus one. On compaction or any uncertain
gap, it discards the affected projection and repeats the snapshot procedure.
Controller restart also performs a full snapshot; no separate durable watch
checkpoint is required for the single-process, single-host MVP. Reconciliation
and event consumers must be idempotent because delivery across reconnect can
repeat an already applied event.

### 10. Error mapping

Repository mapping is deterministic and happens above the Store:

| Condition | Groundplane mapping |
| --- | --- |
| malformed request/query/cursor syntax or cursor/query mismatch | `validation.failed`, bad request, HTTP 400 |
| decoded schema, range, ownership, or domain input failure | `validation.failed`, validation, HTTP 422 |
| impossible internal repository key or owner after boundary validation | `internal`, HTTP 500 |
| missing primary | the resource's `<resource>.not_found` code |
| accepted slug index already owned by another id | `slug.conflict` |
| accepted non-slug name/kind index already owned | new `name.conflict` |
| primary CAS changed after read | new `state.conflict` |
| deletion blocked by dependents | new `resource.in_use` |
| cursor revision compacted | new `cursor.expired` |
| etcd unavailable/deadline with live caller context | new retryable `storage.unavailable` (HTTP 503) |
| caller context canceled | preserve `context.Canceled` in the error chain |
| corrupt record, mismatched id/owner/index, unknown schema | `internal` |
| watch compaction in an internal reconciler | relist; do not expose a public error |

When a transaction comparison fails, the repository rereads its compared keys
and evaluates them in this order: missing primary, occupied unique key, stale
primary revision, missing/mismatched secondary state, unknown invariant. This
prevents transaction timing from changing the public error code.

The proposed code names are owner-approved, but several do not exist in the
current error catalog. Public statuses are now defined; the internal taxonomy
still cannot represent one `validation.failed` code with both bad-request and
validation meanings. The exact Kind/Class representation below must be
accepted before repositories return the new codes. Mapping all storage
failures to `internal` would incorrectly turn transient etcd outages into
non-retryable HTTP 500 responses.

## Decision ledger

This ADR remains Proposed while the entries explicitly marked unresolved lack
owner approval or an exact key/interface needed for implementation.

1. **Blueprint bundle durability (durability accepted; storage mechanics
   unresolved).** Accepted ADR 0012 fixes normalized desired records as the
   sole reconciliation source, requires each immutable submitted bundle
   generation to be durable audit/reparse-verification input, and makes
   regenerated canonical Compose and materializations ephemeral. This ADR
   does not reopen those decisions. It still owns only the exact generation
   key layout, durable generation-record schema, publication transaction,
   retention window, deletion, and garbage-collection mechanics.

   **Recommended resolution:** normalized desired records are the sole
   reconciliation source of truth. Persist each submitted closed bundle as an
   immutable environment generation used only for audit and deterministic
   reparse verification, never as a rollback or reconciliation input. Write
   its file records first, then atomically publish the generation manifest and
   current-generation pointer; readers ignore unpublished generations. Retain
   the current generation and its immediate predecessor for 24 hours, then
   garbage-collect the predecessor. Regenerate canonical Compose and every
   materialization rather than persisting them. Apply ADR 0012's accepted
   limits: at most 64 files, 256 KiB per file, 768 KiB of aggregate decoded
   submitted file bytes, and a UTF-8 relative path of at most 240 bytes; secret
   payloads and compression are not allowed. This remains blocked on owner
   approval for the generation keys, publication transaction, retention, and
   deletion, and garbage-collection mechanics owned by this ADR; bundle
   durability itself is not an open choice.
2. **Remaining label uniqueness.** Define whether secret refs and entry
   destinations have uniqueness rules. Route collision validation also needs
   one canonical key/normalization contract.

   **Recommended resolution:** make secret refs unique by `(owner kind, owner
   id, secret kind, normalized ref)`. For an entry, claim one unique index for
   each `(environment id, entry kind, exact validated destination, target)`,
   where target is the literal `all` or a service id. Permit one `all` entry
   and one service-specific entry for the same destination because the
   accepted merge rule makes the service-specific value win. Make routes
   unique by `(environment id, exposure, normalized host, exact validated
   path pattern)`. Normalize hosts to lowercase IDNA A-label form without a
   trailing dot; validate but do not rewrite path patterns. Any occupied index
   owned by another id returns `name.conflict` with HTTP 409. The project-first
   and platform-fallback ownership contract is already fixed; this exact
   secret-ref uniqueness encoding remains owner-proposed with the rest of this
   item.
3. **Secret ownership (resolved by the authoritative product contract).**
   Reusable Secret resources have project or platform ownership, and effective
   lookup checks the project first before falling back to the platform. This
   records an existing product constraint; it does not accept this ADR.

   **Owner-proposed storage shape (not accepted):** use an explicit `project`
   or `platform` owner discriminator; `project_id` is required if and only if
   the owner is a project. CRUD always names an explicit scope. Effective
   lookup checks the project `(kind, ref)` first and the platform `(kind, ref)`
   only when the project record is absent; values never merge, and deleting a
   project value reveals the platform fallback. Keys and indexes include owner
   kind and owner id, using the literal `platform/-` owner sentinel for
   platform ownership. The eventual storage shape must conform to the
   authoritative ownership and precedence contract rather than treating
   `ProjectID`-only scaffold types as authoritative.
4. **Deletion lifecycle (behavior and tombstone key resolved).** Define the durable deleting/tombstone state, whether
   reads continue to expose a resource while its destructive Task runs, and
   exact cascade/block behavior for tenant, project, environment, service,
   component, connector, secret, runner, recovery-point, and Task deletion.
   Also define recovery after a host side effect succeeds but final deletion
   CAS fails.

   **Approved behavior:** the destructive action atomically creates its
   Task and a tombstone containing target kind, target id, target modification
   revision, task id, phase, and checkpoint. Reads and lists continue exposing
   the resource until final deletion, while every mutation and creation of a
   descendant fails with `resource.in_use` and HTTP 409. Process contained
   descendants in stable-id postorder; tenant contains projects and their
   environments, project contains environments, and environment contains its
   zones, services, routes, volumes, entries, scripts, release groups,
   components, connectors, and backup policy. Cross-resource consumers always
   block deletion rather than being rewritten. Service, component, connector,
   secret, and runner deletion therefore blocks while referenced. After host
   effects, finalization compares the captured revisions and atomically deletes
   the target, its indexes and ciphertext, the tombstone, and completes the
   Task. Mutations are blocked while the tombstone exists, so a final CAS
   conflict is an invariant failure: reread and retry only unchanged state;
   otherwise reconcile, fail the Task, and remove the tombstone atomically so
   an operator can retry. Completed tombstones are never retained. Recovery
   point deletion has no accepted endpoint, and Task deletion is not an MVP
   operator capability; neither is added by this ADR without owner approval.
   **Approved key:** `/v1/runtime/deletions/<target-kind>/<stable-id>`.
   Destructive repository methods still require the typed tombstone schema and
   resource-specific finalization contracts; no temporary key or plain-delete
   path is implemented.
5. **Task record, indexes, event identity, and retention (resolved).** The
   exact keys, index values, and journal mechanics are approved.

   **Approved keys:**

   ```text
   /v1/tasks/<task_id>
   /v1/indexes/tasks/operation/<operation_id>/<task_id>
   /v1/indexes/tasks/active-operation/<operation_id>
   /v1/runtime/task-events/<task_id>/<20-digit-zero-padded-sequence>
   /v1/runtime/task-event-dedup/<task_id>/<step_id>/<attempt>/<ordinal>
   /v1/runtime/task-queue/<task_id>
   /v1/runtime/assignments/<agent_id>/<task_id>
   ```

   **Approved mechanics:** the Controller assigns a `uint64` sequence
   starting at 1 and encodes it as 20 decimal digits. Every Agent-originated
   event carries stable `(task id, step id, attempt, ordinal)` identity. An
   identical duplicate returns its existing sequence; the same identity with a
   different SHA-256 payload is a protocol/internal failure. Insert the event
   and CAS-update the Task summary and next sequence in one transaction. Limit
   durable event JSON to 32 KiB and 1,000 events per Task; subprocess and log
   streams remain excluded. Retain terminal Tasks, events, and
   deduplication records for 90 days; never prune nonterminal Tasks. Run
   pruning daily in transactions within the 96 compare-and-mutation ceiling.
   The Agent protocol carries the approved stable identity. Encode both Task
   indexes and queue membership as canonical
   `{"schema":1,"record_id":"<task-id>"}` JSON with no raw-id compatibility.
   Queue order is ascending Task ULID and has no MVP priority tier. Claiming
   atomically moves the Task from `pending` to `running`, deletes queue
   membership, and creates this strict assignment value:

   ```json
   {"schema":1,"task_id":"task_...","agent_id":"agt_...","agent_generation":1,"claimed_task_revision":42,"assigned_at":"2026-08-20T12:00:00Z"}
   ```

   `claimed_task_revision` is the pending Task modification revision consumed
   by the claim CAS. Terminal Agent acknowledgement deletes the assignment and
   active-operation index in the same transaction that terminalizes the Task
   and idempotency marker. Reconnect lists the bounded assignment prefix and
   its Task primaries at one fixed revision; every record must match the
   authenticated Agent generation before redelivery. Every Task record carries
   the immutable `plan_id`, `plan_hash`, and positive `render_generation`
   required by the protobuf execution bundle. The current repository
   implements creation, claiming, reconnect recovery, pending abort, terminal
   acknowledgement, and append/read. Agent state events carry one-based
   `attempt` and `ordinal`; the Controller allocates the unrelated durable
   journal sequence. Log/output chunks are never supplied to the durable event
   repository. The daily 90-day Task pruning collector remains outstanding.
6. **Observed-state contract.** Define typed observed payloads, ownership when
   more than one Agent exists later, staleness/expiry, and retention. The
   desired/observed separation and projection rule are already approved.

   Agent `online` staleness is resolved independently: missing `Ready` for
   `max(3 * pull_interval, 30s)` makes the Agent stale. The remaining choice is
   the resource-observation envelope, authority, and retention below.

   **Recommended resolution:** keep exactly one latest typed observation per
   resource, not a history log or lease. Its common envelope contains schema,
   Agent id, environment id, resource kind and id, desired generation or plan
   hash, typed status/details, Agent `reported_at`, and Controller
   `received_at`; cap the record at 64 KiB. Only the currently assigned Agent
   may write it, with assignment and desired generation compared in the same
   transaction. Derive staleness at read time after the greater of three pull
   intervals or 30 seconds. Preserve stale evidence, then delete it 24 hours
   after its owner is deleted or reassigned. Component `healthy` and
   runner/Agent `online` remain projections. This is complete only for the
   accepted one-Agent, one-host MVP; future multi-Agent reassignment authority
   requires a separate accepted decision.
7. **Platform settings clarification (resolved).** The reveal-confirmation
   switch is Console-only click protection, not Controller behavior or access
   control. It is one browser-profile value, defaults to `false`, applies to
   every workspace in that Console, and remains in local storage under
   `groundplane-reveal-confirm`. It never enters etcd, the API, CLI, a Task, or
   activity. DNS, forwarding, tailnet, adapter, and runtime settings remain in
   their owning components. No generic platform-settings record, reserved key,
   endpoint, or command exists in the MVP.
8. **Pagination limits and expiry (resolved).** Set default/maximum page limits and the
   public behavior/class/status for an approved `cursor.expired` result. The
   id-ascending order and cursor encoding are already approved.

   **Approved decision:** default to 50 records and accept explicit limits
   from 1 through 200; return no total count. Bind cursor version, collection,
   scope, filters, order, fixed revision, last id, and limit in the query hash,
   and reject a changed query as `validation.failed` with HTTP 400. Reject a
   malformed cursor or encoded cursor longer than 2 KiB the same way, using an
   internal bad-request kind and ClassBadRequest. Configure
   periodic etcd auto-compaction with 24-hour retention. A revision already
   compacted returns `cursor.expired`, ClassConflict, HTTP 409, with
   machine-readable guidance to restart from the first page. Never restart
   silently because that can duplicate or omit records. Deployment must set
   compaction explicitly because etcd leaves it disabled by default.
9. **Conflict and storage errors (resolved; Kind cutover implemented).** Define classes and HTTP statuses for
   `name.conflict`, `state.conflict`, `resource.in_use`, and `cursor.expired`,
   and add missing resource-specific not-found codes. The code names and
   retryable `storage.unavailable` HTTP 503 mapping are already approved.

   **Approved decision:** map `name.conflict`, `state.conflict`,
   `resource.in_use`, `cursor.expired`, and `idempotency.in_progress` to
   ClassConflict and HTTP 409. Map `storage.unavailable` to ClassRetryable and
   HTTP 503, and `idempotency.mismatch` to ClassBadRequest and HTTP 400. Cursor
   syntax/query mismatch remains `validation.failed` and HTTP 400. Add
   ClassNotFound/HTTP 404 codes `zone.not_found`, `route.not_found`,
   `volume.not_found`, `entry.not_found`, `script.not_found`,
   `release.not_found`, and `backup_source.not_found`. A missing backup-policy
   singleton materializes its typed default rather than returning not-found.
   Details may expose stable resource ids and
   retry/restart guidance, but never raw etcd keys, etcd revisions, or secret
   material.

   The implemented narrow taxonomy is a closed internal `Kind` catalog mapping
   each Kind to public Code, Class, and HTTP status.
   `ClassBadRequest` represents malformed requests and idempotency mismatch. Use
   separate `KindMalformedRequest` and `KindValidationFailed` descriptors that
   both render public `validation.failed`, but map to ClassBadRequest/400 and
   ClassValidation/422 respectively. Use `KindIdempotencyMismatch` for
   `idempotency.mismatch`/ClassBadRequest and
   `KindIdempotencyInProgress` for conflict/409. Constructors accept Kind, not
   public Code; Error stores Kind; `errors.Is` compares Kind; no status/class
   override and no legacy Code constructor remain. Framework failures use
   explicit request Kinds for the accepted statuses; an unknown framework
   status fails closed as `request.failed`/500 rather than creating a dynamic
   status. Public problem tuples convert back to Kind only when their Code and
   status match one descriptor.
10. **Agent channel token clarification (resolved).** ADR 0016 supersedes the
    public one-time join-token design in ADR 0007. ADR 0011 now fixes one stable
    32-byte channel token encoded as unpadded base64url, encrypted at rest, and
    indexed by SHA-256 of the raw bytes. It is internal to Controller-owned
    enrollment, is never returned to a human, is not consumed on connection,
    and is revoked only by Agent removal. No join-token record, expiry,
    consumption state, public endpoint, or compatibility reader exists.
11. **Large atomic changes (resolved).** Define Groundplane's etcd transaction, request,
    and record ceilings plus the tombstone/progress/checkpoint state machine
    for any accepted mutation that cannot fit in one transaction.

    **Approved decision:** do not add a general multi-transaction
    desired-state commit protocol in the MVP and do not raise etcd's defaults.
    Preflight every atomic mutation at no more than 96 aggregate compares plus
    mutations in its selected success branch, a 1 MiB physical serialized
    `TxnRequest` including compare-failure reads, and 256 KiB per
    record. Reject a desired mutation or Blueprint apply that cannot fit before
    any write as `validation.failed` with HTTP 422, including the exceeded
    limits in safe details; partial desired state is never visible. A deletion Task may remove
    contained descendants in stable-id order using transactions within those
    limits because its tombstone blocks concurrent mutation. Persist its
    checkpoint after each successful batch and delete the parent last. ADR 0012
    limits a bundle to 512 aggregate resolved Compose resources together with
    its byte and parser ceilings. The limits here apply to persistence
    transactions and records rather than redefining Blueprint parsing.
12. **Mutation idempotency markers (resolved by ADR 0021).** The
    Idempotency-Key grammar, mutation scope, durable schema, retention, replay
    behavior, and same-transaction evidence contract are approved.

    **Approved decision:** require `Idempotency-Key` on every mutating
    REST `POST`, `PUT`, `PATCH`, and `DELETE`. A missing or grammatically invalid
    key is decoded request validation and returns `validation.failed` with HTTP
    422. Console and CLI generate one ULID
    per user intent and reuse it only for a transport retry. Accept 16 through
    128 ASCII characters matching `[A-Za-z0-9._:-]+`. Claim the marker in the
    same transaction as the resource mutation or Task creation. Preclaim
    validation and domain failures write no marker. An identical committed
    direct mutation or terminal Task-backed duplicate replays the exact
    status/body or original Task id; a pending duplicate returns
    `idempotency.in_progress` and HTTP 409; the same key with a different valid
    canonical intent returns `idempotency.mismatch` and HTTP 400. Perform
    synchronous validation before claiming a marker. Retain terminal markers
    for 90 days, never prune pending markers, reconcile pending state through
    its linked Task, and prune at most 24 markers using 48 compares plus 48
    deletes per daily transaction. Agent
    event deduplication uses the task-event identity instead. `api-cli.md` now
    requires the header. OpenAPI, Console, CLI, and generated clients still
    require synchronized implementation with no optional compatibility mode.

    **Accepted dependency:** ADR 0019 defines a versioned, length-framed typed
    canonical intent and requires the fixed 32-byte digest plus version to be
    sealed with the root-age-backed secret-value Protector before etcd. It
    explicitly forbids a plaintext request hash, digest, or hexadecimal
    encoding in durable state or logs and requires constant-time comparison
    after decryption. The durable marker key/value schema remains an owner
    decision in this ADR.

## Alternatives considered

### Owner-nested primary records

Keys such as `/tenants/<tenant>/projects/<project>` make owner lists simple but
make id-only API lookup require a global path index and move records if an
owner can ever change. Flat id primaries plus owner indexes keep the primary
identity stable and are proposed instead.

### One monolithic environment document

One CAS gives a coherent desired aggregate, but every child mutation rewrites
the entire environment, unrelated edits conflict, flat id lookup needs
additional indexes, and a large Blueprint can exceed one etcd transaction.
This remains an owner choice because the current core aggregate and API
document rule conflict.

### Duplicate full records in indexes

Embedding payloads in owner indexes avoids same-revision primary reads but
creates multiple sources that every update must keep byte-identical. Indexes
therefore contain ids only.

### Offset pagination or current-revision cursors

Offsets and unpinned cursors skip or duplicate records under concurrent
writes. Pinned revision plus exclusive last key is proposed instead.

### Blind last-write-wins updates

Blind puts can overwrite a concurrent rename, task transition, or deletion and
cannot maintain indexes atomically. Every write therefore compares the state
it was derived from.

## Consequences if accepted

- Every id lookup, owner list, label resolution, rename, and final deletion has
  one deterministic key/transaction shape.
- Slug and name uniqueness are enforced by etcd comparisons rather than
  process-local checks.
- Pagination and reconciliation observe real etcd revisions and cannot lose
  mutations between a snapshot and watch.
- Secrets remain physically separate from listable metadata and are deleted
  atomically with it.
- Typed repositories can remain domain-specific while sharing one codec,
  cursor, index, and CAS implementation.
- Store's bounded range-at-revision and same-revision multi-get mechanics are
  the required read path for pagination.
- Acceptance requires coordinated updates to the product contract, error
  catalog, desired-state model, Agent task journal, and ADR 0012 rather than a
  storage-only implementation.
