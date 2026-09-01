# ADR 0055: Revision-bound Network observation and backing Blueprint scope

Status: Deferred post-MVP

MVP note: ADR 0037 accepts only creation with one new backing-owned Zone.
Existing-Zone selection and the observation protocol below are outside the MVP
and do not block C10.

## Context

The C10 Backing Service facade is the existing hierarchy:

```text
Project(kind=backing) -> Environment(name=main) -> Service(adapter-backed)
```

ADR 0037 proposes explicit creation against either a newly created Network or
an existing backing-owned Network. The existing-Network branch cannot be made
safe from a Docker list result alone. Docker knows Engine facts; it does not
know an etcd Zone record's `ModRevision`, the current Blueprint revision, or
the Controller projection and mutation fences that authorize selection. A
create transaction also cannot know its future etcd commit revision and place
that value in a Docker label. Guessing, echoing expected labels, or treating a
Task result as steady-state evidence would allow stale or drifted topology to
authorize a new Backing Service.

The current contracts and scaffold also disagree at several seams:

- ADR 0037 requires revision-bound, fresh Network evidence but does not define
  the request, wire, durable row/head, lease, or final publication CAS that
  produces that authority.
- ADR 0048 defines the sole final schema-1 Agent channel and batched
  `ObservedState`, but does not yet bind a steady-state Network snapshot to one
  Controller-issued fixed-revision request.
- `docs/blueprint.md` still describes authored `x-gp-network`,
  `create_network`, `join_network`, and native external-Network syntax even
  though C10 creation owns an explicit Network-choice union and Attach
  resolution generates consumer membership.
- the parser assumes an Environment envelope, has no discriminated backing
  scope, and does not parse the required root-only `x-gp-network-pool`.
- the renderer requires a Tenant and emits `com.groundplane.tenant-id` for
  every Network, which cannot represent a platform-owned backing Project.
- the public Backing Service facade omits the sole Service's immutable
  `backing_network_id`, while ADR 0037 proposes a second nested name,
  `network.zone_id`, for the same identity.

Implementing these seams independently would force the wire, persistence,
parser, renderer, and three human clients to choose incompatible meanings.

## Scope

This proposal closes only the MVP contract for:

- Controller-issued, revision-bound steady-state Network observation over the
  sole local authenticated schema-1 Agent channel;
- durable private observation publication and freshness authority;
- fixed-revision admission of an existing backing-owned Network during C10
  creation;
- the Environment-versus-backing Blueprint parser scope and required typed
  Environment pool;
- generated-only external backing-Network membership and Project-kind-aware
  managed labels; and
- the canonical `backing_network_id` on existing Backing Service Create, List,
  and Show projections.

It does not create a second Network resource, a public observation endpoint,
an operator refresh action, another desired-state grammar, or a compatibility
decoder. It does not decide adapter release contents, Backing Service data
lifecycle, multi-host placement, high availability, or enterprise policy.
Those remain with the existing product and relevant ADR authorities.

## Decision

### 1. The Controller issues one fixed-revision observation authority

Every successful Agent pull cycle may receive at most one internal
`ObservationRequest`. The Controller constructs it from one linearizable etcd
read at positive revision `authority_revision`. The request is an immutable
observation authority, not desired state, and cannot mutate a resource.

ADR 0048's final schema-1 `ControllerMessage` adds the next collision-free
oneof member. `AgentMessage` tags do not change; `ObservedState` remains tag 4.

```proto
message ControllerMessage {
  oneof payload {
    // ADR 0048 fields 1 through 16 remain unchanged.
    ObservationRequest observation_request = 17;
  }
}

enum NetworkObservationOwnerKind {
  NETWORK_OBSERVATION_OWNER_KIND_UNSPECIFIED = 0;
  NETWORK_OBSERVATION_OWNER_KIND_ENVIRONMENT = 1;
  NETWORK_OBSERVATION_OWNER_KIND_BACKING_PROJECT = 2;
}

message ObservationRequest {
  bytes request_id = 1;
  uint64 authority_revision = 2;
  bytes authority_sha256 = 3;
  repeated NetworkObservationAuthority networks = 4;
  uint64 observation_sequence = 5;
  bytes stream_session_id = 6;
}

message NetworkObservationAuthority {
  string zone_id = 1;
  uint64 zone_record_revision = 2;
  string project_id = 3;
  uint64 project_revision = 4;
  string environment_id = 5;
  uint64 environment_revision = 6;
  uint64 blueprint_head_revision = 7;
  string blueprint_revision_id = 8;
  uint64 projection_revision = 9;
  string plan_id = 10;
  bytes plan_hash = 11;
  uint64 render_generation = 12;
  string compose_network_name = 13;
  string physical_network_name = 14;
  string subnet = 15;
  bool internal = 16;
  NetworkObservationOwnerKind owner_kind = 17;
  string owner_id = 18;
  repeated LabelPair expected_managed_labels = 19;
  bytes owner_environment_mutation_epoch = 20;
  uint64 owner_environment_mutation_epoch_revision = 21;
  optional bool owner_environment_operation_lock_absent = 22;
  string expected_ipam_driver = 23;
  repeated NetworkObservationIPAMConfig expected_ipam_configs = 24;
  repeated LabelPair expected_ipam_options = 25;
}
```

`request_id` and `stream_session_id` are exactly 16 fresh Controller-CSPRNG
bytes. The stream id is sampled once after successful Authenticate and remains
fixed for that authenticated Connect stream. `observation_sequence` is the
next positive value from the durable per-Agent sequence below and never
repeats for that AgentGeneration.
`authority_revision`, every record revision, and `render_generation` are in
`1..MaxInt64`. Stable ids use their canonical kind grammar. `plan_hash` is
exactly 32 raw bytes. The request contains 0..512 authority rows sorted by
unsigned `zone_id` bytes. Every `expected_managed_labels` list is sorted by
key then value, has unique keys, and contains exactly 11 pairs for a backing
owner or 12 for an Environment owner.

Every authority row also carries the owner Environment mutation-epoch key's
exact raw bytes and `ModRevision` as read at `authority_revision`.
`owner_environment_mutation_epoch` is 1..1,024 bytes and must decode under the
sole current epoch grammar. The optional operation-lock absence field has
required presence and value `true`, proving the lock key had version zero at
that same revision. Its absence, false, a missing or malformed epoch, or a
nonzero lock means no authority row may be issued. Expected IPAM is the
complete projection-owned shape defined below.

The Controller computes `authority_sha256` as SHA-256 of the deterministic
protobuf serialization of the complete `ObservationRequest` with
`authority_sha256` cleared. The populated field is exactly those 32 raw bytes.
There is no JSON digest, outer-envelope digest, map-order alternative, or
digest of a partial row set.

The authority carries the actual etcd `ModRevision` values read at
`authority_revision`. No renderer or Controller code writes an etcd revision
into a Docker label, and the Agent never claims Docker can attest one.

One request contains every managed Zone on the MVP host. The exact host-wide
maximum is 512 managed Zones. This is a single-host MVP bound, not pagination,
partitioning, sampling, or an enterprise scale target. Request construction
fails closed if the durable count, Zone primaries, and complete authority rows
do not prove the same 0..512 inventory at the request revision.

### 2. `ObservedState` returns one complete response to that authority

ADR 0048's final schema-1 batching fields remain at tags 1 through 4. The
request binding uses the next tags:

```proto
message ObservedState {
  repeated ObservedProject projects = 1;
  bytes snapshot_id = 2;
  uint64 batch_ordinal = 3;
  bool final_batch = 4;
  bytes observation_request_id = 5;
  bytes observation_authority_sha256 = 6;
}
```

`snapshot_id` is exactly 16 fresh Agent-CSPRNG bytes for this completed
inspection. The request id and authority digest are byte-equal to the one
outstanding request. Batches use contiguous zero-based ordinals and
`final_batch=true` only on the last batch. A partial, duplicate, reordered,
mixed-request, mixed-snapshot, or post-final sequence is discarded and never
advances durable observation authority.

The current `ObservedNetwork` fields 1 through 3 retain their task-scoped
Compose-result meaning. They are not overloaded. `ObservedProject` gains a
separate authority-row collection:

```proto
message ObservedProject {
  string project_name = 1;
  google.protobuf.Timestamp observed_at = 2;
  repeated ObservedContainer containers = 3;
  repeated ObservedNetwork networks = 4;
  repeated ObservedVolume volumes = 5;
  repeated ObservedCollision collisions = 6;
  string project_id = 7;
  string environment_id = 8;
  repeated ObservedNetworkAuthorityRow network_observations = 9;
}

enum NetworkObservationRowState {
  NETWORK_OBSERVATION_ROW_STATE_UNSPECIFIED = 0;
  NETWORK_OBSERVATION_ROW_STATE_PRESENT = 1;
  NETWORK_OBSERVATION_ROW_STATE_MISSING = 2;
  NETWORK_OBSERVATION_ROW_STATE_COLLISION = 3;
  NETWORK_OBSERVATION_ROW_STATE_INVALID = 4;
}

enum NetworkObservationInvalidReason {
  NETWORK_OBSERVATION_INVALID_REASON_UNSPECIFIED = 0;
  NETWORK_OBSERVATION_INVALID_REASON_RESPONSE_OVERSIZE = 1;
  NETWORK_OBSERVATION_INVALID_REASON_STRING_ENCODING = 2;
  NETWORK_OBSERVATION_INVALID_REASON_FIELD_BOUNDS = 3;
  NETWORK_OBSERVATION_INVALID_REASON_DOCKER_ID = 4;
  NETWORK_OBSERVATION_INVALID_REASON_LABEL_SET = 5;
  NETWORK_OBSERVATION_INVALID_REASON_MANAGED_IDENTITY = 6;
  NETWORK_OBSERVATION_INVALID_REASON_IPAM = 7;
}

message NetworkObservationIPAMConfig {
  string subnet = 1;
  string ip_range = 2;
  string gateway = 3;
  repeated LabelPair aux_addresses = 4;
}

message ObservedNetworkAuthorityRow {
  string zone_id = 1;
  uint64 zone_record_revision = 2;
  NetworkObservationRowState state = 3;
  uint32 candidate_count = 4;
  string docker_network_id = 5;
  string physical_network_name = 6;
  string driver = 7;
  string scope = 8;
  bool internal = 9;
  bool attachable = 10;
  bool ingress = 11;
  bool ipv6_enabled = 12;
  string ipam_driver = 13;
  repeated NetworkObservationIPAMConfig ipam_configs = 14;
  repeated LabelPair managed_labels = 15;
  string compose_project_label = 16;
  string compose_network_label = 17;
  string project_id = 18;
  string environment_id = 19;
  string blueprint_revision_id = 20;
  string plan_id = 21;
  bytes plan_hash = 22;
  uint64 render_generation = 23;
  NetworkObservationOwnerKind owner_kind = 24;
  string owner_id = 25;
  repeated LabelPair ipam_options = 26;
  NetworkObservationInvalidReason invalid_reason = 27;
  bytes raw_relevant_facts_sha256 = 28;
  uint32 raw_relevant_facts_size = 29;
}
```

Context validation is closed:

- inside `AgentMessage.observed_state`, legacy `ObservedProject` fields 3
  through 6 are empty; fields 7 through 9 are the sole Network snapshot;
- inside `TaskAck.ComposeTaskResult`, `network_observations` is empty and the
  existing task-scoped fields retain their meaning; and
- an unset or unknown enum and every unknown protobuf field are rejected under
  ADR 0048's schema-1 rule.

The Controller groups request rows by `(project_id, environment_id)`. The Agent
returns exactly one `ObservedProject` for each nonempty group, sorted by the
unsigned-byte tuple. A group is never split across envelopes.
`project_name` is exactly `gp-` plus lowercase `environment_id`. Within one
group, `network_observations` contains exactly one row per requested authority,
sorted by `zone_id`; a missing Network is an explicit row, never an omission.

An authority containing zero Networks receives one ordinal-zero final batch
with `projects` empty. Thus an empty inventory is positive complete evidence,
not absence of a response.

### 3. The Agent reports actual Moby facts after a stable inspection

The authenticated stream binds a response to:

```text
agent_id
AgentGeneration
process_generation
AgentConfig ModRevision
pull_interval_seconds
stream_session_id
observation_sequence
request_id
authority_sha256
snapshot_id
```

The Agent enumerates every Moby Network that satisfies at least one predicate:

- its physical name begins with `gp_net_`;
- any label key begins with `com.groundplane.`; or
- its physical name equals a name in the request.

It sorts the first list by Docker Network id, inspects every candidate, then
repeats the list. A changed candidate set, a disappeared candidate, or a
changed name or relevant label makes the cycle incomplete. The Agent sends no
complete snapshot for that cycle. It never fills actual fields by copying
expected request labels.

For each requested Zone, the Agent matches the union of candidates that have
the requested physical name or whose actual
`com.groundplane.network-id` value is the requested Zone id. The resulting row
is exactly one of:

| State | Candidate count | Present-only fields |
| --- | ---: | --- |
| `present` | 1 | all populated and valid |
| `missing` | 0 | every present-only scalar, bytes, and list at protobuf default/empty |
| `collision` | 2..4096 | every present-only scalar, bytes, and list at protobuf default/empty |
| `invalid` | 1 | only a valid candidate id, closed invalid reason, and raw-facts digest/size may be populated |

`zone_id` and `zone_record_revision` are required and match the request in all
states. `unspecified` always rejects. More than 4096 matching candidates fails
the response. `invalid` is mandatory when exactly one candidate exists but a
relevant actual fact cannot satisfy the wire grammar. Its
`candidate_count` is 1. It carries `docker_network_id` only when that exact id
is valid; every other present-only field is default/empty. `invalid_reason` is
the first applicable reason in enum-number order, `raw_relevant_facts_sha256`
is exactly 32 bytes, and `raw_relevant_facts_size` is 1..1,048,577. A missing
reason/digest/size or any populated forbidden field rejects the row.

An unrelated candidate that collides with no requested Zone id or physical
name creates no row. Because every request contains the host's complete
managed-Zone inventory, the snapshot is complete for every managed Zone, not a
global container or Volume snapshot.

For `present`, the wire fields contain exactly these actual Moby facts:

```text
docker_network_id
physical_network_name
driver
scope
internal
attachable
ingress
ipv6_enabled
ipam_driver
ipam_configs[0..4]{subnet,ip_range,gateway,aux_addresses[0..32]}
ipam_options[0..32]
managed_labels
compose_project_label
compose_network_label
project_id
environment_id
blueprint_revision_id
plan_id
plan_hash
render_generation
owner_kind
owner_id
```

`docker_network_id` is exactly 64 lowercase hexadecimal bytes. Structured
fields `project_id` through `owner_id` are parsed from actual labels rather
than supplied from the request. The Controller independently requires those
fields to equal the parsed actual labels and later compares both to authority.

IPAM configs retain Engine order. Aux-address and option maps become sorted
unique `{key,value}` pairs. Every IPAM string is valid UTF-8 without NUL and at
most 255 bytes; an IPAM collection or value outside these bounds produces an
`invalid` row with reason `IPAM`, never a truncated present row.

`managed_labels` is the actual sorted subset of every Moby label whose key
begins with `com.groundplane.`. It has 0..16 pairs with unique keys. Each key
is 1..128 UTF-8 bytes, each value is 0..256 UTF-8 bytes, and neither contains
NUL. Those are the `present` grammar. For exactly one candidate, a duplicate,
invalid UTF-8/NUL value, excessive count, or excessive byte length produces a
complete `invalid` row under the closed priority and raw-evidence contract; it
does not preserve a prior good head. A valid but unexpected reserved label is
reported in a `present` row and makes the admission comparison fail; it is not
hidden by returning an older good row.

For an invalid row, raw evidence is computed from the exact Engine inspect
response body. The observer reads at most 1,048,577 bytes, where the final byte
is solely the deterministic over-limit sentinel:

```text
raw_relevant_facts_sha256 = SHA256(
  "groundplane:network-invalid-relevant-facts:v1\x00" ||
  BE32(raw_relevant_facts_size) || exact_inspect_response_bytes
)
```

The bytes are never stored or exposed; only the bounded size and digest cross
the wire and enter the private row. They are actual Engine bytes and never
contain expected values synthesized by the Agent. A response that exceeds the
body cap records reason `RESPONSE_OVERSIZE`, the exact capped-reader observed
size `1,048,577`, and the digest of those exact first 1,048,577 bytes; it
closes the Engine response without accepting a present candidate. A body of at
most 1,048,576 bytes uses its exact full length and bytes.

### 4. Admission requires the exact physical and label contract

A present Network is selectable only when every actual fact equals:

```text
physical_network_name == "gp_net_" + zone_id
driver == "bridge"
scope == "local"
internal == Zone.internal
attachable == false
ingress == false
ipv6_enabled == false
ipam_driver == authority expected IPAM driver
ipam_configs == authority expected ordered IPAM configs
ipam_options == authority expected sorted IPAM options
compose_project_label == "gp-" + lowercase(environment_id)
compose_network_label == projection Compose network name
```

The exact required managed labels are:

```text
com.groundplane.managed=true
com.groundplane.kind=network
com.groundplane.network-id=<zone_id>
com.groundplane.owner-kind=environment|backing_project
com.groundplane.owner-id=<owner_id>
com.groundplane.project-id=<project_id>
com.groundplane.environment-id=<environment_id>
com.groundplane.blueprint-revision-id=<blueprint_revision_id>
com.groundplane.plan-id=<plan_id>
com.groundplane.plan-hash=sha256:<64 lowercase hex>
com.groundplane.render-generation=<canonical positive decimal>
```

An Environment-owned Network additionally requires exactly:

```text
com.groundplane.tenant-id=<tenant_id>
```

A backing-owned Network must not contain `com.groundplane.tenant-id` at all.
An empty value is presence and fails. Actual `managed_labels` must equal the
appropriate 11- or 12-pair authority list; a missing, changed, or additional
reserved label is drift.

A backing-owned candidate also requires:

```text
owner_kind == backing_project
owner_id == project_id
Project.kind == backing
Project.tenant_id is absent
Environment.project_id == project_id
Environment.name == main
Zone.environment_id == environment_id
```

The renderer becomes Project-kind aware. It emits a Tenant label only for an
Environment whose owner Project is tenant-owned. It does not manufacture an
empty Tenant label for a backing Project.

The immutable Compose projection and observation authority retain the complete
expected IPAM driver, 0..4 ordered configs, 0..32 sorted options, and each
config's subnet, range, gateway, and 0..32 sorted auxiliary addresses.
Groundplane does not leave bridge-IPAM defaults for Moby to choose. A managed
Zone Network's Engine create request has exactly this IPAM shape:

- driver is the literal `default`;
- options is an allocated empty map;
- configs has exactly one element;
- that element's subnet is the Zone's canonical IPv4 CIDR;
- its gateway is the canonical dotted-decimal address whose unsigned IPv4
  value is the subnet network address plus one;
- its IP range is the empty string; and
- its auxiliary-address map is allocated and empty.

The Controller derives expected IPAM only by that algorithm; it never learns
or copies a Moby-selected gateway. A managed Zone subnet prefix length must be
in the inclusive range 0..30. Prefixes `/31` and `/32` are rejected with
`422 validation.failed` before Task publication because network-address-plus-
one is not between the network and broadcast addresses. Configured-parent
containment and global non-overlap remain separate admission requirements.

For observation, the Agent maps an absent or empty Moby IPAM options map to
the same sorted zero-pair representation and an absent or empty auxiliary-
address map to the same sorted zero-pair representation. An absent IP range
maps to the empty string. No non-empty value is normalized. It records zero
through four returned configs in Engine order after validating every subnet,
IP range, gateway, auxiliary name/address, and option key/value against the
closed wire grammar. More than four configs, malformed values, duplicate map
keys after decoding, or an unrepresentable response produces
`NETWORK_OBSERVATION_INVALID_REASON_IPAM` evidence, never `present`.

Present admission requires literal driver `default`, exactly one config with
the derived subnet and gateway, empty IP range, empty auxiliary addresses, and
empty options. Any additional config, option, auxiliary address, range,
gateway, or driver value is drift and returns `409 state.conflict`.

### 5. Wire bounds and scheduling remain closed

Every cap is `proto.Size` of the complete value named and ADR 0048's five-byte
gRPC-frame exclusion remains in force:

| Value | Maximum serialized bytes |
| --- | ---: |
| any `ControllerMessage` | 5,242,880 |
| `ControllerMessage.observation_request` outer | 4,390,912 |
| nested `ObservationRequest` | 4,390,904 |
| one `NetworkObservationAuthority` | 8,192 |
| any `AgentMessage` | 16,777,216 |
| `AgentMessage.observed_state` outer | 16,777,216 |
| one Network-snapshot `ObservedProject` | 8,650,752 |
| one `ObservedNetworkAuthorityRow` | 16,384 |

There are exactly as many authority and response rows as the complete managed
Zone inventory, 0..512. There are at most 512 response rows per project. A
project group that cannot fit its cap fails rather than being split. Every
sender additionally checks actual complete `proto.Size`; the per-row caps do
not imply that 512 individually maximal rows can bypass the outer cap.

`ObservationRequest` does not fit ADR 0048's 262,144-byte control queue and
does not borrow Task-assignment capacity. Each connection has one separate
outbound observation-authority queue of exactly one record and 4,390,912
serialized bytes. The sole-writer cyclic class order becomes:

```text
control, observation-authority, assignment, checkpoint,
terminal, observed, event, bulk
```

At most one record emits from an eligible class per scan. Exactly one request
may be outstanding. The Agent completes or discards it before its next Ready,
and the Controller sends no next request until the completed response either
publishes its head or its original lease is abandoned/expired. Disconnect
before a complete validated response discards the request and every partial
response. Disconnect after admission does not detach the persistence worker
from its exact staging/lease/session authority. Reconnect never resumes a
partial Network snapshot and cannot receive a new request while live staging
exists.

### 6. Complete snapshots publish immutable rows and one leased head

The Controller validates and assembles the entire wire snapshot before any
persistence. Private observation values use the existing strict record
envelope:

```text
{schema:1,kind:<kind>,data:<closed object>}
```

Unknown fields reject. The exact private keys are:

```text
/v1/records/agent-network-observation-rows/<agent_id>/<AgentGeneration>/<snapshot-id-hex>/<zone_id>
/v1/records/agent-network-observation-heads/<agent_id>
/v1/runtime/agent-network-observation-staging/<agent_id>
/v1/runtime/agent-network-observation-sequences/<agent_id>
```

`snapshot-id-hex`, `process_generation`, and `observation_request_id` encode
16 bytes as exactly 32 lowercase hexadecimal characters. Every SHA-256 encodes
as `sha256:` followed by 64 lowercase hexadecimal digits. Timestamps are
canonical UTC RFC3339Nano and round-trip byte-identically through
`Format(time.RFC3339Nano)`. Positive revisions and generations are
`1..MaxInt64`.

The `AgentGeneration` path segment is its canonical positive base-10 form with
ASCII digits only and no leading zero. Zero, a sign, whitespace, another base,
or a byte-different re-encoding rejects the key. The sequence value is a
strict record whose sole data field is the current `uint64` in `1..MaxInt64`;
absence means zero. The Controller advances it by exactly one under its exact
`ModRevision` before sending a request. That allocation transaction also
compares the Agent primary/config and per-Agent staging version zero. A live
staging key blocks allocation. A request that disconnects before completion
may consume a sequence number; gaps are valid, reuse is not. Overflow is
`internal`. This key is monotonic identity, not observation authority and is
never leased.

#### Immutable row

The row kind is exactly `agent-network-observation-row`. Its `data` fields are
exactly:

```text
agent_id
agent_record_generation
snapshot_id
observation_request_id
observation_sequence
authority_sha256
zone_id
zone_record_revision
project_id
project_revision
environment_id
environment_revision
blueprint_head_revision
blueprint_revision_id
projection_revision
plan_id
plan_hash
render_generation
compose_network_name
physical_network_name
subnet
internal
owner_kind
owner_id
expected_managed_labels
owner_environment_mutation_epoch
owner_environment_mutation_epoch_revision
owner_environment_operation_lock_absent
expected_ipam_driver
expected_ipam_configs
expected_ipam_options
state
candidate_count
actual
invalid_reason
raw_relevant_facts_sha256
raw_relevant_facts_size
row_sha256
```

`owner_kind` is `environment | backing_project`; `state` is
`present | missing | collision | invalid`. `expected_managed_labels` is the sorted
`{key,value}` array from the authority. `actual` is JSON `null` for missing or
collision and for invalid. Invalid-reason/digest/size fields follow the wire
invariants and are otherwise their zero values. For present `actual` is a
required closed object with exactly:

```text
docker_network_id
physical_network_name
driver
scope
internal
attachable
ingress
ipv6_enabled
ipam_driver
ipam_configs
ipam_options
managed_labels
compose_project_label
compose_network_label
project_id
environment_id
blueprint_revision_id
plan_id
plan_hash
render_generation
owner_kind
owner_id
```

The row key segments must equal the decoded Agent, generation, snapshot, and
Zone fields. Authority fields reconstruct byte-equal protobuf authority,
including mutation epoch, lock absence, and complete expected IPAM. After
reconstructing byte-identical `A` and `O`, the Controller constructs the
complete strict `agent-network-observation-row` record envelope. The UTF-8 JSON
encoding of that whole envelope, including `{schema,kind,data}`, is at most
65,536 bytes. This is one combined admission bound, not independent estimates
for `A`, `O`, or row data. A response whose reconstructed envelope exceeds the
bound is invalid protocol input: the Controller publishes neither rows nor a
head, abandons the request under its original lease, and closes the
authenticated stream. The Agent emits bounded
`NETWORK_OBSERVATION_INVALID_REASON_RESPONSE_OVERSIZE` evidence rather than an
oversized `present` row when actual relevant facts cannot be represented
within this bound. A row is immutable after its first successful put. Its key
and value are attached to the same original observation lease as staging and
the eventual head.

Let `A` be deterministic protobuf serialization of the exact
`NetworkObservationAuthority`, and `O` deterministic protobuf serialization
of the exact `ObservedNetworkAuthorityRow`, both after recursive unknown-field
rejection. Let `BE16`, `BE32`, and `BE64` be fixed-width unsigned big-endian
integers. With raw binary digests and ids decoded from the record, the sole row
digest is:

```text
row_sha256 = SHA256(
  "groundplane:agent-network-observation-row:v1\x00" ||
  BE16(len(agent_id)) || agent_id ||
  BE64(agent_record_generation) ||
  snapshot_id[16] ||
  observation_request_id[16] ||
  BE64(observation_sequence) ||
  authority_sha256[32] ||
  BE32(len(A)) || A ||
  BE32(len(O)) || O
)
```

The stored value is the prefixed lowercase representation. The JSON fields
must reconstruct byte-equal `A` and `O`; decode recomputes and rejects a
mismatch. There is no JSON-canonicalization alternative.

#### Staging publication proof

The staging kind is exactly `agent-network-observation-staging`. Its `data`
fields are exactly:

```text
agent_id
agent_record_generation
process_generation
stream_session_id
snapshot_id
observation_request_id
observation_sequence
authority_revision
authority_sha256
total_row_count
next_row_ordinal
next_row_batch_ordinal
row_set_chain_sha256
received_at
expires_at
lease_id
created_at
```

`total_row_count` is 0..512 and `next_row_ordinal` is 0..total. Key segments
must equal the value. `next_row_batch_ordinal` starts at zero and advances by
one for each successful deterministic row batch. `row_set_chain_sha256` is
always the current `H_i`; only a final head calls the same bytes `H_n` and
stores them as `row_set_sha256`. `lease_id` is the positive etcd lease attached
to this staging key and every row. Staging is the sole in-flight per-Agent
coordinator, is never observation authority, and no admission read may use it.
`stream_session_id` is exactly sixteen authenticated Controller-CSPRNG bytes
on the wire and exactly 32 lowercase hexadecimal characters without a prefix
in this JSON record. Its decoded bytes equal the authenticated stream/session
identity bound into the request. Absence, mixed case, non-hexadecimal input,
wrong length, or mismatch is durable corruption and returns `500 internal`.

Rows sort by unsigned `zone_id` bytes. Let `D_i` be the raw row digest and
`Z_i` the Zone id. The sole row-set chain is:

```text
H_0 = SHA256(
  "groundplane:agent-network-observation-row-set-init:v1\x00" ||
  BE32(row_count)
)

H_i = SHA256(
  "groundplane:agent-network-observation-row-set-step:v1\x00" ||
  H_(i-1) || BE16(len(Z_i)) || Z_i || D_i
)
```

The final `row_set_sha256` is prefixed lowercase `H_n`. For zero rows it is
`H_0`. Staging stores the current prefixed `H_i` after exactly
`next_row_ordinal=i` rows; it never names an intermediate value
`row_set_sha256`.

When the complete response passes wire validation, the Controller freezes
`received_at=T` immediately. It computes
`L=max(3*pull_interval_seconds,30s)` and `expires_at=T+L`, then grants exactly
one etcd lease for this observation. It never grants, refreshes, keepalives, or
substitutes a later lease. The initial staging transaction compares current
Agent primary/config, per-Agent staging version zero, and the exact already-
allocated sequence value, then puts staging under that lease. The request,
process generation, authenticated stream-session id, sequence, complete row
count, `H_0`, times, and lease id are frozen there.

Each deterministic row transaction takes the largest next consecutive prefix
of one through twelve sorted rows for which
`proto.Size(etcdserverpb.TxnRequest)` is at most 921,600 bytes. It never uses
an estimate of JSON payload bytes. For a batch of `n`, the transaction has
exactly `n + 1` compares, the exact staging `ModRevision` plus `n` row-key
version-zero compares, and exactly `n + 1` mutations, `n` row puts plus the
next staging put. Every put uses the original lease. Thus `1 <= n <= 12`, a
row transaction has at most thirteen compares and thirteen mutations, and a
512-row snapshot uses between 43 and 512 row transactions according to the
frozen canonical values. The exact greedy partition is reproducible from
those values. Zero rows uses the staging-creation transaction and proceeds
directly to final publication.

Before every batch and final transaction, the Controller requires the original
lease to exist, its reported remaining TTL to be positive, and Controller time
to be earlier than the frozen `expires_at`. It sets the storage request
deadline no later than `expires_at`. If the lease or absolute lifetime cannot
cover the pending transaction, it abandons the worker and lease; it never
resets freshness with a new timestamp or lease.

An unknown row-batch outcome is resolved with one linearizable fixed-revision
read of staging and every row key from that batch. If staging and all rows
prove the exact next ordinal, batch ordinal, `H_i`, bytes, and original lease,
the batch committed and processing advances. If staging remains the exact
prior value and every batch row is absent while the lease remains live, the
same transaction is retried. Any partial set, changed bytes, different lease,
or sequence/session mismatch while the lease remains live is durable
corruption: the worker returns `internal` and abandons the original lease.

If the original lease is expired or absent, recovery instead performs one
linearizable fixed-revision read of staging, every row key for this
`(agent_id, AgentGeneration, snapshot_id)`, and the head. Staging and all those
rows absent, with no head naming this snapshot/request/sequence, is safe lease-
expired abandonment, not corruption. The worker does not retry or resume,
grant a replacement lease, or delete or rewrite an exact prior head. That
prior head remains attached to its distinct earlier lease. Any new-request
staging, row, or head that remains visible after expiry, or any partial or
mismatched result, is durable corruption and returns `500 internal`.

#### Leased head

The head kind is exactly `agent-network-observation-head`. Its `data` fields
are exactly:

```text
agent_id
agent_record_generation
process_generation
agent_config_revision
pull_interval_seconds
snapshot_id
observation_request_id
observation_sequence
authority_revision
authority_sha256
row_count
row_set_sha256
received_at
expires_at
lease_id
```

`row_count` is 0..512. `received_at`, `expires_at`, and `lease_id` are copied
byte-for-byte from staging. `created_at` in staging equals `received_at` and
does not define a second time. Agent time is diagnostic only. `expires_at` was
frozen before persistence and is exactly:

```text
received_at + max(3 * pull_interval_seconds, 30 seconds)
```

The head is attached to the same original lease already attached to staging
and every immutable row. No lease is granted at head publication. Only when
staging count and `H_i` equal the frozen complete set and final `H_n` does the
Controller submit the head transaction. That transaction compares exactly:

1. Agent primary `ModRevision`;
2. Agent config `ModRevision`;
3. staging `ModRevision`; and
4. current head `ModRevision`, or version zero when absent.

Before submit, the Controller also validates decoded Agent phase, generation,
config, stream session, and observation sequence. The transaction performs
exactly two mutations: put the head under the original lease and delete
staging. It remains six etcd operations total. Failure abandons the original
lease and publishes no head. Per-Agent staging serialization and the monotonic
sequence prevent an older worker from reading a newer head and overwriting it.

An unknown final outcome uses one linearizable fixed-revision read. An exact
new head under the original lease with staging absent proves commit. The exact
old head plus unchanged exact staging under the live original lease permits
retry of the same final transaction. Any other head/staging/lease/sequence
combination while the original lease remains live is `internal` and the
original lease is abandoned. If the original lease is expired or absent, the
same all-staging/all-snapshot-rows/no-new-head fixed-revision absence test used
for row batches proves safe abandonment. An exact prior head on its distinct
lease is left untouched. If the final CAS succeeded and its original lease
then expired, an overwritten prior head is not reconstructed or used as
fallback; head absence is current. Any new-request object still visible after
expiry or any partial/mismatched state is durable corruption.

Lease expiry deletes staging and every unpublished row automatically. Cleanup
does not invent a durable-resume path and there is no permanently protected
orphan prefix. Rows under a published head share its lease and expire with it;
missing or changed rows while a head remains are durable corruption and
`internal`, not permission to use an older head.

A new complete zero-Network snapshot publishes a zero-row head. Old heads are
never fallback. No next request is sent until exact head publication or
abandonment/expiry of the original lease. On reconnect, a live staging record
blocks the new stream until it expires or exact outcome recovery completes.
Agent generation or config rotation deletes the current head in its existing
rotation transaction or makes the exact generation/config comparison fail.

### 7. Existing-Network creation uses one fixed-revision CAS

C10 resolves `intent.network.existing_id` through one linearizable fixed
revision `R`. It first obtains the head and may issue a second read at exactly
`R` for the selected immutable row revealed by the head's snapshot id.

The exact read set is:

```text
local Agent singleton/current primary
current Agent config
current Network-observation head
selected Network-observation row
selected Zone record
Zone owner Project record
Zone owner Environment record
current owner Environment Blueprint head
current owner Environment Compose projection
owner Environment mutation epoch
owner Environment operation-lock absence
Zone deletion-tombstone absence
owner Environment deletion-tombstone absence
owner Project deletion-tombstone absence
```

The current Compose projection retains `plan_id` and `plan_hash` alongside its
Blueprint revision and render generation and contains the exact selected Zone
id and Compose Network name.

Admission requires all predicates at `R`:

- Agent phase is `ready`;
- Agent generation and config revision equal the head;
- the head, selected row, and their common lease exist, Controller time is
  before the frozen `expires_at`, and neither key has changed lease identity;
- the row state is `present`;
- head/row key-value bindings and row digest are valid;
- row snapshot, request, and authority identities equal the head;
- row authority revisions equal every current record `ModRevision`;
- Blueprint head revision id equals the projection Blueprint revision id;
- projection plan and render identity equal the row authority and actual
  facts;
- projection contains the exact Zone id and Compose Network name;
- current mutation-epoch raw bytes and `ModRevision` equal the row authority,
  and the owner operation-lock key remains version zero;
- Zone and owner records satisfy the backing hierarchy;
- all physical and label facts in section 4 match; and
- every mutation, deletion, and operation-lock fence is current or absent as
  specified.

The final `POST /backing-services` publication transaction compares every
present read-set key by exact `ModRevision`, including the head and selected
row, and every required-absent fence by version zero. Head lease expiry before
commit removes the head and makes the compare fail. Any changed observation,
topology, mutation epoch, lock, deletion fence, or projection returns
`409 state.conflict`.

`missing`, `collision`, and `invalid` are complete current evidence but are
never selectable. They immediately displace an older present head and return
`409 state.conflict` just like an absent or expired head.

The prepared create attempt does not re-read and substitute newer evidence
after a failed CAS. Retry is a new attempt against newly selected authority;
an idempotent replay returns only the exact already-published result.

Every operation capable of changing an owner Environment's physical Network
state advances its Environment mutation epoch and owns its Environment
operation lock. This includes Blueprint Network reconciliation, Zone removal,
and every Network create, remove, or replacement path. Without both fences,
existing-Network admission is invalid.

The host-wide Zone count is a strict durable singleton at
`/v1/indexes/zones/managed-count`, encoded as the closed
`managed-zone-count` schema-1 record with exactly `{count:0..512}`. Every
managed Zone primary publication and successful final removal compares and
updates it in the same transaction. Controller bootstrap linearly audits that
the counter equals the complete managed Zone primary count and that both are
at most 512. A mismatch is `internal`; a count above 512 fails bootstrap and
all Zone/C10 mutation admission closed.

Every path that would publish a new Zone, including C10 `network.create`,
compares the counter and rejects the prospective 513th Zone before Task or
resource publication with `409 state.conflict`. The observation request reads
that counter and exactly all counted Zone primaries at one authority revision;
there is no page, shard, truncation, or chosen subset.

The new backing Environment still carries its own explicit, globally
non-overlapping `network_pool`. When it selects another backing Project's
Network, that Zone subnet need not be inside the new Environment pool because
the new Environment does not own the Zone.

### 8. The parser has one discriminated scope and one pool grammar

The Environment-only parser scope is cleanly replaced by:

```go
type ScopeKind string

const (
	ScopeEnvironment ScopeKind = "environment"
	ScopeBacking     ScopeKind = "backing"
)

type Scope struct {
	Kind          ScopeKind
	EnvironmentID string
	Tenant        string
	Project       string
	Environment   string
}
```

The validation matrix is exact:

| Scope | Envelope and metadata |
| --- | --- |
| `environment` | `kind: environment`; explicit `schema: 1`; Tenant, Project, and Environment all required and exact |
| `backing` | `kind: backing`; explicit `schema: 1`; `metadata.tenant` key forbidden even when empty; Project required and equal to the create-intent slug; Environment exactly `main` |

`schema` must be present and exactly 1. Omission, zero, schema 2, unknown
values, fallback, translation, and negotiation are rejected.

Both envelopes use the normal canonical Compose body directly after root
metadata and root Groundplane extensions. There is no nested `spec` mapping.
The exact structural placement is:

```yaml
kind: environment
schema: 1
metadata:
  tenant: acme
  project: storefront
  environment: production
x-gp-network-pool: 10.200.0.0/24
services: {}
networks: {}
volumes: {}
```

```yaml
kind: backing
schema: 1
metadata:
  project: shared-postgres
  environment: main
x-gp-network-pool: 10.30.0.0/24
services:
  postgres:
    image: <Controller-derived immutable adapter image>
    x-gp-adapter:
      key: postgres:16
```

Keys not used by a document may be omitted under the canonical Compose
grammar. The examples establish placement, not defaults. ADR 0037's proposed
`spec: <canonical single-Service Blueprint spec>` wrapper is cleanly
superseded and rejected; no parser translation or compatibility shape exists.

The parsed extension model gains one root-only typed `x-gp-network-pool`. It
is required exactly once for both scopes, is a canonical IPv4 CIDR, and is
stripped before Compose loading. Includes, overrides, and extended files cannot
declare it. Parser validation proves canonical syntax; Controller IPAM
admission separately proves configured-parent containment and global
non-overlap.

After include, merge, profile, and interpolation resolution, a backing scope
requires exactly:

- one Compose Service total, and that Service enabled;
- zero authored Compose Networks;
- zero Service Network memberships;
- no Attach; and
- one `x-gp-adapter` on the sole Service.

Profiles cannot hide a second Service. A second Service in the fully resolved
Compose model fails even when every active profile would disable it.

Authored `x-gp-network` is forbidden in every root, include, override, and
extension source. Authored native `external: true`, an authored physical
Network `name`, `create_network`, and `join_network` are also forbidden. No
compatibility grammar survives. External membership is generated only after
resolving `x-gp-attachments` or C10's explicit `intent.network` union.

Authored and generated extension allowlists are distinct. Generated
`x-gp-network` and `x-gp-resource` may remain renderer output for validation
and execution metadata; their generated meaning does not make them legal
operator input.

### 9. `backing_network_id` is the sole facade identity

The exact successful Create response cleanly replaces ADR 0037's three-id
nested object by adding the Network id inside that same object:

```json
{
  "backing_service": {
    "project_id": "prj_01J...",
    "environment_id": "env_01J...",
    "service_id": "svc_01J...",
    "backing_network_id": "net_01J..."
  },
  "task_id": "task_01J..."
}
```

There is no response-level sibling `backing_network_id`. It is exactly the
sole adapter Service record's immutable
`BackingNetworkID`. It is never discovered from Docker and never selected by
an Attach.

ADR 0037's proposed `BackingServiceNetworkProjection` is cleanly replaced by:

```text
{
  owner_project_id: id,
  name: string,
  subnet: canonical CIDR,
  internal: boolean,
  selection: "created" | "existing"
}
```

Its nested `zone_id` is removed rather than retained as an alias. The
`BackingServiceListItem` and `BackingServiceDetail` each contain required
top-level `backing_network_id` immediately beside the three facade hierarchy
ids. The exact amended list response is:

```json
{
  "items": [
    {
      "project_id": "prj_01J...",
      "project_slug": "shared-postgres",
      "environment_id": "env_01J...",
      "environment_slug": "main",
      "service_id": "svc_01J...",
      "service_slug": "postgres",
      "backing_network_id": "net_01J...",
      "adapter": {
        "key": "postgres:16",
        "display_name": "PostgreSQL 16",
        "release_manifest_digest": "sha256:<64-lowercase-hex>",
        "repository_digest": "sha256:<64-lowercase-hex>",
        "runtime_major": 16
      },
      "network": {
        "owner_project_id": "prj_01J...",
        "name": "postgres",
        "subnet": "10.30.0.0/24",
        "internal": true,
        "selection": "created"
      },
      "desired": {
        "generation": 1,
        "manifest_digest": "sha256:<64-lowercase-hex>",
        "plan_hash": "sha256:<64-lowercase-hex>",
        "lifecycle_intent": "running",
        "replicas": 1,
        "restart": "recreate",
        "environment_names": [],
        "exposed_ports": [],
        "volume_mounts": []
      },
      "observed": {
        "applied_generation": 1,
        "replicas": 1,
        "health": "healthy",
        "observed_at": "2026-08-25T00:00:00Z",
        "fresh": true
      },
      "active_operation": null,
      "status": "ready"
    }
  ],
  "next_cursor": null,
  "snapshot_revision": 1
}
```

The exact amended detail object has the same top-level fields and the detailed
observed member:

```json
{
  "project_id": "prj_01J...",
  "project_slug": "shared-postgres",
  "environment_id": "env_01J...",
  "environment_slug": "main",
  "service_id": "svc_01J...",
  "service_slug": "postgres",
  "backing_network_id": "net_01J...",
  "adapter": {
    "key": "postgres:16",
    "display_name": "PostgreSQL 16",
    "release_manifest_digest": "sha256:<64-lowercase-hex>",
    "repository_digest": "sha256:<64-lowercase-hex>",
    "runtime_major": 16
  },
  "network": {
    "owner_project_id": "prj_01J...",
    "name": "postgres",
    "subnet": "10.30.0.0/24",
    "internal": true,
    "selection": "created"
  },
  "desired": {
    "generation": 1,
    "manifest_digest": "sha256:<64-lowercase-hex>",
    "plan_hash": "sha256:<64-lowercase-hex>",
    "lifecycle_intent": "running",
    "replicas": 1,
    "restart": "recreate",
    "environment_names": [],
    "exposed_ports": [],
    "volume_mounts": []
  },
  "observed": {
    "lifecycle_epoch": 1,
    "start_fence": "<immutable-start-fence>",
    "agent_id": "agt_01J...",
    "repository_digest": "sha256:<64-lowercase-hex>",
    "runtime_major": 16,
    "replicas": 1,
    "applied_generation": 1,
    "plan_hash": "sha256:<64-lowercase-hex>",
    "zone_id": "net_01J...",
    "zone_revision": 1,
    "subnet": "10.30.0.0/24",
    "physical_network_id": "<64-lowercase-hex>",
    "mounts": [],
    "health": "healthy",
    "health_detail": null,
    "observed_at": "2026-08-25T00:00:00Z",
    "fresh": true
  },
  "active_operation": null,
  "status": "ready"
}
```

The placeholders demonstrate the already-closed ADR 0037 field grammars and
do not introduce literal defaults. Nullable `observed` and `active_operation`,
all nested variants, ordering, status derivation, and list cursor semantics
remain exactly ADR 0037. Any observed `zone_id` is evidence and must equal the
top-level `backing_network_id`; it never becomes a second stable authority.

This is required data on existing Create, List, and Show capabilities, not a
new capability. The CLI renders it in `backing-service list` and
`backing-service show`; the Console store, list, and detail consume the same
field. The internal observation request has no Console action, CLI command, or
public API endpoint and is exempt from the 1:1 rule as Agent-channel traffic.

### 10. Errors fail closed without exposing private evidence

Existing-Network creation maps exact cases as follows:

| HTTP | Code | Case |
| --- | --- | --- |
| 404 | `zone.not_found` | the stable selected Zone primary is absent |
| 409 | `state.conflict` | no current head/selected row exists; the head or lease expired; the complete row is `missing`, `collision`, or `invalid`; a present row is stale, wrong-revision, or physically/label/IPAM drifted; a mutation, operation, deletion, or 512-Zone-cap fence is active |
| 422 | `validation.failed` | the Zone and owner records are structurally valid but owner kind is not `backing_project`, or the decoded Blueprint violates the closed backing grammar |
| 500 | `internal` | a row, head, staging, sequence, key binding, digest, lease binding, count, or backing Project/main Environment/Service hierarchy is malformed, dangling, contradictory, or corrupt |

Wire size, ordering, enum, digest, context, or unknown-field violations close
or reject the internal stream under ADR 0048's protocol error rules. They do
not become a public observation error. Public projections never expose Docker
Network ids, row/head keys, request or snapshot ids, process generation,
expected labels, actual labels, authority revisions, leases, or row digests.

## Authority and supersession

The scoped authority map is:

| Existing source or clause | ADR 0055 authority | Result |
| --- | --- | --- |
| MVP Backing Service hierarchy, Environment pool, stable Zone identity, and Attach relationship | preserved product authority | No second resource or topology is introduced. |
| MVP's otherwise dynamic Zone count on the single managed host | sections 1 and 7 | Narrowed for the MVP to an exact maximum of 512 managed Zones so one request/head is a complete replace-all inventory. |
| ADR 0037 existing-Network freshness and revision requirement | sections 1 through 7 | This ADR supplies the missing observation and admission mechanism and is a normative prerequisite for C10 existing-Network creation. |
| ADR 0037 nested `network.zone_id` | section 9 | Cleanly replaced by required top-level `backing_network_id`; no alias remains. |
| ADR 0037 backing `spec:` wrapper | section 8 | Cleanly replaced by the ordinary direct canonical Compose body used by both envelope kinds. |
| ADR 0048 final schema-1 channel, authentication, batching, queues, and unknown-field rules | sections 1, 2, and 5 | Extended at Controller tag 17 and `ObservedState` tags 5/6 plus `ObservedProject` tags 7..9; schema 2 and negotiation remain forbidden. |
| `docs/blueprint.md` authored external `x-gp-network`, `create_network`, `join_network`, `external: true`, and physical name | section 8 | Cleanly superseded as authored grammar; generated external membership remains renderer-only. |
| Environment-only root parser assumption | section 8 | Cleanly replaced by the two-value `ScopeKind`; no translation through `kind: environment`. |
| unconditional Tenant validation and `tenant-id` label emission | section 4 | Cleanly replaced by the Project-kind label matrix; backing ownership requires Tenant absence. |
| current API/CLI three-id facade response | section 9 | Extended with the required stable Network id on the same existing capabilities. |
| `docs/capabilities.md` C10/C21 state | delivery evidence below | Remains Scaffolded until synchronized implementation and acceptance pass; this Proposed ADR is not implementation evidence. |

ADR 0055 does not supersede ADR 0037's adapter, lifecycle, deletion, or Task
ownership clauses. The current `docs/mvp.md` wording that permits editable
backing images or data-destructive Destroy conflicts with ADR 0037's proposed
release-pinned image, runtime-only Destroy, and permanent Delete boundary. That
is a named C10 contract-alignment dependency, not a choice made silently here.

While ADR 0037, ADR 0048, and ADR 0055 remain Proposed, none independently
authorizes implementation or claims acceptance. Acceptance requires one
synchronized clean replacement across:

1. `docs/mvp.md`;
2. `docs/api-cli.md`;
3. `docs/blueprint.md`;
4. `docs/architecture.md`;
5. `docs/capabilities.md`;
6. source OpenAPI types and regenerated clients;
7. parser and renderer;
8. the Agent/Controller schema-1 wire;
9. observation persistence and C10 admission;
10. CLI; and
11. Console store, list, detail, and create flow.

Old fixtures, defaults, aliases, authored external grammar, unconditional
Tenant labels, and task-only/cloned-label observation paths are removed in the
same vertical. There is no mixed schema or compatibility period before the
first public release.

## Consequences

- Docker reports only Engine facts; the Controller binds them to exact durable
  authority and remains the sole admission decision-maker.
- A newly complete missing, collided, or drifted snapshot replaces prior good
  authority immediately. An expired or old head is never fallback.
- Complete publication is bounded: a 512-row snapshot uses 43..512 exact-size
  row transactions, each with at most thirteen compares and thirteen
  mutations; one six-operation head transaction follows. Immutable complete
  row envelopes are at most 65,536 bytes, and one original lease is shared by
  staging, rows, and head.
- The selected admission read compares only one row, while the staging chain
  proves the complete request set was durably published.
- Agent/config generation changes invalidate evidence without waiting for a
  stale clock supplied by the Agent.
- Invalid but complete current facts replace prior good authority immediately;
  representability failures no longer extend a stale-good window.
- The single-host MVP has a deliberately conservative 512-Zone ceiling; it is
  not a pagination shape or a claim about later multi-host capacity.
- Backing Project Networks have exact managed labels without a synthetic
  Tenant, while tenant Environment Networks retain their Tenant label.
- Both Blueprint scope kinds make the Environment pool an explicit operator
  decision. A joined external Network does not cause the Controller to derive
  or omit the new Environment's pool.
- Authored topology contains only owned native Networks and Attach intent;
  physical external membership is a generated render result.
- Backing Service operators receive one canonical stable Network identity
  through API, CLI, and Console without a public observation subsystem.
- The cost is one bounded internal wire request, three private record kinds,
  one leased-head lifecycle, Project-kind-aware rendering, and a fixed-revision
  C10 admission transaction.

## Rejected alternatives

### Put an etcd record revision in Docker labels

Docker does not know etcd revisions, and a transaction does not know its own
future commit revision before commit. A guessed or earlier value cannot prove
the current record.

### Let the Agent echo expected labels after comparing them

That destroys the evidence needed to diagnose and fence drift. The Agent must
return actual label values; the Controller compares them independently.

### Reuse a Task-scoped Compose observation

C10 selection is steady-state admission and may happen when no Task runs. A
Task artifact also does not bind the current Agent config, fixed revision,
Blueprint head, projection, mutation epoch, and deletion fences.

### Use the last known good observation when a new cycle is bad

That would authorize from stale reality. A complete missing/collision/drift
head replaces the good row, and incomplete traffic gains no head until the old
lease expires.

### Introduce Agent protocol schema 2

ADR 0048 cleanly defines schema 1 as the sole final MVP protocol. The new
messages use collision-free fields in that schema and add no negotiation.

### Expose an observation refresh endpoint and CLI command

Observation is periodic authenticated Agent traffic, not an operator
capability. A public action would violate the existing execution boundary and
add three surfaces that do not change product intent.

### Put the canonical Compose body below `spec`

That would create a second document-body grammar and require translation before
Compose parsing. Both scope kinds use the same direct root body.

### Count only profile-enabled backing Services

A disabled second Service remains authored desired state and could become
active under another profile. C10 is exactly one Service total, so profiles
cannot hide additional Services.

### Page or partition Network observation

One replace-all head cannot safely represent independently advancing pages.
The MVP instead closes the host-wide inventory at 512 managed Zones and sends
all of them in every authority.

### Preserve authored external Network syntax as a compatibility path

The product is pre-release. Keeping both explicit C10 Network intent and
authored `x-gp-network`/external Compose syntax creates two authorities for one
relationship. The old authored grammar is removed.

### Treat an empty Tenant label as absent

Label presence is meaningful. A backing Project has no Tenant; emitting an
empty label hides renderer mistakes and weakens the exact ownership set.

### Derive a backing Environment pool from the selected Network

The pool is an Environment reservation and explicit operator decision. An
external Zone is owned by another Environment and need not be contained by the
consumer's pool.

### Compare all 512 rows in the head transaction

That exceeds the etcd transaction budget. Immutable row batches and the
staging chain prove completeness; the head transaction needs only four exact
compares and two mutations.

## Acceptance evidence

Decision acceptance requires independent review of the complete contract.
Implementation acceptance additionally requires:

### Protocol

- authentication, Agent generation, process generation, and config-revision
  mismatch rejection;
- request-digest mismatch, unknown fields, enum zero, invalid ordering, and all
  size/count boundary tests;
- monotonic sequence, stream-session, one-outstanding-request, and old-worker
  overwrite rejection;
- batch gaps, duplicates, mixed request/snapshot, partial disconnect, and
  post-final rejection;
- one valid empty snapshot;
- proof that actual labels, not expected clones, cross the wire;
- duplicate Zone identity, duplicate physical name, collision-count, and
  Moby list/inspect/list race tests;
- every closed invalid reason, bounded raw actual-facts digest, and immediate
  invalid-head replacement; complete 0..4 IPAM configs, options, auxiliary
  addresses, range, derived network-plus-one gateway, empty-map normalization,
  extra-config, and malformed-IPAM evidence; `/0` and `/30` acceptance and
  `/31` and `/32` `422 validation.failed` rejection; and
- sole-writer scheduling and queue saturation tests proving observation does
  not borrow control or assignment capacity.

### Persistence

- crash after every row batch and before head publication;
- original-lease expiry before staging, during every batch, and before the
  final head, each with and without an exact prior head; proof of safe all-
  absent abandonment and that no timestamp or lease is refreshed;
- immutable-row collision and row/key/digest corruption rejection;
- exact zero-row chain and head publication;
- lease expiry during admission;
- Agent config and generation rotation;
- older-completed-head overwrite rejection;
- unknown final-transaction outcome replay;
- exact all/none/partial row-batch unknown-outcome classification; per-Agent
  staging serialization across reconnect; and
- lease-expiry cleanup with no permanently protected orphan.

### Admission

- missing, stale, wrong-revision, wrong-owner, collision, and expired evidence;
- forbidden Tenant label on a backing Network and missing/extra/changed managed
  labels;
- wrong Blueprint, plan, render generation, Compose names, subnet, internal,
  driver, scope, attachable, ingress, IPv6, and IPAM facts;
- active operation lock, mutation-epoch change, and every deletion tombstone;
- completed physical mutation after observation, proved by exact epoch raw
  bytes plus `ModRevision` and request-time lock absence; and
- exact head/row `ModRevision` compares in the final C10 publication.

### Parser and renderer

- both Scope variants, explicit schema 1, backing `main`, and forbidden
  backing Tenant key including an empty value;
- direct root Compose body with nested `spec` rejected;
- required canonical root-only IPv4 pool in both scopes;
- rejection in root, include, and override sources of authored
  `x-gp-network`, external/name, `create_network`, and `join_network`;
- backing exactly one total Service regardless of profiles, zero-Network,
  zero-membership, zero-Attach, one-adapter invariants; and
- exact 11-label backing and 12-label tenant Environment render fixtures.

### Public parity and host proof

- OpenAPI, generated Go and TypeScript clients, CLI, and Console require the
  same `backing_network_id` for Create, List, and Show; and
- Ubuntu 24.04 creates one backing-owned Network, selects it for a second
  Backing Service, then proves stale and drifted observations block
  publication; and
- bootstrap/count mismatch, 512-Zone complete request/head, and prospective
  513th-Zone rejection before Task publication.

No implementation or acceptance evidence is claimed by this Proposed ADR.
