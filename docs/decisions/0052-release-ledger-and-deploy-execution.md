# ADR 0052: Release ledger and deploy execution

- Status: Accepted
- Date: 2026-08-25

## Context

Groundplane needs one deploy and rollback contract for a single Service and for
an ordered Release Group. The contract must preserve these product properties:

- A Release is durable product history, not a mutable alias for the container
  that happens to be running now.
- The Release selected as the current successful release and the Release that
  is actually serving traffic are different facts. They can diverge after a
  post-serving failure when the failure policy is `leave_active`, or while
  compensation is incomplete.
- A retry must resume or adopt the same published candidate. It must not create
  a second Release or silently render from newer desired state.
- Rollback is a new operation and a new Release. It never reactivates or edits
  a historical Release.
- A Release Group is one ordered operation with one Task and one compensation
  policy, not a collection of independently dispatched child operations.
- Hooks must be integrated with the same checkpoints and compensation rules as
  the deploy that selected them.
- Every operator-facing capability must have exactly one Console action, CLI
  command, and REST API operation.

The current product documents name Release records and the blue-green and
recreate strategies, but they do not close the identity, publication,
checkpoint, retry, adoption, or pruning rules. They also use `active` in ways
that conflate successful history with observed serving identity.

This ADR is accepted implementation authority for the Release path. It does
not change the status of broader Proposed ADRs on which it depends; the exact
persistence and execution subsets adopted below are authoritative here.

## Dependencies

### ADR 0013

This ADR adopts the fixed-revision reads, stable record identifiers,
transaction limits, owner indexes, and durable-record conventions specified
here for Release implementation. ADR 0013 remains Proposed for its broader
scope; that status does not make the Release subset non-authoritative.

The inherited hard persistence bounds are:

- At most 96 aggregate compares plus mutations in the selected transaction
  branch.
- At most 1,048,576 serialized bytes in one transaction request.
- At most 262,144 serialized bytes in one durable record.
- List pages default to 50 items and accept at most 200 items.

### ADR 0040

This ADR depends on ADR 0040 for Script generation selection, runner execution,
hook ordering, hook budgets, hook references, and hook-specific retry safety.
ADR 0040 is Accepted. A deploy or rollback whose sealed selection contains one
or more hooks must fail validation until its runner contract is implemented. A
hook-free deploy or rollback does not depend on Script execution.

Under ADR 0040, automatic hooks are steps in the parent Deploy or Rollback Task.
They never create secondary Tasks. This ADR does not redefine Script execution.

## Decision

### 1. Canonical terms

This ADR uses these terms:

- **Release**: a durable, append-only ledger entry describing one published
  candidate for one Service.
- **Release operation**: one single-Service deploy or rollback, or one ordered
  Release Group deploy or rollback.
- **Attempt**: one Task executing a Release operation. A generic Task retry
  creates a new attempt for the same operation and the same Release ids.
- **Serving Release**: the Release whose workload and, when applicable, router
  target are last proved to be serving for a Service.
- **Current successful Release**: the most recent Release for the Service whose
  full deploy or rollback, including post hooks, completed successfully.
- **Candidate**: the Release being executed by the active Release operation.
- **Publication**: the atomic transition that makes a staged Release operation,
  its candidate Release ids, its Task, and its fences visible together.
- **Compensation**: the ordered attempt to restore the pre-operation serving
  Release after a post-serving failure under `switch_back`.
- **Recovery required**: an exclusive gated execution condition in which
  Groundplane cannot yet prove one safe serving identity or complete required
  compensation. Its latest Task is terminal, but its Release operation and
  fences remain active until recovery proves a terminal outcome.

`serving_release_id` and `current_successful_release_id` are canonical field
names. No field named `active_release_id`, `last_release_id`, or
`previous_release_id` may stand in for either fact.

### 2. Release identity and immutable ledger input

Each candidate receives a `dep_<ulid>` Release id before publication. The id is
unique within the Controller and is immutable across execution, adoption,
timeout, abort, compensation, and Task retry.

One Release belongs to exactly one Environment and one Service. Its published
intent is immutable and contains at least:

- `id`
- `environment_id`
- `service_id`
- `operation_id`
- `operation_kind`: `deploy` or `rollback`
- `group_operation_id`, when applicable
- `group_member_ordinal`, when applicable
- `candidate_workload`: one sealed value containing the selected
  `requested_reference`, exact host-local Docker `local_image_id`, and positive
  `replica_count`
- `tag`
- optional registry or manifest metadata, when available for display; it is not
  workload execution authority and never substitutes for `local_image_id`
- `strategy`: `blue-green` or `recreate`
- `slot`, when the strategy uses a slot
- `on_failure`: `switch_back` or `leave_active`
- `rollback_source_release_id`, for rollback
- `prior_serving_release_id`, when one existed at publication
- `prior_successful_release_id`, when one existed at publication
- `render_input_id`
- `render_input_digest`
- `created_at`
- requesting actor identity and workspace tuple
- originating Task id

The concrete domain value is `WorkloadSeal{RequestedReference, LocalImageID,
ReplicaCount}`. It is stored as `Intent.CandidateWorkload` and repeated exactly
as `ReleaseRenderInput.CandidateWorkload`; the render input additionally owns
optional `PriorWorkload *WorkloadSeal`. Public Release image provenance is the
candidate's requested reference. Registry or manifest metadata is never
required.

The Release id denotes the candidate, not the Task attempt. The originating
Task id remains an audit value even if that Task is later pruned. A retry records
its new Task id in attempt history; it does not change the originating Task id.

No field in the immutable intent may be overwritten. Execution progress is a
separate checkpoint record keyed by Release id. Historical success is never
changed to `superseded`; supersession is a derived relationship to the Service
projection, not a mutation of the Release ledger entry.

The old use of `active` and `superseded` as stored Release statuses is replaced.
On acceptance, mirrors must remove that status contract in the same change.

When an operation reaches `completed`, `failed`, `timed_out`, or `aborted`, the
Controller writes one immutable terminal Release execution summary. It contains
the terminal outcome, final serving checkpoint, effect digests, attempt
references, `rollback_material_digest`, and completion time. It never contains
a mutable availability status or retention timestamp.

Rollback-material retention is a separate mutable projection keyed by Release
id. It contains `status: available|expired`, the material-record references,
projection revision, and nullable `expired_at`. Updating this projection never
changes the immutable Release intent or terminal execution summary.
The terminal transaction writes the immutable execution summary and initializes
this projection as `available` in the same commit.

### 3. Release execution state

The checkpoint record has one of these states:

1. `pending`
2. `running`
3. `candidate_healthy`
4. `switching`
5. `serving`
6. `post_hooks`
7. `compensating`
8. `completed`
9. `failed`
10. `timed_out`
11. `aborted`
12. `recovery_required`
13. `recovering`

`completed`, `failed`, `timed_out`, and `aborted` are terminal. A terminal state
never changes. `recovery_required` retains Release-operation ownership and
blocks later deploys and rollbacks. `task.retry` moves it to `recovering`; a
conclusive recovery moves it to a terminal state, and an inconclusive recovery
moves it back to `recovery_required`.

A generic retry appends a new attempt and resumes the same operation. A failed,
timed-out, or aborted Task attempt is not itself the terminal state of the
Release operation while a safe retry or required recovery remains published.
The terminal Release state is written only when the operation closes with a
proved serving outcome.

Each effect checkpoint contains the plan id, step id, attempt id, Agent id,
acknowledgement id, observed Release label, observed slot, observed render
generation, effect digest, and acknowledgement time. Optional router effects
also contain the router configuration digest and observed target.

The Controller accepts only monotonic checkpoint transitions and performs each
transition together with the applicable projection or fence update in one
transaction. An Agent acknowledgement can be delivered more than once, but an
acknowledgement with different evidence for an already recorded checkpoint is
an integrity error and moves the operation to `recovery_required`.

### 4. Service release projection

Each Service has a release projection containing:

- `serving_release_id`, nullable before the first proved serving checkpoint
- `current_successful_release_id`, nullable before the first completed Release
- the corresponding Release projection revision
- the proved serving slot, when applicable
- the active Release operation id, when one owns the Service fence

The fields have independent update rules:

- `serving_release_id` changes only after acknowledged runtime and router
  evidence proves one Release is serving, or after equivalent compensation
  evidence proves restoration.
- `current_successful_release_id` changes only when the candidate reaches
  `completed`.
- A failed, timed-out, aborted, or recovery-required candidate is never assigned
  to `current_successful_release_id`.
- `leave_active` can leave `serving_release_id` pointing at a failed candidate
  while `current_successful_release_id` continues to point at the earlier
  completed Release.
- `switch_back` restores `serving_release_id` to the proved pre-operation
  Release before the candidate becomes terminal.

There is no Environment-wide current Release or previous Release field. An
Environment Releases page is a view across its Services' Release ledgers.

### 5. Revision-owned render input

Publication seals one immutable render input for each candidate Release. It is
owned by the Release operation and contains the exact fixed-revision inputs
needed to reproduce execution:

- workspace identity and hierarchy revisions
- Environment desired and applied projection revisions
- Service desired record and release projection revision
- resolved Compose project and Service identity
- candidate `WorkloadSeal`, optional prior `WorkloadSeal`, tag provenance,
  strategy, slot, and failure policy; optional registry or manifest metadata is
  display-only
- route and Caddy input revisions and digests
- Secret version references, never plaintext values
- selected Script generation references, when hooks apply
- Release Group snapshot and normalized order, when applicable
- the pre-operation serving and successful Release references
- renderer and plan schema versions

Rendered runtime bytes are derived exclusively from this input. A retry must
use the same render input id and digest even if desired state has changed. A
new desired-state deploy creates a new Release operation and new candidate ids.

The Agent labels every managed candidate container with at least the Release
id, plan id, slot, and render generation. Adoption without an exact label match
is forbidden.

Each render-input shard and group manifest must fit the 262,144-byte durable
record limit. The publisher splits group render input by member. It rejects the
operation before publication if any member cannot be represented within that
limit.

### 6. Fences and serialization

Release execution uses two fence scopes.

#### Per-Service fence

At most one published deploy or rollback operation may own a Service. The
Service fence records the operation id, candidate Release id, Task attempt id,
render input digest, and lease-independent ownership generation.

Deploy, rollback, retry, abort, timeout handling, and reconciliation compare
that generation before changing a runtime checkpoint or Service release
projection. An expired process lease does not clear durable ownership.

Different Services in one Environment may run independent single-Service
Release operations concurrently. They are still subject to Agent scheduling
limits.

#### Environment and group fence

A Release Group operation owns one Environment group fence and the per-Service
fences for every normalized member. Publication acquires the group fence and
the entire member set atomically. It never acquires them incrementally.

The member set is stored as one bounded fence generation rather than one
transaction mutation per member. A single-Service publisher compares the same
fence generation and is rejected when its Service is named by an active group
owner. A group publisher is rejected when any member is owned by a
single-Service operation or another group.

Every publication and every serving or terminal projection update also
compares and rewrites the Environment coordination record. This is the
fixed-revision mutation fence for hierarchy, deletion, desired state, and
applied state. A single-Service Release operation does not hold the unrelated
Environment-wide operation lock for its entire execution. The Environment
group fence is held for the full Release Group execution because the group
snapshot and compensation boundary cover all members.

Lock acquisition order is Environment id, then Service ids in ascending byte
order. The normalized group execution order does not alter lock order.

### 7. Atomic publication

Candidate construction may stage immutable records before publication so that
a group of 32 members can remain within the transaction ceiling. Staged data
is not product state and is invisible to Release GET and list operations.

Every staged Release contains a publication id and digest. Public visibility
requires the matching immutable publication marker. A direct read of a staged
Release without that marker returns not found.

The publisher performs this sequence:

1. Read Environment hierarchy, deletion state, desired records, applied
   projections, Service release projections, group definition, and Script
   references and applied dependency phase plans at one fixed revision. Hook
   selection follows accepted ADR 0040 at that same revision.
2. Normalize and validate the full operation.
3. Allocate the operation id, all candidate Release ids, render input ids,
   publication id, plan id, step ids, and initial Task id.
4. Write immutable Release, render-input, and group-manifest staging records in
   bounded batches. A staging transaction contains at most 16 records and must
   also satisfy the inherited byte and operation ceilings.
5. Execute one publication transaction that compares the fixed-revision
   Environment coordination record, deletion state, relevant desired-state
   revision, current release-fence generation, group revision when applicable,
   idempotency state, and staged manifest digest.
6. In the selected transaction branch, create the publication marker, Release
   owner-index entries, release-fence ownership, Release operation head, Task,
   executor queue entry, workspace and Environment Task indexes, active
   operation index, idempotency marker, and the rewritten Environment
   coordination record.

Publication creates exactly two bounded owner indexes per Release:

- `/v1/indexes/releases/by-environment/<environment-id>/<release-id>` maps to
  the Service id and publication id. An Environment range is ordered by stable
  Release id rather than by Service id.
- `/v1/indexes/releases/by-service/<environment-id>/<service-id>/<release-id>`
  maps to the publication id. A Service range is bounded to one Service and is
  ordered by stable Release id.

Both indexes use the same fixed-revision cursor fields: index revision, last
Release id, direction, and filter digest. The default direction is ascending.
Rollback selection uses descending traversal at that same fixed revision.

For a group with `N` members, where `2 <= N <= 32`, the final publication
transaction has exactly `2N + 11` mutations:

1. `N` Environment Release index entries.
2. `N` Service Release index entries.
3. One publication marker.
4. One Release fence-generation rewrite.
5. One Release operation head.
6. One Task primary.
7. One executor queue entry.
8. One workspace Task index.
9. One Environment Task index.
10. One active-operation index.
11. One idempotency marker.
12. One Environment coordination rewrite.
13. One Task operation-history index, contributed only by the closed Task
    publication fragment.

The same transaction has exactly 11 compares:

1. Environment coordination mod revision.
2. Environment deletion-tombstone absence.
3. Project deletion-tombstone absence.
4. Tenant deletion-tombstone absence.
5. Release Group desired-record mod revision, or Service desired-record mod
   revision for a single-Service operation.
6. Current Release fence-generation mod revision or version zero.
7. Idempotency-marker expected version or mod revision.
8. Staged manifest mod revision.
9. Publication-marker version zero.
10. Release-operation-head version zero.
11. Task-primary version zero.

Allocated ids and immutable staging make per-index absence compares redundant.
Staging has already compared every Release primary with version zero, Release
ids are never reused, and indexes have no independent writer.

At `N = 32`, publication uses 75 mutations and 11 compares, or 86 aggregate
operations. It leaves 10 operations below ADR 0013's ceiling of 96. A
single-Service publication uses 13 mutations and 11 compares, or 24 aggregate
operations. Staging 16 immutable records uses 16 version-zero compares and 16
puts, or 32 aggregate operations.

The publication transaction must contain at most 96 aggregate compares plus
mutations and at most 1,048,576 serialized bytes. The publisher computes both
counts before staging. A request whose final publication would exceed either
bound is rejected without visible product state.

The group member limit of 32, the two exact owner indexes, the bounded fence
generation, and staged immutable inputs are part of the transaction design. An
implementation may not replace them with 32 child publications or add a third
per-Release index to the publication transaction.

Staged records without a publication marker may be garbage-collected after 24
hours. Garbage collection must compare marker absence and staging digest. A
published record is never removed by staging garbage collection.

Idempotency follows ADR 0021. A duplicate request returns the original Task and
candidate Release identities. It never allocates a replacement candidate.

### 8. Single-Service deploy

A deploy selects:

- the requested tag, or the proved serving Release's tag when omitted; before
  the first Release it uses the Service's authored tag
- the requested strategy, or the Service's declared strategy when omitted
- the requested `on_failure`, or `switch_back` when omitted

Only `blue-green` and `recreate` are valid. `rolling` is rejected before
publication.

For blue-green, publication selects the inactive slot relative to the proved
serving projection. The slot is immutable in the candidate Release. For a
Service with no proved serving Release, the first slot is selected by the
Controller's canonical slot ordering and sealed in the render input. A
blue-green Service must expose at least one addressable internal TCP port;
publication rejects blue-green for a portless worker or scheduler.

Native Compose omission of `deploy.replicas` means one and is normalized to the
explicit value `1` exactly once at the Blueprint or direct-input boundary.
Every durable Release thereafter contains one positive explicit replica count;
an absent or zero count in a durable or historical Release is invalid and is
never defaulted or reconstructed from a current projection. Thus the count is
mandatory in the sealed Release, not mandatory in authored Compose YAML.

Before publishing a newly authored candidate Release, the Controller sends one
bounded correlated read-only request over the existing authenticated Agent
channel. The request selects the new `requested_reference`; the Agent performs
Docker `ImageInspect`, returns the exact `sha256:<64-hex>` local Docker image
id, and the Controller seals the resulting
`{requested_reference, local_image_id, replica_count}` as the candidate's one
`WorkloadSeal` value. The all-or-nothing request contains at most 64 unique
selectors. It is ephemeral machine coordination, not an operator capability,
Task, durable preview, or new generic host-execution surface. The Controller
does not read workload Docker state itself.

One publication uses one request for its complete unique candidate-and-prior
selector set; it is never chunked into multiple exchanges. A Blueprint whose
complete set exceeds 64 returns `validation.failed` with HTTP 422 before any
desired-revision, Release-ledger, or private-source staging, idempotency
publication, Task publication, or host effect.

The protobuf contract defines one `ResolveWorkloadImages` request and one
`WorkloadImageResolutionResult` response in the existing stream envelopes
(Controller field 12 and Agent field 11). Both stream dispatchers implement the
exchange; Release prepublication integration remains required before deployment
can depend on it.
The request carries `request_id` plus `selectors[]`. Each selector
is a closed union of exactly one `requested_reference` for a new candidate or
one `local_image_id` for historical, prior, retry, and pre-mutation validation.
The result repeats `request_id` and is a closed union of either ordered
`resolutions[]` or one failure. A resolution echoes its typed selector and
returns `local_image_id`. A failure carries a required zero-based
`selector_ordinal` in `0..N-1` and exactly one of `not_found`,
`identity_mismatch`, or `observation_failed`; it carries no partial successes.
The protobuf ordinal is an optional scalar whose presence is mandatory,
so ordinal zero is distinguishable from an omitted field.

After authentication, each connection reads exactly 16 bytes from the operating
system CSPRNG once and interprets them as one unsigned big-endian 128-bit seed.
An entropy failure or all-zero seed disables only workload-image resolution for
that connection; it does not reject or tear down the authenticated stream, fence
Task dispatch, or affect active Task traffic.

The first `request_id` is the seed encoded as exactly 32 lowercase hexadecimal
ASCII characters. Under the Agent-session Registry mutex, each later allocation
increments that 128-bit value and emits the same fixed-width encoding. The
maximum value is emitted once, after which allocation is exhausted and fails
closed; the counter never wraps to zero or reuses a value. The connection keeps
only its next counter value, exhaustion state, and one active resolution, not a
request-id history map. The id is correlation, not authentication. Correlation
also requires the exact authenticated session state and Agent generation fence:
a late result, a result from another connection, or a result for a request that
is no longer active is discarded and cannot satisfy a newer request. A
malformed result for the active request rejects the whole batch.

One connection permits exactly one active workload-image resolution. A second
new publication preflight while it is active returns HTTP 409
`workload.image_resolution_busy` before staging or publication and is never
retried automatically; the operator may explicitly retry after the active
exchange completes. An already accepted idempotency replay returns its exact
stored response without acquiring the resolution slot or resolving an image,
so only a new publication preflight competes. A connection whose seed is
unavailable or whose counter is exhausted returns HTTP 503
`workload.image_resolution_unavailable` before staging or publication and keeps
the authenticated stream and Task traffic available.

A request contains 1 through 64 unique typed selectors and the complete encoded
protobuf envelope is at most 65,536 bytes. A `requested_reference` is at most
512 ASCII bytes and is an explicitly tagged or digest-qualified valid Docker
named reference. A `local_image_id` is exactly `sha256:` followed by 64
lowercase hexadecimal characters. Both peers enforce the correlation-id shape,
selector-kind, string, uniqueness, count, and envelope bounds before the Agent
calls Docker. Before consuming any resolution, the Controller enforces result
correlation, envelope, outcome, cardinality, order, selector echo, local-id, and
failure-ordinal bounds. A successful response contains exactly one validated
resolution per selector in request order. Requested-reference resolution
returns Docker's exact local id; local-id validation requires
`ImageInspect(local_image_id)` to return that identical id.

The Controller and Agent each cap the complete resolution exchange at 30
seconds. Timeout, disconnect, or an invalid response yields no usable
resolution and prevents staging and publication; it never triggers automatic
tag re-resolution. The exchange remains ephemeral and read-only and creates no
operator API, durable request record, Task, or generic Docker capability.
The exchange alone does not prove Release prepublication integration.

A rollback candidate copies its complete `WorkloadSeal` from the selected
historical Release input. A prior workload used for topology transition or
compensation copies the complete seal from the exact currently serving Release
input. Retry and recovery reuse those same values. They never resolve a
historical `requested_reference` again, derive a missing count or image id from
current desired state or a projection, or replace selected historical bytes
with the image now named by a mutable tag. For historical, prior, retry, and
pre-mutation checks, the Agent request selects the sealed `local_image_id`
directly. Retagging therefore preserves an available historical image for exact
rollback, while a missing sealed id fails before any workload mutation.

The immutable render input contains the candidate `WorkloadSeal` and an
optional prior `WorkloadSeal`; it does not duplicate their members as separate
image, prior-image, replica, or workload-manifest-digest authority. Compose and
execution select the sealed local Docker id, while the requested reference
remains provenance. Resolution and validation do not depend on `RepoDigests`,
so locally built images with no repository digest remain eligible. Deploy and
rollback never build, pull, or push a workload image. Release Script execution
and applied-image evidence consume the sealed local Docker id and must not
fabricate a repository or manifest digest for an image that has none.

For recreate, the durable Release `slot` field is absent and the renderer
creates the exact captured logical workload set of N replicas. An addressable
Service may retain its stable proxy, but that proxy targets the logical set and
no blue/green workload pair exists. A portless Service has the same exact-count
topology without a proxy. The immutable render input seals separate candidate
and prior topology artifacts, including both replica counts. Execution stops
and removes the prior workload set before applying the candidate set without
starting dependencies, then proves the exact replacement set healthy. It never
creates an unsealed physical workload.

Each addressable Service released with blue-green owns one stable
Controller-rendered Caddy proxy and two physical Compose slot workloads.
Internal consumers and routed Caddy target the stable proxy, never a mutable
slot alias. The immutable render input seals both slot identities, the prior
and candidate proxy generations, the exact canonical proxy JSON and digest,
and the prior and candidate Release ids.
The managed proxy's compiled OCI index reference, Agent-selected platform child
manifest, and host-local Docker image/config id remain three distinct identities.
Their preparation and historical-retention contract is separate from
`WorkloadSeal`; current catalog authority never substitutes for a sealed
historical proxy identity.

### 9. Blue-green checkpoints

A blue-green attempt executes one closed five-step procedure per group member:

1. Apply only the candidate's sealed inactive slot.
2. Observe the exact slot labels and pass the sealed health check.
3. Atomically reload the stable proxy from the sealed candidate JSON and prove
   its digest, generation, Release id, and upstream slot.
4. Retain a recover-only probe that accepts only the sealed prior or candidate
   proxy state.
5. Retain an enabled compensation step only for `switch_back`; it atomically
   reloads the exact prior proxy JSON, proves it, then removes only the exact
   candidate slot.

Only the first three steps run during the forward attempt. A group executes
members serially in declared order, and member `N` apply depends on member
`N-1`'s proved serving checkpoint: proxy switch for blue-green or exact-set
replacement acknowledgement for recreate. The Controller records `serving` and updates
`serving_release_id` only from matching proxy evidence, then records
`completed`, updates `current_successful_release_id`, and releases fences.

An acknowledgement loss after the host effect but before the durable
checkpoint is handled by adoption. It is not handled by blindly replaying the
switch.

### 10. Recreate checkpoints

Every recreate Release uses one closed exact-set procedure: remove the exact
sealed prior workload topology, force-apply the sealed N-replica candidate
without dependencies, acknowledge the complete set's exact Release labels,
cardinality, and health, retain a recover-only probe that accepts only the
candidate or prior artifact and captured replica count, and retain an enabled
compensation step only for `switch_back`. An addressable recreate retains only
the stable logical proxy; a portless recreate has no proxy. Compensation
force-applies and health-proves the exact prior artifact and captured count.

Strategy changes are sealed topology transitions. Blue-green to recreate
removes the prior slot pair before applying the exact captured recreate set.
Recreate to blue-green creates and health-proves the candidate slot, switches
the stable proxy, and only then removes the obsolete recreate set. Any
transition to or from blue-green is rejected before mutation unless both the
prior/current serving Release's seal and candidate/destination Release's seal
captured `replica_count == 1`. Replaying recovery probes or compensation after
interruption
is idempotent because each step accepts only the immutable candidate and prior
artifacts, replica counts, and exact workload targets.

### 11. Failure policy and compensation

A failure, timeout, or accepted abort before a serving checkpoint leaves both
Service projection ids unchanged. Candidate cleanup may remove only runtime
objects whose exact Release, plan, slot, and render labels match the candidate.

After a serving checkpoint:

- `leave_active` keeps the candidate serving. The operation becomes terminal
  with its actual failure state, `serving_release_id` remains the candidate,
  and `current_successful_release_id` remains unchanged.
- `switch_back` records `compensating`, restores the exact pre-operation
  serving Release and sealed router state, proves those effects, atomically
  restores `serving_release_id`, then records the candidate's terminal failure
  state and releases fences.

Compensation does not create a Release. It restores the prior proved serving
state as part of closing the failed candidate operation.

If `switch_back` has no `prior_serving_release_id`, its sealed pre-operation
serving state is absence. An immediate failure path that cannot yet prove safe
removal records `recovery_required` and leaves the candidate as the last proved
serving identity. Recovery restores absence by removing exact operation-owned
runtime and route targets and then clearing `serving_release_id`. If a non-null
prior workload or router target cannot be restored and proved, the operation
likewise becomes `recovery_required`. The Controller does not silently
reinterpret `switch_back` as `leave_active`.

Hooks use accepted ADR 0040's typed `RunScript` execution authority. No generic
hook step or fail-open placeholder enters the plan.

### 12. Rollback selection and execution

Rollback selection is performed at the publication fixed revision.

Rollback first requires a non-null `serving_release_id` and resolves that
Release's tag at the same fixed revision. Every implicit and explicit target
must satisfy the same common eligibility predicate:

- belongs to the same Environment and Service,
- is `completed`,
- reached a recorded serving checkpoint,
- has an id different from the current `serving_release_id`,
- has a tag different from the tag of the current `serving_release_id`, and
- has a rollback-material retention projection whose status is `available` and
  whose referenced versions reproduce the immutable material digest.

Eligible Releases are ordered by Release id descending. Because Release ids
embed their allocation ULID, this is the canonical newest-first order; no Task
timestamp participates in selection.

Without an explicit tag, the first Release satisfying the common predicate is
selected.

Successful redeploys of the same tag therefore do not advance the implicit
rollback point.

With an explicit tag, the Controller adds one filter, exact tag equality, to
the common predicate. It still excludes the current serving Release id, the
current serving tag, non-completed Releases, never-served Releases, and Releases
whose rollback material is unavailable. An explicit tag is not an alternate
spelling for deploying an arbitrary image tag.

The immutable terminal Release execution summary records only
`rollback_material_digest`. The separate retention projection records
`status`, material references, and nullable `expired_at`. Pruning or expiry
changes that projection through one compared transition; selection never
discovers availability by partially reading already-pruned detail and never
mutates an immutable Release fact.

If no structurally eligible historical Release exists, selection returns
`409 rollback.no_previous_release`. If structurally eligible history exists but
every otherwise matching candidate has expired or pruned rollback material,
selection returns `409 rollback.source_expired`. With an explicit tag, this
distinction is evaluated only among historical Releases with that tag and an
unavailable source never falls through to another tag. Without an explicit
tag, descending selection may continue to the next older available Release
that satisfies the common predicate.

Selection records the source Release id in `rollback_source_release_id`. The
Controller creates a new candidate Release id and copies the source's complete
candidate `WorkloadSeal`, tag provenance, strategy, and reproducible render
inputs into a new revision-owned render input. It validates the copied sealed
local id directly and never resolves the historical requested reference. The
strategy is the source Release's original strategy, not the Service's current
default. Rollback never runs migrations.

The new Release executes the normal checkpoints and failure policy. On success,
both Service projection ids point to the new Release, not to the historical
source Release.

Both conflict outcomes occur before candidate allocation or publication.

### 13. Retry, adoption, abort, and timeout

#### Retry

A retry is allowed only when:

- the Release operation is still published and non-terminal,
- the required fences still name that Release operation and no other operation
  owns a member,
- the same candidate Release ids and render-input digests are available,
- the latest Task attempt is terminal as failed, timed out, or aborted, and
- the Release operation is retryable or is in `recovery_required`.

Retry creates one new Task and attempt id. In one transaction it transfers the
fence's attempt ownership, appends attempt history, creates the Task and queue
indexes, and records idempotency. It uses the original operation id, Release
ids, plan, selected slots, order, failure policy, and render inputs.

The MVP retry planner uses `recover_only`: it probes the stable proxy against
the sealed prior and candidate configurations and closes through the sealed
failure policy. It never reruns forward slot apply, health, or switch effects,
and it never advances a candidate through a new serving effect.

Retry never repeats a proved irreversible effect and never creates another
candidate.

#### Adoption

Reconciliation may adopt an effect only when exactly one observed runtime
object matches the sealed Service, Release id, plan id, slot, and render
generation. A routed serving checkpoint additionally requires the exact sealed
router digest and target.

No match permits a safe re-execution only for an effect documented as
repeatable by the plan. More than one match, conflicting labels, an unexpected
router target, or evidence from a different render generation moves the
operation to `recovery_required`.

Adoption writes the same durable checkpoint that an in-flight acknowledgement
would have written. It does not invent a synthetic success.

#### Recovery-required resolution

The existing universal `task.retry` action is sufficient for recovery. No
Release-specific resolve action is added. The caller retries the latest
terminal Task for the gated Release operation.

The retry publication transaction must compare all of these facts and update
them atomically:

- the latest Task is the operation's retryable terminal attempt,
- the operation head is `recovery_required`,
- the Environment coordination revision is unchanged,
- every Service and Environment/group fence still names the operation and its
  ownership generation,
- the immutable manifest, candidate Releases, render inputs, and required
  rollback material remain available, and
- the idempotency key is new or is an exact replay.

The transaction creates one recovery Task, transfers only the attempt id within
the existing fence owner, records `recovering`, and queues the sealed probe-first
plan. It does not clear or reacquire a fence.

Before any replay or compensation effect, the Agent reports one fixed probe for
every affected Service. The probe contains:

- the complete Compose-project container inventory for that Service,
- Release id, plan id, slot, render generation, health, and runtime id for each
  matching container,
- logical Service alias target and ownership labels,
- enabled router configuration digest and target, or proved route absence,
- operation-owned hook-runner and temporary-container inventory, and
- operation-owned temporary network, alias, and router artifacts.

The Controller accepts the probe only from the fenced Agent and attempt and
only when its inventory generation is internally consistent. For a Release
Group, all member probes belong to one Agent inventory generation and are
evaluated before any member recovery effect.

The Controller maps the probe to exactly one of these outcomes:

1. If it proves the last durable checkpoint, or one later exact checkpoint,
   and all remaining steps are replay-safe, adopt that checkpoint and continue
   in `resume` mode. Successful completion records `completed` and both Service
   projection ids point at the candidate.
2. If forward replay is unsafe or the attempt's closing outcome is already
   fixed, enter `recover_only`. For `leave_active`, prove the candidate is the
   sole serving target, preserve it as `serving_release_id`, leave
   `current_successful_release_id` unchanged, and close with the stored
   `failed`, `timed_out`, or `aborted` outcome.
3. For `switch_back`, restore and prove the sealed prior serving Release and
   route state. A null prior serving id means restore proved absence and clear
   `serving_release_id`. Close with the stored `failed`, `timed_out`, or
   `aborted` outcome.
4. If no serving effect occurred, preserve the pre-operation projections,
   remove exact candidate-owned runtime, and close with the stored failure
   outcome.
5. If the probe is missing, internally inconsistent, has multiple possible
   serving targets, contains an unknown render generation, or compensation
   cannot be proved, fail the recovery Task with
   `release.recovery_incomplete` and return the operation to
   `recovery_required` without changing ownership.

For a group, `resume` continues at the first incomplete member. `recover_only`
uses the original switched set, compensates it in reverse normalized order for
`switch_back`, and proves every member's final state before clearing any group
fence. Independently safe compensation continues after a member becomes
ambiguous, but one ambiguous member keeps the group gated.

Before terminal gate clearing, a final probe must prove all of these cleanup
invariants:

- exactly one expected serving runtime and route target exists when
  `serving_release_id` is non-null, or neither exists when it is null,
- a blue-green inactive slot exists only when the sealed final plan retains the
  exact prior Release as standby,
- recreate has no second managed Service runtime,
- every losing candidate runtime, runner, temporary container, temporary
  network, alias, and router target owned by the operation is absent, and
- no object with the operation's Release, plan, or render labels is outside the
  sealed final inventory.

The Controller removes only objects carrying the exact operation labels.
Unknown or conflicting runtime is never guessed away and keeps the gate in
`recovery_required`; the operator repairs that host state and retries the same
Task capability again.

One terminal transaction compares the probe generation, fence generation, and
Environment coordination revision; writes final checkpoints and Service
projections; records the terminal Release and Task outcomes; clears the active
operation and fence owner; and rewrites Environment coordination. A later
`task.retry` is rejected with `409 state.conflict`. A new deploy or rollback is
allowed only after this gate-clearing transaction succeeds.

#### Abort

Abort preserves Accepted ADR 0041 exactly. It targets the existing Task id and
never creates another Task or Release operation. `POST /v1/tasks/{task_id}/abort`
has no body and returns exactly `202 {"task_id":"<target Task id>"}` only after
the target Task's durable terminal transaction commits.

A pending Task is atomically terminalized as `aborted` before assignment. A
running Agent Task receives `TaskAbort{task_id, reason:"operator_requested"}`
for its exact durable Agent generation; the HTTP request subscribes before
delivery and waits for the acknowledgement transaction. A running native
Controller Task cancels its exact execution context and likewise waits for the
terminal transaction. Abort never clears a fence as a process-local side
effect.

If the candidate has reached serving, abort applies the selected failure
policy: `switch_back` compensates, and `leave_active` preserves the candidate
as serving. Resource-specific checkpoint, projection, compensation, and fence
updates are part of the target Task's terminal acknowledgement transaction. If
runtime remains ambiguous, that transaction terminalizes the Task as `aborted`
while leaving the Release operation and fences in `recovery_required`.

An already `aborted` Task is an idempotent success returning the same `202`
response. Targets in `completed`, `failed`, or `timed_out` return
`409 task.not_abortable`; natural completion racing abort uses the same error.
The target Task is the abort idempotency scope.

#### Timeout

The Controller configuration key is
`controller.release_execution_timeout`. It is a Go-duration string with:

- Controller-process scope,
- default `15h`,
- minimum `40m`, and
- maximum `24h`.

Invalid syntax or a value outside the inclusive range fails Controller startup.
A configuration change affects only newly published Release operations. The
initial publication seals the duration for every Task attempt of that operation.
There is no API, CLI, Console, Environment, Service, group, or edition-specific
override in the MVP.

Publication computes the required attempt budget exactly:

`20m + (10m * member_count) + (10m * compensatable_member_count) + selected_hook_timeouts`

`member_count` is 1 for a single-Service operation and is the normalized group
size for a Release Group. `compensatable_member_count` equals `member_count`
under `switch_back` and is zero under `leave_active`. The first `20m` is the
Controller, Agent acknowledgement, cancellation, probe, and reconciliation
reserve. Every member's non-hook forward subplan has a hard aggregate cap of
`10m`; every member's compensation subplan has a hard aggregate cap of `10m`.
A health or effect timeout that cannot fit its subplan is rejected at
publication.

Under ADR 0040, at most 16 selected hook executions each contribute at most
`900s`, so hook timeouts contribute at most `4h`. The maximum valid MVP plan is
therefore:

`20m + (10m * 32) + (10m * 32) + (16 * 900s) = 900m = 15h`

The default exactly covers the maximum 32-member, 16-hook failure and full
compensation path. A single-Service hook-free `switch_back` plan requires
`40m`. A single-Service hook-free `leave_active` plan computes to `30m`, while
the Controller configuration minimum remains `40m`. Publication returns
`422 release.deadline_too_short` when the configured duration is below the
computed budget. A value above the budget increases only the enclosing attempt
deadline; it does not increase member or hook step caps.

The configured duration and computed budget are sealed in the Release operation
and copied to every Task attempt. A retry, including a recovery retry, receives
a new absolute deadline using that original sealed duration. Timeout follows
the same cancellation, reconciliation, and failure-policy path as abort. It
never authorizes fence clearing or an assumption that an Agent effect did not
occur.

### 14. Release Groups

The desired-state grammar is the implemented plural root extension
`x-gp-release-groups`. The singular `x-gp-release-group` is invalid and has no
compatibility alias.

Each group has:

- a canonical name
- 2 through 32 unique Service members in the same enabled Compose project
- a normalized execution order that is an exact permutation of the members
- optional default `tag`
- `on_failure`: `switch_back` or `leave_active`, defaulting to `switch_back`

When order is omitted, member declaration order is the normalized execution
order. Group names retain the Blueprint rule: valid UTF-8, nonblank,
non-whitespace-only, and no control characters. This ADR does not introduce a
narrower slug grammar for the Blueprint map key.

The normalized execution order remains exact and serial when the selected
Blueprint revision contains deploy- or rollback-phase Service dependencies.
Publication rejects the operation with a typed deterministic contract error
when that order places a dependent member before one of its selected-member
prerequisites. It never silently reorders the group. A dependency outside the
selected group is sealed as phase-specific prerequisite work in this same
Release plan; it never creates a second Release path or a child Task.

A group deploy accepts an optional tag. Omission uses the group's desired
default tag; if both are absent, publication is rejected. Every member receives
the same selected tag and its own strategy selected from that Service's fixed-
revision desired state. Each member pins its exact immutable host-local Docker
image id through the single all-or-nothing Agent resolution batch before
publication and retains independent Release history. Registry or manifest
metadata is optional and never substitutes for that local id. A group rollback
copies each selected source's complete historical `WorkloadSeal`; it never
resolves the selected tag to current bytes. The persisted group default applies
only to deploy.

A group rollback accepts an optional request tag. When supplied, each member
independently selects its newest eligible historical Release that completed
successfully and reached serving with that exact tag and a tag different from its
current serving Release. When omitted, each member independently selects its
newest such eligible Release whose tag differs from its current serving
Release, using the single-Service rollback rule. The persisted group default is
not a rollback fallback. Failure to select an eligible source for any member
rejects the entire group before publication.

Release Group rollback source resolution has one authority:
`ReleaseLedger.SelectRollback`. The read-only rollback-preview operation calls
that selector for all 2 through 32 members at one fixed revision and returns the
selected Service, Release, and tag in exact normalized order. A missing,
expired, ineligible, or wrong-member result rejects the whole preview. Preview
does not create a Task, mutation, idempotency claim, durable record, or
publication digest.

Rollback may carry the preview's fixed revision. When it does, the Controller
reselects every source at that revision and compares the current Release Group
desired record, Environment mutation epoch, and the Release publication fences
from section 7. Compaction, stale evidence, or changed authority returns
`state.conflict` before publication. Writes outside those authorities do not
stale the preview. Omission selects at the current revision. The request never
accepts client-selected Release ids, an alternate eligibility predicate, a
compatibility alias, or a persisted preview.

This preview-and-confirmation workflow changes no desired Blueprint grammar;
`x-gp-release-groups` remains the sole desired-state extension.

One group operation allocates one candidate Release per member and publishes:

- one ordered immutable group manifest,
- one Task and one attempt,
- one Environment group fence,
- one bounded per-Service fence generation covering every member, and
- all candidate Release ids atomically.

There are no child Tasks. The parent Task exposes member and checkpoint progress
in normalized order.

Members execute serially in normalized order. Each member must complete its
serving checkpoint and post hooks before the next member begins. A member that
fails before serving is not added to the switched set. A member that reaches
serving is added to the switched set even when its post hook later fails.

On the first member failure, timeout, or accepted abort:

- `leave_active` stops forward execution and keeps every already-switched
  member at its final proved serving Release.
- `switch_back` compensates the switched set in reverse normalized order,
  restoring each member's pre-operation serving Release and router state.

Unstarted members remain unchanged. Every member candidate records its actual
terminal outcome. If any required compensation cannot be proved, the group and
affected member become `recovery_required`; compensation continues for other
members when their restoration remains independently provable.

Fences are released together only after all members are terminal and every
required compensation result has been recorded. The Task terminal result
summarizes all member outcomes rather than reporting only the first failure.

The limit of 32 is a hard MVP validation bound. It is not an edition or
licensing limit.

### 15. Hooks

Once ADR 0040 is Accepted, hook selection is sealed at publication from the
same fixed revision as the Release render input. The inherited limits are at
most 16 selected hooks, at most 1,048,576 aggregate selected Script body bytes,
and at most 4,194,304 bytes in the sealed execution plan.

For a group, primary ordering is normalized group member order and secondary
ordering is Script slug byte order, as defined by ADR 0040. Hook results are
member checkpoints in the one parent Task. Every selected Script executes once
per selected logical Service Release, never once per replica.

Pre hooks complete before serving-runtime mutation. Post hooks run after the
member serving checkpoint. On-failure hooks run after compensation determines
the final known serving identity. These boundaries are the same for a
single-Service operation and a Release Group member.

### 16. Terminal records, references, and pruning

Published Release intent, its terminal checkpoint or recovery gate, and the
publication marker are retained for the lifetime of the Environment. Editing,
removing, or spec-applying a Release Group does not delete its historical
Releases. Releases are removed only as part of the Environment's durable
deletion contract.

Release records have no edit, delete, retry, or promote mutation. Retry and
abort act on the Task or active Release operation, never on historical ledger
input.

An active or retryable Release operation pins:

- its render-input records,
- group manifest,
- effect checkpoints and acknowledgement evidence,
- rollback material for every selected historical source,
- referenced Script generations under ADR 0040,
- referenced Secret versions under their retention contract, and
- its active fence generation.

`recovery_required` and `recovering` are active for retention purposes and pin
the same records.

Runtime checkpoint detail and render-input bodies may be pruned only after all
of these are true:

- the Release operation is terminal,
- no retry is allowed,
- no fence refers to the operation,
- no recovery or compensation is pending,
- the durable Release retains the immutable input digest and terminal summary,
  and
- the final Task is independently eligible for pruning.

Rollback material never disappears while its retention projection says
`available`. A retention worker first compares that no active operation pins
the material and atomically changes only that projection to `expired`, recording
`expired_at`. Only then may it prune the detail. The immutable terminal Release
execution summary and its `rollback_material_digest` do not change. A failed or
partial prune leaves the projection `expired`; rollback selection does not
depend on whether leftover bytes still exist. There is no transition from
`expired` back to `available`.

Task and idempotency retention remain governed by their accepted contracts. A
Release's historical Task id remains an opaque audit value after Task pruning;
dereferencing that Task may return not found. Release pages do not depend on
Task retention.

Unpublished staging records are the only Release-related records eligible for
the 24-hour staging garbage collection rule.

### 17. Operator surfaces and 1:1 mapping

Every operation below must exist in the Console, CLI, and REST API with one
shared lower-case action id/OpenAPI `operationId`.

#### CLI contract

`--env <environment>` is the existing global Environment selector and is
required on every command in this section. The global `--id` flag changes
Environment, existing Service, and existing Release Group operands from slugs
to stable ids. For `release-group add`, `<name>` always names the new Release
Group; `--id` affects only Environment and `--services` resolution, while
`--order` is a permutation of those already resolved Service operands. A
Release and a Task have no slug, so their operands are always stable ids.

| Action id | Exact CLI command |
|---|---|
| `service.deploy` | `groundplane --env <environment> [--id] service deploy <service> [--tag <tag>] [--strategy blue-green\|recreate] [--on-failure switch_back\|leave_active]` |
| `service.rollback` | `groundplane --env <environment> [--id] service rollback <service> [--tag <tag>]` |
| `task.abort` | `groundplane --env <environment> task abort <task-id>` |
| `task.retry` | `groundplane --env <environment> task retry <task-id>` |
| `release.list` | `groundplane --env <environment> [--id] release list [--service <service>] [--limit <1-200>] [--cursor <cursor>]` |
| `release.show` | `groundplane --env <environment> release show <release-id>` |
| `release-group.list` | `groundplane --env <environment> [--id] release-group list [--limit <1-200>] [--cursor <cursor>]` |
| `release-group.show` | `groundplane --env <environment> [--id] release-group show <group>` |
| `release-group.add` | `groundplane --env <environment> [--id] release-group add <name> --services <comma-separated-services> [--order <comma-separated-services>] [--tag <tag>] [--on-failure switch_back\|leave_active]` |
| `release-group.edit` | `groundplane --env <environment> [--id] release-group edit <group> [--name <new-name>] [--services <comma-separated-services>] [--order <comma-separated-services>] [--tag <tag>] [--clear-tag] [--on-failure switch_back\|leave_active]` |
| `release-group.remove` | `groundplane --env <environment> [--id] release-group remove <group>` |
| `release-group.deploy` | `groundplane --env <environment> [--id] release-group deploy <group> [--tag <tag>]` |
| `release-group.rollback-preview` | `groundplane --env <environment> [--id] release-group rollback-preview <group> [--tag <tag>]` |
| `release-group.rollback` | `groundplane --env <environment> [--id] release-group rollback <group> [--tag <tag>]` |

For `release-group add`, `--services` contains 2 through 32 unique operands and
`--order`, when present, contains each Service exactly once. The command creates
one complete Release Group.

For `release-group edit`, at least one mutable flag is required. `--services`
replaces the complete membership. If membership changes and `--order` is
omitted, execution order becomes the new `--services` declaration order;
otherwise `--order` must be an exact permutation of the resulting membership.
`--tag` sets the desired default and `--clear-tag` clears it; they are mutually
exclusive. `--name` and `--on-failure` replace their current values.

`release-group remove` deletes the Release Group resource. It does not remove a
member Service. There are no `release-group create` or `release-group delete`
commands and no member-scoped add or remove commands.

`release-group rollback-preview` is a distinct command and capability, not a
`release-group rollback --preview` mode. CLI `--tag` presence is preserved:
omission selects implicitly, while a supplied empty, blank, or surrounding-
whitespace value is invalid and is never normalized into omission.

#### REST request and response types

All REST paths have the `/v1` prefix. All mutations require
`Idempotency-Key`. Optional JSON fields are omitted rather than sent as `null`,
with one exception: `ReleaseGroupEditRequest.tag` is omitted to leave the
default unchanged, is a string to set it, and is JSON `null` to clear it. No
other request field in this contract is nullable. Empty-body operations reject
a non-empty body.

`DeployRequest` is:

```json
{
  "tag": "optional-string",
  "strategy": "optional-blue-green-or-recreate",
  "on_failure": "optional-switch_back-or-leave_active"
}
```

`RollbackRequest` is:

```json
{
  "tag": "optional-string"
}
```

`ReleaseGroupAddRequest` is:

```json
{
  "environment_id": "env_id",
  "name": "canonical-name",
  "service_ids": ["service_id_1", "service_id_2"],
  "order": ["service_id_1", "service_id_2"],
  "tag": "optional-default-tag",
  "on_failure": "optional-switch_back-or-leave_active"
}
```

`order`, `tag`, and `on_failure` are optional. Omitted order copies
`service_ids`; omitted `on_failure` becomes `switch_back`.

`ReleaseGroupEditRequest` is:

```json
{
  "name": "optional-new-name",
  "service_ids": ["optional-service_id_1", "optional-service_id_2"],
  "order": ["optional-service_id_1", "optional-service_id_2"],
  "tag": null,
  "on_failure": "optional-switch_back-or-leave_active"
}
```

At least one field is required. Omitted fields are unchanged. `service_ids`
replaces the complete membership. If `service_ids` is present and `order` is
omitted, order copies the new membership; otherwise `order` must be an exact
permutation of the resulting membership. A string `tag` sets the default and
JSON `null` clears it. `environment_id` and Release Group id are immutable.

`ReleaseGroupDeployRequest` is:

```json
{
  "tag": "optional-string"
}
```

`ReleaseGroupRollbackRequest` is a distinct request type:

```json
{
  "tag": "optional-string",
  "preview_revision": "optional-positive-canonical-decimal-int64-string"
}
```

Both fields are optional. A bodyless request and `{}` select directly at the
current revision; both are the primary contract, not a compatibility path. An
explicit `tag` adds exact tag equality to selection. `preview_revision`
reselects all members from that fixed revision and applies the section 14
authority comparisons. It never consults the persisted group deploy default.
The request accepts no Release ids or eligibility controls.

Tag presence is exact in both the preview query and rollback request. A
supplied empty, blank, or surrounding-whitespace string is invalid and is not
trimmed into omission. The idempotency canonical request includes exact `tag`
presence and value and exact `preview_revision` presence and value. An exact
accepted replay returns the accepted response even when its preview revision is
now stale. A changed payload under the same key is `idempotency.mismatch`.

`ReleaseGroupRollbackPreview` is:

```json
{
  "release_group_id": "release_group_id",
  "revision": "123456789",
  "sources": [
    {
      "service_id": "service_id",
      "release_id": "dep_id",
      "tag": "historical-tag"
    }
  ]
}
```

`revision` is a positive canonical decimal int64 encoded as a JSON string to
avoid JavaScript precision loss. `sources` contains all group members in exact
normalized order. No manifest digest is required.

`ReleaseTaskAccepted` is:

```json
{
  "task_id": "task_id",
  "release_id": "dep_id"
}
```

`ReleaseGroupTaskAccepted` preserves normalized member order:

```json
{
  "task_id": "task_id",
  "release_group_operation_id": "release_group_operation_id",
  "releases": [
    {
      "service_id": "service_id",
      "release_id": "dep_id"
    }
  ]
}
```

`ReleaseGroupMutationAccepted` is:

```json
{
  "task_id": "task_id",
  "release_group_id": "release_group_id"
}
```

`TaskAccepted` is:

```json
{
  "task_id": "task_id"
}
```

The ADR 0041 abort response is not `TaskAccepted`: it does not identify newly
accepted asynchronous work. Its identical one-field JSON shape names the
existing target Task and is returned only after that Task is durably terminal.

`TaskRetryAccepted` is a tagged response. A single-Service Release attempt
returns `ReleaseTaskAccepted`; a Release Group attempt returns
`ReleaseGroupTaskAccepted`. A non-Release Task returns `TaskAccepted` under the
universal Task contract.

`ReleasePage` and `ReleaseGroupPage` contain `items`, nullable `next_cursor`,
and fixed `revision`. Limit defaults to 50 and is valid from 1 through 200.

#### REST operations

| Operation id | Method and path | Query or body | Success |
|---|---|---|---|
| `service.deploy` | `POST /v1/services/{service_id}/deploy` | `DeployRequest` | `202 ReleaseTaskAccepted` |
| `service.rollback` | `POST /v1/services/{service_id}/rollback` | `RollbackRequest`; `{}` selects implicitly | `202 ReleaseTaskAccepted` |
| `task.abort` | `POST /v1/tasks/{task_id}/abort` | empty body | `202 {"task_id":"<target Task id>"}` after durable terminal commit |
| `task.retry` | `POST /v1/tasks/{task_id}/retry` | empty body | `202 TaskRetryAccepted` |
| `release.list` | `GET /v1/releases` | required `environment_id`; optional `service_id`, `limit`, `cursor` | `200 ReleasePage` |
| `release.show` | `GET /v1/releases/{release_id}` | no query or body | `200 ReleaseDetail` |
| `release-group.list` | `GET /v1/release-groups` | required `environment_id`; optional `limit`, `cursor` | `200 ReleaseGroupPage` |
| `release-group.show` | `GET /v1/release-groups/{release_group_id}` | no query or body | `200 ReleaseGroupDetail` |
| `release-group.add` | `POST /v1/release-groups` | `ReleaseGroupAddRequest` | `201 ReleaseGroupDetail` |
| `release-group.edit` | `PATCH /v1/release-groups/{release_group_id}` | `ReleaseGroupEditRequest` | `200 ReleaseGroupDetail` |
| `release-group.remove` | `DELETE /v1/release-groups/{release_group_id}` | empty body | `202 ReleaseGroupMutationAccepted` |
| `release-group.deploy` | `POST /v1/release-groups/{release_group_id}/deploy` | `ReleaseGroupDeployRequest`; `{}` uses the desired default | `202 ReleaseGroupTaskAccepted` |
| `release-group.rollback-preview` | `GET /v1/release-groups/{release_group_id}/rollback-preview` | optional presence-aware `tag` query | `200 ReleaseGroupRollbackPreview` |
| `release-group.rollback` | `POST /v1/release-groups/{release_group_id}/rollback` | `ReleaseGroupRollbackRequest`; `{}` selects each member's newest eligible completed-and-served different-current-tag Release | `202 ReleaseGroupTaskAccepted` |

The Service rollback request always seals `on_failure=switch_back`. Release
Group rollback uses the group's fixed-revision `on_failure`. Neither rollback
request accepts strategy or failure-policy overrides; the optional Release
Group rollback tag is a historical-source selector only.

#### Console actions

| Action id | Exact Console surface |
|---|---|
| `service.deploy` | Service detail Deploy dialog with optional tag, strategy, and on-failure controls |
| `service.rollback` | Service detail Rollback dialog with optional historical tag control |
| `task.abort` | Active Task detail Abort action that waits for durable `aborted`; an already-aborted Task replays success |
| `task.retry` | Terminal Task detail Retry action; recovery mode and probes are shown when the Release operation is gated |
| `release.list` | Environment Releases page with optional Service filter and bounded next-page control |
| `release.show` | Release detail page |
| `release-group.list` | Environment Release Groups page with bounded next-page control |
| `release-group.show` | Release Group detail page |
| `release-group.add` | Environment Release Groups Add dialog with name, complete Services, order, optional default tag, and on-failure controls |
| `release-group.edit` | Release Group detail Edit dialog for mutable name, complete membership, order, default tag, and on-failure policy |
| `release-group.remove` | Release Group detail Remove action that deletes the group resource |
| `release-group.deploy` | Release Group detail Deploy dialog with optional tag override |
| `release-group.rollback-preview` | Preview action within the Release Group detail Rollback dialog; requires the current exact tag input, shows loading and errors, and renders all returned sources in group order |
| `release-group.rollback` | Release Group detail Rollback confirmation; requires a successful current-input preview, sends its revision, invalidates on any exact tag change, and on stale `409` requires a fresh preview and reconfirmation |

#### Errors

Every error uses the existing error envelope. The exhaustive status and code
set introduced or selected by these operations is:

| HTTP status | Error codes |
|---|---|
| `400` | `request.invalid`, `pagination.invalid_cursor`, `pagination.invalid_limit`, `idempotency.mismatch` |
| `401` | `auth.unauthenticated` |
| `403` | `auth.forbidden` |
| `404` | `environment.not_found`, `service.not_found`, `release.not_found`, `release_group.not_found`, `task.not_found` |
| `409` | `resource.in_use`, `state.conflict`, `task.not_abortable`, `release.recovery_required`, `rollback.no_previous_release`, `rollback.source_expired`, `script.retry_unsafe`, `workload.image_resolution_busy`, `idempotency.in_progress` |
| `413` | `request.too_large` |
| `422` | `strategy.not_implemented`, `release.deadline_too_short`, `release.plan_too_large`, `release_group.invalid_members`, `release_group.invalid_order`, `release_group.tag_required` |
| `503` | `workload.image_resolution_unavailable` |

The exact error set per operation is:

| Operation id | HTTP errors |
|---|---|
| `service.deploy` | `400 request.invalid`, `422 strategy.not_implemented`, `422 release.deadline_too_short`, `422 release.plan_too_large`, `400 idempotency.mismatch`, `401 auth.unauthenticated`, `403 auth.forbidden`, `404 environment.not_found`, `404 service.not_found`, `409 resource.in_use`, `409 state.conflict`, `409 release.recovery_required`, `409 workload.image_resolution_busy`, `409 idempotency.in_progress`, `413 request.too_large`, `503 workload.image_resolution_unavailable` |
| `service.rollback` | `400 request.invalid`, `422 release.deadline_too_short`, `422 release.plan_too_large`, `400 idempotency.mismatch`, `401 auth.unauthenticated`, `403 auth.forbidden`, `404 environment.not_found`, `404 service.not_found`, `409 resource.in_use`, `409 state.conflict`, `409 release.recovery_required`, `409 rollback.no_previous_release`, `409 rollback.source_expired`, `409 workload.image_resolution_busy`, `409 idempotency.in_progress`, `413 request.too_large`, `503 workload.image_resolution_unavailable` |
| `task.abort` | `400 request.invalid`, `401 auth.unauthenticated`, `403 auth.forbidden`, `404 environment.not_found`, `404 task.not_found`, `409 task.not_abortable` |
| `task.retry` | `400 request.invalid`, `400 idempotency.mismatch`, `401 auth.unauthenticated`, `403 auth.forbidden`, `404 environment.not_found`, `404 task.not_found`, `409 resource.in_use`, `409 state.conflict`, `409 script.retry_unsafe`, `409 idempotency.in_progress` |
| `release.list` | `400 request.invalid`, `400 pagination.invalid_cursor`, `400 pagination.invalid_limit`, `401 auth.unauthenticated`, `403 auth.forbidden`, `404 environment.not_found`, `404 service.not_found` |
| `release.show` | `400 request.invalid`, `401 auth.unauthenticated`, `403 auth.forbidden`, `404 environment.not_found`, `404 release.not_found` |
| `release-group.list` | `400 request.invalid`, `400 pagination.invalid_cursor`, `400 pagination.invalid_limit`, `401 auth.unauthenticated`, `403 auth.forbidden`, `404 environment.not_found` |
| `release-group.show` | `400 request.invalid`, `401 auth.unauthenticated`, `403 auth.forbidden`, `404 environment.not_found`, `404 release_group.not_found` |
| `release-group.add` | `400 request.invalid`, `422 release_group.invalid_members`, `422 release_group.invalid_order`, `400 idempotency.mismatch`, `401 auth.unauthenticated`, `403 auth.forbidden`, `404 environment.not_found`, `404 service.not_found`, `409 resource.in_use`, `409 state.conflict`, `409 idempotency.in_progress`, `413 request.too_large` |
| `release-group.edit` | `400 request.invalid`, `422 release_group.invalid_members`, `422 release_group.invalid_order`, `400 idempotency.mismatch`, `401 auth.unauthenticated`, `403 auth.forbidden`, `404 environment.not_found`, `404 service.not_found`, `404 release_group.not_found`, `409 resource.in_use`, `409 state.conflict`, `409 idempotency.in_progress`, `413 request.too_large` |
| `release-group.remove` | `400 request.invalid`, `400 idempotency.mismatch`, `401 auth.unauthenticated`, `403 auth.forbidden`, `404 environment.not_found`, `404 release_group.not_found`, `409 resource.in_use`, `409 state.conflict`, `409 idempotency.in_progress` |
| `release-group.deploy` | `400 request.invalid`, `422 strategy.not_implemented`, `422 release.deadline_too_short`, `422 release.plan_too_large`, `422 release_group.tag_required`, `400 idempotency.mismatch`, `401 auth.unauthenticated`, `403 auth.forbidden`, `404 environment.not_found`, `404 service.not_found`, `404 release_group.not_found`, `409 resource.in_use`, `409 state.conflict`, `409 release.recovery_required`, `409 workload.image_resolution_busy`, `409 idempotency.in_progress`, `413 request.too_large`, `503 workload.image_resolution_unavailable` |
| `release-group.rollback-preview` | `400 request.invalid`, `401 auth.unauthenticated`, `403 auth.forbidden`, `404 environment.not_found`, `404 service.not_found`, `404 release_group.not_found`, `409 state.conflict`, `409 rollback.no_previous_release`, `409 rollback.source_expired` |
| `release-group.rollback` | `400 request.invalid`, `422 release.deadline_too_short`, `422 release.plan_too_large`, `400 idempotency.mismatch`, `401 auth.unauthenticated`, `403 auth.forbidden`, `404 environment.not_found`, `404 service.not_found`, `404 release_group.not_found`, `409 resource.in_use`, `409 state.conflict`, `409 release.recovery_required`, `409 rollback.no_previous_release`, `409 rollback.source_expired`, `409 workload.image_resolution_busy`, `409 idempotency.in_progress`, `503 workload.image_resolution_unavailable` |

`release.recovery_incomplete` is not an HTTP dispatch error. It is the terminal
Task error for an accepted recovery attempt whose probe or compensation remains
ambiguous. That Task returns the operation to `recovery_required`. Existing
Agent, health-check, and Script step errors remain nested Task errors under
their owning contracts rather than new HTTP statuses.

`release.list` returns immutable summaries plus derived relationships to the
Service projection. `release.show` returns immutable intent, checkpoint
summary, serving and successful relationships, the separately joined rollback-
material retention projection, and attempt references. It never returns Secret
plaintext or unredacted Script bodies.

`service.show` returns `serving_release_id` and
`current_successful_release_id` and a link to the bounded Release page. It does
not embed an unbounded ledger. The Console Releases page uses `release.list`;
it does not reconstruct history from Tasks or fixture timers.

There is no operator action that directly changes `serving_release_id` or
`current_successful_release_id`.

### 18. Validation and conflicts

Publication rejects before visible state when:

- the Environment or Service is missing, disabled, tombstoned, or outside the
  requester's workspace
- a required Service or Environment/group fence is owned
- strategy is not `blue-green` or `recreate`
- tag or requested-reference provenance, route, health, Secret version,
  render-input digest, or Script reference cannot be sealed reproducibly
- a Release Group has fewer than 2 or more than 32 members
- group members are not unique or are outside one enabled Compose project
- explicit order is not an exact permutation of members
- a group deploy has neither a request tag nor a desired default tag
- rollback has no eligible historical source
- rollback has matching history but its rollback material is expired
- selected hooks violate accepted ADR 0040's membership, size, snapshot, or execution bounds
- a record, plan, request, or final publication transaction exceeds its bound
- the configured Release execution timeout is shorter than the computed plan
  budget
- fixed-revision evidence changes before the publication compare

Expected conflict codes include:

- `resource.in_use` for a conflicting fence owner
- `rollback.no_previous_release` for no eligible rollback source
- `rollback.source_expired` when matching history lacks available rollback
  material
- `release.recovery_required` when the existing `task.retry` recovery path must
  clear a gated operation before another Release operation
- `idempotency.in_progress` from ADR 0021
- the existing validation and not-found codes for malformed or missing
  workspace resources

Validation must identify the group member when a member-specific failure
prevents publication.

## Earliest executable proof

The first focused contract proof uses a locally built image with no
`RepoDigests` and covers all four identity/count boundaries:

1. authored Compose omission normalizes once to candidate count one, while an
   absent or zero count in a durable Release input is rejected;
2. a two-replica candidate and a differently sized prior Release render and
   reconcile their exact independently sealed counts;
3. retagging the requested reference after publication leaves an available
   old local id valid for exact rollback and never selects the tag's new bytes;
4. removing a sealed candidate or prior local id fails validation before the
   first stop, apply, proxy, Script, or compensation workload effect.

The earliest disposable-host proof repeats those cases through Blueprint,
single-Service Deploy/Rollback, Release Group, retry, and compensation. It also
proves that Script image authority and applied-image evidence use the sealed
Docker id without inventing a repository digest.

## Consequences

### Positive

- Release history has stable identity independent of Task attempts and current
  desired state.
- Serving truth remains explicit when post-serving work fails.
- Rollback selection is deterministic at a fixed revision and produces a new
  auditable Release.
- Retry and reconciliation cannot silently create or adopt a different
  candidate.
- Group execution has one atomic publication boundary, one Task, one ordered
  checkpoint stream, and deterministic reverse-order compensation.
- The 32-member group bound remains compatible with bounded publication by
  staging immutable inputs and publishing one fence generation and one
  Environment index plus one Service index per member.
- Release history remains available after Task pruning.
- Console, CLI, and API expose the same operator capabilities and bounded
  ledger pages.

### Negative

- The Controller must maintain a separate Service release projection and
  Release checkpoint model instead of deriving current state from the newest
  Release record.
- Group publication needs invisible staging, a publication marker, cleanup,
  and two owner-index contracts.
- Reconciliation must inspect container labels and router evidence before it
  can resume after uncertain effects.
- `switch_back` can remain in `recovery_required` until exact probes prove
  restoration or proved absence; ambiguous runtime cannot be guessed away.
- Release ledger retention lasts for the Environment lifetime and consumes
  durable storage beyond Task retention.
- Acceptance requires coordinated replacement of the old Release status and
  embedded-ledger mirror contracts in the product docs, OpenAPI, CLI, and
  Console.

## Alternatives considered

### Store one Environment current and previous Release

Rejected. Releases are per Service, different Services can deploy concurrently,
and a Release Group does not create one shared runtime artifact.

### Use one `active_release_id`

Rejected. It cannot represent a failed candidate that is serving under
`leave_active` while an older Release remains the last successful one.

### Reactivate a historical Release during rollback

Rejected. It destroys append-only audit history and makes retry and Task
identity ambiguous.

### Render again from current desired state on retry

Rejected. That makes retry a new deploy under an old identity and breaks
reproducibility.

### Dispatch one child Task per Release Group member

Rejected. It cannot provide one atomic fence boundary, one failure policy, or
deterministic compensation and violates the product's one-Task contract.

### Hold the Environment-wide operation lock for every Service deploy

Rejected. It would unnecessarily serialize independent Services. Per-Service
fences plus Environment revision compares preserve correctness without that
loss of concurrency.

### Mutate every candidate Release from staged to published

Rejected. A 32-member operation would spend the transaction budget on visibility
mutations. One publication marker makes the staged immutable set visible
atomically.

### Silently treat an impossible `switch_back` as `leave_active`

Rejected. The requested safety policy must not be weakened without an explicit
operator decision. The honest gate state is `recovery_required`, and the
existing Task retry path must prove compensation before clearing it.

### Accept singular and plural Release Group Blueprint extensions

Rejected. `x-gp-release-groups` is the implemented grammar. A singular alias
would create two spellings and violate clean replacement.

## Independent-review disposition

Independent review confirmed these product choices, which are now normative in
this proposal rather than open questions:

1. A first deploy under `switch_back` treats a null prior serving identity as
   sealed absence. It gates as `recovery_required` until recovery removes exact
   candidate runtime and proves that absence.
2. The optional `tag` in `x-gp-release-groups` is the group's desired default;
   a deploy-time tag overrides it, and absence of both is an error.
3. Explicit rollback tags select only successful historical Releases through
   the same eligibility predicate as implicit rollback.
4. Published Release intent and terminal summaries live for the Environment
   lifetime; Tasks and eligible detail may expire independently.
5. Environment and Service Release pages use two indexes per Release so both
   traversals are bounded and Environment ordering is stable-id ascending.
6. `controller.release_execution_timeout` defaults to `15h`, is valid from
   `40m` through `24h`, and has no user or edition override.
7. Hook-bearing operations remain unavailable until ADR 0040's Runner contract
   is implemented.

No unresolved product decision remains in this ADR. Its remaining gates are
decision lifecycle, implementation, and synchronization work: ADR 0013 must
accept or replace the persistence contract, ADR 0040's Runner contract must be
implemented before hooks are used, and all product, API, CLI, and Console
mirrors must clean-replace contradictory contracts and remain synchronized.

## Out of scope

- rolling, canary, percentage, or traffic-splitting deployments
- multi-host or multi-Agent Release coordination
- cross-Environment Release Groups
- database migration orchestration
- registry credential and image-build workflows
- scheduled deploys, approvals, promotion pipelines, and GitOps automation
- edition-specific, licensing, or enterprise-only Release behavior
- manual mutation of serving or successful Release pointers
