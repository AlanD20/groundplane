# ADR 0053: Durable hierarchy and backing-facade deletion

- Status: Accepted
- Date: 2026-08-25

Owner approval recorded 2026-08-25 resolves the deletion choices: failed
aggregate deletion retains its tombstone, Project Secrets and Tenant/Project
Runners are contained descendants, hierarchy projections expose
`deletion_task_id`, each parent attempt has a six-hour deadline, and permanent
Backing Service deletion uses the shared receipt engine with `cursor.expired`
mapped to HTTP 409. Environment deletion is the fourth aggregate target and
uses this same engine with `operation_kind=environment.delete`; it is not a
parallel Agent-task cascade. The decision does not claim implementation,
change another ADR's status, or authorize enterprise scope.

## Context

The MVP exposes permanent deletion for a Tenant, an ordinary Project, and a
Backing Service facade, but it does not yet have one complete durable contract
for those operations.

ADR 0013 proposes atomic Task and tombstone publication, visible-until-finalized
resources, stable-id postorder traversal, mutation fences, bounded etcd
transactions, and parent-last finalization. Its generic deletion sketch leaves
typed tombstone schemas and resource finalizers open and permits a failed final
CAS to remove a tombstone. That is unsafe after a hierarchy delete has already
removed descendants: reopening the surviving parent would expose an aggregate
that can no longer be reconstructed from current desired state.

Existing resource contracts provide narrower precedents. Environment deletion
retains immutable intent, cleanup state, and ownership across failed attempts.
Runner removal deletes only Groundplane's local runtime and claims and never
deregisters the Runner at GitHub. Attach and Zone removal define typed
deprovisioning and owned-versus-external Network behavior. ADR 0037 proposes a
detailed Backing Service deletion protocol and parent-owned terminal receipts,
but it is Backing-Service-specific, uses a C10 keyspace, and maps compacted
impact cursors differently from ADR 0013. ADR 0049 proposes the bounded Volume
removal authority needed by a backing cascade.

The current Console fixture actions are not authority for durable deletion.
They use runtime-only `destroy`, remove fixtures immediately, and manufacture
Activity. The public Task type for permanent deletion is `remove`; `destroy`
continues to mean runtime absence without durable data removal.

The current API inventory also permits both generic Project DELETE and the
Backing Service facade DELETE to address a backing Project. One operator
capability cannot have two REST paths under the 1:1 rule.

This proposal defines one MVP deletion coordinator for exactly these three
aggregate operations. It does not grant authority to Proposed ADR 0013 or ADR
0049. Acceptance requires accepting their needed persistence and Volume
contracts, or replacing them with accepted contracts that provide the same
observable guarantees.

## Scoped authority and supersession

This table is exhaustive for overlapping deletion authority. If this ADR is
Accepted, no second exact authority remains active for the listed scope.

| Existing decision | Scope after ADR 0053 acceptance | ADR 0053 authority |
| --- | --- | --- |
| ADR 0013 generic hierarchy deletion | Continues to own general record/index limits, key encoding, fixed-revision reads, ordinary resource deletion, and non-specialized tombstones. | Replaces ADR 0013's failed-finalization rule only for Tenant, ordinary Project, and backing-facade permanent deletion. These aggregates retain their tombstone on failure, abort, timeout, corruption, and successful resource finalization until the 90-day deletion-retention pruner completes. |
| ADR 0037 C10 permanent deletion | Continues to own Backing Service Create, Start, Stop, Destroy, adapter readiness, and facade projection if ADR 0037 is separately Accepted. | Replaces ADR 0037 section 8, its C10 deletion keys, C10 receipt domain separators, parent receipt/replay rules, finalization rules, and its `cursor.expired` HTTP 410 mapping. The generic keys, separators, completion proofs, replay rules, and HTTP 409 mapping below are the sole permanent-delete authority. |
| ADR 0049 Volume removal | Remains the Volume identity, physical cleanup, impact, and bounded finalization authority if Accepted. | Seals each ADR 0049 Agent cleanup batch and Controller finalization as explicit parent actions; it does not duplicate Volume deletion. |
| [Task Abort contract](../features/tasks-and-logs.md#abort) Task abort | Remains the authority for aborting one Task attempt and distinguishing operator abort from Controller shutdown. | Adds aggregate-specific dispatch retirement and tombstone retention; it does not add a second abort Task. |
| ADR 0035 Task pruning | Remains the 90-day Task/event/idempotency retention authority. | Binds deletion retention to the final parent Task's exact `retain_until` and adds a separate bounded deletion-state pruner after ADR 0035 has removed the parent Task and marker. |

ADR 0037 acceptance is not a prerequisite for the generic deletion engine or
ordinary Tenant/Project deletion. Backing permanent deletion requires this ADR
and an Accepted ADR 0049 (or a replacement with the same Volume guarantees),
but does not require ADR 0037's deletion section to become Accepted first.
Acceptance of this ADR directly adopts the backing dependency, SCC, runtime
reconstruction, and facade-finalization clauses stated here. Acceptance still
requires ADR 0013's needed persistence constraints to be Accepted or replaced.

## Decision

### 1. Public operations, Task identity, and facade exclusivity

The three public hierarchy deletion operations are:

| Delete | Public operation id | Parent Task | Workspace | Target |
| --- | --- | --- | --- | --- |
| ordinary Project | `project.delete` | `executor=controller`, `type=remove` | owning Tenant | stable Project id |
| Tenant | `tenant.delete` | `executor=controller`, `type=remove` | owning Tenant | stable Tenant id |
| Environment | `environment.delete` | `executor=controller`, `type=remove` | owning Tenant or Platform | stable Environment id |

The separately owned Backing Service lifecycle exposes
`backing-service.destroy`. Its implementation reuses this engine with the
private `backing.delete` operation and a Platform-owned `remove` Task; this ADR
does not create a second public hierarchy DELETE route for it.

No `delete` Task type is added. The exact private operation kinds are
`project.delete`, `tenant.delete`, and `backing.delete`. A private operation id
is the stable instance identity shared by all parent attempts; it is not the
public route operation id or a Task id.

The parent is a durable Controller coordinator. It performs no workload-host
mutation itself. Agent child Tasks retain existing closed Task types such as
`remove` and `detach`. Native Controller finalizers are checkpointed steps of
the parent operation, not nested Controller Tasks.

`DELETE /projects/{id}` accepts only `kind=tenant`. A backing Project returns
`validation.failed` with HTTP 422 and guidance to use the Backing Service
facade. `POST /backing-services/{project_id}/destroy` accepts only a valid
backing facade; an ordinary Project returns `backing_service.not_found` with
HTTP 404. There is no second permanent-delete route for a backing Project.

### 2. Visibility, ownership, and aggregate fences

A Tenant, ordinary Project, and Backing Service remain listable and showable
until the successful final transaction. Their public Tenant or Project
representation adds:

```text
deletion_task_id: id | null
```

The field names the current parent attempt while the deletion operation exists
and is null otherwise. A retry atomically changes it to the successor parent
Task id. A failed, aborted, or timed-out attempt therefore leaves the resource
visible, fenced, and retryable instead of making it look editable.

While a tombstone exists, conflicting edit, rename, descendant create, new
independent operation, and independent delete publication return
`resource.in_use` with HTTP 409. Active separately owned descendant work blocks
initial publication with `state.conflict` or `resource.in_use`; deletion never
adopts, cancels, or rewrites it. Parent slugs and every remaining unique index
stay reserved while deletion is active or its current attempt is terminal but
retryable. A slug becomes reusable only when the successful final transaction
removes the primary and slug index. Stable ids are never reused. Descendants
already finalized remain absent during retry, and the ancestor fence prevents
recreation.

Bounded publication uses one aggregate mutation authority:

```text
/v1/runtime/hierarchy-coordination/tenant/<tenant-id>
/v1/runtime/hierarchy-coordination/project/<project-id>
```

```text
HierarchyCoordinationV1 = {
  schema: 1,
  target_kind: "tenant" | "project" | "environment" | "backing-service",
  target_id: id,
  mutation_epoch: int64
}
```

Every descendant create, mutation, operation publication, and terminalization
increments the applicable Project and Tenant epochs in the same transaction.
Deletion preparation scans active descendant authority at one fixed revision,
rejects every active separately owned operation, and compares the exact
coordination revisions when it publishes the tombstone. After publication,
every descendant mutation compares all applicable ancestor tombstones absent.
The epochs close the preparation race without comparing an unbounded active
operation set.

Initial publication atomically writes the parent Task, Controller queue record,
Task indexes, active-operation marker, protected idempotency evidence,
immutable intent, tombstone, deletion replay-target locator, deletion cleanup
fence, deletion operation lock, and the incremented hierarchy coordination
epoch. It is rejected above 64 aggregate compares plus mutations or 900 KiB of
serialized request bytes. An unknown outcome is resolved from the protected
idempotency evidence and exact published aggregate; it never creates a second
operation.

### 3. Shared durable deletion records

The typed tombstone keeps ADR 0013's approved key:

```text
/v1/runtime/deletions/<target-kind>/<stable-id>
```

One generic namespace owns locks, immutable plans, action completion, child
authority, receipts, retention, and pruning for all three aggregate deletes:

```text
/v1/runtime/deletion-locks/<target-kind>/<target-id>
/v1/runtime/deletion-replay-targets/~<parent-operation-id>
/v1/runtime/deletion-cleanup-fences/~<parent-operation-id>
/v1/runtime/deletion-intents/~<parent-operation-id>
/v1/runtime/deletion-actions/~<parent-operation-id>/~<20-digit-ordinal>
/v1/runtime/deletion-completions/~<parent-operation-id>/~<20-digit-ordinal>
/v1/runtime/deletion-children/~<parent-operation-id>/~<child-operation-id>
/v1/runtime/deletion-successors/~<parent-operation-id>/~<child-operation-id>/<attempt-id>
/v1/runtime/deletion-receipts/~<parent-operation-id>/~<child-operation-id>/attempts/<attempt-id>
/v1/runtime/deletion-receipts/~<parent-operation-id>/~<child-operation-id>/current
/v1/runtime/deletion-progress/~<parent-operation-id>/~<child-operation-id>/attempts/<attempt-id>
/v1/runtime/deletion-receipt-summaries/~<parent-operation-id>
/v1/runtime/deletion-completion-summaries/~<parent-operation-id>
/v1/runtime/deletion-receipt-scan-cursors/~<parent-operation-id>/<parent-task-id>
/v1/runtime/deletion-completion-scan-cursors/~<parent-operation-id>/<parent-task-id>
/v1/runtime/deletion-prune-intents/~<parent-operation-id>
```

Literal collection, target-kind, `attempts`, and `current` segments are written
exactly as shown. Validated stable resource, Task, and attempt ids are raw key
segments. Operation ids are not stable resource ids under ADR 0013, so parent
and child operation ids are encoded exactly once as `~` followed by canonical
unpadded base64url of their UTF-8 bytes. The 20-digit ordinal is likewise a
non-ID dynamic segment and uses that encoding. Decoders reject empty values,
padding, alternate encodings, invalid stable-id prefixes, and bytes that do not
re-encode identically. No digest or slug appears raw in a key.

The tombstone is canonical JSON and at most 64 KiB:

```text
DeletionTombstoneV1 = {
  schema: 1,
  target_kind: "tenant" | "project" | "environment" | "backing-service",
  target_id: id,
  target_revision: int64,
  operation_kind: "tenant.delete" | "project.delete" | "environment.delete" | "backing.delete",
  operation_id: id,
  deletion_epoch: int64,
  current_task_id: id,
  workspace:
      {type:"tenant", tenant_id:id}
    | {type:"platform"},
  snapshot_revision: int64,
  phase:
      "planning"
    | "executing"
    | "summarizing"
    | "finalizing"
    | "retained",
  plan_count: int64 | null,
  plan_digest: digest | null,
  checkpoint: {
    next_ordinal: int64,
    completed_count: int64,
    completed_prefix_digest: digest,
    active_child_operation_id: id | null,
    active_child_attempt_id: id | null,
    receipt_summary_digest: digest | null,
    completion_summary_digest: digest | null
  },
  created_at: timestamp,
  attempt_deadline: timestamp,
  terminal:
      null
    | {
        status: "completed",
        task_id: id,
        completed_at: timestamp,
        retain_until: timestamp,
        completion_summary_digest: digest
      }
}
```

`plan_count` and `plan_digest` are both null only in `planning`.
`completed_count == next_ordinal`. The two active child ids are both null or
both non-null. `receipt_summary_digest` is non-null only after all Agent child
receipts have been summarized. `completion_summary_digest` and `terminal` are
non-null only in `retained`. Successful resource finalization retains this
completed tombstone until the exact 90-day `retain_until`; it does not retain a
public Tenant, Project, or Backing Service record.

The immutable intent is canonical JSON and at most 64 KiB:

```text
DeletionIntentV1 = {
  schema: 1,
  operation_kind: "tenant.delete" | "project.delete" | "environment.delete" | "backing.delete",
  operation_id: id,
  deletion_epoch: int64,
  target_kind: "tenant" | "project" | "environment" | "backing-service",
  target_id: id,
  target_revision: int64,
  workspace:
      {type:"tenant", tenant_id:id}
    | {type:"platform"},
  root_slug: slug,
  snapshot_revision: int64,
  timeout: duration,
  backing_authority:
      null
    | {
        service_id: id,
        dependency_head_revision: int64,
        row_count: int64,
        row_set_digest: digest
      },
  created_at: timestamp
}
```

`timeout` is exactly six hours. Ordinary Project and Tenant deletion set
`backing_authority=null`. Backing deletion freezes the exact final impact-page
authority.

Each immutable action is canonical JSON and at most 16 KiB. The enum is closed
and covers every destructive descendant effect in the MVP:

```text
DeletionActionV1 = {
  schema: 1,
  parent_operation_id: id,
  ordinal: int64,
  action_kind:
      "attach.grant-revoke"
    | "attach.detach"
    | "environment.agent-cleanup"
    | "service.remove"
    | "entry.remove"
    | "route.remove"
    | "component.remove"
    | "script.remove"
    | "release-group.remove"
    | "release.finalize"
    | "backup-policy.finalize"
    | "key-material.remove"
    | "materialization.remove"
    | "volume.agent-cleanup"
    | "volume.finalize"
    | "zone.remove"
    | "network.remove"
    | "reservation.release"
    | "recovery-point.remove"
    | "orphan-object.remove"
    | "connector.finalize"
    | "environment.finalize"
    | "runner.local-remove"
    | "project-secret.remove"
    | "backing.runtime-reconstruct"
    | "backing-service.finalize"
    | "project.finalize"
    | "tenant.finalize",
  target_kind:
      "tenant" | "project" | "environment" | "service" | "attach"
    | "entry" | "route" | "component" | "script" | "release-group"
    | "release" | "backup-policy" | "key-material" | "materialization"
    | "volume" | "zone" | "network" | "reservation"
    | "recovery-point" | "orphan-object" | "connector" | "runner"
    | "secret" | "backing-service",
  target_id: id,
  target_revision: int64,
  prerequisite_ordinals: [int64, ...],
  procedure:
      {
        kind: "agent-child",
        child_operation_id: id,
        task_type: "remove" | "detach",
        typed_procedure:
            "environment.cleanup"
          | "service.remove"
          | "attach.grant-revoke"
          | "attach.detach"
          | "entry.remove"
          | "route.remove"
          | "component.remove"
          | "materialization.remove"
          | "volume.cleanup"
          | "zone.remove"
          | "network.remove"
          | "recovery-point.remove"
          | "orphan-object.remove"
          | "backing.runtime-reconstruct",
        input_digest: digest,
        timeout: duration
      }
    | {
        kind: "controller-finalizer",
        finalizer:
            "script.remove"
          | "release-group.remove"
          | "release.finalize"
          | "backup-policy.finalize"
          | "key-material.remove"
          | "volume.finalize"
          | "reservation.release"
          | "connector.finalize"
          | "environment.finalize"
          | "runner.remove"
          | "secret.remove"
          | "backing.finalize"
          | "project.finalize"
          | "tenant.finalize",
        fixed_input_revision: int64,
        compare_set_digest: digest,
        mutation_set_digest: digest,
        postcondition_digest: digest
      }
}
```

`action_kind` fixes exactly one executor and one same-purpose typed procedure.
Environment root/Compose cleanup; Attach grant revoke/detach; Service, Entry,
Route, Component, materialization, Zone, Network, Recovery Point, orphan
object, and Volume physical cleanup; and backing runtime reconstruction use
Agent child procedures. Script, Release Group, Release ledger, Backup Policy,
key material, ADR 0049 Volume record/head finalization, reservation release,
Connector finalization, Environment finalization, Runner local teardown,
Project Secret removal, Backing Service facade finalization, Project
finalization, and Tenant finalization use the closed Controller finalizer
domain above. The builder rejects any action-kind/executor/procedure tuple not
in this mapping. `runner.local-remove` invokes the native Runner adapter and
never the workload Agent. A Controller finalizer that needs several bounded
transactions is represented by one sealed action per bounded batch, each with
its own ordinal and fixed input.

Fixed-revision membership does not invent a stored procedure before plan
identity exists. It emits a closed `ControllerFinalizerInput` containing the
finalizer, target kind/id, fixed input revision and digest, and bounded batch
ordinal/count, or a closed Agent input without a child operation id. The
domain assigns stable action ids, ordinals, and prerequisites. The etcd plan
binder then derives the retry-stable child `op_<ULID>` from the retained parent
operation plus node identity and seals the Controller transaction templates.
Only these fully bound actions may enter a plan batch; unbound membership input
is never durable action state.

An ordinary Environment plan seals its exact Services, Attaches and grant
edges, Entries, Routes, Components, Scripts, Release Groups, Release records,
Backup Policy, materializations, Volumes, Zones, Networks, reservations,
Recovery Points, remote orphan objects, Connectors, key material, and
Environment finalizer. Backing deletion omits consumer-owned Recovery Point
and Connector actions because those records are retained external references;
ordinary Environment deletion includes its owned Connector finalizer only
after every retained point/orphan authority permits it. A backing facade plan
seals the adapter runtime reconstruction and the final
`backing-service.finalize` action. No generic Project action may target a
backing Project.

The plan contains every destructive effect; a finalizer may not discover and
delete an unsealed descendant. `prerequisite_ordinals` is strictly ascending,
contains only smaller ordinals, and is part of the action digest. The complete
plan uses every ordinal in the contiguous range `0..plan_count-1`; Agent and
Controller actions share that range and no ordinal may be skipped, reused, or
reserved by executor kind.

The remaining generic values are closed as follows. `DeletionLockV1` is at
most 4 KiB:

```text
DeletionLockV1 = {
  schema: 1,
  target_kind: "tenant" | "project" | "environment" | "backing-service",
  target_id: id,
  parent_operation_id: id,
  deletion_epoch: int64,
  tombstone_key: string,
  cleanup_fence_key: string,
  created_at: timestamp
}
```

`DeletionReplayLocatorV1` is at most 4 KiB and survives target removal:

```text
DeletionReplayLocatorV1 = {
  schema: 1,
  parent_operation_id: id,
  operation_kind: "tenant.delete" | "project.delete" | "environment.delete" | "backing.delete",
  target_kind: "tenant" | "project" | "environment" | "backing-service",
  target_id: id,
  deletion_epoch: int64,
  root_task_id: id,
  current_task_id: id,
  response_digest: digest,
  tombstone_key: string,
  retain_until: timestamp | null
}
```

`DeletionCleanupFenceV1` is canonical JSON at most 4 KiB and is the exact
cleanup authority compared by every receipt and action-completion CAS:

```text
DeletionCleanupFenceV1 = {
  schema: 1,
  parent_operation_id: id,
  deletion_epoch: int64,
  target_kind: "tenant" | "project" | "environment" | "backing-service",
  target_id: id,
  plan_digest: digest | null,
  generation: int64,
  phase: "planning" | "executing" | "summarizing" | "finalizing" | "retained",
  current_task_id: id,
  active_action_ordinal: int64 | null,
  active_child_operation_id: id | null,
  active_child_attempt_id: id | null,
  dispatch: "open" | "retiring" | "closed",
  updated_at: timestamp
}
```

Every resource has an existing hierarchy coordination record before deletion.
Initial publication reads its exact value and ModRevision with
`mutation_epoch=e`, compares both, writes `mutation_epoch=e+1`, and sets
`deletion_epoch=e+1` in the tombstone, intent, lock, replay locator, cleanup
fence, Task immutable input, and every later action/receipt authority. The
positive `deletion_epoch` never changes for that operation or any retry.

The cleanup fence begins at `generation=1`, `phase=planning`, `dispatch=open`.
Every successful rewrite increments generation by exactly one; no replay may
decrement, skip, or reuse a generation. Sealing moves planning to executing.
Action selection and clearing, parent Task ownership transfer, retirement,
receipt consumption, summarizing, finalizing, and retained publication each
compare the exact prior fence revision and bytes and write the exact successor.
An unknown outcome is replayed by a fixed-revision read: the exact expected
successor bytes at one later generation plus the corresponding immutable row
is success; exact prior bytes permit the same CAS retry; any other bytes are
corruption. In the receipt protocol below, "exact parent operation" means the
exact tombstone revision and value, and "cleanup fence" means this key's exact
revision and value. Therefore the existing 9/8/8 operation counts each contain
one constructible cleanup-fence compare and are replay-verifiable.

The two summary scans have separate Task-scoped durable cursors. Both values
are canonical JSON at most 4 KiB. Their parent Task id is a validated stable id
and remains raw in the key; their parent operation id uses the encoded dynamic
segment shown above.

```text
DeletionReceiptSubsetScanCursorV1 = {
  schema: 1,
  cursor_kind: "agent-receipt-subset",
  parent_task_id: id,
  parent_task_type: "remove",
  parent_operation_kind:
      "tenant.delete" | "project.delete" | "environment.delete" | "backing.delete",
  parent_operation_id: id,
  deletion_epoch: int64,
  sealed_plan_sha256: digest,
  next_action_ordinal: int64,
  accumulated_agent_count: int64,
  current_receipt_chain_hash: bytes32,
  phase: "scanning" | "ready"
}

DeletionCompletionScanCursorV1 = {
  schema: 1,
  cursor_kind: "all-action-completion",
  parent_task_id: id,
  parent_task_type: "remove",
  parent_operation_kind:
      "tenant.delete" | "project.delete" | "environment.delete" | "backing.delete",
  parent_operation_id: id,
  deletion_epoch: int64,
  sealed_plan_sha256: digest,
  next_action_ordinal: int64,
  accumulated_action_count: int64,
  current_completion_chain_hash: bytes32,
  phase: "scanning" | "ready"
}
```

`sealed_plan_sha256` equals the exact non-null tombstone `plan_digest`.
`next_action_ordinal` is the next original full-plan ordinal and is in
`0..plan_count`. A `bytes32` value is the canonical unpadded base64url encoding
of exactly 32 bytes; every formula below consumes the decoded raw bytes, as it
does for `sealed_plan_sha256`. `phase=scanning` is valid before the next ordinal
reaches
`plan_count`; `phase=ready` is valid only at `plan_count`. The receipt cursor's
count advances only for Agent actions. The completion cursor's count advances
once for every action and therefore always equals `next_action_ordinal`.

The two scans use these eight distinct literal ASCII domains:

```text
receipt init:       gp-deletion-receipt-scan-init-v1
receipt row:        gp-deletion-receipt-scan-row-v1
receipt chain:      gp-deletion-receipt-scan-chain-v1
receipt summary:    gp-deletion-receipt-summary-v1
completion init:    gp-deletion-completion-scan-init-v1
completion row:     gp-deletion-completion-scan-row-v1
completion chain:   gp-deletion-completion-scan-chain-v1
completion summary: gp-deletion-completion-summary-v1
```

`NUL` below is one `0x00` byte, `uint64be` is exactly eight bytes, and every
SHA-256 result is consumed as its raw 32 bytes. For the Agent-receipt subset,
where `p` is the deterministic canonical JSON bytes of the immutable
`ParentChildProgress{consumed_action:"advance"}` row and `o` is that row's
original sealed-plan action ordinal, the resumable chain is exactly:

```text
R0 = SHA-256(
  ASCII("gp-deletion-receipt-scan-init-v1") || NUL || sealed_plan_sha256
)
Rrow(p) = SHA-256(
  ASCII("gp-deletion-receipt-scan-row-v1") || NUL || p
)
Rnext = SHA-256(
  ASCII("gp-deletion-receipt-scan-chain-v1") || NUL ||
  Rprevious || uint64be(o) || Rrow(p)
)
```

For the all-action scan, where `c` is the deterministic canonical JSON bytes
of `DeletionActionCompletionV1` and `o` is its original sealed-plan ordinal,
the separate resumable chain is exactly:

```text
C0 = SHA-256(
  ASCII("gp-deletion-completion-scan-init-v1") || NUL || sealed_plan_sha256
)
Crow(c) = SHA-256(
  ASCII("gp-deletion-completion-scan-row-v1") || NUL || c
)
Cnext = SHA-256(
  ASCII("gp-deletion-completion-scan-chain-v1") || NUL ||
  Cprevious || uint64be(o) || Crow(c)
)
```

The receipt version-zero cursor stores `R0`; the completion version-zero cursor
stores `C0`. A cursor stores only the current 32-byte chain hash, accumulated
count, and next original ordinal, never concatenated row bytes or serialized
SHA-256 implementation state. A verifier reads raw canonical rows in scan order
and recomputes the same count and chain in one pass. A count-first pass, raw-row
concatenation digest, or marshalled hash continuation is not this protocol.
Both cursors, both scans, and both immutable summaries are mandatory for every
sealed plan. A plan with no Agent actions still runs the receipt-subset scan to
`plan_count`, publishes `child_count=0` with `Rfinal=R0`, and consumes the
receipt cursor; absence of the receipt summary is never an empty-subset marker.

After the plan and all completion rows are sealed, one eight-operation
initialization transaction performs four compares (exact tombstone, exact
cleanup fence, receipt-cursor `ModRevision==0`, completion-cursor
`ModRevision==0`) and four puts (both version-zero cursors, tombstone in
`summarizing`, and the next cleanup-fence generation). An exact existing pair
with the same Task, epoch, plan digest, zero counts, zero next ordinal, initial
`R0` and `C0` chain hashes, and `phase=scanning` is lost-response success; one
missing cursor or any unequal bytes is corruption. A retry never creates a
cursor for a successor parent Task: it resumes the cursors named by the
retained `parent_task_id` that entered summarizing.

The plan digest is exactly:

```text
SHA-256(
  "gp-deletion-plan-v1\0" ||
  u64be(action_count) ||
  for each ordinal:
    u32be(length(canonical_action_json)) ||
    canonical_action_json
)
```

### 4. Deterministic postorder plan

Containment is:

```text
Tenant
  ordinary Projects
    Environments
      Environment-owned descendants
    Project-owned Runners
    Project-owned Secrets
  directly Tenant-owned Runners
```

Backing Projects are Platform-owned and never Tenant descendants. Tenant and
ordinary Project deletion never touches a Backing Service or Platform Secret.

Planning reads accepted owner indexes while the ancestor fence is held. Direct
children are ordered by raw stable-id UTF-8 bytes ascending, each child subtree
is visited, and its parent finalizer is emitted last. Because stable ids are
kind-prefixed, an ordinary Project visits Environment subtrees, Project
Runners, Project Secrets, and then the Project. A Tenant visits ordinary
Project subtrees, directly owned Runners, and then the Tenant.

A Tenant plan is flattened. It never dispatches nested Controller Project
Tasks, because they would deadlock behind the serial native Controller
executor. Environment cleanup is an Agent child Task. Runner, Secret, Project,
Tenant, and facade finalizers are Controller steps checkpointed inside the one
parent Task.

Action rows are written in batches of at most 47. The full batch is exactly:

```text
1 tombstone compare
47 action-key absence compares
47 action puts
1 tombstone/checkpoint put
= 96 aggregate operations
```

The exact arithmetic is `1 + 47 + 47 + 1 = 96`. Every builder also enforces
the independent 900-KiB serialized request limit. A plan is executable only
after `plan_count` and `plan_digest` are sealed together.

### 5. Ordinary Project and Tenant finalizers

An ordinary Project executes:

1. Each Environment's existing owner cascade, including Attach detach;
   Service, Route, Entry, Component, Volume, and Zone cleanup; remote Recovery
   Point and orphan deletion; Connector and key retention until remote absence;
   Environment-root removal; and pool release.
2. Each Project Runner's exact local-only Runner finalizer. It stops and
   removes Groundplane's local Runner container, rootless daemon, bridge, host
   account and subordinate mappings, local state, quota claim, host slot, and
   system subnet. It never removes the GitHub registration.
3. Each Project Secret's typed Controller finalizer, but only after every
   Environment consumer and generated file is gone.
4. The Project finalizer, which atomically removes the Project primary, Tenant
   owner index, scoped slug index, coordination record, and operation lock while
   publishing that sealed action's immutable Controller completion proof.

A Tenant executes:

1. Every ordinary Project subtree in the frozen plan.
2. Every directly Tenant-owned Runner with the same local-only finalizer.
3. The Tenant finalizer, which atomically removes the Tenant primary, global
   slug index, coordination record, and operation lock while publishing that
   sealed action's immutable Controller completion proof.

The resource finalization transaction is parent-last and bounded. Resource
disappearance, slug release, final action completion, retained tombstone
publication, completion-summary binding, Task completion, and terminal
idempotency evidence commit together. It does not enumerate or delete action,
completion, receipt, or progress prefixes. Cross-resource dependencies block
rather than being rewritten,
except for the already-defined backing-owned Attach and Zone cascade. An active
descendant Task, independently owned deletion, restore, backup, prune, or
unknown external effect blocks initial publication.

### 6. Shared child receipts and bounded retry authority

ADR 0037's parent-owned receipt protocol becomes the shared deletion receipt
engine. Its five durable value schemas are adopted field-for-field; only the
generic keys above, the deletion-specific frozen Task owner and authority, and
the domain separator are generalized from C10.

The sole mutable selector and retry-shared ownership record is canonical JSON,
at most 4 KiB:

```text
ParentChildEntry = {
  schema: 1,
  parent_operation_id: id,
  child_operation_id: id,
  current_attempt_id: id,
  current_task_id: id,
  current_task_identity_digest: digest,
  dispatch_state: "allocated" | "dispatch_visible" | "terminal",
  terminal: TaskTerminal | null,
  checkpoint_digest: digest,
  retry_input_digest: digest,
  retry_shared_owner:
      {kind:"task", attempt_id:id, task_id:id}
    | {kind:"parent_receipt", attempt_id:id, receipt_digest:digest}
}
```

`TaskTerminal` is exactly `completed | failed | aborted | timed_out`.
`current_task_identity_digest` domain-separates and hashes the exact immutable
child identity, including parent and child operation ids, action ordinal,
attempt, Task and assignment ids, attempt generation, workspace and owner,
actor, executor, Task type, target, plan id and hash, deletion authority,
immutable and retry-input digests, and timeout. Mutable Task state is excluded.
Tenant and Project children freeze their owning Tenant workspace; backing
children freeze Platform and the exact backing Project, Environment, and
Service owner. A private successor candidate is at most 64 KiB and contains
the exact canonical pending Task plus its parent, predecessor, identity, and
retry bindings; it is not public or dispatchable.

The immutable attempt receipt is canonical JSON and at most 4 KiB:

```text
TerminalAttemptReceipt = {
  schema: 1,
  parent_operation_id: id,
  child_operation_id: id,
  action_ordinal: int64,
  attempt_id: id,
  task_id: id,
  assignment_id: id,
  attempt_generation: int64,
  terminal: "completed" | "failed" | "aborted" | "timed_out",
  terminal_task_digest: digest,
  result_digest: digest | null,
  error_digest: digest | null,
  checkpoint_digest: digest,
  agent_ack_revision: int64,
  published_at: timestamp
}
```

For `completed`, `result_digest` is non-null and `error_digest` is null; the
inverse holds for every non-success terminal. `receipt_digest` is SHA-256 of
the exact canonical bytes. `receipt_revision` is the immutable attempt key's
creation ModRevision and is never embedded in its own value.

The current pointer is canonical JSON and at most 2 KiB:

```text
ParentTerminalReceiptPointer = {
  schema: 1,
  parent_operation_id: id,
  child_operation_id: id,
  current_attempt_id: id,
  current_receipt_digest: digest,
  previous_attempt_id: id | null,
  previous_receipt_digest: digest | null
}
```

The two `previous_*` fields are both null or both non-null. At most two
immutable receipts exist for a child, and the pointer and child selector must
name the same current attempt.

The immutable consumed progress row is canonical JSON and at most 4 KiB:

```text
ParentChildProgress = {
  schema: 1,
  parent_operation_id: id,
  child_operation_id: id,
  action_ordinal: int64,
  task_id: id,
  assignment_id: id,
  attempt_generation: int64,
  terminal: "completed" | "failed" | "aborted" | "timed_out",
  terminal_task_digest: digest,
  result_digest: digest | null,
  error_digest: digest | null,
  checkpoint_digest: digest,
  receipt_revision: int64,
  receipt_digest: digest,
  consumed_action: "advance" | "retry"
}
```

`advance` is valid only for `completed`; `retry` is valid only for a
non-success terminal. A progress row is written once under an absence compare
and is the sole durable consumption authority. Task completion, mutable child
state, Docker name absence, and a Boolean Agent response are never sufficient
proof.

Every sealed action, including every Controller finalizer, has exactly one
immutable completion row at its encoded ordinal key. The row is canonical JSON
and at most 8 KiB:

```text
DeletionActionCompletionV1 = {
  schema: 1,
  parent_operation_id: id,
  deletion_epoch: int64,
  ordinal: int64,
  action_digest: digest,
  target_kind:
      "tenant" | "project" | "environment" | "service" | "attach"
    | "entry" | "route" | "component" | "script" | "release-group"
    | "release" | "backup-policy" | "key-material" | "materialization"
    | "volume" | "zone" | "network" | "reservation"
    | "recovery-point" | "orphan-object" | "connector" | "runner"
    | "secret" | "backing-service",
  target_id: id,
  target_revision: int64,
  executor: "agent" | "controller",
  proof:
      {
        kind: "agent-terminal-receipt",
        child_operation_id: id,
        attempt_id: id,
        task_id: id,
        assignment_id: id,
        attempt_generation: int64,
        receipt_revision: int64,
        receipt_digest: digest,
        progress_key: string,
        progress_digest: digest,
        terminal_task_digest: digest,
        result_digest: digest,
        checkpoint_digest: digest
      }
    | {
        kind: "controller-cas",
        finalizer: DeletionActionV1.procedure.finalizer,
        fixed_input_revision: int64,
        compare_set_digest: digest,
        mutation_set_digest: digest,
        postcondition_digest: digest
      },
  completed_at: timestamp
}
```

An Agent completion is valid only for a consumed `completed` receipt and an
immutable `ParentChildProgress{consumed_action:"advance"}`. It copies the exact
Task, assignment, attempt, terminal receipt, result, and checkpoint identities.
Receipt consumption retains its exact eight-operation transaction. A separate
completion CAS then performs five compares (exact tombstone at the same next
ordinal, cleanup fence, action row, immutable progress row, and completion-key
absence) plus three puts (completion row, tombstone checkpoint advanced by one,
and cleanup fence with the action cleared), exactly eight operations.

A Controller completion row is written in the same fixed-revision transaction
as that action's exact bounded mutations and the tombstone/fence checkpoint.
Its key's actual creation ModRevision is the commit revision; the value never
predicts its own ModRevision. These three fields are digests of canonical
ordered transaction templates. Fixed operands are encoded literally; dynamic
operands use closed typed slots for action id/digest and ordinal, predecessor
checkpoint/fence generation, commit ModRevision, terminal time, and the
completion-row envelope. `compare_set_digest` covers the compare template,
`mutation_set_digest` covers the mutation template including the typed
completion-row mutation, and `postcondition_digest` covers the typed resulting
absence/presence template. The completion envelope is not recursively expanded
inside its own mutation digest. Execution resolves every slot from the sealed
action and compared predecessor state, then must reproduce the same templates.
Replay reads the row, tombstone, fence, and every surviving effect anchor at
one fixed revision and accepts only exact digests and the same transaction
ModRevision. A missing anchor, changed digest, unrecognized slot, or same
ordinal with different bytes is corruption.

The coordinator may select only `ordinal=completed_count`. It advances
`completed_count` and `next_ordinal` together only when that exact action and
completion row compare successfully. Therefore the entire sealed plan, not
only Agent children, completes in contiguous ordinal order `0..N-1`; there are
no executor-specific ranges or gaps.

The Agent-receipt proof scan traverses every sealed action ordinal from zero to
`plan_count-1`. For each ordinal it compares the exact action row. A Controller
action contributes no receipt and advances the scan cursor. An Agent action
must additionally compare its exact child entry, current pointer, immutable
current completed receipt, and immutable `advance` progress row before that row
contributes to the receipt digest. The scan therefore proves the selected rows
are exactly the Agent-executor subset of the sealed plan rather than inferring
the subset from the child namespace.

The scan cursor is exactly
`/v1/runtime/deletion-receipt-scan-cursors/~<parent-operation-id>/<parent-task-id>`.
Every batch compares its actual ModRevision and exact canonical bytes. The put
is derived byte-for-byte from the compared cursor: identity, Task, epoch, plan
digest, and prior current chain hash are the inputs; `next_action_ordinal`
advances by the number of action rows scanned. For each proved Agent row,
`accumulated_agent_count` increments once and `current_receipt_chain_hash`
advances by the exact `Rnext` recurrence above. A Controller row advances only
`next_action_ordinal`: it does not advance the Agent count or chain hash.
`phase` becomes `ready` if and only if the next ordinal equals `plan_count`.

Each transaction scans at most 16 action rows:

```text
3 fixed compares: exact tombstone + cleanup fence + scan cursor
1 compare/action: exact sealed action row
4 compares/Agent action: child entry + current pointer + immutable receipt
                       + immutable `advance` progress
1 put: next scan cursor with next plan ordinal, Agent count, and chain hash
```

The worst case is 16 Agent actions:
`3 + 16 + 4*16 + 1 = 84` aggregate operations. With action values capped at
16 KiB, the inherited receipt values and fixed records bring the conservative
request bound to 724 KiB, below the independent 900-KiB fence. The cursor's
next ordinal always advances through the full plan, while its Agent count and
chain hash advance only for Agent actions. A missing action, an unexpected child
for a Controller action, a missing child for an Agent action, or an
action/progress ordinal mismatch is corruption.

After the cursor reaches `plan_count`, let `Rfinal` be its exact
`current_receipt_chain_hash` and let `child_count` be its exact
`accumulated_agent_count`. The immutable summary field is exactly:

```text
ordered_receipt_set_digest = SHA-256(
  ASCII("gp-deletion-receipt-summary-v1") || NUL ||
  sealed_plan_sha256 || uint64be(child_count) || Rfinal
)
```

Receipt ordinals must be unique, strictly ascending, and equal the original
`action_ordinal` values of the sealed Agent subset. Gaps are required whenever
Controller actions occur between Agent actions; Agent receipt ordinals are not
renumbered and need not begin at zero. `child_count` is exactly the number of
Agent-executor actions selected by the full-plan scan. A plan with no Agent
actions has `child_count=0`, `Rfinal=R0`, the summary digest produced by the
same final formula, and no child receipt rows. The surviving summary is
canonical JSON and at most 2 KiB:

```text
ParentTerminalReceiptSummary = {
  schema: 1,
  parent_operation_id: id,
  deletion_epoch: int64,
  child_count: int64,
  ordered_receipt_set_digest: digest,
  final_checkpoint_digest: digest,
  completed_at: timestamp
}
```

The summary digest and exact summary key are transferred into the parent
checkpoint before individual receipts are removed. Initial receipt-summary
publication is exactly four compares (exact tombstone, cleanup fence, receipt
cursor at `phase=ready`, and summary-key `ModRevision==0`) plus four mutations
(immutable summary put, tombstone checkpoint put, next cleanup-fence generation
put, and receipt-cursor delete), eight aggregate operations. `child_count` and
`Rfinal` must equal the exact ready cursor's count and chain hash;
`ordered_receipt_set_digest` must equal the exact summary-domain finalization
of that count, hash, and sealed plan. The summary
survives child Task pruning and remains retry authority until the completed
parent Task's ordinary retention removal proves no retry-owned record remains.

A lost publication response is success only when one fixed-revision read finds
the exact immutable summary and its digest, the tombstone checkpoint selecting
that digest, the expected next cleanup-fence generation, and the receipt cursor
absent. Summary absence with the exact still-ready cursor permits the original
CAS retry. Summary existence with a cursor still present, or any other
combination, is corruption. Thus a receipt cursor never outlives successful
receipt-summary publication.

After every sealed ordinal has its exact completion row, a separate bounded
scan constructs the all-action completion digest:

The scan cursor is exactly
`/v1/runtime/deletion-completion-scan-cursors/~<parent-operation-id>/<parent-task-id>`.
Every batch compares its actual ModRevision and exact bytes. It then compares
the exact action and completion rows for the contiguous range beginning at
`next_action_ordinal`. Each completion must carry that action's exact ordinal,
digest, target, epoch, and executor proof. The next cursor is byte-identically
derived: immutable identity and plan fields are copied, next ordinal and
`accumulated_action_count` advance by the same batch count, and
`current_completion_chain_hash` advances by the exact `Cnext` recurrence for
every completion in order. `phase` becomes `ready` only at `plan_count`.

```text
ordered_completion_set_digest = SHA-256(
  ASCII("gp-deletion-completion-summary-v1") || NUL ||
  sealed_plan_sha256 || uint64be(plan_count) || Cfinal
)
```

Here `Cfinal` is the ready cursor's exact `current_completion_chain_hash` and
`plan_count` is its exact `accumulated_action_count`. Because this scan includes
every sealed action, it applies `Cnext` exactly once at each contiguous original
ordinal `0..plan_count-1`; unlike the Agent subset, it never skips an action.

The scan processes at most 28 actions per transaction. It performs three fixed
compares (exact tombstone, cleanup fence, and completion cursor), two compares
per action (exact action and completion), and one cursor put:
`3 + 2*28 + 1 = 60` aggregate operations. With 28 action values at 16 KiB,
28 completion values at 8 KiB, fixed/cursor values, and conservative 1-KiB key
and wrapper allowances for all 60 operations, the request is at most 868 KiB.
It rejects a missing, duplicate, out-of-order, wrong-action, or wrong-epoch
row. Its terminal transaction writes canonical JSON at most 4 KiB:

```text
DeletionCompletionSummaryV1 = {
  schema: 1,
  parent_operation_id: id,
  deletion_epoch: int64,
  plan_count: int64,
  plan_digest: digest,
  ordered_completion_set_digest: digest,
  agent_receipt_summary_digest: digest,
  final_checkpoint_digest: digest,
  completed_at: timestamp,
  retain_until: timestamp
}
```

`retain_until` is exactly the final parent Task's ADR 0035 retention timestamp:
90 days after terminal completion. Summary publication, the final parent Task,
the retained tombstone, and replay locator all compare and bind that same
timestamp.

Initial all-action summary publication is exactly four compares (exact
tombstone, cleanup fence, completion cursor at `phase=ready`, and summary-key
`ModRevision==0`) plus four mutations (immutable completion-summary put,
tombstone checkpoint put, next cleanup-fence generation put, and completion-
cursor delete), eight aggregate operations. `plan_count`, `plan_digest`, and
`Cfinal` must equal the exact ready cursor and sealed plan;
`ordered_completion_set_digest` must equal the exact completion-summary-domain
finalization of the ready count, hash, and sealed plan. The exact immutable
receipt summary and tombstone-bound receipt-summary digest are always mandatory,
including its `child_count=0`, `Rfinal=R0` form when the plan has no Agent
actions. `agent_receipt_summary_digest` is never null.

A lost response replays only when one fixed-revision read finds the exact
summary and digest, tombstone selection, expected fence generation, and absent
completion cursor. Summary absence with the exact ready cursor permits the
same CAS retry. Summary plus cursor, or any unequal binding, is corruption. A
completion cursor therefore never outlives successful all-action summary
publication.

The shared engine preserves ADR 0037's receipt publication, consumption,
successor, predecessor-prune, and final-removal budgets. Its subset scan and
both summary-publication budgets are replaced by the cursor-backed generic
transactions in this table:

| Operation | Exact aggregate operations | Conservative request bound |
| --- | ---: | ---: |
| two-cursor version-zero initialization | `4 compares + 4 puts = 8` | 180 KiB |
| terminal receipt publication | `6 compares + 3 puts = 9` | 28 KiB |
| receipt consumption | `6 compares + 2 puts = 8` | 162 KiB |
| successor allocation | `5 compares + 3 puts = 8` | 85 KiB |
| 16-action Agent-subset proof scan | worst case `3 + 16 + 4*16 + 1 = 84` | 724 KiB |
| 13-child predecessor pruning | `3 + 4*13 + 3*13 + 1 = 95` | 474 KiB |
| Agent receipt-summary publication and cursor removal | `4 compares + 4 mutations = 8` | 180 KiB |
| 28-action all-completion scan | `3 + 2*28 + 1 = 60` | 868 KiB |
| all-action completion-summary publication and cursor removal | `4 compares + 4 mutations = 8` | 180 KiB |
| 13-child final receipt removal | `3 + 4*13 + 3*13 + 1 = 95` | 448 KiB |

Every count is independently below 96 operations and every conservative byte
bound is below both the enforced 900-KiB fence and etcd's 1-MiB request limit.
Publication, consumption, successor allocation, proof, predecessor pruning,
summary publication, and final removal retain ADR 0037's exact compare-before-
transfer, immutable-row, unknown-outcome, replay, and conflict behavior.

The receipt table applies only to Agent child receipt ownership. It does not
remove completion rows after summary publication. Its 13-child final-receipt
removal transaction is deferred to the post-retention receipt phase; no receipt
or progress row is removed immediately after summary publication. The current-
child identity separator is exactly
`gp-deletion-current-child-task-v1\0`. The receipt and completion scans and
summaries use only the eight exact init/row/chain/summary domains defined above;
the former `gp-deletion-parent-receipts-v1\0`, former
`gp-deletion-completions-v1\0`, C10 separators, and `/c10/...` keys are invalid
for operations governed by this ADR.

### 7. Retained final state and bounded post-retention pruning

All destructive descendant work occurs before resource finalization as sealed,
bounded actions. The final resource CAS is capped at 64 aggregate operations
and 900 KiB. It compares the exact target/facade revision, tombstone, cleanup
fence, lock, final action and completion, receipt summary, completion summary,
current parent Task, and protected idempotency evidence. The receipt-summary
compare is a reserved member of the 64-operation budget for every plan,
including `child_count=0`. It removes only the bounded target
primary, exact owner/slug/coordination/facade indexes and lock, and it writes
the completed parent Task, terminal idempotency evidence, retained tombstone,
retained cleanup fence, and replay locator. Any facade with more final indexes
than fit this bound must seal earlier index-removal actions while retaining the
primary, slug, and visibility indexes to this CAS.

For the exact 90-day period ending at `retain_until`, the final live deletion
key set is:

```text
tombstone
immutable intent
every immutable action row
every immutable action-completion row
every ParentChildEntry
every immutable attempt receipt and current pointer
every immutable child-progress row
Agent receipt summary, including the child_count=0 empty-subset summary
all-action completion summary
retained cleanup fence
replay locator
completed parent Task, history, events, and ADR 0035 retention indexes
terminal protected idempotency evidence until its ordinary earlier pruning
```

No deletion action, completion, receipt, progress, summary, tombstone, intent,
cleanup fence, or replay locator is removed by resource finalization. This
state is immutable except for bounded ADR 0035 Task/idempotency pruning and the
deletion pruner below.

A corresponding summary and scan cursor are mutually exclusive. The successful
final set contains both immutable summaries and neither cursor. Before its
summary transaction, an incomplete cursor exists instead of that summary and
remains retry evidence across parent attempts.

The daily maintenance loop may create a deletion prune intent only after
`retain_until`, after ADR 0035 has removed the parent Task and its retention,
history, event, dedupe, and idempotency records, and while the exact retained
tombstone, fence, replay locator, receipt summary, completion summary, and
deletion epoch match. Receipt-summary presence and exact digest equality are
mandatory even when `child_count=0`. The prune intent is canonical JSON at most
4 KiB:

```text
DeletionRetentionPruneIntentV1 = {
  schema: 1,
  parent_operation_id: id,
  deletion_epoch: int64,
  retain_until: timestamp,
  phase:
      "actions" | "completions" | "children" | "receipts"
    | "progress" | "receipt-scan-cursor"
    | "completion-scan-cursor" | "final",
  cursor: string | null,
  remaining_count: int64,
  expected_completion_summary_digest: digest
}
```

Action, completion, child-entry, and successor-candidate drain transactions
compare the prune intent and retained tombstone, compare exact records, delete
them, and write the next intent. `2 + 47 + 47 + 1 = 97` would exceed the
ceiling, so each such batch contains at most 46 records:
`2 + 46 + 46 + 1 = 95` operations. Receipt/progress cleanup instead uses the
inherited exact 13-child predecessor and final-receipt transactions, each 95
operations, now gated by the retained tombstone and prune intent rather than an
executable parent fence. The final-receipt transaction always compares the
exact receipt summary and deletes it in its fixed `+1` mutation, but only after
the batches prove every selected receipt/progress row transferred to the
all-action completion summary. With `child_count=0`, the same phase proves the
child/receipt/progress prefixes empty and specializes to `3 compares + 1
receipt-summary delete = 4` operations. The 13-child worst case remains
`3 + 4*13 + 3*13 + 1 = 95`; no table bound changes. Every builder separately
enforces 900 KiB and advances phases only after an exact empty-prefix check. No
prefix delete is permitted.

A cursor never enters retained state after successful summary publication,
because its summary CAS deletes it atomically. Before summary publication an
incomplete cursor is operation retry evidence and neither Task-attempt pruning
nor ordinary cleanup may remove it. The post-retention pruner is the only
alternate removal authority after the parent retention boundary. Each cursor
phase performs three compares (exact prune intent, retained tombstone, and
exact cursor bytes/ModRevision, or exact cursor absence) and two mutations
(cursor delete when present and next-intent put), at most five operations. A
present cursor must match the exact operation, the parent Task identity retained
in the tombstone and replay locator, the deletion epoch, and the sealed plan;
mismatch is corruption.

The final prune CAS uses thirteen compares: exact prune intent, tombstone,
cleanup fence, immutable intent, completion summary, replay locator, and empty
action, completion, child/receipt, progress, successor, receipt-scan-cursor,
and completion-scan-cursor prefixes. It performs six deletes: prune intent,
tombstone, cleanup fence, immutable intent, completion summary, and replay
locator. The exact receipt summary must already have been compared and removed
by the mandatory receipt phase, including for an empty Agent subset. Thus the
final prune is exactly 19 aggregate
operations and leaves no key for the deletion operation. An unknown outcome is
resolved from absence of all six final keys plus the still-absent drained
prefixes; partial absence is corruption. Stable ids remain non-reusable even
after pruning by the global stable-id allocation rule.

### 8. Backing Service permanent deletion

Permanent backing deletion is reachable only through the Backing Service
facade. Before dispatch, every human surface exhausts 47-row pages from:

```text
GET /backing-services/{project_id}/deletion-impact?cursor=<opaque>
```

The final-page token binds the backing Project, adapter Service, immutable
dependency-head revision, row count, ordered row-set digest,
Platform/operator capability, and a ten-minute expiry. It is emitted only on
the final page. A compacted fixed revision returns `cursor.expired` with HTTP
409. Malformed or incorrectly bound cursors remain `cursor.invalid` with HTTP
400. The client restarts the preview after expiry.

DELETE requires exactly:

```json
{
  "impact_token": "opaque-token",
  "confirmation": "<current-project-slug>"
}
```

The publication transaction compares the exact current facade, Project slug,
adapter Service, dependency head, count, and digest frozen by the token. A
rename or dependency change is `state.conflict` before mutation.

Incoming Attach grants are ordered by strongly connected component
condensation, with grant-owner components before target components and the
ascending minimum stable Attach id as the tie-break. Within a cyclic component,
internal grants are revoked in stable `(owner_attach_id,target_attach_id)`
order before member Attaches are detached in ascending stable-id order. A
valid cycle is not corruption; dangling or cross-backing edges are.

The backing cascade applies these exact rules:

- Enabled consumer Backup Policies selecting an affected Attach block with
  `resource.in_use`.
- A disabled policy does not block. Its source remains unresolved and
  non-restorable and cannot later be enabled until edited.
- Historical consumer Recovery Points and Connector reverse references remain
  consumer-owned and are neither removed nor rewritten.
- Another Backing Service using a Zone owned by the deleting Project blocks.
- An externally owned selected Zone is retained.
- A Zone owned by the deleting Project is removed only after every binding is
  proven absent.
- Cross-resource dependencies otherwise block rather than being silently
  rewritten.

Before the first Detach, the Agent revalidates or reconstructs the exact pinned
backing runtime under deletion-only authority. It proves adapter/runtime major,
trusted image or release identity, Network, Volumes, materializations,
generation, and plan hash. It never publishes ordinary Start intent or Attach
readiness and never deprovisions against stale, absent, drifted, arbitrary, or
major-mismatched runtime.

The facade plan then revokes grants and detaches incoming Attaches, drains
active work, stops the Service, removes containers, preserves or removes the
selected Zone according to ownership, completes ADR 0049 Volume removals,
removes generated identities and materializations, removes the Environment
root and owned Network, releases reservations, and finalizes subordinate
records. Recovery Points and Connectors are not children. The backing Project,
`main` Environment, adapter Service, facade indexes, Project slug, tombstone,
and reservations remain until the final facade CAS. That CAS removes the
bounded facade records and reservations, completes the parent Task, and moves
the tombstone and cleanup fence to `retained`; the separate 90-day pruner
removes deletion evidence later.

### 9. Retry, abort, timeout, restart, and corruption

Each parent attempt has a six-hour deadline. The immutable intent retains
`timeout=6h`; each attempt freezes its absolute deadline in the Task and
tombstone. A child is not dispatched unless its complete frozen child timeout
fits in the remaining parent deadline.

Reaching the deadline closes dispatch durably before reporting timeout. The
Controller CASes the exact cleanup fence from `dispatch=open` to
`dispatch=retiring`, increments its generation, and publishes no later action
or child successor. If no action is active, it may terminalize the parent
attempt as retryable `timed_out` in the next bounded CAS. If a Controller
finalizer CAS has an unknown outcome, it first resolves that exact transaction;
it never runs a replacement effect merely because the deadline passed.

If an Agent child is active, the parent writes the existing generation-fenced
Task abort request and durable assignment retirement intent, subscribes before
delivery, and waits for terminal acknowledgement. The child remains the sole
owner of its operation and external effect during that wait. Agent loss or
Controller restart resumes the same assignment/retirement observation. The
parent cannot terminalize `timed_out`, expose Retry, clear the active child, or
allocate a successor until it has published and consumed that child's exact
terminal receipt. A completed receipt advances normally; `failed`, `aborted`,
or `timed_out` is consumed only as `retry` evidence and authorizes no progress.
This wait may finish after the six-hour dispatch deadline; the deadline bounds
new work, not unsafe abandonment of an external effect.

`failed`, `timed_out`, and `aborted` parent attempts are retryable only through
the existing Task Retry surface. Retry creates a new Task id and six-hour
attempt deadline while preserving the same deletion operation id, target,
plan, action identities, immutable intent, timeout, completed checkpoints,
receipts, and remaining resources. It atomically transfers `current_task_id`
and `deletion_task_id`. Retry never removes or reacquires the tombstone, never
resnapshots descendants, and never repeats a completed action.

A non-success child receipt never authorizes destructive progress. Parent
retry allocates exactly one successor for the same child operation, and there
is no parallel successor. On parent Retry, the Controller first observes the
exact child selector, current pointer, durable Agent assignment, retirement
intent, and immutable receipt at one fixed revision. An active assignment is
adopted and observed, never superseded. A completed receipt is completed into
the same action without redispatch. Only a consumed non-success receipt with no
active assignment permits the one exact successor allocation. Abort follows
the same retirement barrier: it stops dispatch for only the current attempt,
propagates to an active Agent child, waits for durable terminal acknowledgement
and receipt, and does not cancel or roll back the deletion intent.

Failure, abort, timeout, Agent loss, Controller restart, and final-CAS
corruption retain the tombstone, deletion lock, slug indexes, immutable intent,
action plan, receipts, checkpoints, replay locator, and remaining resources.
Controller shutdown cancellation leaves a durable running claim recoverable as
required by [Task Abort contract](../features/tasks-and-logs.md#abort). Agent child recovery uses the singleton Agent's durable
assignment and generation; there is no placement choice, failover, or
multi-Agent reassignment.

A final CAS conflict fails closed. If all evidence is unchanged, the same
operation may retry. Missing or contradictory immutable evidence is Internal,
retains the aggregate fence, and never reopens a partially deleted aggregate.
Successful resource finalization removes the operation lock but retains the
completed tombstone and proof set to `retain_until`; only the bounded deletion
pruner removes those retained records.

### 10. Controller and Agent authority

| Work | Authority |
| --- | --- |
| parent planning, fencing, receipts, checkpoints, bounded record deletion | Controller parent Task |
| Environment, Compose, container, materialization, Volume, and Network cleanup | singleton Agent child Task |
| Attach revoke, deprovision, and Network-union apply | Agent child Task |
| Runner daemon, container, account, subid, and local-state removal | native Controller typed finalizer; never workload Agent |
| Secret, ciphertext, and index finalization after consumers stop | Controller typed finalizer |
| Project and Tenant primary/index finalization | Controller parent Task |
| backing runtime reconstruction and host-absence proof | Agent child Task |

The parent never treats Task completion, Docker-name absence, or a Boolean
Agent report as cleanup proof. Typed acknowledgements, immutable receipts, and
resource-specific absence evidence are mandatory.

### 11. Exact API, CLI, and Console parity

The REST surface is exactly:

```text
DELETE /tenants/{id}
  operation id: tenant.delete
  body: forbidden
  response: 202 {"task_id":"..."}

DELETE /projects/{id}
  operation id: project.delete
  body: forbidden
  response: 202 {"task_id":"..."}
  accepted target: kind=tenant only

DELETE /environments/{id}
  operation id: environment.delete
  body: forbidden
  response: 202 {"task_id":"..."}

GET /backing-services/{project_id}/deletion-impact?cursor=...
  operation id: backing-service.deletion-impact

POST /backing-services/{project_id}/destroy
  operation id: backing-service.destroy
  body: exact impact_token and confirmation object
  response: 202 {"task_id":"..."}
```

Every mutation requires `Idempotency-Key`. The current `api-cli.md` command
tree uses `delete` for Tenant, Project, and Environment aggregate deletion;
Backing Service retains its separately owned lifecycle verb:

```text
groundplane tenant delete <slug> [--id]
groundplane project delete <slug> [--id]
groundplane environment delete <slug> [--id]
groundplane backing-service destroy <slug> [--id]
```

`remove` remains the Task type and the CRUD verb for other nouns; it is not
substituted for these current aggregate command names. The Backing Service CLI
exhausts every impact page, displays every effect and blocker, obtains the
final token, confirms the current Project slug, and dispatches destroy. Tenant,
Project, and Environment commands state that Backing Services survive. Runner
warnings state that GitHub registration survives local removal.

The Console contract is:

- Tenant and Project danger zones dispatch the generated client, receive the
  authoritative Task, and poll or watch it.
- Permanent deletion uses Task type `remove`, never `destroy`.
- Fixture records are not removed, Activity is not fabricated, and navigation
  is not timer-driven.
- A resource remains visible and disabled while deletion exists. Navigation
  occurs only after terminal success followed by authoritative not-found.
- Failed, aborted, and timed-out attempts expose Retry and explain that the
  aggregate remains fenced.
- Backing detail exposes distinct **Destroy runtime** and **Delete
  permanently** actions. Permanent deletion loads the complete authoritative
  impact preview and requires the current Project slug.
- Tenant copy says Backing Services survive. Project and Tenant Runner copy
  says GitHub cleanup remains manual.

### 12. Errors

The public contract uses existing RFC 7807 problem vocabulary:

- `validation.failed`/422 for a generic Project DELETE targeting a backing
  Project, malformed strict bodies, or invalid confirmation;
- `backing_service.not_found`/404 when the facade path names an ordinary or
  absent backing Project;
- `cursor.invalid`/400 for malformed or incorrectly bound cursors;
- `cursor.expired`/409 for a valid impact cursor whose fixed revision is no
  longer readable;
- `state.conflict`/409 for changed revision-bound evidence or active separately
  owned work;
- `resource.in_use`/409 for a dependency that the defined cascade cannot own;
- existing protected idempotency problems for mismatched or unresolved
  requests; and
- `internal` for corrupt immutable evidence, invalid grant edges, impossible
  receipt state, or contradictory finalization evidence.

Errors never expose Secret values, host paths, Connector credentials, private
object locators, child Task params, or raw helper diagnostics.

### 13. Required future mirrors before implementation

Acceptance of this ADR requires one synchronized contract slice. This Proposed
ADR deliberately does not edit or claim any of these mirrors yet:

- `docs/mvp.md`: make the deletion behavior, containment, visibility,
  six-hour retry semantics, GitHub-registration preservation, backing survival,
  and facade-only backing deletion authoritative product contract.
- `docs/api-cli.md`: close the duplicate backing Project route, add the backing
  impact and permanent-delete grammar to the command tree and endpoint table,
  add `deletion_task_id`, and synchronize exact operation ids, statuses,
  request bodies, errors, warnings, and idempotency.
- `docs/architecture.md`: place the shared aggregate coordinator, hierarchy
  epoch authority, immutable plan, receipt engine, Controller finalizers, and
  singleton-Agent child boundary in the implementation contract.
- `docs/capabilities.md`: update the C02/C03 hierarchy, C10 Backing Service,
  C17 Runner, and C19 Task ledger entries and their vertical-delivery evidence
  without claiming them implemented or accepted prematurely.
- Console store, public types, fixtures, Tenant and Project danger zones,
  Backing Service detail/impact flow, generated-client calls, Task polling,
  Retry state, and tests: remove immediate fixture mutation and manufactured
  Activity while keeping Destroy distinct from permanent Delete.
- Controller domain/application code: typed intents, tombstones, coordination
  epochs, stable postorder planning, finalizers, retry/abort/recovery, and
  facade validation.
- Persistence code: canonical schemas and key builders, fixed-revision scans,
  monotonic cleanup fences, contiguous action completion, all exact
  compare-and-mutate budgets, version-zero receipt/completion scan cursors,
  receipt proof/summary, 90-day bounded deletion pruning, replay, cleanup, and
  corruption handling.
- Public API/OpenAPI and generated Go/TypeScript clients: exact schemas,
  operation ids, response statuses, idempotency, cursor errors, kind rejection,
  and nullable `deletion_task_id`; generated artifacts must be regenerated,
  never hand-edited.
- CLI grammar and handlers: exact `delete` commands, slug/`--id` resolution,
  complete backing impact pagination and confirmation, survival warnings, and
  authoritative Task output.
- Agent protocol, typed procedures, and adapters: Environment and backing
  cleanup, generation-fenced child recovery, typed absence proof, Attach/Zone/
  Volume handling, and no Runner cleanup through the workload Agent.
- Contract, transaction-boundary, persistence, API, CLI, Console, restart,
  failure-injection, and real-host acceptance tests described below.

No layer-only implementation may claim this proposal complete. The 1:1 rule
must hold after the synchronized replacement.

## Consequences if accepted

- Partially deleted aggregates never reopen. Operators retry the same immutable
  operation until parent-last finalization succeeds.
- Project Secrets and Tenant/Project Runners have explicit containment and
  finalizer authority. GitHub Runner registration remains deliberately outside
  Groundplane ownership.
- Operators can observe the current deletion attempt without treating Task
  terminal failure as resource availability.
- Backing Projects have one permanent-delete capability through one facade.
- One receipt engine provides bounded, restart-safe child consumption for all
  three aggregate deletions.
- Tombstones, slugs, action plans, and receipts may remain indefinitely after
  unrecoverable corruption. This is intentional fail-closed behavior and
  requires operator diagnosis rather than automatic reopening.
- Six-hour attempts bound active work but do not bound the lifetime of the
  deletion intent; large aggregates may require multiple retries.
- The design adds coordination records, immutable action rows, receipts,
  progress rows, and summary cleanup that must be maintained and tested under
  the independent etcd operation and byte limits.

## Acceptance evidence

Before this ADR can become implementation authority, the synchronized slice
must prove:

1. OpenAPI golden tests cover exact operation ids, bodies, success statuses,
   idempotency, kind rejection, and RFC 7807 codes.
2. Generated Go and TypeScript clients and CLI grammar have exact parity.
3. Console tests prove no fixture deletion, no fabricated Task or Activity,
   `remove` versus `destroy`, authoritative polling, persistent deleting state,
   Retry, and complete backing impact pagination.
4. Atomic publication includes Task, tombstone, intent, lock, replay locator,
   and protected idempotency evidence under unknown outcomes.
5. Ancestor-fence races cover every descendant create, mutation, operation
   publication, and terminalization.
6. Fixed-revision active-operation scans and coordination-epoch comparisons
   close every publication race.
7. Mixed child kinds produce exact raw-stable-id postorder and parent-last
   finalization.
8. Environment, Runner, and Secret finalizers behave identically under Project
   and Tenant parents; Runner tests prove GitHub registration is preserved.
9. Tenant and ordinary Project deletion preserve every Backing Service and
   Platform Secret.
10. Backing tests cover SCC grant ordering, enabled-policy blockers,
    disabled-source retention, Recovery Point and Connector retention,
    shared/external/owned Zone behavior, and facade-only deletion.
11. Crash and restart injection covers every cleanup-fence generation and
    tombstone phase, action batch, Agent and Controller completion proof, child
    terminal, receipt publication and consumption, both summaries, every
    90-day prune phase, and both resource-finalization and final-prune CASes.
12. Abort and timeout retain tombstone and immutable plan, and retry neither
    resnapshots nor duplicates an external effect.
13. Slugs remain reserved until successful finalization and are reusable only
    after the same transaction makes the resource not found.
14. Builders prove the exact 47-action/96-operation and 900-KiB boundaries;
    8-operation cursor initialization; 84-operation/724-KiB Agent-subset scan;
    60-operation/868-KiB all-action scan; both 8-operation cursor-consuming
    summary publications; 19-operation final prune; and every retained
    receipt-engine bound. Tests inject lost responses before and after each
    cursor CAS and accept only exact next bytes or exact summary-plus-cursor-
    absence replay evidence. Fixed test vectors cover all eight literal domains,
    `R0`, `C0`, row digests, original-ordinal big-endian encoding, every chain
    step, empty Agent subsets, Controller gaps in Agent subsets, and both final
    summary digests. Restart tests resume at every batch boundary and compare
    exact cursor bytes; independent readers recompute count and hash in one pass
    from raw canonical rows. Tests reject count-first hashing, concatenated-raw-
    row formulas, textual digest bytes in hash inputs, and serialized SHA-256
    internal-state continuation. Empty-subset tests also prove both cursors,
    scans, and summary publications run; the completion summary carries a
    non-null receipt-summary digest; the resource-finalization CAS compares both
    summaries; the four-operation receipt phase removes the empty receipt
    summary; and the later 19-operation final prune remains unchanged.
15. Singleton-Agent tests prove Agent-only workload effects,
    Controller-only Runner cleanup, no placement inference, and
    generation-fenced redelivery.
16. The successful resource-finalization transaction atomically proves resource
    disappearance, index release, final completion, retained-tombstone
    publication, Task completion, and terminal idempotency evidence; the later
    19-operation prune CAS proves the retained deletion key set, including both
    cursor prefixes, is removed.

## Rejected alternatives

### Remove the tombstone when an attempt fails

Rejected because completed child effects cannot be reconstructed or rolled
back safely. The aggregate would reopen in a permanently partial state.

### Treat Project Secrets or scoped Runners as external references

Rejected because they are ownership-indexed descendants and the MVP Console
already presents them as contained. Their specialized finalizers make the
ownership boundary explicit without deleting GitHub state.

### Cascade Tenant deletion into Platform Backing Services

Rejected because Backing Services are Platform-owned shared infrastructure,
not Tenant descendants.

### Dispatch one Controller Project Task per Tenant child

Rejected because the native Controller executor is serial and a waiting Tenant
parent would deadlock its nested Project Tasks. The parent uses one flattened
plan.

### Permit generic Project DELETE for backing Projects

Rejected because it duplicates the facade capability and bypasses the impact
preview and backing-specific dependency authority.

### Resnapshot children on retry

Rejected because completed effects and newly inferred child identities would
make replay non-deterministic and could duplicate external mutations.

### Infer cleanup from Task success or host names

Rejected because Tasks can be pruned, names can drift or collide, and neither
proves typed resource absence under the frozen operation authority.

### Use unbounded recursive transactions

Rejected because etcd enforces independent operation-count and request-byte
limits. Immutable action batches and receipt cursors make progress bounded and
replayable.

## Out of scope

This proposal covers the single-host MVP only. It adds no multi-Agent
placement, Agent failover, multi-host scheduling, high availability, tenant
authorization, RBAC, approval workflow, retention policy, migration or
compatibility layer, bulk-delete API, undo, soft delete, enterprise audit
export, or GitHub registration management. It makes no implementation or MVP
completion claim.
