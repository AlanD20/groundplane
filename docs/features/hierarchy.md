# Tenants, Projects and Environments

## Purpose and scope

Group applications and their resources under stable ownership while allowing
human labels to change. Tenant Projects belong to a Tenant; backing Projects
belong to the Platform. An Environment belongs to its Project and owns desired
state, network allocation, storage and its resource operations.

## Functional requirements

- Stable ids are references; slugs are renamable labels with scoped uniqueness.
  Tenant slugs are global, Project slugs are scoped to their Tenant, and
  Environment labels to their Project. Reject invalid slugs; do not normalize them.
- Display-name editing and slug renaming are separate operations. Rename keeps
  every other field, descendant owner, physical path, desired input and runtime
  identity. It never scans and rewrites descendants or retains a compatibility alias.
- Human paths are read projections of current labels. CLI resolves labels to
  ids; API resource paths use stable ids. Concurrent changes cannot overwrite
  unrelated fields or commit after deletion begins.
- Environment creation owns its stable-id storage-root lifecycle. A label change
  never moves data. Tenant and backing Environments use the same bounded
  directory/Volume authority rather than caller-selected host paths.
- Aggregate deletion is a durable Controller coordination Task. It fences the
  hierarchy, processes contained descendants in deterministic bounded order and
  finalizes the parent last. Separately owned active work is not silently adopted
  or cancelled. Agent child Tasks own workload effects; native finalizers remain
  steps of the parent operation.
- A failed, aborted or timed-out aggregate attempt remains visible and fenced
  with its `deletion_task_id`; Retry advances the attempt, not the operation's
  immutable identity. Already-finalized descendants remain absent. Stable ids
  are never reused, and labels remain reserved until successful final publication.

The exact resource model and public actions are in [mvp.md](../mvp.md) and
[api-cli.md](../api-cli.md). Backing facade operations are a separate feature,
not permission to add a second generic Project-delete route.

## Non-functional requirements

Environment network reservations come from the bootstrap `environment_pool`,
disjoint from `system_pool` and `runner.network_pool`. Each Environment reserves
one globally unique child pool; only its Zone child subnets become Docker bridges.
Pool replacement must contain existing Zones and avoid every other reservation.
Zone settings are immutable and reservations remain fenced until physical cleanup.
Platform-generated networks are not Zone CRUD resources.

Rename is constant-sized regardless of descendants and uses bounded,
context-aware CAS retries. Deletion is restart-safe and bounded by the shared
record/transaction limits. Preserve immutable cleanup intent, receipts,
ownership and fences through partial failure. No parent may reopen for writes
after destructive descendant work merely because its current attempt failed.

## Technical design

| Concern | Current technical contract |
| --- | --- |
| Stable lookup, slug grammar, exactly three CAS attempts and rename fences | [Hierarchy identity](../decisions/0026-hierarchy-lookup-and-rename-transaction-ownership.md) |
| Environment paths and task-owned directory creation/removal | [Environment storage lifecycle](../decisions/0025-environment-volume-root-and-directory-lifecycle.md) |
| Environment/Zone allocation and immutable network settings | [Environment IPAM](../decisions/0033-environment-ipam-and-runner-isolation.md) |
| Parent/child receipts, six-hour attempts, retry, postorder finalization and retention | [Aggregate deletion](../decisions/0053-durable-hierarchy-and-backing-facade-deletion.md) |
| Record/index encoding and bounded fixed-revision reads | [Persistence layout](../decisions/0013-etcd-record-and-index-layout.md) |

These are task-routed technical contracts; storage mechanisms do not authorize
deleting a host, production data or anything outside the user's requested scope.

## Acceptance

Prove slug boundaries and immediate old-label failure; no-op rename without
writes; preservation of owner, display name and descendants; exact retry
exhaustion and cancellation; and deletion-start races. Prove aggregate ordering,
partial failure, original operation replay, child receipts, restart and parent-last
finalization without releasing active descendants or weakening transaction bounds.

## Current status

Tenant/Project lifecycle and aggregate deletion have recorded qualification.
Environment qualification remains coupled to its Service, Route, Script,
Blueprint and recovery paths. See [capabilities.md](../capabilities.md) and
[runtime qualification](../issues/runtime-qualification.md); dated evidence is
not current host state or authorization for another destructive operation.
