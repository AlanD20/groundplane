# Resource deletion

- Decision status: Accepted for Volume, Environment, ordinary Project and
  Tenant deletion
- Explicit limit: permanent Backing Service deletion is deferred outside the
  current MVP

## Volume identity and removal

A Volume has three distinct identities: an immutable stable id, a renamable
Environment-scoped slug and an immutable authored storage key. The key owns the
Compose reference and the directory leaf; the stable id owns durable
references. Changing the slug moves no data and rewrites no consumer. The
physical path is derived from the Environment directory and key, never accepted
as an operator-authored path.

Volume desired state is published only through the Environment desired head.
Creating, active, failed-create and deleting states are runtime/task
projections, not desired authority. Create never adopts an unexpected existing
leaf; task-owned filesystem evidence is required across crashes.

Permanent removal requires a complete fixed-revision impact preview. Only the
final page produces the token used by DELETE, and the operator confirms the
immutable key rather than the mutable slug. The token binds desired consumers,
Backup-policy effects, historical consequences and the replacement desired
revision. A changed consequence invalidates it. There is no force shortcut or
empty-directory exception.

Removal first publishes a desired revision without the Volume and its mounts,
then proves all consumers detached before data destruction. Filesystem traversal
is descriptor-relative and split into bounded, durably checkpointed calls. A
failed attempt retains its cursor and Environment removal lock; Retry appends a
new Task attempt for the same operation and never rebases or repeats completed
work.

Removing a current Backup source does not delete historical Recovery Points or
their Connector authority. If no active source remains, scheduling is disabled
while policy configuration and historical fences remain.

## Aggregate hierarchy deletion

Tenant, ordinary Project and Environment deletion use one durable Controller
coordinator. Initial publication atomically creates the parent Task, visible
tombstone, immutable intent, cleanup fence, lock, replay authority and
idempotency result. The resource remains readable with its current deletion
Task while all conflicting mutation and descendant creation are fenced.

The coordinator freezes descendant membership at one revision and seals one
stable-id postorder action plan. Retry never resnapshots. Tenant plans are
flattened rather than dispatching nested Controller Project Tasks, which would
deadlock the serial native executor. Agent-owned host effects use child Tasks;
Controller-owned record, Secret, Runner and parent finalizers are checkpointed
actions in the same plan.

Every action produces typed immutable completion evidence. Agent work additionally
uses durable child receipts, so Task status or a missing Docker name alone is
not proof of cleanup. Bounded scans summarize the complete plan before the
parent-last final transaction removes the resource and releases its slug.

Failure, abort, timeout, restart or contradictory evidence retains the
tombstone, lock, plan, receipts and already completed checkpoints. A six-hour
attempt stops new dispatch but does not abandon an active external effect. Task
Retry transfers the same operation to a new attempt. A partially deleted
aggregate never reopens.

Successful finalization retains the tombstone and proof set through the final
parent Task's 90-day retention boundary. A separate bounded pruner removes that
evidence only after the Task and idempotency records are gone. Stable ids remain
non-reusable.

Project and Tenant deletion include their owned Environments, local Runner
state and scoped Secrets. Removing a Runner does not deregister it at GitHub.
Platform Backing Services and Platform Secrets are not Tenant descendants and
survive.

## Permanent Backing deletion remains deferred

Backing Service Destroy removes runtime only and retains configuration, data
and the facade hierarchy. Generic Project DELETE rejects backing Projects. No
current API, CLI or Console action permanently deletes a Backing Service.

Any future capability requires a distinct facade Delete with a complete impact
preview and confirmation. It must preserve the retained dependency rules:
incoming Attach grants and credentials are ordered safely, enabled consumer
Backup Policies may block, historical consumer Recovery Points and Connector
references remain consumer-owned, shared or external Zones are not silently
deleted, and backing runtime must be reconstructed and proved before adapter
deprovision. These constraints do not reserve current wire fields or authorize
implementation.

## Consequences

- Destructive work is explicit, observable and resumable, but a failed or
  corrupt operation may leave an aggregate fenced for operator diagnosis.
- Desired publication, runtime cleanup and final record removal have separate
  checkpoints; absence of one record cannot stand in for another.
- Large trees are handled by bounded transactions and helper calls, never
  recursive unbounded deletion.

## Source navigation

Volume behavior is owned by
[`internal/controller/volume`](../../internal/controller/volume/removal_publication.go)
and its durable evidence by
[`internal/infra/volumeremovalrecord`](../../internal/infra/volumeremovalrecord/records.go).
Aggregate planning, execution and finalization are separated into
[`hierarchydeletionplanning`](../../internal/infra/etcd/hierarchydeletionplanning/planner.go),
[`hierarchydeletionexecution`](../../internal/infra/etcd/hierarchydeletionexecution/executor.go)
and
[`hierarchydeletionfinalization`](../../internal/infra/etcd/hierarchydeletionfinalization/preparer.go).
