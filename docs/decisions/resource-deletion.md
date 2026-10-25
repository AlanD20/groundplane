# Resource deletion

- Decision status: Accepted for Volume, Environment, Project, Tenant and
  unused Backing Service deletion

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
new execution attempt in the same Task and never rebases or repeats completed
work.

Removing a current Backup source does not delete historical Recovery Points or
their Connector authority. If no active source remains, scheduling is disabled
while policy configuration and historical fences remain.

## Blueprint declarations without host effects

An omitted Service may have an admitted definition but no compiled runtime. Its
removal cannot require a fabricated Compose artifact or treat a missing runtime
key as proof of absence. The coordinator settles this case in the Controller
only when the original birth publication still proves absence, no unit execution
can write the Service, and live runtime and references are absent. The retirement
receipt records the exact retained declaration and birth proof; those checks and
address release share one transaction. Executed or uncertain Services retain the
protected Agent removal path. See the
[settlement owner](../../internal/infra/etcd/environment_blueprint_service_omission.go).
The implementation is locally compiled, not yet behaviorally qualified.

## Aggregate hierarchy deletion

Tenant, ordinary Project and Environment deletion use one durable Controller
coordinator. Initial publication atomically creates the parent Task, visible
tombstone, immutable intent, cleanup fence, lock, replay authority and
idempotency result. The resource remains readable with its current deletion
Task while all conflicting mutation and descendant creation are fenced.

The coordinator freezes descendant membership at one revision and seals one
stable-id postorder action plan. Retry never resnapshots. Tenant plans are
flattened rather than dispatching nested Controller Project Tasks, which would
deadlock the serial native executor. Agent-owned host effects use typed work
items within the same Task;
Controller-owned record, Secret, Runner and parent finalizers are checkpointed
actions in the same plan.

Every action produces typed immutable completion evidence. Agent work additionally
uses durable attempt receipts, so Task status or a missing Docker name alone is
not proof of cleanup. Bounded scans summarize the complete plan before the
parent-last final transaction removes the resource and releases its slug.

Backup cleanup retires its exact objects and source exclusions before destructive
host cleanup removes Volumes. Consumer workloads stop before Attach deprovisioning,
and credentials remain until Backup cleanup completes. Credential
dependents and incoming grant sources finalize before their referenced Attach.
Each credential owner uses one sealed Agent deprovision plan, then a Controller
finalizer removes the exact captured record, facts and indexes. Network-only,
no-auth and credential-sharing Attaches need only that finalizer.

Deprovision plans use fixed-revision Attach facts and backing configuration,
not a fabricated ordinary Detach lifecycle. Custom hook inputs use the existing
operation-owned encrypted envelope and native Secret pins; Retry keeps those
same inputs. Successful completion releases them atomically. There is no legacy
grant-revoke/detach child path or rewrite of previously accepted invalid plans.

An empty Zone reservation registry and unconfigured scheduling defaults are
metadata, not surviving children. Finalization validates and retires them in
the same revision-checked transaction as their Environment. Live reservations,
configured scheduling, corrupt records or concurrent changes still block it.

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

## Permanent Backing deletion

Backing Service Destroy removes runtime only and retains configuration, data
and the facade hierarchy. Generic Project DELETE rejects backing Projects.
The dedicated Backing Service Delete uses the same durable hierarchy cleanup
owner, with its own route and idempotency identity. The Console confirms the
instance slug and permanent data loss.
An active lifecycle Task blocks admission. A configured Custom before-stop hook
must finish through Remove containers before permanent deletion; aggregate
cleanup never silently skips that hook.

Deletion accepts only an unused instance. Its consumer index must be empty both
at the admission read and in the atomic publication transaction. Publication
marks the Project as deleting, so new consumer Attach writes cannot race into
the instance. Operators detach consumers separately, preserving their existing
credential, grant and Backup-policy protection rules. No automatic consumer
cascade or cross-Environment impact approval is introduced.

The Task removes owned runtime, storage, network and metadata before releasing
the hierarchy. Historical consumer Recovery Points and their Connectors remain
consumer-owned. Instances with accepted deletion leave the active list, while
the durable Task remains available for inspection and Retry.

## Consequences

- Destructive work is explicit, observable and resumable, but a failed or
  corrupt operation may leave an aggregate fenced for operator diagnosis.
- Desired publication, runtime cleanup and final record removal have separate
  checkpoints; absence of one record cannot stand in for another.
- Large trees are handled by bounded transactions and helper calls, never
  recursive unbounded deletion.

## Source navigation

Volume behavior is owned by
[`internal/controller/volume`](../../internal/controller/volume/mutations.go).
Native captured inputs and result evidence are owned by
[`volumework`](../../internal/infra/etcd/volumework/input.go); the independent
[`removal owner`](../../internal/infra/volumeremovalrecord/owner.go) protects
resource admission until settled completion, not until diagnostic expiry.
Aggregate planning, execution and finalization are separated into
[`hierarchydeletionplanning`](../../internal/infra/etcd/hierarchydeletionplanning/planner.go),
[`hierarchydeletionexecution`](../../internal/infra/etcd/hierarchydeletionexecution/executor.go)
and
[`hierarchydeletionfinalization`](../../internal/infra/etcd/hierarchydeletionfinalization/preparer.go).
Captured Attach plans are sealed by
[`hierarchyattachplan`](../../internal/controller/hierarchyattachplan/planner.go).
