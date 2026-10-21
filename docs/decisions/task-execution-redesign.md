# Task execution redesign

- Status: Implemented and deployed to disposable QA; qualification remains partial.
- Scope: replace Task execution and its storage/protocol boundaries, preserving
  existing feature behavior. This document does not authorize implementation.
- Product owner: [Execution redesign](../mvp.md#accepted-task-execution-redesign).

This is the single execution design reference. The superseded shared worker,
assignment journal and hidden child-Task mechanism were removed, not retained
as another execution path. The [QA matrix](../qa-matrix.md) records candidate-specific
proof and remaining gaps; this design alone does not qualify the replacement or
make it available in an older installation.

## Why replace the execution machinery?

Atomic queue claims alone did not isolate responsibilities: shared claim,
worker and plan-validation code all owned feature-specific recovery rules.
A feature change therefore crossed too many shared execution boundaries.

Keep durable Tasks and typed execution. Replace this coupling with a small
scheduler and complete operation implementations. Keep etcd as the control-plane
store; a new broker or general workflow platform would not solve the ownership
problem. Moving files or renaming existing structures alone is not this redesign.

## Three independent responsibilities

| Owner | Responsibility |
| --- | --- |
| Resource state | Desired configuration, last successfully applied configuration, live observations and stable ownership |
| Execution | Accepted work, captured inputs, dependencies, assignment, checkpoints and result settlement |
| History | Safe progress, diagnostics, original failure and retained outcomes for operators |

Resource reads do not require successful execution or retained Task diagnostics.
They show desired declarations and separately identify unavailable applied state
or observation. Execution still requires its exact prepared inputs and ownership.
Deleting history cannot delete a resource's applied configuration or release an
unresolved execution's protection.

## Task, work item and attempt

A **Task** is one accepted operator or system action, with stable identity, actor,
scope, captured intent and overall outcome. Ordinary actions have one work item.
Blueprint Apply has one visible Task containing dependent resource work items;
it does not manufacture hidden operator Tasks.

A **work item** is a typed operation on explicit resources, assigned either to
the Controller or to a particular host. It owns its dependencies, captured input
references, conflict claims and execution result. It is not an arbitrary command.

An **attempt** is one authorized execution of a work item, identified by its
executor and increasing assignment generation. Redelivering an assignment or
reconnecting does not create another attempt. Retry retains the Task and work-item
identity and captured inputs; a request to use newer input is a new action.
Recovery attempts are distinguishable from forward execution.

Work moves from waiting to ready, assigned and running, then to a recorded
outcome. Waiting always identifies the unmet dependency, conflict or unavailable
executor. A lost connection is an observation, not a terminal outcome. Original
action outcome, current attempt state and cleanup state remain distinct fields
rather than one ambiguous status. Task success requires every required work item
to settle successfully or be explicitly satisfied without repeating effects;
superseded or failed required work cannot be reported as successful Apply.
Failure handling for independent branches follows the captured operation policy,
not an improvised scheduler decision. Dependencies are bounded and acyclic.

Large inputs are immutable, bounded references rather than copies in every
event or attempt. Their lifetime follows active execution and resource needs,
not diagnostic retention. Secrets remain encrypted or transient protected
material, never public Task input, events or error text.

Blueprint prerequisite ancestry belongs to the captured authored revision.
Planning reads that snapshot at the same fixed revision as Attach resolution,
instead of reconstructing ancestry from current desired state or a parallel gate.
A direct Attach has no Apply snapshot; an Apply missing its snapshot is rejected.
The [snapshot owner](../../internal/infra/etcd/blueprint_prerequisite_authority.go)
retains this authority independently of Task diagnostics. The clean cutover
advances the storage epoch because older captures may omit inherited ancestry;
the new reader does not guess missing edges or decode an old gate.

## Submission and scheduling

Submission validates intent and atomically publishes the Task, accepted desired
changes, replay identity and required ownership records. Same request key and
same content return the same Task; different content under that key is rejected.
No work becomes executable before its complete required inputs are durably
prepared. Missing or invalid input is not repaired by reading newer desired state.

Some inputs depend on preceding work, such as Custom Attach facts. Such a work
item waits with a specific dependency reason. The Controller prepares it from
the captured intent and exact accepted dependency outputs, then publishes its
immutable executable input before assignment. It never seals nonexistent facts
or silently switches to the latest Blueprint or Secret value.

The scheduler owns dependencies, conflict claims, executor eligibility, capacity,
deadlines and atomic assignment. It does not contain operation-specific Docker,
SQL, deployment or recovery logic. Ready work is scheduled fairly; a blocked item
does not occupy a worker slot or prevent unrelated ready work from being selected.
Related items acquire their required conflict claims atomically before effects,
not one resource at a time while holding partial claims indefinitely.

The operation owner supplies validated preparation, conflict requirements and
result settlement through a narrow boundary. Publication and settlement remain
atomic with the affected resource state; modularization must not split these
transactions into independently successful writes.

## Operation ownership

### Typed effects and secret handling

The Controller captures decisions and immutable inputs. The Agent executes a
closed typed procedure; it does not read Blueprints, infer missing parameters or
turn generic command text into host authority. Built-in adapter commands stay
compiled. Custom Backing hooks are the explicit bounded exception: their captured
command runs inside the selected Backing container, not as a host shell.

Compose artifacts are derived, secret-free execution inputs. Compose CLI owns
Compose mutations; bounded Docker observation checks exact object ownership.
The task-scoped helper receives only the selected artifact, mounts and working
environment. Its Docker access is root-equivalent, not a sandbox against a
compromised Agent. Authentication and host binding remain mandatory.

Secret and hook values use the authenticated transient input channel and encrypted
operation-owned storage. They are not Task parameters, diagnostic text or result
summaries. Missing observations stay missing; restoring a process cannot invent
the cause of its earlier failure.

### Complete operation handlers

Each operation implementation owns execution, meaningful checkpoints, interruption
inspection, safe retries and permitted cleanup. Deployment owns release strategy
and candidate handling; Backup owns capture; Restore owns its destructive effects.
They use the same scheduler and attempt lifecycle, not separate queue engines.

The Controller selects desired configuration, dependencies and policy. The Agent
executes the captured typed operation through compiled handlers. It cannot read a
Blueprint to invent missing instructions or choose a newer configuration. Generic
shell jobs, runtime plugins and permissive fallback decoders are not introduced.
Existing explicitly authorized Script and Custom hook boundaries remain intact.

An operation declares durable checkpoints at meaningful side-effect boundaries.
The engine requires their persistence before the next action that depends on
them. Progress output alone neither authorizes effects nor proves completion.
Polling health or emitting ordinary progress does not require a fresh permission
round trip for every message. Irreversible actions and traffic cutover still
require the current operation-specific ownership and supersession checks.

Controller-local operations, including software handoff and bootstrap, use the
same execution concepts without inventing an Agent or depending on a process
being replaced to report its own successful replacement.

## Remote Agents and host ownership

The execution boundary supports one Controller managing multiple hosts, with one
active managing Agent per host. Host identity is stable across Agent replacement;
an Agent process or connection is not the host identity. The initial migration
can remain single-host without encoding a singleton host into execution records.

Every host work item carries its target host identity. Assignment binds that host,
the authenticated Agent identity and its current generation. Eligibility checks
required operation capabilities, compatible protocol and capacity. An idle Agent
on another host cannot take the work. Host-independent Controller work has no
fabricated host target.

Placement is separate from execution: it selects a host before work becomes
assignable. Future manual or policy-based placement can reuse this boundary.
This contract does not add placement controls, workload migration, automatic
failover or cross-host networking to the MVP.

Agents establish authenticated connections to the Controller. Remote transport
must protect credentials and execution material in transit and bind credentials
to the allowed host; the local channel is not permission to send tokens over
plaintext public networks. Remote enrollment, certificate lifecycle and trust
configuration require their own design before remote operation is enabled.

Operation readiness is separate from general executor capacity. Missing Backup
staging excludes only staging-dependent operations from assignment and new Start
grants. It does not disable unrelated host work, discard occupied slots or prevent
receipt settlement for effects already authorized. Availability changes are
revision-fenced with assignment; the scheduler consumes generic operation
eligibility rather than knowing Backup recovery policy.

Host operations do not rely on Controller access to the remote filesystem,
Docker socket or SSH. Local Volumes, networks, images and conflict claims are
host-scoped. Artifact references must be usable by the selected executor;
Controller-local paths and localhost registry addresses are not remote locations.
An image available on Host A is not assumed present on Host B. Existing explicit
image Fetch behavior is unchanged.

## Interruption, Retry and cancellation

Delivery is at least once, not exactly-once external execution. An Agent durably
records accepted assignment/checkpoint identity and redelivers retained results
until acknowledged. Duplicate messages cannot authorize duplicate effects. On
restart, the operation inspects its checkpoint and actual owned effects before
continuing, cleaning up or reporting that operator attention is required.

A missing heartbeat, timeout or disconnected stream does not prove that execution
stopped. Assignment generations reject stale reports, but rejection alone cannot
stop a Docker mutation. Before conflicting work starts, GP must establish that
the old executor cannot continue those effects, through execution-side ownership
checks and acknowledged termination or equivalent operation-specific proof.
Unknown effects retain only the necessary conflict claims, not an unrelated
host-wide execution lock. Host-wide faults may legitimately block the host and
must be reported as such.

A correctly authenticated terminal report that cannot be settled receives an
exact-report rejection, not a successful receipt or a stream disconnect. The
Agent retains its durable report and returns the refusal to that operation;
unrelated reports can continue. A refusal releases no resource claim, grants no
effect and cannot be used to retire the retained result. Malformed frames and
foreign executor identities remain protocol errors. The
[report boundary](../../internal/common/workreceipt/rejection.go) binds this
response to the full captured report.

Retry classification belongs to the operation: safe transient failures have
finite attempts, backoff and a captured deadline. Invalid input fails promptly.
Ambiguous custom commands do not automatically replay. Explicit Retry can grant
another captured execution only under that operation's safety rules. Recovery
cannot reread latest intent, undo arbitrary migrations or become a generic
database rollback. Successful cleanup never changes the original failure to success.

Latest-wins applies to conflicting desired-state work, not every Task on the host:

- Superseded queued work does not start.
- Running work stops at a defined safe boundary and resolves its owned effects.
- New desired intent and irreversible forward permission are ordered atomically;
  obsolete work cannot receive a new traffic-cutover grant.
- A previously authorized external action may finish after newer intent arrives.
  Its outcome must be reconciled before contradictory effects proceed.
- Supersession revokes forward permission, not permission for captured cleanup.

Abort is a request, not proof of termination. Completion can win the race. A Task
cannot report clean cancellation while effects remain unknown. Task action outcome
and cleanup status are separate: failure or cancellation can require attention;
only verified settlement releases the affected resource claims.

## History, diagnostics and storage pressure

Keep small authoritative execution records and essential checkpoints in etcd.
Detailed diagnostics have a separate bounded storage and retention boundary.
The diagnostic backend and physical schema are implementation decisions, not a
new required service. Neither raw output nor repeated full configuration snapshots
belong in the control-plane journal.

Critical checkpoint or result persistence failure stops dependent effects.
Diagnostic storage failure is reported as incomplete diagnostics; it does not
invent a successful stage or erase execution ownership. Events have stable replay
identity, ordered delivery within a Task and explicit gaps after expiry. Clients
can reconnect and deduplicate. Workload logs remain separate from Task diagnostics.

Retention preserves original outcome, safe failure summary and replay identity
for their defined retention periods. Active or unresolved work retains all inputs
and evidence required for recovery. Applied resource state outlives execution
history when the resource still depends on it. Cleanup cannot invalidate an
active reader or a newly captured reference. Admission under storage pressure
must reserve room for existing work to settle rather than continue unbounded
publication; exact thresholds belong to the storage implementation.

Low reserve pauses new operator state changes, not just Task submissions. Reads,
Agent reports, existing-work recovery, Abort/Retry and maintenance remain available.
The HTTP boundary checks operator admission; the transaction boundary also checks
new Task acceptance from background publishers. Scheduled backups pause without
skipping retention or storage maintenance. The live backend quota and allocated
size determine reserve; reusable pages do not count as reclaimed capacity.
The threshold lives in [storage admission](../../internal/infra/etcd/storage_admission.go).

Backup recovery retains orphan cleanup authority in the same transaction as its
validated recovery receipt, resource settlement and claim release. The orphan
then owns the captured source and receipt identity independently of execution
history. Remote reconciliation cannot proceed before that transfer, and Task
retention cannot remove evidence from an unproved orphan. The
[native cleanup owner](../../internal/infra/etcd/backupwork/capture_orphan_cleanup.go)
replaces the old Task-journal and startup-delivery reader; no legacy proof decoder
is retained.

Hierarchy removal detaches completed Backup, Restore and key-rotation membership
under native terminal Task and resource-revision guards. Subsequent history
collection checks the Environment and membership at one revision: both remain,
or both have been removed. Active hierarchy-removal fences still block collection.
The [membership owner](../../internal/infra/etcd/backupruntime/history_membership.go)
does not depend on a generic Task terminal receipt.

[Diagnostic expiry](../../internal/infra/etcd/executiondiagnostics/retention.go)
uses a revision-bound cursor, independent of operation-owned Task collection.
Retry invalidates the previous expiry cursor. Expiring the event tail retains its
sequence high-water mark, so reconnect cannot mistake new observations for old
ones; the viewer distinguishes expired details from failed diagnostic writes.

## Operator presentation

Console, CLI and API expose the same Task, work-item and attempt meaning under
the existing one-to-one surface rule. Internal work items are inspectable within
their Task, not independently actionable public Tasks.

The [Task execution viewer](../console-ux.md#task-execution-viewer) owns presentation:
action summary, current outcome/blocker and a structured stage list, with detailed
events and technical identifiers on demand. The read model must provide captured
stage names, targets, expected/observed results, attempt identity, timing provenance,
wait dependencies, original failure and separate cleanup status. Optional evidence
is explicitly unavailable when absent. Clients do not parse prose or reconstruct
historical facts from current resources. Operator-visible diagnostics stay bounded
and secret-safe; shared execution does not need feature-specific UI branches to
explain progress.

## Breaking cutover, not compatibility

Replace the execution storage model, Controller/Agent protocol and public Task
projection together, regenerating clients from their owning sources. Remove old
runtime readers, writers, hidden child-Task machinery and dispatch paths. Do not
ship dual execution engines, legacy Task translators or compatibility fallbacks.
Reject incompatible binaries before they can execute or mutate new-format state.

Before cutover, stop new admission and settle or explicitly reconcile outstanding
effects. Never reinterpret an old half-executed Task as a new work item. Historical
Tasks may be exported outside the active execution store; importing them as live
work is forbidden. Resource data may need a one-time offline conversion, not a
permanent old-format reader. The exact conversion must be designed before migration.

No compatibility requirement means no legacy execution path; it does not authorize
deleting applications, databases or Volumes. A disposable reset is an alternative
only with explicit scope and approval. No reset, deployment or migration is
authorized by accepting this document.

The startup gate is owned by [execution storage](../../internal/infra/etcd/workqueue/schema.go).
It initializes a fresh native store atomically and rejects superseded Task
records rather than translating them. A rejection marker also makes the previous
Controller refuse the store after cutover; the release storage epoch and Agent
protocol reject incompatible artifacts through the normal installer and channel.
These guards do not migrate an existing installation. Stop admission, reconcile
outstanding effects with its matching software, and export retained history before
an explicitly designed offline resource conversion. Until that conversion exists,
keep existing installations on their matching build or use an explicitly approved
disposable reset. Startup never deletes workload definitions or data to pass this
gate. Native operation publication begins only after it succeeds.

## Delivery boundary

Write the complete agreed replacement, generated artifacts and removal of old
execution paths before beginning validation. Do not use incremental implement/test
cycles or write tests during implementation. Builds, typechecking, CI, browser
checks and deployment belong to the subsequent QA phase. Source inspection,
formatting and required code generation remain implementation work. After the
implementation is fully written, perform bounded behavior qualification and fix
failures. Do not turn this contract into a broad feature rewrite or a
test-per-helper project.
Required proof concerns duplicate delivery, crashes around effects/checkpoints,
lost acknowledgements, safe cancellation/cutover races, isolated failures, bounded
retry, retention safety and operation-specific recovery. Host routing must reject
wrong-host execution; multiple-host runtime support needs separate live evidence.
The [QA matrix](../qa-matrix.md) records completed checks and their limits;
these obligations are not satisfied merely by accepting this design.

Remote installation and lifecycle, cross-host network/storage access, automatic
placement and failover remain outside this redesign. The implementation owns its
schemas, modules and diagnostic storage. An offline resource conversion procedure
is not provided. That gap is not permission to weaken these boundaries or retain
the old execution engine.
