# Service release and recovery

- Decision status: Accepted
- Scope: Service lifecycle Tasks, immutable Releases, rollback, compensation
  and failed-operation recovery

## Service lifecycle actions are pinned Tasks

Start, Stop and Destroy always create a durable Task and atomically set the
Service runtime intent to running, stopped or absent. A failed attempt does not
silently roll back the requested intent.

If the Service is in the applied projection, execution pins that exact
projection and uses targeted Compose apply, stop or remove. Destroy removes
Service runtime only; it does not delete desired state, the project, networks
or Volumes. If the Service has never been applied, the action is still an
observable Controller-owned no-op Task. A durable per-Service owner serializes
lifecycle actions so queued work cannot execute a newer Blueprint accidentally.

Ordinary lifecycle Tasks pin the serving Release and its acknowledged physical
members. A freshly provisioned PostgreSQL or MySQL Backing Service instead pins
its native provisioning receipt and exact artifact; provisioning is not a
Release. Publication fences that applied projection and the absence of a newer
Release-based runtime. After a database Deploy, lifecycle uses the acknowledged
Release runtime rather than the older provisioning receipt. Neither path invents
historical authority or reads newer desired state during execution.

Remove captures the acknowledged runtime separately from desired configuration.
Its publication compares that runtime's revision, including absence for an
undeployed Service. Cleanup selects the captured proxy, active workload and
retained slot with their original ownership; desired names alone are not runtime
authority. Each physical member must disappear, and unexpected or remaining
members block completion before the Service definition is deleted. See
[removal planning](../../internal/controller/taskplanning/service_removal_runtime.go).

## Releases are immutable history

A Release is an append-only candidate record with immutable render input and a
separate execution checkpoint. Task attempts do not change its identity. Retry
reuses the same operation, Release ids, selected topology, plan and render
input; it never renders newer desired state or creates a replacement candidate.

`serving_release_id` and `current_successful_release_id` are independent facts.
A post-serving failure may leave a candidate serving while the previous
successful Release remains the last completed one. Neither fact is derived
from the newest Release record or from observed container order.

Before publication, candidate workload references are resolved through a
bounded read-only Agent request and sealed with the exact host-local image id
and replica count. Retry and compensation retain their original sealed inputs;
they do not read current desired state. Image-only rollback copies the historical
image identity into a new candidate with the current Service configuration,
replica count, strategy and failure policy. None of these paths resolves a
historical mutable tag again or fabricates a repository digest. Missing historical
image authority fails before mutation.

Rollback selects eligible completed-and-served history at one fixed revision,
then creates a new Release that copies only the historical image authority. It
never reactivates or edits the historical record. Release Groups publish one
ordered operation, one Task and one fence set for all members. Members execute
serially; `switch_back` compensates the proved switched set in reverse order,
while `leave_active` preserves the last proved serving candidate. There are no
child Deploy Tasks.

## Proxy choice is captured, not rediscovered

The [Environment proxy choice](../features/components.md#routes-and-the-http-router)
governs both HTTP routing and stable Service traffic forwarding. A Task must keep
its selected implementation, configuration and predecessor; recovery cannot use
the Environment's later choice. Configuration rendering and strict routing-set
validation share [one owner](../../internal/common/serviceproxy/configuration.go)
so generation checks cannot accept listeners or upstreams that execution did not
capture. Envoy's Service proxy retains stable listeners while captured cluster
updates publish through filesystem discovery. A file write is not acknowledgement:
the [runtime confirmation owner](../../internal/common/serviceproxy/envoy_observation.go)
requires the complete active listener/cluster set and the configuration PID1 will
load on restart. The admin endpoint stays on container loopback. Listener changes
cannot use this target-only update path.

Deploy and Blueprint release preparation obtain the selected engine from the
frozen Environment projection, not from a caller's default. Stored render inputs
must match that choice. Backing containers serve their declared ports directly
through their Attach network; they do not receive the Environment's HTTP proxy.

Blueprint Service fingerprints include the captured engine, so an engine change
cannot skip an otherwise unchanged Service and leave its old proxy applied.
Blueprint Route receipts acknowledge authored metadata before the router runs;
waiting for the target Service would cycle through the shared runtime projection.
Router units claim affected Service resources to serialize forwarding decisions.
Those claims do not acknowledge Service runtime or grant workload mutation authority.

[Release transition admission](../../internal/core/release/proxy_transition.go)
uses the same rules for direct Deploy and Blueprint Apply. Blue-green and rolling
retain the proxy process, image and listener set; changing those requires an
explicit recreate selection. Blueprint preparation passes its captured historical
selection rather than assuming a first deployment. Stored inputs and the machine
plan independently bind the candidate to the actual retained proxy artifact.
Recreate may select the captured new engine's compiled image while recovery keeps
the exact old artifact and image. None of this enables the unfinished Environment
switch coordinator or proves uninterrupted TCP/UDP operation.

This Service-proxy execution is locally implemented, not behavior-qualified.

The Environment HTTP router has separate host/path configuration. Its local
[Envoy planner](../../registered-components/envoy/router.go) captures a consistent
bootstrap, cluster and Route snapshot. Changes need the union of old and new
backends active before replacing Routes, then removal of unused backends; publishing
Routes first can reference unavailable clusters. The
[discovery owner](../../internal/common/envoyrouter/activation.go) prepares that
ordering and recognizes only the captured previous, intermediate and desired
restart files. The compiled executor waits for each full active configuration;
interruption does not authorize adopting an unrelated live configuration.
[Task planning](../../internal/controller/taskplanning/container_config_transition.go)
binds the previous source to retained configuration before sealing, not to the
current desired head. Its [restoration owner](../../internal/common/envoyrouter/restoration.go)
admits captured forward and compensation states. A distinct temporary recovery
Route version lets Envoy acknowledge restored Routes even after rejecting a
forward update. Rejection evidence may authorize a protected restoration write;
it never proves success. Completion still requires the exact prior restart files
and complete active resources without rejection evidence.

[Component recovery](../../internal/common/executionplan/component_recovery_configuration.go)
references the forward source pair rather than duplicating it. Disable/omission
has no activation action: it captures the prior source bound to its file-restoration
step and verifies it after exact runtime recreation. An engine change may leave the
predecessor file untouched. That file is selected from the Task's pinned snapshot,
not a newer configuration head, and carries read-only proof rather than a synthetic
write step. The machine contract rejects a writer to the same path; the compiled
recipe verifies its mount and active configuration. Healthy containers alone
cannot prove that prior Routes serve. This is locally compiled, not qualified.
Direct Routes retain their admission-time executable plan, file sources and
acknowledged router artifact. Dispatch never rerenders them from newer state.
Their [recovery planner](../../internal/controller/taskplanning/route_recovery_plan.go)
uses the same file and singleton-runtime compensation as Blueprint Components;
unrelated workloads in a historical artifact remain read-only inventory. Successful
completion records the exact applied router artifact atomically with the Route
outcome, so the next operation does not recover an obsolete predecessor. Assignment,
reconnect and terminal replay keep the original failure and captured authority.
This path is locally compiled, not behavior-qualified.

The [composition registration](../../internal/app/componentregistration/envoy.go)
binds the upstream image and HTTP-discovery recipe to this same captured-input
path. Router address reservation, configuration preview and Tunnel diagnostics
use the selected provider's typed Zone settings, not an assumed Caddy record.
The Environment's single router slot is selectable through Blueprint, Console,
CLI and API. Changing engines captures the affected native Service plans and
retains their predecessor authority; replacing a running proxy requires explicit
Recreate. Source implementation is complete, but live engine-change and recovery
qualification is still pending.
UDP session handling needs its own proof; TCP connection draining is not UDP evidence.

## Ambiguous effects retain recovery authority

Blueprint Apply uses the same per-Service forward rollout planner as Deploy.
There is no recreate-only Blueprint rollout or separate selected-Service
deployment engine. Its selector belongs to protected request identity, not
authored desired state. Admission validates the complete document and rejects
unselected or shared changes; execution narrows mutations to that Service.
Saving direct Service edits only changes this same authored input.

Blueprint recovery keeps its claim-time restoration alternatives, including the
captured acknowledged runtime and pinned files. Sharing forward rollout steps
does not replace that authority with a later desired projection. The owning seams
are [selection admission](../../internal/controller/blueprint/service_selection.go)
and [release planning](../../internal/controller/taskplanning/blueprint_release_plan.go).

Rolling uses one captured replica sequence for direct Deploy, Blueprint Apply
and image-only rollback. Each replica change advances with its durable Task
events under a fixed deadline; a duplicate start cannot renew that budget.
Scaled recreate predecessors need observed container ids and names because their
shared Compose service name cannot safely select one container. These identities
are captured before publication and rechecked before removal; the original full
runtime remains the recovery source. See the
[replica planner](../../internal/common/rollingrelease/plan.go) and
[predecessor capture](../../internal/controller/rollingpredecessor/capture.go).

The sealed candidate procedure contains the lawful restoration alternatives
for each Service: restore the exact proved serving predecessor, or prove exact
candidate absence when no predecessor existed. Host observation proves a
selected alternative; it never chooses one.

Recreate acknowledgement and restoration require every sealed replica running
with its exact image and ownership. Health must pass when the sealed workload
defines a healthcheck. An unchecked singleton can be running, never proved
healthy; blue-green slots continue to require healthchecks.

Forward execution has a durable effect barrier. Once a mutation-capable step
has durable running evidence, or a Script reaches start authorization, a
reconnect cannot replay the forward prefix. An unproved terminal report creates
private recovery authority for the original Task and switches it to
recovery-only execution. Recovery probes before compensation, advances a
durable cursor, and may run only the sealed probe and restoration steps. It
cannot run candidate apply, Script, health, switch or another target.

The Controller saves a received terminal report before publishing incremental
Release results. Reconnect resumes that bookkeeping from the saved report;
it does not interpret incomplete bookkeeping as permission to undo accepted
host effects. The saved report fences later events and assignment epochs and
is removed atomically with Task completion.

An interrupted Task can require restoration without any individual step having
failed. Exact restoration proof closes that Task with its original failure,
without inventing a failed step. Settlement updates current serving/runtime
facts and the execution checkpoint; already-published Release summaries remain
history rather than being rewritten to describe the restored state.

The original failure remains primary. The original Task becomes terminal only
after exact restoration or absence proof and cleanup. If proof remains
ambiguous, the operation and fences remain gated; ordinary Task Retry resumes
the same recovery authority instead of opening a Release-specific action.

Latest-wins Blueprint reconciliation has one separate handoff rule for a
superseded shared-configuration write. It does not weaken ordinary Release or
unsuperseded Blueprint restoration.

## Recover the pinned configuration, not current configuration

A mutation-capable Task captures the acknowledged per-Service runtime and the
exact pre-operation configuration files it may overwrite, including presence,
content digest, destination and metadata. Recovery may restore only those
pinned files alongside the selected prior runtime. It retains the assignment,
execution epoch, writer and source fences, so stale recovery cannot overwrite a
newer operation.

Pinned Secret values remain undeletable while recovery or retry can still use
them. Deletion returns `resource.in_use`; it does not substitute the latest
value or keep an undeclared hidden copy after successful deletion. This
authority does not permit database restoration, migration reversal, Script
replay, unrelated file restoration or application of the newest Blueprint.

The configuration-restoration extension and its live journey remain
implementation and qualification work. Acceptance of the decision is not proof
that the operator recovery outcome has passed.

## Consequences

- Serving truth and successful history remain explicit through partial failure.
- Exact probes and immutable labels are required before adoption or
  compensation; ambiguous host state is never guessed away.
- Release history can outlive Tasks, while rollback material has its own
  availability projection and retention fence.
- Recovery continues the original Task and operation, preserving one audit and
  failure identity.

## Source navigation

Release records and checkpoint authority live in
[`internal/infra/etcd/releases`](../../internal/infra/etcd/releases/records.go).
Release planning is in
[`internal/controller/taskplanning`](../../internal/controller/taskplanning/release_plan.go),
restoration procedures in
[`internal/common/executionplan`](../../internal/common/executionplan/candidate_release.go),
and recovery persistence in
[`internal/infra/etcd/release_recovery_transition.go`](../../internal/infra/etcd/release_recovery_transition.go).
Agent restoration and pinned-file handling are in
[`internal/agent/release_configuration_recovery.go`](../../internal/agent/release_configuration_recovery.go).
