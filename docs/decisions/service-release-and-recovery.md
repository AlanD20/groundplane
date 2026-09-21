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
and replica count. Historical rollback, retry and compensation use that sealed
identity. They do not resolve a mutable tag again, fabricate a repository
digest or read current desired state. Missing historical image authority fails
before mutation.

Rollback selects eligible completed-and-served history at one fixed revision,
then creates a new Release that copies the historical workload authority. It
never reactivates or edits the historical record. Release Groups publish one
ordered operation, one Task and one fence set for all members. Members execute
serially; `switch_back` compensates the proved switched set in reverse order,
while `leave_active` preserves the last proved serving candidate. There are no
child Deploy Tasks.

## Ambiguous effects retain recovery authority

The sealed candidate procedure contains the lawful restoration alternatives
for each Service: restore the exact proved serving predecessor, or prove exact
candidate absence when no predecessor existed. Host observation proves a
selected alternative; it never chooses one.

Forward execution has a durable effect barrier. Once a mutation-capable step
has durable running evidence, or a Script reaches start authorization, a
reconnect cannot replay the forward prefix. An unproved terminal report creates
private recovery authority for the original Task and switches it to
recovery-only execution. Recovery probes before compensation, advances a
durable cursor, and may run only the sealed probe and restoration steps. It
cannot run candidate apply, Script, health, switch or another target.

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
