# ADR 0064: Candidate Release restoration authority

[ADR 0078](0078-latest-wins-blueprint-reconciliation.md) replaces whole-Blueprint
execution and aggregate promotion with latest-wins private execution units. It
also owns one narrow sequencing exception: after a superseded Blueprint unit
changed shared configuration, its executor is proven stopped and its exact effects
are accounted for, the newest valid accepted input may repair forward without
first restoring the predecessor. The rules below remain authoritative for ordinary
Release and unsuperseded Blueprint failure, and for any predecessor restoration
that is performed; they do not force restoration before that eligible handoff.
Supersession cannot steal recovery ownership, reopen the old forward execution or
use a newer desired head to reject an otherwise lawful cleanup report. The generic
proposal to retain and restore pinned configuration files remains unapproved.

- Status: Accepted
- Member selection and ordinary predecessor capture are owned by
  [Services and Releases](../features/services-and-releases.md#per-service-restoration).
- Date: 2026-09-04
- Native predecessor authority aligned with the owner decision on 2026-09-12.
- Capabilities: C06 Releases, C09 Scripts, C21 Environment Blueprint

## Context

Ordinary Release execution already sealed probe and compensation steps, but
Environment Blueprint candidate execution used a separate apply, Script, and
health sequence. The two paths did not share one restoration procedure.
First Blueprint application consequently fabricated a `baseline` predecessor,
an Agent could report recovery success after running no applicable
compensation, and Controller rejection of an unproven terminal acknowledgement
left a running Task whose reconnect replayed the forward prefix. Repeating the
ordinary deadline then repeated the same impossible terminal path.

Terminal fail-closed validation is correct: a failed candidate must not become
serving, and a Task must not terminalize without exact restoration proof. The
missing authority is a durable recovery state machine, not a permissive ACK
decoder or a synthetic predecessor.

## Decision

### One sealed candidate procedure

One pure concrete module under `internal/common/executionplan` constructs the
immutable `CandidateReleaseProcedure` used by ordinary Release and Blueprint
plans. It introduces no Go interface and performs no I/O.

The plan hash covers, for every candidate mutation:

- the exact candidate Service, Release, artifact, Compose project, and forward
  mutation anchors;
- the complete selected Service/Release set;
- the closed lawful restoration alternatives `serving_predecessor` and
  `candidate_absence`; and
- the exact probe and compensation step identities for each alternative.

A Blueprint plan declares both alternatives and binds the captured native
predecessor references. Claim validates and records the selected alternative. Plan
bytes and hash are immutable across claim, reconnect, deadline, recovery, and
terminal acknowledgement.

For Blueprint and ordinary Releases, `serving_predecessor` binds the exact
per-Service native artifact, Release, target, fixed read revision and canonical
authority digest. Both paths use staged per-Service native witnesses, as
defined in [the predecessor contract](../features/services-and-releases.md#ordinary-release-predecessors).
`candidate_absence` binds the immutable candidate artifact, exact Compose
project, and complete selected Service/Release set. Host observations are proof
for the selected target only; they are never selection or decision authority.
The string `baseline`, synthetic Release identity, and restoration derivation
from a candidate Service's mutable `Current` projection do not exist.

### Claim selects a restoration target per Service

Blueprint publication captures native Release authority for every selected
Service and retains its independently sealed nullable `BlueprintAppliedPredecessor`.
Claim validates those captured sources under the existing writer and Environment
mutation-epoch fence and records one already-declared target per member:

- A member with an acknowledged serving runtime selects `serving_predecessor`
  and requires the exact native artifact, Release references, revision and digest.
- A member without a serving runtime selects `candidate_absence`, including
  configured-only members in a present applied projection. Absence requires an
  explicit empty native witness; missing authority fails closed. The nullable
  applied witness remains independently fenced even when every member selects
  absence.

The Environment artifact is not a recovery selector or a fallback. It can describe
the same runtime with a different artifact identity. Restoration and independent
observation therefore use the same native bytes, while failure preserves the
separately captured applied Environment state.

The claim transaction atomically stores the sorted member targets and canonical
restoration-authority digest in every durable assignment copy while installing
the claim, writer, and positive execution epoch. An Agent cannot choose or
change the target. A mismatch between the nullable witness, plan alternative,
assignment copy, or digest is Internal corruption and performs no effect.

### Execution epochs and the forward barrier

Assignment execution epoch is the common positive attempt fence for Task step
events and terminal acknowledgements. It cleanly replaces reconstruction-only
event-attempt calculation.

A fresh claim uses epoch 1 in `forward` mode. A reconnect may atomically
increment the epoch and remain `forward` only after proving the absence of all
durable effect-possible evidence. Any accepted running or terminal event for a
mutation-capable candidate step, any Script checkpoint at
`start_authorized` or later, or an existing recovery record permanently
selects `recovery_only`. Transport loss by itself does not imply recovery.

No mutation-capable candidate step may begin until its `running` event has been
durably accepted by the Controller for the exact assignment and epoch.
`RunScript` retains ADR 0040's stronger `start_authorized` checkpoint before
body or container creation.

Event acceptance, reconnect, scheduler claim, deadline, and acknowledgement
transitions compare the exact Task, assignment, writer, mode, epoch, and
authority digest. A changed old-epoch message conflicts; byte-identical replay
under its recorded digest is read-only success where the state machine defines
a completed replay.

### Private recovery record and recovery-only execution

An otherwise valid terminal acknowledgement with
`reconciliation_required=true` and insufficient restoration proof does not
terminalize or release ownership. Its acknowledgement transaction atomically:

- creates once `/v1/records/release-recoveries/{task_id}`;
- records the immutable primary report status, result, and canonical digest;
- records Task, assignment, operation, plan, selected restoration target and
  authority, recovery step ids, cursor, phase, evidence revision, and the
  original non-renewable recovery deadline;
- advances assignment mode to `recovery_only` and its next epoch; and
- keeps the same public Task `running` with `result=nil`, the claim, writer,
  active operation, and timeout ownership intact.

The recovery record is private. It is not another Task, desired-state record,
retry attempt, presentation source, or decision authority.

Recovery-only dispatch carries only the immutable selected probe and
compensation ids plus cursor and authority. It can never run materialization,
candidate apply, `RunScript`, health, switch, Component action, a completed
forward step, or an alternative restoration target. It probes before
compensation on every replay. A committed probe or compensation advances the
record cursor atomically, making crash replay idempotent.

Compensation reverses only candidate mutations whose running evidence was
durably accepted and whose completion is recorded. Zero applicable
compensation after any completed mutation is an invariant failure, never
success. A first-candidate Script failure removes only the exact plan-owned
candidate workloads from the selected Compose project, probes exact absence,
and emits typed `candidate_absence` evidence. It preserves the original Script
failure as the primary result and clears reconciliation-required only after
that exact absence proof.

### Deadlines and row progress

A forward deadline with no effect-possible event or Script checkpoint
terminalizes as the Controller-owned `timeout_before_effect` result after an
exact absence proof. A forward deadline with effect evidence atomically creates
or advances the same recovery record, changes the assignment to
`recovery_only` at the next epoch, and replaces the forward timeout row with a
recovery row before continuing later due rows.

The recovery deadline is fixed when forward authority is first published and
never extends. Its expiry cannot fabricate restoration evidence, delete the
record, terminalize the Task, retain a permanently claimed dead owner, or stop
the scheduler from processing later rows. Expiry retains a redispatchable
recovery authority and schedules bounded later work. Timeout, scheduler,
reconnect, event, and acknowledgement races use the same compare-and-swap
fences; exactly one transition wins.

### Terminal recovery acknowledgement

A final recovery acknowledgement must match the selected target, plan,
assignment, operation, epoch, recovery digest, cursor, and exact typed
restoration evidence. It terminalizes the original Task atomically with:

- the immutable original failure as primary;
- exact `serving_predecessor` or `candidate_absence` restoration evidence;
- Release recovery-record removal;
- assignment, claim, active-operation, timeout, writer, and transient source
  cleanup; and
- the existing Script source-reference close path when applicable.

For selected Script executions still exactly `not_started`, the proven final
recovery acknowledgement is the sole Controller authority for
`parent_failure_before_start` plus explicit absence and `cleanup_proven`.
Any later or unknown checkpoint fails closed. The Controller then uses ADR
0062's existing retry-forbidden `normal_completion` pages while retaining all
Task and recovery authority; source-root finalization joins this terminal
transaction rather than preceding it.

Desired Blueprint head and failed staged candidate Releases remain. For
`serving_predecessor`, the exact native runtime is restored and the applied
Environment projection remains unchanged. For `candidate_absence`, no candidate
workload is present and no applied predecessor is invented. Exact terminal replay is
read-only; different evidence or an old epoch conflicts.

The terminal Task result retains its execution epoch and recovery-record digest
after the live assignment is removed. Persistence compares that authority on the
first report and on replay; transport-only checks are insufficient. Recovery
replay must compare the submitted authority before normalizing the result to the
recorded failure. A timeout proven to precede all effects requires no compensation
on either first acknowledgement or replay. The diagnostic alone is not that proof.

### Private Agent wire

The private protobuf contract contains only the closed restoration target,
execution mode, execution epoch, immutable candidate procedure, selected
recovery directive and authority, typed candidate-absence evidence, and
terminal acknowledgement epoch/recovery digest. Generated protobuf code is
regenerated from `proto/agent.proto`. The existing private assignment schema is
replaced in place; there is no alternate schema, version fallback,
compatibility decoder, or public OpenAPI/API/CLI/Console change.

## Consequences

- Ordinary Release and Blueprint candidate execution share one restoration
  vocabulary and one hash-covered procedure.
- Controller state, not connection state or Docker discovery, decides whether
  forward work can resume.
- A failed first application can prove exact absence without fabricating a
  predecessor.
- Recovery retains the original public Task and original failure while
  remaining redispatchable across Controller and Agent restarts.
- Recovery implementation requires synchronized plan, wire, Agent,
  persistence, scheduler, ACK, timeout, and race proof before C21 can claim the
  behavior. The pure candidate-procedure foundation alone is not completion
  evidence.

## Rejected alternatives

### Treat reconnect as retry

Rejected. Connection loss says nothing about whether an acknowledged mutation
or authorized Script began.

### Terminalize recovery-required acknowledgement

Rejected. It releases serialization while the candidate may still affect the
host and weakens the existing correct fail-closed boundary.

### Select restoration from host state

Rejected. Host state is observation evidence and may be partial or stale; it
cannot choose between sealed Controller decisions.

### Create a recovery Task

Rejected. A second public Task would split one operation and primary failure
across journals. The private recovery record is continuation authority for the
original Task only.

### Renew the recovery deadline

Rejected. Reconnect-driven renewal permits indefinite authority. Expiry must
retain bounded redispatch without claiming unproved success.
