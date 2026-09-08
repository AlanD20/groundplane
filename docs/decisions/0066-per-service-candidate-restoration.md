# ADR 0066: Per-Service candidate restoration

- Status: Accepted
- Date: 2026-09-05
- Capabilities: C06 Releases, C21 Environment Blueprint
- Replaces: ADR 0064's assignment-wide target selection

## Context

An applied Environment projection can contain configured-only Services and
unrelated serving Services. Its existence does not establish a serving Release
for every candidate. QA first startup after configured-only Apply selected
`serving_predecessor`, then repeatedly failed because the selected Service had
never served. The same defect occurs when adding first-time candidates beside
already-running Services. A global absence target would also be wrong for a
mixed candidate set containing real serving predecessors.

## Decision

Keep ADR 0064's immutable procedure, writer and epoch fences, original Task,
recovery record, primary failure, and exact terminal proof. Replace only the
assignment-wide target with one closed target per selected Service/Release.
Do not add a global mixed target or let a helper choose from Docker state.

Claim opens the exact writer-fenced applied artifact and selects each declared
member alternative: restore its acknowledged serving runtime if present;
otherwise prove absence of that member's first candidate. A configured-only
Service is not serving. Conflicting, ambiguous, or incomplete runtime metadata
is corruption, not absence. Ordinary Releases use their sealed single-member
alternative through the same authority shape.

The canonical assignment authority binds the complete sorted candidate set,
each selected target, and the exact nullable applied artifact witness even if
every candidate selects absence. That artifact may contain unrelated serving
Services that must remain untouched. Its presence, revision, and bytes remain
CAS-fenced independently of the selected member targets.

The private wire, durable assignment, Agent validation, recovery-step selection,
helper dispatch, and Controller terminal checks consume that one member map.
Remove the superseded global target; there is no compatibility decoder,
old-Task rewrite, inferred default, or mutable re-selection on reconnect.

Each recovery helper invocation acts on its exact member only. Absence removes
and proves only that member's plan-owned candidate workloads and first proxy,
never another member or an unrelated Service. Serving compensation explicitly
selects the predecessor workload/proxy without dependencies or whole-project
reconcile; it must not start configured-only Services from the prior artifact.
Foreign identities fail closed. All required member proofs must agree with
the same authority before one terminal acknowledgement can close the Task.

A serving-member probe distinguishes restored, restoration-required, and
observation failure. The private Compose helper's closed
`restoration_required` outcome is legal only for a selected serving-member
`CandidateRestorationProbe`, after exact owned inventory observation. It has
no success evidence and cannot close a Task or satisfy compensation. The
Agent may use it only to reach the already-declared compensation obligation;
it does not authorize any new mutation. Foreign/ambiguous identity or failed
observation remains failure. Compensation still needs complete restoration
evidence and the independent workload postcondition before terminal proof.

Independent observation uses a concrete read-only descriptor opened from the
validated original plan, selected serving recovery step and canonical witness.
It preserves historical labels exactly. The Moby observer accepts this
descriptor through its explicit restoration method; mutation helpers cannot.
Appending a historical artifact to the current plan and resealing it is invalid:
historical ownership is not current-plan execution authority.

The applied projection stays exact on failure, including a configured-only
projection. First-candidate absence does not imply that the Environment had no
applied record. No desired-head rollback, serving promotion, resource deletion,
or recovery-deadline renewal is introduced.

## Verification obligations

- First startup after configured-only Apply selects and proves absence.
- First startup beside unrelated serving Services preserves them exactly.
- One mixed candidate set restores old members and removes first candidates.
- Unknown, duplicate, incomplete, or changed member/witness authority rejects.
- Actual publication, claim, reconnect, recovery, and terminal replay preserve
  the member map, source revisions, primary failure, and serialization fences.
- Helper proof covers exact container identities and excludes whole-project
  start, dependency expansion, and removal of unrelated runtime.
- Live failed startup recovers through the normal API, then corrected startup
  succeeds, with no manual repair of frozen Tasks.

Source evidence includes actual mixed Blueprint preparation/publication, claim,
reconnect, public Worker admission/execution, durable progress ACKs, independent
Moby observation with a fake read-only Engine, terminal rejection/acceptance and
replay. Portless and addressable cases pass. Helper tests cover selected scope,
owned candidate cleanup, foreign-state rejection and active proxy config proof.
Affected package race suites pass; this is not a full repository gate or live
Docker recovery proof. Live failed/corrected startup remains the deployment
acceptance obligation, not an already-completed result.

Retained inactive blue-green slots whose Release identity is absent from the
captured artifact remain fail-closed; this change does not claim general
historical slot recovery. Initial QA hosting does not exercise that topology.
