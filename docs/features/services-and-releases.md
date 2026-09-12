# Services and Releases

## Purpose and scope

Run an Environment's declared Services and keep an immutable record of Deploy,
Rollback and recovery. Desired configuration, serving runtime and live health
are different facts. A saved edit or successful old Task is not proof that a
Service is currently running or reachable.

## Functional requirements

- Direct Service create/edit changes normalized desired state without deploying.
  Explicit lifecycle actions select and execute that state. Remove is its own
  impact-checked Task and retains current visibility until successful cleanup.
  It never implicitly deletes Volumes or immutable Release history.
- Deploy and Rollback resolve one immutable candidate and exact predecessor
  authority before execution. Release Groups retain declared serial order,
  tag-selection rules, hook ordering and failure policy; frontends do not build
  their own selectors from public history.
- Replica count, strategy, tags, image identity and execution inputs are sealed.
  Partial execution cannot be reported as successful convergence. Recovery keeps
  the original Task and primary failure rather than creating a repair Task.
- Console, CLI and API expose the same lifecycle and history. Observed workload
  health remains separate from desired intent and Route reachability.

The exact lifecycle, replica and selection behavior belongs to [mvp.md](../mvp.md)
and [api-cli.md](../api-cli.md). Authored inputs belong to [blueprint.md](../blueprint.md),
and hook behavior to [Script execution](setup-scripts.md).

## Non-functional requirements

Use immutable, digest-bound images, plans and historical runtime bytes. Compare
source revisions and the Environment mutation epoch before publication or claim.
Keep ordinary transaction and record ceilings. Reject missing, foreign,
ambiguous or altered authority rather than infer a default from Docker or the
latest desired state. Cancellation and lost acknowledgements do not prove
external effects were absent.

## Technical design

| Concern | Current technical contract |
| --- | --- |
| Desired/runtime records | [Service records](../decisions/0028-service-desired-runtime-record.md) |
| Start, stop, restart and destroy | [Service lifecycle](../decisions/0042-service-runtime-lifecycle.md) |
| Immutable ledger, tags, replicated rollout and Release Groups | [Release execution](../decisions/0052-release-ledger-and-deploy-execution.md) |
| Typed Compose execution | [Agent Compose procedures](../decisions/0022-agent-compose-execution.md) |
| Forward barriers, recovery-only mode, epochs and terminal proof | [Candidate recovery](../decisions/0064-candidate-release-restoration-authority.md) |
| Read-only serving replica counts, freshness and public states | [Serving-workload observation](../decisions/0077-serving-workload-observation.md) |

### Per-Service restoration

Each selected Service/Release has one closed restoration target: its acknowledged
serving predecessor or exact first-candidate absence. Configured-only state is
not serving; conflicting or incomplete metadata is corruption. Do not use an
assignment-wide target, a global mixed default or host-side selection.

Blueprint and ordinary Release candidates select recovery from each Service's
captured native Release, under the writer and Environment mutation-epoch fences.
The plan, assignment, restoration and independent observation bind the same exact
historical artifact bytes. Explicit empty native authority means first-candidate
absence; a missing member or witness is invalid. The Environment-wide applied
artifact never substitutes for a native Release.

The canonical authority also binds the exact nullable applied artifact, including
when every member selects absence: unrelated serving Services may still be in
that artifact. Its presence, revision and bytes remain independently fenced. It
records Environment state, not a disk or database snapshot to restore. The native
Release source is used because an Environment artifact can have a different
identity; recovery and verification must not disagree about which record proves
the prior runtime. This decision was confirmed on 2026-09-12.

The private wire, assignment, Agent validation, recovery dispatch and terminal
checks use one member map. Reconnect cannot reselect it or rewrite old Tasks.
Each helper acts only on its exact member. Absence removes and proves only that
member's plan-owned candidate workloads and first proxy. Serving compensation
selects its exact predecessor workload/proxy without dependency expansion or
whole-project reconcile. Configured-only and unrelated Services remain untouched.
Every member's proof must agree before one acknowledgement closes the Task.

`restoration_required` is legal only for a serving-member
`CandidateRestorationProbe` after exact owned inventory observation. It carries
no success evidence and only directs execution to the already-declared
compensation. Foreign/ambiguous identity or failed observation remains failure.
Compensation requires complete restoration evidence and an independent workload
postcondition.

Independent observation opens a concrete read-only descriptor from the validated
original plan, selected recovery step and canonical native witness. It retains exact
historical labels and is accepted only by the Moby observer's restoration method,
not mutation helpers. Appending history to a current plan and resealing it would
falsify ownership and is forbidden. Failure preserves the exact applied projection,
including configured-only state; candidate absence does not imply no Environment
applied record. No desired rollback, serving promotion, resource deletion or
deadline renewal follows from an absence proof.

### Ordinary Release predecessors

Deploy/Rollback captures the exact per-Service serving Release at publication's
fixed read revision through the immutable lifecycle renderer. The Environment
mutation epoch fences it. The staged `ReleaseRenderInput` owns canonical current
and optional retained inactive runtime artifacts, bound by its manifest digest.
No full historical tree or duplicate witness enters the aggregate marker.

The plan binds explicit predecessor artifact, Release, target and optional
inactive-artifact references. Historical labels remain exact. Only that named
predecessor may have historical ownership; it cannot be a forward candidate via
ComposeApply. Scoped predecessor removal and sealed recovery remain valid. An
inactive predecessor needs the shared restoration pair for both captured artifacts.

Claim validates staged witnesses and source revisions, then persists them in
sorted assignment authority. It never selects a predecessor from the latest
Environment applied artifact. Agent admission verifies exact plan-bound bytes,
not IDs alone; terminal recovery checks immutable intent/render lineage and those
bytes before accepting proof. Existing Release history remains readable. An old
unstarted plan without complete authority requires a fresh operation, not a
fallback or rewrite. [Blueprint predecessor storage](blueprints.md#bounded-native-predecessor-references)
owns the corresponding aggregate publication design.

## Acceptance

Prove each lifecycle and explicit removal, immutable selection and replay,
replica-strategy convergence, group order and hooks. Recovery cases include first
startup after configured-only Apply, first startup beside unrelated workloads,
mixed old/new members, changed witness/source rejection and exact retained bytes
after unrelated Environment changes. Exercise actual publication, claim, Worker
admission, reconnect, helper scope, terminal rejection/acceptance and replay.
Live failed then corrected startup must recover through normal surfaces without
manual repair. Independent observation cannot substitute for execution proof.

## Current status

Direct lifecycle and historical singleton deploy/rollback have recorded evidence.
Mixed candidate and predecessor tests cover storage/worker seams, including fake
read-only Engine observation; that alone is not live Docker recovery or full CI.
Uncaptured inactive-slot history remains fail-closed, not generally recoverable.
Exact-count recreate, replicated rollout/recovery and grouped hook qualification
remain open in [capabilities.md](../capabilities.md).

Service observation has a local protocol and Docker foundation: closed
request/result validation, pure state aggregation and a bounded list/inspect-only
observer. Focused race tests, vet and Staticcheck pass with Go 1.26.7. The tests
include the real Docker client over a private HTTP transport and a protobuf
round trip; they do not contact Docker or prove live workload visibility.

Agent/channel worker and session handling are integrated on `main` with focused
race and transport proof. The owner approved the eight composition-only wiring
lines; [head.md](../head.md) records the resolved decision and remaining gates.

`internal/controller/serviceobservation` selects the serving Release and immutable
render input at one storage revision, verifies their digest and workload identity,
and uses the sealed replica count. Portless, proxied singleton and blue/green
workloads retain their exact rendered names. After the bounded Agent read, it
rechecks serving, render, intent and runtime-sidecar revisions at one newer view.
Changed or expired evidence becomes unavailable without failing desired reads.
The Controller clock supplies the 15-second window; no observation is persisted.

Existing Service list/show now expose the public observation through regenerated
OpenAPI and clients. The public reader also fences ready-Agent revision and
generation changes. Unavailable evidence does not fail the desired read;
create/edit responses omit observation. Service reads live in
`internal/controller/serviceread`; `internal/app` only composes them.

The CLI presents the same states, timestamps, serving expectations and seven
counts in table, JSON and YAML. Console feature modules own validation,
projection and local expiry. Visible Environment reads refresh serially every
10 seconds, with one cancellable in-flight request. Provisioning stays separate
from runtime aggregation; desired edits do not replace serving expectations.

Focused Go race/HTTP/CLI checks, Console tests and build pass. An isolated
GET-only browser fixture proves rendering, manual refresh and expiry during a
stalled response. [Visibility evidence](../acceptance/router-and-visibility.md#service-observation)
records the exact scope. Full CI, deployment and live qualification remain open;
[head.md](../head.md) routes the next work. No new endpoint or mutation was added.
