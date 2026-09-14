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

Release preparation preserves profile-disabled definitions when projecting current
Attach memberships. Explicit Deploy selects only its named Service for runtime
planning, even when a Compose profile left it configured-only. Authored profiles
remain in the sealed YAML; this selection does not enable other profile members.

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

Selected lifecycle and restoration checks observe a shared Compose project.
Named Volumes and networks absent from the sealed artifact are outside that
operation's resource scope; their presence cannot invalidate its workload proof.
Collisions on required resources, unidentified resources and unknown collision
kinds still fail closed. Original observation evidence remains unchanged.

Independent observation opens a concrete read-only descriptor from the validated
original plan, selected recovery step and canonical native witness. It retains exact
historical labels and is accepted only by the Moby observer's restoration method,
not mutation helpers. Appending history to a current plan and resealing it would
falsify ownership and is forbidden. Failure preserves the exact applied projection,
including configured-only state; candidate absence does not imply no Environment
applied record. No desired rollback, serving promotion, resource deletion or
deadline renewal follows from an absence proof.

### Candidate runtime bindings

Before publishing an ordinary or grouped Deploy/Rollback, capture the complete
current Attach membership set at the Release planning revision. The Environment
mutation epoch fences that capture. Replace the reserved Attach overlay in the
candidate's private projection, then seal it into the immutable Release input.
Standalone Attach/Detach changes its own records without advancing the desired
Compose revision; an older Entry or Blueprint artifact cannot select a new
candidate's memberships. Desired revision and historical Release bytes stay
unchanged. Predecessor capture remains separate from candidate configuration.

Ordinary Deploy/Rollback publication also retains the exact prepared candidate
Compose artifact from the sealed execution plan. Publication checks the plan
against its candidate descriptor and Task identity, steps and generation, then
stores deterministic bytes in the existing Release publication marker. This is
not the authored Blueprint artifact: native workload rendering and captured
Attach bindings can change it. The existing 256 KiB Release-record ceiling and
transaction limit still apply.

These are prepared execution inputs, not acknowledged runtime. In blue-green
execution the artifact can contain the proxy configuration from before the
sealed switch step. A runtime receipt must bind the actual selected member and
successful activation evidence; copying this artifact alone cannot prove the
post-switch proxy state. A switch changes proxy configuration, not the existing
container's ownership labels. The receipt must retain that container's captured
ownership independently of the newly applied workload and switched configuration.
Failed, skipped or merely staged work must not replace
the last acknowledged inputs. Receipt sources must also remain available after
their originating Task is pruned. Current Attach Task references alone do not
provide that retention. [ADR 0079](../decisions/0079-pinned-task-configuration-recovery.md)
permits restoring only the failed Task's exact pinned pre-operation files, with
ownership and concurrency fences; current desired state is never a substitute.

Pinned-file postconditions are checked independently through the closed
materialization helper in read-only verification mode. The helper reads only the
sealed destination under the Environment bind. It checks the complete input frame
before filesystem access, then verifies file bytes, uid/gid, mode and presence.
Symlinks, hardlinks and unsafe parent paths are failures, not repair instructions.
The destination must still identify the checked inode after hashing. A changed
file is distinct from missing authority or a helper failure. This checker does
not itself restore files; its recovery-execution caller remains pending.

The ordinary Release writer derives each prepared post-activation runtime
from the sealed plan. `ComposeWorkloadApply.EnsureProxy` determines proxy ownership:
when true, Compose applies the candidate proxy; otherwise the receipt keeps the
exact proxy service, ownership and referenced resources from the captured serving
predecessor. It selects the candidate workload, replaces proxy configuration
and YAML with the sealed switch configuration, and retains only referenced
resources. When both Releases use opposite blue-green slots, it copies the prior
serving workload without its proxy from the sealed predecessor artifact. The
publication marker retains these prepared values under its existing 256 KiB
limit; none is applied authority yet.

A successfully completed ordinary Release member writes its self-contained
`service-acknowledged-runtime` record with its checkpoint, terminal summary and
serving projection in one transaction. The source includes Task, plan/hash,
step, assignment, Agent, execution epoch, render generation, effect digest and
acknowledgement time. Proxy proof must match the exact activation configuration;
recreate proof must match the candidate artifact, Release and target. Failed,
skipped, compensated and recovery-required members preserve the existing record.
The current record survives Task pruning and is deleted by successful Service
removal, including the Service finalizer used by ancestor deletion. Original
Release input and terminal history are unchanged.

Terminal batches measure each whole member, including the runtime compare/put,
caller proof guards and physical storage-key prefix. They stop before the next
member would exceed 96 aggregate operations or 1 MiB. An individual member that
cannot fit fails without a partial write; the limits are not increased.

Standalone Attach/Detach also prepares a selected running consumer's runtime from
its sealed plan and an existing acknowledged record. It preserves the stable
proxy, retained inactive workload and their resources; a changed resource shared
with the preserved proxy fails preparation. Equivalent YAML mapping order is not
a resource change. Stopped/configured-only consumers prepare no runtime update.

Publication, claim and successful terminal acknowledgement compare the exact
prior runtime revision and immutable preparation. Completion writes the selected
runtime, Attach outcome and Task outcome atomically. Failed execution, stale
authority and replay cannot promote a prepared value or rewrite an existing
acknowledgement. Missing runtime is rejected, never fabricated from old Release
input; this is not an online migration for installations lacking those records.

Blueprint success prepares a compact member selection from its already-sealed
executed artifact, retaining a distinct inactive slot separately only when needed.
It does not duplicate each current artifact in the publication marker. Successful
completion derives the self-contained runtime and commits it with the Blueprint
Task and serving projection, under the held writer and epoch. Configuration-only,
failed and compensated work cannot promote a candidate. The maximum 32 candidates
with 16 hooks remains within the existing 256-operation arms and 1 MiB envelope.

Entry create/edit/bulk-upsert capture the existing acknowledged runtime at a fixed
revision. Compact Task recipes bind each selected Service's source revision and
new artifact identities. Publication, claim and completion compare those exact
sources. Successful completion atomically advances selected current/retained
workload receipts and Entry metadata, preserving the stable proxy and unrelated
runtime. The aggregate applied Compose artifact remains unchanged; it is not the
selected Service's native authority. Stopped/materialization-only work creates no
runtime receipt. Missing running receipts fail closed without Release backfill.

The shared `common/taskmaterialization` model owns the closed file-source format.
`infra/runtimeconfiguration` stages immutable source sets independently of Task
history; a prepared set is not applied state. Blueprint, Entry and ordinary Release
publication bind the previous acknowledged set in a typed Task field. Claim checks
that head and the retained members; successful acknowledgement advances the head
in the same terminal transaction. Task encoding, cloning and Retry preserve this
authority. An already-applied Environment without it fails closed, without guessing
from historical Release input. Local regression evidence is H16.

`infra/materializationcontent` now retains exact non-secret generated Component
file bytes in bounded immutable chunks. Blueprint and Route producers stage them
before clearing their transient buffers. Delivery and Route replay load the full
record's retained bytes, not output from a later renderer. Secret-derived outputs
are rejected at this store's boundary; their sources stay in the Secret/Entry
stores. Missing or corrupt content fails closed. H17 records the local proof.

Explicit Route create/edit/removal and applied Entry removal now bind the same
configuration and Secret-pin authorities at publication's fixed revision. Their
existing generic acknowledgement owns success; desired-only removal does not
invent a runtime update. H21 records local publication and Entry completion proof.
These writers are not yet an enabled recovery source. Recovery execution still
needs the same authority. The current configuration snapshots retain source
metadata, not resolved Secrets.
Readers must not use a partially maintained record, backfill it from old Release
input, or infer a missing acknowledgement. Consumer cutover and any clean QA
rebuild follow only after those writers and source-retention checks are closed.
Their existence does not authorize further implementation; current work is scoped
by the owner instruction in [head.md](../head.md).

### Ordinary Release predecessors

Deploy/Rollback captures the exact acknowledged per-Service runtime at publication's
fixed read revision. Immutable Release history supplies identity and target
metadata, not reconstructed runtime bytes. Missing or inconsistent receipts fail
closed. The Environment mutation epoch fences the capture. The staged `ReleaseRenderInput` owns canonical current
and optional retained inactive runtime artifacts, bound by its manifest digest.
No full historical tree or duplicate witness enters the aggregate marker.

Blueprint uses the same receipt reader. It retains each receipt revision in its
compact source reference and compares it at publication alongside the existing
serving projection and applied-source fences. Replay retains that exact source;
later Attach or Entry writes cannot silently change the predecessor. The reader
changes only artifact identities, preserving YAML, labels, bindings, proxy bytes
and any acknowledged inactive slot. H20 records local producer and store proof;
file-restoration execution and the live failure journey remain pending.

The sealed candidate procedure can bind one probe/restore pair per forward file
write. Each pair records the previous snapshot digest or explicit initial
absence, and restores only that forward destination. `configurationrecovery`
selects the retained source and derives repeatable execution IDs; the original
content-store identity remains unchanged. Snapshot loads use the exact immutable
reference at a fresh fixed storage revision, without selecting a current head.
An older Component file can serve a later Task only with its exact original
record and bytes. H22 records these local source and plan checks; Agent recovery
execution and the real failure journey remain separate qualification.

Durable recovery accounting selects a file compensation only after its forward
write has recorded Running or Completed progress. It records files in sealed
order before native effects; reconnect reconstructs the same recovery sequence
and cursor. File probes precede native probes, then file restoration precedes
reverse native restoration. Untouched files and host observations cannot create
write permission. Native completion requirements remain unchanged. H23 records
the local journal and replay checks, not a live restoration.

Controller planning captures file sources before sealing the candidate plan;
publication reuses that snapshot identity and fences the prior head. Replay and
delivery resolve the original retained source, not the candidate's current file.
The Agent waits for durable Running acknowledgement before a forward file write.
A failed file-bearing candidate enters durable recovery, where file restoration
precedes native compensation. Every restored file is independently verified with
the read-only helper before reporting completion. Native health is probed again
after file restoration. Reconnect retains the exact plan, execution epoch and
deadline; it cannot extend the recovery budget. Recovery transfer buffers survive
repeated proof reads within that assignment and are cleared on retirement.
H24 records local Controller/store and Agent checks. The full failed-deployment
journey and real helper execution on QA remain unqualified; this does not add a
generic file-only Task recovery procedure.

The plan binds explicit predecessor artifact, Release, target and optional
inactive-artifact references. Historical labels remain exact. Only that named
predecessor may have historical ownership; it cannot be a forward candidate via
ComposeApply. Scoped predecessor removal and sealed recovery remain valid. An
inactive predecessor needs the shared restoration pair for both captured artifacts.

Proxy rollback uses the predecessor's captured configuration bytes, digest and
generation. A mutable ledger revision is not proxy generation, and current names
or exposed ports cannot reconstruct historical configuration. The next proxy
generation advances beyond the serving generation. Conflicting rollback metadata
fails before Task publication; terminal proof still requires the exact captured
generation and digest and identifies which check failed.

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
request/result validation, pure state aggregation and a bounded workload/proxy
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

The approved proxy correction extends that read with exact acknowledged proxy
ownership and configuration, fenced by its runtime receipt revision. A fixed
local Caddy admin GET compares live configuration with the acknowledged digest.
An absent/stopped or mismatched proxy prevents healthy/running status; a failed
or ambiguous observation is unavailable. Proxy containers never become workload
replicas. This check detects the stale-target boot failure but does not replace
independent routed application checks in QA.

Proxy startup and activation share one closed procedure in
`internal/common/serviceproxy`. New containers initialize their private runtime
file from sealed Compose input. Switch and compensation stage complete bytes,
sync and atomically rename them before reload, then verify both runtime-file and
live configuration digests. Restart loads that selected file; recreation starts
from its own sealed input, without inherited selection from a Volume. Candidate
startup explicitly starts an existing stopped stable proxy without reconciling
its new Compose definition or replacing a running proxy. It uses the shipped
Compose's supported `start` command; activation owns the bounded readiness check.
The command regression executes the shipped tooling, rather than only comparing
generated arguments. REL-02, SVC-06/SVC-15
and OBS-05 cover these sequences; implementation proof is not live qualification.

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
records the exact scope. Fresh QA now proves healthy observation of 11 Services
and 12 workload replicas, CLI/API semantic parity and matching Console runtime
status. An exact Blueprint reapply preserves every serving Release. Full CI,
failure-state and recovery qualification remain open; [head.md](../head.md) records
the private evidence and application-level blocker. No new endpoint or mutation
was added.
