# Implementation checkpoint

Status: live handoff for implementation work, dated 2026-09-02. This document
is implementation evidence only; it is not product authority. Product behavior
and vocabulary remain authoritative in `docs/mvp.md`, with the source-of-truth
order in `docs/agents.md` resolving conflicts.

The integration owner MUST update this file in the same commit whenever a lane
lands or the remaining set changes. Branch names, candidate counts, and this
checkpoint do not prove acceptance by themselves; acceptance requires the
evidence named by `docs/capabilities.md` and `docs/delivery.md`.

## Active Controller and Agent settings separation

The source implementation and generated API clients now contain the ADR 0063
separation, but this lane is not yet accepted or landed. Platform Host is
health-only and links to dedicated Controller and Agent pages. Agent runtime
configuration remains on the Agent resource. The Controller has an exact YAML
document API, CLI commands, and Console editor backed by startup-parser
validation, SHA-256 optimistic concurrency, durable exact idempotent replay,
atomic mode-0600 replacement, and restart-required reporting against the running startup snapshot.

Remaining evidence is bounded to focused Go package checks, the Console
typecheck/build, generated-artifact cleanliness, and one live read/edit/revert
journey. Do not mark C01 Accepted again or deploy this lane until those checks
prove the exact generated tree.

## Current delivery pipeline

Independent feature lanes now run continuously in parallel. Each lane has one
durable repository-local worktree and branch, one writer, and disjoint owned
files; all repository-managed temporary state uses ignored repo-local `.tmp/`
or `.tmp-*`; system `/tmp` is forbidden. A lane freezes only after
implementation and its named focused proof, receives one aggregate
blocker-only review, and gets at most one bounded correction batch followed by
one delta-only verification. Corrections rerun only the exact tests covering
their changed behavior; non-blockers are recorded in `docs/issues/` and do not
hold landing. Approved lanes land immediately as signed straight-line
fast-forwards, and their worktree and branch are cleaned up only after `main`
contains the exact landed tree. The root agent owns time/token circuit-breaker
interrupts and diagnosis; no repeated full review loop or unlanded WIP cleanup
is permitted.

The CoreDNS transactional lifecycle landed on `main` in `3884d075`.
Independent remaining features continue in their own durable worktrees.

On every compaction or restart, the head agent first rereads the four mandatory
recovery documents, queries actual agent status, and reconciles it with durable
worktrees and frozen candidates. Completed agents are not active capacity.
Stopped lanes advance independently into freeze/review/correction/landing, and
every freed slot is immediately refilled with the next contract-closed,
disjoint writer lane. Reviewers never wait for unrelated writers, and reviewers
never start before their corresponding writer has stopped and the candidate is
frozen.

Pipeline restoration now has an enforceable occupancy criterion: running
delegates must equal the lesser of free delegated slots and eligible delegated
actions. Ordinary writers, reviewers, correction batches, and bounded contract
scopers are subagents; the head coordinates, adjudicates, constructs signed
direct-child candidates, lands, cleans, and refills. A compaction that leaves
the head as the sole worker while eligible delegated work exists has not
restored the pipeline.

## Accepted and integrated checkpoint

The current capability ledger marks these slices **Accepted**: `C01`, `C02`,
`C03`, `C06`, `C08`, `C10`, `C11`, `C14`, `C15`, `C18`, `C19`, and `C20`.

The accepted commits landed on `main` in the current checkpoint are:

- Controller reflection (`bc67c752`)
- Activity scope (`6dae5f93`)
- CLI volume (`d813e337`)
- Script checkpoint (`78ac8b8c`)
- Typed persistence (`861914fc`)
- Continuous pipeline and legacy-surface removal (`fb524d0f`)
- Component authority (`2f399e99`)
- Compaction recovery and circuit-breaker enforcement (`d50497d7`)
- Nullable closed Component configuration (`f1ae44ab`)
- Closed Component Capability boundary (`d7c03522`)
- Hierarchy deletion replay correctness (`38c5f33d`)
- Repeatable host deployment (`1916ef5f`)
- Deterministic deployment SSH configuration isolation (this status-changing commit)
- Transient Environment and Service logs (`86ea9bf2`)
- Blueprint Script desired-state reconciliation (`d0e7297d`)
- Generic HTTP Route reconciliation (`cc0ff8e9`)
- Console live operator state (`591eb215`)
- Runner token-loss retry and broker ownership semantics (this status-changing commit)
- Cloudflare Tunnel independent edge-tunnel runtime (this status-changing commit)
- Blueprint omitted-resource preservation (`56493253`)
- Resolver host projections (`24e07efc`)
- Caddy template configuration (`c1f39188`)
- Generic HTTP-router origin projection (`a7cd6d85`)
- Prepared Script source-reference contract (`39e919e7`, `177e0913`)
- Hardened prepared Script source publication authority (this commit)
- Backup protocol contract (`31b80d82`)
- Canonical Backup artifact foundation (`1397d587`)
- Script materialization-proof foundation (`f9acf442`)
- Continuous worktree delivery contract (`09d8b03d`)
- Native private workload Tunnel Secret QA driver (this status-changing commit)
- Complete CoreDNS capability lifecycle (`3884d075`)
- Provenance-fenced CoreDNS clean-start bootstrap (this status-changing commit)
- Claimable idempotent automatic CoreDNS Task publication (this status-changing commit)
- Exact automatic CoreDNS execution-plan hash sealing (this status-changing commit)
- Overridable CoreDNS validation port (this status-changing commit)
- Cache-first exact-digest registered-image acquisition (this status-changing commit)
- Least-privilege registered-validator execution capability (this status-changing commit)
- Post-authentication Agent stream fault recovery (this status-changing commit)
- Sealed inverse Compose lifecycle compensation (this status-changing commit)
- Serialized Compose Engine calls for nested managed-Volume population (this commit)
- CoreDNS effective parsed-configuration observation proof (this status-changing commit)
- CoreDNS full-template authoring through the existing config vertical (this status-changing commit)
- Host Console startup/runtime configuration boundary (this status-changing commit)
- Blueprint Script apply-wins regression proof (`b71d6054`)
- Fixed-snapshot HTTP Route provider planning (`3884d075`)
- Console live release projection and task-state honesty (`11cfff96`)
- Console API task and Service-form parity hardening (`6f2e383d`)
- Mandatory post-compaction pipeline restoration (`0fa72a09`)
- Complete release Script hook runtime (`767f04c7`)
- Release Script hook ordering, recovery compensation, and serving-state authority (this commit)
- Bounded native private acceptance topology hosting driver (`4224df75`)
- Target-registry pull for the bounded private workload `--skip-images` driver (this status-changing commit)
- Zero-Component backing-service aggregate transaction shape (this status-changing commit)
- Complete backing-service desired-topology publication (this status-changing commit)
- Bounded atomic backing-service bootstrap transaction envelope (this status-changing commit)
- Collision-free Component and Environment materialization acknowledgement ownership (this status-changing commit)
- Exact deployment RepoDigest parsing (`b3e4ca3e`)
- Mandatory head-agent capacity saturation (`e22d418d`)
- Collision-free Backup wire and Volume projection contract (`fc3ed992`)
- Atomic Controller deployment rollback (`691bff3d`)
- Architecture gate ratchet restoration (`95d2d93a`)
- Typed Huma log and Task-event streams (this commit)

Other branch refs are not acceptance evidence until the integration owner
constructs and lands their signed direct-child candidate.

## Rejected CoreDNS candidates

Signed candidates `61473e56`, `815aa683`, `2ff1575c`, and `18fb7093` are rejected and MUST NOT land.
The first review found runtime rollback, contentless observation, successor
lineage, absence CAS, DNS proof, and disable-ordering blockers. The second
review confirmed three remaining blockers: partial transaction metadata could
poison deterministic helper replay, self-hashed semantically empty DNS evidence
could cross the shared proof boundary, and failed enable compensation tried to
prove a disabled predecessor as serving. The third review additionally found
mutable runtime recomputation of a sealed update plan, pre-promotion successor
planning, non-durable rollback-to-absence ordering, missing disable rollback
serving proof, and a file-versus-directory mount mismatch. This clean replacement uses atomic
transaction-directory publication, validates DNS semantics before ACK and
durable projection, and distinguishes disabled-state enable rollback from the
serving rollback required by update and disable.
The fourth review found that the Agent assignment boundary rejected the generic
Component apply operation carried by a Component-owned update Task. This
replacement authorizes that operation only when the sealed Task resource kind
is `component`; ordinary update Tasks remain unable to select it.
Signed candidate `35e6861f` was superseded before review because the inherited
clean-start path did not seal its automatic-reconcile marker or the required
two-step update procedure. This replacement uses the same closed Task shape as
subsequent automatic Component reconciliation.
Signed candidate `797fa6f9` is rejected and MUST NOT land. Its review found
that operator mutations did not acquire the automatic resolver's per-Component
active fence and that stale-projection successors incorrectly identified new
operations and plans as retries. This replacement unifies the active fence for
all Platform Component attempts and reserves `retry_of` for exact retries.
Signed candidate `58452f21` is rejected and MUST NOT land. Its review found
that an operator-origin retry did not reacquire the shared active fence and
that pending abort bypassed Platform Component terminalization, leaving the
fence permanently owned by an aborted Task. This replacement routes retry and
pending abort through the same authoritative fence lifecycle as assignment and
acknowledgement.
Signed candidate `1bdf150c` is rejected and MUST NOT land. Its review found
that a serving CoreDNS update which reapplied its generated Service was sealed
as a first enable and could remove the serving predecessor during compensation,
and that an exact retry reused the managed-config transaction already marked
rolled back by its prior Task attempt. This replacement derives lifecycle mode
from sealed predecessor evidence independently of Service-apply procedure shape
and scopes helper transaction identity to the immutable Task attempt.
Signed candidate `0e5e932d` is rejected and MUST NOT land. Its review found
that the generic sealed-plan validator still restricted every four-step
Component Service-apply procedure to first enable, rejecting the corrected
serving-predecessor update before dispatch. The same review found that update
compensation did not retain the predecessor Compose artifact and therefore
could not restore its image or ownership labels after candidate Service apply.
This replacement seals both exact artifacts for a Service-changing update,
validates their distinct ownership generations, and reapplies the predecessor
after managed-config rollback before accepting rollback serving proof.
Signed candidate `edb3a424` is rejected and MUST NOT land. Its review found
that an enabled but unhealthy predecessor could still be misclassified as a
first enable, rollback Compose apply selected the candidate step from the
original sealed plan, rollback observation retained the candidate config
identity, and retry-origin lineage could block stale-projection convergence.
This replacement treats every enabled observation as a serving predecessor,
fails closed when an existing generated Service has no observation, executes
Compose restoration through a derived validated predecessor plan, seals the
predecessor config id and generation, and preserves the originating render
input across retry successors.
Signed candidate `78e1719e` is rejected and MUST NOT land. Its review found
that an already-enabled Component with no durable observation could still be
treated as a first enable when its generated-Service projection was empty.
This replacement fails that ambiguous state closed while preserving a genuine
disabled-to-enabled first activation.

Signed candidate `e11fcdd2` is rejected. It conflated the registered
Environment-plan digest with the final generic Agent execution-plan digest,
sealed automatic successors before their final predecessor and step authority,
and left operator lifecycle Tasks without the required execution hash. This
replacement persists both authorities separately, seals only final Task inputs,
and preserves strict drift checks across startup, operator lifecycle, automatic
successor, and configured-volume-root paths.

Both rejected commits and their trees remain forbidden. This commit is the
replacement implementation; `C07`, `C12`, and `C13` remain **Scaffolded** until
the reference-host capability journey supplies their acceptance evidence.

## Completed baseline item

Component authority is complete in `S06` through integration commit
`2f399e99`; it is not part of the remaining baseline set.

The foundational remediation set is also integrated: reflection-based
conversion recovery is removed, persistence boundaries are typed, Script
checkpoints are typed, Component task authority is sealed, Component config is
a nullable closed union, registered Components use the closed Capability
boundary, and hierarchy deletion replay is fixed-revision and race-safe.

ADRs 0047 and 0048 are accepted together as of 2026-08-30. This removes the
C16 protocol-decision blocker. The canonical protocol-independent artifact,
Volume archive, PostgreSQL dump, age-size, and object-boundary foundation is
also landed. Generated terminal-receipt persistence and delivery,
Config/Volume transfer, staging recovery, managed PostgreSQL release/runtime,
capture/upload, Restore, live R2 proof, and end-to-end acceptance evidence
remain pending; C16 stays Scaffolded and the Console Restore action stays
unavailable.

The schema-1 wire table now follows the live post-Script protocol without tag
reuse: Backup Agent additions begin at tag 11, Controller additions at tag 12,
and TaskAssignment Backup authority uses tags 11 through 14. Volume capture
and Restore carry the complete immutable ADR 0047 projection at tag 5 while
retaining reserved tag 4 and `consumers`. The prior tag collision and missing
Volume-authority placement are closed; Backup runtime implementation remains
the next C16 work.

## Architecture gate checkpoint

The Component SDK standard-library allowlist regression, two forbidden
reflection imports, stale oversized entry, and missing finite test baseline are
closed without widening the Agent-channel test baseline. The architecture gate
still reports unrelated production size/frozen-total drift and the separate
CoreDNS concrete-import finding; those remain MVP-required before final CI.
`make architecture-check` remains a required final-CI gate.

The CoreDNS replacement adds one non-production size finding for
`internal/controller/agentchannel/server_test.go`; it is recorded in
`docs/issues/coredns-agentchannel-test-size.md` and does not block this landing.
It remains MVP-required because the final candidate must pass the complete
architecture gate.

## Remaining baseline steps

Ten baseline delivery steps remain in the current capability ledger:

1. `S01` Generated API foundation
2. `S02` Durable hierarchy
3. `S03` Desired resources
4. `S04` Agent task channel
5. `S05` Deploy and rollback
6. `S07` Backing services and attaches
7. `S08` Entries and secrets
8. `S09` Connectors and backups
9. `S10` Runners and release groups
10. `S11` Production delivery

Each row remains subject to its required evidence and the final MVP completion
rule in `docs/capabilities.md`.

## Active implementation lanes and unfinished evidence

Release post hooks now run from the sealed candidate after start and before
readiness, so migration-dependent healthchecks do not deadlock. Retry probes
compensate only candidate-touched members, compensation failure suppresses
failure hooks, and `on-failure` runners start only when final Agent serving
evidence matches their policy-selected candidate or predecessor Release. An
absent or ambiguous serving identity completes the durable no-start path
without guessing. Prepared Blueprint Script sources now use a bounded,
restart-safe authority with exact existing-or-staged evidence, canonical owner
and digest validation, atomic activation requirements, and idempotent
abandonment of partial preparation. Blueprint apply now seals its exact
candidate projection before downstream planning and final publication consumes
a locator-bound, copy-safe, single-use authority. Known prepublication failures
abandon the exact staged claim with a bounded context independent of request
cancellation; unknown publication outcomes remain unresolved until durable
idempotency evidence identifies the winner. The closed internal Release
operation vocabulary now includes `blueprint_apply`; it does not create a new
operator capability or Task. Blueprint execution plans now carry a distinct
sealed `blueprint_apply` operation across the Controller/Agent boundary;
ordinary reconciliation cannot authorize Script execution, and every
Blueprint post-deploy Script must follow the exact candidate Compose apply with
matching Service, Release, and image authority. Blueprint apply still publishes
Script desired state without creating candidate Releases or executing hooks.
The authoritative contract now requires the one Blueprint Environment update
Task to publish and execute bounded post-deploy Scripts for eligible running
singleton Services, while preserving stopped/absent intent and excluding
Release Group members. Candidate Release/Script publication and terminal
promotion, compensation, and retry remain the next C21 implementation lanes
required by the private clean-start topology.

The desired-topology landing candidate cleanly replaces the transitional flat
Zone and topology projection authority. Environment revisions now carry the
sorted `DesiredZones`, `DesiredServices`, and `DesiredRoutes` sets selected by
the durable desired head. Zone and Route removal stage immutable candidates,
promote the exact candidate only after terminal success, and retain the
selected predecessor on failure, abort, and retry. Generated Component Services
remain outside `DesiredServices` and ordinary Service surfaces; enabled Component
runtime identities and ownership extend the immutable normalized Compose artifact's
exact coverage. The deleted identity types and
`services`, `networks`, `routes`, and `suppressed_routes` projection fields have
no compatibility reader or fallback authority. Focused persistence, Controller,
App, and Volume proofs pass on the combined tree. Reference-host publication is
the next acceptance step; this source evidence does not claim host acceptance.

Signed candidate `cca6f4a2` is rejected and MUST NOT land. Its two blind
reviews found that desired-only Route mutations could not start without an
enabled router, Route removal still depended on obsolete flat records, Route
observations used an absence-only CAS that blocked edits and retries, failed
mutation replay selected the wrong projection, Zone deletion conflated desired
head and applied projection revisions, and generated Component Services leaked
into ordinary Service reads and mutations. The replacement uses desired-head
only Route lifecycle authority, revision-aware observation replacement,
replay-stable predecessor/candidate selection, separately fenced Zone
authorities, and Component-ownership filtering across every ordinary Service
surface. The exact Route, Zone, and Service delta proofs pass on the combined
replacement tree.

The remaining isolated implementation and acceptance lanes are:

1. CoreDNS reference-host capability acceptance for the landed implementation.
2. Backup terminal delivery, capture/upload, Restore, and runtime composition.

CoreDNS now persists one required operator-authored full Corefile template,
validates and substitutes its single `{groundplane}` marker in the registered
SDK-only renderer, and exposes authoring through the existing API, CLI, and
Console config action. The existing config GET now exposes the generic typed
managed-file projection with the durable Template and registered-renderer
Corefile output. It reads only the already-persisted resolver baseline and
current host-resolution projection, has no capture or persistence authority,
and adds no endpoint or CoreDNS-specific Agent procedure. API, CLI, and Console
surface the same preview; reference-host acceptance remains pending.

The bounded native private acceptance topology journey completed on `the disposable QA host` on
2026-09-01. The production Controller serves the embedded Console on the
approved loopback and host listeners; the Controller-managed Agent and CoreDNS
Component are healthy; PostgreSQL and Valkey backing services are healthy; all
application Attaches are ready; and the recreated application and identity
services are running from the Groundplane projection. The Environment Caddy
Component is enabled and healthy at its allocated address, all six HTTP Routes
report `served`, the `/up` route returns HTTP 200, and WebSocket completed a
WebSocket upgrade through Caddy. The `edge-tunnel` Component remains authored
but disabled for this no-Tunnel acceptance run.

The 2026-09-01 clean reference-host deployment reaches an active Controller on
both approved listeners, completes Agent enrollment and CoreDNS activation,
reuses the target registry's immutable Agent, Runner, application, and identity
images, and creates the bounded private workload Tenant, Project, and ready Environment.
The first PostgreSQL backing-service publication exposed that tenant-only
Environment Components made the aggregate exceed the fixed 96-operation
transaction ceiling before durable publication. This commit removes those
Components from backing aggregates while retaining their required Zone,
Service, Volume, Entries, encrypted credential, and Agent Task; the corrected
retry then exposed that backing creation omitted its generated Zone and Service
from the desired topology before Volume-mount preflight. This commit publishes
that exact topology with the Service's backing-network ownership preserved;
the next clean retry reached the atomic backing-facade publication and exposed
that its `43/37/43` compare/success/failure shape does not fit ADR 0051's
ordinary `32/32/32` Blueprint partitions. This commit retains all-or-nothing
facade visibility and applies a backing-bootstrap-only bound: the selected
compare-plus-success arm remains at most 96 operations and the full
compare/success/failure request remains at most 128 operations. Ordinary
Blueprint publication remains unchanged. The corrected build was deployed and
the bounded topology journey completed as recorded above.
Live
execution proves automatic CoreDNS publication is claimable and its registered
and generic execution-plan authorities match. After the exact immutable image
was present, the validator still exited because the generated Corefile fixed
`.:53` and prevented its safe `-dns.port 0` validation override. This commit
uses the default-port Corefile form so validation can bind an ephemeral port
while the serving Service still defaults to port 53. Clean-host evidence now
confirms that the Agent acquires a missing catalog-authorized validator image by
its exact immutable digest before container creation. The validator retains its
nonroot, network-none, read-only, all-capabilities-dropped, no-new-privileges
profile while preserving only `NET_BIND_SERVICE` in the bounding set required
to execute the pinned CoreDNS binary. Reference-host activation retry and
topology proof remain next.

The old speed-phase Backup capture/Restore and Script hook branches are rejected
as incomplete source material and are not landing candidates. Unfinished
evidence areas include CoreDNS reference-host capability acceptance, Backup and
Restore, complete Console / CLI / API 1:1 coverage, and the complete operator
journey. These are implementation work
items, not permission to change the product contract or skip a landing blocker.

## Private acceptance workload

The private acceptance workload, its deployment driver, topology, credentials,
and detailed QA evidence remain in the repository-local ignored workspace. The
public repository records only generic product behavior and capability status.

## Disposable QA host authority

The owner designates `the disposable QA host` as a disposable QA/testing host and explicitly
authorizes Groundplane state on that machine to be repaired, reset, or wiped
without another approval. Preserve the source repository and any non-Groundplane
data. Prefer the smallest repair that restores the journey when it is faster;
use a clean reprovision when accumulated test state makes that more efficient.
The deterministic host driver and focused dry-run contract are implemented;
the corrected bounded real-host execution and observations remain pending.

## Attach selected desired-head correction (2026-09-01)

The no-Tunnel `private workload/` operator journey reached Attach creation after native Blueprint apply and direct Entry/file publication. Attach creation incorrectly required the selected desired revision to have a historical Blueprint audit record, so a valid direct-mutation head failed with `Attach Blueprint head is missing`.

Attach mutation authority now consists of the versioned Environment desired head and its exact immutable Compose projection revision. Creation and detach compare the desired-head key and the immutable projection root independently; no synthetic Blueprint audit alias or compatibility path is introduced. Focused race tests pass for the app, etcd, and controller Attach paths. The next QA action is to deploy this commit and resume the same operator journey at Attach creation.

## Edge-tunnel contract correction (2026-09-01)

The Cloudflare Tunnel registered Component now provides the generic
`edge-tunnel` capability. Groundplane retains only its Secret reference,
resolves the token transiently, starts or stops the connector, and reports
health. Caddy enablement and health are no longer prerequisites. Tunnel DNS,
public hostnames, ingress rules, origin targets, and protocol remain outside
Groundplane authority. No old capability key or compatibility path remains.

## Environment Blueprint authoring surface (2026-09-01)

The write-only Environment Blueprint contract is replaced in source by a
canonical authoring read, side-effect-free typed validation diff, and
revision-fenced apply. The CLI now uses `environment blueprint
show|validate|apply`, and the Console Blueprint tab loads Controller YAML
instead of fabricating desired state from frontend projections. The authoring
serializer excludes runtime identities and observations and emits multiline
Script bodies as literal YAML blocks. Generated API artifacts and focused
execution evidence remain required before this source slice changes C21 from
`Scaffolded`.

## Blueprint Script staged source authority (2026-09-02)

Schema-1 Script runner sources now use one strict existing-or-staged authority. Existing sources carry a positive MVCC revision; same-Blueprint candidate sources carry the exact Environment revision/render stage, fixed read revision, and canonical value digest. Shared validation rejects empty, dual, cross-stage, or tampered authority; manual Script plans remain existing-only. Candidate Release and Script publication with complete ADR 0062 membership remains the next C21 lane before the private topology can be accepted.

## Blueprint candidate Release, Script, and image authority (2026-09-02)

Blueprint apply now builds eligible changed singleton Services from the sealed candidate projection, publishes their candidate Releases and durable post-deploy Script executions in the one Environment update Task, and binds every prepared Script source to exact existing-or-staged authority. Legal tag-authored images are resolved only by the Agent after the sealed candidate Compose step; a typed create-only execution-step result records the proved immutable image and must be Controller-acknowledged before the runner can start. Exact replay validates the current assignment and reuses that evidence without another mutation; conflicting or stale replay fails closed. Successful terminal publication stores typed resolved Release image evidence without mutating immutable intent, so public Release reads and later Script plans use the digest-pinned image. Component and ordinary Compose operations remain outside this authority. Candidate compensation and retry completion remain the next C21 runtime lane before private topology acceptance.

## Blueprint candidate terminal and retry runtime (2026-09-02)

Successful Blueprint Environment update Tasks now promote the exact candidate Releases, applied Compose projection, Component candidates, and Route observations in one aggregate-budgeted terminal transaction without rewriting the desired head. Failure terminalization requires strategy-specific proof of exact predecessor restoration or first-candidate absence; reconciliation-required outcomes remain nonterminal. Retry preserves the sealed predecessor, Release, attempt, source, and image epoch and transfers only while every Script execution is durably not_started; missing, corrupt, started, stale, or unknown lineage fails with script.retry_unsafe and publishes no successor. Terminal replay validates the retained manifest, evidence, image, summary, and attempt postconditions before accepting success.

## Blueprint Script namespace registration (2026-09-02)

The exact-main private topology run exposed that the canonical root `x-gp-scripts` extension was omitted from namespace validation, rejecting valid Blueprint input before parsing. This lane registers the missing canonical key and adds focused acceptance and unknown-extension rejection coverage; the reference-host rerun remains pending.

## Blueprint Volume slug placement (2026-09-02)

The next exact-main private topology run exposed that the documented top-level Volume `x-gp-slug` extension was absent from namespace validation while the unrelated root `x-gp-backup` extension was incorrectly admitted on Volumes. This clean replacement registers `x-gp-slug` only for top-level Volumes and rejects the old wrong-scope placement; the reference-host rerun remains pending.

## Blueprint Release task restart shape (2026-09-02)

The exact-main private topology run created the valid 18-step Blueprint Environment update Task but restart assignment quarantined it because the resolver counted an extra full-reconcile step and omitted sealed post-deploy hook steps. Candidate Release restart validation now derives the shared bounded procedure shape, reproduces each apply, hook, and health step in durable order, and validates the hook member and Script execution bindings before Agent assignment. Focused Controller and Blueprint Release regressions pass; reference-host rerun remains pending.

## Blueprint Compose procedure restart shape (2026-09-02)

Environment Tasks now persist one closed Compose procedure kind: no Compose
mutation, full reconcile, or candidate Releases. Blueprint apply, Entry
mutation, and backing-service producers write that authority explicitly, and
restart validation rejects mismatched Release or health parameters before
reproducing the exact step count and operation. A no-candidate Blueprint keeps
its materialization, managed-Volume, Attach, and Component work without an
invented full reconcile; ordinary Entry reconciliation and backing creation
retain their explicit full-reconcile procedure. Focused producer and restart
regressions cover both shapes; reference-host rerun remains pending.

## Blueprint prerequisite gate authority (2026-09-02)

The root `x-gp-requires` grammar is now closed to registered `backing-attach` targets, `exists|ready|completed_successfully` conditions, and nonempty `start|deploy|rollback|always` phase sets. Blueprint apply resolves each authored Attach label once at a fixed pre-stage revision, seals its stable Attach id, producer Task id, MVCC revision, exact Task DAG, phase plans, and final step ids, then publishes that gate atomically with the Environment desired head and sole Task. Agent claim fails closed on missing, mismatched, corrupt, unmet, or raced gate evidence; retry and exact replay retain the original DAG root and digest. Same-Task Attach dependencies, missing targets, duplicate requirements/phases, and cycles are rejected before execution. Registered Component capability availability remains validated by the existing closed catalog before staging. Focused source proof is required before landing; reference-host acceptance remains pending.

## Public Blueprint Script Task step evidence (2026-09-02)

Blueprint Environment update Tasks now persist the selected non-secret Script
id and slug on each exact post-deploy RunScript step. Task detail projects that
immutable evidence while ordinary steps omit both fields; durable validation
rejects partial, invalid, or ambiguous duplicate metadata. Focused persistence,
producer, and task.show contract regressions cover the public schema.

## Blueprint Script Task-step identity (2026-09-02)

Every durable and public Task step now carries one closed `operation|script`
kind. Script steps require their paired stable Script id and slug, operation
steps reject Script identity, and missing, partial, or unknown variants fail
closed without a compatibility reader. Blueprint `RunScript` steps publish the
exact durable identity through Task detail and events; OpenAPI and both
generated clients expose the same closed contract. Focused race proofs, the
complete internal compile graph, and deterministic API regeneration pass.
Reference-host Blueprint acceptance remains pending.

## Host-resolution helper clean-start correction (2026-09-02)

Reference-host clean bootstrap proved that the isolated host-resolution helper
could not replace the root-owned resolver target after dropping every Linux
capability. The helper now restores only `DAC_OVERRIDE` while retaining a
read-only root filesystem, network isolation, `no-new-privileges`, and all
other capabilities dropped. Focused isolation proof passes; exact-host
Blueprint acceptance remains pending.

## CLI Task-step projection parity (2026-09-02)

The generated-client adapter now preserves the mandatory `operation|script` Task-step
kind and the paired Script identity in CLI Task detail, rejecting invalid closed-union
responses instead of emitting empty or partial fields.

## Repository-local deployment staging (2026-09-02)

The deployment workflow no longer stages archives in system `/tmp`. Local build
scratch is confined to the ignored repository `.tmp/` hierarchy, and the remote
bundle uses the root-owned `/root/.groundplane/.tmp/` hierarchy with exact-path guards.

## CLI Component closed-union response decoding (2026-09-02)

Component list and detail now read generated raw HTTP responses once under the
existing response-size bound and pass successful bodies through the strict
public Component decoder. Valid CoreDNS responses no longer fail in the
generated eager one-of parser, while malformed closed-union variants still
fail. Focused CLI adapter proof passes; reference-host Component list and
detail remain pending.

## Initial Blueprint Component candidate projection (2026-09-02)

The first desired publication may prepare an enabled Environment Component
against newly staged Zones while no runtime-applied Environment projection
exists. Any existing Zone must match the exact record and revision selected
from the runtime-applied projection, and atomic publication CAS-fences that
projection's presence or exact revision. A no-Release Blueprint retry is
identified by the closed Compose-procedure marker and reacquires its exact
candidate address; ordinary Component retries continue to require an applied
projection. Focused persistence regressions pass; reference-host Blueprint
acceptance remains pending.

## Immediate Agent Task wakeup (2026-09-02)

Accepted Controller mutations now send a coalescing wake to connected Agents
after durable acceptance, allowing queued Tasks to use the last valid Ready
capacity without waiting for another poll. A pending Agent configuration change
invalidates old Ready capacity before drain completes. Assignment lifecycle
fencing closes new admission, waits outside the global Registry lock, and is
bounded by the caller context even when an admitted send is uncooperative.
Transient Script, Secret, and Entry bytes are cleared even when fenced
admission rejects the resolved assignment. Focused race proof passes.

## Blueprint profile-disabled Service selection (2026-09-02)

Implicit Blueprint candidate Releases and post-deploy Scripts now select only
active authored Services from the sealed normalized Compose projection. A
profile-disabled Service retains its desired change for a later explicit
deployment but is never silently activated or hooked by Environment Blueprint
apply. Existing stopped, absent, replicated, and Release Group exclusions stay
unchanged. Focused candidate-selection race proof passes; reference-host
Blueprint acceptance remains pending.

## Managed-config clean-start ownership recovery (2026-09-02)

An absent managed-config target now journals the exact orphan owner observed
during preparation. Replay accepts only that recorded ownership state or the
transaction's own owner, so a stale transaction cannot adopt or remove a
same-byte successor published by another transaction. Focused helper race proof
passes; clean-host CoreDNS activation and Blueprint acceptance remain pending.

## Blueprint profile-activation candidate selection (2026-09-02)

Clean-host execution proved Agent enrollment, automatic CoreDNS activation,
backing-service publication, and Attach readiness before exposing that an
Environment Blueprint transition from profile-disabled to active Services
could complete without candidate Releases or post-deploy Script steps. Candidate
selection now carries the exact predecessor and candidate normalized Compose
membership: profile-disabled to active is a material transition even when the
flattened Service record is unchanged; active to profile-disabled remains
desired-only; and absent or ambiguous membership fails closed. Focused
Controller and application race proofs pass. The exact reference-host Blueprint
and Script acceptance rerun remains pending.

## Blueprint prerequisite authoring round-trip (2026-09-02)

Canonical Environment Blueprint authoring now preserves the persisted authored `x-gp-requires` graph, and validate/apply accept the already-closed registered prerequisite grammar instead of rejecting every nonempty requirement. Fixed-revision Attach resolution and durable execution-gate authority remain unchanged. Focused application and Controller proofs pass; generated-artifact and reference-host Blueprint acceptance remain pending.

## Blueprint predecessor Compose identity (2026-09-02)

Blueprint candidate Release planning now carries the already-sealed predecessor serving Release, target, and proxy generation into the generic Compose render identity. Strict render validation remains unchanged and rejects incomplete authority. The focused Blueprint Release-plan proof passes; exact reference-host apply remains pending.

## Blueprint candidate workload image binding (2026-09-02)

Blueprint Release image binding now selects exactly one matching candidate workload role for each stable Service identity while ignoring its legitimate same-id stable proxy. Missing, mismatched, or duplicate candidate workloads still fail closed. Focused Controller regressions pass; exact reference-host apply remains pending.

## Blueprint non-Entry omission authority (2026-09-02)

Environment Blueprint and direct Route create/edit publication now reject silent Service, Zone, or Route identity loss with `resource.in_use`; a pending Remove-shaped Task cannot authorize omission. Existing canonical authoring continues to carry omitted resources forward, sealed Route removal remains separate, and exact completed Volume removal remains accepted. Focused persistence, Route transaction, and application carry-forward proofs pass; reference-host Blueprint acceptance remains pending.

## Runner-snapshot Network and Volume Script source authority (2026-09-03)

Blueprint post-deploy Script preparation preserves one logical Network and
Volume membership per execution against that execution's already-prepared
immutable runner snapshot. Multiple executions may share the same logical
Network or Volume while retaining distinct snapshot keys, positive prepared
revisions, and canonical digests only when the stable source owner is identical
and both witnesses are existing evidence. Same-execution conflicts, cross-owner
evidence, staged evidence, and every other source family remain strict. The
fresh reference-host Blueprint exposed the former cross-execution rejection;
focused source-reference race proof and the real Blueprint construction
regression now pass. Final Blueprint publication still leaves the Environment
applied Compose predecessor or absence authoritative until successful terminal
Task acknowledgement. Exact reference-host Blueprint and Script acceptance
remains pending.

## Blueprint applied-predecessor authority (2026-09-04)

Only a Blueprint candidate Environment update Task with release-publication
authority now seals an independent nullable applied-Compose predecessor in its
materialization writer. Generic Attach and Detach Tasks retain only the shared
Environment writer fence and do not read or compare Blueprint generations. A
candidate may validly observe applied absence at any positive desired generation;
a present applied predecessor must carry a valid identity and a render generation
strictly below the candidate, and its exact presence, key revision, identity, and
generation are sealed before Agent effects. Unproven failure remains read-only,
proven compensation terminalizes atomically, and retry chains retain the original
predecessor authority without duplicate transaction compares. Focused etcd race
proof passes; exact reference-host Blueprint acceptance remains pending.

## Script runner inherited image ownership evidence (2026-09-04)

Script runner ownership validation now requires every sealed Environment value
exactly once and the exact closed `com.groundplane.*` ownership-label set while
permitting immutable defaults inherited from the digest-pinned image. Once
Docker returns a container id, the Agent validates and adopts that exact
identity without unvalidated removal, checkpoints it before any pre-start
outcome, and cleans only under the acknowledged ownership evidence. Ambiguous,
invalid, mismatched, running, or exited post-create state remains a recovery
invariant; cancellation after capture records `abort`, and a plain no-id create
failure remains `start_failure`. Focused Docker runner, Agent runtime, and
Script checkpoint race proofs pass; exact reference-host Blueprint and Script
acceptance remains pending.

## Script runner cleanup authority (2026-09-04)

Script runner adoption still validates the complete immutable container shape,
while post-checkpoint cleanup now authorizes removal only from the exact durable
container id and closed Groundplane ownership-label digest. An image may supply
its working directory only when the sealed projection leaves it empty; an
authored working directory remains exact. Cleanup can therefore remove and
prove absence of an already-checkpointed runner after a non-ownership shape
failure without weakening foreign-container protection, and a cleanup failure
no longer masks the original create failure. Focused Docker runner and Agent
runtime race proofs pass; exact reference-host Blueprint and Script acceptance
remains pending.

## Unified Blueprint final-publication contract correction (2026-09-02)

ADRs 0051 and 0062 plus the Blueprint grammar now replace the ordinary
`32/32/32` final-publication partition and Backup-only `128/128/128` envelope
with one dedicated envelope for every final Environment Blueprint publication.
Each comparison, success, and failure arm may contain at most the configured
etcd maximum of 256 operations, and the encoded protobuf request may contain at
most 1 MiB. Derived maxima of 512 selected operations and 768 full logical
operations are diagnostics only, never rejection limits. Ordinary
non-Blueprint `Store.Transact` keeps its existing 96-selected-operation
protection; staging, sealing, and other non-final-publication limits do not
change.

The required implementation proof is `30/42/30` for the legal QA Blueprint
with eleven Release candidates, two hooks, and two staged physical sources;
`44/98/44` for maximum non-Backup Script; `117/147/117` with two candidate
Attaches; the Backup-only `152/90/152` accounting; and `174/175/174` for
combined Backup plus Script, whose selected success, selected failure, and
full counts are 349, 348, and 523. Boundary proof must accept a byte-fitting
256-operation arm, reject 257, reject an encoded request over 1 MiB, and retain
the non-Blueprint 96-operation rejection. Every operation remains distinct and
atomic because it carries desired-head, Task, marker, Release, Script, source,
Attach, or Backup authority.

The dedicated typed final-publication executor is now implemented without an
ambient context flag. Ordinary `Store.Transact` remains capped at 96 selected
operations, while the typed Blueprint path enforces 256 operations per arm and
the actual 1 MiB encoded request ceiling. Focused boundary proof covers 256-arm
acceptance, 257-arm rejection, encoded overflow, and ordinary protection.
Exact legal composed-shape, candidate Attach/Volume visibility, retained Attach
race, failure-injection, and fixed-envelope fixtures now pass. Reference-host
acceptance remains pending; C16 and C21 remain **Scaffolded**.

## Blueprint secret Entry Script-source digest (2026-09-02)

Blueprint post-deploy Script source retention now derives a secret Entry generation
digest from its validated durable ciphertext while the transient runner binding
retains the plaintext digest required after decryption. Plain Entry generations keep
their validated plaintext digest. Focused persistence proof passes; exact
reference-host Blueprint and Script acceptance remains pending.

## Blueprint candidate image observation role selection (2026-09-02)

The reference-host Blueprint Task proved that an addressable recreate artifact
legitimately carries a stable proxy and a singleton workload under the same stable
Service identity. Agent image observation now ignores the proxy and selects exactly
one candidate workload by its closed role, requested image, and candidate Release
label; duplicate matching workloads still fail closed. Focused observer proof and
the exact reference-host Blueprint rerun remain required before acceptance.

## Atomic Blueprint Backup desired-state implementation (2026-09-02)

Environment Blueprint apply and validate now resolve authored Backup Connector and source labels,
preserve omission through a fixed-revision compare-fenced preparation, publish a present policy as a complete replacement, carry a
public-safe Backup decision projection, and atomically publish source catalog,
policy, schedule, enabled Connector reference, and lazy age-key authority through
the unified final-publication executor. Canonical authoring reconstructs current
labels and emits a reproducible disabled policy when a retained Connector no longer
exists. Direct Backup Policy source pre-ensure remains unchanged.

Canonical disabled authoring now preserves frequency, retention, encryption, and ordered
source labels after Connector deletion while omitting only the unavailable Connector;
enabled missing Connector fails closed. The canonical policy validator enforces age
encryption for every config source, including disabled policies.

Focused application, parser, desired-revision compilation, persistence,
transaction-envelope, source-identity, lazy-key, Script-publication,
candidate-Attach-limit, retained-reference, exact maximum-shape,
same-candidate Attach/Volume visibility, and failure-injection proofs pass.
Generated-artifact checks and reference-host acceptance remain pending; C16 and
C21 remain **Scaffolded**.

## Bounded terminal materialization drain (2026-09-04)

An Agent worker now retires materialization correlation after a terminal result
instead of deleting it while Controller records may still be in flight. Retired
transfers retain exact assignment, plan, step, sequence, length, and digest
authority while immediately clearing every accepted or rejected plaintext chunk.
Incremental digest state uses the canonical destroyable materialization hasher,
the receiver enforces the protocol's 32-chunk ceiling, and incomplete retired
state is destroyed at the immutable assignment deadline. An exact assignment
replay replaces and destroys only its matching retired state; active duplicates
and mismatched correlation still fail closed. Focused Agent and materialization
hasher race proofs pass; exact reference-host Blueprint and Script acceptance
remains pending.

## Blueprint claim-time mutation epoch authority (2026-09-04)

A claimed Blueprint candidate attempt now transfers the Environment mutation
epoch to its live materialization writer before Agent effects. Active terminal
acknowledgement compares that exact writer revision, proven terminalization
atomically refreshes attempt authority and the epoch, and retry creation uses
the failed source attempt authority before transferring the successor epoch.
Exact durable terminal replay remains independent of later epoch movement.
Focused original-attempt, retry-chain, independent-mutation, and replay race
proofs pass; exact reference-host Blueprint acceptance remains pending.

## Recovered Agent assignment event-attempt authority (2026-09-04)

Every private Agent Task assignment now carries one positive Controller-authored
event-attempt epoch used by all step events from that delivery. A recovered
assignment derives the next epoch from its exact fixed-revision durable event
journal, rejects mismatched identity or overflow, and is not redispatched at or
after its immutable deadline. The Agent validates and preserves that epoch
instead of reusing attempt one after reconnect. Protobuf regeneration and the
focused Controller and Agent race proofs pass; exact reference-host Blueprint
acceptance remains pending.

## Environment Services Console tab (2026-09-04)

The Environment Console now provides a dedicated Services tab with an explicit
service count and a responsive inventory of every Service's desired image,
network Zones, runtime state, and observed health. It reuses the existing
Service inspect behavior and the shared right-side create/edit drawer instead
of introducing a second mutation path. Independent review found no landing
blocker; Console build verification was not requested for this slice.

## Blueprint prerequisite mutation-epoch continuation (2026-09-04)

An exact sealed prerequisite Attach acknowledgement now advances the existing
Blueprint requirement gate and Environment mutation epoch together before the
dependent Blueprint Task claims execution. Only a successfully completed exact
Attach and producer Task recorded in the immutable requirement DAG can carry
that authority; failed, aborted, timed-out, retried, or unrelated Environment
mutations cannot refresh it. The focused etcd race proof covers successful,
failed, aborted, and unrelated-mutation paths, and independent review plus the
single correction-delta verification found no remaining landing blocker.

## Bounded Script source-reference normal release (2026-09-04)

The canonical Script operation source root now carries one mandatory closed
retry disposition and owns resumable normal release without a parallel Release
operation record. Normal release processes logical windows of at most sixteen
members through deterministic transactions inside the ordinary 96-operation
ceiling, while preparation abandonment retains its independent safe physical
page size. Exact memberships, counts, Script aggregates, caller guards, root
revision, and cursor are fenced against replay and races. Focused race tests,
independent review, and the single correction-delta verification pass.

## Pending Blueprint Script abort lifecycle (2026-09-04)

Aborting a pending Blueprint Task now atomically validates the sealed Script
source membership, fences assignment, records explicitly Controller-owned
`abort_before_start` cleanup proof, and moves the source root into abandoned
normal release. Bounded release resumes from its cursor, and only exact drain
proof terminalizes the same Task and removes its root. Root tampering blocks
claim and abort, completed replay requires the cleanup and Task timestamps to
match, and unrelated Script executions cannot use the Controller-owned
checkpoint shape. Focused races, independent review, and the single correction-
delta verification pass.
