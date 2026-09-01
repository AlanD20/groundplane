# Implementation checkpoint

Status: live handoff for implementation work, dated 2026-09-01. This document
is implementation evidence only; it is not product authority. Product behavior
and vocabulary remain authoritative in `docs/mvp.md`, with the source-of-truth
order in `docs/agents.md` resolving conflicts.

The integration owner MUST update this file in the same commit whenever a lane
lands or the remaining set changes. Branch names, candidate counts, and this
checkpoint do not prove acceptance by themselves; acceptance requires the
evidence named by `docs/capabilities.md` and `docs/delivery.md`.

## Current delivery pipeline

Independent feature lanes now run continuously in parallel. Each lane has one
durable repository-local worktree and branch, one writer, and disjoint owned
files; `/tmp` is reserved for ephemeral artifacts. A lane freezes only after
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
idempotency evidence identifies the winner. Blueprint apply still publishes
Script desired state without executing hooks; candidate Release publication
and staged Blueprint post-deploy execution are the next C21 lane required by
the private clean-start topology.

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
