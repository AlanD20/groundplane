# Remaining runtime qualification

- Owner: primary delivery owner for the affected feature
- Severity: high for missing data-safety or exercised destructive-path proof;
  medium for unavailable health; low for isolated presentation/fixture hygiene
- MVP-required: yes for the actual hosting and recovery paths exercised by Gate A/B

This file records unresolved proof, not a historical implementation journal.
The [task list](../../tasks/todo.md) owns scheduling; [head.md](../head.md) owns
operational authority. The owner authorized fresh QA with disposable data on
2026-09-12, without restoring incident-affected state. That state's integrity
remains unresolved; the fresh run does not qualify it.

## Storage and source retirement

The normalized desired revision is the sole Volume authority;
the remaining flat `VolumeRecord` is read projection, not a writable primary.

Remaining acceptance includes mounted-Volume consumer detachment, crash/Retry
recovery, real manual-Script source consumption and source-family retirement.
In particular, qualify prepared references through physical destruction and
cross-Environment Network ownership; allocating a source count alone is not
proof that every deletion path fences it. Keep unchanged workload bytes, native
Release ownership, proxy identity and persistent-resource authority. No coverage,
ownership or immutable-source guard may be relaxed to pass the journey.

Requirements are in [Volumes and Entries](../features/storage-and-entries.md),
[Script execution](../features/setup-scripts.md), and their routed technical
contracts. Local source-retention proof and retained wider failures are in
[Script evidence](../acceptance/script-execution.md#manual-immutable-source-integration).
Recheck code and relevant evidence before implementing an older suspected gap.

Candidate qualification at `6c89049fa` reproduced a local VOL-07 failure on
2026-09-14: `TestVolumeRemovalSuccessorCompletesRetainedAbsence` cannot complete
a retried removal whose directory absence is already proved. The production
terminal publisher constructs 27 comparisons, exceeding its fixed limit of 26,
and returns Internal before committing. Normal, late-index and lost-response
variants fail in `.tmp/recovery-candidate-runtime-packages.log`. No live removal
or data loss was observed. The owner approved the narrow repair. Diagnosis traced
the extra comparison and write to configuration recovery: the shared desired
publisher attached a source snapshot even to Volume Tasks that write no
configuration. It also staged unnecessary source records before rejected
metadata/Volume publications.

Acceptance: finish the same retained removal after proven absence without another
physical deletion, preserve exact Task/operation/replay authority, reject stale
ownership, and stay within the existing comparison, mutation and byte budgets.
The corrected producer selects configuration authority only for Blueprint Apply
or an explicit file writer; it rejects supplied configuration authority on other
desired mutations. Normal, late-index, lost-response and late-Task variants now
pass locally without changing finalization guards or budgets. H26–H27 in
[acceptance](../acceptance.md) record the proof and subsequent test-only fixture
corrections. The complete durable-store race suite now passes; the complete root
run plus the two affected-package reruns also pass their recorded assertions.
Live Volume-removal qualification remains unrun.

## Blueprint integration

The 692,408-byte historical failed publication and 412,657-byte local reproduction
were corrected without raising the 262,144-byte marker limit. The local running
marker measured 71,808 bytes in the local reproduction.
That closes the old blanket oversized-publication blocker, not every Blueprint
recovery case or the newer explicit-context first-Apply/reapply requirements.
Preserve exact witnesses and all record, assignment and transaction ceilings.

Acceptance is the actual current producer, source-retirement and recovery paths,
then the current full bundle through normal surfaces. Do not replay a stale
desired revision over a working Environment merely to repeat an old test.

The owner confirmed per-Service native recovery authority on 2026-09-12. Contracts,
claim validation, Agent admission, restoration and independent observation now use
the same captured native artifact. The applied Environment witness remains
independently fenced; it is not a recovery fallback. Local mixed Worker proof
covers serving restoration, first-candidate absence, immutable history and terminal
replay in `.tmp/native-recovery-dlIg4I6U/mixed.log`. This closes the earlier local
artifact-identity mismatch, not live recovery qualification. Owner: primary delivery
owner. Remaining acceptance is the real operator journey and full CI.

The broader local proof passes in `agent-final.log`, `authority-final.log`,
`executionplan-final.log` and `renderer.log` in the same directory, with race
checking and coverage. It includes native rollback predecessors, source-revision
conflicts, reconnect, terminal replay and malformed-authority rejection. Scoped
vet passes. The architecture gate reports the same 125 outstanding findings;
the three findings on edited mixed-fixture files concern imports already present
before this change. No baseline was raised.

## Unchanged Blueprint recreates router

Owner: Blueprint/Component delivery owner. Severity: high for uninterrupted
ingress. BP-05 failed on clean `76578c72f`, deployed as
`0.0.0-qa.prodops20260914.1` on 2026-09-14. Applying the same successful bundle
recreated the ingress router while retaining its image. The new container carries
the reapply's plan id and generation; both labels and Compose configuration hash
changed. Other container identities/start times were unchanged across that Apply.
The Task completed and subsequent authenticated requests passed; neither proves
connection continuity. The run did not measure the interruption duration.

The later Attach probe initially compared against its pre-reapply snapshot.
Comparing the saved post-reapply observation instead proves the four Attaches
preserved the replacement router. This corrects attribution, not the reapply
failure. Evidence and exact identities are H29 in [acceptance](../acceptance.md).

The exact pre-reapply and published artifacts prove the cause: normal Service
Deploys added native proxy configs. `RetainEnvironmentComponentRuntime` compared
the whole Environment resource map and declined retention although the router
did not consume those configs. New plan/generation labels then changed Compose
identity. Its earlier tests covered whole-map equality, not unrelated resources
introduced between first Apply and reapply by native Deploy.

The owner-approved correction compares only definitions referenced by the
unchanged Component. It reuses the existing canonical resource-reference reader;
changed or missing consumed resources still prevent retention, and desired shared
resources are preserved. Component configuration, ownership, source validation
and publication fences are unchanged. H30 records failing-before/passing-after
regressions and successful replay of the exact QA artifact pair. The affected
race checks, vet and pinned Staticcheck pass; live closing proof remains required.

Acceptance: repeat the first Apply, normal Service Deploys and exact reapply;
preserve unchanged router identity/start time and held application connections,
while still applying genuine router changes correctly. Then resume the saved
cutover/recovery case without replaying its completed mutations. Application
baseline is healthy; additional Attaches remain, with no Entry rebind, old Detach
or failed-candidate fault performed. The repair is deployed in H31, but its
isolated proof stopped on the first-Deploy defect below. Finish that proof;
do not expand this permission to latest-wins or broader reconciliation work.

H33 resumes after the first-Deploy repair: Deploy completes, but exact reapply
still recreates the isolated router and breaks a held HTTP connection. The native
workload and proxy remain unchanged; the real application is healthy. This is a
remaining failure, not proof the resource-comparison correction closed BP-05.
Earlier fixed-revision capture showed an applied artifact without Component
Services despite the running router. The retention reader requires applied
Component bytes; diagnose the acknowledgement/source-capture path for configured-
only Apply before proposing a correction. Do not fall back to desired state or
repeat Apply to hide the first-cycle defect.

The owner approved this remaining repair. The acknowledgement predicate skipped
all existing applied updates without native Release publication, including a
Blueprint that successfully executed Components. It now distinguishes explicit
no-Release Apply from metadata-only edits. The regression fails before the change;
the producer/completion check requires the exact new Component artifact and an
unchanged independent native Service receipt. An older test incorrectly required
the aggregate artifact to remain unchanged and missed this distinction. Failed
Tasks, Entry/Volume rules, source guards and transaction limits remain intact.
H35 now passes the fresh first-Apply/Deploy/reapply cycle with unchanged router
and native runtime and 90 requests on one held connection. The observed H33
failure is repaired on the current candidate. No stored history was rewritten;
other genuine-change and interruption variants retain their own matrix status.

## First Deploy loses profile-disabled Service

Owner: Services and Releases delivery owner. Severity: high for first deployment.
H31 / SVC-06 fails on `2895989f2`, deployed as
`0.0.0-qa.prodops20260914.2`. An isolated configured singleton with a Compose
profile and no Attaches completes Blueprint Apply, but normal blue-green Deploy
returns Internal before accepting a Task. The Controller reports
`sealed release binding Service is absent`.

The captured desired artifact contains both Service YAML and metadata. Its
staged Release keeps metadata but drops that Service's YAML. The ordinary Release
publisher calls `MutateAttachNetworkArtifact`, which loads inactive-profile
definitions into `DisabledServices`, then marshals without including them.
The later sealed-binding check correctly rejects the inconsistent result. This
path is separate from the scoped Component-retention correction; do not relax
the binding check or add workload inputs to hide the failure.

Acceptance: normal first Deploy preserves configured profile-disabled Services
through current-Attach projection, publishes a valid Task and serves the declared
application. Cover empty and present Attach selections without enabling unrelated
Services or changing immutable source input. Then resume the same BP-05 sequence.
The owner approved the scoped repair. The shared serializer now includes disabled
definitions. The same publisher regression then exposed zero expected replicas:
explicit Release planning still treated its selected Service as disabled. It now
selects only the named Release members, retaining authored profiles. Both empty
and replacement Attach cases pass the real publication/reconstruction tests.
H32 records local proof. H33 proves the same isolated Service now completes first
Deploy on the repaired candidate. Its subsequent router-continuity failure is
recorded above and does not reopen the missing-definition rejection. Broader
SVC-06 traffic variants remain unqualified.

## Recovery after runtime configuration changes

Current H36 result on `18e936fa9`: the repaired backing cutover passes, but stopping
only the next candidate exposes repeated failure of its Service proxy recovery
probe. The public event stream records at least 271 failed/running attempts for
that step. The Task remains active beyond the 600-second QA observation bound;
the Agent initially remains healthy with one claim. Repeated attempts then exhaust
the 1,000-event Task journal limit. Controller rejects further progress at that
limit; the Agent becomes degraded and Service observations become unavailable.
Earlier profile, serving Release and Service health checks passed during the wait,
but automatic recovery and a later healthy Deploy are unproved.
H37 establishes the observation-scope cause: the Service's captured predecessor
contains its one required Volume, but the shared project has five other Volumes.
The observer reports those absent from the artifact as collisions; the Agent
excluded unrelated networks but not unrelated Volumes. Compensation and subsequent
probes therefore rejected matching restored workload identity. The local correction
excludes only named Volumes outside the sealed artifact. Required and unidentified
Volume conflicts still fail; full Agent race checks pass. Earlier fixtures tested
healthy replicas without unrelated project Volumes, so missed this sequence.

Journal exhaustion followed because each redispatch advances the execution epoch
and generates new progress events. The owner approved trimming oldest events on
append. H38 verifies the local rolling 1,000-event window, bounded replay and
per-step mutation/progress checkpoints. Recovery checks and timeouts are unchanged;
old public history now expires as explicitly approved. Neither repair is deployed.
The normal updater requires healthy idle Agent state, so a one-time data-preserving
repair installation awaits owner approval before the same Task can resume.
The historical incident below remains evidence, not the cause of H36.

Owner: Services and Releases delivery owner. Severity: high. This is an active
recovery blocker, not deferred qualification. The failed-candidate QA sequence on
2026-09-13 first completed Attach creation, Entry rebinding and old Attach removal,
then stopped the exact new workload candidate before promotion. The Task reached
`timed_out` with its recovery steps completed. The Agent remained healthy and
released its claim, but the restored serving workload rejoined the old backing
network and lost the new one. The authenticated application endpoint returned 500.

`releaseoperation.captureServingRuntime` calls `servicelifecycle.CaptureRelease`
and renders the original immutable Release input. That source predates the
successful Attach/Entry changes. The restoration contract binds exact historical
bytes, but those bytes no longer describe the last successfully running workload.
Correct restoration proof against that source does not prove application recovery.
Earlier focused Attach and recovery checks did not cover this combined sequence.

A subsequent normal Deploy completed after removing the owned fault-test hook;
its functional check returned 401 followed by 500 on authentication refresh.
A read-only follow-up proved that the new workload joined both backing networks:
its new password authenticated only at the new address, while DNS chose the old
address. The earlier Entry artifact captured both memberships; later Detach
changed Attach records but not that artifact. New Release publication reused the
stale union. The publisher now captures current Attach records at its fixed read
and seals that union into the new candidate, without changing historical sources.
The complete publication regression reproduces the removed network returning;
it now proves removal, current membership inclusion and unchanged input bytes.
This correction is locally verified and deployed as `0.0.0-qa.recovery20260913.6`;
it does not repair predecessor capture. The next normal Deploy staged a candidate
but returned Internal without an accepted Task. The Controller reported
`release durable record is corrupt`; read-only source, native rendering, hook
selection and staged-render checks passed. The later rejection came from hook
loading: it incorrectly compared recaptured generated Compose bytes with the
authored Blueprint artifact. The hook-source correction is deployed as
`0.0.0-qa.recovery20260913.7`. One fresh normal Deploy completed and restored the
application baseline: all 11 Services are healthy, the profile is preserved,
the API workload uses only the current backing network, and both actual realtime
replicas pass authentication and subscription checks. This proves forward Deploy,
not failed-rollout recovery. Its evidence is `hook-source-forward-deploy-result.json`
and `hook-source-forward-deploy.log` in the same repair directory. Do not replay
the prior fault or proceed to interrupted-Task/reboot testing until product-owned
recovery is proved.
Evidence is `failed-rollout-terminal.json`, `after-candidate-recovery-runtime.json`
and `candidate-operator-repair.log` in
`.tmp/qa-recovery-repair-20260913-63vxXPTf/`.

Required outcome: failed deployment restores the runtime that was successfully
applied immediately before it, including later Attach/Entry changes. Investigate
every recovery/lifecycle consumer of the historical Release assumption. A proposed
separate record of acknowledged per-Service runtime needs its exact ownership and
publication rules resolved in the existing feature/ADR; it is not implemented or
approved by this issue record. Preserve immutable Release/Task history and exact
validation. Latest desired configuration and host observation are not substitutes
for acknowledged state.

The applied-runtime audit found a separate Entry selection defect: capture reads
running Service ids, but the planner discarded them and treated every exposed
retained Release as runnable. The correction pins that set on the Task and
intersects it with affected consumers during reconstruction. It prevents Entry
edits from starting stopped or absent Services; it does not supply acknowledged
runtime receipts or repair stale recovery input. The earlier running-only tests
could not expose the discarded operational intent.

The shared-consumer audit also found configuration-file paths whose contents can
change after the immutable Release input was captured. Exact Compose bytes alone
therefore cannot prove exact configuration restoration. The network failure above
is observed QA evidence; the file-generation gap is a source-audit finding, not a
separately executed fault test. The owner approved capturing acknowledged runtime
and exact configuration sources before each Task, with sealed prior-file
restoration during compensation, on 2026-09-14.
[ADR 0079](../decisions/0079-pinned-task-configuration-recovery.md) narrows
ADR 0064's former ban on recovery-only materialization. It must not restore
databases, reread latest desired inputs, rewrite history or silently change Entry
deletion and secret-retention requirements. Implementation and live proof remain
outstanding; the decision is no longer blocked on owner approval.

The owner also approved the required source-lifetime extension on 2026-09-14:
Secret deletion returns `resource.in_use` while a recoverable Task holds its exact
value. Existing Script guards remain intact; ordinary desired references still
do not block deletion. Reservation and delete/Retry must be mutually fenced, and
successful deletion must leave no hidden ciphertext copy. The Entry and Blueprint
runtime writers have local proof. File-source retention and recovery-reader work
may resume; dependent live faults still wait for a valid baseline and recovery
proof. Do not substitute latest Secret values or fabricate Script owners for
non-Script Tasks.

Acceptance: reproduce Deploy, successful Attach/Entry change and Detach, then
failed Deploy through normal surfaces. Prove preservation of the pre-failure
bindings, actual authenticated application behavior and unrelated workloads.
Cover failed configuration changes and relevant reconnect/replay so unapplied
intent cannot become the recovery source. Scope further tests to the confirmed
shared consumers; the code correction and full live proof remain outstanding.

## Same-name backing endpoints

Owner: Backing Service delivery owner. Live QA on 2026-09-13 exposed a DNS
collision when one consumer joined two distinct Valkey backing networks. Both
Attach URL facts used the bare hostname `valkey`. After Entry rebinding, the new
password was correctly present in the consumer and authenticated against the new
instance's address, but Docker DNS resolved that hostname to the old instance.
Removing the old Attach restored correct resolution and both native realtime
replicas passed actual authentication and subscription checks.

This blocks qualification of overlapping same-name backing connections, including
that migration window. It is not an authentication-mode failure. Evidence is
`cache-cutover-diagnostic.log` and `cutover-finish-after-dns.log` in
`.tmp/qa-recovery-repair-20260913-63vxXPTf/`.

H35 supersedes that failing variant on fresh fixed-candidate backings: both actual
client replicas authenticate and subscribe after Entry rebinding while both networks
remain attached, then pass again after dependent-first old Detach. All five native
consumer replicas use only the new network afterward; serving Releases, proxies
and checked application data survive. No direct-IP workaround or early Detach
bypassed the overlap assertion. ATT-12's password-only cutover variant passes;
old frozen artifacts and unrelated qualification are not implicitly migrated.

The owner-approved local correction publishes a Service-id-derived backing
network alias and uses it in standalone and Blueprint owner/grant HOST/URL
facts. The original mismatch fails in
`.tmp/evidence/backing-endpoint-fix/red.log`; focused Go 1.26.7 race tests, vet
and pinned Staticcheck pass in that directory. These are local checks, not a
live ATT-12 pass. Existing frozen backing artifacts lack the new alias; the
approved disposable QA run must create fresh backing instances on the fixed
candidate. No stored-artifact migration or production deployment is included.

Acceptance: give each backing endpoint a stable, unambiguous runtime identity
and use it consistently in HOST and URL facts. Attach one consumer to two
same-kind Backing Services with distinct credentials; prove each fact reaches
only its own instance before, during and after a credential/Entry cutover.
Keep isolation and authentication checks intact. Do not qualify this case by
hardcoding instance IPs, bypassing facts or weakening credentials.

## Service health and Route visibility

The old generic ingress hint was corrected locally using existing Route states.
It still needs deployment. Service observation and its Environment aggregate
are implemented locally but need live qualification; unavailable evidence must
not become Healthy. Desired intent
and successful old Tasks do not prove workload or public reachability.

Use [Services and Releases](../features/services-and-releases.md).
Acceptance is consistent Console/CLI/API
observation, correct unavailable/unhealthy semantics and the deployed Route
summary without claiming more than its evidence.

## Verification debt

The 2026-09-12 maintenance slice cleared five protobuf copy-lock findings and
seven Staticcheck findings across Agent, execution-plan, Agent-channel and etcd
packages. Plan validation now clones protobuf messages; runner snapshots remain
pointers. Unused assignments are removed, Zone conversions are explicit, and
late log-subscription cancellation uses the existing bounded cleanup context.
Scoped vet, Staticcheck, formatting and affected race tests passed. Evidence:
`.tmp/ci-cleanup-7arYNpkD/{vet-after,staticcheck-after,analyzer-focused,analyzer-etcd-focused,analyzer-zone-focused}.log`.

This is not a full-package or full-CI pass. The hook-recovery failure retained in
`analyzer-focused.log` is superseded by the native recovery proof above. The exact
affected terminal-publication, source-reference and Zone selections passed
separately. [Script acceptance](../acceptance/script-execution.md#explicit-context-qualification)
retains the broader qualification requirements.

The CoreDNS CLI fixture now exercises Component read, config read and full
Corefile replacement. The Blueprint fixture uses `environment blueprint apply`,
reads the current revision and asserts the exact `If-Match` fence on the PUT.
The complete CLI subtree passed race tests with coverage, and vet passed.
Evidence: `cli-subtree-corrected-loopback.log`, its matching coverage file and
`cli-vet.log` in the same evidence directory. The first local run was blocked
by sandbox socket permissions; the passing run used isolated loopback servers,
not a live Controller.

The subsequent minimum QA preparation corrected the 13 CLI ST1005 findings and
removed the two overwritten artifact assignments in
`internal/controller/component_action.go`. CLI/Controller Staticcheck and the
affected Component/CoreDNS race tests pass, with coverage, in
`.tmp/qa-fresh-20260912-NRo98B/{analyzer-scoped,component-race}.log`. This closes
those 15 analyzer findings, not full-CI or live qualification.

The integrated scan corrected a further 13 analyzer findings and a protobuf-copy
vet finding; the intentional nil-context negative test retains a single explained
SA1012 suppression. Full tagged Staticcheck and vet pass. The formatter gate now
excludes ignored caches, selects `gofmt` explicitly and preserves tool failures.
Its regression tests pass. The full tagged race run exposed four stale fixtures
and a terminal-event replay defect; focused corrections pass, including rejection
of changed replay identities and payloads. Exact evidence is in
`.tmp/qa-fresh-20260912-NRo98B/`.
The final full affected etcd, etcd/releasegroup and Controller Agent-channel
packages pass race tests with coverage in `persistence-full.log`. This closes
the retained package failures, not full CI or live workload qualification.

Privileged Backup staging qualification is unavailable locally because sudo needs
a password. The approval review rejected the proposed QA-host mount test; it was
not executed. Owner: QA delivery owner. Acceptance: obtain explicit authority and
pass the bounded mount test on an approved root-capable target. This remains
unqualified and does not expand initial hosting into Backup delivery.

The two Agent recreate-probe fixtures were repaired on 2026-09-12. They now use
sealed plans, explicit native predecessor references and validated assignment
authority. Historical labels remain unchanged; the blue-green transition uses
one replica on each side. Separate observations prove the historical blue slot
and the candidate/prior exact-count path, including unrelated collision filtering.
Four negative cases reject a missing seal, missing authority, changed witness
or unselected step before observation. Production validation is unchanged.

Evidence is in `.tmp/recreate-fixtures-Rpw7yBic/`: `before.log` reproduces both
missing-hash failures; `probes.log` and `recovery-evidence-final.log` pass with
race checking and coverage. Agent vet and Staticcheck pass (`vet-final.log`,
`staticcheck-final.log`). This unit fixture treats the Controller-owned authority
digest as opaque; it does not qualify Controller publication or live recovery.
The prerequisite-gate and hook-terminal fixtures now carry complete staged Release
authority and the executed resource set. Their three retained failure groups are
closed; the two mixed-recovery groups now have the native recovery proof above. Focused race tests
with coverage, including shared atomic-publication shapes, pass in
`.tmp/blueprint-recovery-ar6iu0wf/gate-terminal-final.log`. Scoped etcd vet and
Staticcheck pass in `etcd-vet-final.log` and `etcd-staticcheck-final.log` in that
directory. This fixture-only repair does not qualify full CI or live recovery.

Go 1.27 crashed the pinned analyzer in the recorded run; Go 1.26.7 ran it and
reported SA4006. That tool crash is not a source finding or a green result.
Exact current tasks and evidence are in [tasks/todo.md](../../tasks/todo.md).
Architecture-only cleanup remains separately [deferred](deferred-architecture-cleanup.md)
under its explicit authority, without waiving real safety, build or runtime defects.
