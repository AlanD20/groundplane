# Acceptance evidence

[The product QA matrix](qa-matrix.md) owns the case catalogue. This document
indexes evidence and defines the record required for each execution. Feature
requirements remain in their owning documents; a test report cannot change them.

## Case run record

Before a qualification run, enumerate the selected case IDs and every required
variant from the matrix. Start them as NOT RUN. Keep the following fields for
each case; a shared run header may supply identical build/target fields once.

| Field | Required content |
| --- | --- |
| Case | Stable matrix ID, explicit variant, requirement link and test-definition revision. |
| Implementation | Exact automated file and test selector, or reproducible bounded manual steps; explicitly state missing automation. |
| Candidate | Source commit, clean/dirty state, dirty-patch identity if applicable, Controller binary/release digest, Agent image digest and relevant client/test revisions. |
| Environment | UTC start/end, supported architecture, configuration/fixture identity, selected ingress and test dependencies. Private identity/topology belongs only in ignored evidence. |
| Preconditions | Initial desired and serving state, actual application baseline, known data, ownership inventory, permission and existing blockers. |
| Action | Exact operator operation/request, protected request identity, Task/attempt identity, parameters and actual fault point if any. Keep secret values out. |
| Expected result | Independent application/data/runtime assertions and predeclared bounds, including what must remain unchanged. |
| Observed result | Actual assertion results, traffic errors/latency/reconnects, data/content checks, Task outcome and runtime changes; not just an exit code. |
| Outcome | PASS, FAIL, BLOCKED, NOT RUN, or NOT APPLICABLE with a contract reason. Use PARTIAL for incomplete historical evidence, never promote it to PASS. |
| Evidence | Immutable run directory and exact receipt/log/capture paths with retained integrity information. Preserve failing evidence; a subsequent run does not overwrite it. |
| Cleanup | Owned resources removed/restored, independent absence/baseline check, any residual state and whether it blocks the next case. |
| Limits | Untested variants/surfaces, missing probes, mitigations and user decisions; link the finding and any later closing run. |

Only complete assertions and successful required cleanup permit PASS. An expected
rejection is a pass only when both the specified rejection and absence of forbidden
effects are proved. Failure to inject a fault is NOT RUN for the recovery case.
Product success after a manual workaround does not close its failed recovery case.

Store private run manifests, commands, identities and raw artifacts under unique
ignored repo-local evidence directories. Track only sanitized case IDs, outcome,
tested public build identity where available, evidence locator and remaining limits.
Do not publish application names, credentials, private endpoints or topology.
Missing old provenance is a recorded gap, not permission to reconstruct a successful
run from assumptions. Reuse needs an explicit unaffected-input justification.

## Matrix evidence register

Initial mapping reviewed on 2026-09-13. H references are historical evidence, not
results for the next clean integrated candidate. This is a bounded mapping of
available records, not an assertion that all older tests have been inventoried.

| Ref | Matrix cases and outcome | Evidence and limits |
| --- | --- | --- |
| H1 | HOST-01: PASS for the dated API/CLI read-only journey. | `host-health-20260913T181552Z.68MSID/result.txt` in the ignored repair evidence directory records exit 0, canonical Host/API/CLI parity and successful tunnel/runtime cleanup, with no service mutation. Deployed Controller baseline was `0.0.0-qa.recovery20260913.7` / `637990dd4`; CLI was built from the current dirty checkout and its hash is in `cli.sha256`. Does not prove Console, bootstrap, application health, upgrade or clean-candidate qualification. |
| H2 | UP-01, UP-07, UP-12, JOURNEY-05: PARTIAL. | `upgrade-traffic-within-api-limit-result.json` records 600 seconds, 2,948 HTTP 200 responses, zero recorded request failures, 118 pongs on the same WebSocket and worst response 5.105 seconds. `native-failure-result.json` records automatic predecessor recovery, unchanged checked application runtime/data and the original failed Task. These cover a successful GP update and a failed native Controller candidate, not all phases, routes, load levels or application-rollout recovery. Exact per-candidate digests/traffic timing still need correlation from the underlying run before reuse as a complete matrix pass. |
| H3 | BP-04/05, OBS-01, HTTP-02/09, BACK-06/10, SCRIPT-03, JOURNEY-01: PARTIAL. | The [QA checkpoint](head.md#private-workload-qa) and ignored `fresh-baseline.json`, `check-fresh-resolved.log` and earlier hosting receipts record successful private hosting, exact reapply, actual authentication/realtime checks and healthy observations. Different parts used different builds. No row-wide current-build or every-mode/surface pass is inferred. |
| H4 | SVC-14, ENT-08, ATT-08, SCRIPT-04, JOURNEY-03: PARTIAL. | `native-cutover-result.json` and `hook-source-forward-deploy-result.json` record successful cutover and forward Deploy; the latter records all 11 Services healthy, preserved checked data/unrelated Releases, current-only backing membership and both actual realtime clients passing. Forward Deploy used `637990dd4` / `.7`. These do not cover every case variant or automatic recovery. |
| H5 | SVC-15, JOURNEY-02: FAIL. | Failed-candidate recovery restored an obsolete backing network after successful runtime configuration changes; application returned HTTP 500 although recovery steps completed. [The recovery finding](issues/runtime-qualification.md#recovery-after-runtime-configuration-changes) retains the cause and required proof. Later normal Deploy repaired the baseline, not the failed automatic-recovery result. No closing live recovery run exists. |
| H6 | UI-03/04, TASK-02/03/04/05, SCRIPT-02/08, BACK-05, UP-03: PARTIAL. | [Independent edge checks](head.md#next-work) and the ignored simple-edge/authentication evidence record protected replay/rejection, Script Abort/unsafe Retry rejection, explicit authentication validation and rejected update input. These are subsets on historical builds, not all variants or current Console parity. |
| H7 | HTTP-08, UP-12: PARTIAL, with unresolved failure. | [Safe-update evidence](acceptance/safe-updates.md) retains whole-build Tunnel timeout/continuity gaps. A private HTTP success cannot close them; public and private paths need distinct execution records and authority. |
| H8 | ATT-12: FAIL during the overlapping-name window. | [The DNS finding](issues/runtime-qualification.md#same-name-backing-endpoints) records new credentials valid at the new address while the bare backing name resolved to the old instance. Completing old Detach restored resolution; concurrent same-name backing use remains unqualified. |
| H9 | ATT-08/09/10/11, SVC-13/16/17, BACK-05: PARTIAL, local implementation only. | The Attach/Detach runtime slice after `e06a0d2bc` passes `TestPrepareAttachRuntimes*` / `TestAttachRuntimeSharedResourceEquality` in `internal/common/executionplan/attach_runtime_test.go`, `TestAttachRuntime*` in `internal/infra/etcd/attach_runtime*_test.go`, and `TestDraftAttachIdentityPreservesAuthenticationAndSecretOwnership` in `internal/controller/attachplanning/sealer_test.go`. `.tmp/attach-runtime-local-qualification.json` binds exact source hashes and proof paths. The broader Attach/Detach race run also passes. These prove selected-only projection, retained shared-resource safety, source conflicts, atomic acknowledgement, replay and pruning independence at local boundaries, not a live application case. No recovery reader was connected or deployed. |

H10 records the 2026-09-14 HOST-01 API/CLI read-only rerun:
`.tmp/qa-pinned-recovery-20260914/host-health-20260914T024049Z.4ZDrpp/result.txt`
passes canonical assertions and owned cleanup without a service mutation. The
deployed Controller remains `0.0.0-qa.recovery20260913.7`; exact running digest,
Agent selection and client binary hash are in `api.json` and `cli.sha256`.
The client was built from dirty source. This refreshes only that read-only
baseline, not application health or clean-candidate/recovery qualification.

H11 is local BP-04/SVC-15/JOURNEY-02 support, not a live pass:
`TestBlueprintSuccessRetainsAcknowledgedServiceRuntime` in
`internal/infra/etcd/blueprint_executed_artifact_external_test.go` and
`TestBlueprintRuntimeSharesTerminalCommitAndReplay` in
`internal/infra/etcd/blueprint_runtime_atomic_test.go` prove executed runtime
selection, atomic completion, no-effect rejection and read-only replay.
The failing-before receipt check is
`.tmp/pinned-recovery-blueprint-runtime-red.log`; the full affected passing race
run is `.tmp/pinned-recovery-blueprint-runtime-final-bounded.log`, including
maximum-candidate/hook and stale-authority cases. Existing record/transaction
limits are unchanged. No recovery reader or pinned-file execution is qualified.

H12 is local ENT-08/SVC-13/15/JOURNEY-02 support, not a live pass:
`TestEntryMutationPreparesOnlySelectedAcknowledgedRuntime` and the Entry serving
producer tests prove current/retained workload selection, unchanged stable proxy,
exact source rejection at publication/claim/acknowledgement, atomic receipt/Task
completion and stopped/absent materialization-only behavior. The terminal oracle
checks the new environment-file path independently; the aggregate applied artifact
stays unchanged rather than promoting unrelated desired decisions. Fixtures model
the preceding runtime acknowledgement; they do not execute Docker or prove file
source retention. Journal/retry checks preserve the compact recipe.
`.tmp/entry-runtime-final-tests.log` records the Go 1.26.7 race run, including the
two affected Blueprint prerequisite cases after their missing runtime-role label
was corrected. Pinned vet and Staticcheck pass; the final architecture report has
123 pre-existing findings and no additions. No full CI or deployment ran. File
retention and recovery-reader integration were blocked on Secret deletion policy
at this run. The owner subsequently approved exact recoverable-Task pins; that
decision resumes implementation, not qualification by these tests.

H13 is local REL-01 support for the owner's 2026-09-14 restart boundary.
`TestClosePreservesEtcdRuntime` fails against the old stop-on-close implementation
in `.tmp/etcd-controller-restart-red.log`. The corrected manager and Controller
shutdown checks pass with Go 1.26.7 and race detection in
`.tmp/etcd-controller-restart-final-tests.log`; focused vet and Staticcheck pass.
`test_foundation_restart.py` executes the real remote probe body with fake
commands: Agent restart succeeds, while etcd restart/stop and changed Agent
identity fail. The old probe fails this regression in
`.tmp/etcd-controller-restart-verifier-red.log`; the corrected run passes in
`.tmp/etcd-controller-restart-verifier-final.log`. These verifier checks are
delivery safety, not host or application passes. No live restart, deployment,
traffic run or data check ran in this slice. The complete existing
`make verifier-helper-check` also passes in
`.tmp/etcd-controller-restart-helper-check.log`.

H14 is local SEC-07 support for the approved recovery Secret pin policy.
`TestSecretRecoveryPinBlocksDeletionAdmissionDirectFinalizationAndHierarchy`,
`TestSecretRecoveryPinBlocksDeletionRetry` and
`TestMalformedSecretRecoveryPinsFailClosed` exercise the actual deletion
boundaries with seeded, value-free pin records. They preserve the existing
Script checks and reject malformed or ambiguous memberships without deletion.
The focused run is `.tmp/secret-recovery-deletion-final.log`; vet evidence is
`.tmp/secret-recovery-deletion-vet.log`. Pinned Staticcheck passes with the
repository-local cache in `.tmp/secret-recovery-deletion-staticcheck-local.log`;
the first attempt could not write its external default cache. This is deletion-side proof only, not
automatic reservation/release, restored configuration, deployment or a live
SVC-15/JOURNEY-02 pass.

H15 extends local SEC-07 support with `internal/infra/tasksecretpins` lifecycle
tests. Preparation compares exact Secret source revisions and deletion fences;
activation joins a caller-owned Task transaction with two compares and two writes.
Tests cover interrupted preparation, changed replay, fixed bounds, activation CAS
loss, explicit release, restart/resume, unrelated operation preservation and
abandonment racing Task publication. Restart cleanup uses the durable descriptor,
not the lost process-local input list. Race, vet and pinned Staticcheck pass in
`.tmp/recovery-secret-pin-lifecycle-{tests,vet,staticcheck}.log`.
These are local module tests with a controlled store. The actual Task lifecycle
and startup callers, deployed Secret protection and recovery journey remain unrun.

H16 is local SVC-15/JOURNEY-02 source-acknowledgement support. The immutable source
repository tests cover independent lifetime, partial staging, changed replay,
missing/corrupt members and bounded reads/writes. `TestRuntimeConfiguration*`
checks success-only acknowledgement, head races, missing applied authority, Task
serialization and Retry preservation. Existing Blueprint success, ordinary Release
publication and Entry runtime reconstruction tests exercise the new Task bindings.
Evidence is `.tmp/runtime-configuration-module-tests.log`,
`.tmp/runtime-configuration-typed-task.log` and
`.tmp/runtime-configuration-codec-green.log`. Initial Entry parameter-count and
Retry-copy failures are preserved in the corresponding `entry-callers` and
`codec-proof` logs. Vet and pinned Staticcheck pass in the `typed-*` and `retry-*`
logs under `.tmp/runtime-configuration-`. Architecture reports the same 123 prior
finding identities in `.tmp/runtime-configuration-architecture.log`.

This retains source metadata, not all generated Component bytes. Other file
writers, exact source lifetime, automatic Secret pins, recovery execution and
live qualification remain unfinished. H5's live failure is not closed.

H17 is local SVC-15/JOURNEY-02 support for exact generated Component file content.
The retained-content repository checks immutable replay, partial publication,
independent Task lifetime, corrupt/missing chunks, exact metadata and byte bounds,
and rejection of Secret-derived sources before storage access. The Controller
resolver proves it returns retained bytes without consulting the renderer;
Blueprint and Route producer/replay checks exercise staging and loading.
Pinned race tests pass in `.tmp/materialization-content-race-20260914.log`.
Vet and Staticcheck pass in
`.tmp/materialization-content-{vet,staticcheck}-final-20260914.log`.
The first analyzer attempt coincided with a helper extraction and is retained
separately. No deployment or live recovery pass is claimed. Recovery must still
bind these retained records to its selected files and execution authority.

H18 extends local SEC-07/SVC-15 support through actual desired publication,
Agent claim, acknowledgement, failed-attempt Retry and Secret deletion. Tests
prove atomic pin activation and release authorization, failed/Retry preservation,
unchanged failed history, restart cleanup, unavailable-source rejection, expired
source Retry rejection, and newest-attempt expiry including a start/finish race.
The scheduler test proves cleanup runs before daily pruning. The pin-module
tests, these regressions and affected configuration/startup checks pass with the
race detector in `.tmp/recovery-secret-lifecycle-expiry.log`; focused vet and
pinned Staticcheck pass in `.tmp/recovery-secret-lifecycle-final-{vet,staticcheck}.log`.
Earlier test-fixture compilation errors and the first actual lifecycle pass are
retained in `.tmp/recovery-secret-actual-lifecycle-{first,second,third}.log`.
These are local controlled-store checks, not deployed source protection, file
restoration or a closing SVC-15/JOURNEY-02 result.

H19 is local SVC-15/JOURNEY-02 support for independent pinned-file verification.
Real filesystem tests reject changed bytes, mode, ownership, absence, unsafe
links and destination replacement during hashing, without repairing the selected
file. Truncated input is rejected before root access. Controlled Docker tests
prove a read-only bind, read-only root, fixed verification command, read-only
capability and distinct drift versus helper-failure results. The affected
materializer and runner packages pass race tests in
`.tmp/recovery-file-verification-packages.log`; the added replacement race passes
in `.tmp/recovery-file-verification-replacement.log`. Vet and pinned Staticcheck
pass in `.tmp/recovery-file-verification-{vet,staticcheck}.log`.
The initial test-only formatting error is preserved in
`.tmp/recovery-file-verification-first.log`; its corrected check passes separately.
No real Docker helper, deployment or recovery execution ran. H5 remains failed.

H20 is local SVC-15/JOURNEY-02 acknowledged-runtime reader proof. The shared
reader and ordinary/Blueprint producers preserve exact receipt YAML, bindings,
proxy metadata and retained-slot bytes, reject unavailable/foreign/mismatched
receipts, and never call the historical runtime renderer. The real two-pass
Blueprint store journey exercises receipt publication, next-publication source
comparisons, marker replay and Agent claim. Its second publication uses 40
comparisons/19 mutations; claim uses 18/7. A separate modeled maximum request
adds all 32 receipt comparisons and remains within unchanged limits at 225
comparisons/176 mutations; this is a request-budget check, not 32 live workloads.
Race evidence is `.tmp/acknowledged-readers-integrated-race.log` and
`.tmp/acknowledged-reader-producers-race.log`; vet and pinned Staticcheck pass in
`.tmp/acknowledged-readers-and-writers-{vet,staticcheck}.log`.
The complete ordinary Attach/Entry-to-failed-Deploy journey, file restoration and
live qualification remain unrun. H5 is not closed.

H21 is local SVC-15/JOURNEY-02 source-writer support. Route creation/removal and
applied Entry removal publish configuration references and Secret-pin ownership
with their Tasks. Entry completion advances its acknowledged configuration head;
never-applied removal remains Controller-only. Conflicting Environment owners,
invalid Route targets and conflicting shared desired-head revisions are rejected.
Two same-Environment completion effects and an identical Route/Service head
comparison are accepted without weakening ownership. The focused race check
passes in `.tmp/configuration-writers-primary-shared-head.log`; vet and pinned
Staticcheck pass in `.tmp/configuration-writers-final-{vet,staticcheck}.log`.
Earlier failure logs are preserved. Route Agent execution, recovery and deployment
remain unqualified; these checks do not close H5.

H22 is local SVC-15/JOURNEY-02 sealed-file and source-selection proof. Plan checks
reject unbound or overlapping recovery steps, foreign destinations, changed
metadata and invented files where the predecessor was absent. Source checks
retain prior bytes, owner and mode, derive repeatable probe/restore identities,
preserve original content-store identities, reject missing/foreign sources and
exclude unrelated files. The exact immutable snapshot can be loaded at a fresh
storage revision; this test does not emulate a real etcd compaction. Retained
Component content remains available to later generations without allowing future
content into an earlier one.
Race evidence is `.tmp/pinned-file-authority-combined.log`,
`.tmp/pinned-file-sources-valid-mode.log` and
`.tmp/pinned-file-source-consumer-first.log` (the latter's passing runtime/content
packages only; its transient etcd compilation failure remains preserved).
Vet and pinned Staticcheck pass in `.tmp/pinned-file-sources-{vet,staticcheck}.log`;
common execution-plan checks also pass in the bounded file-accounting logs.
Earlier test fixture failures are preserved. These are local checks, not file
recovery execution or a deployed result. H5 remains failed.

H23 is local SVC-15/JOURNEY-02 durable file-accounting proof. A real Task journal
records a started file write, and a restarted repository reconstructs that file's
compensation while excluding an untouched file. Foreign, reordered and non-Running
evidence is rejected. The canonical sequence and probe/compensate/proven cursor
boundaries are checked. The controlled record fixture does not exercise the full
candidate-assignment transition or Agent file I/O. Race, vet and pinned Staticcheck
pass in `.tmp/recovery-file-accounting-{race,vet,staticcheck}-20260914.log`.
No record-size, transaction or native-effect requirement changed; H5 remains failed.

H24 is local SVC-15/JOURNEY-02 Controller/Agent integration proof. The real two-pass
Blueprint producer, publication, replay, claim and acknowledgement test preserves
the prior file bytes and owner under the existing record and transaction limits.
Controller delivery tests load the original retained content under new recovery
execution IDs and reject changed metadata or missing bytes. Agent tests cover a
partially written file, durable admission before I/O, restore followed by separate
read-only verification, native re-probe after file restoration, reconnect and
buffer retirement. Deadline or epoch drift is rejected. The helper is controlled
in these Agent tests; they do not execute a complete failed Task on a live host.

The combined race check passes in `.tmp/pinned-file-recovery-combined-final.log`;
vet and pinned Staticcheck pass in
`.tmp/pinned-file-recovery-integrated-{vet,staticcheck}.log`. Initial failures in
older executor fixtures are preserved in `.tmp/pinned-file-agent-affected-first.log`
and subsequent correction logs. Those fixtures now provide sealed recovery pairs;
production validation was not relaxed. Architecture reports 122 pre-existing
finding identities, none added, in `.tmp/pinned-file-recovery-final-architecture.json`.
The extracted Agent executor removes one oversized-file finding; no baseline or
limit changed. Full CI and deployed recovery remain unqualified; H5 remains failed.

H25 records candidate qualification at `6c89049fa` on 2026-09-14, not a deployed
recovery pass. The broader tagged race run is
`.tmp/recovery-candidate-runtime-packages.log`. Controller, configuration-source,
runtime-configuration and retained-content packages pass. App startup initially
fails because its store fixture accepts only the older Script preparation scan;
the corrected test covers all three startup scans and failure at each one, passing
in `.tmp/recovery-candidate-startup-fixture.log`. The durable-store package has 30
failing test functions. Some fixtures lack acknowledged sources or count private
staging as published authority; these failures still need bounded classification.

VOL-07 fails in `TestVolumeRemovalSuccessorCompletesRetainedAbsence`: normal,
late-index and lost-response variants reach a production refusal at 27 comparisons
against the 26-comparison terminal limit. No limit changed and no live removal ran.
This is a candidate blocker requiring an owner decision, not a Backup/Restore test.
The existing issue record owns its required outcome.

The Agent, Agent-channel and Release-group package checks pass using the original
run plus the socket-permitted transport rerun in
`.tmp/recovery-candidate-{affected-packages,agent-loopback}.log`.
`make ci` stopped on 14 sandbox ownership errors in deployment fixtures; all 73
deployment checks subsequently pass with the real ownership view in
`.tmp/recovery-candidate-deployment-trusted.log`. Console checks/build, Controller
build and local Agent image smoke pass; their logs are
`.tmp/recovery-candidate-{ci,controller-build,agent-image}.log`. Generated files
match Git. The initial formatter invocation selected ambient Go 1.27; the pinned
formatter found six wrapping differences, now corrected without behavior changes.
Formatting and verifier-helper checks pass in
`.tmp/recovery-candidate-{format-stable,verifier-final}.log`.
These passing prerequisites do not make the failed durable-store gate green.

Read-only HOST-01 passed at 07:12 UTC, including owned cleanup, in
`.tmp/qa-pinned-recovery-20260914/host-health-20260914T071211Z.t0M5Ot/`.
The target still runs the old QA release. No QA reset, deployment or deliberate
fault ran. Full CI, live recovery, restart/upgrade continuity and the approved
30-minute traffic run remain unqualified.

H26 records the owner-approved VOL-07 correction after H25. A bounded diagnostic
in `.tmp/volume-retry-comparison-diagnostic.log` identifies the extra comparison
as the Environment configuration head. The shared desired publisher had attached
configuration authority to a Volume Task without file writes. The correction
selects that authority only for Blueprint Apply or explicit file writers and
rejects unexpected supplied authority on other desired mutations. Temporary
diagnostic instrumentation was removed. No terminal safeguard, transaction limit
or immutable history changed.

`TestVolumeRemovalSuccessorCompletesRetainedAbsence` passes all four variants in
`.tmp/volume-retry-owner-selection-green.log`: normal completion, late derived
indexes, lost-response replay and stale-Task refusal. The test also rejects
unrelated configuration authority on the claimed retry. Its existing successful
terminal assertions retain 26 comparisons, 18 mutations and 13,250 physical bytes.
Vet and pinned Staticcheck pass in `.tmp/volume-retry-{vet,staticcheck}.log`.

The full race rerun, `.tmp/volume-retry-integrated-store.log`, resolves 22 of H25's
30 failing durable-store functions without changing their assertions. Eight remain:

| Remaining functions | Traced mismatch; remaining proof |
| --- | --- |
| `TestBlueprintRuntimeRetentionActualPublisherSources`, `TestEnvironmentBlueprintPublishesComponentWithSharedDesiredHeadCompare` | Setup writes applied state without its configuration acknowledgement; the former also expects rejection only at final publication. Supply consistent authority or assert the intended earlier rejection, then rerun. |
| `TestBlueprintCandidateAfterNativeRollbackUsesSealedPredecessor`, `TestBlueprintNativePredecessorBlueGreenSnapshot`, `TestBlueprintNativePredecessorSourceCAS` | Fixture changes serving/render records without advancing the acknowledged runtime receipt. Align the simulated successful transition before testing candidate capture or later races. |
| `TestEnvironmentBlueprintCombinedMaximumInjectedFailurePublishesNoAuthority`, `TestEnvironmentBlueprintTopologyPublicationHasConstantCompactShape` | Audit doubles intercept private source staging as well as final publication; topology also expects the older comparison count. Target the intended final transaction and retain the actual budget assertions. |
| `TestBlueprintPublicationExcludesVolumeRemovalLock` | Global storage revision counts private staging as public Task/desired publication. Independently check current public authority, including the retained removal lock. |

These were source-traced test mismatches, not passing results or proof that no
further defect was hidden behind them. No additional product defect was confirmed
by that classification. H27 records their subsequent test-only corrections and rerun.
No QA reset, deployment, removal or fault ran; live VOL-07 and H5 remain unqualified.

H27 records production-operation candidate checks on `dde45bf9c` plus the
test-only fixture corrections. No production source, validation rule, transaction
limit or application configuration changed. Existing BP-04, SVC-15/JOURNEY-02,
VOL-07, BACK-01 and HTTP-06 cases receive local supporting proof only:

- H26's eight functions now pass. Applied-state fixtures include acknowledged
  configuration; native Deploy/Rollback fixtures include current/retained runtime
  receipts before the actual producer reads them. Original Blueprint state stays
  unchanged. Existing source-replacement/pruning, replay, recovery-proof and
  maximum-publication assertions remain.
- Fault injectors now target final publication, not private source staging.
  The topology assertion includes the configuration-head comparison (23 comparisons,
  12 writes); combined-maximum budgets are unchanged. Rejected Volume-conflicting
  publication is checked against current Task/queue/desired/configuration authority
  and the unchanged removal lock, not global storage revision.
- `.tmp/prod-operations-fixtures-first.log` passes seven corrected functions and
  the legal maximum-shape table, but records a misrouted topology audit. Its scoped
  correction passes in `.tmp/prod-operations-fixtures-topology.log`. The earlier
  compile error is preserved in `.tmp/prod-operations-fixtures-publication.log`.
- `.tmp/prod-operations-candidate-race.log` is the complete tagged root-module
  race run: the durable-store package passes in 141.843s. Only
  `TestBackingCreationSpecs` and
  `TestRouteRemovalSelectsAgentProviderPlanForAppliedRoute` failed. The former
  omitted the required explicit Valkey authentication choice. The latter's store
  returned unrelated index records for configuration reads. Its test double now
  retains acknowledged configuration and immutable staging with revision checks;
  the actual Route publisher and original provider/Task assertions are unchanged.
  Both full affected packages pass in
  `.tmp/prod-operations-fixtures-adapters-network-corrected.log`; the initial
  test-edit syntax error remains in `.tmp/prod-operations-fixtures-adapters-network.log`.

Root vet, pinned Staticcheck and formatting pass in
`.tmp/prod-operations-candidate-{vet,staticcheck,format}.log` before the two
later fixture corrections. Architecture reports the same 122 existing finding
identities in `.tmp/prod-operations-candidate-architecture.json`; no finding or
limit was added. After the two later fixture corrections, affected vet and pinned
Staticcheck and the root formatting gate pass in
`.tmp/prod-operations-fixtures-final-{vet,staticcheck,format}.log`. The final
architecture report is `.tmp/prod-operations-fixtures-final-architecture.json`,
with the same 122 finding identities and no additions. These results
do not close H5, H8, live restart/upgrade continuity or the approved 30-minute run.
No live operation ran; the existing QA installation remains unchanged. This is
not full CI or production qualification; Backup/Restore remains deferred.

H28 records the fresh live QA baseline on 2026-09-14, clean source `76578c72f`,
version `0.0.0-qa.prodops20260914.1`. Evidence is
`.tmp/qa-prod-operations-20260914-HrZj9vKz/`; exact Controller binary/release and
Agent digests are in `host-health-20260914T083836Z.JpFM59/api.json` and the private
run record. The approved guarded reset archived installation/history/logs and
preserved registry/images; it removed 21 managed containers, nine networks and
nine disposable Volumes. Volume contents were discarded, not backed up.
`fresh-install.log`, `fresh-workload-setup.log` and `deploy-fresh.log` record normal
installation and application deployment. HOST-01's API/CLI journey and owned
tunnel cleanup pass. `verify-fresh-app.log` and `fresh-baseline.json` prove all
11 Services healthy, authenticated registration/profile, route isolation, actual
WebSocket authorization/subscription, Identity/TLS, reviewer login and both native
password-only backing clients. JOURNEY-01 is PARTIAL: no fresh full background-job
transaction, public-ingress or continuity claim is made. Test inputs and topology
stay ignored; this is not full CI or production qualification.

H29 records BP-05 FAIL during the same run. The unchanged Apply completed at
08:45:23 UTC but recreated the ingress router at 08:45:17 UTC. `fresh-runtime.json`
and `after-unchanged-reapply.json` show only that container changed identity/start
time across Apply. Its image is unchanged; plan/generation labels and Compose
configuration hash changed. Serving Releases and checked profile survived.
`cutover-recovery-proof.log` later failed its retained-Component assertion after
four successful Attaches because it compared against the pre-reapply snapshot.
`cutover-attached-runtime.json` versus the saved post-reapply observation proves
the Attaches preserved the replacement router. The attribution error does not
close the actual reapply failure; the [finding](issues/runtime-qualification.md#unchanged-blueprint-recreates-router)
separates observed evidence from the incomplete retention diagnosis.

`cutover-paused-baseline.log` confirms healthy platform/application, authenticated
profile and zero active Tasks after the stop. The second backing and its four
Attaches remain; Entry still selects the original backing. No Entry cutover,
old Detach, failed-candidate injection, Controller restart, upgrade or sustained
run executed. ATT-12 and SVC-15/JOURNEY-02 are incomplete; H5/H8 remain open.
No product code or history was changed, no recovery workaround ran, and no
interruption duration was measured. Owner decision is required before repair
or resuming the paused journey. Exact action receipts and evidence hashes are
retained in the private run directory.

H30 records the owner-approved local BP-05/H29 repair. Read-only, fixed-revision
capture in `.tmp/router-retention-20260914-KE6VP5fr/planning-probe-nnf2kmnh/`
proved that the pre-reapply artifact lacked native proxy configs introduced by
normal Service Deploys. Whole-map comparison prevented unchanged router retention.
The owned diagnostic tunnel closed successfully. `red.log` reproduces that cause
in `TestComponentRetentionScopesResourceChangesToConsumer`; `green.log` passes
after comparison is scoped to consumed resources. Changed/missing consumed
definitions, actual Component changes, source validation and input immutability
remain covered. `real-artifact-replay.log` verifies the captured QA pair locally.
The full Controller and Blueprint Release package race checks pass in
`controller-loopback.log` and `packages.log`, respectively. The initial Controller
attempt could not open sandboxed test listeners; no test workaround was added.
`vet.log` and `staticcheck-local.log` pass; the initial analyzer attempt's external
cache denial remains in `staticcheck.log`. `architecture.json` has the same 122
existing findings and no additions. No limit, dependency, API or execution
authority changed. These are local results, not a deployed or closing BP-05 pass.

H31 records live continuation on clean `2895989f2`, version
`0.0.0-qa.prodops20260914.2`. In the H28 private run directory,
`retention-update-result.json` binds the completed protected update to its exact
release and binary/image digests. `retention-update-traffic-result.json` records
45 seconds, 45 authenticated HTTP 200 responses, zero request errors, eight pongs
on the same WebSocket and maximum HTTP latency 0.245 seconds. Before/after
receipts preserve application, backing, router and etcd container identities,
images and start times; the Agent update is permitted. JOURNEY-05 is PARTIAL,
not the approved 30-minute/five-request-per-second run or full upgrade proof.

The isolated BP-05 canary completed network-only and configured Blueprint Apply,
then its first normal blue-green Deploy failed before Task acceptance. SVC-06 is
FAIL for this profile-disabled variant; reapply and held canary traffic did not
run. `retention-canary.log` and `canary-deploy-controller.log` record
`sealed release binding Service is absent`. Read-only source capture in
`.tmp/router-retention-20260914-KE6VP5fr/planning-probe-ion3ukfy/diagnostic.log`
shows the Service in desired YAML and metadata, but only in staged Release
metadata. The owned tunnel closed. Attach projection marshals the loaded project
without including its disabled Service definitions. No input workaround, retry,
new product repair or runtime-file modification followed. The isolated canary
is preserved. `canary-failure-diagnostic.log` confirms all 11 real application
Services healthy, authenticated profile success and no active Task. BP-05 closing
proof, Entry cutover, old Detach and failed-candidate recovery remain pending;
the new defect requires the owner's scoped repair decision.

H32 records the approved local H31 repair. `TestPublishFirstBlueGreenPreservesProfileDisabledService`
uses the real publisher, immutable render storage, plan reconstruction and prepared
runtime authority with empty and replacement Attach selections. Both variants fail
with the live missing-Service error in `.tmp/first-deploy-profile-red.log`.
Preserving definitions exposes the next zero-replica rejection in
`.tmp/first-deploy-profile-green.log`: explicit Deploy still treated its selected
profiled Service as disabled. Selecting only named Release members resolves that
rejection without removing profiles or weakening runtime validation.
`.tmp/first-deploy-profile-packages.log` passes both complete affected Controller
and Release-operation race suites, including exact singleton replica counts,
source immutability, current network selection and publication/reconstruction.
Final vet and pinned Staticcheck pass in `.tmp/first-deploy-profile-final-vet.log`
and `.tmp/first-deploy-profile-final-staticcheck.log`. Formatting passes; architecture
has the same 122 finding identities, with no new findings or changed limits, in
`.tmp/first-deploy-profile-architecture.json`. This is local proof, not a live pass.

H33 records live continuation from signed `13d130929`, version
`0.0.0-qa.prodops20260914.3`, in the H28 private evidence directory.
`profile-update-result.json` binds the protected update to exact release and
binary/image digests. Its traffic receipt reports 45 authenticated HTTP 200
responses in 46 seconds, zero errors, nine pongs on the same WebSocket and maximum
HTTP latency 1.858 seconds. Application, backing, router and etcd container
identities/images/start times survived; Agent replacement is permitted. This is
PARTIAL JOURNEY-05, not sustained-load or complete upgrade qualification.

`canary-resume.log` records the same isolated profile-disabled Service's successful
first blue-green Deploy, closing H31's observed rejection. The subsequent exact
Blueprint reapply completed from 09:52:09 to 09:52:16 UTC but recreated its router
at 09:52:10 UTC. `canary-before-reapply.json` and `canary-after-reapply.json` show
only the router identity/start time changed; the native workload and stable proxy
were preserved. `canary-traffic.stderr` records `RemoteDisconnected` on the held
HTTP connection. BP-05 is FAIL, not a closing pass for the earlier H30 correction.
The probe lacks per-request timestamps, so no exact outage duration is claimed.
`canary-after-reapply-http.json` subsequently reports HTTP 200;
`canary-connection-diagnostic.log` proves the real application's 11 Services and
authenticated profile remain healthy. `canary-after-reapply-active.json` is empty.
No retry, input workaround, Entry cutover, old Detach or candidate fault followed.
The owned canary remains for diagnosis; further repair requires owner decision.

H34 records the owner-approved H33 acknowledgement repair. The no-native-Release
Blueprint procedure was incorrectly grouped with metadata-only edits, so success
could leave the older applied Component artifact. `TestConfiguredBlueprintAcknowledgementRetainsAppliedArtifact`
fails before the correction in `.tmp/configured-router-ack-red.log`. It now requires
the sealed applied artifact and root/predecessor guards on success while failed
Apply and metadata-only edits publish nothing. The actual Blueprint producer and
terminal test additionally requires the Component artifact to advance and the
independent native Service runtime receipt to remain unchanged.

The complete durable-store race run in `.tmp/configured-router-ack-package.log`
passes except five functions sharing an old assertion that the aggregate artifact
must stay unchanged. That assertion confused aggregate Component authority with
the native Service receipt. After its correction, all five pass in
`.tmp/configured-router-ack-package-rerun.log`; focused acknowledgement, Entry and
Blueprint checks pass in `.tmp/configured-router-ack-final-focused.log`. Final vet
and pinned Staticcheck pass in `.tmp/configured-router-ack-final-{vet,staticcheck}.log`.
Formatting passes. `.tmp/configured-router-ack-architecture.json` has the same 122
finding identities, with no new findings or changed limits. This is local proof;
the fresh first-Apply/Deploy/reapply live cycle remains required. Existing applied
history is neither backfilled nor rewritten.

H35 records live qualification on signed `18e936fa9`, version
`0.0.0-qa.prodops20260914.4`, in the H28 private evidence directory.
`router-ack-update-result.json` binds the completed protected update to exact
release and binary/image digests. Its 45-second traffic window has 45 authenticated
HTTP 200 responses, zero errors, nine pongs on the same WebSocket and maximum
HTTP latency 0.214 seconds. Application, backing, router and etcd runtime identities
and start times survive; Agent replacement is permitted. This is partial update
qualification, not the sustained run.

`ack-canary.log` and `ack-canary-reapply-result.json` record BP-05 first-cycle PASS:
fresh configured Blueprint Apply, explicit first Deploy and exact reapply preserve
all router/native container identities and start times. `ack-canary-traffic.json`
records 90 successful requests with identical content on the same TCP connection
from 11:09:09 to 11:09:54 UTC, enclosing the entire reapply Task. Both owned canary
Environments remain for normal cleanup; the original failed run is preserved.

`cutover-resume.log` and `native-cutover-result.json` record the resumed four-consumer
Attach/Entry/Detach PASS without replaying setup. After Entry rebinding, both actual
realtime replicas pass password-only authentication, unauthorized/admin rejection
and async subscription while both backing networks remain attached. Only then do
the four dependent-first old Detaches run. All five consumer replicas afterward
use only the new network; both client probes pass again. Serving Release ids,
stable proxies, retained Components and authenticated profile survive; all 11
application Services are healthy. ATT-12's tested overlap variant passes, not
by direct-IP substitution or finishing Detach before checking authentication.
Fresh complete background-job and sustained-traffic qualification remain open.
The approved failed-candidate probe then starts from this healthy baseline; its
separate terminal and recovery outcome must be recorded before SVC-15 can pass.

H36 records the subsequent SVC-15/JOURNEY-02 attempt on the same candidate.
`candidate-fault-proof.log` and `failed-rollout-fault.json` prove only the new,
identity-selected candidate was stopped before promotion. The original serving
slot was untouched. The run stops after its 600-second observation limit with
the original Task still nonterminal; no additional fault, manual repair, healthy
Deploy, hook removal or Abort ran.

`failed-rollout-events.log` captures the public Task stream under an eight-second
reader timeout (exit 124 expected). The Service proxy recovery-probe step has at
least 271 alternating running/failed attempts. That identification comes from the
sealed ordinary Release step mapping, not from generic completed-step counts.
Controller and Agent warning logs contain no observed report-rejection error.
`during-failed-rollout.log` verifies all 11 application Services, authenticated
profile and original serving Releases during the failure wait. The Agent is
healthy with one claim. Recovery is BLOCKED, not successful merely because the
application still serves. Exact probe failure diagnosis and any repair require
owner decision; the active Task, owned hook and evidence remain preserved.

The final check then catches deterioration: `recovery-bound-expired.log` fails
its 20-second health observation because the Agent is degraded and all Service
observations are unavailable. `recovery-final-controller.log` identifies the
secondary cause: retries reach the maximum 1,000 durable Task events, so Controller
rejects subsequent Agent progress. `recovery-bound-result.json` records the Task
still running with one claim. Earlier healthy/no-rejection observations do not
describe this later state. No event limit, runtime file or history was modified.
`recovery-final-profile.log` records normal token refresh and authenticated HTTP
200 with the original profile preserved; `recovery-final-result.json` separates
that application pass from the stuck Task and degraded Agent. This does not prove
uninterrupted traffic throughout the fault or successful automatic recovery.

H37 diagnoses H36 and verifies its local Volume-scope correction on 2026-09-14.
Private `recovery-agent-prelimit.log` preserves the forward health timeout followed
by compensation and probe errors: `release restoration observation identity
diverges`. `planning-probe-bzw7j1a4/diagnostic.log` captures the exact immutable
predecessor; its supervised read-only tunnel closed successfully.
`recovery-collision-runtime.json` shows the restored workload's matching labels
and healthy state. `recovery-resource-collisions.jsonl` identifies five unrelated
project Volumes absent from that artifact. The scoped verifier excluded unrelated
networks but rejected every Volume collision. This explains the first recovery
failure, independently of the later journal exhaustion.

`TestReleaseRestorationWorkloadSetRequiresExactHealthyLineage` now covers unrelated,
required and unidentified Volume collisions beside the exact healthy predecessor.
Its new unrelated-Volume variant fails before correction in
`.tmp/recovery-volume-scope-red.log`. The corrected shared lifecycle scope excludes
only named Volumes absent from the sealed artifact; exact workload lineage and
required/unknown resource guards remain. The recreate fixture now names its
required Volume explicitly instead of assuming every project Volume is selected.
Focused tests pass in `.tmp/recovery-volume-scope-green.log`; the full Agent race
package passes in `.tmp/recovery-volume-scope-agent.log`. Vet, pinned Staticcheck,
formatting and diff checks pass. Architecture retains the same 122 existing
finding identities with none added; `.tmp/recovery-volume-scope-architecture.json`
records that comparison without a baseline change. The repair is local, not deployed or a live SVC-15
pass. No host mutation or history rewrite ran. Event-saturation handling remains
an unresolved contract decision before deployment and continuation of the same Task.

H38 verifies the owner-approved rolling Task history on 2026-09-14, following H36's
hard-cap failure. `TestTaskEventRollingWindowPreservesRecoveryAndReplay` appends
1,003 events through the repository, retaining exactly 1,000 events and replay
records with sequences 4–1,003. It exercises an atomic comparison conflict, lost
commit response and repository restart, retained/trimmed replay, changed payload
and stale identity rejection, out-of-order ordinal eviction, preserved file and
Component mutation evidence, cursor expiry and terminal stream drain. Controller
projection and CLI stream regressions prove retained step state and fresh streams
starting above sequence 1,000. These are TASK-06/07 and SVC-15 local checks, not
live recovery or uninterrupted application evidence.

The complete durable-store and Agent-channel race suites pass in
`.tmp/task-event-window-packages.log`. Final affected race checks pass in
`.tmp/task-event-window-final.log`; final vet and pinned Staticcheck pass in
`.tmp/task-event-window-final-{vet,staticcheck}.log`. Pinned API regeneration passes
in `.tmp/task-event-window-api-final.log`. Formatting and diff checks pass;
`.tmp/task-event-window-final-architecture.json` retains the same 122 existing
finding identities without a baseline change. The earlier CLI sequence ceiling
failure is retained in `.tmp/task-event-window-surface.log`; its corrected check
passes in `.tmp/task-event-window-cli.log` and the final affected run.

Private `recovery-history-before-trimming.jsonl` archives the live Task's existing
1,000 public events before deployment. The Task remains on the old candidate;
neither H37 nor H38 is deployed. The healthy/idle Agent requirement prevents normal
native update while recovery is stuck. A one-time data-preserving repair installation
awaits owner approval. No runtime reset, process restart, event deletion or new
fault occurred in this local repair. After deployment, resume that same Task and
require exact restoration proof before further faults; normal upgrade qualification
remains separate.

H39 records the explicitly approved one-time QA repair on 2026-09-14, source
`7322d2806`, version `0.0.0-qa.prodops20260914.5`. Staging passes in private
`recovery-repair-stage.log`; the selected Controller and Agent digests are retained
in `recovery-repair-candidate.json`. The installer archives the old binaries,
configuration and settled native-update metadata verbatim on the QA host under
`/root/.groundplane/.tmp/recovery-repair-7322d2806`. With the Controller stopped,
an exact revision comparison changes only the enrolled Agent image. Its ID,
generation, credentials and Task assignment remain intact. The original Task is
compared unchanged before startup. The normal reconciler replaces the Agent;
the installer does not change Task history, results, files owned by applications
or etcd runtime. This is repair evidence, not native-upgrade qualification.

The original failed rollout reaches terminal `timed_out` with its original failed
step, execution epoch 754, exact recovery-record digest and compensated proxy proof.
The Agent is healthy with no in-flight claim. `repaired-task-events.jsonl` contains
exactly 1,000 events at sequences 5–1,004 and drains to completion. The original
1,000 events remain archived separately. `repaired-recovery-result.json` and
`resume-repaired-rollout.log` verify all 11 healthy Services, the saved authenticated
profile and serving Releases, five native consumers on only the current backing,
and both actual realtime clients' password authentication and async subscription.
Every non-Agent container identity, image and start time, including etcd, survives
the installation. `recovery-repair-traffic-result.json` records 45 HTTP 200s,
zero failures and nine pongs on the same WebSocket over 45 seconds, maximum HTTP
latency 0.394 seconds.

The owned sleep hook was removed through its normal operation. The subsequent
healthy Deploy completed, removed the old failed candidate and preserved the
profile and unrelated serving Releases (`failed-rollout-result.json`). Hook/source
history remains in Git/Task records; no application data was deleted. SVC-15's
resumed recovery and follow-up Deploy pass after repair. H36 remains a failed
bounded run; repeat the failure case cleanly before claiming unattended recovery.
This result does not qualify normal upgrades or the approved sustained run.

H40 separately passes REL-01's normal Controller restart on the same candidate.
The foundation restart journey verifies a new Controller PID, preserved runtime
directory inode, unchanged Agent identity/generation with permitted restart, and
identical etcd container ID, start time and restart count. All non-Agent container
identities/images/start times, serving Releases and the authenticated profile stay
unchanged. `repaired-controller-restart-foundation.log` retains one transient
Controller API error during its permitted readiness wait; application traffic
has no failure. `repaired-controller-restart-traffic-result.json` records 45 HTTP
200s and eight pongs on the same WebSocket over 45 seconds, maximum 0.209 seconds.
`repaired-controller-restart-result.json` records the combined assertions. Owned
traffic processes exited; the application and evidence remain. This is not a
host-reboot, pressure, sustained-load or native-upgrade pass.

H41 is the clean SVC-15/JOURNEY-02 rerun on `7322d2806`, deployed as
`0.0.0-qa.prodops20260914.5`, on 2026-09-14. The prior baseline passed all 11
Service observations and the saved authenticated profile. A new owned sleep hook
held the pre-promotion window; the test stopped only the exact new candidate.
The primary forward health step timed out at 13:49:25 UTC. Compensation immediately
returned `candidate restoration proof failed`, followed by repeated failed probes.
The observer stopped after 600 seconds without a terminal Task. This is FAIL, not
a successful automatic recovery or another passing H39 run.

The read-only immutable-input probe and actual runtime snapshot identify the cause:
the saved predecessor proxy expects the last Deploy's plan and render generation,
but that successful Deploy preserved the original proxy container and its old
labels. `PrepareCandidateRuntimes` copies candidate proxy ownership when constructing
the post-switch receipt; its fixture verifies switched config but not distinct old
proxy labels. The helper's exact inventory check rejects that mismatch before
compensation. Do not weaken ownership validation or rewrite the sealed Task to
turn this run green. The producer needs to preserve the acknowledged proxy identity
separately from newly applied proxy configuration.

Evidence is private `clean-candidate-fault-proof.log`,
`clean-failed-rollout-{accepted,fault}.json`, `clean-recovery-initial-agent.log`,
`clean-recovery-failed-runtime.json` and `planning-probe-l9bn_1lt/diagnostic.log`.
The read-only tunnel closed successfully. `clean-failure-profile-result.json`
confirms the saved profile remains usable and unchanged after normal token refresh.
The Agent remains healthy with one in-flight claim; the Task, hook and stopped
candidate remain. No second fault, abort, manual workload repair or reset ran.
The owner approved correcting the writer and rebuilding the disposable QA state
with incident evidence retained. Normal upgrades, sustained traffic and interruption/reboot
remain paused, not attempted passes.

H42 records the owner-approved capacity cleanup during this continuation. The QA
filesystem had 721–722 million available bytes, reported as 97 percent used.
Two obsolete reset archives occupied about 2 GiB. Both were streamed into ignored
local `prior-qa-reset-archives.tar`; its SHA-256 matched an independent second
remote tar stream before removing only the two remote source directories.
`archive-relocation-{verified,result}.json` records exact paths, size and hash;
`archive-relocation.log` shows available space increasing to 2,815,567,872 bytes.
The removed copies are recoverable from that local archive. Application data,
etcd, images and the current repair archive were unchanged.

Version `0.0.0-qa.prodops20260914.6` then built and staged successfully, with its
immutable identities in `normal-upgrade-stage.log`. It was not activated and still
contains H41's defect. The ignored 30-minute mixed-workload probe is prepared,
not run: no fixture users, load traffic or queued jobs were created. Preparation
does not count as REL-04 or upgrade evidence.

H43 records H41's local writer repair, not a deployed recovery pass.
`ComposeWorkloadApply.EnsureProxy` identifies whether the forward operation
applies the proxy. When false, the receipt now combines the candidate workload
with the sealed predecessor's proxy ownership, YAML and resources, then applies
the exact switch configuration. When true, candidate proxy ownership remains
correct. This covers ordinary Deploy and Rollback without changing recovery
inventory checks, Task history, first-Deploy creation or Blueprint execution.
Conflicting shared resource definitions fail before receipt publication.

The strengthened Deploy/Rollback regression fails before the repair; the copied
original tool output is `.tmp/proxy-receipt-fix/focused-red-copied-from-tool-output.txt`.
Its focused green run is in that directory. The first full package run exposed
a shared fixture rename; moving that mutation into the new regression fixed the
test coupling without a production workaround. Final proof in
`.tmp/qa-proxy-recovery-20260914-pH4bBi2l/` includes passing pinned Go 1.26.7 race
checks in `writer-executionplan.log`, `writer-publication.log` and
`writer-controller.log`. Vet, pinned Staticcheck and changed-file formatting pass.
`writer-architecture.json` has the same 122 prior finding identities, none added
or removed; no baseline or limit changed. The owner-approved disposable rebuild
and live recovery rerun remain pending. Private reset preparation has not stopped
the Controller or changed application data.

H44 records the owner-approved disposable QA rebuild after signed repair
`939fd039f`. Fresh exact-target checks and a stopped Controller preceded removal
of 28 managed containers, 11 reviewed never-started QA helpers, 11 managed networks
and nine disposable Volumes. Deleted Volume data is not recoverable. Registry
and images were preserved; the saved pre-installation resolver was restored.
The old installation and Task history were archived without editing them.
`incident-installation.tar` in `.tmp/qa-proxy-recovery-20260914-pH4bBi2l/` matches
a second remote tar hash; only then was the exact remote archive removed.
The `reset-*` and `archive-relocation-*` receipts record scope and hashes.
Space increased to 4,216,629,248 available bytes. This reset is an approved
qualification setup, not product-owned recovery of the invalid H41 Task.
Fresh `0.0.0-qa.proxy20260914.1` installation and Agent enrollment completed.
`deploy.log` records release `sha256:81c670b0710f015621d1420a060db89686da831e75bf010e54114fbc52f5e195`,
Controller `sha256:b04e1ff189fc1db8b2fd6e5524860c2e717babbeb100d02399e0a79bc5a0bcda`
and Agent `sha256:7fae2dbebed889168d0a630a9db9dc79025469bde2e87575addf103567b4a5d0`.
Application setup is running; this installation record claims no new recovery,
hosting-continuity or upgrade pass.
The read-only Host API/CLI journey passes independently, with owned cleanup, in
`host-health-20260914T143727Z.bO8sNS/` under the same evidence directory.

H45 records the fresh hosted-application baseline on `939fd039f` /
`0.0.0-qa.proxy20260914.1`. Normal hierarchy, explicit password-only backing
creation, Secret import, six Attach waves and the grouped Blueprint Apply
completed. Eight ordinary Service Deploys then completed, including first
blue-green Deploy after configured-only Apply. No runtime files or history were
manually repaired. Evidence in the H44 directory is `bootstrap-workload.log`,
`deploy-fresh.log` and their exact accepted/final Task receipts.

`verify-fresh-app.log` passes actual registration/OTP/profile access, route
isolation, unauthorized and foreign-session denial, strict TLS, reviewer login,
held private-channel subscription and both native realtime replicas using
password-only authentication. All 11 Services and the platform are healthy.
`identity-full-proof.log` and `identity-full-result.json` additionally pass
clearly marked synthetic uploads through real scanner/worker processing,
authenticated reviewer approval and the signed callback updating the backend.
These are fresh functional hosting passes, not failed-rollout recovery,
uninterrupted upgrades, sustained load or public/provider ingress qualification.

H46 passes the clean SVC-15/JOURNEY-02 sequence on `939fd039f` /
`0.0.0-qa.proxy20260914.1`. Exact reapply preserves the baseline; four new Attaches,
Entry rebinding and dependent-first removal of four old Attaches complete.
Both native realtime replicas authenticate and subscribe during the overlapping
backing window and after Detach. All five consumer replicas use the new backing
network only; proxies, retained Components, serving Releases and checked data
survive. `cutover-recovery-proof.log` and `native-cutover-result.json` retain proof.

A successful API Deploy then creates the receipt consumed by the next failed
candidate. The fault stops only that exact candidate before promotion. The Task
started at 14:48:38 UTC and ended `timed_out` at 14:54:30 UTC, preserving its
original failure. An authenticated profile check passes during the timeout,
and all 11 Services, current backing bindings, actual password-only clients and
saved profile pass after automatic recovery. The Agent is healthy with zero
in-flight claims. The owned hook is removed normally; a later healthy Deploy
completes and cleans the failed candidate without changing unrelated Releases.
No repair, reset, Task edit or ownership-check waiver occurred during this run.
Receipts in the H44 directory are `candidate-fault-proof.log`,
`failed-rollout-{accepted,fault,terminal,result}.json`,
`during-candidate-timeout-result.json` and `automatic-recovery-result.json`.
H41 remains failed historical evidence; the new pass does not rewrite it.

The approved sustained run has started, not passed. Its first setup attempt
used an incorrect probe bootstrap path and stopped before creating users or
traffic; `run-sustained.log` is retained. After inspecting the real container
working directory, the corrected run creates five scoped users and holds five
authenticated WebSockets, recording `sustained-results.jsonl`. The separately
staged `0.0.0-qa.proxy20260914.2` has not yet been activated. Upgrade, sustained,
interruption and reboot claims require their own completed results.

H47 records the normal native upgrade sequence on the H43 production source.
From `0.0.0-qa.proxy20260914.1`, normal update Task
`task_01M2G6WAK0MJDQQX4QAJ2J2A2W` installs `.2`. A deliberately non-starting
Controller candidate then fails as `task_01M2G6Y4MG2KRYWJF49AVCND63`; native recovery
restores `.2` without application repair. The failed Task remains failed/recovered.
Normal update `task_01M2G78G666NMXE8JF5TTCYXMH` subsequently installs `.3`, whose
Controller is `sha256:601dabb8921ae717d47a5e2a0ea5b7ade6401551b0255c819eb98237563f1c2e`
and Agent is `sha256:d0d4ab37cf415bfdede9f1c4990586d5bc061f0122494b5428c909dd11bfad8b`.
Every non-Agent container identity/start time, serving Release and checked profile
is preserved through each transition. The foundation normal-restart verifier then
passes with unchanged etcd and application runtimes, permitted Agent restart and
owned marker cleanup. Receipts in the H44 directory are `normal-upgrade-result.json`,
`native-failure-result.json`, `successive-upgrade-result.json`,
`normal-restart-result.json` and `normal-restart-foundation.stdout`.
These operations ran during the active mixed workload; final traffic qualification
remains pending. This does not prove every UP cancellation, journal-phase,
Agent-startup-failure, standalone-update or ingress variant.

H48 records REL-03's Controller-loss variant on `.3`. After observing the exact
running sleep-only Script, the probe kills only the Controller process. Original
Task `task_01M2G7FRHB93NG33CQ4T3FAF7X` completes within its 120-second observation
bound. Exact-key replay returns the same Task. A live Docker event stream proves
one physical runner start; the runner and owned Script are removed, and all
11 Services, the existing profile and serving Release identities survive.
`interrupted-task-live-result.json`, `interrupt-live-events.jsonl` and
`interrupted-task-live.log` retain evidence in the H44 directory. The first
attempt also completed but its retrospective Docker buffer omitted the start
event, so `interrupt-incomplete-result.json` remains incomplete proof, not a
duplicate-run defect or a pass. Its owned Script was removed before the corrected
probe. No product code changed. The separate Agent-loss variant also passes:
the exact Agent process is killed while a runner is active, original Task
`task_01M2G7JNWF0CGYRAKWW8XAREZS` completes, replay returns that Task, and live events
show one runner start. Application profile/Release authority and owned cleanup
pass. Evidence is `interrupted-task-agent-result.json`, `interrupt-agent-events.jsonl`
and `interrupted-task-agent.log`. Reboot and other effect boundaries remain separate.

H49 passes the approved REL-04 target and the measured private-ingress
JOURNEY-05 sequence from H47 on `.1` through `.3`: 1,800.035 seconds, five
authenticated clients at one request/second each, 9,000 HTTP 200 responses and
zero unexpected request failures or WebSocket disconnections. Each of the same
five authenticated WebSockets receives 360 pongs; all 30 real queued jobs reach
their intended held channel. Normal access-token refresh is exercised without
lowering the requested load. HTTP p95 is 0.194049 seconds, below the one-second
target; maximum is 2.299403 seconds. The maximum is not hidden by the passing
percentile. The primary application profile and all serving Release identities
survive, with all 11 Services and the platform healthy. The probe closes its
connections and joins its workers; five scoped QA users remain as declared test
data. No production source or application source changed during the run.

Evidence in the H44 directory is `sustained-results.jsonl`,
`sustained-result.json`, `sustained-complete.json` and
`run-sustained-path-fixed.log`. The mixed window includes normal upgrade,
broken-Controller recovery, later successful upgrade, normal restart and the
separate Controller/Agent process-loss probes in H47–H48. This is bounded private
hosting and upgrade continuity, not public/provider ingress, indefinite uptime,
resource exhaustion, every native activation phase or complete production
qualification. A final already-selected standalone Agent update returns
`state.conflict`/409 with unchanged Tasks, all container identities/images/start
times and native update history (`selected-agent-refusal-result.json`). It proves
refusal and current selection, not a successful standalone replacement.

H50 records an unsuccessful guest-reboot probe on the H49 installation. The guest
accepted `systemctl reboot`, but SSH did not return within 240 seconds. Preserved
Firecracker and serial logs show the guest reboot request followed by successful
VMM process exit, not a restarted VM. The VM manager's subsequent start command
reported success without a new process; a later manager reboot was attempted.
Neither attempt establishes automatic reboot or GP cold-start qualification.
The owner stopped that recovery work, destroyed the VM and supplied a fully
fresh QA replacement at the same address and SSH identity. No pressure test ran.
Evidence remains in the H44 directory: `reboot-proof.log`, `reboot-command.json`,
`reboot-firecracker.log`, `reboot-firecracker-console.log`,
`reboot-manual-vm-start.log` and `reboot-manager-recovery.log`.
H43–H49 remain evidence for the destroyed installation only. Replacement setup
uses `.tmp/qa-owner-reset-20260914-hK6Yqeii/`, never old accepted Task receipts.
Further reset/reboot needs an owner decision; no VM-manager repair is selected.

H51 records the owner's replacement QA installation from signed `2676fbae6`,
with unchanged H43 production repair, as `0.0.0-qa.owner20260914.1`. Strict SSH
verification succeeds against the retained trusted identity; preflight found an
uninstalled host with about 17.5 GB available. Normal prerequisite setup and fresh
installation complete. Agent enrollment Task `task_01M2G9KK5K31GQYG0WD74N36F2`
completes; release is `sha256:8b40637feb476439f5446202409dedd475619019af5835ea98727729ccdc25ff`
and Agent is `sha256:621f7f1f793d1315d00a4e00702b3fc313fb575c99b4cb7062c915ed0dba38e9`.
The Host API/CLI journey passes with owned tunnel cleanup in
`.tmp/qa-owner-reset-20260914-hK6Yqeii/host-health-20260914T154737Z.GpvuV8/`.

Both unchanged application images transfer and publish at the exact previously
qualified registry digests. The private transfer check initially compared a
classic Docker config ID with the replacement's containerd manifest ID. Retained
diagnostics prove identical configuration and layers and the expected manifest.
The corrected private probe verifies that manifest and the configuration/layers,
then verifies the published RepoDigest; it resumes publication without another
transfer. No GP or application code changes. `deploy.log`, `publish-images.log`,
`image-transport-comparison.json` and `publish-images-resumed.log` retain evidence
in the same directory. Fresh application setup is active; this record does not
yet qualify its functional behavior, recovery, upgrades or pressure on the
replacement. The earlier installation's H43–H49 results are not silently rerun
or represented as current observations.

H52 passes the replacement's fresh functional hosting baseline on H51's
`0.0.0-qa.owner20260914.1`. New hierarchy, PostgreSQL, explicitly password-only
Valkey and credentials are created through normal GP actions. All six Attach
stages complete; Blueprint Task `task_01M2G9RAKS1GC9GHWEW4W260QP` completes all
43 steps, including TLS preparation. Eight ordinary Deploys then complete.
No resource identities, accepted Task receipts or runtime/data from the destroyed
installation are restored.

All 11 Services and the platform are healthy, and the Agent has zero in-flight
work. Registration/OTP/profile, denied unauthorized/foreign access, both actual
realtime replicas' password-only authentication and async subscriptions, strict
TLS and reviewer login pass. Clearly synthetic uploads pass real scanning/worker
processing, authenticated approval and the signed callback updating the backend.
The read-only Host API/CLI proof remains H51. Evidence in its directory is
`bootstrap-workload.log`, `deploy-fresh.log`, `verify-fresh-app.log`,
`identity-full-proof.log`, `identity-full-result.json` and
`identity-complete-baseline.json`, with exact normal-operation receipts.
Owned setup Script cleanup completes; application state and synthetic QA data
remain for continued testing. No new operational fault, reboot, reset or pressure
test ran on the replacement. This qualifies restored test hosting, not a rerun
of H46–H49 or complete production readiness.

H53 records the upgrade-only UP-03 run on H51/H52's unchanged
`0.0.0-qa.owner20260914.1`, production source `939fd039f`, built from `2676fbae6`.
The running Controller digest is
`sha256:8a586e85d911d25afc5e579eb5445b11cada8ce5362244758a72d3c0b77e915c`;
the full release and Agent identities are captured in `upgrade-edge-baseline.json`.
The test definition is `upgrade-refusals.py` in
`.tmp/qa-owner-reset-20260914-hK6Yqeii/`; exact timestamped API/CLI receipts and
before/after snapshots are retained there. The matrix defines these variants
before execution. Selected Controller and Agent requests pass with HTTP 422
`validation.failed` and HTTP 409 `state.conflict` respectively. A canonical absent
release digest fails with HTTP 500 `internal`, not an input/resource refusal.
No request creates a Task or changes update history, any container identity,
image/start time, serving Release or the checked authenticated profile.

`upgrade-refusals-result.json` preserves the failed outcome; its concurrent
`upgrade-refusals-traffic-result.json` passes 45 seconds, 45 authenticated HTTP
200 responses, zero request failures/disconnections, nine pongs on the same
WebSocket and maximum HTTP latency 0.198 seconds. No test resource was created,
no cleanup is required and no product source changed. The host remains healthy.
Activation faults were not started after this assertion failure. The
[missing-candidate finding](issues/runtime-qualification.md#missing-upgrade-candidate-refusal)
requires a fix/defer decision; these results do not complete UP-03 or upgrade QA.

H54 records the owner-approved missing-release classification repair. The actual
release-store regression fails only its absent-release variant before the fix in
`.tmp/upgrade-missing-release-red-trusted.log`; its initial sandbox run fails
filesystem trust setup and is retained separately. The fix classifies absence
only at the requested digest leaf. Missing parent, manifest/binary and closed
store still return Internal; existing unsafe-path/digest checks remain unchanged.
Both complete release-store and native-coordinator race suites pass in
`.tmp/upgrade-missing-release-tests.log`. Focused vet and pinned Staticcheck pass
in `.tmp/upgrade-missing-release-{vet,staticcheck}.log`; changed-file pinned
formatting and diff checks pass. This is local proof, not closure of H53's live
failure. Next is normal native deployment and the original request rerun, then
the remaining upgrade-only cases. No broader implementation is included.

H55 closes H53's missing-candidate defect on the fresh QA host and qualifies the
selected UP-04/05/11 preparation variants. The owner-approved repair is signed
on `main` as `64b2056dd`; normal staging builds `0.0.0-qa.owner20260914.2`.
The architecture check retains the same 122 finding identities with no additions;
`.tmp/upgrade-missing-release-architecture.json` is not a full-CI pass.

Before activation, an observed 160-second sleep-only Script remains running while
native Task `task_01M2GF3JAC0VKNZE4QW4ER5W0W` fails its 120-second drain
(122.341 seconds measured from submission). A second 60-second Script remains
running while native Task `task_01M2GF8PBWJ6N9MHNAFYPM5XP2` is aborted before
activation (1.976 seconds). Both Scripts later complete, each with exactly one
physical start in live Docker events. Exact protected-key replay returns each
original native Task both while running and after settlement. Following work
proves dispatch resumed; both owned Scripts and their runners are removed through
normal operations. All runtime identities/start times and checked application
data remain unchanged. These cases ran on `.1`, not the subsequently activated fix.

Normal update Task `task_01M2GFBCNCYT1KBXWCP9YP91PC` then completes `.2`:
release `sha256:6baaecc017178b5fb65e11bb6eac05c8e6e244d32b66c4588f617fadec22505a`,
Controller `sha256:87f18553ad7096e696d6099e110693cb797859d33ef08eae537504cb97bf8011`,
Agent `localhost:5000/groundplane-agent@sha256:cacb870d06b56087882a06ced5ad738ac5d94237939b875a19cd2ea194341a29`.
All non-Agent runtime identities/start times, including etcd, and serving Releases
and checked profile survive. The original missing-digest request now returns
422 `validation.failed`; selected Controller/Agent refusals also pass. No refusal
adds a Task or changes runtime/history. The shared 360-second traffic run passes
360 authenticated HTTP 200 responses, zero failures/disconnections, 65 pongs on
one WebSocket and maximum HTTP latency 0.201 seconds.

Evidence in `.tmp/qa-owner-reset-20260914-hK6Yqeii/` is
`upgrade-preparation-result.json`, both per-variant event streams and exact Task
receipts, `upgrade-fix-stage.log`, `upgrade-fix-result.json`,
`upgrade-refusals-fixed-result.json` and `upgrade-preparation-traffic-result.json`.
The original failed evidence is retained unchanged. This closes the reproduced
error-classification defect, not every UP-03 variant, all concurrency races or
Console lost-response qualification. No application deploy, reset or manual
installation repair was used.

H56 passes the UP-08 coordinated unready-Agent variant and the selected
UP-06/10/11 trial guards on H55's healthy `.2` baseline. A distinct immutable
release combines the retained compatible `.1` Controller with an owned
sleep-only Agent image; it is a deliberate QA candidate, not shipped software.
Native Task `task_01M2GFFDS0057F91K69Y8Q8WCC` reaches the real `starting`
journal phase with that exact Agent image. Ordinary Script edit, Script Run and
committed Abort each return 409 `resource.in_use`. Reads and exact-key update
replay remain available, with unchanged Script content/order.

GP automatically restores H55's exact Controller/Agent digests. The same Task
remains failed with phase `recovered`; no manual installation, journal change or
application redeploy occurs. All non-Agent runtime identities/images/start times,
serving Releases and the authenticated profile survive. A bodyless Agent update
refuses the already-qualified image, proving the failed trial did not replace the
selection. The previously refused owned Script edit succeeds after recovery;
normal Script removal completes and healthy staging selection is restored.
The 240-second concurrent window passes 240 authenticated HTTP 200 responses,
zero failures/disconnections, 43 pongs on one WebSocket and maximum latency
0.232 seconds. `unready-agent-result.json`, its exact operation/fixture receipts
and `unready-agent-traffic-result.json` are in H55's evidence directory.
The unused candidate fixture remains retained as evidence, not selected.
Scheduled-writer and unknown-publication variants are not inferred from this run.

H57 passes UP-09's `starting` interruption variant. Following H56's complete
recovery and traffic pass, a new native Task
`task_01M2GFRJ3HCB8940PR7S6MV1EJ` selects the same deliberate unready fixture.
The probe opens a pidfd for systemd's current MainPID, hashes that process's
actual executable and rereads its exact Task's journal. Only matching candidate
digest and `starting` phase authorize SIGKILL. GP restores H55's Controller and
Agent automatically; the original Task terminalizes failed/recovered. No manual
restart, journal rewrite or application operation is used. Exact-key replay,
qualified Agent selection, checked profile, serving Releases, all non-Agent
runtime identities/start times and owned Script cleanup pass. Healthy staging
selection is restored. The full 180-second traffic window passes 180 authenticated
HTTP 200 responses, zero failures/disconnections, 33 pongs on one WebSocket and
maximum HTTP latency 0.203 seconds. Evidence is `interrupted-starting-fault.json`,
`interrupted-starting-result.json` and `interrupted-starting-traffic-result.json`
in H55's directory. Other interruption phases and lost-commit boundaries remain
separate; this does not qualify host reboot.

H58 completes UP-03's remaining malformed, incompatible storage/channel and
corrupt binary variants against H55's repaired `.2`. Fixtures use the retained
different Controller digest so an unchanged-executable rejection cannot mask
missing compatibility validation. Only a newly staged owned corrupt fixture's
bytes are changed before any Task refers to it; prior releases and Task/journal
history are untouched. All four requests return 422 `validation.failed`; Task
history, runtime identities/images/start times and checked data remain unchanged.
Healthy staging selection is restored; unselected fixtures remain as evidence.

The first associated traffic probe fails on its first HTTP request with 401,
before the refusal batch starts; its stale test session and successful normal
refresh are retained in the logs. It is not a passing continuity window or a
runtime outage. The confirmation reuses the same four fixtures without restaging,
refreshes/verifies the session before starting traffic, and waits for the first
successful authenticated request and WebSocket pong before submitting refusals.
It passes 45 HTTP 200 responses, zero failures/disconnections, nine pongs on the
same WebSocket and maximum latency 0.202 seconds. Evidence is
`upgrade-candidate-refusals-result.json`, `candidate-refusals-traffic.stderr`,
`confirm-candidate-refusals.log`, `candidate-refusals-confirmed-result.json` and
its traffic result in H55's directory. The host remains healthy with zero active
Agent work. H55–H58 do not qualify successful standalone Agent replacement,
re-enrollment, Console lost-response behavior or every phase/race in the matrix.

H59 passes UP-02's standalone replacement on the separately authorized clean
disposable host. Evidence is `.tmp/qa-standalone-agent-20260914-PsuapVs5/`.
GP-managed routing fronts a Python HTTP/WebSocket canary with a client-written,
fsynced sentinel on a managed Volume. Revision-fenced Controller config changes
and normal restarts establish the mismatch without changing stored Agent history.
The preparatory downgrade Task `task_01M2GHSB4B63N4V5BTYEY3BB28` and measured
upgrade Task `task_01M2GHV1SPWGRNSYHWMCJFDXBT` each complete with a new container,
exact desired image, one generation increment and healthy authenticated Ready.
Active and terminal protected-key replay return the original Tasks, with no
second replacement. Non-Agent containers, including etcd, retain exact ids,
images and start times; Service state and sentinel bytes survive.

The initial probe stops after the successful downgrade because it compares
refreshed `observed_at`/`expires_at` fields. The corrected comparison excludes only
those timestamps and finishes proof against the original Task; no downgrade is
repeated. `standalone.log` retains that failed assertion, and
`resume-standalone.log` records the continuation. The two held-connection windows
pass eight HTTP requests/two pongs and ten requests/three pongs, zero errors or
disconnections, maximum latencies 0.0032 and 0.0051 seconds. These short upgrade
windows are not a new sustained-load pass. Exact receipts are
`preparatory-downgrade-result.json`, `standalone-upgrade-result.json`, and both
traffic results. Native-selection re-enrollment remains pending; the hosting QA
machine is untouched. No product code changed.

H60 completes UP-02's qualified-selection enrollment variant in H59's disposable
installation and evidence directory. Controller version
`0.0.0-qa.standalone20260914.3` is built from clean `8fdb5dbfe`, with unchanged
product code and reused Console assets and Agent image. Normal native update
Task `task_01M2GJ0TNVA2ENSB2VWFAPKW65` qualifies release
`sha256:a894b0a5133b3c55b2fefa8a81a900f752b536a4e176dca70c263628f12ae62f`.
Bootstrap configuration deliberately still names the older `.1` Agent digest.
Normal CLI removal Task `task_01M2GJ0ZC3ZCYZKZA8ZTEMYV08` deletes the old Agent;
join Task `task_01M2GJ1104CFWSPXPTJC5N7MXP` creates a new Agent at the qualified
`.2` image, proving selection precedence rather than equal fixture inputs.
The new Agent reaches authenticated Ready with zero in-flight work.

Application and etcd container ids/images/start times, Service state and exact
client-written sentinel bytes remain unchanged through native update, removal,
join and Controller restarts. Eighteen HTTP 200 responses and four pongs on the
held connections pass over 17 seconds, with zero errors/disconnections and maximum
latency 0.0052 seconds. Bootstrap configuration is restored to `.2` through the
normal revision-fenced API and restart; the qualified selection remains intact.
Evidence is `qualified-enrollment.log`, `qualified-enrollment-result.json`,
`qualified-enrollment-traffic-result.json` and `qualified-enrollment-final.json`.
The disposable canary, Volume and image caches remain; no operation touched the
existing hosting QA installation. This closes UP-02, not the other partial
upgrade cases or full production qualification.

H61 FAILS REL-02's owner-selected idle-host reboot variant on H60's separate
disposable canary installation. Before the reboot, Controller, Agent and etcd
are healthy; the Agent has zero in-flight work and the complete Task list has no
pending/running Task. Controller and Docker boot services are enabled. The
existing routed canary returns its exact previously client-written Volume
sentinel and completes a real WebSocket handshake/ping/pong. Source, selected
release, boot id, Service/runtime identities and Task history are retained.

One `systemctl reboot` returns exit zero. The host does not return with a changed
boot id within the 240-second bound; SSH connection attempts time out. No second
reboot, reset, manual start or VM-manager change is attempted. Therefore GP startup,
release selection, HTTP/WebSocket recovery and persisted data after reboot remain
unverified. The observed failure is automatic host recovery; the current cause
is not established and must not be inferred from H50's earlier VMM failure.
The existing application host is untouched. Evidence is
`.tmp/qa-reboot-20260914-KpdkEBhU/`: `before-reboot.json`, `tasks-before.json`,
`boot-before.json`, `canary-before.json`, `reboot-command.json`, `reboot.log`,
`reboot-result.json` and retained SSH receipts. The disposable VM must be brought
back without resetting its disk before post-boot qualification can continue.
Manual startup, if approved/performed, would not turn this automatic-reboot
failure into a pass. No product code changed or data was deliberately removed.

H62 resumes H61 read-only after the owner restarts the existing disposable VM.
A changed boot id is verified. Controller and Docker systemd services are enabled
and running with zero service restarts recorded; Controller, Agent and etcd are
healthy. The qualified Controller digest and native update record, Agent identity
and complete Task history match the pre-reboot capture. The original sentinel
bytes survive in the managed Volume's verified bind source. The first read used
Docker's unmounted Volume mountpoint and failed; its error is retained and is not
evidence of data loss. No Volume was mounted or container started for the read.

Application recovery FAILS: the canary workload and its generated serving proxy
remain exited, and the still-running Environment router returns HTTP 502.
Both stopped containers have Docker restart policy `no`; neither reports OOM.
The primary's upgrade canary Blueprint omitted the native Compose `restart`
field. The proxy renderer copies `authored.Restart` in
`internal/controller/service_proxy.go`; the product contract preserves native
Compose restart semantics. This fixture therefore cannot qualify automatic
application startup. It does not reproduce GP ignoring an authored restart
policy, and earlier uninterrupted-upgrade results remain valid for their scope.
No product code, Blueprint, service lifecycle or host configuration was changed.
Correcting the fixture through normal Blueprint/Deploy and repeating the boot
check is the proposed next scoped step. H61's automatic-host return failure
remains failed, separate from successful platform startup after owner intervention.

Evidence is in H61's directory: `post-boot.log`, `owner-boot-id.json`,
`owner-startup.json`, `owner-after-reboot.json`, `owner-runtime-detail.json`,
`owner-volume-mapping.json`, `owner-tasks-after.json` and
`owner-post-boot-finding.json`. Workload HTTP/WebSocket recovery is not claimed.
The existing hosting QA installation is untouched.

H63 attempts the approved canary correction, not another reboot. Current exported
Blueprint input is compared with the saved authored input; only native export
normalization is removed from the comparison. The initial raw comparison failure
is retained and causes no mutation. The only authored change is
`services.web.restart: unless-stopped`. Blueprint Apply
`task_01M2GPPZQVXDGBFMZCZ9DKKK88` completes. The following blue-green Deploy
`task_01M2GPQ4Z53K3BC5PMK5JCQPW1` FAILS proxy activation and completes automatic
predecessor recovery. Its Agent diagnostic is
`COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED`.

Before Deploy the existing proxy is stopped. The blue-green planner sets
`EnsureProxy` only when the prior strategy is recreate; proxy switching executes
commands inside the existing proxy. That path cannot switch this stopped proxy.
Recovery starts the retained predecessor: subsequent HTTP 200, WebSocket pong
and exact original sentinel checks pass, with a healthy Agent and zero claims.
Docker inspection proves both restored workload and proxy still have restart
policy `no`. Desired publication and recovered service availability therefore do
not qualify the requested restart-policy change. No new reboot or second Deploy
ran. No product source, immutable history or direct runtime configuration changed.
The proposed scoped continuation is a normal recreate Deploy to replace the
container configuration, followed by independent restart-policy inspection and
the approved reboot trial. This does not close the stopped-proxy blue-green
limitation or permit treating the failed Deploy as success.

Evidence in H61's directory includes `correct-restart.log`,
`correct-restart-normalized.log`, `restart-policy-current-blueprint.json`,
`restart-policy-blueprint.json`, both accepted Task receipts, the failed Task's
terminal record and event stream, `restart-policy-failed-state.json`,
`restart-policy-after-failure.json` and `restart-policy-recovered-canary.json`.
The existing application host remains untouched.

H64 PASSES the owner-approved recreate correction and post-boot GP/application
checks, but FAILS automatic host return. The owner subsequently clarified:
the host was killed, and they restarted it during the observation window.
The initial automatic-recovery interpretation is withdrawn. Recreate Deploy
`task_01M2GQESMWNFRJ59H1YB524DWV` completes normally
using the already accepted restart-policy input. Independent Docker inspection
proves `unless-stopped` on the running singleton workload, serving proxy and
Environment router. Routed HTTP 200, real WebSocket ping/pong and the original
client-written sentinel pass before reboot. Preparation receipts and
`recreate-policy-result.json` are retained in H61's evidence directory. No manual
Docker lifecycle/configuration operation or product code change is used.

The new reboot trial has its own directory,
`.tmp/qa-reboot-policy-20260914-F10roqpX/`. Preflight proves no pending/running
Tasks, enabled boot services, healthy platform and functioning canary. One
`systemctl reboot` returns exit zero. After the owner's host restart, polling
observes a changed boot id and verified healthy hosting after 220.84 seconds.
That interval includes external intervention, not automatic host recovery.
Controller, Agent and etcd are healthy; exact container ids/images/running states,
Service state, qualified Controller digest/update record, Agent identity and
complete Task history match the new pre-reboot baseline. Existing sentinel bytes
match, HTTP returns 200, and a new WebSocket completes handshake/ping/pong.
Post-boot inspection retains all three restart policies. No agent-issued service
start, second reboot, reset or VM-manager change occurs; the owner's reported
host intervention remains essential and is not excluded by those observations.

Evidence includes `before-reboot.json`, `tasks-before.json`, `boot-before.json`,
`canary-before.json`, `reboot-command.json`, `reboot.log`, `after-reboot.json`,
`canary-after.json`, `reboot-result.json`, `post-reboot-policies.json` and
`post-reboot-startup.json` and `owner-intervention-correction.json`. The original
probe result is retained but superseded for its automatic-recovery claim. Guest
polling cannot distinguish autonomous return from an external manual start.
The 220.84 seconds measures submission to completed verification including owner
intervention, not an automatic-recovery or exact outage duration. Reboot disconnects
are expected; this is not uninterrupted service or single-host high availability.
This pass covers the idle recreate/singleton canary with serving proxy, not an
unfinished Task, blue-green slot boot or the complete production application.
H61's failed return and H63's stopped-proxy limitation remain separate. The
existing application host is untouched, and the disposable fixture remains intact.

H65 completes the owner-selected three proxy repairs' bounded verification on
2026-09-14, not full production qualification. Clean source `9e0732ed4` contains
the repairs in `263edafc7` and `51b79b0da`, installed as
`0.0.0-qa.proxyboot20260914.2`. Controller bytes are
`sha256:423d6c69ed7db4891c16cdf57564dd61044e4bf11483f76fc45e1fa34817d647`;
Agent image is
`localhost:5000/groundplane-agent@sha256:b92bd4dc01fe493152df024b58d483adac160fdd9f2e04692a186d9b2b9bf196`.
Private target, timestamps, setup Tasks and fixture identities are retained in
`.tmp/qa-proxy-final-20260914-2MGvGqY2/`.

The owner-approved reset removes 21 managed containers, eight networks and eight
disposable Volumes, preserving registry/images. Installation/history/logs are
archived in byte-verified ignored `incident-installation.tar`; only its verified
remote copy is removed. Normal fresh bootstrap, six Attach stages, configuration
Apply and eight workload Deploys complete. All 11 Services and independent
authenticated profile, routed realtime and strict TLS portal checks pass before
fault injection. The other QA host is untouched.

SVC-06's stopped-proxy variant PASSES: stop the exact owned API proxy, require
API/CLI degraded status with one healthy workload replica, then ordinary Deploy
starts that same container and completes with working application checks. The
already-running variant preserves exact proxy id/start time through another
Deploy. Forty-five authenticated HTTP requests and nine pongs on the same
WebSocket pass over 45 seconds with no errors, maximum HTTP latency 0.299 seconds.
The socket belongs to a separate unchanged Service; this is not proof of a held
WebSocket through the selected proxy's reload. OBS-05's stopped/return-to-healthy
API/CLI variant PASSES; wrong-route API/CLI proof from the preceding incident is
reused because that code is unchanged. Missing-proxy and Console decoding have
local regressions, not new live deletion/browser trials.

REL-02's idle full-stack blue-green operator-managed stop/start variant PASSES.
Before stopping, no pending/running Task exists; live/restart routing agrees with
the selected Release and differs from initial startup configuration. After the
same VM starts, a changed boot id, all container ids/images/running states,
Agent id, complete Task history and the existing profile are verified. Live and
restart proxy configuration exactly match their pre-boot selection. All 11
Services, authenticated HTTP, WebSocket authorization/subscription and strict TLS
portal checks pass without any container-level intervention. Reboot downtime is
expected; automatic VM-manager return, in-flight recovery and single-host HA are
not qualified.

Exact receipts include `proxy-stopped-proxy-result.json`,
`proxy-running-proxy-result.json`, `proxy-running-traffic-result.json`,
`proxy-before-boot-routing.json`, `proxy-after-boot-routing.json`,
`proxy-boot-result.json`, `vm-stop.log`, `vm-start.log` and `result.md`.
The earlier unsupported-flag trial and subsequent recovery identity rejection
remain failed evidence in `.tmp/qa-fullstack-boot-20260914-tUH4XNUJ/`.
Reset did not repair or qualify that recovery path. The real shipped-tooling
regression fails on `start --wait` and passes with supported `start` in
`.tmp/proxy-start-shipped-{red,green}.log`; the existing actual Caddy regression
also proves switch and compensated configuration survive container restart.
The fresh host is healthy and idle. No further case, repair, cleanup or broader
QA is selected; work stops here.

H66 records the scoped SVC-09 recovery-identity implementation on 2026-09-15,
based on `49f7433ef` plus this repair. Offline incident comparison identifies the
failed candidate as absent from the captured predecessor artifact; the old
workload and proxy labels match. Recovery incorrectly classified its own candidate
as foreign, then stopped before compensation. Existing executor doubles supplied
already-classified observations and did not exercise that Docker classification.

The integrated Agent/Moby-observer regression traverses a sealed recovery pair:
healthy predecessor alone, then predecessor plus a created unhealthy candidate,
then candidate ownership drift. Restoring predecessor-only observation reproduces
the exact `release restoration observation identity diverges` error at candidate
creation; the correction passes and still rejects the changed ownership. Native
recovery's predecessor selection also passes with the candidate present. Separate
regressions cover blue-green probe/compensation descriptor binding, immutable
copies, ordinary observation strictness, wrong plan/Release/image, unknown workload,
proxy impersonation, duplicate identity and absent/unhealthy predecessor.

Affected Agent, execution-plan, Moby-observer and app race suites, vet and pinned
Staticcheck pass. The first sandboxed Agent run could not create its Unix sockets;
the socket-enabled local run passes. Formatting and diff checks pass. Architecture
reports the same 122 pre-existing findings, none on changed files; full CI is not
claimed. Evidence is
`.tmp/recovery-identity-Tncp08Qx/`: `integration-red.log`, `integration-green.log`,
`shared-recovery.log`, `delivery-race.log`, `delivery-vet.log` and
`delivery-staticcheck.log`. Tests use generic fixtures and no live host. This is
local implementation proof, not a passing full failed-Deploy operator journey,
deployment, Controller terminal acknowledgement or production qualification.
The original failed Task remains archived and unchanged. No live cleanup ran.

H67 PASSES the owner-selected live validation of H66 on 2026-09-19. Source
`30245a441` is built and staged as `0.0.0-qa.recovery20260919.1`, then activated
through normal Controller update Task `task_01M2X306M4Z7E1B0KR9JM2RHF7`.
Controller digest is
`sha256:c50f8dc65b308e0097c26f4c30dd605c746a1b1d6e67bfbc56de60a6717ab420`;
Agent image is
`localhost:5000/groundplane-agent@sha256:b22fb3d3ca2cb888700c7dd0dfec7815f1eaad7787f4fb1ceb298f1d88d76951`.
The native update preserves every non-Agent container identity, image and start
time, including etcd, all serving Releases and the existing authenticated profile.
Forty-four requests and eight pongs on the same WebSocket pass over 44 seconds,
zero errors/disconnections and maximum HTTP latency 0.199 seconds.

SVC-09 injects exactly one failure through a temporary post-deploy Script:
`sleep 20; exit 23`. The new candidate is observed running before the hook exits;
the hook runs after candidate creation and before readiness/promotion. Agent logs
confirm exit 23, not an unrelated failure. Task
`task_01M2X31S1V8ZNXEXH1SY1ND4ZW` automatically closes as failed in 26.463 seconds
including that deliberate delay. Its failure remains recorded and the candidate
does not become the serving Release. The previous serving Release,
all pre-existing running container identities/images/start times and the checked
application profile survive. All 11 Services are healthy and the Agent releases
its claim. No manual container start, configuration repair or reset is needed.

The fault Script is removed through its normal operation, preserving all original
hooks exactly. Subsequent ordinary Deploy `task_01M2X32Q3GK56Z86CJ240MNX3Z`
completes and removes the replaced failed candidate; unrelated serving Releases
and the profile remain unchanged. API and CLI agree on the original failed Task
and subsequent completed Task. Across recovery and that follow-up, 96 authenticated
requests and 18 pongs on the same WebSocket pass over 96 seconds, zero errors or
disconnects, maximum HTTP latency 0.287 seconds. The socket belongs to the unchanged
separate WebSocket Service, not the selected API proxy.

Evidence is `.tmp/qa-recovery-live-20260919-4oXbEvK0/`: `stage.log`,
`upgrade-result.json`, `failure.log`, `created-candidate.json`,
`failed-task-terminal.json`, `agent-final.stderr`, `final-result.json`,
`upgrade-traffic-result.json`, `recovery-traffic-result.json` and the private
API/CLI receipts. Owned traffic children exit and their two control files and
unique empty remote directory are removed. Local evidence and the original
archived incident are retained. The selected journey is complete; no production,
broader QA, Backup/Restore or new Console journey ran. This proves the exercised
post-creation hook-failure path, not every interruption/timeout variant or full
production readiness. H66's foreign-identity/health negatives remain local proof.

The repair evidence directory for H1–H5 is
`.tmp/qa-recovery-repair-20260913-63vxXPTf/`. Older referenced run directories and
their exact build history remain in the operational checkpoint and existing
acceptance records. Missing or unreviewed evidence remains explicit in the matrix.

## Evidence index

These are dated working records, not current host health, executable next actions
or permission to repeat a mutation. Read the feature document for requirements
and [head.md](head.md) for current authority and pauses. Keep related future proof
in the same evidence document rather than creating a report for each commit.

| Evidence | Scope |
| --- | --- |
| [Repository consolidation](acceptance/repository-consolidation.md) | Landed source, baseline failures, removed worktrees and recovery archive |
| [Safe updates](acceptance/safe-updates.md) | Native update implementation, QA candidates, continuity failures and recovery limits |
| [Router and visibility](acceptance/router-and-visibility.md) | Full Caddyfile, native validation, retained reload and Route summary |
| [Script execution](acceptance/script-execution.md) | Ordered hooks, explicit contexts, immutable sources and remaining qualification |
| [Storage integrity incident](acceptance/storage-integrity-incident.md) | Disk/AOF failures, bounded cleanup and the unresolved integrity pause |

No record establishes full CI, Gate A, Gate B or production readiness beyond the
scope it explicitly proves. Failed and unavailable checks remain visible.

These tracked working records retain existing private verification context.
They are not approved public artifacts; sanitize that context before any public
publication. Consolidation does not authorize publishing private topology.

## Release packaging tooling

H68 records local PKG-01/02 support on 2026-09-19, built from `d0a03f01c` plus
the release-packaging changes in this commit. Ten tests in
`scripts/test_release_bundle.py` pass: exact Controller/descriptor and pinned-image
roundtrip, wrong-version/architecture refusal before extraction, traversal/link/
duplicate/missing-member refusal, altered bytes, deterministic archive headers,
no overwrite, fresh/native routing, stage/config refusal, partial-layout and
capacity refusal without setup/pulls, and help/invalid-input handling without
root access. Existing bootstrap/staging/update-replay tests are unchanged.
The complete 83-test deployment suite passes in
`.tmp/release-packaging-checks.log` with the real filesystem ownership view;
the sandbox-only attempt hit existing ownership-fixture errors.

Native linux/amd64 Controller and CLI builds pass with Go 1.26.7,
Node 24.19.0/npm 11.17.0, CGO disabled and stripped binaries. The Controller
contains production Console assets and matches its generated release descriptor;
its test version is `0.0.0-qa.packaging20260919.1`. The Console lockfile audit
reports zero known vulnerabilities in `.tmp/release-packaging-npm-audit.json`.
No installer ran on a host, no registry push or GitHub Release was performed,
and no new application/update continuity or arm64 qualification is claimed.
Archive/header and fake-process checks are not a fresh-host installation pass.

PKG-03 local image checks pass in `.tmp/agent-release-image-proof/result.log`:
linux/amd64 scratch image `groundplane-agent:release-probe2` is 147,450,780 bytes,
versus 275,992,667 bytes for `groundplane-agent:size-baseline` (46.6% smaller,
Docker's uncompressed image-size measure). Entrypoint, empty command and root
identity are unchanged. Docker 29.1.3, Compose 2.40.3 and a real Compose document
parse pass; the closed helper dispatch reaches its expected empty-input refusal.
The image retains the CA bundle and explicit PATH/HOME/temp directories without
a shell, package manager, Buildx or Compose 5. Three fake-tool tests in
`scripts/test_release_agent.py` pass for no implicit push, realistic push-output
parsing and exact newly reported RepoDigest confirmation. This is not a real
registry publication, valid-frame workload application or complete helper-mode
qualification. An attempted broader unchanged Go package run encountered the
sandbox's Unix-socket permission limit; it is not reported as passing.

## Private data

Groundplane is exercised against a private representative workload, but its
identities, domains, credentials, source paths, network topology, service
inventory, deployment driver, and captured host state are not public product
artifacts. Detailed operator-journey evidence remains in the repository-local
ignored workspace.

The public repository records reproducible contracts, generic fixtures, and CI
proofs. Public documentation must use generic names and documentation-only
addresses and must never embed a private deployment topology.
