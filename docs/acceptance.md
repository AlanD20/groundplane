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

## Private data

Groundplane is exercised against a private representative workload, but its
identities, domains, credentials, source paths, network topology, service
inventory, deployment driver, and captured host state are not public product
artifacts. Detailed operator-journey evidence remains in the repository-local
ignored workspace.

The public repository records reproducible contracts, generic fixtures, and CI
proofs. Public documentation must use generic names and documentation-only
addresses and must never embed a private deployment topology.
