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
