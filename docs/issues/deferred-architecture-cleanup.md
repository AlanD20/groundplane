# Deferred architecture cleanup snapshot

- Status: cleanup paused; exact existing findings deferred for release 0.0.1 on 2026-09-19
- Owner: primary agent; further cleanup requires fresh owner authorization
- Severity: low for architecture-only structure; reclassify any demonstrated runtime or safety defect
- MVP-required: no under the current deployment-first authority
- Snapshot: `787ab7a95` (`f6c52530` tree), before the six landed correction commits
- Historical command: `make architecture-check`
- Acceptance: after successful hosting and fresh authorization, close the applicable rows without
  raising file allowances or frozen totals, then pass the architecture gate from a clean worktree

## Authority and safety boundary

The owner paused cleanup and approved the
[0.0.1 release deferral](../delivery.md#approved-architecture-debt-for-001).
The current exact snapshot is `architecture-deferred.json`: 105 findings, including
55 file-size findings, two frozen-total findings and 48 test-only imports.
Already-tested bounded extractions are retained; no further extraction is authorized.
The strict checker and its baseline remain unchanged. New or changed findings
still block CI, and non-structural release gates remain mandatory.
The historical snapshots below explain earlier debt, not current counts.

Unrelated refactoring and architecture cleanup remain deferred until the user
authorizes that work. The architecture gate may remain red solely for this deferred
cleanup. This is not permission to ignore compile/build failures, security defects, data-safety
risks, or runtime-correctness defects; those remain deployment blockers.

The 108 count below is a frozen historical snapshot, superseded by the current
release snapshot above. Baseline allowances must not be weakened to hide findings.

## Snapshot accounting

### Custom backing hooks exception

On 2026-09-20 the owner approved a temporary exception limited to integrating
Custom backing creation, hooks and their Task input/checkpoint lifecycle, then
running QA. No general cleanup or checker change was authorized.

The exact snapshot updates cover `internal/agent/worker.go`, app Attach,
Controller composition and Service mutation files, execution-plan dispatch,
Agent-channel dispatch, Controller plan composition, etcd Attach publication,
backing creation, Task lifecycle and pruning. The app and etcd frozen-total
entries are updated for this feature's code only. The now-under-limit app
backing creation entry is removed; its replacement Attach mutation finding keeps
the snapshot at 105 entries. All test-import allowances remain unchanged.

Unrelated pending hierarchy-admission changes are excluded from this exception
and the deployment source. The exact etcd total is therefore checked against
the selected integration snapshot, not a dirty worktree containing those edits.
`architecture-baseline.json` and both checker implementations remain unchanged.
Build, behavior, secret safety and operator-journey checks remain required.
Structural compliance remains deferred, not qualified.

### Historical accounting

| Group | Count | Classification |
| --- | ---: | --- |
| Imports | 25 | All test-only; absent on pre-consolidation `main` |
| File size | 75 | 67 pre-main, 2 feature growth, 4 formatting-only, 2 stale baseline |
| Frozen totals | 2 | Both pre-main |
| Stale legacy entries | 6 | Baseline hygiene |
| Total | 108 | Historical checker result |

## Work completed after the snapshot

- The integrated QA direct-retention regression adds one Controller test-only
  import in `volume_retained_publication_external_test.go`. It belongs to the
  same app-owned integration-fixture move; the small publisher correction is
  below the unchanged 600-line ceiling.
- The September 9 bounded Blueprint correction adds two etcd external journey
  files with three test-only imports across Controller/blueprintrelease/Agent
  (`blueprint_publication_size_external_test.go` and
  `blueprint_publication_assignment_external_test.go`). Move these with the
  existing app-owned integration-fixture work. New production files are bounded;
  the touched pre-existing oversized `blueprintrelease/service.go` remains
  635 lines, unchanged from its current-main version. No allowance changed.
- The September 9 Entry integration reran the gate; it remains red. Its two
  new etcd external journey files add three test-only layer-import findings
  (Agent/Controller in `entry_removal_agent_external_test.go`, Controller in
  `entry_removal_publication_external_test.go`). Include them in the same
  app-owned integration-fixture migration below. New production files remain
  bounded; the changed pre-existing oversized files did not grow relative to
  their current-main versions. The gate also retains the earlier file-size,
  frozen-total, test-placement, and stale-baseline findings. No allowance or
  production import rule was weakened, and no full gate pass is claimed.
- `286fafb5` replaced one registered-Component reflection import with typed equality.
- `ea155830` removed three Controller concrete-Component test imports.
- `91a429da` moved two Agent adapter test imports into app-owned composition.
- These six import/reflection rows are source-fixed; 19 imports across nine etcd test files were
  still present at the last source inspection. No gate rerun converts that observation into a
  current checker count.
- `4c70ba5b` extracted Compose invocation emission; `composehelper.go` moved from the snapshot's
  785 lines to 638, below its unchanged 644-line allowance.
- `fd4435e3` landed the MVCC Blueprint test Store prerequisite. It did not perform the remaining
  public-fixture or nine cross-layer etcd test migrations.

## Service observation channel checks

The 2026-09-12 channel slice ran `make architecture-check` with Go 1.26.7;
the working-tree gate reports 126 findings. Evidence is in
`.tmp/service-channel-1qVALR/architecture.log`. No baseline changed. The Registry
shrinks from 763 to 749 lines but remains above its old allowance. The new Docker
read composition adds eight direct `internal/app` lines (24,909 to 24,917).
The owner approved this composition-only increase on 2026-09-12. The historical
baseline remains 16,804; its exact-equality check still reports drift. This
approval does not change the baseline or waive other architecture requirements.

The Script unused-value and Agent-channel nil-context findings recorded in
`.tmp/service-channel-1qVALR/staticcheck.log` were corrected on 2026-09-12.
Late subscription cancellation uses the existing one-second cleanup context;
the caller still returns promptly and cancellation retains ownership until
delivery. Scoped Staticcheck and affected race tests passed in
`.tmp/ci-cleanup-7arYNpkD/`. See [verification debt](runtime-qualification.md#verification-debt)
for the remaining fixture and broad-gate limits. No architecture baseline changed.

## Deferred work groups

### Service observation public integration

The 2026-09-12 public/API/CLI/Console integration reran the architecture gate:
125 existing findings remain in `.tmp/service-source-ak14iK/architecture-final.log`.
No baseline changed. The new Backing-page size finding was corrected through a
feature-owned Service section. Service read behavior moved out of `internal/app`,
reducing its direct production total from 24,917 to 24,799 lines; its Controller
composition file shrank from 1,359 to 1,353. The root Console store and Environment
page also shrank. These reductions do not clear the older allowances or make the
full gate green. Existing owners and acceptance conditions above remain unchanged.

### Previously deferred groups

- Public test fixtures and cross-layer placement: migrate the nine etcd test files into app-owned
  integration tests using public fixtures. Supplementary plan:
  `.tmp/mvp-floor-correction-20260908/test-seams/fixture-closure.md`.
- Service extraction: the unfinished opaque-receipt/typed-Service proposal was
  identified by historical branch `refactor/mvp-service-mutations-20260908` and
  worktree `.tmp/mvp-service-mutations-20260908`. These are not active-worktree
  instructions. Obtain and inspect the preserved local recovery archive before
  any authorized reuse; if unavailable, do not assume recoverability. Do not transplant
  the proposal as though it were landed implementation.
- Environment Blueprint extraction: a proposed extraction spanning seven production files and
  roughly 3,927 lines depends on EntryGeneration and AttachFact prerequisites. It is a deferred
  proposal, not approved implementation. Supplementary report:
  `.tmp/mvp-floor-correction-20260908/app-scope/environment-blueprint-closure.md`.
- Backup Task terminal seam: deferred proposal only. Supplementary report:
  `.tmp/mvp-floor-correction-20260908/etcd-backup-task-terminal-seam.md`.
- Broader file/frozen-total cleanup: group cohesive modules; never raise allowances as the fix.

## Runtime issues are routed separately

Architecture deferral does not waive a demonstrable hosting defect. In particular, the Attach
network-only choice and nil-Release image authority may affect emitted runtime plans and must be
treated as deployment/runtime work if the compile, build, or host journey demonstrates impact.
Supplementary reports:

- `.tmp/mvp-floor-correction-20260908/no-pull/attach-network-only-decision.md`
- `.tmp/mvp-floor-correction-20260908/no-pull/attach-authority-closure.md`
- `.tmp/mvp-floor-correction-20260908/no-pull/command-emission-closure.md`

## Re-entry acceptance

After the user authorizes cleanup:

1. Re-run the architecture gate and reconcile this historical snapshot with current findings.
2. Prioritize by cohesive ownership, not raw line-count movement.
3. Preserve or lower every `architecture-baseline.json` allowance and frozen total.
4. Run focused tests for each extraction and the architecture gate from a clean worktree.
5. Update this issue with landed commits and current evidence; do not rewrite the frozen appendix.

## Appendix A: all 25 historical import findings

```tsv
rule	path	import	test_only	main_1cb9a05a	preformat_9af787496	current_787ab7a95	minimal_existing_compliant_seam
layer-import	internal/agent/adapter_runtime_test.go	github.com/AlanD20/groundplane/internal/adapters	yes	no	yes	yes	adapter-runtime owner; internal/app composition registration
layer-import	internal/agent/adapter_runtime_test.go	github.com/AlanD20/groundplane/internal/adapters/valkey9	yes	no	yes	yes	adapter-runtime owner; internal/app composition registration
component-module-import	internal/controller/blueprintrelease/managed_service_plan_test.go	github.com/AlanD20/groundplane-registered-components/caddy	yes	no	yes	yes	controller SDK/image fixture; internal/app concrete-registration integration
component-module-import	internal/controller/blueprintrelease/managed_service_plan_test.go	github.com/AlanD20/groundplane-registered-components/cloudflaretunnel	yes	no	yes	yes	controller SDK/image fixture; internal/app concrete-registration integration
component-module-import	internal/controller/component_default_network_test.go	github.com/AlanD20/groundplane-registered-components/cloudflaretunnel	yes	no	yes	yes	controller SDK/image fixture; internal/app concrete-registration integration
layer-import	internal/infra/etcd/blueprint_executed_artifact_external_test.go	github.com/AlanD20/groundplane/internal/controller	yes	no	yes	yes	internal/app cross-layer integration test
layer-import	internal/infra/etcd/blueprint_mixed_observer_external_test.go	github.com/AlanD20/groundplane/internal/agent	yes	no	yes	yes	internal/app cross-layer integration test
layer-import	internal/infra/etcd/blueprint_mixed_producer_external_test.go	github.com/AlanD20/groundplane/internal/controller	yes	no	yes	yes	internal/app cross-layer integration test
layer-import	internal/infra/etcd/blueprint_mixed_producer_external_test.go	github.com/AlanD20/groundplane/internal/controller/blueprintrelease	yes	no	yes	yes	internal/app cross-layer integration test
layer-import	internal/infra/etcd/blueprint_mixed_producer_external_test.go	github.com/AlanD20/groundplane/internal/controller/taskcontract	yes	no	yes	yes	internal/app cross-layer integration test
layer-import	internal/infra/etcd/blueprint_mixed_runtime_external_test.go	github.com/AlanD20/groundplane/internal/controller	yes	no	yes	yes	internal/app cross-layer integration test
layer-import	internal/infra/etcd/blueprint_mixed_runtime_external_test.go	github.com/AlanD20/groundplane/internal/controller/servicelifecycle	yes	no	yes	yes	internal/app cross-layer integration test
layer-import	internal/infra/etcd/blueprint_mixed_runtime_external_test.go	github.com/AlanD20/groundplane/internal/controller/taskcontract	yes	no	yes	yes	internal/app cross-layer integration test
layer-import	internal/infra/etcd/blueprint_mixed_worker_external_test.go	github.com/AlanD20/groundplane/internal/agent	yes	no	yes	yes	internal/app cross-layer integration test
layer-import	internal/infra/etcd/blueprint_mixed_worker_external_test.go	github.com/AlanD20/groundplane/internal/controller	yes	no	yes	yes	internal/app cross-layer integration test
layer-import	internal/infra/etcd/blueprint_native_predecessor_external_test.go	github.com/AlanD20/groundplane/internal/controller	yes	no	yes	yes	internal/app cross-layer integration test
layer-import	internal/infra/etcd/blueprint_native_predecessor_external_test.go	github.com/AlanD20/groundplane/internal/controller/blueprintrelease	yes	no	yes	yes	internal/app cross-layer integration test
layer-import	internal/infra/etcd/blueprint_native_predecessor_external_test.go	github.com/AlanD20/groundplane/internal/controller/taskcontract	yes	no	yes	yes	internal/app cross-layer integration test
layer-import	internal/infra/etcd/blueprint_retained_producer_external_test.go	github.com/AlanD20/groundplane/internal/controller	yes	no	yes	yes	internal/app cross-layer integration test
layer-import	internal/infra/etcd/blueprint_retained_producer_external_test.go	github.com/AlanD20/groundplane/internal/controller/blueprintrelease	yes	no	yes	yes	internal/app cross-layer integration test
layer-import	internal/infra/etcd/blueprint_retained_producer_external_test.go	github.com/AlanD20/groundplane/internal/controller/taskcontract	yes	no	yes	yes	internal/app cross-layer integration test
layer-import	internal/infra/etcd/blueprint_retained_rollback_external_test.go	github.com/AlanD20/groundplane/internal/controller	yes	no	yes	yes	internal/app cross-layer integration test
layer-import	internal/infra/etcd/blueprint_retained_rollback_external_test.go	github.com/AlanD20/groundplane/internal/controller/blueprintrelease	yes	no	yes	yes	internal/app cross-layer integration test
layer-import	internal/infra/etcd/blueprint_terminal_budget_external_test.go	github.com/AlanD20/groundplane/internal/controller	yes	no	yes	yes	internal/app cross-layer integration test
component-module-import	registered-components/cloudflaretunnel/cloudflaretunnel_test.go	reflect	yes	no	yes	yes	slices.Equal or direct field assertions
```

## Appendix B: all 75 historical file-size findings

```tsv
rule	path	baseline	limit	main_lines	preformat_lines	formatted_lines	group
oversized-file-growth	console/src/features/environment/environment-page.tsx	4005	600	4172	4256	4256	pre-existing-main
missing-oversized-baseline	console/src/lib/environment-lifecycle.ts	none	600	608	608	608	pre-existing-main
oversized-file-growth	console/src/lib/store.tsx	3251	600	3791	3796	3796	pre-existing-main
oversized-file-growth	console/src/lib/types.ts	608	600	635	637	637	pre-existing-main
missing-oversized-baseline	internal/agent/release_execution.go	none	600	559	645	645	feature-growth
missing-oversized-baseline	internal/agent/worker.go	none	600	1425	1332	1332	pre-existing-main
oversized-file-growth	internal/app/attach_mutations.go	1178	600	1266	1317	1317	pre-existing-main
missing-oversized-baseline	internal/app/backing_service_creation.go	none	600	608	692	692	pre-existing-main
oversized-file-growth	internal/app/controller.go	1223	600	1515	1513	1513	pre-existing-main
missing-oversized-baseline	internal/app/entry_bulk_upsert.go	none	600	608	608	663	pre-existing-main
missing-oversized-baseline	internal/app/entry_desired_mutation.go	none	600	924	924	961	pre-existing-main
missing-oversized-baseline	internal/app/entry_edit.go	none	600	605	605	605	pre-existing-main
oversized-file-growth	internal/app/environment_blueprint.go	1333	600	2179	2170	2200	pre-existing-main
missing-oversized-baseline	internal/app/environment_blueprint_authoring.go	none	600	598	602	602	feature-growth
oversized-file-growth	internal/app/service_mutations.go	861	600	1125	1127	1148	pre-existing-main
missing-oversized-baseline	internal/architecturecheck/go_rules.go	none	600	602	602	605	pre-existing-main
oversized-file-growth	internal/common/executionplan/plan.go	1186	600	1608	1548	1548	pre-existing-main
oversized-file-growth	internal/controller/agentchannel/registry.go	602	600	803	803	803	pre-existing-main
oversized-file-growth	internal/controller/agentchannel/server.go	1000	600	1279	1145	1145	pre-existing-main
missing-oversized-baseline	internal/controller/agentchannel/server_test.go	none	1000	1574	1581	1581	pre-existing-main
oversized-file-growth	internal/controller/blueprintparser/namespace.go	904	600	1047	1047	1047	pre-existing-main
missing-oversized-baseline	internal/controller/blueprintrelease/service.go	none	600	577	589	635	formatting-only
missing-oversized-baseline	internal/controller/dnsresolver/platform_component_render_planner.go	none	600	621	621	621	pre-existing-main
missing-oversized-baseline	internal/controller/entry_removal_plan.go	none	600	699	699	699	pre-existing-main
missing-oversized-baseline	internal/controller/entry_routes.go	none	600	704	704	704	pre-existing-main
oversized-file-growth	internal/controller/localagent/localagent.go	967	600	977	977	977	pre-existing-main
oversized-file-growth	internal/controller/localagent/localagent_test.go	1236	1000	1472	1472	1476	pre-existing-main
missing-oversized-baseline	internal/controller/network/route_mutations.go	none	600	656	656	659	pre-existing-main
missing-oversized-baseline	internal/controller/plan.go	none	600	1104	1101	1123	pre-existing-main
missing-oversized-baseline	internal/controller/release_plan.go	none	600	538	571	737	formatting-only
missing-oversized-baseline	internal/controller/script_runner_projection.go	none	600	720	695	709	pre-existing-main
missing-oversized-baseline	internal/core/model.go	none	600	604	608	608	pre-existing-main
oversized-file-growth	internal/infra/docker/composehelper/composehelper.go	644	600	943	785	785	pre-existing-main
missing-oversized-baseline	internal/infra/docker/runner/host.go	none	600	906	906	959	pre-existing-main
missing-oversized-baseline	internal/infra/docker/scriptrunner/runner.go	none	600	831	832	886	pre-existing-main
oversized-file-growth	internal/infra/etcd/attach_repository.go	1521	600	1736	1736	1741	pre-existing-main
oversized-file-growth	internal/infra/etcd/attach_repository_test.go	1203	1000	1423	1485	1485	pre-existing-main
missing-oversized-baseline	internal/infra/etcd/backing_service_creation.go	none	600	604	604	650	pre-existing-main
oversized-file-growth	internal/infra/etcd/backup_artifact_repository.go	2813	600	2912	2912	2912	pre-existing-main
missing-oversized-baseline	internal/infra/etcd/backup_key_rotation.go	none	600	680	680	735	pre-existing-main
oversized-file-growth	internal/infra/etcd/backup_policy_preparation.go	695	600	790	790	790	pre-existing-main
oversized-file-growth	internal/infra/etcd/backup_policy_replacement.go	799	600	815	815	815	pre-existing-main
missing-oversized-baseline	internal/infra/etcd/backup_policy_replacement_test.go	none	1000	1160	1160	1164	pre-existing-main
oversized-file-growth	internal/infra/etcd/backup_runtime_record_test.go	1025	1000	1039	1039	1051	pre-existing-main
oversized-file-growth	internal/infra/etcd/backup_runtime_repository.go	2058	600	2129	2129	2133	pre-existing-main
oversized-file-growth	internal/infra/etcd/backup_runtime_repository_test.go	5337	1000	5374	5374	5379	pre-existing-main
oversized-file-growth	internal/infra/etcd/backup_secret_resolution.go	992	600	997	997	998	pre-existing-main
missing-oversized-baseline	internal/infra/etcd/blueprint_release_publication.go	none	600	787	711	778	pre-existing-main
missing-oversized-baseline	internal/infra/etcd/component_task_lifecycle.go	none	600	790	790	805	pre-existing-main
oversized-file-growth	internal/infra/etcd/connector_deletion_test.go	1713	1000	1737	1737	1737	pre-existing-main
oversized-file-growth	internal/infra/etcd/deletion_tombstone.go	671	600	680	680	680	pre-existing-main
oversized-file-growth	internal/infra/etcd/entry_repository.go	699	600	760	760	765	pre-existing-main
oversized-file-growth	internal/infra/etcd/environment_acknowledgement.go	709	600	736	736	739	pre-existing-main
missing-oversized-baseline	internal/infra/etcd/environment_blueprint_staging.go	none	600	850	850	860	pre-existing-main
missing-oversized-baseline	internal/infra/etcd/environment_compose_projection.go	none	600	926	922	926	pre-existing-main
oversized-file-growth	internal/infra/etcd/environment_deletion_lock_test.go	1375	1000	1505	1505	1508	pre-existing-main
oversized-file-growth	internal/infra/etcd/hierarchy.go	899	600	955	955	955	pre-existing-main
missing-oversized-baseline	internal/infra/etcd/hierarchy_deletion_controller_finalizer.go	none	600	676	676	722	pre-existing-main
missing-oversized-baseline	internal/infra/etcd/hierarchy_deletion_membership.go	none	600	659	659	723	pre-existing-main
missing-oversized-baseline	internal/infra/etcd/hierarchy_deletion_root_ack.go	none	600	574	574	642	formatting-only
missing-oversized-baseline	internal/infra/etcd/host_resolution_reconciliation.go	none	600	1342	1342	1342	pre-existing-main
oversized-file-growth	internal/infra/etcd/idempotency_test.go	1089	1000	1097	1097	1099	pre-existing-main
missing-oversized-baseline	internal/infra/etcd/release_task_terminal.go	none	600	624	624	673	pre-existing-main
missing-oversized-baseline	internal/infra/etcd/route_task_lifecycle.go	none	600	706	706	735	pre-existing-main
oversized-file-growth	internal/infra/etcd/runner_repository.go	1132	600	1164	1164	1164	pre-existing-main
oversized-file-growth	internal/infra/etcd/runner_task_lifecycle.go	1059	600	1060	1060	1060	pre-existing-main
missing-oversized-baseline	internal/infra/etcd/script_execution.go	none	600	1072	1073	1139	pre-existing-main
missing-oversized-baseline	internal/infra/etcd/script_repository.go	none	600	600	600	669	formatting-only
stale-oversized-baseline	internal/infra/etcd/service_repository.go	904	600	490	504	504	stale-baseline
oversized-file-growth	internal/infra/etcd/store.go	773	600	849	777	783	pre-existing-main
oversized-file-growth	internal/infra/etcd/task_journal.go	1157	600	1307	1340	1342	pre-existing-main
oversized-file-growth	internal/infra/etcd/task_lifecycle.go	3124	600	3943	3927	3956	pre-existing-main
oversized-file-growth	internal/infra/etcd/task_pruning.go	844	600	922	922	922	pre-existing-main
stale-oversized-baseline	internal/infra/etcd/task_repository.go	637	600	746	556	556	stale-baseline
oversized-file-growth	pkg/api/types.go	836	600	1148	1151	1151	pre-existing-main
```

## Appendix C: both historical frozen-total findings

```tsv
path	baseline_total	main_total	preformat_total	formatted_total	group
internal/app	16804	24114	24475	24733	pre-existing-main
internal/infra/etcd	60604	91734	93999	96189	pre-existing-main
```

## Appendix D: all six historical stale-legacy findings

```tsv
rule	path	subject	message
stale-legacy-finding	internal/core/envelope.go	ComponentSpec.Config	legacy finding open-model-field is no longer present
stale-legacy-finding	internal/core/model.go	Component.Config	legacy finding open-model-field is no longer present
stale-legacy-finding	internal/infra/etcd/route_removal_intent.go	reflect	legacy finding reflect-import is no longer present
stale-legacy-finding	internal/infra/etcd/service_lifecycle.go	reflect	legacy finding reflect-import is no longer present
stale-legacy-finding	pkg/api/types.go	Component.Config	legacy finding open-model-field is no longer present
stale-legacy-finding	pkg/api/types.go	ComponentEnableRequest.Config	legacy finding open-model-field is no longer present
```
