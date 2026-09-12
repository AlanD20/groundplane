# Safe-update evidence

This document consolidates local and disposable-QA evidence for Controller and
Controller-owned Agent updates recorded on 2026-09-10. Requirements remain in
[the Safe updates feature](../features/upgrade-safety.md). Host observations are
dated, source-bound evidence, not current health or production qualification.

## Scope and authority

Live work targeted disposable QA `10.25.0.2` only. Production, provider ingress,
host firewall and application configuration were unchanged. The explicit legacy
bootstrap is distinct from normal native update Tasks. Full CI, later feature
qualification and production approval remain separate gates.

All local Go evidence used Go 1.26.7, `GOTOOLCHAIN=local` and repository-local
temporary/cache state under `.tmp/production-mvp-20260910/`, unless a narrower
path is named below.

## Agent admission and uncertain publication

Channel foundation source was `7fbd4935b`. Two failing regressions established
the admission defect:

- `TestUpdateBusyAgentRestoresRealRegistryDispatch` reported
  `rejected busy update left ordinary dispatch paused`; and
- `TestDispatchReadyPauseDuringPreparationRemainsDispatchable` showed a valid
  delayed assignment being quarantined.

Update preparation now uses reversible operation-owned holds, drains admitted
claim/plan preparation, releases only its own hold on known busy/prepublication
failure, and wakes dispatch. Removal, revocation, reconnect and other holds stay
monotonic. Once replacement publication is attempted, the hold remains until a
same-value primary-revision CAS proves whether the old transaction committed.
Committed generation reuse, delayed commit fencing and retained predecessor
recovery are covered.

Executed commands/results:

```text
go test -race -count=1 ./internal/controller/agentchannel ./internal/controller/localagent -timeout 90s                         pass
go test -race -count=1 ./internal/controller/agentchannel ./internal/controller/localagent ./internal/app -run '^(TestRegistry|TestDispatch|TestConnect|TestUpdate|TestReconcile|TestAgentUpdate|TestLocalAgent)' -timeout 90s   pass
go test -race -count=1 ./internal/infra/etcd -run TestLocalAgentReplacementResolutionFencesLateCommit -timeout 90s              pass
go test -race -count=1 ./internal/controller/localagent ./internal/controller/agentchannel -timeout 90s                          pass
go test -race -count=1 ./internal/app -run '^(TestLocalAgent|TestAgentUpdate|TestControllerTaskUpdate)' -timeout 90s              pass
go vet ./internal/controller/agentchannel ./internal/controller/localagent ./internal/app                                       pass
go vet ./internal/controller/localagent ./internal/app                                                                            pass
```

The first broad run deadlocked after permanent revocation was incorrectly made
to wait for uncooperative plan preparation; corrected packages passed. Etcd-wide
vet retained protobuf copy-lock warnings at
`script_source_reference_codec.go:153`, `:218` and `:237`.

Cold-start recovery restores an all-generation hold from the running Task before
listeners, fences the current primary before credential generation, restores an
expired partial replacement's predecessor, and completes lost-ACK replay without
rotation. Full race tests for `localagent`, `controllertask`, `agentchannel` and
the selected app paths passed; no QA installation was made by these increments.

## Native coordinator and release publication

ADR 0074's frozen native Task pins manifest, predecessor executable and optional
Agent identity/image/generation. It validates its plan hash and 600-second claim
budget before effects. Active work drains for at most 120 seconds and is never
aborted. Candidate qualification requires executing-inode digest, all HTTP
listeners, channel listener, fresh etcd health and authenticated Agent Ready.

Activation and Abort have one journal-CAS winner. A `stopping` phase prevents a
stale watchdog from stopping a recovered Controller. The regressions
`stale rollback stopped the recovered Controller`, obsolete-watchdog
`state.conflict`, and acceptance of a group-writable release ancestor all failed
before correction and passed afterward. Release-directory ancestors remain
trusted and non-group/world-writable.

Full race suites passed for:

- `internal/common/controllerupgrade`;
- `internal/controller/controllerupgrade`;
- `internal/controller/controllertask`; and
- `internal/infra/controllerrelease`.

Affected app/etcd Agent-selection, Task-retry and native-retry-refusal tests and
scoped vet passed. Listener tests required permitted local ephemeral sockets;
root-ownership tests ran in the real ownership namespace because the restricted
namespace maps root to uid 65534. No trust check was relaxed.

Publication retains the last healthy release selection before a later failed
trial, rejects generic native Retry, selects Agent inputs at request time, and
starts native/Agent recovery before listeners. A matching durable claim is
mandatory for an unfinished journal. No remote state changed during this local
evidence.

## Bootstrap, deployment client and operator surfaces

Source-derived metadata pins Controller bytes, display version, storage epoch
and channel schema without executing the candidate. Descriptor-relative staging
rejects unsafe ancestors, symlinks, hardlinks and changed bytes. Bootstrap
installs the recovery guard before service enable. A legacy host requires an
explicit idle maintenance bootstrap; normal deployment never overwrites legacy
executable/config/unit state, and `--stage-only` never activates.

The deployment client fsyncs release/key receipts before POST, replays the same
key after a lost response, and retains unresolved/failed/timed-out receipts for
explicit resume. A 720-second client timeout does not trigger SSH rollback or
Abort. `scripts/deploy.py` shrank from 1,531 to 841 lines; new modules stayed
below 600 lines.

`make deployment-check` passed 38 tests at the bootstrap checkpoint. Go metadata
and update-route race/vet, exact Node 24.19.0/npm 11.17.0 release/Console checks,
version injection and `bin/controller-release.json` agreement passed. OpenAPI
and clients regenerated without contract drift. The first frozen npm install
failed with sandbox DNS `EAI_AGAIN`; the pinned network-enabled retry passed.
Evidence includes `native-deployment-tests.log`, `native-bootstrap-build-retry.log`,
`native-bootstrap-api.log` and `native-bootstrap-architecture.log`.

The `controller.update` vertical comprises Console review at
`/platform/controller`, `groundplane controller update --release sha256:…` and
digest-only `POST /controller/update` returning protected `202 {task_id}`.
`GET /host` exposes executing digest, guarded availability, nullable candidate
and latest retained Task. Console retains release/key before acceptance and Task
id afterward in tab-scoped storage; reload and uncertain acceptance reuse the
same request. Terminal native Tasks do not offer generic Retry.

Focused Controller, update, CLI/client and etcd history/pruning race tests,
scoped vet, API generation, 62 Console tests and the production build passed.
An isolated loopback fixture recorded zero uncaught UI errors, 11 successful
final reads, same-key resolution to one Task, and correct 320/768/1024/1440px
layout; fixture LCP was 507ms and CLS 0.00, not production performance evidence.
Evidence is in `.tmp/production-mvp-20260910/`, including
`controller-ui-requests.json`.

The broad architecture gate remained red for accumulated oversized/frozen
module drift, stale baselines and cross-layer acceptance placement. This is CI
debt, not an unavailable-resource exception.

## Native trial mutation isolation

A strict-storage audit proved an unfinished native trial did not isolate all
Controller writes: Script PATCH returned 200 with one write and zero admission
checks, while scheduler publication/expiry/pruning also ran. The correction
adds journal admission to HTTP and scheduler writers. GET/HEAD and established
read-like POST operations remain available. Only protected replay and the
matching native Task's exact Abort retain write semantics; every other mutation,
Retry and maintenance pass fails closed until settlement.

Startup source recovery was checked against predecessor source `85a472476`.
Count-only Script rewrites remain readable; nonzero order or explicitly authored
context fails the predecessor decoder. This proves why the hold is required but
does not make arbitrary future schemas downgrade-compatible.

Evidence under `.tmp/production-mvp-20260910/` includes:

- `native-mutation-admission-red.log`, the HTTP/scheduler reproduction;
- `native-mutation-admission-green.log` and
  `native-mutation-admission-fixed-green.log`;
- `native-mutation-http-final-race.log`;
- `native-mutation-recovery-race.log`;
- `native-trial-script-compatibility.log`; and
- `native-mutation-admission-vet.log`.

The guarded live trial—reject Script/Blueprint writes before Healthy, preserve
desired state, then admit writes after settlement—remains blocked by the storage
integrity pause.

## Disposable-QA releases and recovery

### Baseline A and normal A to B

Source `7022a2c6e` was installed by explicit `scripts/deploy.py --bootstrap` as
`0.0.0-qa.native20260910.1`.

- Controller/recovery SHA-256:
  `4d6fed201291a03fe9b3524378d01bcec2de36f3e6a1df65ae8a10389bbf0d86`.
- Release:
  `sha256:59be873f8c10fb4429d911a3e799a8cf8df9131cf6c6581e189ad1c67ef34882`.
- Agent:
  `localhost:5000/groundplane-agent@sha256:7268003b038e7bf1fd45f303dafdbe551287c4a3c98b01b150e6a5483e3d4142`,
  container `f227c3e21119`.
- Agent Task `task_01M24Q1W3MRAQAN9DE2A2MPBW7` completed
  03:51:15.804 UTC.

Bootstrap did not change application identities. Its first public WebSocket
sample failed after 30 markers during a Cloudflared QUIC loss that began before
Controller stop. A Python HTTP monitor producing 403 while curl returned 200
was rejected as an invalid instrument. Evidence includes
`bootstrap-public-failure-diagnostic.txt`, `bootstrap-websocket-continuity.log`
and `bootstrap-http-continuity.jsonl`.

The first normal B request returned 422 because bootstrap installed Controller
and guard mode 0755 instead of 0500. Mode-only repair was applied after focused
proof. Distribution also saw public QUIC/502 loss while A still ran, before
activation; this remains a failed distribution sample.

Task `task_01M24R3HJHA1NAFVQXEPNDH9TG` then completed B:

- version `0.0.0-qa.native20260910.2`;
- Controller SHA-256
  `6106db1660ba3c8a57ab850541850dc931aa97c720be34638753a54343b8552e`;
- release
  `sha256:aab1bc55201f56bd8d61394f561be16c4699f401dce3bc4555d72d278ff668c3`;
- Agent
  `localhost:5000/groundplane-agent@sha256:22d02c3cd0129c02269e1119d916ec23fb3a8b0007c1a2ba4d34fac1e9b9f251`.

That isolated activation passed 3,000 held WebSocket deliveries and 1,200
verified-TLS HTTP requests with zero failures.

### Busy, Abort and failed-candidate recovery

Task `task_01M24RE5RYG03GM1AD3GQSMJPV` exhausted its 120-second drain without
aborting a 240-second Script; a following Script proved dispatch resumed. Task
`task_01M24RMM78T3KG1CVS5H14RBA0` was aborted during preparation, and
`task_01M24RMMCTXEF6FCG2WVFC6WQW` completed afterward.

Non-executable Task `task_01M24RSDMV50HHENNR555A1G1W` and interrupted unready
Task `task_01M24RYAZFZ1GSKNTWHB3Z86A4` both failed with automatic predecessor
recovery and retained identity. Recovery windows each passed 3,000 WebSocket
deliveries and 1,200 HTTP requests. No SSH rollback, application deploy or
Script replay was used.

### Releases C through G and continuity failures

| Release | Source/task and immutable identities | Traffic result and limit |
| --- | --- | --- |
| C | source `c2eea5b2d`; Task `task_01M24SR4NW9SYE0ZP84G4SAKJG`; Controller `.3` SHA-256 `53248ff3f5390e119b9625190cfee3385370917e889d83fc81fe5be29773f6ea`; release `sha256:e35bb89e867fe96a7066d46013fcb27e07e955a8cc7fe3b6e6fc8fe2f1ef42db`; Agent `233aedb40be555ef25bdc49ef274c745cc1ce86ded60169558a2647328c03ca2` | 1,294,483,968-byte paced archive; WebSocket and one HTTP request failed before activation. Later activation and cached-stage windows passed, but did not qualify whole-build continuity. |
| Coordinated rollback | Task `task_01M24SXMMDX2TBFJYVHTP2AH57` | Deliberately unready Agent hit the 120-second Ready deadline; Controller C and Agent C recovered, same Task retained, five-path traffic passed. Storage/channel mismatch each returned 422 before Task publication. |
| D | Task `task_01M24V9SG35EDX2S4BQJKNQRV8`; Controller `.4` SHA-256 `455592f11c09634b07bce2430f911ea7d610984c5a44966c489b6fae4e8d38bc`; release `sha256:f34915444677db2d85046b86f6725ce7fa7d04f2242682db33c6eab004bf34fb`; Agent `1f9242006e8920260d3bfd0b2962c2b6fd3f83fa38b0d5da3461f8e60d64b0f9` | 3,000 WebSocket deliveries passed. Cached build; HTTP window overlapped E and is not a zero-failure D proof. |
| E | Task `task_01M24VNKBECZ36ZERESC05Q6FP`; Controller `.5` SHA-256 `775ecac42e5db4d9d11f96d01b9544c26836e5432437deb1d53dcc4068ff5317`; release `sha256:5edfc685242b68f481a0cfe75cb6d3de50c7dc12dc567c3ebe906e073ab10f40`; Agent `f9ea92fccc4833bd899cad31764a92f5fc8e0a198fee27f9f6966235aa3f95c0` | 276,611,072-byte Agent-only archive; WebSocket failed after 107 markers and HTTP recorded 24 failures before activation. Task success does not erase the traffic failure. |
| F | Task `task_01M24W6DBC0VECK1C62YKH7NMY`; Controller `.6` SHA-256 `93f193157b9b3a0fd2e5f9389951cff067aa1aff464e816e74aca5ee06472f65`; release `sha256:b2d2854b9068c877ee88ee2913a56c5c679a64b431cf9a0c9d078cb156dc134e`; Agent `aa84f5fcebea97d3e07a96a44dece75c616b4c357e354556d4990a5e3852d7d7` | Diagnostic 2 CPU/2 GiB/no-swap wrapper and `GOMAXPROCS=2`; 3,000 WebSocket and 1,200 HTTP passed. One sample did not prove the cause. |
| G | source `45c3d5d7f`; Task `task_01M24WQY66ZACTHF2896D72M0K`; Controller `.7` SHA-256 `8d370a6ec97e75672dde9c0f5a80534bc0aa0f60715828a288fa1e0b3d89b1ee`; release `sha256:43f699bd8fdf01eced5998ac46c3ac03b784a3812b34e32c09790a0fd57bceab`; Agent `a056ee3d3ffc5947c9854dbe0b23c5c967dab22c8a936d7199ced17892ec5893` | Checked-in two-thread/two-program build limits; all 55 deployment tests and the uncached whole path passed 3,000 WebSocket and 1,200 HTTP with no Tunnel event. This does not retroactively pass four uncapped samples or prove the QUIC mechanism. |

Transport changed to paced 64 KiB chunks at at most 4 MiB/s and later reused
matching target content; native preparation stopped transferring unchanged
Runner images. Passing cached/build-only windows and one bounded whole build do
not establish arbitrary shared-builder capacity. Raw evidence names from the
source report are retained under `.tmp/production-mvp-20260910/`, including
`qa-native-update-{c,d,e,f,g}-deploy.log`, the matching continuity logs,
`paced-transfer-*`, `bounded-*`, `compiler-*` and `deployment-55-tests.log`.

The standalone Agent busy attempt returned
`state.conflict: Agent already runs the selected image` before publication, so
it did not prove busy drain. A pre-native image-mismatch fixture remains needed.
The first compatibility HTTP instrument used an invalid unversioned path and is
not compatibility evidence.

## Remaining qualification

The dated evidence proves normal activation, protected busy/Abort behavior,
failed/interrupted Controller recovery, coordinated Agent rollback and
compatibility refusal in the named windows. It does not prove current host
health, universal whole-build public continuity or production readiness.

Still required:

- reproduce and resolve the later whole-build Tunnel timeout evidence without
  assuming shared-builder pressure is the cause;
- live watchdog and changed-boot recovery;
- standalone Agent busy drain with a valid mismatch fixture;
- guarded trial/write isolation after storage integrity is qualified;
- full CI and downstream feature/recovery qualification; and
- explicit production approval.
