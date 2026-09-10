# Agent update admission: local regression evidence

Scope: first `upgrade-safety` increment, not complete platform upgrades or QA
acceptance. Channel foundation is7fbd4935b; this commit connects the updater.

## Reproduced failures

- `TestUpdateBusyAgentRestoresRealRegistryDispatch` failed on the former updater:
  `rejected busy update left ordinary dispatch paused`. Existing fake-only tests
  had proved no credential/container mutation but not resumed task admission.
- `TestDispatchReadyPauseDuringPreparationRemainsDispatchable` exposed a second
  failure: a valid assignment delayed by a pause was added to quarantine.

## Correction

Separate reversible, operation-owned preparation holds from permanent lifecycle
fences. Drain all admitted claim/plan preparation for the selected generation
before checking idle. Drop only the caller's hold on busy/failed preparation,
including cancellation, and wake the live dispatcher. Preserve other holds,
reconnect fencing and monotonic removal/revocation. Distinguish deferred valid
assignments from corrupt-plan quarantine and account for completed sends before
honoring a newly acquired pause.

The updater retains its pause once durable replacement publication is attempted:
a failed response is not proof of no commit. Committed replacement/recovery
establishes the irreversible predecessor fence. End-to-end automatic recovery
of uncertain publication remains part of the next coordinated-upgrade slice.

## Local checks

Go1.26.7, `GOTOOLCHAIN=local`, repo-local `.tmp/production-mvp-20260910`
temporary/cache directories:

- Full `go test -race -count=1 ./internal/controller/agentchannel
  ./internal/controller/localagent -timeout 90s`: pass.
- Updated/final selection `go test -race -count=1
  ./internal/controller/agentchannel ./internal/controller/localagent
  ./internal/app -run
  '^(TestRegistry|TestDispatch|TestConnect|TestUpdate|TestReconcile|TestAgentUpdate|TestLocalAgent)'
  -timeout 90s`: pass.
- `go vet ./internal/controller/agentchannel ./internal/controller/localagent
  ./internal/app`: pass.
- Repository-pinned golines and `git diff --check`: pass.

The first broad run caught a deadlock caused by incorrectly extending permanent
revocation to wait for uncooperative plan preparation. Permanent fences retain
their send-only drain; reversible update preparation tracks admitted operations
separately. The revocation race and full affected packages pass after correction.

No QA binary/image was installed by this increment. No production, application,
provider or host-network state changed. Full CI and runtime qualification remain
initiative gates, not implied by this local evidence.

## Unknown-publication resolution increment

The lifecycle now uses a same-value primary-revision CAS to resolve unknown
replacement publication. A delayed old transaction cannot commit after this
barrier. An already committed generation resumes without another rotation.
Storage outages retain one operation-owned recovery record in the Manager;
reconciliation resolves it before admitting later lifecycle mutations. Recovery
retains the pinned previous image through an uncertain rollback response.

New regression proof (Go1.26.7, same repo-local caches):

- `go test -race -count=1 ./internal/infra/etcd -run
  TestLocalAgentReplacementResolutionFencesLateCommit -timeout 90s`: pass,
  covering both an already committed and a deliberately delayed transaction.
- `go test -race -count=1 ./internal/controller/localagent
  ./internal/controller/agentchannel -timeout 90s`: pass.
- `go test -race -count=1 ./internal/app -run
  '^(TestLocalAgent|TestAgentUpdate|TestControllerTaskUpdate)' -timeout 90s`: pass.
- Additional cancellation and deferred committed/failed-candidate rollback
  regressions pass. The original real-Registry unknown test still proves no
  dispatch while the storage barrier is unavailable.
- `go vet ./internal/controller/localagent ./internal/app`: pass.

Package-wide etcd vet additionally reports existing protobuf copy-lock warnings
at `script_source_reference_codec.go:153`, `:218` and `:237`. That file is
unchanged in this increment; correction belongs to Script/CI qualification and
is not waived. Cold Controller restart still needs admission restoration and
replacement-attempt fencing from the durable native Task before this initiative
can claim complete upgrade safety. No new QA deployment is implied.
