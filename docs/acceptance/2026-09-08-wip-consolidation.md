# Local WIP consolidation — 2026-09-08

Scope: consolidate the accumulated manual Script/source-removal work on main
before implementing the owner-approved Volume-removal cutover. No subagents,
new branches, secondary worktrees, push, deployment, Backup/restore work, or
full CI was used. The original working tree had 49 changed tracked files and
62 untracked files; none was discarded.

## Commit boundaries

| Concern | Commit |
| --- | --- |
| Owner-approved Volume plan and consolidation checkpoint | `88c33073a` |
| Actual Script Network ownership in runner plans | `ba845bd83` |
| Bounded unpublished preparation recovery | `56c960c8a` |
| Source retry retention, transfer, and expiry | `f2239fb36` |
| Source adapter and startup recovery barrier | `8c9b2f134` |
| Secret removal/source-acquisition exclusion | `3d2d06a1e` |
| All-generation Entry removal protection | `2169fc09d` |
| Service removal/source-acquisition exclusion | `e9d294d3c` |
| Desired Entry/Volume publication guards | `efe62ff4a` |
| Sealed Script source membership derivation | `81a02f48b` |
| Connected manual execution state machine | `8e43f7649` |
| Controller-generated plan/body admission regression | `c921c78df` |
| Serving-Release and private Entry journeys | `053194e58` |

The manual state-machine commit is larger than the other slices because
publication, claim, terminal cleanup, Abort, Retry, and expiry must share one
reference protocol. It also replaces the old Blueprint-specific helper files
and includes the direct lifecycle regressions. The source primitives,
resource-removal guards, and external journey tests are separate commits.

## Verification method

Each code slice was staged explicitly, exported with `git checkout-index` into
an ignored repo-local snapshot, and tested there without later uncommitted files.
Snapshots and Go scratch/cache state live under `.tmp/`; they are not additional
Git worktrees. Deleted helper files were also removed from the reused snapshot.

Focused tests use `-race -count=1`. Slice proofs include the complete executionplan
package; Controller Script tests; source recovery/expiry; Secret, Entry, and Service
guards; desired-removal publication; source derivation; manual lifecycle; generated
plan admission; and four serving-Release journey variants. `make proto` reproduces
both generated Go protocol files byte-for-byte. Handwritten changed Go files are
checked with gofmt and the module-pinned golines configuration; generated protocol
files are not passed through handwritten-source formatting.

The final combined verification covers:

```sh
go test -race -count=1 ./internal/infra/etcd ./internal/app \
  -run '^(TestManualScript|TestEntry|TestSecret|TestService|TestDesiredVolumeRemoval|TestVolume|TestScriptSourceReference|TestTaskJournal|TestInitializeScriptSourceReferences)'
go test -race -count=1 ./internal/infra/scriptsourcereference ./internal/volume
```

These proofs are local persistence, projection, and hermetic execution evidence.
They do not establish live host acceptance or a green whole-repository suite.
Final combined results: persistence 4.353s, app startup selection 1.253s,
complete source-reference package 1.164s, and complete Volume package 1.216s.
The final formatting-only Entry test adjustment passes its focused race proof
(1.109s).

## Reproduced baseline failures

The following failures also occur on the unchanged `88c33073a` source snapshot:

- Four Controller Route/Entry removal/mutation plan tests reject incomplete
  Compose identity coverage.
- Four subcases of
  `TestAbortPendingBlueprintReleasesScriptExecutionAuthorityBeforeTerminalization`
  reject corrupt durable Release records during fixture publication.
- `TestBlueprintPublisherTerminalPreservesExecutedArtifact/addressable` rejects
  a corrupt durable Release record.
- `TestBlueprintHookRecoveryTerminalReport` rejects incomplete native predecessor
  authority during mixed Worker admission.

The Controller suite initially could not open local test listeners inside the
sandbox; rerunning with ephemeral local-listener permission exposed the four
Controller baseline failures above. No production listener was changed.

## Remaining scope

The owner-approved next implementation is [Volume removal](../../tasks/plan.md).
The publication guard alone does not connect its durable removal runtime,
checkpoint exchange, bounded filesystem cleanup, Retry, or finalization.
Network/Attach source protection, old rootless QA Script record disposition,
retained-runtime mutation and oversized Blueprint blockers remain as recorded in
[the head checkpoint](../head.md). Backup/restore, deployment, and full acceptance
gates remain paused. This consolidation is not floor-MVP completion.
