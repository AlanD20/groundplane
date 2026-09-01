# C06 architecture debt

- Status: MVP-required before final `make ci`
- Owner: C06 Release integration owner
- Scope: structural extraction only; the architecture baseline must not be
  raised or otherwise weakened

## Evidence

On 2026-08-26, after `make generate`, the conflict-sensitive Release and
Blueprint race suites, and the Console build passed, `make architecture-check`
reported on the current-main integration tree:

- frozen direct-production total drift: `internal/app` is 18,983 lines against
  17,770; `internal/infra/etcd` is 58,004 lines against 54,269
- oversized growth in `console/src/lib/store.tsx`,
  `internal/common/executionplan/plan.go`,
  `internal/infra/docker/composehelper/composehelper.go`,
  `internal/infra/etcd/deletion_tombstone.go`,
  `internal/infra/etcd/idempotency.go`, `internal/infra/etcd/store.go`,
  `internal/infra/etcd/task_journal.go`,
  `internal/infra/etcd/task_lifecycle.go`, and `pkg/api/types.go`
- missing oversized entries for `internal/agent/worker.go`,
  `internal/app/release_operations.go`, `internal/controller/plan.go`, and
  `internal/infra/etcd/releasegroup/store.go`; these are findings to remove by
  extraction, not allowances to add
- the stale oversized entry for `internal/core/model.go`

The authoritative command and full machine-readable finding set is:

```sh
make architecture-check
```

## Acceptance

1. Extract Release application orchestration and Release persistence into
   cohesive subpackages so both frozen direct-production totals are at or below
   their existing limits.
2. Split each grown oversized host along real module boundaries until it is at
   or below its existing allowance; do not add an oversized-file exception.
3. Remove only stale baseline entries whose underlying finding is closed.
4. Keep `architecture-baseline.json` allowances and frozen totals unchanged or
   lower.
5. `make architecture-check` and final `make ci` pass from a clean worktree.
