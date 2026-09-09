# Volume mutations after native Release deployment

## Removal integration progress — 2026-09-09

The actual DELETE publisher, Script exclusion, durable runtime, assignment-owned
Agent checkpoints, bounded helper calls, Retry and terminal ownership release
are connected. A hermetic DELETE/worker/terminal journey and separate real-file
bounded cleanup/replay proof pass; see `docs/head.md`. Live acceptance is pending.

The retained single-slot removal projection reproduced the coverage failure
below for both blue and green. Removal now preserves historical Service labels;
the cleanup artifact changes only its plan-local id, retaining the exact baseline
YAML and mount evidence. The new projection regression passes, including
unrelated mount preservation and missing-label rejection. Reconstructed removal
plans also pass. No coverage or plan ownership guard was relaxed.

Volume add/edit and Entry still relabel and fully reconcile retained workloads.
Their correction remains required. Removal consumer execution also needs a
dependency-free selection proof; the existing targeted apply still permits
Compose dependency expansion. These local checks are not live retained-runtime
acceptance, and this issue remains a floor blocker.

## Retained-runtime mutation failure

- Owner: Controller Volume capability
- Severity: primary operator-journey blocker
- MVP-required: yes; management acceptance is not complete
- Observed: 2026-09-08, disposable QA `10.25.0.2`, `qa-application`

The requested normal Volume lifecycle test failed before publication. No test
Volume was created and existing application volumes were not changed.

The first failure was HTTP 500: `Compose artifact Service labels are incomplete`.
`NormalizedEnvironmentArtifact` deliberately returns authored Service identities
without runtime execution labels. The Volume mutation incorrectly passed those
identities into the runtime metadata rewrite. The correction retains only the
normalized YAML from that second mutation, after independently validating the
complete runtime artifact. The focused regression failed before correction and
the Volume package passes with race detection, including rejection of missing
labels on actual runtime metadata.

The live rerun then rejected with HTTP 422:
`Environment Compose logical Service coverage is incomplete`. The direct Volume
mutation relabels all retained runtime Services with the new plan/generation.
Consequently, a retained stable-proxy plus one serving blue/green slot is treated
as a fresh incomplete topology by the publication coverage validator. This guard
must not be relaxed: a Volume identity operation must preserve valid serving
authority rather than manufacture a new deployment topology.

A service-scoped Entry referencing a disposable project Secret also returned
the same 422 coverage error before publication. Project Secret creation and
masked reads succeeded; new Entry consumption and replacement are therefore
not proven. Include Entry direct-mutation ownership in the same targeted audit.

Evidence is retained under `.tmp/qa-management-20260908/evidence/`:
`volume-lifecycle-20260908T162336Z.fuygCW` (500) and
`volume-lifecycle-20260908T163758Z.8xRRe5` (422). Both tunnel supervisors completed
cleanup; no Volume cleanup was needed. The first verifier used its legacy
system-temporary default; the second explicitly used repository-local `TMPDIR`.

## Required proof

Prove add, slug-only edit, fixed-revision impact, confirmed remove, and replay on
an Environment with independently serving native Releases and retained
blue/green runtime. Preserve unchanged workload bytes, ownership, release
authority, and health; detach only the named Volume's consumers on removal.
Bind publication, reconstructed plans, and Agent execution to the same immutable
inputs. Keep incomplete fresh runtime topology and corrupt execution labels
rejected. Update the verifier's directory-mode assertion to the current product
contract (`0755` managed leaf under the private `0700` Environment directory)
before treating that later assertion as authoritative.
