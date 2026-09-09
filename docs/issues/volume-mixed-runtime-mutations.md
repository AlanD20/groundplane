# Volume mutations after native Release deployment

## Removal integration progress — 2026-09-09

Integrated QA deployment exposed another publication boundary: retained physical
runtime metadata made direct mutations demand an Apply-only Release fragment.
The real native Apply -> retained managed Apply -> Volume create regression
reproduced that exact 422. The shared publisher now scopes the check to Apply;
direct mutations retain their sealed baseline and head fence, and still cannot
publish Blueprint domains. Actual create/edit/DELETE and reconstruction pass,
alongside Apply tamper/source fences and existing Agent/Entry terminal checks.
The corrected Controller builds; live rerun remains pending. Evidence:
`docs/acceptance/2026-09-09-integrated-floor-qa.md`.

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

Volume add/edit now retain runtime ownership too. Add executes only its directory
and selected Docker Volume ensure; edit verifies the existing Volume without
creating or repairing it. The private helper's `require_existing` field is
read-only. Historical ownership is accepted only for the exact one-Volume
create/verify plan shapes; adding workload execution or another resource rejects.
Both retained slot projections and a real native-renderer plan pass. A joined
public create/edit, Agent-worker and terminal-acknowledgement test proves active
creation, read-only rename and replay with faked Docker/filesystem effects.
No Compose/container command is permitted in that test.
The same journey reproduced a resource-only create falsely promoting the entire
desired projection as applied. Create/edit terminal acknowledgement now preserves
the prior applied-workload snapshot (or its absence), rather than asserting that
unexecuted desired Service changes were deployed.

Volume removal now selects only the baseline runtime instances mounting the
selected Volume and disables dependency expansion. Entry create/edit/bulk-upsert
preserve retained runtime metadata and select exposed workload instances without
restarting stable proxies. Entry deletion defers the desired head until its
materialization-only Agent cleanup (or never-applied Controller finalizer)
succeeds. Reconstructed plans, real Agent-worker execution with a faked helper,
terminal acknowledgement, Retry, expiry retention and writer exclusion pass.
See `docs/acceptance/2026-09-09-entry-retained-mutations.md` and the Volume
selection evidence. These local checks are not live retained-runtime acceptance;
that remaining proof keeps this issue open as a floor blocker.

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
