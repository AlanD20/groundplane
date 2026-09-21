# Delivery

Deliver coherent changes on `main` using signed conventional commits and the
owner's configured identity. Stage exact files; preserve unrelated work. No AI
co-author trailers. [Agent workflow](agents.md#execution-policy) owns delegation,
scope decisions and progress limits.

## Completion and blockers

Report implementation, verification and deployment separately. A committed
implementation is not a passing release, and a passing unit check is not live
qualification. Current user instructions may explicitly select implementation-only
work; label it unverified rather than running forbidden checks.

Block affected work for a demonstrated build failure, product-contract violation,
introduced coding-standard violation, security failure, data loss, destructive
lifecycle error, race or broken primary operator journey. Record actionable
out-of-scope findings without investigating speculative improvements.
A workaround does not close product-owned recovery.

## Testing policy

Straightforward rendering, static pages, wiring and direct implementation do not
require dedicated tests or QA cases. Tests are for complex logic, interacting
actions and substantial edge cases. Each test needs a concrete failure-prevention
reason; a coverage percentage is not one. Ask the user when uncertain.

No static source-text or contract-shape tests: no assertions about source strings,
declaration structure or generated schema inventories. Test observable behavior
when a test is justified. Compiler, generation-parity and architecture gates are
separate mechanical requirements, not product coverage.

Review existing tests by their actual assertions, not just a missing comment.
Retain useful regressions; remove only tests within the authorized cleanup scope
whose behavior has no defensible rationale. Record any lost or missing proof.

## Verification ladder

Choose the smallest appropriate proof for the changed behavior. Reuse results
only while relevant inputs and dependencies are unchanged; a new commit hash
alone does not require rerunning everything. Documentation-only work uses
editorial and link review, not application tests.

The [QA matrix](qa-matrix.md) owns behavioral cases and variants.
[Qualification records](acceptance.md#minimum-record-for-future-runs) identify the tested build,
independent expected result, actual result, cleanup and limits. A healthy process,
completed Task or agreement between internal projections cannot replace an
application/data assertion.

Broader integration and real operator journeys belong at their relevant milestone,
not after every straightforward edit. Preserve failed evidence and explicit
NOT RUN/BLOCKED results. Running a command without injecting the intended fault
does not qualify recovery.

## Required gates

The repository's local integration/release entry point is `make ci`.
The [Makefile](../Makefile) and [workflows](../.github/workflows) own its exact
commands; manifests own toolchain and generator versions. Do not copy a pipeline
into this document.

The gate includes applicable formatting, static analysis, race tests, generation
parity, architecture and artifact checks. Generated files are regenerated, not
edited. Production Console packaging must work without adjacent build files.
The lockfile must have no known vulnerabilities at delivery; do not use
`--force` or `--legacy-peer-deps` to conceal dependency conflicts.

Default CI does not run sudo or privileged host tests.
`backupstage-host-acceptance-compile` compiles the mount test;
`backupstage-host-acceptance` executes it only on an explicitly authorized
disposable root-capable host. Backup/Restore qualification remains deferred;
an unrun privileged check is not a pass.

### Approved architecture debt for 0.0.1

The [deferred snapshot](../architecture-deferred.json) and
[baseline](../architecture-baseline.json) record the exact previously approved
0.0.1 structural exceptions, including Custom hooks and the 92-line deletion guard.
The release checker permits only matching historical findings; it does not accept
new debt. Do not regenerate allowances without approval.

The production restructuring and test migration are integrated. Approved totals
were re-anchored below the previous ceilings; per-file limits were not raised.
Existing test-import deferrals now name the replacement child modules, preserving
the same test files, dependency layers and 0.0.1 expiry. These changes do not
authorize new exceptions. Strict compliance and live qualification remain unproved.
See [remaining qualification](issues/runtime-qualification.md#architecture-and-tests).
The 0.0.1 exception does not automatically extend to later releases or waive
security, data safety, builds, races or generated-artifact parity.

## Release and deployment

Use [installation and release procedures](deployment.md). Version preparation,
artifact publication and host installation are separate actions. Confirm the
selected version and destination; do not invent a release or deploy a local
implementation merely because its code is committed.

A release needs complete applicable CI and the relevant operator/failure-path
qualification on each supported architecture. [Product acceptance](mvp.md#acceptance-gates)
distinguishes hosting from Backup/Restore. [Capability status](capabilities.md)
must not claim qualification beyond recorded evidence.

A user-designated disposable QA host can be reset within its explicitly authorized
journey. That permission never extends to production, other hosts or unrelated
data. Historical documentation cannot identify today's authorized host; current
instructions must do so. Record what was removed and any retained evidence.
