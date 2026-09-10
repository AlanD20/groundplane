# Script final-primary publication fence — 2026-09-10

Local task4 checkpoint. Explicit runner preparation and authoring remain pending;
no live QA mutation or production qualification is claimed.

## Implemented

Manual execution and Blueprint hooks now share the final Script-primary check.
It compares the captured authored metadata to the current primary after source
preparation, allowing only reference-count bookkeeping. Blueprint's bodyless
primary is validated using its separately captured immutable body; that body is
not added to mutable metadata. The resulting exact revision enters the final
publication transaction. Duplicate references to one primary share one fence.

Both inherited and explicit hooks are fenced: otherwise an inherited-to-explicit
edit could evade an explicit-only check. Script/Environment/Service identity,
body generation and supplied Script-set generation must agree. The existing
plan-derived execution constructor leaves Script-set generation to publication;
that established input form remains valid, with the selected primary supplying
the generation as before. Context/source equality is rechecked before producing
the publication fragment.

A metadata edit before the final read is rejected. An edit after that read loses
the final CAS and publishes no Task, queue entry, Release marker, source root or
Environment head. Frozen executions and source-release ownership are unchanged.

## Evidence

All logs below are under`.tmp/production-mvp-20260910/`, using Go1.26.7 and
repository-local temporary/cache paths with two-way compilation bounds.

- `script-context-blueprint-primary-red.log`: missing shared Blueprint fence.
- `script-context-blueprint-primary-green.log`: primary comparison tests pass.
- `script-context-blueprint-final-behavior-red.log`: actual final publication
  accepted a late Script edit before the fence was wired; maximum-shape tests
  also showed that the primary comparisons were absent.
- `script-context-blueprint-final-green.log`: the final transaction rejects
  the late edit; focused primary and maximum-shape tests pass.
- `script-context-blueprint-focused-race.log`: all`Script`-named tests plus
  final legal-shape and injected combined-maximum failure tests pass
  with`-race -count=1`.

The combined maximum fixture includes32Releases,16explicit hooks,17physical
sources, candidate Attaches and Backup. It uses192comparisons,176success
mutations and192failure reads, within the unchanged256-per-arm Blueprint limit.
Ordinary96-operation transactions are not enlarged. Shape fixtures now provide
real primary metadata and wire context; they are still bounded storage-shape
proof, not complete runnable explicit plans. Existing real-body fixtures retain
their actual preparation/release counts.

The broader`Script|Blueprint`race selection failed in five existing groups:

- `TestBlueprintRequirementGateClaimEpochAllowsOnlyExactPrerequisiteAcknowledgements`
- `TestBlueprintRequirementGateNonSuccessPrerequisiteNeverRefreshesEpoch`
- `TestBlueprintCompletedHooksReleaseSourceFenceBeforeNextPublication`
- `TestBlueprintMixedProducerRecoversServingAndFirstCandidate`
- `TestBlueprintHookRecoveryTerminalReport`

`script-context-blueprint-final-race.log`preserves those failures. The same
groups reproduce with the exact previous-commit versions of all changed files,
using Go's read-only build overlay without changing the checkout:
`script-context-blueprint-baseline-race.log`and`script-primary-baseline/overlay.json`.
The failures concern corrupt Release fixture authority, incomplete normalized
Compose coverage and missing Agent native-predecessor authority. They remain
task6 requirements; this comparison does not resolve or waive them.

`script-context-blueprint-final-vet.log`retains the three previously recorded
protobuf copy-lock warnings in`script_source_reference_codec.go:153/218/237`.
No broad-green or full-CI claim is made.

## Next

Prepare only eligible explicit resources and authenticated image seals, build
the actual minimal Controller projection, then enable context authoring across
Blueprint/API/CLI/Console. Preserve the pending native-trial mutation-isolation
audit and live storage-integrity qualification before deployment/schema writes.
