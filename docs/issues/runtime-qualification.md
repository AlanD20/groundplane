# Remaining runtime qualification

- Owner: primary delivery owner for the affected feature
- Severity: high for missing data-safety or exercised destructive-path proof;
  medium for unavailable health; low for isolated presentation/fixture hygiene
- MVP-required: yes for the actual hosting and recovery paths exercised by Gate A/B

This file records unresolved proof, not a historical implementation journal.
The [task list](../../tasks/todo.md) owns scheduling; [head.md](../head.md) owns
operational authority. Live writes remain paused by the storage incident.

## Storage and source retirement

The normalized desired revision is the sole Volume authority;
the remaining flat `VolumeRecord` is read projection, not a writable primary.

Remaining acceptance includes mounted-Volume consumer detachment, crash/Retry
recovery, real manual-Script source consumption and source-family retirement.
In particular, qualify prepared references through physical destruction and
cross-Environment Network ownership; allocating a source count alone is not
proof that every deletion path fences it. Keep unchanged workload bytes, native
Release ownership, proxy identity and persistent-resource authority. No coverage,
ownership or immutable-source guard may be relaxed to pass the journey.

Requirements are in [Volumes and Entries](../features/storage-and-entries.md),
[Script execution](../features/setup-scripts.md), and their routed technical
contracts. Local source-retention proof and retained wider failures are in
[Script evidence](../acceptance/script-execution.md#manual-immutable-source-integration).
Recheck code and relevant evidence before implementing an older suspected gap.

## Blueprint integration

The 692,408-byte historical failed publication and 412,657-byte local reproduction
were corrected without raising the 262,144-byte marker limit. The local running
marker measured 71,808 bytes in the local reproduction.
That closes the old blanket oversized-publication blocker, not every Blueprint
recovery case or the newer explicit-context first-Apply/reapply requirements.
Preserve exact witnesses and all record, assignment and transaction ceilings.

Acceptance is the actual current producer, source-retirement and recovery paths,
then the current full bundle through normal surfaces. Do not replay a stale
desired revision over a working Environment merely to repeat an old test.

## Service health and Route visibility

The old generic ingress hint was corrected locally using existing Route states.
It still needs deployment. Service observation and its Environment aggregate
are implemented locally but need live qualification; unavailable evidence must
not become Healthy. Desired intent
and successful old Tasks do not prove workload or public reachability.

Use [Services and Releases](../features/services-and-releases.md).
Acceptance is consistent Console/CLI/API
observation, correct unavailable/unhealthy semantics and the deployed Route
summary without claiming more than its evidence.

## Verification debt

The 2026-09-12 maintenance slice cleared five protobuf copy-lock findings and
seven Staticcheck findings across Agent, execution-plan, Agent-channel and etcd
packages. Plan validation now clones protobuf messages; runner snapshots remain
pointers. Unused assignments are removed, Zone conversions are explicit, and
late log-subscription cancellation uses the existing bounded cleanup context.
Scoped vet, Staticcheck, formatting and affected race tests passed. Evidence:
`.tmp/ci-cleanup-7arYNpkD/{vet-after,staticcheck-after,analyzer-focused,analyzer-etcd-focused,analyzer-zone-focused}.log`.

This is not a full-package or full-CI pass. The broader selection in
`analyzer-focused.log` reproduced `TestBlueprintHookRecoveryTerminalReport`:
Agent admission rejects incomplete native predecessor authority. This failure
was already recorded in [Script acceptance](../acceptance/script-execution.md#explicit-context-qualification).
The exact affected terminal-publication, source-reference and Zone selections
passed separately.

The CoreDNS CLI fixture now exercises Component read, config read and full
Corefile replacement. The Blueprint fixture uses `environment blueprint apply`,
reads the current revision and asserts the exact `If-Match` fence on the PUT.
The complete CLI subtree passed race tests with coverage, and vet passed.
Evidence: `cli-subtree-corrected-loopback.log`, its matching coverage file and
`cli-vet.log` in the same evidence directory. The first local run was blocked
by sandbox socket permissions; the passing run used isolated loopback servers,
not a live Controller.

The CLI Staticcheck run found 13 unchanged ST1005 error-capitalization warnings
in `internal/cli/component.go`, `component_config_file.go` and
`component_enable.go` (`cli-staticcheck.log`). Owner: CLI maintenance. Acceptance:
align error text with the Go standard, preserve behavior and pass scoped checks.
These files are outside the fixture correction; no broad analyzer pass is claimed.

Retained Agent fixture work includes
`TestObservedRecreateProbeAcceptsSealedBlueGreenPrior` /
`TestObservedRecreateProbeUsesEachSealedArtifact`. The latter both still fail
with a missing 32-byte plan hash (`probe-before.log` in the same evidence
directory). Their fixtures must supply a valid sealed plan and selected recovery
authority, not just hash-shaped bytes. Keep validation and repair fixtures at
their owning seam; do not bypass assertions. Owner: Agent recovery maintenance.
Acceptance: both tests prove their original recovery invariants through valid
sealed authority, with race tests passing. Broader Script/Blueprint failures
remain open with their recorded acceptance evidence.

Go 1.27 crashed the pinned analyzer in the recorded run; Go 1.26.7 ran it and
reported SA4006. That tool crash is not a source finding or a green result.
Exact current tasks and evidence are in [tasks/todo.md](../../tasks/todo.md).
Architecture-only cleanup remains separately [deferred](deferred-architecture-cleanup.md)
under its explicit authority, without waiving real safety, build or runtime defects.
