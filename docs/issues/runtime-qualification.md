# Remaining runtime qualification

- Owner: primary delivery owner for the affected feature
- Severity: high for missing data-safety or exercised destructive-path proof;
  medium for unavailable health; low for isolated presentation/fixture hygiene
- MVP-required: yes for the actual hosting and recovery paths exercised by Gate A/B

This file records unresolved proof, not a historical implementation journal.
The [task list](../../tasks/todo.md) owns scheduling; [head.md](../head.md) owns
operational authority. Live writes remain paused by the storage incident.

## Storage and source retirement

The [September 9 floor](../acceptance/hosting-floor.md) closed bounded Volume,
Entry, manual-Script and exact-reapply journeys. Earlier statements that these
paths were wholly unimplemented or still blocked by the same publication error
are superseded. The normalized desired revision is the sole Volume authority;
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
marker measured 71,808 bytes; complete publication/assignment proof and the
subsequent full Apply/exact-reapply journey are recorded in
[hosting-floor evidence](../acceptance/hosting-floor.md).
That closes the old blanket oversized-publication blocker, not every Blueprint
recovery case or the newer explicit-context first-Apply/reapply requirements.
Preserve exact witnesses and all record, assignment and transaction ceilings.

Acceptance is the actual current producer, source-retirement and recovery paths,
then the current full bundle through normal surfaces. Do not replay a stale
desired revision over a working Environment merely to repeat an old test.

## Service health and Route visibility

The old generic ingress hint was corrected locally using existing Route states.
It still needs deployment. Service observation and its Environment aggregate
remain unfinished; unavailable evidence must not become Healthy. Desired intent
and successful old Tasks do not prove workload or public reachability.

Use [Kobwnewe hosting](../features/kobwnewe-hosting.md) and the preserved draft
work identified in [head.md](../head.md). Acceptance is consistent Console/CLI/API
observation, correct unavailable/unhealthy semantics and the deployed Route
summary without claiming more than its evidence.

## Verification debt

Run the final relevant gates after correction, not as a documentation check.
Retained findings include `script_runtime.go:79` SA4006, protobuf copy-lock
warnings, Script/Blueprint and CLI failure groups, the CoreDNS config-read fixture,
and sealed-hash fixtures in `TestObservedRecreateProbeAcceptsSealedBlueGreenPrior`
and `TestObservedRecreateProbeUsesEachSealedArtifact`. Keep hash validation and
repair fixtures at their owning seam; do not bypass assertions.

Go 1.27 crashed the pinned analyzer in the recorded run; Go 1.26.7 ran it and
reported SA4006. That tool crash is not a source finding or a green result.
Exact current tasks and evidence are in [tasks/todo.md](../../tasks/todo.md).
Architecture-only cleanup remains separately [deferred](deferred-architecture-cleanup.md)
under its explicit authority, without waiving real safety, build or runtime defects.
