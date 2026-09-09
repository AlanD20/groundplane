# Volume removal runtime selection — 2026-09-09

Local implementation evidence, not live-host acceptance or a complete floor.

The Controller removal-plan regression first failed because the consumer apply
did not set `NoDependencies`. Both the initial Volume publisher and Controller
reconstruction now set it. The existing machine validator admits dependency-free
non-forced apply for the exact three-step Volume-removal shape: candidate apply,
the selected Docker Volume removal, and its baseline directory removal.

The shared selector binds baseline and candidate ownership, runtime metadata,
the removed Volume id/key, and cleanup targets. It selects runtime Compose names
whose baseline mount references that key and rejects retained candidate mounts.
It does not select every proxy/slot sharing a logical Service id. Startup
authorization and actual helper commands use this same selector. Empty runtime
selection is a no-op, never an unqualified Compose `up`.

The retained native renderer supplies proxy/Release/image metadata in the new
Controller test. That fixture is augmented with a slot mount and a dependency
on the stable proxy. Real plan sealing, startup authorization, and helper Execute
with a FakeRunner prove the command is exactly `up --detach --no-deps <slot>`.
The variant without a deployed mount issues no command. These tests do not run
Docker or the real DELETE publisher. They complement, rather than replace, the
existing publication/Agent/checkpoint journey.

Passing checks used `-race -count=1` and repository-local Go cache/temp paths:

- Controller: `TestResolveVolumeRemovePlanDetachesConsumersBeforeCleanup`,
  `TestVolumeRemovalSelectsOnlyMountedNativeWorkload`, and
  `TestVolumeRemovalRejectsChangedConsumerAuthority`.
- Volume/helper/machine-contract selection:
  `Test(BuildVolumeMutationProjectionAndPlansRoundTripAddRemove|Volume|ManagedVolume|StartupDependencyClosure|ComposeHelper)`.
- Existing Blueprint selection boundary:
  `^TestBlueprint(ManagedNoDependencies|.*Retained|.*ComponentChain)`.

Coverage evidence: `.tmp/volume-selection-red.cover`,
`.tmp/volume-selection-green.cover`, `.tmp/volume-selection-native.cover`,
`.tmp/volume-selection-affected.cover`, and
`.tmp/volume-selection-blueprint-boundary.cover`.

The physical retained-container inspection remains unchanged and fails closed
if any named-volume or overlapping direct-bind reference remains, including
stopped containers outside the published runtime artifact. This change does not
authorize deleting those containers, bypass that proof, or declare live Volume
removal accepted. Entry reconciliation, manual Script cleanup, oversized
Blueprint Apply, required floor gates, and disposable QA remain outstanding.
