# Blueprint authoring and application

A Blueprint describes desired Environment configuration using Compose plus GP
extensions. It is not a record of running containers.
[The Blueprint reference](../blueprint.md) owns syntax and accepted fields.

## Read, edit, validate and apply

Use `environment blueprint show|validate|apply` in the CLI, or the Environment
Blueprint editor in the Console. Export produces canonical single-file YAML.
Import can accept a closed multi-file bundle.

Export reconstructs normalized desired state. It does not preserve comments,
anchors, aliases or source-file boundaries. Script bodies use literal YAML
blocks. Drafts stay local until explicitly submitted.

Read the desired revision before Validate or Apply and send that exact quoted
`If-Match`. A stale revision fails without writes. Revision zero means no
desired head. Validate parses the same input as Apply but creates no Task,
revision or host effect; its diff is create, update or retain.

Apply publishes one reconcile Task with immutable inputs. Follow its outcome;
acceptance does not imply successful application. Required workload images must
already exist in the host Docker daemon.

## Retention and identity

The intended rule is that omission retains existing resources, never deletes or
renames them. **Current Entry reconciliation conflicts with this rule:** it can
select omitted Blueprint-owned Entries for removal. Retain existing Entry keys
until this [implementation gap](../issues/runtime-qualification.md#blueprint-entry-omission)
is corrected and qualified.

Use the explicit protected Remove operation to delete a resource. Ambiguous
preservation fails instead of guessing.

Blueprint Entry keys identify their reconciliation identity, separately from
destination env keys or file paths. Source/exposure edits retain the Entry id;
type, destination, file ownership and secret-storage class are not in-place
identity edits. Direct and Component-owned Entries are preserved.

Existing Service runtime intent remains separate: applying a Blueprint must not
restart a deliberately stopped or destroyed Service merely because its definition
is still present.

## Hooks and partial failure

Setup and migration hooks follow [Script rules](setup-scripts.md).
For Custom backing hooks, first finish a standalone Attach before using its newly
produced facts. A single Apply cannot create that hook-based owner and consume
its new output in already-sealed files. Ready facts and credential reuse remain
usable; see [Custom backing hooks](backing-services.md).

Retry/recovery use the captured input, not the latest desired document.
Partial failure must remain visible. Pinned configuration recovery is limited to
files that the failed operation was authorized to replace; it is not database
restoration or migration reversal.

## Latest-wins design: not yet available end to end

The accepted design compares resource-level effective inputs and cancels obsolete
work when a newer valid input supersedes it. Conflicting work waits for proven
executor stop and accounted effects; unrelated work may continue.

Only a superseded Blueprint shared-configuration write may hand off forward
repair from accounted-but-diverged effects. Unknown effects remain restricted.
This does not authorize replaying Scripts or superseding ordinary Deploy,
Backup, upgrade or destructive operations.

The selector exists, but unit persistence, late plan preparation, safe handoff
and full runtime integration remain incomplete. Do not depend on automatic
supersession in production.

## Design and qualification

[Desired-state publication](../decisions/desired-state-publication.md) explains
immutable inputs, bounded staging, atomic visibility and the supersession design.
[Capability status](../capabilities.md) and the [QA matrix](../qa-matrix.md) own
implementation limits and behavioral proof.
