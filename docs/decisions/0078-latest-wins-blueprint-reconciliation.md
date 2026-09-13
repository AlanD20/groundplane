# ADR 0078: Apply only the latest required Blueprint changes

- Status: Accepted
- Date: 2026-09-13
- Owner approval: one current Blueprint, resource-level change selection and
  automatic supersession with a safe execution handoff.
- Implementation and live qualification are incomplete.

## Context

The operator maintains one Blueprint. Internal input snapshots protect accepted
work from concurrent edits; they are not a queue of Blueprint versions that must
all execute. Comparing the latest submission with the preceding desired input
can skip work that never succeeded. Applying shared configuration before selecting
workloads can also affect Services that were not meant to change.

The current implementation prepares one Environment-wide execution plan and
promotes its applied state in one terminal transaction. It cannot safely gain
latest-wins behavior by removing its Environment lock or cancelling a subprocess
and immediately starting another one.

## Decision

### One desired document, resource-level work

Parse and validate the complete submission before accepting it or superseding
existing work. Keep one current desired Blueprint. An internal acceptance token
orders successful publications; wall-clock timestamps and client arrival order
do not decide which publication won. Existing `If-Match` protects against a stale
editor overwriting accepted edits. Latest-wins execution does not bypass that
input check or idempotency replay.

Normalize typed resources and compute private fingerprints of their effective
inputs. Ignore YAML formatting and unordered mapping order. Include every input
that changes the resource's operation, including consumed file contents, exact
protected configuration generations and relevant Attach bindings. Do not hash
secret plaintext into public metadata. Dependency ordering alone does not mean
that every change to a dependency must restart all its consumers.

Compare required inputs with successfully applied inputs for each resource, not
with the previous submission. An equal desired fingerprint does not prove an
earlier attempt succeeded or that runtime drift is absent. Unknown effects must
be resolved before they can be treated as applied or absent.
Their resource scope comes from the affected execution or observation, not the
latest desired unit: newer input may no longer reference a file changed earlier.
Until resolved, those writes fence both readers and writers on the same resources.

The planner groups changes that share inseparable effects into an execution
unit. Each unit declares its exact read resources, write resources and required
predecessors. For example, changing a generated env file and restarting its
consumers must not leave another unit free to overwrite that same file. Read-only
sharing does not create a write conflict. Unchanged resources receive no mutation;
known drift may require a targeted repair even when desired input is unchanged.

### Latest accepted changes supersede obsolete work

Acceptance atomically selects the latest desired inputs and records supersession
of older incompatible reconciliation work. An invalid or rejected submission
does neither. Replaying an accepted request returns its original Task and never
reselects old desired state.

- Pending superseded units lose dispatch eligibility atomically. Their immutable
  input and journal remain; they do not execute merely to drain a queue.
- Running superseded units lose permission to start further forward steps. The
  Controller requests cancellation and retains their resource claims while
  cancellation, effect accounting and required cleanup settle.
- New work cannot mutate conflicting resources until the old executor is proven
  stopped and its effects are accounted for. A cancelled context, timeout or
  disconnected Agent is not that proof. Unknown ownership blocks conflicting
  work visibly; it does not block unrelated resources by policy.
- Still-required units continue even if another unit from their original Apply
  is superseded. A new acceptance token alone does not obsolete equal effective
  inputs. Shared-file or dependency conflicts are not unrelated work.
- Replan remaining work against the latest accepted input and settled applied
  state. If A is running and B then C arrive, discard obsolete pending B and
  proceed toward C after A's handoff; do not finish B first.

Late messages may record real effects of the exact old execution. They cannot
promote superseded input as the latest desired state, authorize another old
forward step or release another execution's claims. Cancellation and natural
completion races have one durable winner. Cleanup and recovery remain bound to
their original execution; newer input cannot rewrite an old plan.

### Task and failure ownership

Keep the existing Apply action, CLI command and API endpoint. One accepted Apply
has one public Task and operation. Private execution units belong to that Task;
they are not hidden operator Deploy requests or child public Tasks. The Task's
submitted inputs stay immutable while each unit's execution inputs are sealed
after its prerequisites and superseded predecessors have settled.

Successful units retain their verified applied results. Partial application and
supersession must remain visible in the original Task; a Task containing cancelled
units cannot claim that its entire input was applied. Use the existing `aborted`
terminal state for superseded work, with its supersession reason and replacement
Task identified. Do not add a second public Task status vocabulary. Natural
completion that won before supersession remains completed. Retry cannot revive
superseded desired intent; a new Apply targets current intent.

This replaces whole-Environment applied-state promotion as the acknowledgement
model for Blueprint reconciliation. Public reads still have one desired document;
applied results and live observations are separate facts, not alternate Blueprints.
Cancellation must not destroy those results just to make an aggregate look atomic.

Scope automatic supersession to Blueprint reconciliation. It does not silently
cancel manual Scripts, explicit Deploy/Rollback or Release Groups, Backup/Restore,
native upgrades or destructive removal. Their serialization, activation and
irreversible-effect guards remain. A conflict with such work waits or returns
its existing explicit conflict; this approval does not expand their cancellation
authority. Stopped/absent Services and explicit Release Group selection retain
their product rules.

There is no automatic database restore or migration reversal. Preserve healthy
serving workloads where possible and clean up only the affected execution's
owned effects. The earlier proposal to restore pinned configuration files during
recovery remains a separate unresolved decision; this ADR does not authorize it.

## Implementation boundaries

`internal/core/blueprintreconcile` owns pure change, conflict and handoff selection.
It accepts concrete resource fingerprints and execution facts, not Compose, etcd,
Agent messages or process handles. The Controller owns effective-input capture,
resource grouping and execution-plan preparation. Capability-owned persistence
atomically guards acceptance, dispatch, per-resource claims and acknowledgements.
The Agent executes only the current authorized unit and reports exact effects.

Applied input authority must follow the affected resources across all successful
writers, including explicit Releases, Entry updates and Attach changes. The
existing aggregate Environment projection is not that authority: ordinary Release
completion can leave its native inputs behind, while mixed Blueprint completion
can advance its desired fields for Services that did not execute. Neither that
aggregate nor an original Release's historical configuration may stand in for
the complete per-resource result. Capture sealed input identities when their
effects are acknowledged; retain separate uncertainty for unaccounted effects.

Keep the current serialization guard until that complete handoff is connected and
proved. A pure selector or a new cancellation reason alone is not implemented
latest-wins reconciliation. Do not add a permissive fallback for older records.
Preserve accepted Task and Release history; scope migration and source retention
to the records needed by current state and unfinished work.

This decision supersedes ADR 0051's requirement to bind each accepted Blueprint
to one prebuilt Environment-wide execution and ADR 0064's whole-Blueprint terminal
promotion model. Their bounded input storage, immutable execution evidence,
recovery proof and source-safety rules remain unless explicitly replaced here.

## Acceptance

Prove A-to-B-to-C supersession, invalid-input non-interference, pending/claim and
cancellation/completion races, same-input retry after failure, unrelated progress,
shared-file and dependency conflicts, and restart during handoff. Old execution
messages must not authorize effects after handoff or claim newer input applied.
Hash order must be deterministic; relevant dependency/file changes must select
all affected consumers while unrelated changes do not. Prove the behavior through
the existing Console, CLI and API and a real Controller/Agent before enabling it
on the QA journey. A healthy process or passing pure test is not that proof.
