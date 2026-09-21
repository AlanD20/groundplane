# Desired-state publication

- Decision status: Accepted
- Scope: Environment desired state, Blueprint publication, direct desired
  mutations and latest-wins reconciliation

## One immutable desired document

Each Environment has one current desired head pointing to an immutable,
lossless normalized revision. That revision is the sole runtime-authoritative
desired document. Flat Service, Entry, Volume and other records may support
queries, but they are projections and never an alternate writer or render
input.

Every revision keeps two distinct streams:

- normalized desired input, containing the typed operator decisions needed to
  reproduce planning and rendering; and
- audit input, containing the accepted Blueprint bundle or the typed direct
  mutation that explains the change.

Execution reads normalized input. It never reparses an audit bundle, overlays a
mutable patch, or consults the latest head as a substitute for the revision it
was assigned. Generated paths, Docker names, labels, observations, runtime
intent and secret plaintext are not desired input.

## Publication is staged privately and exposed atomically

Blueprint HTTP bodies are streamed through bounded private scratch space. The
complete input is parsed and validated before durable publication. Normalized
and audit streams are written as immutable private chunks, verified, then
sealed behind one compact integrity root.

One final compare-and-swap makes the head, Task, idempotency result and all
bounded execution-adjacent authority visible together. Private staging is not
accepted product state and cannot serve public replay. Crash cleanup may remove
abandoned private state, but published revision data remains owned by its head,
Tasks and retention references.

The final publication builder has its own bounded transaction envelope because
all desired, Task and idempotency authority must become visible together.
Ordinary repository transactions retain their smaller general-purpose bound.
Overflow rejects the candidate before publication; required fences are never
omitted to fit an old accounting example.

## Direct mutations use the same head

Direct desired mutations load one fixed base revision, derive a complete new
normalized revision and publish it through the same head seam. They do not
rewrite an authored file, add an overlay or write a flat record as authority.

Service desired input and Controller-owned runtime intent remain separate typed
subrecords even when stored atomically together. Desired reconciliation
preserves runtime intent. Start, Stop and Destroy change runtime intent without
changing authored Service input.

Service create and edit publish desired state without implicitly deploying it.
Service removal is an immutable candidate until the pinned cleanup Task proves
success; failure retains the visible Service and current head. Removal does not
cascade through Routes, Attaches, Release Groups, dependencies, Entries,
Scripts or Volumes. Those references must be changed by their owning actions.

Entry metadata selects an immutable value generation. Secret literal bytes stay
out of listable desired metadata. A Task or retry resolves the exact selected
generation, not a later current value. Entry deletion removes its generations
only after the pinned materialization cleanup succeeds and no retained
execution reference still uses them.

## Latest accepted input wins execution

There is one current Blueprint, not a queue of complete Environment versions
that all must execute. Accepted publications receive an internal ordering token.
Planning compares resource-level effective inputs with successfully applied
inputs and groups inseparable effects into private execution units with exact
read, write and predecessor claims.

When newer valid input makes an older unit obsolete:

- an undispatched unit loses dispatch authority atomically;
- a running unit may start no further forward effects;
- conflicting new work waits until the old executor is proved stopped and its
  effects are accounted for; and
- unrelated units may continue.

Unknown effects retain their resource claims. A cancellation, timeout,
disconnect or expired process claim is not proof that the executor stopped.
Late messages may account for the old execution, but cannot publish it as the
latest desired state or authorize more old forward work.

One narrow exception allows forward repair after a superseded Blueprint unit
changed shared configuration: once that executor is proved stopped and its
exact effects are classified as accounted-but-diverged, the successor may plan
from those effects and the newest accepted input without first restoring the
predecessor. This exception does not apply to an ordinary failure, manual abort,
Deploy/Rollback, Release Groups, Backup/Restore, upgrades, destructive removal
or arbitrary Script execution.

The latest-wins design is accepted, but integration and live qualification are
not implied. Guarded or disabled execution remains incomplete until resource
claims, handoff, acknowledgement and recovery are connected end to end.

## Consequences

- A current desired read follows exactly one head; runtime and applied facts
  cannot make an absent desired resource reappear.
- A queued or retained Task keeps immutable input, even when it can no longer
  claim host mutation authority.
- Public Task history records partial success and supersession honestly;
  cancellation does not erase verified applied results.
- There is no compatibility reader or dual desired schema in the clean-start
  MVP.

## Source navigation

Revision storage is owned by
[`internal/infra/etcd/desiredrevision`](../../internal/infra/etcd/desiredrevision/repository.go),
head publication by
[`internal/infra/etcd/environment_desired_publication.go`](../../internal/infra/etcd/environment_desired_publication.go),
and Blueprint composition by
[`internal/controller/blueprint`](../../internal/controller/blueprint/apply.go).
The latest-wins selection model is in
[`internal/core/blueprintreconcile`](../../internal/core/blueprintreconcile/selection.go).
Service and Entry persistence boundaries are in
[`internal/infra/etcd/services`](../../internal/infra/etcd/services/record.go)
and [`internal/infra/etcd/entries`](../../internal/infra/etcd/entries/repository.go).
