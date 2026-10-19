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

Metadata-only actions publish a new immutable input while retaining the exact
Compose artifact and render generation. They stage immutable content privately,
then atomically publish the resource change, desired head and replay result.
Advancing desired metadata is not a Deploy and creates no extra execution Task.
The storage owner is `internal/infra/etcd/desiredauthoring`; each resource owner
supplies its typed mutation and concurrency guards. First-use Zone and Entry
actions initialize a durable empty projection under the Environment fence,
rather than inventing an in-memory fallback.

Service desired input and Controller-owned runtime intent remain separate typed
subrecords even when stored atomically together. Desired reconciliation
preserves runtime intent. Start, Stop and Destroy change runtime intent without
changing authored Service input.

Service create and edit publish desired state without implicitly deploying it.
Service removal is an immutable candidate until the pinned cleanup Task proves
success; failure retains the visible Service and current head. Removal does not
cascade through Routes, Attaches, Release Groups, dependencies, Entries,
Scripts or Volumes. Those references must be changed by their owning actions.

Omitting a Backing Zone with external Attach consumers is rejected until its
separate impact-approved Remove action completes. The Blueprint's own
Environment revision cannot authorize effects in other Environments.

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
An omitted owned resource becomes a removal unit; only acknowledged absence
satisfies it. Merely dropping it from the desired document cannot prove cleanup.

One operator-visible Blueprint Apply Task owns those units as typed work items.
Host-effect work executes on its captured Agent and host through the shared
scheduler, durable checkpoints and operation-specific recovery. An Entry value derived and encrypted by
the Controller has no Agent effect before Deploy; it settles through an atomic
Controller write of its exact value generation and unit receipt, fenced by the
parent, current head and unit epoch. It does not create a no-op Agent Task or
restart a running Service. A stopped or absent Service also has no host effect
from a definition edit: the Controller can acknowledge that unit only while
the exact non-running runtime-intent record remains unchanged. A later Start
uses the current desired Service definition. A Route unit can similarly be
acknowledged against its exact sealed projection; the HTTP-router Component
owns any host configuration that consumes it. Script definitions settle as
Controller metadata before their Service can deploy; execution remains a hook
within that Service's work item. Publication flips the complete active Script
set with the desired head. Older executions retain their pinned set and bodies,
including definitions omitted by the new input; omission never deletes their
recovery authority. Work items are not separate operator actions: Task lists
contain one Apply, whose details expose its grouped work items and attempts.
The parent remains nonterminal while any required unit is running or has
unknown effects. One work item's terminal status alone cannot mark the Task
successful or release its resource claims.

Apply uses the Controller's configured release execution timeout. Each work
item receives only the Task's remaining forward-work budget; it does not
restart the whole timeout for each resource.

The Task's absolute deadline is captured at publication, not renewed by a
Controller process restart or a new work attempt. Expiry atomically closes the same
selection gate as Abort. The first durable stop outcome wins: deadline expiry
does not overwrite an earlier Abort or proved work failure. Pending work
is withdrawn and running attempts are asked to stop; the Task becomes timed
out only after exact settlement proves that no unaccounted effects remain.
Recovery may therefore outlive the forward-execution deadline without granting
more forward work.

Some effective inputs depend on facts from a Custom backing Attach created by
the same Apply. Seal the operator's authored decisions at publication, then
build dependent runtime files only after that Attach's hook has completed and
its exact output has been captured. The parent's durable unit plan may expand
while waiting for those facts and must be sealed before successful completion.
Reconciliation fences each expansion against the current head and planning
epoch. A failed or superseded hook cannot be replaced with facts from another
revision, and an empty unsealed plan is never evidence of success.
All Service work waits for one complete Environment runtime projection while
any Custom Attach fact needed by that projection is pending. Independent Zone
and Volume units may proceed. This keeps Service renders on one sealed input
instead of introducing per-Service snapshots with separate fact lifetimes.
That projection includes the complete shared-network membership of its ready,
acknowledged Attaches. Its seal fences the exact Attach record revisions; a
completed hook without the consumer's network binding is not a complete Apply.

An Attach that can produce facts has one immutable, encrypted input generation
prepared before admission. The Apply owns it until its Attach work is published;
publication transfers ownership to that work item atomically.
Secret values and hook inputs remain protected by exact-generation pins through
retry and recovery. A successful hook writes a create-only output generation;
the mutable current Attach facts record is not replay authority. Dependent
Entries and the Environment runtime projection select that exact output
generation. Neither parent completion nor Task pruning may delete a generation
while an Attach, Entry, runtime projection or recoverable Task still uses it.
Superseded or failed work releases generations only after those references and
the executor's effects have been accounted for.

An exactly restored failed work item closes the Apply's publication gate and
eventually fails the visible Apply after other work settles. The coordinator
does not retry it automatically: a new Apply is a new operator decision. An
operator Abort already committed before that receipt retains its Aborted result.

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
