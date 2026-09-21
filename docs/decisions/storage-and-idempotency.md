# Storage, idempotency, and durable journals

Status: Mixed. The named storage subsets below are accepted. The complete
general storage model remains Proposed where this document lists unresolved
choices; those recommendations must not be treated as product authority.

Groundplane uses one dedicated etcd instance for desired state, durable
operations, indexes, observations that require persistence, and replay
evidence. Stable ids are authority; mutable labels are lookup and presentation.
Every important mutation must have one transaction owner and durable evidence
for an uncertain outcome.

Product authority: [Hierarchy](../features/hierarchy.md),
[Tasks and logs](../features/tasks-and-logs.md),
[Secrets and Connectors](../features/secrets-and-connectors.md), and
[Blueprints](../features/blueprints.md).

## Etcd client boundary

Use the official etcd v3 Go client behind the repository's narrow `Store`
interface. The infrastructure package owns client construction, deadlines,
watch and snapshot translation, error mapping, and shutdown. Domain,
Controller, CLI, and public API packages never import etcd or gRPC types.

Ordinary persistence does not shell out to `etcdctl`, wrap a legacy import, or
pretend another key-value store is compatible. Snapshot restore is an explicit
operator procedure, not an automatic Store behavior. This keeps client
upgrades and transport dependencies behind one adapter and permits deterministic
in-memory Store implementations at real side-effect seams.

Current source: [Store boundary](../../internal/infra/etcd/store.go) and
[module dependencies](../../go.mod).

## Storage model and open decisions

The accepted storage direction is normalized flat primary records keyed by
stable id, plus transactional membership and uniqueness indexes. Ownership is
data and index state, not primary-key nesting. Projections such as Host,
Activity, backing-service, and runtime health are assembled rather than stored
as competing desired resources. Latest observations stay separate from desired
records.

Creates, edits, renames, deletes, indexes, and subordinate ciphertext are
committed with revision comparisons. Readers use fixed-revision ranges and
same-revision primary reads. Lists are stable-id ascending; cursors bind one
query and one etcd revision, and a compacted revision expires instead of
silently restarting. Watches begin after a captured snapshot revision and
re-snapshot on compaction or an uncertain gap.

Durable records have strict versioned codecs. Malformed durable state is an
internal error, never a validation error or an invitation to repair on read.
A breaking schema uses an explicit migration and clean cutover: there are no
dual writes, compatibility readers, aliases, or fallback decoders.

Accepted limits keep ordinary non-Blueprint mutations within one bounded etcd
transaction and one bounded record. Blueprint final publication uses its
separately accepted atomic envelope, while aggregate deletion may checkpoint
bounded batches under a tombstone. Destructive actions publish their Task and
tombstone atomically, keep the resource readable but mutation-fenced during
work, process owned descendants in stable order, and delete the parent last.

Task primaries, queue and assignment authority, monotonic events, stable event
deduplication, bounded retained events, terminal retention, and oldest-event
folding into compact checkpoints are accepted. Rendered Compose, generated
configuration, secret-bearing output, and execution plans that can be
reconstructed are not alternate desired-state stores.

The complete general layout is still **Proposed**. The following choices remain
unapproved unless another accepted feature decision has since taken ownership:

- exact storage publication, retention, and garbage collection for immutable
  submitted Blueprint bundle generations;
- remaining uniqueness rules and normalization for Secret references, Entry
  destinations, and Route collisions;
- the generic Secret storage shape beyond the accepted product ownership and
  project-before-platform fallback; and
- the generic resource-observation envelope, write authority, staleness, and
  retention beyond resource-specific accepted observation contracts.

Implementation may follow approved resource-specific decisions without
silently accepting these general recommendations. Current storage mechanics
and capability owners live under [etcd infrastructure](../../internal/infra/etcd).

## Canonical mutation intent

Every mutating human request is validated before idempotency evidence is
claimed. The handler constructs a typed canonical intent from the registered
method and route, resolved durable owner, ordered validated path values,
effective query values, content kind, and typed body. The canonicalizer never
accepts an `http.Request`, raw headers, a stream, arbitrary maps, or the
Idempotency-Key.

Version 1 is an immutable length-framed binary encoding. It distinguishes
types and absent/null/default semantics, orders object members, forbids floating
point input, and hashes the complete domain-separated value with SHA-256.
Equivalent JSON layout, query escaping, and multipart framing therefore do not
change intent. Blueprint intent uses the already validated closed-bundle
manifest and verified file digests rather than buffering or rehashing raw
multipart bytes.

The digest is sensitive because a secret-bearing request could otherwise
become an offline guessing oracle. It has no public text form and is cleared by
its owner. Before persistence, its version and bytes are sealed with the
Controller root age key. Comparisons decrypt only inside the protector and use
constant time; malformed evidence is internal corruption, while mismatch is
reserved for two valid unequal intents.

Current source: [canonical intent](../../internal/controller/idempotency/intent.go),
[protection](../../internal/controller/idempotency/protect.go), and
[secret envelope](../../internal/controller/secretvalue).

## Durable idempotency

Every human-API `POST`, `PUT`, `PATCH`, and `DELETE` requires one validated
Idempotency-Key. Console and CLI create one key per user intent and reuse it
only for transport retry. Synchronous validation and domain failures before
publication create no marker.

One owner-, method-, route-, and key-scoped marker is the serialization
authority. Claiming it and publishing the direct mutation or Task occurs in
the same etcd transaction. There is no check-then-mutate API. Resource
repositories contribute opaque plans; the coordinator adds protected evidence,
preflights the transaction envelope, and submits once.

An equal pending Task returns in-progress. An equal terminal marker replays
the exact original public status and bytes, including the original Task id. A
different valid intent returns mismatch. Unknown transaction outcomes may be
declared committed only when a linearizable marker read proves the same
protected intent; later resource state is never used to guess.

Task publication creates a pending marker. The same terminal transaction that
settles the Task also settles the marker and creates retention authority.
Terminal evidence remains for 90 days; pending evidence is never pruned. Marker
retention is pruned before a referenced Task, using fixed-revision reads and
compare-and-delete transactions so a reused key cannot be deleted by an old
collector pass. Secrets, exact response bytes, logical keys, and protected
comparison material never enter diagnostics, logs, metrics, traces, or Task
events.

Current source: [Controller coordinator](../../internal/controller/idempotency),
[marker persistence](../../internal/infra/etcd/idempotency), and
[marker pruning](../../internal/infra/etcd/idempotency_pruning.go).

## Hierarchy renames

Tenant, Project, and Environment slugs are mutable labels. Rename preserves the
stable id, owner, physical identity, display state, descendants, Blueprint
history, and every durable reference. Human paths are current projections; a
rename never scans or rewrites descendants.

Slugs are validated exactly and rejected rather than normalized. The
Controller owns a bounded reread/CAS loop so a concurrent edit can win without
having a later rename restore stale non-slug fields. Only state contention is
retryable. The repository transaction compares the primary, current and new
slug indexes, stable owner index, and deletion tombstone, then cuts directly
from old index to new. Rename-to-current validates the same authority but is a
no-op.

There is no old-slug alias, redirect, dual index, public etcd revision, or
compatibility lookup. A tombstone created before commit makes the rename fail
as in-use.

Current source: [hierarchy use cases](../../internal/controller/hierarchy) and
[rename transaction](../../internal/infra/etcd/hierarchymutations/rename_transaction.go).

## Reusable Secret persistence

Reusable Secrets belong to exactly one Project or the platform. Environment
values are Entries, not another Secret scope. Resolution checks a Project
Secret first and falls back to the platform only when the Project record is
absent; values never merge.

Listable metadata and encrypted value state are separate but transactionally
owned. Responses expose metadata and source kind, never ciphertext or direct
plaintext. CLI creation reads content from a file or stdin rather than an argv
value. Stable-id reads are scope checked, and key resolution uses one fixed
storage revision.

Deletion is an asynchronous Controller finalizer. Its tombstone hides the
Secret from reads and fallback while keeping all state recoverable until the
Task settles. Success removes metadata, indexes, ciphertext, and tombstone
atomically; failure restores visibility. Desired references do not by
themselves block deletion, but an exact recoverable execution pin does.

Current source: [Secret use cases](../../internal/controller/secrets),
[Secret reads](../../internal/infra/etcd/secrets), and
[Secret mutations](../../internal/infra/etcd/secretmutations).

## Task retention

Terminal Tasks, their durable events, and deduplication evidence have a 90-day
retention boundary. Cleanup is a restart-safe state machine because one Task
may own more subordinate keys than one accepted transaction can remove.

Admission proves the exact terminal retain-until index, absence of idempotency
authority, no conflicting retry, and matching immutable operation history. A
resource-specific transfer may permit pruning an old attempt during an active
retry only when every retry-shared ownership record is atomically transferred
to that exact new attempt.

The pruning intent removes owner/history indexes and attempt-scoped inputs,
then drains checkpoint, event, and deduplication prefixes in revision-fenced
batches. Empty phases require absence proof; malformed or cross-Task state is
corruption, not skippable debris. Plan-scoped inputs survive until the final
attempt that references them.

Assigned Agent Tasks also require exact immutable terminal-receipt and delivery
evidence. Awaiting, pending, or applied delivery is retained indefinitely;
only fixed-revision clean evidence is eligible. Pruning never infers delivery
from age, released domain ownership, or missing history.

Current source: [Task journal records](../../internal/infra/etcd/taskjournal),
[retention selection](../../internal/infra/etcd/task_retention.go), and
[phased pruning](../../internal/infra/etcd/task_pruning.go).

## Task ownership and views

Every Task freezes its operator-visible owner when created. Workspace,
Tenant, Project, and Environment ownership comes from the initiating capability,
not its execution target or a hierarchy lookup performed later. A retry copies
the source Task's owner tuple; a cascade keeps the initiating owner while
touching descendants.

The journal also freezes actor class and Controller-authored timestamps. The
single-operator MVP records `operator` or `system`, never a token, digest,
username, or inferred role.

Creation writes immutable workspace and, when applicable, Environment indexes.
Global, workspace, Project, and Environment views use the same Task primaries
at one fixed revision and stable-id order. Activity is an alias over that same
record set and cursor identity, not another journal. Historical Tasks stay in
their original scope after label changes or deletion.

There is no target- or Component-owned journal view, no compatibility backfill,
and no inference for pre-contract Task records. Current source:
[Task owner codec](../../internal/infra/etcd/taskjournal/owner.go) and
[Task handlers](../../internal/controller/handlers/task_routes.go).

Verification for these contracts must exercise behavior, failure paths,
generation, or architecture. Static source-text and declaration-shape tests are
not substitutes.
