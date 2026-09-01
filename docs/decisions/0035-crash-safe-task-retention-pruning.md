# ADR 0035: Crash-safe Task retention pruning

- Status: Accepted
- Date: 2026-08-22
- Partially superseded: 2026-08-30 by ADR 0048, only for the Backup-only
  terminal-receipt key, record, replay, and pruning clauses; the generic Task
  retention/pruning state machine and bounds remain in force

## Context

ADR 0013 retains terminal Tasks, events, and event-deduplication records for
90 days and permits bounded deletion to checkpoint stable-id batches under a
tombstone. ADR 0021 requires the Task's idempotency marker to be deleted in an
earlier transaction. One Task can own 1,000 events and 1,000 dedupe records,
so deleting every record atomically cannot fit the 96-operation transaction
ceiling.

Task attempts also own private retry inputs. Component candidate intents are
attempt-scoped. Attach render inputs are plan-scoped and shared by every retry
attempt because retries preserve `plan_id`.

Backup attempts additionally own checkpoint cursor and checkpoint-dedupe
records. Those records remain subordinate to the public Task primary, so that
primary cannot disappear before their bounded cleanup authority is durable and
their cleanup has completed.

Every terminal Backup and Backup-prune Task also owns the sole immutable replay
receipt defined by ADR 0024 at
`/v1/runtime/backup-terminal-receipts/{task_id}`. It shares the terminal Task's
modification revision and must remain available after compactable history and
released Backup domain ownership no longer can prove the original atomic
outcome.

## Decision

Every terminal transition atomically publishes a retain-until index keyed by
the Task's exact `retain_until` timestamp and stable Task id. The daily
scheduler prunes idempotency markers first and then invokes Task pruning.

Task pruning processes the oldest expired index and starts only when:

- the Task is terminal and its retained timestamp matches the index exactly;
- the Task's original idempotency marker is absent;
- the operation has no active retry; and
- the Task primary and immutable operation-history index still match.

The no-active-retry rule has one narrow resource-specific exception. A pruning
hook may prove that retry atomically transferred every retry-shared ownership
record to the exact active retry: the records identify the same operation and
the same new Task owner, and the active-operation marker identifies that new
owner exactly. The expired candidate must own no retry-shared resource, while
all shared authority remains protected by the operation and new owner. Only
then may the superseded attempt prune while its retry is active, preventing an
expired retained head from starving the global retention index. Environment
deletion is the accepted use: its tombstone, operation lock, cleanup intent,
and active-operation marker provide the complete atomic transfer proof.

The start transaction creates one private, phase-aware Task-pruning intent and
removes the expired retain-until index, immutable operation-history index,
owner indexes, service lifecycle render input, and any present
Component/Route/Entry removal intents under the task and relevant
revision/absence fences. It does not delete the Task primary or its Backup
checkpoint records. Every intent carries completion booleans for the
checkpoint cursor, checkpoint dedupe, and Task-primary phases; only event and
event-dedupe records carry remaining counts.

For a Backup or Backup-prune Task, that start transaction also requires the
receipt to exist at the Task's exact modification revision, validates its
complete Task binding and strictly older prior Task revision, compares that
receipt revision, and copies it into the private pruning intent. An ordinary
Task has no receipt. Missing, torn, malformed, or later reconstructed Backup
receipt evidence is corruption and prevents pruning from starting. The start
transaction retains the receipt; no checkpoint, Task-primary, event, or dedupe
phase may remove it.

Every Task traverses the cursor and dedupe checkpoint phases. For a Backup
Task, each nonempty bounded cleanup transaction compares the private intent
revision and every record's ModRevision, deletes at most 47 checkpoint
records, and writes the next intent. When a checkpoint prefix is empty, the
phase transition compares the private intent revision and the prefix-absence
fence, then advances its completion boolean. Missing, extra, malformed, or
cross-Task checkpoint records are corruption and stop cleanup. Non-Backup
Tasks take the same two phases with empty prefixes. The public Task remains
visible throughout this checkpoint work as the records' ownership anchor.

After both checkpoint booleans reach true, one fenced phase transition sets
Task-primary deletion and removes the public Task primary only when its
revision and both checkpoint-prefix absence fences still match. The start
transaction has already removed the retention, history, owner, and present
Component/Route/Entry intent records. Event and Task-dedupe counts then drain
in the same at-most-47-record compare-and-swap batches; when a count is zero,
prefix-empty verification still runs without a subordinate batch. Each
bounded batch stays within the 96-operation transaction ceiling. The
persisted phase and cursor resume after Controller failure, and the intent is
deleted only after every phase reaches zero. A concurrent retry and prune
compare the same Task and active-operation fence, so exactly one can win.

After the checkpoint phases, Task-primary deletion, event drain, and event-
deduplication drain are all complete and their empty-prefix checks have passed,
the final fenced transaction compares the private intent and the captured
receipt revision, then deletes the receipt and pruning intent together. A
failure leaves both records for exact resumption. Thus the receipt outlives the
public Task during subordinate cleanup but cannot become an unowned durable
authority after pruning completes.

An Attach render input is deleted with the final retained attempt for its
operation. Earlier attempt pruning retains the shared plan input while any
other operation-history index remains. No general compatibility scan or
unbounded orphan collector is introduced for the clean-start MVP.

## Consequences

Task history and its Backup terminal receipt disappear at the accepted 90-day
boundary without exposing a partial journal or orphaning Backup checkpoint or
replay evidence. Cleanup resumes after Controller failure from the private
phase-and-cursor intent and remains within etcd transaction limits. Component
candidate intents do not leak beyond their Task, while retry-shared Attach
plans remain available until their final retained attempt expires. Backup retry
is currently fail-closed while retained checkpoint state exists; this is an
implementation gap against ADR 0024's accepted checkpointed-retry behavior,
not a new retention decision. Once implemented, retry must resume from the
durable checkpoint state rather than infer from partial history.
