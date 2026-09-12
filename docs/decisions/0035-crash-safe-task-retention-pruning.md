# Task retention and bounded pruning

## Purpose

Remove expired Task history without exceeding etcd transaction limits, losing
retry-shared inputs or deleting the evidence needed to finish Agent terminal
delivery. Terminal Tasks, events and event deduplication records have a 90-day
retention boundary. One Task can own 1,000 events and 1,000 dedupe records, so
cleanup cannot be one 96-operation transaction.

The [Task feature](../features/tasks-and-logs.md) owns operator behavior.
The [terminal-delivery contract](../features/backups/agent-protocol.md#terminal-delivery)
owns generic Agent receipts and delivery states. It replaces the former
Backup-only receipt design; there is no second receipt key or decoder.

## Admission and retry ownership

Every terminal transition atomically publishes a retain-until index keyed by
the exact timestamp and stable Task id. The daily scheduler prunes idempotency
markers first, then selects the oldest expired Task index. Pruning requires:

- terminal Task state and an exact retained timestamp/index match;
- absence of the original idempotency marker;
- no active retry; and
- matching Task primary and immutable operation-history index.

A resource-specific hook may allow an expired attempt to prune during an active
retry only by proving atomic transfer of every retry-shared ownership record
to that exact new attempt. Records must name the same operation and new Task,
the active marker must agree, and the old attempt must own no shared resource.
Environment deletion supplies this proof through its tombstone, operation lock,
cleanup intent and active marker. This exception prevents one expired attempt
from starving global retention without releasing a retry's authority.

## Restart-safe phases

The start transaction creates one private phase-aware pruning intent. Under
Task and relevant revision/absence fences it removes the retain-until index,
operation-history and owner indexes, Service lifecycle render input, and any
present Component/Route/Entry removal intents. It retains the Task primary as
the ownership anchor for Backup checkpoint cleanup.

The intent has completion booleans for checkpoint cursor, checkpoint dedupe and
Task-primary phases. Only event and event-dedupe phases carry remaining counts.
Every Task traverses both checkpoint phases; non-Backup prefixes are empty.

A nonempty checkpoint transaction compares the intent revision and each record's
ModRevision, removes at most 47 records and writes the next intent. An empty
prefix advances only under intent-revision and prefix-absence fences. Missing,
extra, malformed or cross-Task checkpoint records are corruption, not cleanup
candidates to skip. Both completed checkpoint phases and both absence fences
must hold when the public Task primary is deleted.

Events and Task dedupe records then drain in at-most-47-record CAS batches.
A zero count still requires prefix-empty proof. Each batch stays within the
96-operation ceiling. Persisted phases/cursors resume after Controller failure;
retry and pruning compare the same Task and active-operation fence, so only one
wins. The intent remains until every subordinate phase is complete.

## Generic terminal receipts

An assigned Agent Task's immutable receipt is published at the terminal Task's
exact modification revision. Native Controller Tasks and pending Abort without
an Agent assignment have none. Pruning must reconcile missing delivery state
against that exact receipt before admission; it cannot infer delivery from age,
released domain ownership, compacted history or a later reconstructed record.

Receipts in `awaiting_ack`, `pending` or `applied` persist indefinitely.
Only exact fixed-revision-validated `clean` delivery and receipt evidence is
eligible for retention. Capture and fence receipt authority in the private
pruning intent, retain it through subordinate cleanup and Task-primary removal,
and delete eligible receipt/clean evidence only when the final cleanup proof
holds. Failed transactions preserve resumption authority. Bulk receipt retention
is bounded to 24 validated pairs / 96 operations; never substitute the obsolete
Backup-only receipt or bypass terminal-delivery cleanup.

## Shared inputs and acceptance

Component candidates are attempt-scoped. Attach render inputs are plan-scoped
because retries retain `plan_id`; remove an Attach input only with the final
retained attempt, after proving no operation-history index remains. There is no
unbounded orphan collector or compatibility scan.

Prove all phases under restart, corruption, empty prefixes, concurrent Retry,
lost terminal delivery and transaction limits. Checkpointed Backup retry remains
a required feature behavior, not permission to infer a resume point from partial
history. Its implementation and qualification gaps are in
[Backups](../features/backups.md#current-status).
