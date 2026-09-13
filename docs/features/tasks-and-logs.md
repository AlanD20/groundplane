# Tasks, Activity and logs

## Purpose and scope

Give operators one durable record for asynchronous work, progress, Retry and
Abort, plus transient Service and Environment logs. A log stream is not Task
history, and a step's terminal event is not the Task's terminal outcome.
The product model and exact actions remain in [mvp.md](../mvp.md) and
[api-cli.md](../api-cli.md).

## Functional requirements

- Tasks retain stable identity, immutable workspace ownership, Controller-authored
  timestamps and the closed actor values `operator` or `system`. Ownership comes
  from the initiating capability, never the executor or a later hierarchy lookup.
- Retry creates a new attempt with the source operation's frozen owner and
  feature-defined immutable inputs. It does not silently rebase work. A feature
  may reject Retry when it cannot satisfy its recovery contract.
- Activity and Tasks expose the same records, ordering, scope filters, page
  boundaries and cursor identity. Historical Tasks do not move after a rename
  or deletion. The MVP has no general execution-target or Component filter.
- Task progress resumes by immutable event sequence. Service and Environment
  logs are non-resumable live streams, not durable event payloads.
- Resource-specific terminal publication remains authoritative for side effects,
  locks and recovery. A disconnected client or cancelled subprocess is not
  proof that an external mutation had no effect.

### Abort

Abort targets the selected Task and creates no other Task or operation. The
response is `202 {"task_id":"<target Task id>"}`. This preserves one progress
resource and avoids placing cancellation behind the work it must cancel.

Pending work is atomically terminalized as `aborted` before assignment. Running
Agent work receives `TaskAbort{task_id, reason:"operator_requested"}` for the
exact durable Agent generation. The request subscribes before delivery and
returns only after acknowledged terminal publication. Running native Controller
work cancels its exact execution context and also waits for terminal publication.
Controller shutdown is different: it preserves the durable running claim for
restart recovery.

An already aborted Task returns the same success. Completed, failed or timed-out
Tasks return `task.not_abortable` (409), including natural completion that wins
a race with Abort. The standard `Idempotency-Key` remains required; there is no
body or separate abort marker because the addressed Task's durable terminal state
owns replay. Resource-specific activation and recovery fences still apply.

Corrupt ownership is an Internal error, not an exception to cleanup. Offline or
generation-mismatched Agent delivery fails closed; the durable timeout may still
finish the Task and the operator may retry the Abort request.

### Blueprint supersession

[Latest-wins reconciliation](../decisions/0078-latest-wins-blueprint-reconciliation.md)
adds Controller-requested cancellation of obsolete Blueprint execution units.
It reuses the original public Apply Task and existing terminal states. Queued
units lose dispatch eligibility atomically; running units retain their claims
until cancellation and effects settle. Unrelated units continue. Superseded work
records the replacement Task and cannot be revived by Retry. This is not a new
Abort endpoint or permission to interrupt Backup, native activation, destructive
removal or arbitrary Script effects.

If a Blueprint unit eligible for automatic supersession changed shared
configuration, its existing Task detail and events must distinguish
accounted-but-diverged effects from unknown effects and verified applied success.
After proven executor stop and exact effect accounting, the replacement Task may
repair forward under
[ADR 0078](../decisions/0078-latest-wins-blueprint-reconciliation.md#forward-repair-after-a-superseded-shared-configuration-write);
the old Task still records its supersession reason, replacement Task and actual
partial outcome. It is never rewritten as successful or reopened. Service reads
continue to expose the actual observation, including `unavailable` or `degraded`
where applicable, while the successor has not restored health. Manual Abort and
non-Blueprint Tasks gain no such handoff or new public Task state.

## Non-functional requirements

Each Task has at most 1,000 durable events, each at most 32 KiB of JSON. Events
contain closed progress metadata, not subprocess output, secret values or raw
Agent diagnostics. Terminal Tasks and their subordinate records have a 90-day
retention boundary with bounded, restart-safe cleanup. Exact storage bounds and
error behavior are owned by the detailed contracts below.

All stream goroutines and subscriptions belong to the caller's context. A failed
live stream disconnects safely; it cannot invent durable progress or retry
non-compaction storage failures forever.

## Technical design

### Agent report failures

The Agent's closed Component failure diagnostics map to the existing durable
Task classes: configuration rejection to `config_rejected`, activation failure
to `compose_failed`. They must never become `none` or an unknown wire value.

After report validation, a durable publication conflict for the exact assignment
delivered on the current session quarantines that assignment for that session.
The Controller logs the reason and keeps the Agent connection alive. It does not
record terminal success, release ownership or repeatedly redispatch the same
conflicting report. A new session can resume the existing recovery authority.
Authentication, malformed reports, stale authority and other protocol failures
remain strict connection errors.

Ready reports describe actual worker capacity; they do not erase durable claims.
Unrelated work can proceed only within both limits. With concurrency one, an
unresolved assignment still prevents a new claim even though the Agent is online.

| Concern | Current technical contract |
| --- | --- |
| Atomic mutation replay and operation ownership | [Durable idempotency](../decisions/0021-durable-mutation-idempotency.md) |
| Fixed-revision Activity scopes and pagination | [Task ownership](../decisions/0039-task-ownership-projection.md) |
| Event sequence, assignment epochs, snapshot/watch handoff and terminal drain | [Task event streaming](../decisions/0023-task-event-sse-resume.md) |
| Retention phases, retry-shared inputs and transaction bounds | [Task pruning](../decisions/0035-crash-safe-task-retention-pruning.md) |
| Transient log selection, transport, bounds and errors | [Service and Environment logs](../decisions/0059-transient-log-sse.md) |

These are maintained technical contracts, not an instruction to read every
decision. Select the one affected by the task. Feature-specific terminal
receipts, checkpoints and restoration belong to that feature's design.

## Acceptance

Prove exact protected replay; immutable ownership through rename/delete/retry;
identical Activity and Task pagination; sequence-safe stream reconnect and
compaction; terminal drain; context-bound shutdown; durable Abort under races;
and restart-safe retention without orphaned shared inputs. Each feature must also
prove its own terminalization, failure, Retry and unknown-outcome behavior.
No successful Abort may claim an external effect was undone without that proof.

## Current status

Tasks, Activity, retention, Retry and Abort have recorded qualification.
Feature-owned recovery and Task producers still have gaps, especially Backups,
Scripts and release orchestration. See [capabilities.md](../capabilities.md) and
[the current task list](../../tasks/todo.md). No new runtime verification was
performed by the documentation migration.
