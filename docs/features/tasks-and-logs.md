# Tasks, Activity and logs

Tasks record asynchronous actions. Activity shows the same records in their
Platform, Tenant, Project or Environment scope. Workload logs are separate.

The [native execution engine](../decisions/task-execution-redesign.md) uses the
Task, work-item and attempt model below. Selected installed journeys are
qualified; full recovery and failure-boundary qualification remains incomplete.
See [current limits](../capabilities.md) and the [QA matrix](../qa-matrix.md).

## Follow an operation

One Task represents one accepted action. It contains resource work items and
their execution attempts, not hidden operator Tasks. A Blueprint Apply can run
independent resources in parallel while dependent work waits. Controller-only
changes do not invent Agent executions.

Open the exact Task from its action toast, notification or Activity row. The
inspector shows the requested action, resource, status and recorded work counts.
Each resource shows its current blocker, named stages and recorded results.
When executable input cannot be prepared, waiting work also shows the last safe
preparation issue and its code. Repeated identical issues do not create more
writes. The issue clears after successful preparation; it is not a host execution
attempt, a failed effect or an attention-state transition.
Expand a stage for expectations, timing and a safe diagnostic code. Latest
attempts are visible first; earlier attempts remain inspectable. Filtering a
work page filters that page, not unseen resources.

A failed action and its cleanup are separate. Successful cleanup does not turn
the original action into success. The original failed attempt remains linked
while later attempts run. Missing or expired details are explicitly unavailable;
the Console never reconstructs a historical result from today's Service settings.
Raw events and technical identities are secondary disclosures. Retained events
can be searched, filtered and copied without exposing captured Secret inputs or
arbitrary subprocess output.

Stage timestamps describe recorded observations. Assignment and Start permission
are not measurements of process startup. A disconnected Agent, lost response or
quiet event stream does not prove that execution stopped.

Checkpoint publication retries bounded transaction contention against fresh
execution state. A refused native checkpoint stops its operation without closing
the shared Agent connection. When diagnostic storage remains available, the
attempt shows a failed **Save execution checkpoint** step with the public reason
and recovery guidance. This reports a missing acknowledgement, not proof that a
host action failed or was undone. Controller logs include its Task, work item and
attempt identifiers; private inputs and raw command output remain excluded.

Task and Activity lists show newest-created actions first. Scope, action, status
and resource-kind filters apply to the full journal. Cursor pages retain their
storage revision; refresh to obtain current membership. Task details use the same
fixed-revision rule for work items and attempts.

For CLI inspection, use `groundplane task show <id>`, then `task attempts`,
`task stages` or `task events` for the selected work. Task Show's `--limit`,
`--after` and `--revision` select its work page. Command help and
[API conventions](../api-cli.md) own the exact invocation and wire format.

## Run after whole Tasks

Task-producing action review forms separate **Execution timing** from action
configuration. **Start immediately** is the default; **Wait for Tasks** opens
the **Run after** picker. The picker lists only
unfinished Tasks, including queued, waiting and running work, with server-side
pagination and the configured page size (5, 10, 25 or 50). Search applies to the
current page. Selected prerequisites remain visible separately when changing
pages or after completion. `task list --status active` and
`GET /api/v1/tasks?status=active` expose the same unfinished-Task filter.
Select unfinished Tasks or add an unfinished Task ID, up to 16. These are whole-Task
prerequisites, separate from dependencies between operations inside one Task.
Synchronous metadata saves and Retry of an existing Task do not create a new
dependency list.

The Controller accepts the requested action once and returns its Task ID.
While prerequisites are pending, it does not capture mutable execution inputs,
claim resources or consume an execution slot. Preparation starts only after all
prerequisites succeed and fully settle. A prerequisite failure or cancellation
prevents the dependent action from running; inspect the linked Task for its
outcome. The prerequisite list is immutable and participates in request replay
identity.

Task details show the prerequisite links, whole-Task waiting state and any failed
prerequisite. Opening Task inspection preserves the current request draft. For a
queued Release Group rollback, sources are selected after prerequisites finish;
the Console does not submit a preview captured before them. An explicitly
provided API preview revision remains an operator precondition.

Use repeatable `--after TASK_ID` with the matching CLI action, or include
`"after": ["task_…"]` in its JSON request. For example:

```sh
groundplane service deploy api --after task_01J00000000000000000000000
groundplane image fetch nginx:alpine --after task_01J00000000000000000000000
```

Acceptance identifies the Task, not a Release or an image digest that has not
yet been selected. Read the Task and resulting resources after it succeeds.
The CLI and Console submit once; neither client implements dependency ordering
by polling and resubmitting requests.

Simple actions may wait for prerequisites or preparation for up to 24 hours.
Once captured, each operation keeps its usual execution timeout, bounded by the
Task's deadline; Service and Group releases retain their configured whole-Task
deadline. The inspector shows a temporary preparation blocker and its deadline.
Expiry or a stopped prerequisite ends the dependent with a reason and predecessor
links. It does not leave an executor occupied while waiting.

## Discord Task alerts

Open Task alerts in Environment settings or Platform settings. Each destination
is independent: Platform receives only Tasks without an Environment scope.
Enable Discord notifications, enter a Discord webhook URL and Save alerts.
The saved URL is encrypted and never returned. Leave its replacement field blank
to keep it; Remove webhook disables notifications when saved. An optional Console
origin adds a link to the exact Task.

Send test queues a notification using the saved destination. Last delivery shows
the result, time and relevant Task; a queued test is not a completed delivery.
CLI equivalents are `groundplane task-alert show|set|test --platform`, or use the
normal Environment scope. `set --webhook-url-file` reads a credential from a file
or standard input; `--enabled=true` enables the destination. The API uses
`GET/PUT /task-alerts` and `POST /task-alerts/test`, selecting `platform=true` or
`environment=<id>`. Writes require an idempotency key and the current settings
revision.

New failed and needs-attention transitions enqueue one notification atomically
with their Task update. Repeated reports of the same state do not enqueue again;
a later retry that fails can produce another transition. Existing history is not
backfilled. Notifications contain action, target, scope, Task identity and time,
not captured inputs, Secrets or subprocess output.

The Controller keeps pending delivery across restarts and makes at most five
attempts, respecting rate-limit delays. Permanent webhook rejection stops retrying;
delivery failure appears in settings and does not fail or block the original Task.
Changing or disabling the destination cancels its older queued notifications.
If Discord accepts a message but its reply is lost, a retry can deliver a duplicate.
Deleting an Environment removes its alert settings. This is Task alerting, not
availability or resource monitoring.

## Retry and cancellation

Retry selects a work item and its exact recorded attempt. It retains the Task
identity and captured inputs; newer desired state requires a new action.
The Controller supplies whether Retry is available and why it is unavailable.
The request rechecks that decision atomically. Repeating a lost request with
the same idempotency key does not authorize another execution.

When cleanup needs attention, Retry cleanup continues only captured recovery.
It does not resume the obsolete forward action or reread newer desired state.
Operation-specific retry limits still apply. An interrupted Custom hook with
unknown effects is not automatically rerun; an explicit Retry must authorize
repeating its captured command when the operation permits it.

Cancellation addresses the existing Task. It revokes future forward permission;
already-authorized effects must be accounted for before resource protection is
released. Completion can win the race. A cancellation response, timeout or
Controller restart does not prove host termination or reverse a database change.
Software activation has its own non-cancellable boundary.

A newer Blueprint can supersede obsolete work at safe boundaries. Already-started
effects remain protected until reconciled. Unrelated resources can keep making
progress; the failed Apply is not retried automatically after cleanup.
See [Blueprint reconciliation](blueprints.md).

## Diagnostic retention

When etcd approaches its quota, GP pauses new operator changes and scheduled
backups with `storage.unavailable` (HTTP 503). Reads, Agent reports, accepted work,
Abort/Retry and maintenance remain available. Let retention, compaction and
defragmentation reclaim space before retrying. This gate does not raise the quota
or guarantee that an already-full backend can persist recovery.

Disposable stage details and events have their own bounded retention, independent
of Task history, captured inputs, applied resource state and recovery checkpoints.
Task history has a 90-day retention boundary. Active or unresolved effects retain
their operation-owned protection even when display details expire.

The event stream resumes by its Task-event sequence. Reconnect can redeliver
observations, so consumers deduplicate by event ID. An expired cursor requires
a fresh Task snapshot. A diagnostic-storage failure is reported as incomplete
details, never as a successful stage. The authoritative Task outcome, not a quiet
stream or completed-stage count, determines whether the action finished.

## Logs

The Console keeps the newest 1,000 received lines in a rolling buffer. Incoming
lines replace the oldest ones; reaching the limit does not stop live streaming.
The counters distinguish received, retained and filtered lines. Search and copy
use all retained matching lines, while only rows near the viewport are rendered.
Scroll away from the bottom to read older output; **Jump to latest** resumes
following the end. Pause stops the current stream; Resume / reload starts a new
read using the selected per-container tail.

Service and Environment log actions read workload stdout/stderr through the
Controller, including provisioned Backing Service containers. During deployment,
logs include the latest published candidate as well as the serving Release, so
first-start failures are visible before the candidate becomes healthy. Use the
tail/follow controls and stop the stream when finished.
These logs are not persisted as Task events and cannot be resumed from a durable
cursor. An empty stream may mean the selected workload has emitted no output.

The stream is bounded and tied to the client request. It selects its sources at
setup rather than silently switching to new containers after a deployment.
Failure disconnects the stream; reconnect starts a new request.
Lines are UTF-8 normalized and truncated at 32 KiB.

GP does not add its own Secret values or generated env files to log frames,
but it does not redact arbitrary workload output. Applications must not print
credentials. Script stdout/stderr is discarded, not exposed by this log feature.

## Design and qualification

[Execution design](../decisions/task-execution-redesign.md) explains scheduling,
typed operation ownership and cutover. [Capability status](../capabilities.md)
owns qualification limits. Historical Task success is not current workload health.
