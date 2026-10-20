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

Task and Activity lists show newest-created actions first. Scope, action, status
and resource-kind filters apply to the full journal. Cursor pages retain their
storage revision; refresh to obtain current membership. Task details use the same
fixed-revision rule for work items and attempts.

For CLI inspection, use `groundplane task show <id>`, then `task attempts`,
`task stages` or `task events` for the selected work. Task Show's `--limit`,
`--after` and `--revision` select its work page. Command help and
[API conventions](../api-cli.md) own the exact invocation and wire format.

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

Service and Environment log actions read current workload stdout/stderr through
the Controller, including provisioned Backing Service containers. Use the
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
