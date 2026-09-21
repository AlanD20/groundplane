# Tasks, Activity and logs

Tasks record asynchronous operations. Activity presents the same records in their
Platform, Tenant, Project or Environment scope. Workload logs are a separate,
transient stream.

## Follow an operation

A Task has stable identity, its initiating workspace, operator/system actor,
captured inputs, progress and an eventual completed, failed, timed-out or aborted
outcome. Rename or deletion does not move its history to a different owner.
Step completion is not Task completion.

Task events resume by sequence. The latest 1,000 events are retained; each is
bounded to 32 KiB. Sequence numbers never reset when older events are trimmed.
An expired resume cursor needs a fresh snapshot. Trimming progress does not
discard the Task's failure or recovery evidence. Terminal history has a 90-day
retention boundary.

Activity and Task lists use the same ordering, scope filters and cursor semantics.
There is no general Component or execution-target Task filter.

## Retry

Retry creates a new attempt of the same operation with the captured inputs.
It does not silently apply later configuration. Pending, running and completed
attempts cannot be retried; only one retry may be active.

Feature-specific restrictions still apply. For example, a Runner retry needs a
fresh token, old-key Restore cannot recover its request-only identity, and
uncertain Script effects cannot be blindly repeated. See the relevant guide.

## Abort

Abort addresses the existing Task; it creates no second Task.
Pending work is terminalized before assignment. Running work must acknowledge
cancellation and durable terminal publication. A lost client connection does not
prove that work stopped.

Already-aborted work returns the same success. If completion or another terminal
outcome wins, Abort returns `task.not_abortable`. Offline or stale-Agent delivery
fails closed. Native update activation has its own non-abortable boundary.

Controller shutdown is not operator Abort: durable running claims remain available
for restart recovery. Abort never implies that a database write or migration
was undone.

## Logs

Service and Environment log actions read current workload stdout/stderr through
the Controller. Use the tail/follow controls and stop the stream when finished.
These logs are not persisted as Task events and cannot be resumed from a durable
cursor. An empty stream may mean the selected workload has emitted no output.

The stream is bounded and tied to the client request. It selects its sources at
setup rather than silently switching to new containers after a deployment.
Failure disconnects the stream; reconnect starts a new request.
Lines are UTF-8 normalized and truncated at 32 KiB.

GP does not add its own Secret values or generated env files to log frames,
but it does not redact arbitrary workload output. Applications must not print
credentials. Script stdout/stderr is discarded, not exposed by this log feature.

## Recovery and supersession

A durable report conflict may leave an Agent connected while its assignment
remains restricted. Ready capacity does not erase that assignment or prove
recovery. Follow the Task's failure and resource evidence rather than repeatedly
retrying or resetting history.

[Latest-wins Blueprint reconciliation](blueprints.md) has a separate accepted,
unfinished supersession contract. It does not add an Abort endpoint or make
obsolete work eligible for ordinary Retry.

## Design and qualification

See [Task execution and event delivery](../decisions/task-execution-and-event-delivery.md)
and [technical decisions](../decisions/README.md) for report, replay and transient
transport boundaries. [Capability status](../capabilities.md) owns qualification
limits; historical Task success is not current application health.
