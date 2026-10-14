# Tasks, Activity and logs

Tasks record asynchronous operations. Activity presents the same records in their
Platform, Tenant, Project or Environment scope. Workload logs are a separate,
transient stream.

## Follow an operation

A Task has stable identity, its initiating workspace, operator/system actor,
captured inputs, progress and an eventual completed, failed, timed-out or aborted
outcome. Rename or deletion does not move its history to a different owner.
Step completion is not Task completion.

A Blueprint Apply remains one visible Task. Its inspector lists the retained
resource executions with their own outcome, captured stages, health observations
and recovery state. These are not separate operator Tasks; their routes and
controls stay private. The list defaults to five executions per page, with
5/10/25/50 choices. CLI Task Show accepts `--execution-limit` and
`--execution-cursor` for the same pages. Page membership is revision-pinned;
each execution's recorded progress is refreshed while Apply runs. Older executions without
captured names remain unnamed; GP does not reconstruct them from current state.
Controller-only Entry settlement does not invent an Agent execution record.

The API and structured CLI response also expose `reconciliation_required`.
When true, terminal status does not prove that effects were restored or cleanup
settled: the affected resources may remain fenced. Keep the original Task and
follow its feature-specific recovery instructions. Retry is available only
where the Controller can prove it safe.

Every Console action that publishes a Task provides an **Open task** link as
soon as its ID is known. Top-right toasts follow accepted Tasks, and the bell
opens the notification center for operations started in the current session.
Dismissal hides a toast without losing its Task link; reloading the Console
clears session notifications, not recorded Activity.
The link opens that exact Task, not a filtered history list; it can be copied or
opened in another tab. The inspector refreshes pending/running status until the
Task finishes. A rejected request without a Task ID has no Task to open.
The inspector leads with the action, affected resource, status and execution
timing. New Tasks capture safe step descriptions when their execution plan is
sealed, so completing an operation or changing its resource cannot erase those
descriptions. Expand a step to see its purpose, selected target, timeout, attempts
and recorded transitions. Event timestamps show when the Controller received
the transition; they are not exact subprocess timing. The inspector shows saved
failure diagnostics and runtime evidence when present, not guessed outcomes.
It does not expose shell commands, credentials or stdout. Older Tasks without
captured descriptions are explicitly marked; their history is not rewritten.
Unavailable failure details are
identified explicitly, not inferred from a failed status. Exact IDs and plan
hashes remain available under Technical details.

Task details separate the original failure from recovery. The original outcome,
failed operation, safe diagnostic and recorded exit code remain visible while
GP restores or cleans up the captured resources. Recovery has its own progress,
deadline and last retained failed step. Finishing recovery steps means the
Controller is still waiting for final proof; only an accepted terminal result
is shown as **Recovery verified**. The original action does not become successful.
Passing a recovery deadline does not release protected resources or prove that
the Agent stopped. Missing causes and retired progress remain unavailable,
not reconstructed from current Service settings. API Task details and CLI Task
Show expose the same structured failure and recovery fields. This read-only
presentation is locally implemented; interruption/reload journeys are not yet
qualified.

Agent-executed steps can record a bounded failure cause, such as a missing file,
permission denial, timeout, unavailable image or failed process, with its exit
code when available. These details appear in the live timeline, expanded stage
and structured CLI/API response. The latest per-step state retains its diagnostic
when events are trimmed; an accepted final result retains the original cause
separately from recovery. A later attempt can replace a step's latest state.
GP does not copy arbitrary provider messages into these fields or infer a cause
from successful cleanup. Native Controller Tasks retain a classified execution
failure with their terminal outcome; duplicate acknowledgements must retain the
same cause. This includes image operations and software preparation. Native Tasks
do not fabricate Agent steps or exit codes. A Controller update preserves an
observed candidate failure across process handoff and reports it after predecessor
restoration; recovery errors cannot replace that cause. Agent updates save an
observed candidate failure before rollback preparation and retain it with the
restored generation. A later update cannot inherit it. Missing observations remain
unavailable, not inferred from a rollback.

Controller operations record named starts and outcomes in the same event stream.
These include Agent enrollment, image operations, connection checks, Runner
lifecycle, software preparation, software updates and etcd Apply. The inspector names the current
operation, failed operation and last retained success without inventing Agent
steps. Events use the active Task claim and the same sequence and retention rules
as Agent events. A progress-storage failure keeps the claim unresolved rather
than publishing an unrecorded outcome. A recorded operation is not proof of Task
completion. Software preparation records source retrieval, component builds,
registry publication and readback verification. Updates record preparation,
process handoff, candidate readiness and predecessor restoration when those
actions execute. The outgoing Controller records its wait for restart; the new
process records its own readiness checks. A failed candidate remains a failed
action even if restoration succeeds. These diagnostics have compiled locally;
their live qualification remains open.

Health waits record expected, observed, running and ready container counts,
including starting checks, unhealthy containers and exited containers. Missing
probe tools have a safe diagnostic; command output and command contents are not
recorded. The inspector shows the Agent's elapsed wait and time limit separately
from the Controller's receipt time. A minute without a new readiness report is
marked as an old update, not proof that the container or executor stopped.
The latest snapshot survives progress trimming and remains available through
Task details and the event stream. These observations explain the wait; they
do not replace the execution or recovery proof required for Task completion.
This diagnostic implementation has compiled locally but is not live-qualified.

Expand Configured readiness to see the check captured for each selected workload:
its type, expected replica count, authored timing and retries. HTTP/TCP checks can
show their loopback port; process patterns, commands and full URLs are withheld.
An omitted timing override does not assert an image or Docker default. The display
also says whether GP requires a healthy result or only running containers. These
details remain tied to the original Task even after Service settings change.

Task events resume by sequence. The latest 1,000 events are retained; each is
bounded to 32 KiB. Sequence numbers never reset when older events are trimmed.
An expired resume cursor needs a fresh snapshot. Trimming progress does not
discard the Task's failure or recovery evidence. Terminal history has a 90-day
retention boundary. Disposable events and attempt summaries have a separately
configurable diagnostic lifetime. Expiry leaves the immutable Task outcome,
first failure, captured execution/recovery authority and request replay intact.
Pending work or held recovery evidence is not eligible. The inspector identifies
expired diagnostics instead of presenting missing events as unfinished steps.

Task details also retain summaries of earlier attempts after their detailed events
are trimmed. These show the captured action, last recorded outcome, safe failure
cause and Controller receipt times when available. A missing outcome is not
success. The summary history is bounded to 64 KiB and at most 1,000 summaries;
the inspector reports how many older summaries were discarded. It retires with
the Task and cannot authorize Retry or prove restoration. The Console inspector,
API Task details and structured CLI Task Show share this locally implemented
history. Individually compressed summaries avoid rewriting the entire archive
for each trimmed event; history remains a diagnostic projection, not execution
authority. Trimming/restart behavior is not yet live-qualified.

Activity and Task lists show newest-created Tasks first (descending Task ID),
with the same scope filters and fixed-revision cursor semantics on every page.
Status, operation type and resource-kind filters search the full recorded
journal, not just the current page. Continuation cursors bind the exact filters.
Components links to the same Activity view filtered to platform Component work;
it does not maintain a second Platform Task list. Exact execution-target filtering
is not available.
Console pages default to five Tasks, with 5/10/25/50 choices.

## Retry

Retry creates a new attempt of the same operation with the captured inputs.
It does not silently apply later configuration. Pending, running and completed
attempts cannot ordinarily be retried; only one retry may be active.

An unsettled recovery can offer **Retry recovery** after automatic execution
stops. Deterministic or unsafe-to-repeat failures stop immediately. Only safe
captured restoration after a classified transient dependency failure can retry
automatically, with one- and five-second backoff and at most three failed
executions inside its existing deadline. Report redelivery does not count as a
new failure. The stopped state survives restarts, preserves resource ownership
and releases the execution slot for unrelated work.

Explicit Retry authorizes another bounded recovery attempt in that same Task,
not its forward actions or newer desired state. Its result and first failure
remain unchanged until exact restoration proof permits settlement. A lost Retry
response replays the same authorization. Local implementation is not installed
failure-path qualification.

A running Blueprint Apply can instead offer **Retry captured Detach hook** when
a Custom Detach hook started but no result was saved, and its recovery attempt
has durably stopped. This authorizes one execution of the original command and
inputs in the same hidden child Task. The command may already have changed the
backing service: the operator must decide whether repeating it is safe. If
several hooks are waiting, Retry selects the oldest eligible child, one at a time.
Saved results and completed steps are not repeated. Lost responses replay the
same authorization, not another command execution. Cleanup preserves the original
failure and the Apply still fails after its children settle; a new Apply is
required to try the desired state again. This path is locally implemented and
compiled, not yet behavior-qualified.

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
