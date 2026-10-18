# Task execution and event delivery

This document describes the existing mechanism. The
[accepted replacement](task-execution-redesign.md) governs the next execution
architecture and breaking cutover; it is not implemented. Existing typed-authority,
secret-handling and unknown-effect safeguards remain required. Mechanism-specific
sections here will be replaced during implementation, not retained as a second
execution path.

- Decision status: Accepted
- Scope: Controller-authored work executed by the single-host Agent, including
  Compose, built-in adapter procedures, Custom hooks and durable Task-event
  streaming

## Typed execution is the trust boundary

The Controller owns product decisions, durable desired state, dependency order,
retry policy and the complete execution plan. The Agent validates and executes
that plan; it does not read Blueprints, infer desired state or invent missing
parameters.

Plans are versioned, bounded, deterministically hashed and composed from a
closed catalog of typed steps. Artifacts and steps refer to stable resource ids,
not mutable names. Unknown operations, fields, ids, cross-references or hashes
fail before a host mutation. Generic command text, shell fragments, executable
names, argument maps and compatibility decoders are not general execution
authority. The sole operator-authored command exception in this scope is a
validated Custom Backing hook carried by its dedicated typed procedure and
executed inside the selected backing container.

Compose artifacts are derived output, not durable desired state. They contain
canonical secret-free YAML, stable ownership metadata and the exact resource
projection needed by the plan. Compose CLI is the only Compose mutation path;
the Moby API is used only for bounded observation and exact ownership checks.
Groundplane mutates a runtime object only when its full expected ownership tuple
matches the sealed plan. An unmanaged name collision fails closed.

Compose runs in a task-scoped helper created from the approved Agent image. The
helper has a fixed working directory and environment, receives its artifact on
standard input, has no ambient interpolation or registry credentials, and gets
only the mounts required by the selected operation. The persistent Agent keeps
its narrow mount set. The Docker socket remains root-equivalent, so this helper
constrains mistakes and confused-deputy access; it is not containment from a
compromised Agent.

Deadlines and cancellation are explicit plan inputs. Cancellation of an
external mutation never proves that no effect occurred. Cleanup is followed by
bounded observation, and ambiguous or contradictory evidence retains recovery
authority instead of reporting clean success or abort.

Blueprint children request permission before each forward step. The Controller
orders that grant atomically with the recorded start, comparing the current
desired head and exact unit claim. While a newer head is still being planned the
Agent waits; obsolete work stops. These replies do not manufacture execution
events. Replayed event receipts preserve history without renewing revoked
permission. Captured recovery remains available after supersession. This requires
matching Controller and Agent channel schemas, not a compatibility decoder.
See [step admission](../../internal/infra/etcd/blueprint_step_start.go).

## Built-in procedures and Custom hooks remain typed

Attach and Detach use typed adapter procedures containing only the registered
adapter key, lifecycle phase and bounded identity values. For built-in adapters,
SQL, commands and templates stay compiled into the Agent binary. Controller and
Agent therefore ship the same built-in adapter registry; an unknown key is a
permanent plan failure.

Custom is an explicit typed exception, not a generic command runner. Its
operator-selected image may have declared Attach, Detach, before-stop and
after-start hooks. The Controller seals each hook event, stable target identity,
bounded command and timeout, declared inputs and fact schema into a
`BackingHookProcedure`; the Agent runs it inside the already selected backing
container and validates its declared result. The same logical input/output and
fact-classification boundary applies as for built-in provisioning. No hook is
host execution, an arbitrary plan step or ambient Agent subprocess authority.
Without a provisioning hook, Custom Attach is network-only, and Custom has no
managed Backup support. Exact environment and result syntax remains in
[Backing services and Attaches](../features/backing-services.md#unified-provisioning-and-optional-custom-hooks).

Generated credentials may cross the authenticated channel only as transient
sealed plan material. Custom hook inputs and secret-classified results follow
the same transient and encrypted-fact boundaries. Secret values are not Task
parameters, logs or results, and owned buffers are cleared after use. Token
authentication is the single-host MVP boundary; mutual TLS remains outside this
decision.

### Interrupted Attach removal

An authored Detach captures its immutable intent, ordered procedures and original
assignment before execution. Each mutation waits for the Controller to record
its Running event. Recovery resumes only the unfinished suffix of the compiled
revoke, network-withdrawal and Detach procedures; its cursor advances atomically
with the corresponding completed event. Completed procedures are not replayed.
The original failure remains the Task outcome even after cleanup succeeds, and
the visible Apply fails rather than retrying the Blueprint automatically.

Successful cleanup retires the exact Attach/runtime state and records absence in
the same terminal transaction. Detach retains database data. Custom hooks retain
their original plan but do not gain replay authority from recovery. A saved result
is reused without rerunning the command. A STARTED hook without a result blocks
until an operator uses Retry on the visible Apply. That action atomically advances
the child assignment and resource-claim epoch and issues a one-use authorization
bound to the exact checkpoint, plan and step. Starting the authorized attempt
archives the previous checkpoint and consumes its permit atomically. Reconnect
alone cannot grant permission to repeat an interrupted command.
See the [capture owner](../../internal/infra/etcd/attach_removal_recovery_authority.go)
and [executor](../../internal/agent/attach_removal_recovery.go). This integration
is locally compiled; interruption and reconnect remain unqualified.

## Task events resume by durable sequence

Task publication captures a bounded, secret-free description of each sealed
step. Read presentation uses that metadata, not the dispatch resolver: dispatch
checks current execution eligibility, which legitimately changes after completion.
Descriptions are presentation only, never permission to execute. Missing older
metadata stays missing; no live-state reconstruction or history rewrite is used.
See [the safe projector](../../internal/common/taskdescription) and
[Task journal capture](../../internal/infra/etcd/taskjournal/descriptions.go).

Health-wait descriptions capture a narrow projection from the selected sealed
artifact, with the executor's physical workload selection. Only check type,
loopback port, replica requirement and authored timing/retries are retained.
Arbitrary command arguments, URL paths and process patterns are excluded. Missing
projection data is unavailable, not reconstructed from current Service settings;
unspecified timing is not reported as an effective default. The projection shares
the immutable step record and its retention boundary, rather than a second journal.

Failure presentation reads the existing original report and recovery cursor at
the same storage revision as the Task and its events. It must not pick the latest
failed event as the original cause: that event may belong to restoration. After
accepted recovery retires its execution record, the terminal Task result retains
the original outcome and accepted recovery identity. This avoids a second recovery
journal or live-resource reconstruction. Event completion alone cannot authorize
the **Recovery verified** label. See the
[recovery reader](../../internal/infra/etcd/task_recovery_observation.go) and
[human projection](../../internal/controller/handlers/task_recovery_projection.go).

Execution failures use a closed, output-free diagnostic catalog in
[Task progress](../../internal/common/taskprogress/failure.go). Typed helper
results and known error kinds select the code; arbitrary error text is never
public evidence. The authenticated channel checks codes and outcome consistency
before persistence. Step checkpoints and final results retain these observations
within the existing journal, not a parallel logging system. Recovery may add its
own failed-stage observation but must not replace the original failure with a
cleanup error. This machine/storage change requires matching binaries; no
compatibility decoder is provided.

Native Controller updates persist an observed candidate cause in their existing
handoff journal, atomically with entering rollback. The first cause survives
watchdog and Controller restarts; later recovery attempts cannot overwrite it.
The update executor returns a settled outcome separately from an unresolved
execution error, so exposing a cause does not weaken restoration requirements.
Only a settled outcome reaches the normal Task terminal transaction. A startup
guard may restore a failed process without knowing its cause; that cause remains
unavailable rather than inferred from the restored image. See
[handoff persistence](../../internal/infra/controllerrelease/journal.go) and
[settled outcome](../../internal/controller/controllerupgrade/coordinator_outcome.go).

Agent replacement uses its existing aggregate for the first observed candidate
failure. A generation-and-revision comparison saves it before rollback preparation;
restoration carries it into the next generation. Terminal Task projection checks
the pinned previous image and exact restored generation before reading that cause.
A fresh forward update clears the aggregate observation, while the prior Task
retains its own result. The observation changes neither authenticated Ready proof
nor the recovery direction. See
[replacement observation](../../internal/infra/etcd/agentregistration/replacement_failure.go)
and [Task outcome](../../internal/controller/localagent/task_update_outcome.go).

The public Server-Sent Events id is the canonical decimal Task-event sequence,
never a storage revision. `Last-Event-ID` selects a suffix of one Task journal.
Within a connection, each sequence is emitted at most once and in order;
reconnect is at-least-once and clients can deduplicate by that id.

Delivery takes one fixed snapshot and then watches both the Task and its event
journal from the next storage revision. A compacted watch is recovered by a new
snapshot and the last emitted sequence. Storage revisions, internal keys and
compaction details never enter the public stream.

Only authoritative terminal Task state closes the stream. The server drains
events committed with that terminal transition before closing. Errors before
headers use the normal problem response. After headers, transport, storage or
integrity failure closes the connection without inventing an error event; the
client resumes from its last event id.

Every assignment delivery has a positive Controller-authored execution epoch.
Agent event identity includes that epoch. Recovery advances it only after the
preceding epoch has durable effect evidence; an eventless failed delivery may
reuse the same next epoch. This prevents two executions from sharing one event
identity without adding a public delivery record.

Within an epoch, each durable step waits for its start receipt. An allowed receipt
also supplies the journal's committed ordinal watermark; new observations and
outcomes increment from it. Restarting the Agent therefore cannot reuse an old
health observation's identity with new timing or failure data. The original start
remains an exact replay, and conflicting payloads remain rejected. Channel schema
is checked independently of sealed plan schema: changing delivery receipts requires
matching binaries, not rewriting or invalidating unchanged plans.

Trimming raw events also updates a bounded Task-owned attempt-summary record in
the same transaction. Keeping it outside the Task record preserves the Task's
existing size limit without replacing per-step replay checkpoints. It retains
only allowlisted observations and Controller receipt timing, never raw output.
Task details combine that archive with retained events at one storage revision;
the archive retires atomically with the Task. Summary eviction is explicit, and
no execution or recovery consumer uses this diagnostic record as authority.
See [attempt summaries](../../internal/infra/etcd/taskjournal/attempt_history.go)
and [atomic trimming](../../internal/infra/etcd/task_event_retention.go).

## Consequences

- Controller and Agent changes to the execution protocol require a coordinated
  cutover; there is no generic-step fallback.
- Stable ids and sealed artifacts make retries reproducible across label
  renames and newer desired state.
- Observation is evidence for reconciliation, not another writer or a source
  of desired state.
- Secret values, Compose YAML, raw subprocess output and host paths do not
  belong in Tasks, events or diagnostics.

## Source navigation

Current plan shapes and validation live in
[`internal/common/executionplan`](../../internal/common/executionplan/plan.go).
Custom hook planning and execution live in
[`internal/controller/taskplanning`](../../internal/controller/taskplanning/backing_hook_plan.go),
[`internal/common/executionplan`](../../internal/common/executionplan/backing_hook.go)
and
[`internal/agent/backinghookruntime`](../../internal/agent/backinghookruntime/execute.go).
Agent-side assignment validation is in
[`internal/agent/taskassignment`](../../internal/agent/taskassignment/plan.go),
Compose execution is in
[`internal/infra/docker/composehelper`](../../internal/infra/docker/composehelper/execution_dispatch.go),
and durable event replay is in
[`internal/infra/etcd/task_event_stream.go`](../../internal/infra/etcd/task_event_stream.go).
These links identify current owners; they do not by themselves prove runtime
qualification.
