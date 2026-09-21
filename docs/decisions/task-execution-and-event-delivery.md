# Task execution and event delivery

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

## Task events resume by durable sequence

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
