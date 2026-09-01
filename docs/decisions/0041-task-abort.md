# ADR 0041: Task abort targets and terminalizes the existing Task

- Status: Accepted
- Date: 2026-08-23

## Context

The MVP already fixes `POST /tasks/{id}/abort`, `task abort <id>`, an abort
control on the selected Console Task, the `TaskAbort` Agent message, and the
`aborted` terminal state. The remaining ambiguity was response identity: the
API promised `202 {task_id}` without saying whether that id names the target
Task or a second Task created only to cancel it.

A second abort Task would split one operator action across two journals, make
retry and activity identity ambiguous, and deadlock behind the serial native
Controller runner when asked to abort its current Task. A process-local
"abort requested" acknowledgement would instead lose accepted work on
Controller restart.

## Decision

Abort always targets one explicit Task id and never creates another Task or
operation. The response is exactly `202 {"task_id":"<target Task id>"}`.
The Task id is therefore both the action target and the progress resource.

Only `pending` and `running` Tasks are abortable. A pending Task is atomically
terminalized as `aborted` before assignment. A running Agent Task receives
`TaskAbort{task_id, reason:"operator_requested"}` for its exact durable Agent
generation; the request subscribes before delivery and returns only after the
Agent acknowledgement has committed. A running native Controller Task has
its exact execution context cancelled and likewise returns only after its
terminal transaction commits. Controller shutdown cancellation remains
distinct and leaves the durable claim running for restart recovery.

An already `aborted` target is an idempotent success returning the same
response. `completed`, `failed`, and `timed_out` targets fail with
`task.not_abortable` (409). Natural completion racing an abort is reported by
that same rule. The standard `Idempotency-Key` header remains required, but
the Task is the idempotency scope: there is no request body, and its durable
terminal state is the uniqueness and replay authority.

No cancellation acknowledgement weakens resource ownership. Resource-specific
terminalization still runs in the Task acknowledgement transaction; corrupt
ownership is Internal rather than a compatibility exception.

## Consequences

- Console, CLI, and API follow one Task id before and after abort.
- A successful HTTP response proves durable terminal state; there is no
  restart-vulnerable intermediate abort intent.
- Agent delivery and native Controller cancellation close natural-completion
  races before claiming success.
- Offline or generation-mismatched Agent delivery fails closed; the Task may
  still reach its durable timeout and the operator may retry the request.
- Aborting an external mutation never assumes cancellation means no effect;
  its existing typed acknowledgement and reconciliation rules remain intact.
