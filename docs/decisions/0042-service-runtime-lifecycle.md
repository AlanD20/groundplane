# ADR 0042: Service runtime lifecycle task ownership

- Status: Accepted
- Date: 2026-08-23

## Context

ADR 0028 separates Blueprint-authored Service desired state from
Controller-owned `runtime_intent`, but the executable contract also needs to
fix how Start, Stop, and Destroy behave when a Service has or has not reached
the applied Environment projection. The public actions are bodyless, so Stop
cannot accept an operator-selected grace period. Task retry and Controller
restart must not cause an action to render from newer desired state, and
concurrent actions for one Service must not execute out of order.

## Decision

- `POST /services/{id}/start`, `/stop`, and `/destroy` always create a durable
  Task and atomically set `runtime_intent` to `running`, `stopped`, or `absent`.
  Failure, timeout, or abort does not roll the requested intent back.
- A Service present in the current applied Environment projection creates a
  120-second Agent Task. Publication stores an immutable render snapshot with
  the exact hierarchy labels, Blueprint revision, applied identity projection,
  artifact id, and render generation. Retry copies that exact snapshot and
  preserves the plan id and hash.
- Start uses targeted Compose apply. Stop uses targeted Compose stop with a
  fixed 30-second grace period. Destroy uses targeted Compose remove and does
  not remove the Compose project, networks, volumes, or desired Service.
- A Service absent from the current applied projection creates a 30-second
  Controller Task with one deterministic no-op step. The intent and Task still
  commit atomically so the action is observable and replayable.
- One durable active lifecycle index serializes actions per stable Service id.
  Every terminal status releases it in the same Task acknowledgement
  transaction; retry reacquires it atomically.
- The immutable per-attempt render snapshot is deleted atomically when its Task
  journal is pruned.
- Backing Services follow the same public and Task semantics, but their render
  snapshot and execution are owned by the backing-runtime capability rather
  than being inferred from a tenant Blueprint.

## Consequences

- Repeating an action is legal and convergent, but it still produces a new
  observable Task after the prior attempt is terminal.
- A queued action cannot silently execute a later Blueprint or renamed
  hierarchy.
- Stop behavior is predictable across Console, CLI, and API without adding an
  undocumented request body.
- Backing Service lifecycle cannot be implemented by reusing a tenant
  Environment renderer; the backing-runtime slice must provide its exact
  immutable projection before that path is marked complete.
