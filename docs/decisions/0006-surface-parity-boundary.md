# ADR 0006: Operator surface parity boundary

Status: accepted

## Context

Groundplane requires every operator-facing product capability to remain
identical across the Console, CLI, and REST API. A literal rule covering every
executable command would incorrectly require Console actions and REST
endpoints for local daemon launchers, same-host diagnostics, shell completion,
and version output.

## Decision

The 1:1 invariant covers operator-facing Controller capabilities. Each such
capability has exactly one Console action, CLI command, and REST endpoint.
The Console action id and OpenAPI `operationId` are the same stable lowercase
`noun.verb` identity. Multiword nouns and verbs use kebab case. This identity
is not a presentation label and does not change when command help or UI copy
changes.

The closed exceptions are local process commands (`controller serve` and
`agent-run run`), same-host diagnostics (`controller key show` and
`controller etcd show`), and local CLI tooling (`version` and `completion`).
These commands do not call the Controller human API and are not Console
product capabilities. A new exception requires an explicit product-contract
change.

Agent enrollment is not an exception. `agent join` is the operator-facing
mirror of the Console action and `POST /agents`; the native Controller owns
credential provisioning and the managed Agent container lifecycle. The
Controller-to-Agent gRPC channel remains a machine surface outside this human
API parity manifest.

The manifest records the full human operation contract, not only route
presence. Reads and synchronous updates return `200`, resource creates return
`201`, Task dispatch returns `202 {task_id}`, and a synchronous bodyless delete
returns `204`. Entity edits use `PATCH`; the three renames use synchronous
`POST /{id}/rename`; `PUT` is reserved for explicit singleton replacement or
Blueprint apply. Component detail, actions, and config use stable Component
ids; Components have no independent create, edit, or delete route. Attaches
require explicit service and backing-service operands and have no detail
route. `agent config show` and `task events` are ordinary parity operations,
not exceptions.

## Consequences

- OpenAPI/client drift checks cover the operator-facing parity manifest.
- Local tooling is tested as tooling rather than represented by fake Console
  actions.
- Machine bootstrap may use the protocol required by the Agent without
  exposing credentials or enrollment controls as routine Console actions.
- The exception list cannot grow implicitly during implementation.
