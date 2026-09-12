# ADR 0003: Compose model and local runtime

Status: accepted

## Context

Groundplane must preserve valid Compose semantics while retaining ownership of
deploy policy, release history, cross-project ordering, secrets, and tasks.

## Decision

Use compose-go/v2 to parse and work with the Compose model. Validate rendered
documents with `docker compose config`, then apply them through the Docker
Compose v2 CLI using `internal/common/runner`. The Agent is the only process
that invokes the runtime.

Groundplane owns stable project names, labels, execution plans, health gates,
blue-green switching, rollback decisions, and durable records. It does not
pretend local Compose implements Swarm update policy.

## Consequences

- No handwritten partial Compose parser or parallel Docker execution path.
- CLI invocation remains observable, timeout-limited, cancellable, and easy to
  fake in tests through the shared Runner.
- A future runtime is a separate capability adapter, not compatibility logic
  inside the Compose implementation.

## Runtime constraints

Pin and test the Compose/Engine versions used by the packaged Agent. Local Compose
is an execution primitive, not a continuously running Groundplane release controller.
The Controller owns desired state and release policy; the Agent executes bounded
validated procedures. Groundplane also owns secret storage, materialization
permissions and cleanup; Compose file mounts are not encrypted secret storage.
Preserve Compose semantics through the real parser and generated-runtime tests.
The current grammar is in [blueprint.md](../blueprint.md), not the retired runtime
research report. Re-evaluate platform behavior from primary sources if the runtime
choice or pinned implementation changes.
