# ADR 0002: Human API and generated clients

Status: accepted

## Context

Console, CLI, and REST must remain one contract. Handwritten route tables and
clients can drift without a build failure.

## Decision

Use Huma v2 typed handlers as the code-first REST boundary and emit one OpenAPI
document. Generate the CLI's Go client with oapi-codegen v2 and the Console's
TypeScript contract with openapi-typescript. Generated files are never edited.
CI regenerates them and fails on drift.

RFC 7807 remains Groundplane's error envelope. Huma transport types stay at
the Controller boundary; domain packages do not import the framework.

## Consequences

- One operation definition drives REST, CLI, and Console types.
- The current handwritten `net/http` route table and CLI transport are
  replaced, not wrapped for compatibility.
- A generated contract does not prove UI parity by itself; the capability
  manifest still verifies one Console action and one CLI command per product
  operation.
