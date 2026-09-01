# ADR 0062: Environment Blueprint authoring projection

Status: Accepted

Date: 2026-09-01

## Context

Environment Blueprint was write-only. The Console fabricated an approximate
YAML view from partially loaded frontend state, so it could display fields the
Controller did not accept and omit current desired decisions. Editing also had
no concurrency fence.

## Decision

The Environment Blueprint is a revisioned authoring singleton:

- `GET /environments/{id}/blueprint` returns canonical single-file YAML plus
  the selected desired revision and `ETag`;
- `POST /environments/{id}/blueprint/validate` validates a closed multipart
  bundle without side effects and returns a typed non-destructive diff;
- `PUT /environments/{id}/blueprint` applies the same bundle and returns the
  reconcile Task;
- validate and apply require one exact quoted `If-Match` revision;
- revision `0` represents an Environment with no desired head;
- the projection is reconstructed from normalized authored Compose and typed
  desired records, not generated runtime artifacts or uploaded audit layout;
- comments, anchors, aliases, and source-file boundaries are not preserved;
- omitted existing resources are retained; removal remains an explicit
  protected resource action;
- Script bodies are emitted as literal YAML block scalars.

The Console keeps drafts locally and exposes view, edit, single-file import,
closed-bundle import, export, validate, and apply. The CLI cleanly replaces
`environment apply` with `environment blueprint show|validate|apply`.

## Consequences

Console, CLI, and API operate on the same Controller-authored document.
Generated ids and observations cannot leak into editable YAML, and concurrent
desired mutations cannot be overwritten by a stale editor. Source formatting
is intentionally not a durable contract.
