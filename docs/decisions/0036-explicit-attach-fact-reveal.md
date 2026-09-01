# ADR 0036: Explicit Attach fact reveal

Status: accepted

## Context

Attach records durably retain listable fact schema and separately encrypted
values. The Console must show the real Controller projection, while list
responses must never decrypt or duplicate secret values.

## Decision

- Attach list items include stable backing Project ownership and fact-set
  metadata: grant Attach id, key, and secret classification.
- Fact values are returned only by `GET /attaches/{id}/facts/{key}`. The
  optional `grant_attach_id` selects a granted fact set.
- The Controller derives Environment scope from the durable owning Attach and
  accepts only ready Attach facts. Callers cannot supply or override scope.
- `attach fact <attach> <key> [--grant <attach>]` and the Console reveal action
  are the exact CLI and Console counterparts of the endpoint.
- List hydration may resolve non-secret database and role facts for labels.
  Secret password and URL facts remain masked until an explicit reveal.

## Consequences

The Console no longer manufactures database, role, password, or URL values.
No plaintext fact is added to the durable Attach record, task journal, list
response, activity record, or generated desired-state projection.
