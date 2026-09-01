# ADR 0058: Revision-authoritative direct desired mutations

- Status: Accepted
- Date: 2026-08-28

## Context

The MVP exposes direct Service create, edit, and Remove actions alongside full
Environment Blueprint replacement. ADR 0051 makes one immutable Environment
desired revision and one desired head the sole authority for rendering, but it
explicitly did not define Service Remove. The current scaffold writes direct
Service create and edit results only to the flat Service projection while the
Agent plan renderer reparses the Blueprint audit bundle. A direct mutation can
therefore be visible through API, CLI, and Console without affecting a later
Compose artifact.

Adding a mutable overlay, suppression list, or alternate desired head would
create two authorities. Rewriting an arbitrary file in a multi-source authored
bundle would also guess which source the operator intended to own the change.

## Decision

### One revision owns runtime-authoritative normalized state

Every Environment desired revision owns two distinct immutable streams:

- audit input, which explains the operator action; and
- normalized desired state, which is the only input to plan rendering.

A Blueprint revision's audit input is its existing verified bundle. A direct
mutation revision's audit input is the closed canonical record:

```text
schema             1
base_revision_id   task id of the exact prior desired head
operation          service.create | service.edit | service.remove
target_id          stable Service id
request            the exact normalized public mutation input, or no body
```

The normalized stream contains the complete parsed Compose project plus the
complete parsed Groundplane extension model needed to reproduce rendering. It
contains operator decisions, not Controller-derived paths, Docker names,
ownership labels, rendered files, runtime intent, observations, or secrets.
Stable resource identities remain in the existing revision projection.

The Controller renderer loads the normalized stream for the pinned revision.
It never reparses audit input at execution time. Blueprint publication parses
and validates the bundle once, then stores both streams. A direct mutation
loads one fixed base revision, clones its normalized stream, applies the typed
mutation, validates the complete result, and stores a new revision. There is no
overlay, suppression record, mutable normalized document, scan fallback, or
second desired head.

### Create and edit save desired state without deploying

Direct `service.create` and `service.edit` atomically publish:

- the new immutable desired revision and desired-head replacement;
- the matching flat Service projection and indexes; and
- the protected direct idempotency response.

They retain their existing synchronous `201 Service` and `200 Service`
responses. They do not create a reconciliation Task. The new desired state is
applied by the next explicit Service or Environment deploy, matching the MVP
Console contract. Edit replaces only the documented direct-edit subset while
preserving every normalized native field outside that subset.

### Remove is a candidate until host cleanup succeeds

`DELETE /services/{id}` publishes one protected Remove Task, one deletion
tombstone, and one immutable candidate desired revision that omits the Service.
The current desired head and visible Service projection remain unchanged while
the Task is pending, running, failed, timed out, or aborted.

If the Service exists in the current applied projection, the 120-second Agent
Task uses the exact pinned applied artifact and targeted `ComposeRemove` with
`whole_project=false`. It never removes a network, named Volume, host Volume,
or another Service. A never-applied Service uses a 30-second Controller no-op
Task. Successful terminal acknowledgement atomically promotes the candidate
head, deletes the Service primary and indexes, removes the tombstone, and
releases the operation fences. Every non-success terminal result preserves the
current head and visible Service. Retry copies the exact candidate and render
input; it never rebases.

### Direct Remove never cascades

Direct Service Remove returns `resource.in_use` before Task publication when
the Service is referenced by another desired resource, including:

- a Route target;
- an Attach consumer or backing binding;
- a Release Group membership;
- another Service dependency;
- a Service-scoped Entry exposure;
- a Script or hook target; or
- an active Service, release, group, hierarchy, or Environment operation.

The operator must remove or edit those resources through their own explicit
actions first. Immutable release history is retained and remains addressable by
stable ids. Volumes mounted only by the removed Service are retained; Volume
deletion remains its own impact-checked workflow.

The Environment writer fence serializes Blueprint publication, direct desired
mutation, Route/Entry/Volume desired mutation, Attach topology mutation, and
Service Remove candidate promotion. Blueprint omission of an existing Service
continues to fail until that Service's explicit Remove has completed.

### Public surfaces remain 1:1

No new human operation is added. The existing surfaces remain:

- Console Service create, edit, and Remove actions;
- `service add`, `service edit`, and `service remove`;
- `POST /services`, `PATCH /services/{id}`, and `DELETE /services/{id}`.

All three address the same Controller capability. Remove returns
`202 {task_id}` and create/edit retain their synchronous resource responses.

## Clean replacement

The implementation removes:

- execution-time reparsing of Blueprint audit files;
- flat-only direct Service desired writes;
- fixture-only Console Service deletion; and
- any path that deletes a Service primary before successful Task
  acknowledgement.

No compatibility reader, legacy direct-write path, or dual publication is
retained.

## Acceptance

The capability is complete only when automated and reference-host evidence
proves:

1. direct create and edit change the next rendered Compose artifact;
2. fields outside the direct-edit subset survive unchanged;
3. an active Remove keeps the current head and Service visible;
4. failed Remove preserves runtime-desired state and retry uses the same
   candidate;
5. successful Remove targets only the selected containers, promotes the exact
   candidate, and retains networks, Volumes, and release history;
6. every listed reverse reference rejects before durable publication;
7. concurrent Blueprint and direct mutations have one winner without lost
   desired state; and
8. Console, CLI, and API observe the same resource and Task identities.
