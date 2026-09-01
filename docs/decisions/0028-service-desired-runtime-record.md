# ADR 0028: Service desired and runtime record boundary

- Status: Accepted
- Date: 2026-08-22

## Context

A Service has Blueprint-authored desired state and Controller-owned
`runtime_intent`. Existing contracts require the two models to remain
separate, but did not define the initial intent or whether their durable
representations occupy independent keys. Leaving either choice implicit would
make create, Blueprint reconciliation, lifecycle actions, and Remove disagree.

## Decision

Each Service has one flat primary at
`/v1/records/services/<service-id>`, as specified by ADR 0013. Its versioned
payload contains three fields:

```text
environment_id
desired       (the complete core.Service)
runtime       (the complete core.ServiceRuntime)
```

`desired.id` and `runtime.service_id` must both equal the primary Service id.
The single record is the atomic consistency boundary, while the distinct typed
subrecords preserve authorship: Blueprint input can never contain or overwrite
`runtime`.

A Service created directly or first introduced by Blueprint starts with
`runtime_intent=running`. Reconciliation replaces only `desired` and preserves
`runtime` byte-for-byte. Start, Stop, and Destroy update only `runtime` to
`running`, `stopped`, and `absent`; their Task creation and intent update commit
in one transaction. Remove deletes the Service primary and its runtime state
together after its Task completes successfully.

The primary is indexed by environment ownership and scoped name using the ADR
0013 paths. All writes are revision-fenced and observe the Environment and
ancestor deletion tombstones.

## Consequences

- There is no orphanable second runtime key and no cross-key read race.
- Blueprint apply cannot reset a stopped or absent Service.
- A newly declared Service participates in the next reconcile unless the
  operator explicitly stops or destroys it.
- Public Service responses assemble desired fields and runtime intent from one
  revision.
