# ADR 0037: Backing-service lifecycle contract

- Status: Accepted
- Accepted: 2026-08-28
- Owners: Controller, Console, CLI, and Agent
- Related: ADR 0021, ADR 0022, ADR 0025, ADR 0028, ADR 0031, ADR 0049, ADR 0051, ADR 0053, ADR 0068

## Context

The MVP Backing Service is a facade over one ordinary hierarchy:

```text
Project(kind=backing) -> Environment(name=main) -> Service(adapter-backed)
```

The read facade already composes those records. Creation and lifecycle need one
small contract that preserves the same resource model without introducing a
second Blueprint transport, a plugin system, or a partial aggregate.

## Decision

### 1. Identity and aggregate shape

The Backing Service id is its stable backing Project id. Creation allocates and
publishes exactly:

```text
one backing Project
one Environment named main
one backing-owned Zone
one adapter-backed Service
one adapter-defined managed data Volume
one Agent create Task
```

The response exposes the facade plus the Task id. The facade continues to expose
the immutable Environment, Service, and backing Zone ids. No duplicate
Backing-service primary is persisted.

The adapter key, Environment id, Service id, Zone id, Volume id, Service name,
Volume key, and Zone ownership are immutable. Project slug and name remain
ordinary renamable labels through the Project capability.

### 2. Closed create request

`POST /backing-services` is a protected JSON mutation requiring one
`Idempotency-Key`. Its body is exactly:

```json
{
  "slug": "shared-postgres",
  "name": "Shared PostgreSQL",
  "description": "Primary shared database",
  "adapter": "postgres:16",
  "network_pool": "10.200.60.0/24",
  "zone": {
    "name": "postgres",
    "subnet": "10.200.60.0/28",
    "internal": false
  }
}
```

`description` is the only optional member and defaults to the empty string.
Unknown members are rejected. The only adapter values are `postgres:16` and
`valkey:9`.

The operator always supplies `network_pool`, `zone.name`, `zone.subnet`,
and `zone.internal`. The subnet must be canonical IPv4 CIDR, contained by the
Environment pool, and globally non-overlapping with every Environment pool and
Zone reservation according to the existing IPAM rules.

Creation has no existing-Zone branch, uploaded Blueprint, Compose fragment,
image field, volume field, omitted subnet default, inferred network, adapter
options map, or runtime plugin discovery.

### 3. Compiled adapter catalog

The existing compiled adapter registry is the sole source of adapter-specific
desired state. Each accepted adapter version fixes:

- the adapter contract revision and managed image identity;
- the Service name, image, health check, exposed port, restart policy, and
  initial `runtime_intent=running`;
- the single managed Volume key, initial slug, and container mount target;
- bootstrap Entries and encrypted generated values;
- fact schema and prefix;
- Attach provision, grant, revoke, and deprovision procedures; and
- Backup and restore strategy availability.

The Controller resolves and freezes the catalog entry during create
publication. Operators cannot override these fields through C10. Ordinary
Service, Volume, Entry, Secret, Attach, and Backup capabilities remain the only
post-create mutation surfaces permitted by their own contracts.

A missing adapter, unsupported contract revision, unavailable pinned image, or
invalid catalog output fails before durable mutation with
`strategy.not_implemented` or `validation.failed` as appropriate.

### 4. Atomic publication and execution

Backing desired rendering uses an explicit closed Project-owner kind. A
tenant-owned render requires its stable Tenant id and emits the reserved
`com.groundplane.tenant-id` label. A backing-owned render requires the Tenant
id to be absent and does not emit that label at all; an empty Tenant label is
not a valid substitute. Project and Environment ownership labels remain
required in both cases.

One transaction validates and commits the complete hierarchy, scoped indexes,
global pool and subnet reservations, Volume identity/path, adapter desired
records, encrypted bootstrap values, protected idempotency evidence, immutable
Task plan, Task indexes, and queue entry.

The transaction fences Tenant/Project/Environment deletion state, all relevant
slug/name indexes, IPAM reservations, adapter registry revision, and the Agent
execution-plan schema. A conflict commits nothing. Exact replay returns the
original response bytes and ids.

The create Task uses the existing Environment directory, managed Volume,
materialization, Compose apply, and health-convergence steps. It never invokes
a shell or a C10-specific Agent procedure. Failure retains the complete durable
aggregate and its stable ids for ordinary Task retry; retry reuses the frozen
plan and never allocates another Project, Environment, Zone, Service, or
Volume. Successful completion makes the facade ready for Attach.

The initial create Task deadline is 300 seconds.

### 5. Lifecycle

These bodyless protected actions address the facade by backing Project id:

```text
POST /backing-services/{project_id}/start
POST /backing-services/{project_id}/stop
POST /backing-services/{project_id}/destroy
```

The Controller resolves the facade at one fixed revision and delegates to the
existing immutable adapter Service lifecycle:

- `start` sets `runtime_intent=running` and applies the frozen desired state;
- `stop` sets `runtime_intent=stopped` and stops the Service;
- `destroy` sets `runtime_intent=absent` and removes only Service runtime.

Destroy does not delete the Project, Environment, Zone, Volume, Entries,
credentials, Attach history, or data. Recreate uses Start. Lifecycle Tasks use
the existing Service lifecycle deadlines, retry, terminalization, and
idempotency rules.

There is no Backing-service delete endpoint in the current MVP/API contract.
Permanent Backing deletion is outside the current MVP by owner decision on
2026-09-12. The [deferred extension in ADR 0053](0053-durable-hierarchy-and-backing-facade-deletion.md#8-deferred-backing-service-permanent-deletion)
requires separate approval and a distinct Delete capability across Console,
CLI and API, with impact preview and confirmation. It cannot redefine Destroy.

### 6. Public parity

The Console create form, CLI, and REST request expose the same decisions:

```text
backing-service create
  --slug <slug>
  --name <name>
  [--description <text>]
  --adapter postgres:16|valkey:9
  --network-pool <cidr>
  --zone-name <name>
  --zone-subnet <cidr>
  [--zone-internal]
```

The CLI has no image, volume, Compose-file, Blueprint, existing-Zone, or generic
adapter-option flag. Console adapter choices come from the two compiled MVP
values, not from a public catalog endpoint.

Create returns `201 {backing_service,task_id}`. Start, Stop, and Destroy each
return `202 {task_id}`. The Console observes the returned Task and renders the
same facade records; it never manufactures readiness or activity.

## Rejected alternatives

- Multipart Backing Blueprint creation.
- Selecting or inferring an existing Zone.
- A second persisted Backing-service aggregate.
- Generic adapter plugins or a public adapter catalog.
- User-selected image, volume, mount, or bootstrap internals.
- Sequential calls to public Project, Environment, Zone, Volume, and Service
  mutations.
- Deleting durable data as a consequence of Destroy.
- Compatibility readers for the former scaffold request.

## Consequences

C10 creation is intentionally narrower than ordinary Blueprint authoring. This
keeps the MVP deterministic and makes one atomic aggregate feasible while the
ordinary resource capabilities remain reusable after creation. Supporting
custom backing templates or existing networks is post-MVP work and requires a
new accepted contract rather than widening this request implicitly.
