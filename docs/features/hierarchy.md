# Tenants, Projects and Environments

Tenants group an organization's Projects. Each Project contains Environments,
such as staging and production. An Environment owns its workload configuration,
network allocation, storage and operations. Platform-owned backing Projects
provide shared services instead of belonging to a Tenant.

## Names and ownership

IDs are stable references. Slugs are renamable labels: Tenant slugs are globally
unique, Project slugs are unique within their Tenant, and Environment slugs within
their Project. Invalid slugs are rejected, not silently normalized. The CLI
normally resolves slugs; API paths use IDs.

Renaming a slug and editing a display name are separate actions. Neither moves
data, changes descendant ownership nor changes runtime identity. The old slug is
not retained as an alias. Human-readable paths reflect current labels.

## Network allocation

Each Environment reserves a globally unique pool from the bootstrap
`environment_pool`, separate from `system_pool` and `runner.network_pool`.
Only Zone subnets become Docker bridges; the Environment pool itself does not.
The Console shows the calculated pool bounds when creating a Zone.

A replacement Environment pool must contain existing Zones and avoid other
reservations. Zone network settings are immutable. Reservations remain held until
physical cleanup completes. Platform-generated networks are not editable Zones.

An internal Zone has no outbound host gateway. This is an outbound-connectivity
choice, not a claim that mutually trusted workloads become hostile-tenant-safe.
See [network decisions](../decisions/network-and-shared-access.md).

## Removing a hierarchy

Removal coordinates descendant cleanup and removes the parent last. It is not a
shortcut around active child operations. A descendant already being deleted,
including one awaiting Retry, blocks a competing parent deletion with
`resource.in_use`. Resolve that existing operation first.

Partial failure leaves the hierarchy visible and fenced against new writes.
Retry continues the original operation; descendants already removed stay removed.
Labels remain reserved until successful completion and IDs are never reused.
Aborting a deletion does not recreate data already removed.

Backing-service lifecycle is separate: its runtime Destroy action does not grant
a generic way to delete its hidden Project or persistent data.

## Design and qualification

[Storage and identity](../decisions/storage-and-idempotency.md) explains stable
lookup and bounded rename. [Resource deletion](../decisions/resource-deletion.md)
explains why cleanup ownership survives failures. Current qualification is tracked
in [capabilities](../capabilities.md) and the [QA matrix](../qa-matrix.md), not
inferred from a successful parent Task on an older build.
