# ADR 0033: Environment IPAM and Runner isolation

## Status

Accepted for the MVP.

## Context

Groundplane runs every MVP workload through one Docker daemon on one host.
Docker rejects overlapping bridge subnets across that daemon, while Compose
project names do not create separate IPAM address spaces. Reserving one large
Tenant range would require predicting the future capacity of every Project and
Environment at Tenant creation. Reserving no parent range would make Zone
allocation hard to explain and easy to collide.

GitHub self-hosted Runners are Tenant- or Project-scoped and must support Docker
builds, container actions, and service containers. Giving workflow code the
host Docker socket would grant control over every Groundplane container and
network, defeating the Tenant isolation boundary. Sharing a Runner bridge would
also permit lateral communication between jobs.

## Decision

Machine bootstrap requires two non-overlapping IPv4 CIDRs:

- `environment_pool` is the allocation root for Environment reservations;
- `system_pool` is the allocation root for generated Agent, Runner, and other
  platform networks.

They are local startup inputs and never Docker networks. Every Environment,
including a backing Project's `main` Environment, reserves one globally unique
child `network_pool`. Every Zone reserves exactly one child subnet and is the
only layer materialized as a Docker bridge. Environment pool replacement is
valid only when the new CIDR contains every existing Zone and overlaps no
reservation. Zone name, subnet, `internal`, and ownership are immutable;
network changes use add, move, and remove. Reservations remain fenced until
physical cleanup succeeds.

Environment Zones store a stable `net_` id and derived
`owner_kind: environment`; backing `create_network` Zones use
`owner_kind: backing_project`. Platform component networks are generated
artifacts and never appear through Zone CRUD. Physical Zone names are
`gp_net_<stable-zone-id>`.

A Tenant may own at most five Runner records across its direct and Project
scopes. The quota record contains the sorted stable ids of both scopes and is
not an ownership index. A direct Runner has exactly one Tenant owner index; a
Project Runner has exactly one Project owner index. The Project's durable
Tenant id selects the same combined Tenant quota. No Runner has both owner
indexes.

Each Runner consumes a dedicated `/29` from `system_pool`; no Runner shares a
bridge or joins an Environment, backing, or platform network. The native
Controller supervises a distinct rootless Docker daemon for each Runner under
a dedicated unprivileged host UID and subordinate UID/GID range. The Runner
container mounts only that daemon's Unix socket. It never receives the host
Docker socket, host networking, privileged mode, or `CAP_NET_ADMIN`. UID/cgroup
egress policy blocks Groundplane private pools except the Controller endpoint
while retaining DNS and outbound internet.

### Bounded host identity slots

Machine-local Controller configuration supplies three finite inclusive ranges:
`runner.host_uid_range`, `runner.subuid_range`, and `runner.subgid_range`.
Ranges use canonical unsigned decimal `first-last` syntax. The subordinate
block size is fixed at 65,536 ids in the MVP. The host UID range cardinality
must equal the number of complete blocks in each subordinate range, all three
ranges must describe at least five slots, and no remainder is accepted.
Bootstrap rejects host UIDs already assigned to another account and subordinate
blocks overlapping any existing `/etc/subuid` or `/etc/subgid` assignment.

Slot `i` is the exact tuple `host_uid = host_first + i`,
`subuid = [subuid_first + i*65536, 65536]`, and
`subgid = [subgid_first + i*65536, 65536]`. One durable allocation record at
`/v1/runtime/runner-host-slots/{zero-padded-slot}` stores the exact Runner id
for each claimed slot. Allocation always takes the lowest free slot. The Runner
record persists the slot, exact UID, both subordinate starts and counts, and
dedicated subnet. The create transaction compares and updates that registry and the
system-pool registry together. Exhausting either finite pool is
`resource.in_use`; it never causes a range expansion or allocation guess.
Retries reuse the exact allocation. No API, Console action, or CLI command
manages these machine-local ranges.

### Replayable lifecycle and transient registration token

Runner create is one protected Controller Task. Its initial transaction
atomically publishes the `provisioning` Runner record, its one owner index, the
combined Tenant quota claim, host slot, system subnet, Task and queue records,
active operation, and idempotency evidence before any host mutation. The Task
then provisions the assigned host identity, subordinate mappings, rootless
daemon, bridge, local state, Runner container, and one-time GitHub registration.
Success changes the lifecycle to `ready` and completes the Task atomically.
Failure changes it to `failed` while retaining every allocation for an exact
retry or later removal.

The operator supplies a short-lived GitHub registration token to every create
or failed-create retry attempt. The token exists only in that HTTP request and
the in-process Controller executor buffers for that attempt. It, its digest,
and any reversible derivative are excluded from the Runner record, Task,
events, idempotency marker, logs, argv, environment, and local durable state;
buffers are cleared after the registration step. Durable idempotency intent
binds the Runner request and only the fact that a token was supplied. A
transport replay returns the original Task and never consumes a replacement
token or creates another attempt.

If the Controller loses the transient token before registration can be proven,
the create Task terminates as failed with `registration_token_required`; it is
never resumed by guessing or by reusing secret material. The operator-facing
Runner Retry action requires a fresh single-use token and creates a new Task
for the same Runner id, quota slot, host slot, and subnet. Generic Task Retry
rejects such a Runner create Task because it cannot supply the required token.
An existing local registration may be observed and reused, but Groundplane
never calls GitHub to generate, fetch, rotate, or remove a registration.

Runner removal is a protected Controller Task. Its initial transaction creates
the typed deletion tombstone, immutable cleanup intent, Task, active operation,
and idempotency evidence while leaving the Runner and every allocation visible
and fenced. Cleanup stops and deletes only the local Runner container, rootless
daemon, bridge, host account/subordinate mappings, and local state. Successful
finalization atomically deletes the Runner and its sole owner index, releases
the Tenant quota, host slot and system subnet, removes the tombstone and intent,
and completes the Task. Failure retains the Runner and allocations, clears the
active removal fence in the terminal transaction, and permits a protected
retry. Removal never deregisters the Runner through GitHub.

The removal Task keeps the Runner id as its ordinary `target`. Its immutable
durable `Params` additionally freeze exactly `runner_tenant_id`,
`runner_owner_kind`, `runner_owner_id`, `runner_host_slot`, and
`runner_network_cidr`. Begin and retry validate those five values against the
exact Runner record and immutable cleanup intent before publishing the Task.
They remain on every terminal attempt after successful deletion so replay can
prove the exact owner-index, Tenant-quota, host-slot, and subnet cleanup without
reading a deleted Runner or scanning any registry. Retries copy them unchanged.

These Params are non-secret cleanup identity only. They never contain a host
UID, subordinate UID/GID start or count, any other `/etc/subuid` or
`/etc/subgid` value, a registration token, token digest or derivative,
credential, or secret. This durable evidence does not add a public request or
response field and does not become an Agent execution payload.

`online` is observed state, not Runner desired state and not a mutable record
field. The Controller projects it from the supervised local runtime; absent or
stale evidence is offline. Create, retry, remove, and ordinary record mutation
cannot write an operator-supplied online value.

## Alternatives considered

### Tenant network pools

Rejected because the operator cannot predict all future Project and
Environment capacity when a Tenant is created. Environment reservations match
the lifecycle and topology ownership boundary directly.

### Duplicate CIDRs in isolated Compose projects

Rejected because Compose projects share the Docker daemon's IPAM and host
routing table. Docker rejects overlapping bridge subnets.

### One shared Runner network per Tenant

Rejected because containers on one bridge can communicate with each other and
CIDR resizing requires network replacement. Per-Runner allocation grows by one
independent subnet without disrupting existing jobs.

### Host Docker socket or privileged Docker-in-Docker

Rejected because either grants workflow code a path to host-level container
and network control. A dedicated rootless daemon preserves Docker-compatible
jobs without sharing the rootful daemon.

## Consequences

- Environment creation and edit gain an explicit `network_pool` contract.
- Zone network settings become immutable and Zone edit is removed.
- Machine bootstrap must validate both allocation roots against host routes and
  existing Docker networks before the Controller serves mutations.
- Runner bootstrap requires rootless Docker prerequisites and subordinate id
  allocation on the host.
- Five Runners consume at most five independent `/29` networks per Tenant.
- Clean-start MVP delivery reproduces the `qa-workload/` topology but does not import
  populated `/infra/vol/*` directories.
