# ADR 0033: Environment IPAM

## Status

Accepted for the MVP.

## Context

Groundplane runs every MVP workload through one Docker daemon on one host.
Docker rejects overlapping bridge subnets across that daemon, while Compose
project names do not create separate IPAM address spaces. Reserving one large
Tenant range would require predicting the future capacity of every Project and
Environment at Tenant creation. Reserving no parent range would make Zone
allocation hard to explain and easy to collide.

## Decision

Machine bootstrap requires `environment_pool` as the allocation root for
Environment reservations. It is a local startup input, never a Docker network,
and is disjoint from `system_pool` and `runner.network_pool`. Every Environment,
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

## Alternatives considered

### Tenant network pools

Rejected because the operator cannot predict all future Project and
Environment capacity when a Tenant is created. Environment reservations match
the lifecycle and topology ownership boundary directly.

### Duplicate CIDRs in isolated Compose projects

Rejected because Compose projects share the Docker daemon's IPAM and host
routing table. Docker rejects overlapping bridge subnets.

## Consequences

- Environment creation and edit gain an explicit `network_pool` contract.
- Zone network settings become immutable and Zone edit is removed.
- Machine bootstrap must validate the Environment allocation root against host routes and
  existing Docker networks before the Controller serves mutations.
- Clean-start MVP delivery reproduces the `qa-workload/` topology but does not import
  populated `/infra/vol/*` directories.
