# ADR 0044: Project bounded live host health with fixed formatting

- Status: Accepted
- Date: 2026-08-23

## Context

C01 already defines one Console-shaped `GET /host` response and three narrow
Controller ports for system, etcd, and local Agent observations. Production did
not compose those ports because the unit, rounding, dependency-failure, and
no-Agent contracts were not fixed. The Console therefore continued to display a
fixture plus runtime claims that were not present in the API.

The MVP is one Linux host, one native Controller, one host-level etcd node, and
zero or one Controller-owned Agent container. Host health is an operator
snapshot, not a metrics database or monitoring subsystem.

## Decision

`GET /host`, `groundplane host show`, and the Platform Host page consume one
live read model. Nothing in this model is persisted or added to the Task
journal.

Linux system observations use these exact sources and formatting rules:

- hostname is `gethostname(2)`, architecture is the Go runtime architecture,
  and OS is `/etc/os-release` `PRETTY_NAME`;
- uptime is floored to the largest whole unit among seconds, minutes, hours,
  and days, with singular grammar at one;
- CPU model comes from `/proc/cpuinfo`, logical cores from the Go runtime, and
  load is the one-minute `/proc/loadavg` value divided by logical cores,
  rounded to the nearest whole percent and clamped to `0..100`;
- memory used is `MemTotal - MemAvailable`, and swap used is
  `SwapTotal - SwapFree` from `/proc/meminfo`;
- disk is the root filesystem from `statfs(2)`, with total `blocks * block size`
  and used `(blocks - free blocks) * block size`;
- byte values use binary IEC units (`KiB`, `MiB`, `GiB`, `TiB`), round to one
  decimal place, and omit a trailing `.0`; percentages round to the nearest
  whole number and clamp to `0..100`; and
- Docker is the daemon version returned by the already-owned Engine client. A
  failed version probe renders `unavailable` without making the rest of Host
  health unreadable.

The host-level etcd source probes configured endpoints without exposing them.
The MVP node label is exactly `single-node`. All configured endpoints healthy
is `healthy`, a non-empty healthy subset is `degraded`, and none healthy is
`failed`. DB size is the largest healthy member DB size using the same IEC
formatter, or `unavailable` when no member answers.

The Agent source reads the durable singleton plus its current authenticated
health. Provisioning or updating is `pending`, ready with current health is
`healthy`, ready without it is `degraded`, deleting or no Agent is `stopped`.
Pull interval, concurrency, and sorted `key=value` labels come from the durable
Agent config; before enrollment they come from the Controller's bootstrap Agent
config. No credential, token, socket, endpoint, or container id enters this
projection.

The Controller row is `healthy` because a caller can receive the response, its
runtime is exactly `groundplane-controller.service`, and its version is the
shared build version. Expected etcd and Agent unavailability is data; malformed
or unreadable host sources still fail closed through the one error taxonomy.

The Console loads this response directly and exposes explicit loading/failure
states. It does not invent API addresses, scheduler timing, last-report ages,
etcd configuration, or Host mutations from unrelated fixture actions.

## Rejected alternatives

### Persist samples in etcd

Rejected because the MVP needs a current operator snapshot, not time-series
storage. Persistence would create retention and write-amplification policy with
no accepted product use.

### Sample CPU counters during each request

Rejected because an accurate utilization delta requires a delay or shared
sampling process. Normalized one-minute load is instantaneous to read,
deterministic, and sufficient for the bounded Host card.

### Return raw endpoints and probe errors

Rejected because endpoint topology and private diagnostics are local bootstrap
concerns. The human API exposes safe health state only.

### Keep Console fixture fallbacks

Rejected because fixture values would make an unavailable Controller appear
healthy and violate the Console-store contract.

## Consequences

- C01 remains Linux-specific, matching the MVP Controller and Agent runtime.
- Host values are deliberately display-grade and cannot replace monitoring or
  alerting.
- The Controller-managed loopback etcd endpoint is observed safely without
  exposing its address or expanding the one-node MVP topology.
- Changing a source, unit, rounding rule, or dependency-state mapping is a
  product-contract change.
