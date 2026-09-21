# Host, Controller and Agent

Use Platform → Host for host capacity, Controller access and the Agent table.
Controller and Agent are not Components and have no separate sidebar sections.

## Host health

Host is a read-only snapshot, not a settings resource or monitoring history.
It reports hostname, architecture, OS, uptime, CPU load, memory/swap, root disk
usage and Docker/etcd/Agent status. Capacity uses binary units. Expected dependency
unavailability is reported as unavailable or degraded rather than replaced with
fixture data. Raw endpoints, tokens and internal diagnostics are not public.

A healthy Controller row means that the Controller served the request. It does
not prove application health. Likewise, container presence and socket keepalive
do not prove that an Agent is accepting work; its authenticated Ready reports do.

## Controller configuration

The Controller detail at `/platform/host/controller` shows its startup YAML.
Read the current content and revision before replacing it. Saving validates the
complete document and atomically replaces the file, preserving supplied comments
and formatting. Stale writes conflict.

Saving does not hot-reload the process. `restart_required` remains true until
restart or exact revert. The startup file and host key are bootstrap state outside
Environment Blueprints and need separate operational protection.

Use `controller config show` and `controller config set --file PATH` from the
CLI. The matching API is `GET/PUT /controller/config`.
See [installation](../deployment.md) before changing listeners or host paths.

## Agent enrollment and settings

`agent join` creates the local Agent through a Controller Task. GP generates
and delivers the local channel token privately; the operator does not exchange
certificates or tokens, and there is no separate approval state.

The Agent detail at `/platform/host/agents/{id}` owns runtime settings.
Configuration changes drain existing work and apply at an idle Ready without
restarting running Tasks. The Controller owns Agent container creation,
replacement, removal and recovery after Docker returns. The Agent never updates
itself.

Removing an Agent stops new assignments, cancels active work, revokes its identity
and removes its owned runtime. Updating an Agent requires an explicit immutable
image and idle-work admission; it must not silently abort application work.

## Restart and update boundaries

Normal Controller restart may restart the Agent but must leave etcd running.
Application requests, held connections and data must survive.
After a host reboot, GP must restore service without manual repair; reboot itself
cannot be interruption-free.

Software updates use [the update workflow](upgrade-safety.md), not application
Scripts. The Console may reconnect during Controller replacement while the
update Task retains its identity and result.

## Design and qualification

See [architecture](../architecture.md#processes-and-data-flow) for bootstrap
ownership and [technical decisions](../decisions/README.md) for channel identity.
[Capability status](../capabilities.md) owns current qualification limits.
Historical host readings are not evidence of today's host health.
