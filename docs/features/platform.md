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

## Images

**Host → Images**, also linked from the Agent detail, lists the local Agent's
Docker images, tags, digests, size and container count. This is a live daemon
observation, not the registry catalog. Counts include stopped containers; zero
does not mean an image can safely be removed. The MVP has one local Agent host.

Fetch accepts an explicit tag or SHA-256 digest from a public registry or GP's
managed private registry. For example, `groundplane image fetch nginx:latest`.
Public registries use anonymous access over HTTPS. Only GP's registry receives
its installer-managed credentials; external private-registry logins are not
supported.

Fetch returns an immutable reference and a Task. Wait for completion, then select
that reference when creating a Service or deploying it. Fetch does not move local
tags, change a Service or start containers. Retry retains the accepted content;
a new Fetch can select newer tag content. Deploy's optional `--image` chooses a
reference for that Release without editing the Service; it cannot accompany
`--tag`. Service forms and Deploy offer the images currently on the host.

Use `groundplane image ls` to inspect the same inventory. Removal uses
`groundplane image remove sha256:...` and creates a Task for that exact local
image ID, never a mutable name. The API exposes `GET /images`, `POST /images/fetch`
and `DELETE /images/{id}` under its ordinary version prefix.

Removal rechecks containers and retained Release/runtime material, including
child images inside an OCI index. Unfinished or retryable host Tasks conservatively
block removal until they complete or their retained records expire. Docker may
also refuse images with dependent images or multiple references. There is no
force removal, bulk prune or registry garbage collection. Registry content and
application data are untouched.

During removal, new image selections are rejected and pre-removal selections
cannot publish stale work. If Docker's deletion result is uncertain, GP keeps
the durable fence. Retry that removal Task to settle the operation; do not reset
history or delete its fence manually.

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
