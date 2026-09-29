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

Each row represents one image ID. Names, compact tags, size and usage are shown
in the list; open the image for full references, Fetch history and protection
details. Several tags or repository names can point to that same image.
Click the usage badge to see container names and states, including stopped
containers. Matched Services and Environments link to their current Console
pages; containers outside GP are identified separately. Missing GP resource
records do not hide a container or imply that removal is safe. Other removal
protection, such as retained rollback inputs, appears separately from container use.
Sort images by name, creation date or size; the date is the image's creation time,
not when it was fetched. Pages default to five rows, with 5/10/25/50 choices.

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

The inventory also shows Fetch history: the requested tag, exact manifest digest,
request time and Task status. Only completed Tasks confirm a successful Fetch;
a failed attempt can leave downloaded content on the host. `nginx:latest` stays associated
with the digest that Fetch selected even if the registry later moves the tag.
This history lasts as long as its Task record; it is not a local Docker tag.

Fetch Task details show the requested image, selected platform, current
checking/download/verification stage and the recorded failure explanation.
Download counts cover layers reported by Docker so far, not a fixed overall
percentage; cached layers need no download. Progress survives Console reloads.
Full image digests and operation IDs are under technical details. Completion
means the image was verified locally, not that a Service was deployed.

Use `groundplane image ls` to inspect the same inventory. Removal uses
`groundplane image remove sha256:...` and creates a Task for that exact local
image ID, never a mutable name. The API exposes `GET /images`, `POST /images/fetch`
and `DELETE /images/{id}` under its ordinary version prefix.

Removal rechecks containers and retained Release/runtime material, including
child images inside an OCI index. Terminal Component and Blueprint attempts
protect their captured candidate and predecessor images, not unrelated images.
Terminal Runner creation retains its Runner's exact image; retry requires a fresh
registration token. Active work, or a Task whose complete image dependencies
cannot be determined, still blocks removal. Missing or corrupt retained inputs
never authorize deletion.
The Remove action deletes the selected image and its local tags together.
Multiple tags alone do not block removal. GP resolves each repository through
Docker's exact content descriptor, removes those references without force, and
requires final exact-ID deletion to succeed. It never removes by mutable tag.
An interrupted operation may remove some references before failing; Retry
continues against the same image ID, not whatever a tag points to later.

Protected images keep a disabled Remove action; open their details for the reason.
Docker still rejects container/dependency conflicts. If Docker does not expose
the content descriptor needed to remove aliases safely, only its ordinary
exact-ID deletion is available. There is no force removal, bulk prune or registry
garbage collection. Registry content and application data are untouched.

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
