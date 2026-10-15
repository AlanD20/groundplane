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

Agent details expose the last acknowledged installed image separately from a
desired update image. During provisioning or replacement, installed identity is
unavailable until that generation is accepted; a prepared candidate is not an
installed version. A degraded Agent can still have a recorded installed identity,
which does not prove that its container is currently running.

The Agent's accepted version and source commit survive Controller restarts;
health still requires a fresh authenticated report. Source builds lead with their
recorded short commit, while release builds lead with their version. Full commits
and immutable artifact identities remain available in details. CoreDNS, Caddy and
Tunnel images come from their retained runtime acknowledgements, not a new desired
configuration or the current image catalog. Optional version/build labels are
read only from that exact immutable local image configuration. A tag captured at
installation remains visible as “Installed from tag” even after the registry tag
moves. It describes the installed selection; it does not claim the tag still
resolves to those bytes. Missing tags and version labels use a compact digest.
An unavailable metadata lookup retains the acknowledged image and captured tag.
etcd's image, captured installation tag and optional image-version label
come from its owned container and immutable local image configuration. Its reported
server version is a separate status observation, unavailable when that response
cannot establish it. Neither comes from the next selected bootstrap image.

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

## Host workspace

Host Overview brings together capacity, Controller and etcd health, CoreDNS and
Agents. Open each resource for its actions. Controller has Overview and Startup
configuration sections. Platform Settings → Software updates owns Controller-only,
Agent-only and combined updates, including preparation, Apply, staged Controller
releases and direct immutable Agent images. Host resource pages link to that
workspace; runtime configuration remains with each resource.

Host Components lists CoreDNS. Environment routers and tunnels remain in their
Environment workspace. CoreDNS has Overview, DNS records and Resolver settings
sections, with configuration drafts retained when switching sections.

## CoreDNS connection checks

Open CoreDNS → Overview → Test DNS. The check queries the running resolver and
each upstream in its acknowledged configuration; unsaved settings are not tested.
Saving configuration remains separate. CoreDNS must be enabled and its
configuration Task settled before a check can start.

Default queries use root NS for the default upstream and an A lookup of each
forwarding domain. Optional query names can override one query per domain, using
A or NS. The name must select that domain under the configured longest-suffix
rules. GP chooses the endpoints; operators cannot inject arbitrary servers.

The result records the queried name, server, answer samples, total answer count,
elapsed time and whether the query passed through CoreDNS or went directly to an
upstream. NXDOMAIN and an empty answer differ from refusal, timeout and connection
failure. The Console provides search, sorting and pagination; execution details
identify the exact applied configuration and Agent generation checked.

Each query is limited to two seconds, within a one-minute query budget. The
check does not change configuration or reload CoreDNS. GP records the result
under its Task and marks it stale when its captured sources change. Opening a
page or refreshing results only reads stored evidence. An interrupted check is
not replayed by Task Retry: start a new check explicitly.

CLI: `component check COMPONENT_ID`, optionally with repeated
`--query domain=name:A` or `--query domain=name:NS`; `component check-result
COMPONENT_ID` reads the latest retained check, or `--task TASK_ID` selects one.
The matching API is `POST/GET /components/{id}/dns-checks`. These surfaces are
implemented locally; live DNS and interruption qualification is still pending.

## Host terminal

The Console's top-bar Terminal action opens a root shell on the Controller host.
Anyone who can reach the Console can use it; there is no separate login or token.
Keep Console/API listeners restricted to trusted private clients. This is host
access, not a container shell or a Task log.

The bottom panel can be resized or maximized within the browser tab. Its tabs
hold independent shells; use New tab to open another, select a tab to switch,
and right-click a tab (or long-press on touch) for Rename, Close tab and
Terminate session. Each tab also has its own close button. The host permits four active sessions
total, shared by Console and CLI operators.

Switching tabs or hiding the panel keeps shells and their output running. Closing
an active tab asks to terminate its shell and removes the tab only after the
session ends. Terminate session asks for confirmation and ends only that tab's
shell and its owned foreground/background processes, retaining its output until
the tab is closed. Closing an already-ended tab needs no confirmation.
Reloading the Console reconnects available tabs for the owning browser session;
it never silently opens replacement shells. A Controller restart terminates
these transient sessions. Open a new tab explicitly to start a new shell.

Platform Settings configures operator-input inactivity: 15 minutes by default,
from 1 minute to 8 hours. Save takes effect after the next Controller restart.
Shell output does not refresh the timeout. Commands and output are not logged;
only a bounded in-memory output buffer supports reconnect.

CLI: `groundplane host terminal` requires an interactive TTY. It uses the same
terminal backend. The API's Open request returns its WebSocket continuation;
see [request replay](../api-cli.md#protected-requests-and-uncertain-outcomes) for its transient boundary.

## etcd configuration

Open Host → etcd to edit its YAML. Save validates and atomically stores the
document without restarting anything. Apply is a separate action against the
saved revision; unsaved text is not applied. It creates a native Controller Task
and briefly restarts only etcd. Control-plane requests may reconnect; application
containers are left running. A normal Controller restart does not apply pending
etcd edits.

The editor accepts quota, snapshot count, heartbeat/election timing and log
level. GP owns history compaction; etcd's independent auto-compactor is not
configurable because it cannot honor GP's protected capture revisions.
Node identity, listeners, data path, image
and GP's transaction capacity remain bootstrap-owned; unknown keys are rejected.
The document is limited to 8 KiB. Defaults and bounds live in the
[configuration schema](../../internal/common/config/etcd.go).

If activation fails, GP restores the exact predecessor settings using the same
data directory. An interrupted activation has an independent local receipt so
bootstrap can restore etcd before reading the Task store. Failed Apply requires
a new explicit Apply, not Task Retry. Local recovery checks are not live workload
continuity qualification.

CLI: `controller etcd config show`, `set --file PATH`, and `apply`.
API: `GET/PUT /etcd/config` and `POST /etcd/config/apply`.

### Storage maintenance

Host → etcd reports database quota headroom, backend usage, reusable space,
current key/value payload and separate WAL size. It also reports due cleanup
counts, the last cleanup pass, compaction/defragmentation results and any
protected revision with its owning operation. Due counts include records still
held by recovery or request replay; they are not permission to delete them.
`host show` and `GET /host` expose the same status.
Quota headroom uses the whole backend size: reusable pages are not additional
quota capacity until defragmentation returns them to the filesystem.

Configure the `maintenance` block in Controller startup YAML through its existing
Console editor, CLI or API. Save validates; a Controller restart activates the
policy without restarting etcd. The default diagnostic lifetime is 90 days, the
recent-history window is one hour, and cleanup runs every minute with a two-second
work budget shared across independent pruning owners. Scans retain their progress
across interruptions and rotate past held records without weakening their guards.
See the [maintenance schema](../../internal/common/config/maintenance.go) for
supported settings and bounds.

For example, retain disposable diagnostics for one day while preserving the
default Task/replay lifetime:

```yaml
maintenance:
  diagnostic_retention: 24h
  cleanup_interval: 1m
  history_retention: 1h
  defrag_start_utc: "03:00"
  defrag_window: 15m
```

The Console refreshes this status every 15 seconds while the page is visible.
Refresh does not replace an unsaved configuration draft. If the API is temporarily
unavailable, the last observation remains visible with a refresh warning.

Compaction discards eligible historical versions, not current application records.
Unfinished captures and Blueprint/deletion preparation protect their required old
views. Defragmentation reclaims the resulting reusable file space only above its
configured byte and percentage thresholds, within a UTC maintenance window.
The default window starts at 03:00 UTC and lasts 15 minutes. Each pass is bounded
to 30 seconds; etcd requests may briefly pause, but application containers and
their data stay untouched. Local implementation is not live qualification.

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
in the list; open the image as a full workspace for Overview, Containers, Tags & digests,
and Fetch history. Its URL retains the selected image and section. Links to
Services and Environments provide a return to the image; returning to the image
list preserves its search and usage filters. Several tags or repository names can point to that same image.
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
the current local tag or that exact reference when creating a Service or deploying
it. A successful tagged Fetch assigns that local Docker tag to its verified
content; a new successful Fetch can move the tag. It does not change a Service
or start containers. Retry retains the accepted content and cannot move a tag
backward over a newer accepted selection. Deploy's optional `--image` chooses a
reference for that Release without editing the Service; it cannot accompany
`--tag`. Service forms and Deploy offer the images currently on the host.

The inventory also shows Fetch history: the requested tag, exact manifest digest,
request time and Task status. Only completed Tasks confirm a successful Fetch;
a failed attempt can leave downloaded content on the host without changing the
previous local tag. History records which digest each Fetch selected; it does
not imply that an older image still owns a tag moved by a later Fetch. This
history lasts as long as its Task record. Current tags appear separately, with
one inventory row and picker choice per image. Older content remains selectable
by digest. Running containers and retained Releases keep their exact images.

If Docker's tag-write outcome cannot be verified, GP blocks a new selection of
that tag until the original Fetch Task is retried and its outcome is resolved.
It never treats an uncertain write as proof that the previous tag survived.

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
