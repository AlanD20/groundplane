# Services and Releases

A Service is an Environment's declared workload. Saving its configuration changes
desired state; it does not prove that the workload has been deployed. A Release
records the exact inputs and outcome of a Deploy or Rollback. Live workload health
is reported separately from both.

## Runtime actions

- **Deploy** applies the selected Service's configuration and image using the
  chosen strategy. Explicit selection also permits a configured-only Service
  whose Compose profile was not enabled; it does not start other profile members.
- **Start** and **Stop** operate on the whole logical workload set.
  Stopped intent must survive unrelated configuration changes.
- **Destroy** (**Remove containers** in the Console) removes runtime, not the
  Service's configuration or persistent Volumes. Container-local files are lost.
- **Remove** (**Delete Service** in the Console) checks dependencies and performs its own cleanup Task. It leaves the
  Service visible until cleanup succeeds and does not delete Volumes or Release
  history. Resolve Route, Attach, group and other reported references first.

Start, Stop and Destroy use the configuration GP last acknowledged for that
workload, including a workload first started through Blueprint Apply. Pending
Service edits are not deployed by these actions; use Deploy to apply them.
A Service without an acknowledged runtime can record running intent through
Start without creating containers. Deploy or Blueprint Apply creates its first
runtime. There is no standalone Restart action; the `restart` configuration
field controls Docker's automatic restart policy.

## Editing configuration

The Console, CLI and API edit the same authored desired state. Command and
entrypoint are literal argument arrays, not shell snippets. Reset clears the
override and uses the image default; an empty argument within an array remains
an argument. Working directory and container user also have explicit resets.
Network aliases, dependency conditions/phases and log rotation are editable
alongside the existing runtime, networking and storage settings.

Save does not restart containers. Use Deploy, or
[apply the selected Service from its Blueprint](blueprints.md#apply-one-service),
to run the saved settings. Both honor the declared release strategy; a Deploy
may explicitly override it for that Release. Entries, file configuration and
Secrets keep their dedicated actions rather than a second raw environment editor.

In API PATCH requests, omitting a new setting preserves its authored value.
An empty `command` or `entrypoint` array resets to the image default; empty
`working_dir` or `user` clears that override. Empty `aliases`, `depends_on` or
`logging` objects clear those settings. Use CLI `service edit --help` for its
argument, reset and structured-file flags.

## Selecting an image

The image must already exist on the host. GP does not pull or build an application
image during Deploy. Use [Host Images](platform.md#images) to fetch missing
content first. Deploy accepts either a complete image reference (`--image` in
the CLI) or a tag within the configured repository (`--tag`), never both.
Without an override, it uses the Service image and current tag. Selecting an
image for one Deploy does not edit the Service's saved configuration. GP resolves
the reference once and pins the local image identity for the accepted operation.

Redeploying the same tag can apply configuration changes. If that local tag has
been moved to different bytes, a new Deploy can select those bytes; an existing
Release never changes with the tag. Retry retains its original image and inputs.

## Strategies and replicas

The strategy chosen for a Deploy overrides the declared default for that Release
only. An omitted authored replica count means one; accepted execution captures an
explicit positive count.

| Strategy | Behavior and limits |
| --- | --- |
| `blue-green` | One replica and an addressable internal TCP port are required. GP starts the inactive slot, waits for candidate health and switches the stable proxy. The old healthy slot is retained. Live and restart proxy configuration must agree. |
| `recreate` | Stops the previous set before starting the requested replica count. Downtime is expected. Every selected container must be running; configured healthchecks must also pass. Without a healthcheck, GP reports running, not healthy. |
| `rolling` | Not implemented; rejected. |

After a blue-green switch, both slots remain running. The next Deploy replaces
only the inactive slot, then switches traffic after candidate health passes.
An existing connection can finish on the previous slot until that slot is
replaced by a later Deploy. On the first switch from recreate to blue-green,
GP removes the obsolete singleton instead of retaining a third workload.

Switching between blue-green and recreate is accepted only when both the previous
and candidate counts are one. Replicated blue-green is outside the MVP. Start,
Stop, Destroy and recovery must address the complete captured set.

An existing blue-green proxy cannot change its Zones, network aliases, ports or
restart policy during a traffic switch. Save may record those settings, but Deploy
and Blueprint Apply reject that rollout rather than recreating the proxy silently.
Keep its topology unchanged or explicitly choose recreate with its expected downtime.

## Rollback

By default GP selects the newest successful Release that actually served and has
a different tag from the currently serving Release. A same-tag redeploy does not
advance that rollback point. An explicit tag selects an eligible historical
Release, not today's image bytes under that tag.

Rollback creates a new Release using the selected historical image, replica count
and strategy. It does not rewrite history or reverse database migrations. Missing
or expired historical authority is an error, not permission to reconstruct it from
current configuration.

## Release Groups

A group contains an explicit ordered list of 2–32 Services. Members run serially,
not as an atomic simultaneous traffic switch. All required pre-deploy hooks must
finish and clean up before the first member starts.

For group Deploy, a request tag overrides the saved group tag. If neither exists,
GP rejects the request; it does not fall back to each Service's current tag.
Each member resolves that tag against its own image before publication.

For group Rollback, the saved group tag is ignored. An omitted request tag selects
each member's newest eligible different-tag Release. A supplied tag must be
nonblank and have no surrounding whitespace. Every member needs an eligible
source or the whole request is rejected.

The Console previews the selected Service, Release and tag in group order before
confirmation. Changing the input invalidates that preview. A stale preview must
be refreshed and confirmed again; clients do not infer eligibility from history.
API and CLI callers can select against current state without a preview revision.

Both Deploy and group execution default to `switch_back` on failure. This restores
the exact captured predecessor; already-switched group members are restored in
reverse order. `leave_active` leaves completed switches for an explicit operator
decision. Neither policy undoes a migration or other Script side effect.

## Failure and recovery

Recovery belongs to the original Task and preserves its primary failure. Its
targets are each Service's captured serving predecessor or proved first-candidate
absence, never a later desired revision. The accepted configuration recovery
contract also restores that Task's pinned pre-operation files, not databases or
a machine snapshot. Secret values needed by a recoverable Task cannot be deleted.

Unknown ownership or missing proof can prevent safe completion even when the
Agent is connected. GP must report the conflict instead of declaring success or
touching unrelated workloads. See [Task status](tasks-and-logs.md).

## Reading health

Observed counts describe the serving workload and its expected replicas. A running
container without a healthcheck is not evidence of application health. Addressable
Services also require their stable proxy. Observations become stale after 15
seconds from the request's start; disconnection or identity mismatch is reported
as unavailable. Route reachability is a separate observation.

Backing Services use their acknowledged provisioning configuration rather than a
Deploy Release. Their containers are observed with the same freshness and exact
ownership checks; a saved definition alone cannot produce a healthy status.

## Design and qualification

The [release and recovery decisions](../decisions/service-release-and-recovery.md)
explain immutable selection, runtime ownership and recovery proof. The
[observation decisions](../decisions/platform-runtime.md) explain live reporting.
Use the [Blueprint reference](../blueprint.md) for authored fields.

The production restructuring is not freshly qualified. Historical results do not
prove every strategy, replica count, reboot or failure combination on current
source. Remaining cases are tracked in the [QA matrix](../qa-matrix.md) and
[current limitations](../capabilities.md).
