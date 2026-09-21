# Services and Releases

A Service is an Environment's declared workload. Saving its configuration changes
desired state; it does not prove that the workload has been deployed. A Release
records the exact inputs and outcome of a Deploy or Rollback. Live workload health
is reported separately from both.

## Runtime actions

- **Deploy** applies the selected Service's configuration and image using the
  chosen strategy. Explicit selection also permits a configured-only Service
  whose Compose profile was not enabled; it does not start other profile members.
- **Start**, **Stop** and **Restart** operate on the whole logical workload set.
  Stopped intent must survive unrelated configuration changes.
- **Destroy** removes runtime, not the Service's configuration or persistent data.
- **Remove** checks dependencies and performs its own cleanup Task. It leaves the
  Service visible until cleanup succeeds and does not delete Volumes or Release
  history. Resolve Route, Attach, group and other reported references first.

## Selecting an image

The image must already exist on the host. GP does not pull or build an application
image during Deploy. The operator chooses a tag, not a different image repository.
The default is the Service's current tag. GP resolves that reference once and
pins the local image identity for the accepted operation.

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
| `recreate` | Stops the previous set before starting the requested replica count. Downtime is expected. Success requires the full selected set, not one healthy replica. |
| `rolling` | Not implemented; rejected. |

Switching between blue-green and recreate is accepted only when both the previous
and candidate counts are one. Replicated blue-green is outside the MVP. Start,
Stop, Destroy and recovery must address the complete captured set.

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

## Design and qualification

The [release and recovery decisions](../decisions/service-release-and-recovery.md)
explain immutable selection, runtime ownership and recovery proof. The
[observation decisions](../decisions/platform-runtime.md) explain live reporting.
Use the [Blueprint reference](../blueprint.md) for authored fields.

The production restructuring is not freshly qualified. Historical results do not
prove every strategy, replica count, reboot or failure combination on current
source. Remaining cases are tracked in the [QA matrix](../qa-matrix.md) and
[current limitations](../capabilities.md).
