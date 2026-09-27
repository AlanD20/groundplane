# Runners

**The complete isolated Runner lifecycle is not qualified or fully integrated.**
This guide records the accepted capability and operator constraints; it is not a
claim that current GP can safely host production CI jobs.

A Runner is a persistent GitHub Actions self-hosted runner owned by a Tenant,
Project or Environment. It is not a Project or an ordinary Agent workload. The native Controller
owns its local lifecycle and registration.

## Creation and limits

Creation selects one Tenant, Project or Environment owner, a canonical GitHub repository or
organization URL, a slug, optional custom labels and a fresh short-lived GitHub
registration token. Obtain the token through GitHub settings; GP does not create
or fetch it.

A Tenant can have at most five Runner records across all three ownership scopes.
Provisioning, failed and deleting records count until cleanup releases them.
Host identity slots, subordinate UID/GID ranges and network space are also finite;
exhaustion reports `resource.in_use`, not automatic pool expansion.

Only the slug is editable. Owner, GitHub URL, GitHub-visible name, labels and the
release-selected image are immutable. Renaming does not re-register the Runner.
There is no in-place Runner update, autoscaling, ephemeral pool, multi-host
placement or GitHub Enterprise support in the MVP contract.

## Tokens and Retry

The registration token is transient. GP does not save it in Tasks, logs, replay
records or durable runtime configuration. If the Controller exits before
registration is proved, recovery cannot reconstruct the token.

Failed creation retains the Runner and its allocations. Use Runner-specific
Retry with a **fresh token**; generic Task Retry cannot retry Runner creation.
Retry keeps the same Runner ID, quota claim, host slot and subnet. Replaying the
original request does not replace the token in an existing attempt.

## Isolation

Each Runner owns a dedicated rootless Docker daemon, unprivileged host identity
and distinct subordinate UID/GID ranges. It receives that daemon's socket, never
the host Docker socket. Jobs and service containers stay inside its user namespace.
Jobs use ordinary Docker commands directly; no custom Docker proxy filters them.
Docker privileges and networking options are bounded by the rootless daemon's
namespace, not host-root authority. Only trusted workflows may run here.

Each Runner gets a `/29` from the separate `runner.network_pool`, configured as
a `/24`, `/25` or `/26`. It joins no Environment, backing, platform or other
Runner network. Egress policy blocks direct access to GP private pools except the
private Controller API and local registry endpoints while allowing DNS and ordinary internet access. Same-Tenant
ownership does not permit communication with another Runner.

## GP command access and image delivery

**Accepted, not yet integrated:** a Runner executes ordinary GitHub Actions
workflows. Jobs may use the existing GP CLI or private API with full operator
authority. Tenant, Project and Environment ownership organizes Runners and their
lifecycle; it does not limit their GP commands or accessible resources.

There is no Runner-specific API gateway, command allowlist or permission system.
Normal validation, deletion approvals, concurrency and replay checks still apply.
Only trusted workflows should run here: they can manage other projects, shared
Backing Services, Secrets and host-wide GP operations. Direct network isolation
does not prevent actions that a workflow requests through GP.

The image includes the GP CLI. Jobs receive `GROUNDPLANE_HOST` pointing to the
first trusted private address in the Controller's `listen.http` configuration.
A loopback-only Controller cannot create or retry a Runner: configure a private
listener first. No public listener or API relay is created automatically.

The API remains private; Runner access does not require public exposure.

**Accepted, not yet integrated:** jobs build and push with ordinary Docker commands
to a private registry on the same host. CoreDNS supplies its internal name to both
the Runner's rootless daemon and host Docker. An explicit GP image-fetch operation
then loads the selected image into host Docker before ordinary Service Deploy.
No external registry or public DNS configuration is required.

The working implementation exposes **Host → Fetch image** in the Console and
`groundplane image fetch <reference>` in the CLI. Use an explicit tag or digest
under `registry.groundplane.internal:5000`. Acceptance returns a Task and its
selected immutable reference; it does not mean the image is available yet.
The response's `config_digest` identifies the pinned OCI configuration; it is
not Docker's store-specific local image ID.
Wait for the Task to complete, then use that reference in ordinary Deploy.
Task Retry keeps the original selection, even if the tag has moved. A new Fetch
is a new selection. The CLI's `--idempotency-key` lets automation resolve uncertain
acceptance without creating another operation. This path is not live-qualified.

The two Docker daemons have separate image stores: a successful build or push
alone does not make an image available for GP Deploy. Fetch must complete first.
Failed build, push or fetch leaves the serving Release unchanged. Removing a
Runner preserves shared registry data and application images. See the
[runtime design](../decisions/runner-isolation.md) for identity and access rules.

## Status and removal

Online status comes from current local runtime observation, not an editable flag.
Missing, stale or mismatched evidence reports offline. Reboot recovery must prove
new runtime ownership rather than reuse stale process identities.

Removal cleans up only ownership-proved local resources. Failed cleanup retains
the record and allocations needed for retry. **It does not deregister the Runner
in GitHub**; remove that separate registration manually.

## Design and qualification

[Runner isolation decisions](../decisions/runner-isolation.md) explain the process,
network, token and cleanup boundaries. The [QA matrix](../qa-matrix.md) retains
the behavioral cases needed before this isolation can be relied on. See
[current limitations](../capabilities.md) for the release boundary.
