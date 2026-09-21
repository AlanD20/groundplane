# Runners

**The complete isolated Runner lifecycle is not qualified or fully integrated.**
This guide records the accepted capability and operator constraints; it is not a
claim that current GP can safely host production CI jobs.

A Runner is a persistent GitHub Actions self-hosted runner owned by a Tenant or
Project. It is not a Project or an ordinary Agent workload. The native Controller
owns its local lifecycle and registration.

## Creation and limits

Creation selects one Tenant or Project owner, a canonical GitHub repository or
organization URL, a slug, optional custom labels and a fresh short-lived GitHub
registration token. Obtain the token through GitHub settings; GP does not create
or fetch it.

A Tenant can have at most five Runner records across direct and Project scopes.
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
Host networking, privileged mode, `CAP_NET_ADMIN` and published host ports are
forbidden.

Each Runner gets a `/29` from the separate `runner.network_pool`, configured as
a `/24`, `/25` or `/26`. It joins no Environment, backing, platform or other
Runner network. Egress policy blocks GP private pools except the authenticated
Controller endpoint while allowing DNS and ordinary internet access. Same-Tenant
ownership does not permit communication with another Runner.

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
