# Release, packaging, and upgrade authority

Status: Accepted architecture with named qualification gaps. Packaging or
decision acceptance does not mark a platform or upgrade path release-ready.

Groundplane releases must identify executable bytes and managed images
immutably, support both accepted host architectures, preserve one-host
authority, and recover without making the code being replaced the only rollback
mechanism.

Product authority: [Safe updates](../features/upgrade-safety.md),
[Platform runtime](../features/platform.md),
[Services and Releases](../features/services-and-releases.md), and
[deployment](../deployment.md).

## Agent image

Groundplane ships one reproducible Agent OCI image. Its build pins the Go
builder, Docker CLI runtime, Compose v2 binary, and upstream manifest digests.
The final minimal image copies only the static Docker CLI, CA roots, the
accepted Compose plugin, and the Agent binary. It does not inherit a shell,
package manager, another Compose major, or host packages.

The persistent Agent and short-lived helpers use the same image and trusted
binary in different closed modes. The image runs as root because the accepted
Agent runtime needs Docker authority, has an explicit entrypoint and no inherited
command, and exposes Docker and Compose only to validated Agent procedures.
There is no helper image, Agent systemd service, bootstrap Compose project, or
host-tool fallback.

Version metadata comes from explicit release input, while deployment uses the
registry-readback digest rather than a mutable tag. Local build tags are handles,
not execution authority. Updating any toolchain, runtime, Compose version, or
base digest is a reviewed contract change.

Current source: [Agent Dockerfile](../../Dockerfile.agent) and
[Agent release script](../../scripts/release-agent.sh).

The earlier image qualification covered `linux/amd64` only. Current release
authority requires both supported platforms as described below; no other
architecture is claimed.

## Supported platforms

The MVP supports exactly `linux/amd64` and `linux/arm64`. Installation rejects
another operating system or architecture before host mutation. This is one
local host on either platform, not multi-host scheduling, mixed-architecture
placement, or emulation.

A Groundplane-managed image release is one OCI index digest with exactly one
runnable child for each supported platform. The release record binds repository,
index, platform, authenticated variant, child manifest, config digest, and any
Groundplane-added layers. An executable release identity refers to the complete
two-platform release, not to a host-specific alias.

Vendor-controlled indexes may contain unsupported or non-runnable descriptors,
but the compiled catalog binds exactly one accepted child and verified config
for each supported platform. Other descriptors are ignored and never become
fallback authority. Missing, duplicate, ambiguous, mismatched, or unsupported
platform evidence fails closed. Docker still selects only the child matching
the local compiled host architecture.

A release must prove the same operator journey on both architectures and on
the accepted Ubuntu and Debian host versions. Groundplane-controlled image
publication and compiled third-party catalog readback have separate evidence
requirements. The catalog correction and full dual-platform runtime proof
remain pending; this decision does not claim deployment.

Current source: [registered image catalog](../../registered-components/catalog)
and [Agent image validation](../../internal/agent/componentaction/validator_image.go).

## Minimum hosting scope

The MVP hosts many Tenants, Projects, and Environments on one machine. It does
not promise tenant network isolation on shared backing bridges; strict peer
firewalling is post-MVP.

Workload deploy resolves and pins an exact image identity already present in
the local Docker daemon before mutation. Deploy does not build, pull, or push
images. Groundplane-managed Agent, etcd, Component, and backing images remain
installation or release assets. Registry integration and registry hosting are
post-MVP.

Routes are generic Groundplane resources. A registered router consumes them;
it need not create them. A Route remains valid and unserved when no router is
enabled. Alternative routers would be source registrations, not runtime
plugins.

Backup and restore cover PostgreSQL Attach, configuration, and Volume sources,
and restore only to the original surviving Environment. They are not one
cross-source atomic snapshot. Valkey backup/restore remains required, but its
namespace-safe per-consumer form versus an explicit shared-instance RDB source
is unresolved; runtime must reject it as not implemented until that decision,
format, and proof land. Live data-directory archival is not authorized.
Automated empty-host disaster recovery is post-MVP and would need metadata,
Controller key, configuration, images, data backups, and bootstrap—not merely
an etcd snapshot.

Runner networking uses its own machine pool, disjoint from system and
Environment pools, subdivided into bounded per-Runner networks. This decision
does not claim Runner implementation or qualification.

`x-gp-release-groups` is the sole Blueprint grammar. A group coordinates 2–32
logical Services as one operator action but executes them serially, not as an
atomic distributed switch. Deploy resolves one requested or stored tag
independently for every member and pins each local image before publication.
Rollback chooses an eligible historical serving Release per member and rejects
the whole request before publication if any member lacks one. Members retain
their own histories and hooks; there is no group hook resource or simultaneous
switch guarantee.

Replica counts are durable and survive deploy, rollback, restart, and reapply.
Blue-green with more than one replica is rejected before mutation. Host ports
remain forbidden and restore remains original-target only.

These are documentation-contract corrections with runtime alignment pending.
They add no compatibility grammar, fallback behavior, delivery claim, or
acceptance evidence.

## Controller update and recovery

The Controller is not a Component. Its update is a native Controller Task, not
an Agent assignment, Script, uploaded binary, arbitrary path, URL, or shell
command. Application containers, routes, data, Agent identity, and queued
application Tasks survive the operation.

Deployment stages a root-owned immutable release directory. A bounded manifest
pins the Controller binary digest, display version, Agent image digest,
persistent-state epoch, and Agent-channel schema; the exact manifest bytes
define release identity. Both manifest and binary are rechecked before use, and
the candidate must be storage- and channel-compatible. Distribution and
signature policy remain deployment concerns rather than runtime upload paths.

Console, CLI, and API invoke the same protected update operation. The Task
freezes candidate, predecessor, Agent, and idempotency identity. Lost acceptance
is resolved from durable evidence, and only one native activation may be
active. Retry is a new explicit update after recovery, never blind replay of a
host effect.

Before activation, the Controller pauses Agent admission and drains current
work without aborting it. It retains the exact predecessor executable and
writes a bounded fsynced activation journal tied to the Task. A transient
systemd recovery service runs the predecessor's closed recovery mode, swaps the
candidate, starts it, and waits for the same Task to prove executing-binary,
listener, etcd, and authenticated-Agent readiness.

The candidate resumes the Task and brings the Agent to the release-pinned
digest through the normal Agent replacement contract. Failure restores the
Controller predecessor and, when changed, the prior Agent with fresh generation
authority. Recovery is replay-safe across interrupted swaps, repeated startup,
watchdog timeout, and reboot; rollback success reports a failed update rather
than false success.

An unfinished activation journal blocks ordinary mutations and scheduler
mutation passes, while reads and the closed read-only operations continue. The
native executor and recovery writes remain available, Agent dispatch stays
paused, and cancellation is accepted only before committed activation. This
is trial isolation, not permission for forward-only schema migration or an
arbitrary downgrade.

The last qualified manifest becomes the default Agent image for later
enrollment. Independent Agent update requests retain their own explicit digest
and recovery authority. A combined published upgrade performs Controller and
Agent operations in order without making the second failure roll back an
already qualified Controller.

Bootstrap installs the recovery executable, guard, and sole persistent unit.
After that baseline, routine deployment only stages immutable bytes and invokes
the protected endpoint; it does not rewrite service files, configuration,
identity, CLI, Runner state, or Agent independently. A private deployment
receipt retains the release, idempotency key, and accepted Task for explicit
resume after client interruption. It is not another Task journal.

Current source: [shared release values](../../internal/common/controllerupgrade),
[update orchestration](../../internal/controller/controllerupgrade),
[activation filesystem](../../internal/infra/controllerrelease), and
[deployment tool](../../scripts/deploy.py).

The upgrade architecture is accepted. Interrupted-boundary, rollback-under-
traffic, bootstrap, dual-platform, and live-host qualification remain evidence
requirements tracked by the feature and capability status documents.
