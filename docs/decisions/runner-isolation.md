# Runner runtime and local image delivery

Status: implemented; selected live delivery qualified in
[H75](../acceptance.md#runner-delivery). Full isolation/lifecycle qualification remains open.

## Trusted workflows, separate Docker engines

A Runner is a normal persistent GitHub Actions runner for trusted workflows.
Tenant, Project or Environment ownership controls organization and lifecycle,
not GP permissions. Jobs use the existing private GP CLI/API with full operator
authority. There is no second command gateway, API permission system or Docker
filtering proxy. This is not a sandbox for hostile workflows.

Each Runner has an allocated host user, subordinate UID/GID ranges, home,
dedicated rootless Docker daemon and private network. The listener container
runs on host Docker as that allocated non-root user, with a read-only root
filesystem, dropped capabilities and no-new-privileges. It receives the dedicated
rootless daemon's socket, never the host Docker socket or Agent credentials.
Its workspace has the same absolute path inside and outside the listener so
ordinary Docker bind mounts refer to the actual checked-out files.

Docker builds, job containers and service containers run in the dedicated
rootless engine. Docker options act inside its user/network namespace; GP does
not maintain a parallel Docker API implementation or request allowlist.
Rootless operation limits direct host privileges, but does not restrict what
trusted jobs can request through the GP API.

## Native lifecycle and ownership

The native Controller owns Runner allocation, registration, local observation
and cleanup. It uses the existing host adapters, not a new privileged helper
protocol or an Agent execution role. Release artifacts provide the immutable
Runner image and required host tools.

Allocation atomically reserves the host identity, subordinate ranges and network.
Creation records its plan and issued operations before effects. Recovery observes
uncertain effects before retrying; a name alone is not permission to adopt or
delete an existing resource. A conflicting identity fails without modifying the
foreign resource. Failed cleanup retains the Runner and its allocation.

The rootless daemon and listener must recover after a host restart without a
fresh registration token. Process and socket identities must be observed again;
stale process evidence cannot establish online status. GP does not infer GitHub
job readiness merely from successful container creation.

Removal stops the listener, then its build daemon and owned resources. Keep
network restrictions until the Runner processes are gone. Cleanup affects only
that Runner; shared registry content and hosted application images survive.

## Registration and restart

A fresh GitHub registration token is request-only Controller memory, consumed
once by the configure process over attached input. Do not put it in container
configuration, command arguments, desired state, journals or logs. Jobs and the
long-running listener must not inherit it.

The configured Runner home retains GitHub's normal registration credentials.
These are not the transient registration token. A normal restart uses that
registration without reading another token from stdin. If an interrupted
registration cannot be proved complete, report that a fresh token is required;
do not replay an uncertain token delivery. Removal remains local and does not
deregister the Runner in GitHub.

## One private registry and existing DNS

Build results cross the engine boundary through a private OCI registry on the
same host. Runners use normal Docker build and push. A separate explicit GP
image-fetch operation loads the selected content into host Docker; ordinary
Service Deploy then resolves a host-local image as before.

The registry is shared host infrastructure, not owned by one Runner or its
project. Keep its data across Runner removal and GP restart. Do not add automatic
image deletion or garbage collection to Runner cleanup.

Reuse CoreDNS to resolve the registry's stable internal name to its private
address. Both the rootless daemon and host Docker need that resolution; configuring
only the job container is insufficient. CoreDNS must be reachable on the required
private interface without becoming a public resolver. Registry traffic stays
private and does not traverse application Routes or a public Tunnel.

Registry credentials and TLS trust are managed inputs. Name resolution does not
authenticate a registry, and an internal name does not justify disabling TLS
verification. Do not give a job the host Docker socket to work around delivery.

## Explicit fetch, unchanged Deploy

The fetch operation has one API endpoint, CLI command and Console action. It
resolves a requested image to registry content and retains that identity for the
accepted operation and its retries. Inspect the resulting host image before
reporting success. A tag moving during a retry must not silently change content.

Fetch does not edit a Service or start a Release. After successful push and fetch,
the workflow requests ordinary GP Deploy. Failed build, push or fetch leaves
the serving Release and application data untouched. This keeps one deployment
pipeline and makes local image delivery usable independently of Runner ownership.

## Implementation and qualification

The current code contains allocation, Controller Tasks, a runtime-plan journal,
a transient token broker and local Docker/host adapters. H75 records the selected
real GitHub delivery journey; it does not establish full isolation or reboot
recovery. Current limitations belong in [capabilities](../capabilities.md);
behavioral acceptance cases live in [the QA matrix](../qa-matrix.md#isolated-runners).

Code owners are [Controller lifecycle](../../internal/controller/runner),
[runtime planning](../../internal/core/runner),
[host operations](../../internal/infra/docker/runner),
[local journal](../../internal/infra/runnerjournal), and
[durable ownership](../../internal/infra/etcd/runner_runtime_ownership.go).
