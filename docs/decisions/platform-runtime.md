# Platform runtime and observation

The [accepted Task replacement](task-execution-redesign.md#remote-agents-and-host-ownership)
requires host-specific execution compatible with future remote Agents. It does
not implement remote enrollment, lifecycle or placement. The single-host topology
and lifecycle below remain the current runtime boundary.

Status: Accepted architecture. Delivery and qualification status belongs in
[capabilities](../capabilities.md) and the linked feature documents.

Groundplane has one native Controller, one Controller-managed local Agent
container, one Docker host, and durable Task/work-item/attempt storage. The Controller owns
policy and durable intent; the Agent applies bounded workload procedures. This
document records the runtime boundary and the read-only observation paths that
depend on it.

Product authority: [Platform runtime](../features/platform.md),
[Services and Releases](../features/services-and-releases.md), and
[Tasks and logs](../features/tasks-and-logs.md).

## Compose execution

Groundplane uses `compose-go/v2` for the Compose model and the Docker Compose v2
CLI for validation and execution. Only the Agent invokes the workload runtime,
through the shared subprocess Runner. Groundplane owns project identity,
labels, execution plans, health gates, release switching, rollback, secret
materialization, and durable records; Compose remains an execution primitive,
not a second release controller or secret store.

The implementation must preserve the real Compose grammar rather than grow a
partial parser or a parallel Docker execution path. Runtime and Engine versions
are release inputs. A future runtime is a separate capability adapter rather
than compatibility branches in the Compose adapter.

Current source: [native Release execution](../../internal/agent/releaseruntime),
[typed host helpers](../../internal/infra/docker/composehelper), and
[Compose rendering](../../internal/controller/composerender).

## Independent etcd configuration activation

The native etcd lifecycle owns saved and applied documents separately. Otherwise
an unrelated Controller restart would activate a mere Save. Explicit Apply freezes
both the selected document and its predecessor in a native Task, outside Agent
execution. A local activation receipt survives store unavailability; interrupted
activation restores the predecessor before bootstrap opens etcd. Completed
activation replay verifies the selected runtime without another restart.

The shared [configuration document store](../../internal/infra/configdocument)
owns byte preservation, revision fences and protected replacement receipts.
[etcd lifecycle](../../internal/infra/docker/etcdcontainer) owns host effects;
[activation service](../../internal/controller/etcdconfig) owns publication.

## Controller-owned Agent

`groundplane-controller.service` is the only persistent Groundplane service.
It creates, configures, starts, health-checks, replaces, rolls back, reconciles,
and removes the local Agent OCI container through Docker. There is no Agent
systemd unit, bootstrap Compose project, or Agent self-management path.

The Agent runs with the Docker authority needed for validated workload
procedures, but receives only the accepted Docker socket, Agent state,
Controller channel, runtime configuration, and token mounts. It never receives
the host root, a generic runtime root, or workload materialization roots. Docker
restart policy does not replace Controller reconciliation.

Enrollment is an asynchronous operator capability across Console, CLI, and
API. The Controller creates the Agent record and credential, injects runtime
material, starts the container, and waits for authenticated readiness. Human
surfaces never return a token, certificate, bootstrap file, or approval
artifact. Removing the Agent fences assignments, terminates owned work under
the Task contract, revokes channel authority, and then removes runtime and
durable state.

Remote Agents, multiple hosts, mTLS, certificate rotation, and Agent-managed
updates are outside the MVP.

Current source: [Agent management](../../internal/controller/agentmanagement)
and [Agent registration persistence](../../internal/infra/etcd/agentregistration).

## Agent runtime and identity

The Agent image is immutable and selected by OCI digest. Assignment authority
binds the captured operation input to its Task, work item, attempt, host and
authenticated Agent generation. Retry retains the Task and work-item identity;
only an authorized new attempt can execute. Reconnect replays the same attempt's
durable checkpoints and reports rather than creating a new execution.

The Controller owns execution permission. The Agent's local operation journal
retains effects and exact unacknowledged results across interruption, but cannot
authorize replay of an ambiguous command. See the
[Task execution contract](task-execution-redesign.md#interruption-retry-and-cancellation).

Agent replacement is a Controller Task. It pauses new assignment admission,
requires the Agent to be idle, advances generation and credential authority,
replaces the digest-pinned container, and waits for authenticated readiness.
Failure restores the retained predecessor with a fresh generation. Uncertain
publication is revision-fenced and recovered before admission reopens; it is
never guessed from current container state. A successful rollback leaves the
Agent ready on the predecessor but the update Task failed.

Installed build identity belongs to the durable Ready transition, while capacity
and health belong to the live session. Combining them would erase an installation's
version after a Controller restart or relabel it with an unaccepted candidate.
Enrollment and replacement capture the matching authenticated build report in the
same lifecycle CAS; recovery verifies that report against the accepted identity.
Component image reads use retained acknowledged artifacts at the desired read's
fixed storage revision, never a mutable tag or a newer catalog.
The compiled catalog retains a verified selection tag alongside its index and
platform digests. That tag participates in catalog/plan identity and travels with
the retained Compose artifact, including historical rerenders. Installed-software
reads use this capture; they do not query the current tag to name old content.
Digest-only selections keep an empty tag. Native etcd captures its verified tag
in the owned container at creation and reads that capture independently of the
next bootstrap selection.
Optional version labels remain owned by that immutable local image configuration;
the bounded reader cannot change the accepted image/configuration ids. Missing
local metadata does not erase durable installation proof.

Native Controller build descriptors bind source provenance to the binary digest.
The preparation reader requires that provenance to match the frozen source
selection, and the release store carries it unchanged into activation history.
Installed etcd image metadata is read by immutable local image configuration id;
its live server-version response remains a separate observation.

Current source: [Agent channel](../../internal/controller/agentchannel),
[native assignment authority](../../internal/common/workexecution),
[durable operation journals](../../internal/infra/agentworkjournal), and
[Agent update orchestration](../../internal/controller/agentmanagement/update.go).

## Local Agent channel authentication

The local Agent uses one authenticated bidirectional gRPC stream over the
Controller-owned Unix socket. The socket directory is mounted read-only so a
recreated socket remains visible to the running container; the MVP exposes no
Agent TCP listener.

The Controller generates a high-entropy token, stores it encrypted under the
existing root age key, and maintains only a digest lookup for connection
resolution. It atomically materializes the token in a narrow root-owned runtime
file mounted read-only into the Agent. Tokens never appear in process arguments,
environment variables, Docker labels, logs, Task records, API responses, or
plaintext etcd values.

Authentication requires both the presented Agent id and the token digest to
resolve to the same current Agent generation. The token survives reconnect and
Controller-owned container replacement, and Agent removal is its revocation
operation. Missing, malformed, unreadable, or mismatched credentials fail
closed. Restoring etcd and the Controller age key is sufficient to rematerialize
the runtime file.

Current source: [channel transport](../../internal/controller/agentchannel/transport)
and [channel authorization persistence](../../internal/infra/etcd/agentregistration/channel_authorization.go).

## Serving-workload observation

Service list and detail reads may request a bounded live observation over the
existing authenticated Agent stream. This is a read-only exchange, not a Task,
watch, durable cache, log read, or reconciliation input.

The Controller freezes the serving Release, sealed expected replica count,
render identity, runtime intent, and acknowledged proxy authority at one
storage revision. The Agent lists and inspects only containers whose ownership
matches that frozen target. Selected workload replicas count; stable proxies,
scripts, Components, and retained releases do not. Addressable Services also
check the exactly owned stable proxy and acknowledged configuration, without
claiming end-to-end application reachability.

Backing provisioning does not create a Release. For a Backing Service, the source
is its acknowledged Environment artifact at the same fixed storage revision.
Its explicit backing target has no Release or slot and matches the provisioning
plan and generation. Desired configuration is never a substitute for that receipt.

Each Service result contains a complete bounded count partition and its selected
container rows, or is unavailable. Rows expose identity, image, state and
healthcheck status, never environment variables or healthcheck output.
Empty complete evidence means absent; malformed, duplicate,
overflowing, changing, timed-out, disconnected, or ownership-mismatched
evidence is unavailable rather than a partial healthy sample. The Controller
rechecks source revisions before returning the snapshot, and the Console
expires it locally. Desired reads still succeed when live evidence is
unavailable.

Public states are `unavailable`, `absent`, `failed`, `stopped`, `starting`,
`healthy`, `running`, and `degraded`. Runtime intent remains separate. There is
no new endpoint, mutation, Blueprint field, persistence schema, or compatibility
reader.

Current source: [shared observation contract](../../internal/common/serviceobservation),
[Docker observer](../../internal/infra/docker/serviceobserver),
[Controller source selection](../../internal/controller/serviceobservation), and
[Console projection](../../console/src/features/service/service-observation.ts).

The observation architecture is accepted. Full deployment and live
qualification remain tracked in the Service feature; this decision does not
claim them.
