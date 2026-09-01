# ADR 0059: Non-resumable transient Service and Environment log streams

- Status: Accepted
- Date: 2026-08-29

Workload-authored Docker stdout/stderr is forwarded unchanged except the
contract's UTF-8 normalization and 32 KiB line truncation. Groundplane never
adds Secret-store values, generated credentials, or generated env-file
material to log frames. Workloads are responsible for not printing secrets.
MVP has no secret-aware redaction.

## Context

The MVP replaces `scripts/logs.sh` with Logs actions on a Service and an
Environment. The public routes and CLI placeholders already exist, but the
event shape, source selection, failure behavior, and Controller-Agent seam
are not defined. Logs are high-volume execution output, not Task events:
Task-event persistence is bounded and durable, while log output is explicitly
transient and must never enter etcd.

The MVP has one host and one Controller-managed local Agent. The Controller
owns human API decisions; the Agent owns Docker reads and workload execution.
This decision preserves that authority boundary and the Console/API/CLI
one-to-one contract.

## Decision

### Caller usage

The existing commands gain one bounded option and retain their current target
resolution:

```text
groundplane environment logs [--tail N] [--follow]
groundplane service logs <name> [--tail N] [--follow]
```

`<name>` is a Service slug unless global `--id` selects a stable id. The
Console exposes the same Logs action on Environment and Service surfaces.
Both frontends call exactly one of:

```text
GET /api/v1/environments/{id}/logs?tail=N&follow=true
GET /api/v1/services/{id}/logs?tail=N&follow=true
```

`tail` is canonical decimal `0..1000`, defaulting to `200`. `follow` is
canonical `true|false`, defaulting to `false`. Unknown or duplicate query
parameters, malformed values, request bodies, and `Last-Event-ID` are
rejected. There is no resume protocol.

### Public SSE representation

The response is `200 text/event-stream`. Each message is `event: log` with
one compact JSON object:

```json
{
  "sequence": 1,
  "service_id": "svc_01...",
  "service_name": "api",
  "container_id": "...",
  "container_name": "...",
  "release_id": "dep_01...",
  "slot": "blue",
  "stream": "stdout",
  "timestamp": "2026-08-29T12:34:56.123456789Z",
  "line": "application output",
  "truncated": false
}
```

`sequence` is a one-based delivery order within this connection only. It is
not a cursor and is not emitted as an SSE `id`. `stream` is `stdout` or
`stderr`; `slot` is `blue`, `green`, or `singleton`. Docker line endings are
removed, invalid UTF-8 is replaced with `U+FFFD`, and lines over 32 KiB are
truncated with `truncated: true`.

### Source selection

The Controller resolves targets from one fixed etcd read before committing
SSE headers:

- Service logs select that Service's current `serving_release_id`.
- Environment logs select every Service in that Environment, sorted by stable
  Service id, with each Service's current serving release.
- A Service with no serving release contributes no source and is not an
  error.
- Backing-service Environments are never included through an Attach.

The source set is frozen for the request. The Agent matches the Controller's
target against the managed ownership labels:

```text
com.groundplane.managed=true
com.groundplane.environment-id=<environment id>
com.groundplane.service-id=<service id>
com.groundplane.release-id=<serving release id>
```

Only workload/singleton containers are selected; stable proxy containers are
excluded. Every matching replica is included, sorted by container name and
then container id. A blue-green inactive candidate is excluded because only
the Controller's serving release is requested. Each source retains Docker
order; the combined stream uses Agent arrival order.

### Controller-Agent transport

Logs use the existing authenticated, Agent-initiated bidirectional gRPC
channel, not a second channel and not a Task. The protobuf extension is:

```text
ControllerMessage: LogSubscribe { request_id, targets[], tail, follow }
ControllerMessage: LogCancel    { request_id }
AgentMessage:      LogEvent     { request_id, source metadata, timestamp, line, truncated }
AgentMessage:      LogEnd       { request_id, reason }
AgentMessage:      LogReady     { request_id }
```

The request id is transient and opaque. The Agent validates the target labels,
reads Docker through a read-only Moby `ContainerLogs` seam, demultiplexes
stdout/stderr framing, and returns bounded records. The Controller owns the
short-lived per-request broker and SSE encoding. Controller code never reads
the Docker socket, and the Agent never decides which release is serving.
`LogReady` is private and non-terminal. The Agent sends it only after Docker
source discovery and all initial source setup succeeds, including successful
zero-source setup. It is never a public SSE event and adds no resumability.

### Bounds and retention

- At most 256 containers and 128 Service targets per request.
- At most 8 concurrent log subscriptions per Controller/Agent.
- Each subscription has a 128-record bounded queue; overflow closes that
  subscription rather than growing memory or silently dropping output.
- No log bytes, offsets, source lists, or stream errors are persisted in etcd,
  Task events, durable files, or normal daemon logs.
- Workload-authored Docker stdout/stderr is forwarded unchanged except for
  UTF-8 normalization and 32 KiB line truncation. Groundplane never adds
  Secret-store values, generated credentials, or generated env-file material
  to log frames. Workloads are responsible for not printing secrets; MVP has
  no secret-aware redaction.

### Errors and termination

Before headers, ordinary RFC 7807 errors apply: malformed input is HTTP 400,
unacceptable media type is 406, a missing target is 404, Agent/Docker
unavailability is 503, and invalid durable or Docker state is 500. A terminal
availability `LogEnd` before `LogReady` preserves that pre-header 503. After
`LogReady`, SSE headers are committed and later Agent/Docker failures close the
stream. A valid target with no matching containers returns 200 followed by EOF.

After headers, client cancellation closes the subscription without an error.
Agent disconnect, Docker read failure, source removal, queue overflow, or
Controller failure is safely logged and closes the connection; no synthetic
`event: error` or problem body is emitted. `follow=false` closes after the
bounded tail is drained. `follow=true` closes when all selected Docker
streams end. A later release or new replica is visible only after reconnect.

## Alternatives considered

### Durable sequence and `Last-Event-ID` resume

Rejected for the MVP. It would turn high-volume output into another durable
journal, require retention and replay semantics, and duplicate the separate
Task-event contract. Reconnecting with a fresh bounded tail is sufficient for
the single-host operator workflow.

### Controller-side Docker reads

Rejected. It violates the closed host-authority boundary and makes the
Controller a second workload runtime authority.

### Dynamic source discovery during `follow`

Rejected for the MVP. Fixed target membership keeps release/replica selection
auditable, avoids a second watch protocol, and bounds the Agent subscription.

## Consequences

The smallest safe implementation is a transient Controller broker, one
typed Agent-channel extension, and one read-only Docker log adapter. The
tradeoff is that a long-lived follow stream does not follow newly created
replicas or a later release switch; reconnecting obtains a new bounded tail.
Persistent search, replay, remote Agents, multi-host fan-out, and richer log
filters remain outside the MVP.

The API, CLI, Console, transport, and read-only Docker source contract are
accepted by this ADR.
