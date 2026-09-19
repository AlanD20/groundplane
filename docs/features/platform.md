# Host, Controller and Agent

## Purpose and scope

Run one self-hosted Groundplane installation on one Linux machine. The native
Controller owns product decisions and bootstrap; its local Agent executes typed
workload procedures. Host health is a read-only snapshot, not another durable
resource, metrics store or settings authority. The MVP supports `linux/amd64`
and `linux/arm64`, not multi-host scheduling or emulation.
The installation tooling targets Ubuntu 24.04/26.04 and Debian 13. Each OS and
architecture needs its own host qualification; accepting an OS in the installer
does not establish that qualification. See [deployment](../deployment.md).

## Functional requirements

- The native Controller is the only Groundplane systemd unit. It owns the private
  etcd container and Agent container lifecycle, including restart after Docker
  recovery. Neither depends on an Agent Task for its own bootstrap.
- A normal Controller restart may restart the Agent, but must leave etcd running
  unchanged. Application requests, held connections and data must survive.
  etcd recovery is independent of a Controller restart; it is not an Agent Task.
- Agent enrollment, configuration and replacement are explicit operator actions.
  Current-generation authenticated `Ready` is liveness evidence; container
  presence or HTTP/2 keepalive is not a substitute.
- Platform Host shows bounded health and capacity. Controller and Agent have
  their own settings pages and may be linked from Host, never edited there.
- [Safe updates](upgrade-safety.md) owns native release activation, drain,
  predecessor recovery and the trial-write boundary. A responsive API does not
  prove a trial has authority to publish ordinary mutations.

The product and surfaces are owned by [mvp.md](../mvp.md) and
[api-cli.md](../api-cli.md). Bootstrap and deployment procedures belong to
[deployment.md](../deployment.md).

### Host health

`GET /host`, `host show` and Platform Host consume one live, non-persistent read
model. Linux collection and formatting are fixed:

| Value | Source and meaning |
| --- | --- |
| Identity | Hostname from `gethostname(2)`, architecture from the Go runtime, OS from `/etc/os-release` `PRETTY_NAME` |
| Uptime | Largest whole unit of seconds, minutes, hours or days, floored; singular at one |
| CPU | Model from `/proc/cpuinfo`, logical cores from Go, one-minute `/proc/loadavg` divided by cores, rounded to whole percent and clamped to `0..100` |
| Memory and swap | `/proc/meminfo`: `MemTotal - MemAvailable` and `SwapTotal - SwapFree` |
| Disk | Root filesystem `statfs(2)`: total `blocks * block size`, used `(blocks - free blocks) * block size` |
| Byte display | Binary IEC units (`KiB`, `MiB`, `GiB`, `TiB`), one decimal place with trailing `.0` omitted; percentages rounded to whole numbers and clamped to `0..100` |
| Docker | Daemon version from the owned Engine client, or `unavailable` when its probe fails |

The etcd node label is `single-node`. All configured endpoint probes healthy
means `healthy`, a non-empty healthy subset means `degraded`, and none means
`failed`. DB size is the largest healthy member's size in the same IEC format,
or `unavailable`. Endpoints and raw probe errors remain private.

Agent provisioning or updating is `pending`; ready with authenticated health is
`healthy`, ready without it is `degraded`, and deleting or absent is `stopped`.
Pull interval, concurrency and sorted `key=value` labels come from durable Agent
config, or Controller bootstrap Agent config before enrollment. No credential,
token, socket, endpoint or container ID enters this projection.

The Controller row is `healthy` because it served the response, with runtime
`groundplane-controller.service` and shared build version. Expected dependency
unavailability is data; malformed or unreadable host sources fail closed through
the shared error taxonomy. Console loading/failure is explicit: no fixture
fallback, invented API address, scheduler timing, report age or Host mutation.
These display-grade values do not replace monitoring or alerting.

### Controller and Agent settings

Dedicated Console pages are `/platform/controller` and `/platform/agents/{id}`.
The Agent config singleton remains the sole Agent authority with immediate
drain-and-reconfigure semantics; configuration does not restart running Tasks.

The Controller editor reads and replaces the exact startup YAML through
`GET /controller/config` and `PUT /controller/config`, mirrored by
`controller config show` and `controller config set --file PATH`. Responses
contain absolute path, exact content, `sha256:` revision and `restart_required`.

Replacement requires exact YAML, the last-read revision and an idempotency key.
Use startup's defaults, strict known-field decoder, single-document rule and
semantic validator. Durably bind the key to that revision and YAML before file
publication. Equal retries replay the stored response; different intent under
the same key fails. Publish with atomic same-directory rename, mode `0600` and
directory sync. Stale replacement is a state conflict.

Saving changes the file immediately, preserving operator comments and formatting,
but does not hot-reload the process. `restart_required` compares current bytes
with the startup snapshot and clears after restart or exact revert. Bootstrap
YAML remains outside etcd and in recovery exports; editing it does not make it
Environment desired state.

## Non-functional requirements

Bootstrap cannot depend on the store or Task loop it is starting. Runtime
authority is closed: only the Controller owns Agent replacement; workload
effects use validated Agent procedures. Keep credentials and raw diagnostics
outside public health. Do not persist health samples without a new product
requirement. Exact release, channel, mount and image restrictions remain in the
technical contracts below.

## Technical design

| Boundary | Current technical contract |
| --- | --- |
| Agent identity and replacement | [Agent runtime identity](../decisions/0010-agent-runtime-and-channel-identity.md) |
| Local authenticated channel | [Channel authentication](../decisions/0011-agent-channel-authentication.md) |
| Controller-owned local Agent and bootstrap | [Local Agent lifecycle](../decisions/0016-controller-owned-local-agent.md) |
| Container release inputs | [Agent OCI packaging](../decisions/0043-agent-oci-packaging.md) |
| Exact supported release variants | [Dual-platform release authority](../decisions/0060-dual-platform-mvp-release-authority.md) |

Shared process placement remains in [architecture.md](../architecture.md).
Health collects raw Linux, etcd and Agent observations through bounded ports;
public formatting and safe error projection belong to the Controller read model.

## Acceptance

Prove bootstrap and Docker recovery without cyclic ownership; exact-generation
authentication and stale-session rejection; deterministic health formatting and
dependency failures; secret-free responses; strict YAML validation, stale-write
rejection, durable replay and restart-required semantics. Native updates and
two-platform releases additionally require their own recovery and host proofs.

## Current status

Host health, Agent lifecycle and settings have recorded qualification. Native
updates retain the [Safe updates](upgrade-safety.md#current-status) limits.
Current work and the live-mutation pause are in [head.md](../head.md); a dated
health result never establishes present host health or operational permission.
Normal shutdown previously stopped etcd through its manager's `Close`. The local
correction closes only the Docker client, leaving the store running. Its regression
proves no inspect/stop call even when client closure fails; Controller shutdown
still closes owned clients and preserves errors. The foundation verifier permits
an Agent start-time change but requires unchanged etcd identity, start time,
restart count and running state. Local fixture proof passes; REL-01 application
continuity and deployed lifecycle proof remain unrun.
