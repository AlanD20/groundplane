# ADR 0016: Controller-owned local Agent

- Status: Accepted
- Date: 2026-08-20
- Supersedes: ADR 0007; the Agent enrollment/bootstrap exception in ADR 0006

## Context

The single-host MVP runs the Controller as a native systemd service and the
Agent as an OCI container. The Agent needs Docker access to execute validated
workload procedures, but letting it supervise or update its own container
would make the process being replaced responsible for its own verification,
recovery, and rollback.

ADR 0006 classified Agent enrollment as a local bootstrap exception to the
Console/CLI/API 1:1 rule. ADR 0007 consequently exposed a one-time token
through `POST /agent-join-tokens` for an operator to place in a plaintext
bootstrap file before the Agent created its own record over gRPC. That topology
no longer fits a Controller-managed local container: the Controller can create
the record, credential, runtime materializations, and container as one task
without exporting a credential.

## Decision

### One native lifecycle owner

`groundplane-controller.service` is the only Groundplane runtime supervisor.
The native Controller fully owns the local Agent OCI container through Docker:
creation, configuration injection, start, stop, replacement, health checking,
rollback, reconciliation after Docker restarts, and removal.

The Agent container runs as root with host networking and Docker restart policy
`no`. It mounts only the Docker socket read-write,
`/var/lib/groundplane/agent` read-write at the same path, the read-only
`/run/groundplane/controller` channel directory, the read-only runtime config,
and the read-only token file. It never receives the host filesystem root, a
generic host runtime mount, or workload materialization roots.

There is no `groundplane-agent.service` and no bootstrap Compose file. The
Agent may use the Docker socket only to execute Controller-supplied workload
procedures. It never creates, replaces, updates, or rolls back its own
container.

An Agent update is a Controller-owned task. The Controller fences new
assignments, requires the Agent to be idle, rotates its generation and token,
replaces it at the configured digest, and rolls back the prior digest when the
replacement does not become ready. The Agent never receives an image-update
payload or invokes Docker against itself. ADR 0010 fixes the exact lifecycle.

### Operator-facing enrollment

Agent enrollment is an operator-facing Controller capability and follows the
1:1 rule:

- Console: the Platform Agent enrollment action;
- CLI: `groundplane agent join`;
- API: `POST /api/v1/agents` returning `202 {"task_id":"..."}`.

The Controller task creates the Agent record, internally generates its channel
token, injects the credential and Controller-owned runtime config, starts the
container, and waits up to 120 seconds for authenticated `Ready`.

No token, CA, certificate, bootstrap file, or combined enrollment artifact is
returned through the human API or printed by the CLI. There is no public
`/agent-join-tokens` or REST pair endpoint, pending approval state, approval
command, or compatibility route.

ADR 0006 remains accepted for its other closed exceptions: local process
commands (`controller serve`, `agent-run run`) and local CLI tooling (`version`,
`completion`). Only its Agent enrollment/bootstrap exception is superseded.
ADR 0007 is superseded in full.

### MVP channel authentication

The local MVP Agent channel uses a Controller-generated token. The Controller
keeps the credential inside the Controller-managed runtime boundary and
invalidates it when the Agent is removed. Accepted ADR 0011 fixes the UDS at
`/run/groundplane/controller/agent.sock`, a 32-random-byte token encoded as
unpadded base64url, application-level encrypted storage, SHA-256 digest lookup,
and narrow read-only runtime mounts. Missing `Ready` for
`max(3 * pull_interval, 30s)` makes the Agent stale.

Agent removal first stops new assignments, aborts active tasks with reason
`agent_removed`, revokes the channel token, and waits for the Agent to be
offline. It then removes the container, runtime material, config, and Agent
record. Removed credentials are never accepted again.

mTLS, Controller CAs, Agent certificates, certificate renewal, and CA rotation
are post-MVP concerns for a future remote or multi-host topology. They are not
an alternate MVP authentication path.

## Consequences

- One native reconciliation loop owns the Agent container; no systemd/Docker
  split-brain lifecycle is possible.
- Human enrollment is asynchronous and participates in Console/CLI/API parity
  without exposing a machine credential.
- The Agent remains powerful enough to apply workloads but cannot make itself
  the authority for update or recovery.
- Controller restart and Docker restart recovery can rematerialize runtime
  state and reconcile the Agent container from durable Controller records.
- Code and generated contracts must remove the public join-token route,
  plaintext bootstrap configuration, certificate-based MVP paths, and
  Agent-side self-update messages without compatibility aliases.
