# ADR 0011: Local Agent channel token authentication

- Status: Accepted
- Date: 2026-08-20

## Context

Accepted ADR 0016 gives the native `groundplane-controller.service` full
lifecycle ownership of the single-host MVP's local Agent OCI container. It
also selects token authentication for that machine channel and defers mTLS,
CAs, and Agent certificates until a post-MVP remote or multi-host topology
requires them.

The human enrollment surface is also settled. `groundplane agent join` calls
`POST /api/v1/agents`, receives `202 {task_id}`, and exposes no credential. The
Controller task creates the Agent record, generates and injects the channel
credential, starts the container, and waits for an authenticated `Ready`.
There is no public join-token endpoint, plaintext bootstrap artifact, REST pair
call, pending approval state, or approval command.

The remaining contract question is how the Controller persists and injects a
recoverable local runtime credential without leaking it into process arguments,
Docker metadata, logs, task records, or plaintext etcd values.

## Decision

### Owner-approved topology and public surface

- `POST /api/v1/agents` returns `202 {"task_id":"..."}` and never returns a
  token, CA, certificate, or bootstrap document.
- The native Controller creates the Agent record and owns the Agent container
  through Docker. There is no `groundplane-agent.service`.
- The local Agent authenticates to the Controller with a Controller-generated
  token. The Agent never generates enrollment keys or certificates.
- Removing the Agent removes its container and invalidates its channel token.
- mTLS, Controller CAs, Agent certificates, certificate renewal, and CA rotation
  are post-MVP.

### Exact local token contract

Use a gRPC bidirectional stream over a Unix domain socket at
`/run/groundplane/controller/agent.sock`. The Controller creates the parent
directory as root with mode `0700` and the socket with mode `0600`. Docker
bind-mounts the parent `/run/groundplane/controller` directory read-only into
the Agent container at the same path. Mounting the directory rather than the
socket inode ensures that removing and recreating the socket remains visible
inside the running container. The MVP opens no TCP listener for the Agent
channel.

For each Agent record, generate exactly 32 random bytes with the operating
system cryptographic random source and encode them as unpadded base64url. The
encoded token is 43 ASCII characters. Random-source failure fails the creation
task before an Agent record or container becomes visible.

Persist two values transactionally with the Agent record:

- the token encrypted with the Controller's existing application-level age
  key, so the Controller can rematerialize it after restart or container
  replacement;
- a SHA-256 digest lookup that maps the raw-token digest to the Agent id, so a
  presented credential can be resolved without decrypting the stored token on
  every connection.

Neither plaintext nor encoded token appears in an etcd key, Agent metadata,
observed state, task payload, event, log, API response, or Docker label. The
encrypted value uses the repository's canonical secret envelope rather than a
second encryption format.

Immediately before creating the container, the Controller atomically writes
the encoded token to a root-owned mode `0400` runtime file at
`/run/groundplane/agents/<agent-id>/token`. It creates the parent directory as
mode `0700`, writes and syncs a temporary file in that directory, renames it
over the final path, and syncs the directory. Docker bind-mounts only the final
file read-only at `/run/groundplane/agent.token` inside the container. The token
is never passed through an environment variable, command-line argument,
container label, image layer, or Compose document.

The Agent reads the runtime file, sends `agent_id` and the decoded 32-byte token
in its gRPC `Connect` message, and never logs either value. The Controller
hashes the presented raw token, resolves the digest lookup, and requires its
Agent id to equal the presented id before serving config or work. The
token is stable across reconnects, Controller restarts, and Controller-owned
Agent container replacements. It is not consumed and has no independent
renewal action.

`DELETE /api/v1/agents/{id}` first stops new assignments, aborts every active
task with reason `agent_removed`, revokes the digest lookup and active channel
authority, and waits for the Agent to be offline. It then removes the Agent
container, runtime directory, encrypted token, Agent record, and config as one
task. A later `agent join` creates a new Agent id and new token; no removed
credential is restored or accepted.

Enrollment must receive an authenticated `Ready` within 120 seconds. Ordinary
online status becomes stale after `max(3 * pull_interval, 30s)` without a
`Ready`; HTTP/2 keepalive does not extend that deadline.

### Failure boundaries

- Failure before the durable Agent record and credential transaction commits
  leaves no Agent and no runtime file.
- Failure after commit but before readiness is retryable by rematerializing the
  encrypted credential and reconciling the Controller-owned container.
- A missing, malformed, unreadable, or mismatched token fails closed and the
  Controller serves no Agent config or task.
- The token file is a runtime materialization, not a bootstrap or disaster-
  recovery artifact. Restoring etcd plus the Controller age key allows the
  Controller to rematerialize it.

## Owner-approved exact values

The owner approved the complete local-channel contract on 2026-08-20:

- `/run/groundplane/controller/agent.sock`, parent-directory mode `0700`,
  socket mode `0600`, the read-only same-path mount of
  `/run/groundplane/controller`, and the absence of an MVP TCP Agent listener;
- 32 random bytes encoded as unpadded base64url and SHA-256 over the raw bytes;
- encrypted-token plus digest fields in the Agent transaction and their exact
  reconciliation with ADR 0013's durable record layout;
- host runtime path `/run/groundplane/agents/<agent-id>/token`, container path
  `/run/groundplane/agent.token`, root ownership, modes `0700`/`0400`, atomic
  materialization, and read-only bind policy.

No owner choice remains for a public token endpoint, plaintext bootstrap
artifact, one-time consumption, Agent systemd unit, Agent self-update, mTLS,
certificate algorithms, validity periods, renewal, or CA rotation in the MVP;
those paths are removed or deferred rather than left open.

## Consequences

- Human enrollment never handles or exposes an Agent credential.
- The Controller can recreate the Agent container after restart without
  rotating or exporting a token.
- Compromise of etcd alone does not reveal the token; compromise of both etcd
  and the Controller age key remains within the existing secret-store threat
  boundary.
- UDS permissions and a high-entropy token provide separate local boundaries.
- Agent removal is the credential revocation operation.
- Accepted ADR 0016 supersedes ADR 0007 and ADR 0006's enrollment exception;
  ADR 0013's proposed durable layout must align with these accepted mechanics.
