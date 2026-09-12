# ADR 0010: Define the Agent runtime and channel identities

- Status: Accepted
- Date: 2026-08-20
- Narrowed by: [Backup execution contract](../features/backups/agent-protocol.md) on 2026-08-30 for the sole schema-1 Agent channel,
  assignment generations, generic terminal receipts, and Backup transfer and
  recovery messages; the runtime-container ownership invariants remain in force

## Context

The Agent is the ECS-agent-style execution plane. It needs the Docker socket
and explicit host materialization roots to apply Controller-supplied work, but
it must not control the lifecycle of the container in which it runs. A broken
or compromised Agent cannot be the only verifier or recovery mechanism for its
replacement.

Accepted ADR 0016 selects one native lifecycle boundary for the single-host MVP:
`groundplane-controller.service`. The Controller remains alive independently of
Docker and fully owns the local Agent OCI container through Docker. A second
`groundplane-agent.service` would split ownership and create two competing
reconciliation loops.

The task channel also lacks stable step identity. `ExecutionStep` has an ID,
but protobuf `Step` does not, and `TaskEvent.step` is an unconstrained string.
A reconnect or duplicate assignment therefore cannot correlate checkpoints
unambiguously. `TaskAck` additionally compresses terminal state to `success`,
despite the durable task model distinguishing `failed`, `timed_out`, and
`aborted`.

## Decision

### Agent runtime

The Agent remains an immutable OCI container. The native
`groundplane-controller.service` creates, starts, stops, replaces, rolls back,
and removes that container through Docker. There is no
`groundplane-agent.service` and no bootstrap Compose file.

The Controller runs one Agent image pinned by OCI digest with:

- user `root`;
- host networking;
- Docker restart policy `no`;
- the Docker socket read-write to execute validated workload procedures;
- `/var/lib/groundplane/agent` read-write at the same path;
- the read-only `/run/groundplane/controller` Agent-channel directory;
- the single read-only token-file mount defined by ADR 0011; and
- Controller-injected runtime config from the root-owned host file
  `/run/groundplane/agents/<agent-id>/config.yaml`, atomically materialized with
  mode `0444` and mounted read-only at `/run/groundplane/agent.yaml`.

The mount allowlist never includes the host root, a generic host runtime root,
or workload materialization roots. Active task checkpoints are durable
Controller records rather than container-local files. The native
Controller, not Docker restart policy, reconciles a stopped or missing Agent
container.

The Agent never loads, stops, recreates, or rolls back its own container. An
Agent update is a Controller-owned operation. The Agent may be asked to quiesce
new assignments and persist checkpoints, but it never receives a self-update
image or invokes Docker against itself.

### Channel identity and resumption

The task channel changes so that:

- every serialized step carries the immutable `step_id` from the execution
  plan;
- every event identifies `task_id`, `step_id`, and a monotonic event sequence;
- acknowledgements carry the exact terminal enum (`completed`, `failed`,
  `timed_out`, or `aborted`) rather than a boolean;
- a repeated assignment with the same `task_id` and `plan_hash` resumes from
  the Agent's durable checkpoint;
- the same `task_id` with a different `plan_hash` is rejected as a contract
  violation;
- retries always use a new `task_id`, retain `operation_id`, and set
  `retry_of`, matching the durable Controller model.

Local MVP channel authentication is token-based under accepted ADR 0011.
mTLS, CAs, and Agent certificates are post-MVP and are not part of this
update transport.

### MVP Agent update

The Controller configuration key `agent.image` is the bootstrap desired release
input. ADR0074's last qualified native manifest overrides it for later enrollment
and updates without rewriting Controller YAML. Unfinished native recovery blocks
new image selection; a failed later trial preserves the last successful release.
The selected image must remain an OCI digest reference. An update request is bodyless:
there is no bundle, uploaded archive, tag, URL, version field, release trust
root, or Agent-side image-update message in the MVP. Distribution and
authenticity of the selected image are deployment concerns; the Controller
enforces immutable digest identity at its runtime boundary.

The Controller accepts an update only for the local Agent in `ready` phase and
only while it has no active Task assignments. It fences new assignments before
checking. A busy Agent fails with `resource.in_use` before credential, record,
runtime-file, or container mutation. The update never aborts operator work.

Update preparation uses an operation-owned reversible assignment pause, not the
permanent deletion/revocation fence. It drains admitted claims and sends before
checking for idle. A busy result or failed preparation releases only its own
pause and wakes dispatch, including across an equal-generation reconnect.
Cancellation does not cancel already running Agent work. Once replacement
publication is attempted, an uncertain response retains the pause until durable
recovery fences the predecessor; it is never treated as proof of no mutation.
Valid assignments delayed by a pause remain dispatchable, not quarantined.

Unknown replacement publication is resolved with a storage barrier, never an
unchanged read alone. A same-value CAS advances the prior primary revision so a
delayed old transaction cannot subsequently commit. A proven uncommitted attempt
releases its pause; an already committed generation is irreversibly fenced and
resumed without rotating twice. If storage cannot resolve it immediately, the
lifecycle owner retains the operation and retries resolution before subsequent
reconciliation/mutations. Controller restart admission restoration uses the
durable native Task's pinned identity; the current-process hold is not durable
authority by itself.

For an accepted update, the Controller:

1. retains the current image digest as the rollback digest;
2. advances the durable Agent generation and image to the selected immutable
   release, creates a fresh channel token, and atomically replaces the
   Controller-owned runtime config and token materialization;
3. revokes the previous generation, waits for it to disconnect, and replaces
   the container under the immutable Agent id;
4. waits up to 120 seconds for the replacement generation to authenticate and
   report `Ready`; and
5. on readiness failure, advances generation and token again, restores the
   retained image digest, recreates the prior image, and waits up to another
   120 seconds for authenticated `Ready`.

The Agent id, operator config, labels, enrollment Task identity, creation time,
and first-ready time survive both replacement and rollback. The update Task
has a 300-second deadline. Successful replacement completes the Task. A
successful rollback leaves the Agent ready on its previous digest but fails
the update Task. An unresolved rollback retains the running native claim and
its all-generation admission hold, including after deadline expiry. Subsequent
bounded passes recover the pinned predecessor before the failed/timed-out
terminal acknowledgement; they never report false success or delete the Agent.
The starting Controller restores this hold before opening the Agent channel and
revision-fences any old publication attempt before issuing a new credential.
If the candidate is already durably Ready, replay confirms fresh authenticated
readiness and completes without another rotation, even if a lost Task
acknowledgement crossed the deadline. Abort and qualification have one winner;
an accepted pre-qualification Abort restores the predecessor before finishing.

The one-for-one operator surfaces are:

- Console: Update on the selected Agent;
- CLI: exactly one of `groundplane agent update <id>` or
  `groundplane agent update --all`; and
- API: bodyless `POST /api/v1/agents/{id}/update`, returning
  `202 {"task_id":"..."}`.

`--all` is explicit target selection for the singleton MVP. The CLI resolves
the current Agent and invokes the same per-id endpoint once. It does not add a
bulk endpoint, alternate response shape, or implicit no-target behavior.

## Consequences

- Docker failure does not take down the native Controller responsible for
  reconciling the Agent container after Docker returns.
- The Agent has Docker authority for validated workloads but not for its own
  lifecycle or an unrestricted host filesystem.
- Update identity is the immutable digest accepted from Controller
  configuration; image distribution and signature policy are post-MVP.
- A broken replacement can be rolled back without relying on that replacement
  to execute recovery logic.
- Task events and reconnects become deterministic and idempotent.
- Generated protobuf code must be regenerated after the channel contract is
  implemented; no compatibility fields are retained.
