# ADR 0022: Execute generated Compose projects through a typed Agent procedure

- Status: Accepted
- Date: 2026-08-20

## Context

Groundplane renders one canonical Docker Compose project from normalized
Controller state and delegates host effects to the local Controller-owned
Agent. Accepted ADR 0003 fixes the runtime technology: use `compose-go/v2` for
the Compose model, validate rendered output with `docker compose config`, and
apply it through the Docker Compose v2 CLI using `internal/common/runner`.
The Agent is the only process that invokes the workload runtime.

That accepted dependency choice is not yet an executable machine contract.
The current protobuf `TaskAssignment` carries plan metadata separately from a
generic `Step { op, map<string,string> params }`, but it carries no Compose
artifact. The Agent converts that message to an adapter `Step` and discards
the operation id, retry id, plan id and hash, render generation, task type,
target, typed task parameters, and timeout before submitting work. The worker
has a Runner but no Compose adapter, plan verifier, event/result port, or
observed-state publisher.

The current `internal/infra/docker.ComposeProject` and `Applier` are also only
scaffold shapes. They cannot express a per-service Stop separately from
Destroy, targeted service removal, explicit health waiting, a full-project
reconciliation, or the artifact and expected ownership used to verify a plan.
Its comment that either the Docker API or Compose CLI may apply a project
conflicts with accepted ADR 0003. The Docker API may support read-only
observation, but it is not a second workload apply implementation.

The accepted persistent-Agent mount topology creates another required seam.
ADR 0016 forbids a host root, generic workload root, or environment-volume
mount on the persistent Agent. Docker Compose reads service `env_file`
contents on the client side. Rendering secrets into Compose YAML would violate
the secret boundary, while mounting every environment into the persistent
Agent would reverse ADR 0016. A short-lived helper with one authorized volume
is needed for Compose to read the already materialized file without adding a
persistent broad mount.

The existing documents also conflict on Docker ownership labels. The
architecture and one MVP section use the reverse-DNS `com.groundplane.*`
namespace, while another MVP section, protobuf comments, and existing Docker
scaffolds use `groundplane.*`. Supporting both would create two ownership
authorities. The accepted rename contract is clearer: Compose project names,
Docker identities, and paths are derived from stable ids and never from
mutable labels, although the current Blueprint parser still derives its
Compose project name from the environment label.

ADR 0009 correctly requires typed execution payloads and rejects the generic
step map. Its Valkey RDB/AOF and cross-database grant questions are unrelated
to Compose execution and do not block this closed Compose procedure. The owner
approved the recommendation and its implementation on 2026-08-22.

## Existing constraints

Any accepted decision must preserve these contracts:

- the Controller owns desired state, plan construction, dependency ordering,
  release decisions, health-gate placement, and retry policy;
- the Agent is a bounded, cancellable executor with no Blueprint or product
  decision authority;
- the Controller sends one typed execution plan with a stable plan id, plan
  hash, render generation, canonical Compose artifacts, typed steps, expected
  labels, and ordering;
- protobuf is authoritative for dispatch; generated `x-gp-execution` is an
  inspection and replay projection that must identify the same plan;
- `compose-go/v2` remains the Compose model and Docker Compose v2 remains the
  apply runtime;
- the Agent validates with `docker compose config` before every apply and
  invokes Compose through the shared Runner;
- native Compose fields retain their native meaning and no partial handwritten
  Compose executor is introduced;
- start, stop, and destroy set durable service runtime intent to `running`,
  `stopped`, and `absent` respectively;
- deploy and rollback are per-service operations, and a route switch occurs
  only after the Controller-placed health gate succeeds;
- applying the same plan twice converges without duplicate resources;
- retries observe current labeled state and task checkpoints before reapplying
  a side effect;
- only Groundplane-owned resources may be mutated; an unmanaged name collision
  fails closed;
- generated Compose and materializations are ephemeral render-plan data, not
  durable desired state;
- Compose YAML, task records, logs, errors, argv, environment variables, and
  labels contain no secret value;
- the persistent Agent keeps its accepted narrow mounts and never receives a
  host root or persistent workload-volume root; and
- unknown steps, mismatched payloads, plan-hash failures, unsafe paths, and
  impossible observed state fail closed with no compatibility decoder.

## Recommended decision

### 1. Carry one typed execution plan

Replace the duplicated `TaskAssignment.plan_id`, `plan_hash`, and
`render_generation` fields plus generic task/step parameter blobs with one
typed `ExecutionPlan`. Task identity, operation identity, retry identity, and
the overall task deadline remain assignment fields. The plan shape is:

```text
ExecutionPlan {
  schema:             uint32, exactly 1
  plan_id:            canonical plan_<ulid>
  plan_hash:          exactly 32 SHA-256 bytes
  render_generation:  positive uint64
  operation:          closed PlanOperation enum
  target_id:          canonical stable resource id
  artifacts:          ordered repeated ComposeArtifact
  steps:              ordered repeated ExecutionStep
}
```

The closed initial `PlanOperation` values are `reconcile`, `deploy`,
`rollback`, `start`, `stop`, `destroy`, `remove`, and `component_apply`.
Unknown enum values are rejected. A later operation is an additive machine
contract change, not a free-form string.

The Controller computes `plan_hash` as SHA-256 over deterministic protobuf
serialization of the complete `ExecutionPlan` with `plan_hash` cleared.
Unknown protobuf fields are rejected before hashing. The plan uses no map
fields: every key/value collection is a sorted repeated pair, so deterministic
serialization has one byte representation. Version 1 is immutable. A future
hash representation requires a new plan schema and a clean cutover.

The Agent validates the plan schema, every id, generation, artifact, step,
cross-reference, closed enum, bound, and digest before starting a helper. It
recomputes the hash and compares the 32 bytes in constant time. The
`x-gp-execution` projection in each artifact must contain the same plan id,
hexadecimal `sha256:<64-lowercase-hex>` plan hash, render generation, operation,
and ordered local step projection. A mismatch is an Internal procedure error;
the Agent never repairs either representation.

One plan contains at most 16 Compose artifacts, at most 1 MiB of canonical
YAML per artifact, and at most 4 MiB when deterministically serialized. These
are machine-message safety limits, not Blueprint input limits. The Controller
rejects an oversized render before dispatch.

### 2. Make each Compose artifact self-contained

The protobuf shape is:

```text
ComposeArtifact {
  artifact_id:           canonical cfg_<ulid>
  owner_kind:            environment | platform
  owner_id:              environment id or empty for platform
  project_name:          canonical generated Compose project name
  canonical_yaml:        validated UTF-8 bytes
  yaml_sha256:           exactly 32 SHA-256 bytes
  authorized_volume_dir: canonical environment host volume or empty for platform
  services:              sorted repeated ComposeService
  networks:              sorted repeated ComposeNetwork
  volumes:               sorted repeated ComposeVolume
}

ComposeService {
  service_id:   canonical svc_<ulid> or component-owned stable service id
  compose_name: exact canonical Compose service key
  expected_labels: sorted repeated LabelPair
  expected_replicas: positive uint32
  has_healthcheck: whether WaitHealthy is permitted
}

ComposeNetwork and ComposeVolume carry their canonical stable id, exact
Compose key, exact Docker resource name, and their own sorted expected labels. Labels are per resource;
an artifact-level label set is forbidden because it cannot express distinct
service, network, and volume ownership tuples.

Managed Docker volume names are `gp_vol_<volume-id>`. Their data directory is
`<environment.volume_dir>/<Compose-key>` and generated Compose uses the local
driver with an exact bind to that directory. Authored physical volume names,
external volumes, and custom driver options require a separate resolved
contract and are not silently rewritten.

Before any apply that can create a managed Docker volume, the plan contains
one `ManagedVolumeDirectoriesEnsure` step selecting the authenticated
Environment artifact by `artifact_id`. It carries no path or Compose name.
The socketless directory helper derives the artifact's uniquely named managed
volume leaves, verifies the existing Environment namespace, and idempotently
creates only direct root-owned mode `0755` children. Existing ownership, mode,
symlink, or traversal mismatches fail closed and are never repaired. The
Environment namespace ancestors remain root-owned mode `0700`.

LabelPair {
  key:   one accepted ownership-label key
  value: its exact validated value
}
```

The artifact is the only Compose input. Includes, fragments, extension
interpretation, profiles, build contexts, relative host paths, implicit
`.env`, unresolved interpolation, and remote resources are already compiled
or rejected by the Controller. `canonical_yaml` contains no secret bytes.
Service `env_file`, config-file, secret-file, and bind paths that must be read
by Compose are absolute paths below the one authorized environment volume.
Platform artifacts must not contain an environment file path.

The Agent verifies the artifact digest before parsing or execution. It uses the
artifact service table to resolve stable service ids from steps to Compose
service names. Steps never carry an independently editable Compose name.

### 3. Use typed Compose step payloads

`ExecutionStep` contains a canonical `step_<ulid>`, one positive
`timeout_seconds`, and exactly one payload in a protobuf `oneof`:

```text
ComposeApply {
  artifact_id:    cfg_<ulid>
  service_ids:    ordered stable service ids
  full_reconcile: bool
}

ComposeStop {
  artifact_id:      cfg_<ulid>
  service_ids:      non-empty ordered stable service ids
  grace_seconds:    positive uint32, at most 300
}

ComposeRemove {
  artifact_id:    cfg_<ulid>
  service_ids:    ordered stable service ids
  whole_project:  bool
}

WaitHealthy {
  artifact_id:  cfg_<ulid>
  service_ids:  non-empty ordered stable service ids
}
```

For `ComposeApply`, `full_reconcile=true` requires an empty service list and
applies the complete artifact. `full_reconcile=false` requires a non-empty
service list. For `ComposeRemove`, `whole_project=true` requires an empty
service list and is valid only when finalizing deletion of the complete
generated project. `whole_project=false` requires a non-empty service list.
Duplicate service ids and references outside the selected artifact are
invalid.

This cleanly replaces the ambiguous `compose_down` operation with the named
`compose_stop` and `compose_remove` payloads. There is no generic mode string,
parameter map, shell command, or compatibility translation. Non-Compose step
payloads remain blocked until their typed contracts are accepted; the
Controller cannot dispatch them through a generic fallback.

### 4. Preserve data across service lifecycle actions

The lifecycle behavior is:

- start and ordinary reconciliation use `ComposeApply`;
- deploy and rollback apply only the Controller-selected service or slot set,
  followed by the separately authored `WaitHealthy` step where required;
- stop uses `docker compose stop` for the selected services and preserves
  containers, networks, and volumes;
- destroy uses `docker compose rm --stop --force` for the selected services,
  preserves desired state, networks still in use, and every volume, and leaves
  reconciliation aware that runtime intent is `absent`;
- remove deletes desired state through the Controller task, then uses targeted
  Compose removal for artifacts that are no longer desired; and
- complete project finalization may use `docker compose down
  --remove-orphans`, but never `--volumes`.

No service lifecycle operation deletes a named volume or host volume. Volume
deletion occurs only through the explicit Volume removal workflow after its
own reference and backup policy checks. The Compose adapter has no
`removeVolumes` boolean.

### 5. Derive project names only from stable ids

The two project-name forms are:

```text
groundplane-infra
gp-<lowercase-environment-id>
```

`groundplane-infra` is reserved for the platform-owned project. Every tenant
or backing environment uses `gp-` followed by its complete lowercased stable
environment id. The Controller derives the name once from the owner; the
Agent validates the exact form. A slug, display label, authored Compose
`name`, filesystem location, task id, or render generation never contributes
to it.

The Controller overrides or rejects an authored Compose project name during
Blueprint normalization. Renaming a tenant, project, environment, or service
does not change the Docker Compose project name.

### 6. Use one ownership-label namespace

Replace every workload ownership label with this closed namespace:

```text
com.groundplane.managed=true
com.groundplane.kind=<closed resource kind>
com.groundplane.tenant-id=<tenant id, when applicable>
com.groundplane.project-id=<project id, when applicable>
com.groundplane.environment-id=<environment id, when applicable>
com.groundplane.service-id=<service id, when applicable>
com.groundplane.release-id=<deployment id, when applicable>
com.groundplane.slot=<blue|green, when applicable>
com.groundplane.plan-id=<plan id>
com.groundplane.render-generation=<base-10 uint64>
com.groundplane.agent-id=<agent id, Agent container only>
com.groundplane.agent-generation=<base-10 uint64, Agent container only>
```

Keys and values are case-sensitive. The Controller emits the exact expected
set for each managed service, network, and volume. The Agent requires
`managed=true`, the correct kind and owner ids, and the plan/generation labels
before treating a resource as owned by the current plan.

An unchanged Environment Component may retain the exact applied runtime's
plan/generation labels under ADR0075. The Controller proves effective-runtime
equality and fences that source before publishing the frozen result. The Agent
requires unique Component ownership, pinned image and canonical nonfuture
generation; selecting it permits only targeted, non-forced, no-dependencies up.
This is not native Service/Release predecessor authority.

Compose's reserved `com.docker.compose.*` labels may be checked for consistency
but are not Groundplane ownership authority. The old `groundplane.*` keys are
removed from workload and Controller-owned Agent-container code in the same
cutover. There is no dual reader, dual writer, alias, or migration fallback.
Existing development resources with old labels are unmanaged and must be
removed explicitly before the accepted runtime is enabled.

### 7. Run Compose in one task-scoped helper

The persistent Agent never receives an environment-volume mount. For every
Compose step it creates one short-lived helper from the same exact
digest-pinned Agent image selected by the Controller-owned Agent lifecycle.
The image contains the Groundplane Compose-helper entrypoint, Docker CLI, and
one pinned Docker Compose v2 plugin. The image digest, rather than a floating
CLI version probe, fixes those bytes.

The helper configuration is:

- user `0`, restart policy `no`, network mode `none`, and read-only root;
- all Linux capabilities dropped, `no-new-privileges`, and default seccomp;
- the Docker socket mounted read-write at `/var/run/docker.sock`;
- for an environment artifact, exactly its authorized host volume mounted
  read-only at the identical absolute path;
- for a platform artifact, no environment-volume mount;
- no host root, sibling environment, Agent state directory, device, or other
  mount; and
- no request bytes, host paths, ids, labels, or secret values in container
  labels, argv, or environment variables.

The Agent sends one big-endian uint32-length-framed protobuf helper request
through stdin containing schema 1, the validated task and operation ids, the
sealed plan, selected step id, and positive remaining timeout. The helper
revalidates the plan and selects exactly one Compose artifact and payload. The helper
rejects duplicate headers, unknown fields, early EOF, extra bytes, an
out-of-order record, or a digest mismatch. It retains no request after exit.

The helper entrypoint invokes Docker Compose through
`internal/common/runner.Runner`. Creating, attaching stdin, starting, waiting,
stopping on cancellation, observing after ambiguity, and forced removal form
one Agent-side helper operation. Once Docker returns a helper id, every path
attempts terminal cleanup and explicit removal. A helper cleanup failure is an
Internal failure and cannot be hidden by an earlier operation error.

The helper has no caller-derived Docker name or label. Lifecycle cleanup uses
only the immutable id returned by create. Stdout is bounded to 64 KiB and must
contain exactly one framed typed response; stderr is bounded to 32 KiB and is
discarded at the adapter boundary. Exceeding either bound fails Internal and
forces cleanup.

### 8. Fix the Compose work directory and environment

The Agent image contains an empty root-owned directory at
`/run/groundplane/compose`, mode `0700`. It is the helper process working
directory and the exact `--project-directory`. Canonical YAML is passed only
through stdin with `--file -`; it is not written to a host path, Agent state,
task record, or temporary artifact tree.

The helper process receives a replacement environment, not the Controller,
Agent, systemd, or Docker host environment:

```text
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
HOME=/nonexistent
DOCKER_HOST=unix:///var/run/docker.sock
COMPOSE_DISABLE_ENV_FILE=1
```

`COMPOSE_FILE`, `COMPOSE_PROJECT_NAME`, `COMPOSE_PROFILES`, proxy variables,
credential variables, and every ambient interpolation value are absent. The
project name is always an explicit CLI flag. The Controller rejects a
canonical artifact containing an unresolved interpolation expression, so
Compose never obtains a value from process environment.

Every invocation begins with:

```text
docker compose
  --project-name <artifact.project_name>
  --project-directory /run/groundplane/compose
  --file -
```

The helper does not use a Docker configuration directory, credential helper,
registry login, or host Compose configuration. Image pulls use the Docker
daemon's existing configured authority; no credential crosses the task.

### 9. Validate and apply with explicit CLI semantics

Every `ComposeApply` first invokes the common prefix plus
`config --quiet --no-interpolate` against the exact canonical YAML. The
Controller has already resolved allowed interpolation. `--no-interpolate`
prevents the Agent helper from creating a second interpretation and makes any
remaining expression visible to the Controller's pre-dispatch validation.

After successful validation:

- full reconciliation runs `up --detach --remove-orphans`;
- targeted apply runs `up --detach <resolved-service-names...>` and never adds
  `--remove-orphans`;
- stop runs `stop --timeout <grace-seconds> <resolved-service-names...>`;
- targeted removal runs
  `rm --stop --force <resolved-service-names...>`; and
- whole-project finalization runs `down --remove-orphans`.

No command uses `--volumes`, `--rmi`, `--env-file`, an implicit Compose file,
an implicit project name, a shell, a pipeline, or string-built arguments.
Service names are separate argv elements resolved from stable ids through the
artifact table.

An Agent-side `config` rejection is an Internal procedure failure because the
Controller must validate the same canonical artifact before dispatch. A
non-zero mutating command is a failed task step. The helper returns a closed
result containing the exit code and a bounded, redacted diagnostic category;
raw stderr and stdout are short-lived and never stored in the task journal.

### 10. Make deadlines and cancellation explicit

The assignment carries one positive task timeout. Every Compose step carries
one positive timeout no greater than the task timeout. The effective deadline
is the earlier of the remaining task deadline and the step timeout. There is
no adapter, Runner, helper, Compose, or health-wait default.

The Runner and helper use the step context. On cancellation or deadline:

1. stop the Compose CLI process;
2. force-remove the helper using a separate 30-second cleanup context;
3. perform one read-only observation using a separate 30-second reconcile
   context; and
4. report `aborted` or `timed_out` with the observation and
   `reconciliation_required=true` when the mutation's completion is unknown.

A cleanup or observation invariant failure is Internal and takes precedence
over claiming a clean abort. The Controller keeps the procedure and checkpoint
and must reconcile the observed labels before retry. The Agent never infers
that cancellation means no Docker side effect occurred.

`WaitHealthy` polls read-only observation until every selected service has its
expected replica count and every replica is running with health `healthy`.
The Controller may emit `WaitHealthy` only for a service with an explicit
Compose healthcheck. A service without one is rejected before dispatch rather
than treated as healthy. The step context is the only wait deadline.

### 11. Observe through the Moby API without applying through it

The Compose CLI remains the only write path. The adapter uses the existing
official Moby API/client dependency for read-only observation. It lists
containers, networks, and volumes by the accepted Groundplane ownership
labels, then performs exact-name lookups for expected resources so an
unmanaged collision cannot be hidden by the label filter.

The typed result is:

```text
ObservedProject {
  project_name
  observed_at: Controller-independent Agent timestamp
  containers: sorted by container_id
  networks:   sorted by network_id
  volumes:    sorted by volume name
  collisions: sorted by resource kind and name
}

ObservedContainer {
  container_id
  name
  service_id
  image_reference
  image_id
  state:  created | running | exited | dead
  health: none | starting | healthy | unhealthy
  optional exit_code
  labels: sorted accepted Groundplane label pairs
}

ObservedNetwork {
  network_id
  name
  labels: sorted accepted Groundplane label pairs
}

ObservedVolume {
  name
  mountpoint_digest
  labels: sorted accepted Groundplane label pairs
}

ObservedCollision {
  kind: container | network | volume
  name
}
```

`mountpoint_digest` is SHA-256 over the Docker-reported mountpoint with a
domain separator and is used only for same-host drift comparison. The raw host
mountpoint never crosses the Agent channel, task event, log, error, metric, or
trace. A collision reports only kind and public Docker name, never labels or
host paths from an unmanaged resource.

Observation does not mutate, restart, stop, relabel, or remove anything.
Groundplane mutating methods accept only resources whose complete required
ownership tuple matches the current artifact. Missing labels, an old
generation, a different plan, or an exact-name unmanaged resource fails
closed and returns reconciliation evidence to the Controller.

The observer lists through a read-only Moby interface, inspects only resources
belonging to the generated Compose project or occupying an expected network or
volume name, and returns only accepted Groundplane labels. Docker-reserved
Compose labels are verification inputs but never ownership authority.

### 12. Keep errors, events, and output safe

The Agent emits step-started, bounded progress, terminal step state, final
observation, and Task acknowledgement through a dedicated result port owned by
the worker. The Docker adapter does not log task success, construct protobuf
messages, or mutate durable Task state.

Caller cancellation and deadline sentinels remain cancellation. Docker socket
or daemon unavailability with a live caller is a retryable runtime failure.
Malformed plans, artifact/hash mismatch, impossible ownership, unsafe paths,
helper cleanup failure, and invalid observation are opaque Internal failures.
A Compose command's ordinary non-zero exit is a typed failed-step result, not
an HTTP validation error.

Logs may contain the operation name, task id, step id, project name, service
ids, exit code, and duration. They never contain Compose YAML, helper framing,
env-file content, stdout/stderr bytes, a raw Docker mountpoint, process
environment, image-registry credentials, or a secret-derived value. Task
events may stream only output that a step-specific sanitizer has classified as
safe; no raw Compose output is persisted.

## Security boundary

The task-scoped helper prevents accidental persistent access to every
environment volume and makes the one authorized path auditable. Read-only
mounting prevents Compose from rewriting materialized files. Fixed stdin,
workdir, environment, argv, and project identity prevent ambient interpolation
or host configuration from changing a plan.

The helper is not containment from a compromised Agent. Its read-write Docker
socket is root-equivalent and can create a different container with arbitrary
mounts. The persistent Agent already has that authority under the accepted
single-host MVP. No requirement, test, operator copy, or security claim may
describe this helper as protection from an Agent-process compromise. A future
Docker authorization proxy or narrower host execution service requires a new
accepted ADR.

The Moby observation client is read-only by interface and code ownership, not
by Docker-socket permissions. Its port exposes only list and inspect methods;
mutating Moby methods do not enter the observation adapter.

The execution plan remains ephemeral derived output. Before every initial or
reconnect dispatch, the Controller deterministically regenerates it from the
retained desired-state generation and durable records, then verifies its id,
hash, generation, target, operation, and ordered step ids against the Task.
Canonical Compose bytes are never persisted as a second source of truth.

## Implementation order

1. Update the authoritative product, Blueprint, architecture, standards, and
   capability documents with the accepted lifecycle, label, project-name,
   helper, and machine-contract decisions.
2. Replace the generic protobuf task Step with the versioned typed plan,
   artifact, Compose payload, observation, event, and acknowledgement messages;
   regenerate protobuf Go code without hand editing it.
3. Replace Controller `ExecutionPlan` and `BuildPlan` with the same typed shape,
   deterministic hash, artifact bounds, service-id mapping, and
   `x-gp-execution` verification projection.
4. Implement the task-scoped Compose-helper entrypoint and its strict framed
   stdin decoder, fixed environment, Runner calls, result framing, and cleanup.
5. Replace `internal/infra/docker`'s scaffold with one deep Compose module that
   owns helper lifecycle, command construction, validation, mutation,
   cancellation reconciliation, and Moby read-only observation.
6. Inject that module plus a worker result port into the Agent worker pool;
   preserve the complete assignment and validate the plan before queueing.
7. Wire Task events, final observation, and Task acknowledgement over the
   authenticated channel with stable event identity and bounded buffers.
8. Implement start, stop, destroy, remove, reconcile, deploy, rollback, and
   component plan construction against the typed procedures.
9. Remove generic parameter maps, ambiguous `compose_down`, `removeVolumes`,
   label aliases, label-derived project names, stale SDK-or-CLI comments, and
   every compatibility decoder in the same cutover.
10. Run focused hermetic tests, race-enabled repository checks, and the real
    host L2 lifecycle/deploy/rollback scenario before changing capability
    status.

No partial runtime enables Compose dispatch before steps 1 through 7 are
complete. An internal command-builder may be developed earlier, but it is not
wired to a task or described as implemented.

## Consequences

Accepted [Backup execution contract](../features/backups/agent-protocol.md) preserves this ADR's Controller-owned plan and Console/API
boundary while making schema 1 the sole Agent channel meaning. Backup uses its
closed authority, checkpoint, transfer, terminal-delivery, and recovery
messages; it does not fall back to generic step maps or create a second Compose
execution path.

- the Agent receives enough authenticated, bounded information to apply one
  plan without rereading Blueprint state or inventing parameters;
- Compose parsing, validation, and apply retain one official dependency path;
- the persistent Agent keeps its narrow mount contract while Compose may read
  one authorized environment's materialized files;
- service Stop, Destroy, and Remove have distinct host effects and never
  delete data volumes implicitly;
- project identity survives every human-label rename;
- one label namespace becomes the only mutation authority, so stale
  development resources require explicit cleanup at cutover;
- applying and observing are separate implementations behind one deep module:
  Compose CLI mutates and Moby reads;
- cancellation produces reconciliation evidence rather than assuming an
  atomic external command;
- the protobuf and worker changes are intentionally breaking and require one
  coordinated Controller/Agent deployment; and
- Valkey restore and other non-Compose typed payload decisions remain separate
  and cannot fall back to generic execution maps.

## Alternatives considered

### Apply Compose through the Moby API

Rejected because ADR 0003 selects the Compose v2 CLI, a handwritten API apply
path would duplicate Compose semantics, and the project explicitly forbids a
parallel Docker execution implementation. Moby remains appropriate for
read-only observation.

### Run Docker Compose directly in the persistent Agent

Rejected because Compose must read materialized env/config files while ADR
0016 forbids persistent environment-volume mounts. Adding a broad
the configured workload volume-root mount would expose every environment for the Agent container's
complete lifetime.

### Render secret environment values into canonical YAML

Rejected because Compose YAML, plan hashes, protobuf tasks, diagnostics, and
replay projections are non-secret artifacts. Secret values remain transient
and enter workloads through separately materialized files.

### Mount every environment volume read-only in the persistent Agent

Rejected because it reverses the accepted narrow mount topology, requires
Agent-container recreation whenever an environment changes, and turns one
task's need into permanent ambient access.

### Preserve generic step parameter maps during migration

Rejected because a compatibility decoder would keep the arbitrary-text
execution boundary ADR 0009 rejects. Unknown operations remain unavailable
until their typed payload exists.

### Keep both ownership-label namespaces

Rejected because dual readers and writers create two mutation authorities,
make drift ambiguous, and prevent a clean determination of whether a resource
is managed. The accepted cutover removes old development resources instead.

### Derive a Compose project from slugs or authored `name`

Rejected because labels are renamable and physical identity must survive a
rename. Stable environment ids already provide a deterministic project name.

### Use `docker compose down --volumes` for Destroy or Remove

Rejected because service lifecycle and desired-state deletion are not volume
deletion authority. Data removal remains a separate explicit resource action.

### Treat missing healthchecks as healthy

Rejected because it would let a deploy or rollback switch traffic without the
health gate the Controller authored. Plans omit `WaitHealthy` for operations
that do not require a health gate and reject it when no healthcheck exists.

### Return only a boolean healthy observation

Rejected because reconciliation must distinguish absent, created, running,
exited, starting, unhealthy, and unmanaged-collision states. A boolean loses
the evidence needed for safe retry.

## Verification contract

Acceptance requires focused, race-enabled tests proving:

1. strict plan, artifact, id, enum, cross-reference, count, size, and unknown-
   field rejection before any helper or Docker call;
2. deterministic plan hashing, constant-time comparison, artifact digest
   validation, and exact `x-gp-execution` agreement;
3. project names are `groundplane-infra` or
   `gp-<lowercase-environment-id>` and remain unchanged across every label
   rename;
4. only the `com.groundplane.*` keys are accepted and old labels never grant
   mutation authority;
5. helper creation uses the digest-pinned Agent image, exact socket and
   optional one-volume mounts, read-only root, no network, dropped
   capabilities, fixed environment, fixed workdir, and no request-bearing
   argv, env, or labels;
6. helper framing rejects duplicate, unknown, reordered, truncated, extended,
   oversized, and digest-mismatched input and clears every owned buffer;
7. every apply invokes `config --quiet --no-interpolate` first against the
   identical stdin bytes and never invokes mutation after config failure;
8. full and targeted apply, stop, targeted remove, and whole-project
   finalization produce the exact argv and never add `--volumes`, `--rmi`,
   `--env-file`, a shell, or an implicit project/file input;
9. targeted apply never removes an unselected service and full reconciliation
   removes only labeled orphans belonging to the selected project;
10. Stop preserves containers, networks, and volumes; Destroy removes only
    selected containers; project finalization preserves volumes;
11. effective deadlines select the earlier Task/step deadline, cancellation
    stops and removes the helper, and an ambiguous mutation always triggers
    bounded observation before acknowledgement;
12. a cleanup or observation failure cannot be reported as a clean abort or
    successful operation;
13. `WaitHealthy` requires every expected replica to be running and healthy,
    rejects a no-healthcheck plan, and stops exactly at its context deadline;
14. Moby observation is read-only, deterministically sorted, detects unmanaged
    exact-name collisions, and never returns a raw mountpoint;
15. only complete current-plan ownership tuples may be mutated; missing,
    foreign, or old-generation labels fail closed;
16. errors, logs, events, metrics, traces, helper metadata, and task records do
    not contain YAML, env-file bytes, raw Compose output, host paths, process
    environment, or secret material;
17. worker capacity, duplicate Task assignment, TaskAbort, Task timeout,
    step progress, final observation, and Task acknowledgement remain bounded
    and race-free; and
18. a real-host L2 scenario applies twice, starts, stops, destroys, restarts,
    deploys with a health gate, rolls back, reconciles a manually stopped
    labeled container, rejects an unmanaged collision, and preserves every
    volume without handwritten scripts.

The generated protobuf output must be clean, `make ci` must pass, and no
untracked Compose artifact, helper container, or plaintext materialization may
remain after the verification suite.

## Owner approval required

Implementation remains blocked until the owner explicitly approves all of:

1. the version-1 typed `ExecutionPlan`, deterministic protobuf SHA-256 plan
   hash, bounded canonical `ComposeArtifact`, service-id mapping, and strict
   `x-gp-execution` agreement;
2. clean replacement of generic Compose step maps and ambiguous
   `compose_down` with typed apply, stop, remove, and health payloads;
3. Stop preserving containers, Destroy removing selected containers, complete
   project finalization using `down --remove-orphans`, and no implicit volume
   deletion in any service lifecycle;
4. `groundplane-infra` and `gp-<lowercase-environment-id>` as the complete
   rename-stable project-name contract;
5. clean cutover to the exact `com.groundplane.*` ownership labels with no old
   label reader, writer, alias, or migration fallback;
6. the task-scoped digest-pinned Agent helper with a Docker socket, exactly one
   optional read-only authorized environment volume, no broad persistent
   mount, and the explicit root-equivalent Docker-socket threat boundary;
7. canonical YAML through strict stdin framing, fixed
   `/run/groundplane/compose` workdir, replacement environment, no implicit
   interpolation, and exact Compose config/apply argv;
8. explicit Task/step deadlines, 30-second cleanup and observation contexts,
   cancellation reconciliation, and no assumption that a canceled external
   command made no change;
9. Compose CLI as the only mutation path and the Moby API as the read-only
   observation path with the exact state/health/network/volume/collision
   schema; and
10. the security, error, output-secrecy, implementation-order, clean-cutover,
    and verification requirements above.

Approval moves this ADR to Accepted and authorizes synchronized contract and
implementation work. It does not approve ADR 0009's Valkey RDB/AOF or
cross-database grant choices, enable generic command payloads, or mark any
runtime capability Implemented or Accepted before its required checks and L2
scenario pass.
