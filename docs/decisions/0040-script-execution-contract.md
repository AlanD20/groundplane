# ADR 0040: Script execution contract

- Status: Accepted; atomic bulk reference publication and close amended by ADR 0062
- Date: 2026-08-23
- Capability: C09 Scripts

## Context

Groundplane has durable per-Environment Script resources and reserves Script
execution as the explicit operator-authored automation exception to the closed
Agent step catalog. Manual runs and deploy, rollback, and failure hooks must use
one execution mechanism. They must not become adapter shell escapes, direct
commands in a serving container, or frontend-specific behavior.

The previous contract left security and recovery decisions open. It also
combined stable identity with a Script name, accepted undefined request
parameters, assumed output chunks could ride ordinary Task events, and proposed
running some Scripts through Docker exec. Docker exec cannot give the Controller
durable container ownership, exact process-tree termination, or reconnect-safe
terminal and cleanup evidence.

Arbitrary Script bodies also create an idempotency limit: once a body starts,
the Controller cannot prove that retrying it will not repeat a partial external
effect. The contract must represent that boundary rather than claiming generic
Task Retry makes arbitrary shell safe.

## Decision

### Authored Blueprint grammar

Scripts are authored in one Environment-level extension:

```yaml
x-gp-scripts:
  migration-hook:
    slug: migrate
    service: api
    when: pre-deploy
    script: |
      php artisan migrate --force
```

`x-gp-scripts` is a map. Its key is an authored, immutable reconciliation key,
not the Script slug. The key and slug are each 1 through 63 ASCII bytes and
match:

```text
^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$
```

Reconciliation keys are unique within the owning Environment and are stored
without normalization. Duplicate YAML keys are invalid. Slugs are also unique
within the owning Environment, are renamable labels, are stored without
normalization, and their raw ASCII bytes define deterministic ordering.

Each map value contains exactly:

- `slug`: required; the current scoped Script slug;
- `service`: required; the current name of one Service in the same
  Environment;
- `when`: required; exactly `manual`, `pre-deploy`, `post-deploy`,
  `pre-rollback`, `post-rollback`, or `on-failure`; and
- `script`: required; a non-blank valid UTF-8 body with no NUL byte and a
  maximum encoded size of 65,536 bytes.

Unknown fields are rejected. There is no repeated reconciliation-key field and
there are no authored parameters, arguments, timeout, interpreter, user,
working directory, or environment overrides. Desired state must not contain
literal secret values; a body consumes only values and files already resolved
for its target Service.

`x-gp-scripts` clean-replaces the old `scripts.<name>` and authored
`x-gp-task` Script wording in the Blueprint. Neither old form is accepted.

### Stable identity, rename, and reconciliation

The Controller allocates one `scr_<ulid>` when a Blueprint reconciliation key
first appears in an Environment. The Script stores that immutable key and
`origin=blueprint`; the Script id owns every durable reference. The mutable
slug is only the CLI lookup and display label. The target is stored and
returned as a stable Service id; a Service name is presentation.

Reapplying the same Environment and exact reconciliation key preserves the
Script id and reconciles `slug`, body, and `when` from that immutable submitted
bundle generation. The target Service is immutable in the MVP. Omitting an
existing Script does not delete it, and apply never infers a rename or identity
from an omitted key and a new key. A new key whose desired slug is already
owned fails conflict; it never adopts the existing Script.

A Script created through the human create capability stores `origin=api` and
has no reconciliation key. Blueprint apply never adopts it. A Blueprint key
whose slug collides with it fails conflict. Conversely, human edit may change
the slug, body, or `when` of either origin, but it never mutates an immutable
submitted Blueprint bundle or the reconciliation key. The next successful
apply of a Blueprint-origin Script deterministically restores those three
fields from the newly submitted bundle. This apply-wins rule is the complete
precedence contract; no hidden override record exists.

Rename is one explicit operator capability:

```text
PATCH /scripts/{id}
{"slug":"new-slug"}
```

The existing edit request accepts exactly any non-empty subset of `slug`,
`script`, and `when`; it rejects `service_id` and unknown fields. The endpoint
requires `Idempotency-Key`, addresses the Script by stable id, returns the
complete Script with `200`, and returns conflict when the replacement slug is
already owned in the Environment. The mutation atomically replaces the scoped
slug index and Script primary while preserving the Script id, origin,
reconciliation key, and every immutable submitted bundle.

The 1:1 rename surfaces are:

- API: `PATCH /scripts/{id}` with `slug`;
- CLI: `groundplane script edit <slug> --slug <new-slug>`, with global `--id`
  selecting the target by id; and
- Console: the Script edit form exposes the slug and submits the same PATCH.

Body and `when` edits use the same endpoint and remain available on all three
surfaces. Explicit removal remains task-backed. Slug rename never changes
stable references, authored reconciliation identity, captured operations, body
generations, or Task history.

### Immutable body generations and active references

Every Script primary records `active_generation`, beginning at 1. A body change
creates the next integer generation as an immutable record containing Script
id, generation, body size, SHA-256 digest, and exact UTF-8 bytes. An unchanged
body, slug-only edit, or `when`-only edit creates no generation. A generation
is never overwritten.

Operation publication creates a forward reference from every selected
`(script_id, generation)` to its operation and stable Script execution id, plus
the reverse operation membership. The Script primary carries the aggregate
active-reference count used by edit, remove, and publication transactions.
References remain until the operation can no longer be retried and every
selected execution has `outcome_recorded` and `cleanup_proven`, or remains
fenced `not_started` only while the retry-open rules below apply.

Publication creates the same forward/reverse references for the immutable
runner snapshot and every source generation selected by it. Those references
have the same lifetime as the Script-generation references. A source edit or
removal therefore cannot invalidate a published execution, and historical
source generations cannot be pruned while an operation may still execute or
recover them.

An inactive generation is prunable only when it is not the Script's active
generation and its forward-reference set is empty. Script removal may delete
the primary and slug index only when every generation has an empty reference
set; task-backed removal otherwise fails `resource.in_use`. Daily maintenance
prunes eligible generations in batches of at most 16.

Edit, removal, and operation publication use compare-and-swap transactions over
the same Script primary revision, slug index, active generation, reference
count, and owning Environment revision. Exactly one concurrent mutation wins:

- an edit either commits first and publication captures the new primary and
  generation, or publication commits first and retains the old immutable
  generation;
- removal atomically installs its tombstone only while the active-reference
  count is zero; and
- publication atomically creates the Task, sealed plan authority, execution
  records, forward/reverse generation references, and updated counts.

A retry that is permitted below transfers `current_task_id` ownership without
changing the operation, execution ids, plan, or generation references.

### Cardinality, size, and transaction bounds

An Environment may own at most 64 non-tombstoned Scripts. Script creation and
Blueprint apply reject a 65th Script before mutation.

A release operation, including one Blueprint apply selection, selects at most
16 hook executions in total across
pre-operation, post-operation, and possible `on-failure` phases. The aggregate
UTF-8 body bytes of all selected hook executions must not exceed 1,048,576
bytes. A manual run selects exactly one execution. The deterministic serialized
sealed execution plan, including private Script body artifacts, must not exceed
4,194,304 bytes. A violated bound fails validation before Task publication.
The same 16-execution and 1,048,576-byte limits apply to the complete Blueprint
apply selection and reject an over-bound apply before Task publication.

Blueprint reconciliation stages one complete next Script-set generation. It
writes at most 16 Script or body-generation records per batch and limits every
encoded etcd transaction to at most 128 compares plus operations and 1,048,576
bytes. A final compare-and-swap flips the Environment's active Script-set
generation; incomplete staging generations are invisible and maintenance may
prune them. PATCH, publication, reference release, removal, and pruning compute
the same operation-count and encoded-byte budgets before submitting a
transaction. An over-budget transaction is split only where the staged
generation protocol permits it; an atomic publication or ownership transfer is
rejected rather than partially committed.

### Singleton restriction

Every Script target must have effective replicas exactly 1. For blue-green,
each physical slot also has exactly one container. Script creation and
Blueprint apply reject another replica count, and Service replacement rejects
changing a targeted Service away from 1 while any Script points to it. Manual
and release publication validate the invariant again against the sealed
Service definition. The Controller never chooses a replica.

### Manual run and frontend parity

Manual execution is exactly one operator action:

```text
POST /scripts/{id}/run
```

The request is bodyless and requires `Idempotency-Key`. Any request body,
including `{}`, is malformed. Parameters, substitutions, argv, and request-time
environment values do not exist. The action may run any Script; `when` controls
automatic lifecycle selection, not whether the operator may run it explicitly.

The response is `202 {"task_id":"task_..."}`. The Task has type `script`,
targets the stable Script id, and has actor `operator`. Reusing the same
idempotency key for the same intent returns the original Task; reusing it for a
different intent is rejected.

Manual publication requires the target Service to have a non-empty
`current_successful_release_id` whose release record and immutable image digest
still exist. Absence of any of those three facts returns `409` with
`script.no_successful_release`; it creates no Task, operation, execution,
reference, or successful idempotency result. Desired Blueprint image text,
observed containers, a failed release, and an in-progress candidate are not
fallbacks. The publication transaction compares the successful-release pointer
and release revision, so a concurrent first deploy either commits first and is
captured or the manual run fails this precondition.

The three run surfaces are exactly:

- API: bodyless `POST /scripts/{id}/run`;
- CLI: `groundplane script run <slug>`, with global `--id` and no run-specific
  flags; and
- Console: the Script row Run action, which submits the same request and opens
  the ordinary Task detail surface.

Automatic hooks are steps of their parent Deploy or Rollback Task, or of the
one Blueprint Environment update Task, never secondary Tasks or additional
operator actions.

### One typed plan payload

Every manual, pre, post, and `on-failure` execution compiles to one dedicated
`RunScript` member of the existing typed `ExecutionStep` catalog. Adapters and
components cannot emit it, and it never compiles to a generic exec operation.

ADR 0022 currently closes `PlanOperation`, `ExecutionStep`, and all Moby
mutation methods against Script execution. Acceptance of this ADR explicitly
and narrowly amends that accepted contract by adding `script` to
`PlanOperation`, adding the `RunScript` oneof member below, and authorizing the
exact task-scoped runner lifecycle in the next section. A manual run uses
`PlanOperation.script`; explicit lifecycle hooks remain `RunScript` steps
inside `deploy` or `rollback`, while Blueprint apply uses the distinct sealed
`PlanOperation.blueprint_apply` inside its one Environment update Task. An
ordinary `reconcile` plan never gains Script authority. No Proposed
implementation may use that authority before this ADR is Accepted.

The payload contains exactly:

```text
script_execution_id
script_id
script_generation
environment_id
service_id
release_id
render_generation
service_definition_sha256
body_sha256
runner_snapshot_id
runner_snapshot_sha256
```

`script_execution_id` is stable for this selected execution across a permitted
Task retry and recovery. `release_id` identifies the sealed applicable release;
manual execution resolves the current successful release, pre/post execution
uses the candidate release, and `on-failure` selects one of the sealed candidate
or previous release definitions from the final serving checkpoint. The plan
hash covers this payload, the complete derived runner definition, the immutable
runner-snapshot projection, and every private artifact digest.

### Atomic runner snapshot and source fences

Publication creates one immutable `ResolvedRunnerSnapshot` for each selected
execution. It contains exactly:

```text
snapshot_id
script_execution_id
tenant_id, tenant_mod_revision
project_id, project_mod_revision
environment_id, environment_mod_revision
service_id, service_mod_revision, service_definition_sha256
release_id, release_mod_revision, image_reference, image_digest
blueprint_bundle_generation, render_generation
network_topology_revision
networks[]: network_id, network_mod_revision, rendered_attachment
mounts[]: stable source id, source_mod_revision, rendered_mount
entry_bindings[]: entry id, immutable value generation id, digest, typed destination
secret_values[]: owning resource id, immutable value generation id, digest
runner_projection_sha256
```

Image authority is a closed union. Manual, pre-deploy, and failure runners carry the already verified digest-pinned Release image. A Blueprint post-deploy runner for a tag-authored candidate instead carries only a `procedure_service` authority naming the earlier sealed `ComposeApply` step, artifact, Service, candidate Release, and requested image reference. It never contains a fabricated digest. After that step, the Agent proves the selected owned candidate container through ContainerInspect and ImageInspect, submits one typed immutable execution-step result, and waits for Controller acknowledgement before `RunScript` can receive `start_authorized`.

The Controller persists that result create-only by operation, plan hash, and step. Exact replay receives the same acknowledgement; different evidence is a state conflict and never replaces the first result. Reconnect and permitted retry reuse the acknowledged immutable reference, digest, and local image id instead of resolving the tag again. The resolved image becomes Release aggregate evidence without mutating the immutable Release intent.

The Service, network-topology, applied-Environment, per-Network, and per-mount
revision members above are represented on schema 1 by one closed
`ScriptSourceAuthority` union. `existing` contains exactly one positive etcd
`ModRevision`. `staged` contains the candidate Environment id, desired
revision id, render generation, positive fixed read revision, and SHA-256 of
the exact canonical value required in final atomic publication. Empty or mixed
authority is invalid; a staged authority must match the snapshot's candidate
Environment revision and generation. Removed revision-only field numbers and
names are reserved and are not reused.

Each `entry_bindings[]` member records the immutable Entry generation's
plaintext digest, storage classification, and exactly one typed destination:
an Environment key, or an absolute container file target with numeric uid,
gid, and mode. Bindings are selected from the fixed-revision applied
Environment projection when their exposure is `all` or the target Service
name. The snapshot also records that projection's immutable revision id,
render generation, and etcd mod revision.

Steady-state rendering uses the current successful Release as the image and
normalized Service-definition authority. An active Deploy, Rollback, or
Blueprint apply instead uses only its exact sealed candidate or predecessor
Release and candidate or applied projection until terminal promotion. Script
snapshots bind to that applicable sealed Release and projection; no successful
or serving authority changes before promotion. Publication reads the required
inputs at one fixed MVCC revision and compares every pointer and source
revision; it never substitutes desired-head or current-generation values later.

Arrays use stable-id byte order. `rendered_attachment`, `rendered_mount`, and
the runner projection are deterministic closed protobuf values, not YAML or
maps. Secret plaintext is absent from the snapshot; the private assignment
artifact carries the exact decrypted bytes selected by each referenced secret
generation.

Task publication is one compare-and-swap transaction that reads and compares
the Script primary and body generation, complete Tenant/Project/Environment
hierarchy and revisions, Service primary revision, successful or candidate
release pointer and release revision, immutable Blueprint bundle generation,
render generation, network-topology revision and each selected Network
revision, every mount source revision, every materialization current-generation
pointer, and every reusable-secret current-generation pointer. It then creates
the Task, operation, executions, snapshots, all forward/reverse references, and
counts. A changed pointer or revision retries the entire read; it never mixes
old hierarchy, Service, release, network, mount, materialization, or secret
inputs. A bound violation rejects publication rather than splitting it.

Assignment and reconnect regenerate private bytes only from those referenced
immutable generations and verify every digest against the snapshot. They never
read a current pointer. Release-plan publication applies the same transaction
to every selected hook, so hook membership, order, runner inputs, and secret
generations are one atomic operation snapshot.

The private assignment artifact table carries exactly one `ScriptBodyArtifact`
per selected execution: execution id, Script id, generation, size, SHA-256,
resolved numeric runner UID, resolved numeric runner GID, and body bytes. It
also carries one `ScriptEntryArtifact` for every typed Entry binding, repeating
the sealed binding identity and digest plus the exact immutable-generation
bytes. Assignment and reconnect reconstruct those bytes only from the pinned
generation and verify the snapshot digest; they never read an Entry current
pointer. Plaintext Entry bytes never enter the plan, snapshot, Task, event,
label, or log. The
sealed runner user is either empty, meaning `0:0`, or an exact base-10
`uid:gid`. A name, a UID without GID, image-default named user, negative value,
overflow, or any value the Controller cannot resolve to that numeric pair
fails publication with `script.user_unresolved`; the Agent never guesses from
host accounts.

The Agent derives every path component only from already validated canonical
ids and creates
`/var/lib/groundplane/agent/tasks/<assignment-id>/<script-execution-id>` as a
root-owned mode `0700` private directory. It walks from a pre-opened Agent-state
directory descriptor with beneath-only, no-magic-link, and no-symlink
resolution; every ancestor and leaf must be on the same expected filesystem,
root-owned, non-hard-linked, and of the expected type and mode. Existing,
foreign, symlink, traversal, or ownership evidence fails Internal and creates
no runner.

The Agent writes a new same-directory temporary regular file with exclusive,
no-follow creation, verifies the exact byte count and SHA-256, `fchown`s it to
the sealed numeric UID/GID, sets mode `0400`, fsyncs it and its directory, and
atomically renames it to the fixed leaf `body`. The runner receives only that
exact file descriptor-resolved host file as a read-only bind mount at
`/groundplane-script-body`. The mount target is the canonical absolute
one-component path `/groundplane-script-body`; its only ancestor is the
container root, which cannot be an image-controlled symlink. The Agent rejects
any different target, dot component, separator, or pre-normalized variant, and
Docker create must report the exact bind destination before start. A non-root
runner can therefore read its body without making the task-private directory
traversable to the corresponding host account. The body is never sent on
stdin, placed in argv or environment, copied into the runner writable layer,
or written into labels, Compose YAML, public Task events, or durable public
Task state.

### One-off runner through ADR 0022's helper boundary

Docker exec is not part of the Script contract. Every Script execution creates
one task-scoped one-off container through ADR 0022's digest-pinned, framed,
fixed-environment task helper, using the sealed applicable Service/release
definition. This rule applies equally to manual, pre, post, and `on-failure`
Scripts; no Script enters a serving container.

ADR 0022 otherwise permits only Compose-CLI writes and a read-only Moby port.
Acceptance of this ADR adds one closed `ScriptRunnerLifecycle` helper method
whose Moby write surface is exactly create, attach-output, start, wait, stop,
kill, inspect, and remove for the container id returned by that same create.
It accepts only a validated `RunScript` step and `ResolvedRunnerSnapshot` under
the current assignment fence. It cannot list by caller input, exec, copy,
commit, pause, restart, rename, update, mutate an image/network/volume, or act
on a serving container. This is the complete authorization and the only
amendment to ADR 0022's mutation boundary.

The runner projection is closed. For the pinned `compose-go/v2` v2.14.0
`ServiceConfig`, every field, including normalization-only fields, has exactly
one disposition:

- copied exactly: digest-pinned `image`, `platform`, sealed numeric `user`,
  `working_dir`, fully resolved `environment` and `env_file` result, `dns`,
  `dns_search`, `dns_opt`, `extra_hosts`, `sysctls`, `ulimits`,
  `oom_kill_disable`, `oom_score_adj`, `pids_limit`, `shm_size`, `init`,
  `isolation`, `runtime`, `read_only`, `tmpfs`, `cap_drop`, `security_opt`,
  `group_add`, `blkio_config`, `cpu_count`, `cpu_percent`, `cpu_shares`,
  `cpu_period`, `cpu_quota`, `cpu_rt_period`, `cpu_rt_runtime`, `cpus`,
  `cpuset`, `mem_limit`, `mem_reservation`, `mem_swappiness`, `memswap_limit`,
  `storage_opt`, and only the CPU, memory, and PID members of
  `deploy.resources.limits` and `.reservations`; resource device and generic-
  resource requests are rejected;
- copied exactly as resolved mounts: each `volumes`, `secrets`, and `configs`
  item, including stable source identity, type, target, read/write flag,
  propagation/consistency, subpath, no-copy flag, tmpfs options, and config or
  secret target UID/GID/mode; relative or mutable source text is forbidden;
- copied as network membership: stable Network ids plus endpoint driver
  options, interface name, and attachment priority; service/slot aliases,
  authored aliases, the Service-level and attachment-level MAC addresses,
  static
  IPv4/IPv6 addresses, link-local addresses, MAC address, and gateway priority
  are cleared so a one-off runner cannot claim serving identity or an address;
- forced: normalization-only `name` is an id-derived runner name;
  `entrypoint` and `command` are the exact values below,
  `restart: no`, `pull_policy: never`, replicas exactly one, `attach:false`,
  `stdin_open:false`, `tty:false`, `stop_signal: SIGTERM`,
  `stop_grace_period: 10s`, healthcheck absent, `logging.driver: none` with an
  empty options map, legacy `log_driver:none` and empty `log_opt`, resolved
  `env_file` paths cleared after their values enter `environment`, no
  published `ports` or `expose`, no `depends_on`, no profiles, no service or
  deploy annotations, no Compose extensions, empty normalization-only
  `custom_labels`, and exactly the ownership labels below; `container_name`,
  `hostname`, and `domainname` are absent; and
- rejected for a Script target when non-empty or non-default: `build`,
  legacy `dockerfile`, unresolved `extends`,
  `privileged`, `cap_add`, `devices`, `device_cgroup_rules`,
  `credential_spec`, `cgroup`, `cgroup_parent`, `ipc`, `pid`, `uts`,
  `userns_mode`, legacy `net`, `network_mode`, `volume_driver`,
  `volumes_from`, `links`, `external_links`, `pre_start`, `post_start`,
  `pre_stop`, `use_api_socket:true`, `gpus`, `label_file`,
  `scale`, `deploy.mode`, `deploy.replicas`, `deploy.endpoint_mode`,
  `deploy.labels`, `deploy.restart_policy`, `deploy.placement`,
  `deploy.update_config`, `deploy.rollback_config`, any device or generic-
  resource member below `deploy.resources`, `develop`, `provider`, and
  `models`.

Any field added by a later Compose schema is rejected for `RunScript` until a
new accepted disposition is added here. A resolved mount of the Docker socket,
host root, Agent state, Controller runtime, another Environment, a device node,
or the Script task-private root is rejected even if the serving Service uses
it. Other Service mounts retain their original access mode; the body is the
only additional mount. This closed projection is serialized in the snapshot
and plan hash, so the Agent never derives or drops a field.

The typed mutation replaces the Service entrypoint and command with exactly:

```text
/bin/sh /groundplane-script-body
```

POSIX `/bin/sh` is the only interpreter. A shebang remains a comment. All
referenced Service materializations must have generation-matched absence,
content, and ownership proof before `start_authorized`; a merely successful
older materialization is insufficient.

The runner carries exactly these ownership labels:

```text
com.groundplane.managed=true
com.groundplane.kind=script-runner
com.groundplane.tenant-id
com.groundplane.project-id
com.groundplane.environment-id
com.groundplane.service-id
com.groundplane.script-id
com.groundplane.script-generation
com.groundplane.script-execution-id
com.groundplane.release-id
com.groundplane.operation-id
com.groundplane.plan-id
com.groundplane.render-generation
```

Acceptance of this ADR extends ADR 0022's closed resource-kind enum with
exactly `script-runner` and its closed ownership-key set with exactly
`com.groundplane.script-id`, `com.groundplane.script-generation`,
`com.groundplane.script-execution-id`, and `com.groundplane.operation-id`.
The already accepted Tenant, Project, Environment, Service, release, plan, and
render-generation keys remain mandatory. The runner carries the complete
stable Tenant/Project/Environment hierarchy even though execution is scoped to
one Environment; omission or mismatch of any applicable label denies every
inspect and mutation. No generic custom-label allowance is added.

No body, body digest, secret-derived value, slug, current Task id, or assignment
id is a Docker label. After create, the Agent captures the immutable Docker
container id. Every inspect, stop, kill, log drain, and remove addresses only
that id after revalidating the ownership labels. The runner never uses
automatic removal.

There is one recovery-only exception before an immutable container id has been
checkpointed. At `body_prepared`, the Agent may issue one direct Docker inspect
for the exact deterministic name `gp-script-<lowercase-script-execution-id>`.
It may not list or search containers. The inspected container must be stopped
in `created` state and match the sealed name, digest-pinned image, numeric user,
entrypoint, command, logging mode, complete closed label set, and exact private
body mount. The Agent then captures its immutable id and every later operation
uses only that id. Absence permits one create. Any mismatch, running or exited
state, ambiguous result, or invalid id is a recovery invariant failure and
never authorizes replacement execution.

### Durable execution checkpoints and crash recovery

Every Script execution has exactly these durable states:

```text
not_started -> start_authorized -> body_prepared -> container_created
            -> outcome_recorded -> cleanup_proven
```

Each transition is acknowledged by the Controller and compare-and-swap fenced
by `operation_id`, `current_task_id`, `assignment_id`, `step_id`, and
`script_execution_id`. `body_prepared` stores the verified body digest, numeric
UID/GID, host device/inode identity, and fixed leaf. `container_created` stores
the immutable container id and exact ownership-label digest.
`outcome_recorded` stores the first authoritative outcome reason, optional
normal exit code, `output_truncated`, and observation timestamp.
`cleanup_proven` stores independently verified absence of that exact container
id, the exact body device/inode, the fixed `body` leaf, and the now-empty
execution directory under the same safe descriptor walk.

The Controller execution record is the sole state machine. Agent messages only
propose its next transition; neither Agent memory nor Docker state is another
checkpoint authority. The Controller's Script reconciler is the sole recovery
writer and advances the same transitions after disconnect, timeout, abort, or
Controller restart. A transition may be adopted only from evidence valid for
its immediate predecessor; states are never skipped or moved backward.

The outcome reason is one closed wire enum:

```text
normal_exit
start_failure
runtime_failure
timeout
abort
abort_before_start
expiry_before_start
no_serving_release
recovery_invariant_failure
```

`normal_exit` is the only reason that carries an exit code, including a
nonzero shell exit. `start_failure` means the body or container could not be
prepared before process start. `runtime_failure` means Docker attach, start,
wait, or terminal observation failed without a normal process exit. Timeout
and abort are authoritative over an incidental shell status. The three
before-start/no-serving reasons prove that no process started. A recovery
invariant reason is used only when no earlier process outcome was durably
recorded; a later cleanup invariant retains the earlier reason and separately
sets `reconciliation_required`.

The Controller commits `start_authorized` before the Agent may create the Task
directory, temporary body, final body, or runner. Before that checkpoint,
absence of both body and runner is required and non-execution is proven. After
body creation, the Agent must obtain Controller acknowledgement of
`body_prepared` before Docker create. After Docker create, it must obtain
acknowledgement of `container_created` before Docker start. Thus a lost create
acknowledgement can expose only an unstarted container.

Recovery applies exactly these rules:

- at `start_authorized`, an exact unacknowledged body may be verified and
  adopted as `body_prepared`; any other Task-private leaf is removed only by
  device/inode under the safe directory walk before the same body is rebuilt;
- at `body_prepared`, absence of a runner permits one create; one exact
  label-matching created runner is adopted, while a running or exited runner is
  an invariant failure because start lacked Controller acknowledgement;
- at `container_created`, a created runner may start once, a running runner is
  rejoined or terminated according to current Task authority, and an exited
  runner is inspected into `outcome_recorded`; and
- duplicate, ownership-mismatched, path-mismatched, or otherwise unverifiable
  evidence sets `reconciliation_required` and is never permission to execute a
  replacement body.

Before `start_authorized`, abort atomically moves `not_started` directly to
`outcome_recorded(reason=abort_before_start)` with proved initial absence and
then to `cleanup_proven`. Retry expiry may make the same two transitions with
`reason=expiry_before_start`. At or after `start_authorized`, abort follows the
same state machine: it never prepares a new body or creates a runner after the
abort fence, stops/kills any already created runner, records `abort`, and
proves cleanup. An execution at `start_authorized` or `body_prepared` with no
runner moves directly to `outcome_recorded(reason=abort_before_start)` after
proving runner absence.

`outcome_recorded` acknowledgement is the Agent's authority to remove the
runner and body; it is not Task terminal acknowledgement. A lost cleanup
acknowledgement may advance only from the durable outcome plus proven absence
of the exact container id, body device/inode, fixed leaf, and execution
directory. The Task cannot become terminal until every selected execution is
`cleanup_proven`.

### Timeout, cancellation, cleanup, and absence proof

Every Script execution receives exactly 900 seconds. There is no authored or
request override. Every release plan reserves 900 seconds for each of its at
most 16 selected executions; a parent deadline cannot silently shorten one.

On timeout or Task abort, the Agent stops the whole runner container with a
10-second grace period, then kills the container if it is still running. This
container/cgroup boundary terminates the complete Script process tree.
`timeout` and `abort` are authoritative; an incidental shell status does
not replace them.

Outcome observation and cleanup use a detached, non-renewable 60-second
context after execution authority ends or the process exits:

- at most 10 seconds for graceful container stop;
- at most 10 additional seconds for forced kill and stopped-state observation;
- at most 20 additional seconds for output drain, final inspect, and terminal
  evidence delivery; and
- after Controller `outcome_recorded` acknowledgement, at most 20 additional
  seconds to
  remove the exact container id, private body file, and empty execution
  directory and prove all three absent.

Failure or ambiguity in stop, kill, stream drain, inspect, removal, or absence
proof retains all available evidence, sets `reconciliation_required`, and does
not claim `cleanup_proven`. Timeout and abort do not run `on-failure`: execution
authority has been cancelled or exhausted.

Outcome reason and externally reported step result are separate. The durable
outcome reason preserves the first authoritative process fact. Result
precedence is: ownership, recovery, or cleanup invariant failure; timeout or
abort; ordinary pre-start, runtime, or nonzero-exit failure; successful zero
exit. Thus an invariant failure reports Internal with
`reconciliation_required=true` even if the preserved outcome reason is zero
exit, nonzero exit, timed out, or aborted. It can never be reported as
completed or a clean abort. The earlier reason remains subordinate typed
evidence and is never overwritten. The reconciler records Internal as the
pending final result, but the Task remains nonterminal until the invariant is
resolved and cleanup is proven; it never terminalizes merely to expose the
error.

A Task or parent operation may become terminal only after every selected
Script execution has an `outcome_recorded` checkpoint and `cleanup_proven`.
Abort prevents
all unstarted executions from receiving `start_authorized`, terminates every
authorized runner as above, and reaches Task `aborted` only after all cleanup
proofs; any invariant failure instead fails Internal and retains reconciliation
authority. No later hook starts while an earlier hook lacks cleanup proof.

Script abort uses ADR 0041 without a Script-specific public state or response.
The bodyless `POST /tasks/{id}/abort` requires `Idempotency-Key`, rejects any
request body including `{}`, targets exactly the supplied Task, and never
creates another Task or operation. A pending Script Task atomically records
`aborted_before_start` and fenced body/container absence for every selected
execution, advances them to `cleanup_proven`, sets operation retry disposition
to `abandoned`, closes the operation, releases all references, and terminalizes
that same Task as `aborted` before assignment.

For a running Script Task, the abort request subscribes before delivery of
`TaskAbort{task_id, reason:"operator_requested"}` to the exact durable Agent
generation. It returns only after the Script reconciler has stopped or killed
every authorized runner, recorded each outcome, proved every container, body,
leaf, and execution-directory absence, and committed the single Task-terminal,
operation-close, and reference-release transaction. The response is then
exactly `202 {"task_id":"<target Task id>"}`. There is no public
`abort_requested` phase or early accepted response. A private cancellation
fence may prevent new execution while acknowledgement is pending, but it is
not terminal truth, replay authority, or permission to release a reference.

An already `aborted` Task is idempotent success with the same `202` response.
A `completed`, `failed`, or `timed_out` Task returns
`409 task.not_abortable`; natural completion racing abort follows that rule.
If cleanup or reconciliation cannot yet prove the abort terminal transaction,
the Task remains nonterminal and the request cannot report success. A lost HTTP
request may be retried under the same Task-scoped idempotency key; only the
durable terminal Task state decides the replay result. No Script cleanup
continues after an `aborted` Task terminalization because this contract makes
cleanup proof part of that same terminal transaction.

Every operation records `retry_disposition` as exactly `undecided`,
`available`, `transferred`, `forbidden`, `abandoned`, or `expired`, plus
optional `retry_expires_at`. One authoritative finalization path begins only
after every selected execution is `cleanup_proven`, except that the
retry-available case requires every execution to remain `not_started` with
fenced body/container absence:

- success, any execution that reached `start_authorized`, and an accepted abort
  set retry to `forbidden` or retain `abandoned`, then enter ADR 0062's
  `normal_completion` bounded release while the Task or parent Task remains
  nonterminal; its final transaction closes the operation, terminalizes that
  Task, and publishes its retain-until index;
- a retry-eligible failure for which every selected execution remained
  `not_started` terminalizes the Task with `retry_disposition=available`, sets
  `retry_expires_at` to the Task's existing 90-day `retain_until`, publishes
  the Task's retain-until index, and retains the open operation and every
  reference; and
- permitted Retry atomically sets `transferred`, changes `current_task_id` and
  assignment fences, then returns to `undecided` under the new Task without
  changing any snapshot or reference.

There is no separate operator abandon endpoint. Abort is the explicit abandon
path for active work. For terminal retry-available work, the existing daily
Task-retention collector is the expiry authority. At or after
`retry_expires_at`, and only while no retry transfer or active-operation owner
exists, it first advances each still-`not_started` execution through
`expiry_before_start` and `cleanup_proven`, then enters ADR 0062's
`retry_expiry` bounded release with disposition `expired`. The already-terminal
Task and its existing retain-until index remain unchanged throughout that
release. Its final transaction closes the operation and deletes the reference
root without re-terminalizing the Task or republishing retention; removing the
root unblocks ADR 0035 pruning. Task pruning is blocked until that transaction
succeeds.

ADR 0062 owns the exact prepared memberships, counts, bounded release
transactions, recovery cursor, and final close transactions. Normal completion
and retry expiry are distinct durable release paths and are never inferred or
interchanged during recovery. A replay resumes only the recorded path and
cursor. Missing, underflowing, mismatched, or path-inconsistent membership,
Task, retention-index, operation, or root state is an Internal invariant
failure; no partial release occurs. A nonterminal or
`reconciliation_required` execution outside the normal finalization fence
retains every reference. Journal pruning never releases references implicitly.

### No human Script output in the MVP

The smaller MVP contract has no `ScriptOutput` Agent message, output API,
output SSE event, replay/reset token, CLI stream, or Console live-output pane.
The runner's combined stdout and stderr are attached only to an Agent-owned
drain established before container start. The runner uses Docker logging driver
`none` with no options, so Docker retains no replayable log. The Agent discards
bytes without decoding or logging them, counts through byte 1,048,577, and then
continues draining without incrementing the saturated counter.

`output_truncated=false` is valid only when the same uninterrupted attachment
observes exact stream EOF after process exit and the saturated counter never
reaches 1,048,577. It is `true` when more than 1,048,576 raw bytes were
observed, attachment or drain was interrupted, EOF cannot be proved, or
recovery first observes a container after the original attachment was lost.
The flag therefore means output was over-bound or completeness is unknown; it
never means retained output exists. Invalid UTF-8, Controller-channel
disconnect, reconnect, chunk boundaries, and terminal repaint sequences have
no human transport behavior and cannot affect execution.

API and Console expose only ordinary Task state plus the typed Script terminal
reason, optional exact exit code, and truncation boolean. The CLI prints that
same terminal metadata when it later reads the Task; `script run` itself prints
only the accepted Task id under the ordinary output formatter. Adding human
Script output later requires a new 1:1 API/CLI/Console contract with bounded
transport, redaction, reconnect/reset, encoding, and retention semantics.

Normal process exit zero completes the Script step; nonzero fails it. The exact
normal exit code is durable in the typed Script terminal result. Failure before
process start, timeout, abort, and unverifiable recovery omit a process exit
code; their terminal or recovery state is authoritative.

### Retry safety

The Agent never automatically retries a Script execution. Request idempotency
prevents duplicate Task publication; it does not make a body safe to repeat.

Generic Task Retry may transfer the pinned operation to a new Task attempt only
when every selected Script execution is durably `not_started`. The transfer
atomically changes `current_task_id` and assignment fencing while preserving
operation id, plan hash, execution ids, body generations, and active references.

If any selected execution reached `start_authorized`, or its state is unknown,
`POST /tasks/{id}/retry` rejects with `script.retry_unsafe`. Recovery continues
the same authorized execution from container evidence; it never starts a
replacement. After the barrier, an operator must choose a new manual run or
release operation with a new operation id, making possible repeated effects
explicit.

### Lifecycle selection and ordering

Release publication snapshots current Script ids, slugs, generations, bodies,
target Services, and every immutable runner snapshot/source reference
atomically with the release plan. Within one Service and phase, hooks run in
ascending current scoped Script slug byte order. A later rename does not reorder
the captured operation.

For a release group, the declared Service group order is primary. Within each
Service and each phase, current Script slug byte order is secondary. The limit
of 16 selected executions and aggregate 1 MiB of bodies applies to the entire
group operation, not separately per Service. The plan budgets an exact 900
seconds for each selected execution.

Pre-deploy and pre-rollback runners use the sealed candidate release definition
and complete before serving-runtime mutation. A pre-hook failure leaves serving
and last-successful state unchanged and needs no release compensation. Its
runner must reach cleanup before `on-failure` may begin.

After all pre hooks clean up, blue-green applies and starts the inactive
candidate, then runs matching post hooks before readiness observation, the
alias/router switch, and the serving checkpoint. Recreate removes the prior
workload, applies and starts the candidate, then runs matching post hooks before
readiness observation and recreate acknowledgement. Post hooks run as new
one-off runners from the sealed candidate release definition. This ordering
permits a healthcheck to depend on a post-deploy migration without deadlocking
the Release.

Blueprint apply publishes Script desired state and executes matching
`post-deploy` hooks only for a newly introduced singleton Service or a
materially changed existing singleton Service whose effective
`runtime_intent` is `running`. Stopped or absent existing Services retain their
desired changes for a later explicit Release action and are not implicitly
applied or hooked. Release Group members are excluded from implicit candidate
Release and hook execution; Blueprint publishes their desired change only,
while explicit group Deploy/Rollback retains declared serial order and
`on_failure` policy. Candidate Releases come only from the sealed candidate
projection. The one Environment update Task owns one operation id and one
Agent assignment; it creates no child Deploy Task or hidden Deploy request.

Services are ordered by dependency topology then slug bytes, and each
Service's hooks by slug bytes. The complete apply selection is limited to 16
hook executions and 1,048,576 aggregate UTF-8 body bytes, rejected before Task
publication. The sealed plan runs materialization, managed Volume ensure,
Attach procedures, candidate workload apply/start without readiness,
`RunScript`, `WaitHealthy`, then Component actions. Compose apply does not
intrinsically wait. Controller-only terminal publication atomically promotes
Releases, the applied projection, Components, and Routes. Failure preserves the
desired head and is accepted only with proven predecessor restoration or
first-candidate absence, so no failed candidate becomes a serving Release or
served Route. Manual Scripts do not execute during apply. Retry transfers only
while every selected execution is durably `not_started`; `start_authorized` or
an unknown state returns `script.retry_unsafe` and recovery continues the
authorized execution.

For a failure after activation, urgent `switch_back` compensation completes
before `on-failure`; a pre-activation or `leave_active` failure needs no switch.
`on-failure` runners use the sealed definition selected by the final known
serving checkpoint. A missing or ambiguous serving identity blocks
`on-failure` with `recovery_required` rather than selecting a guess.

On an initial release with no previous successful release, a pre-activation
failure has no serving definition. Its selected `on-failure` executions move
to `outcome_recorded(reason=no_serving_release)` without start
authorization and then `cleanup_proven` from fenced absence; the original
failure remains primary. Once the candidate is the proven serving release, it
is the eligible definition even if no older successful release exists. This is
not a fallback to desired image text or an observed container.

`on-failure` follows the same slug ordering, ownership, checkpoint, timeout,
output, and cleanup rules. Its failure is secondary: it never recursively runs
`on-failure`, starts another compensation, or replaces the operation's primary
failure.

### Clean replacement of earlier wording

This ADR is the sole Proposed C09 execution contract. On acceptance it
supersedes every Script-specific statement in ADR 0022 and the Blueprint that
describes `scripts.<name>`, authored `x-gp-task` Scripts, Docker exec, stdin
Script bodies, random-id ordering, execution in a serving/candidate container,
or output chunks in public Task events. Those behaviors are removed, not
compatibility modes. Non-Script decisions in ADR 0022 are unaffected.

This accepted contract replaces every earlier Script execution model. Partial
implementations must fail closed until their durable checkpoint, private Entry
artifact, and cleanup paths are production-composed; they must not retain an
older execution path as a compatibility fallback.

## Consequences

- Scripts have stable ids, bounded renamable scoped slugs, immutable referenced
  bodies, and deterministic release ordering.
- One sealed, task-scoped Compose runner mechanism serves manual, pre, post,
  and failure execution; no Script enters a serving container.
- Container ownership and Controller-acknowledged checkpoints make recovery,
  termination, and cleanup evidence exact across reconnects.
- Parameters, multi-replica Script selection, alternate interpreters, and
  per-Script runtime overrides remain post-MVP contract changes.
- Script output is drained and discarded; the MVP exposes only typed terminal
  metadata and never creates a partially replayable human-output channel.
- Generic retry remains safe by refusing to cross an arbitrary-effect start
  barrier whose non-execution cannot be proven.
- This ADR is Accepted; implementation evidence is tracked by C09 and S12.
- Acceptance requires synchronized replacement in the MVP, Blueprint,
  ADR 0022, REST/OpenAPI, protobuf, Controller, Agent, CLI, and Console; no
  layer may retain the superseded behavior.
