# ADR 0025: Environment volume-root and directory lifecycle

- Status: Accepted
- Date: 2026-08-22

## Context

Every Groundplane environment owns a host directory that contains its bind-
mounted volumes and generated file materializations. The directory is part of
the product contract: the API and Console expose `environment.volume_dir`,
volume mounts resolve below it, backups address selected volumes below it, and
tenant, project, or environment label renames must not move it.

Before this decision, conflicting contracts and scaffolds left the root and
lifecycle unclear:

- product scaffolds used `/infra/vol` while Blueprint documentation named
  `/var/lib/groundplane/vol`;
- some fixtures used mutable tenant and project slugs in physical paths;
- materializer validation could not represent a backing Project without a Tenant;
- no configuration setting owned the root or proved a supplied `volume_dir`; and
- a materializer with mountpoint creation disabled could not create its own
  prerequisite directory.

Those were design inputs, not current path authority. The decision below owns
the root and lifecycle.

Accepted ADR 0016 gives the persistent Agent no host root, generic host-runtime
mount, or workload-materialization root. It may use the Docker socket only for
Controller-supplied workload procedures. The socket remains root-equivalent,
so a task-scoped helper narrows accidental and confused-deputy access but does
not contain a compromised Agent.

The owner-approved lifecycle choices in ADR 0053 fix the shared
destructive-resource lifecycle: create a Task and tombstone atomically, expose
the resource until finalization, block mutation, process contained descendants
in stable-id postorder, perform host effects, then finalize durable deletion.
ADR 0053 owns the Environment-specific public `DELETE /environments/{id}`
entry, parent operation, tombstone, lock, immutable plan, receipts, retry, and
parent-last finalization. This ADR supplies only the Environment path and
directory effects consumed by that shared engine.

Accepted ADR 0020 describes Entry materialization below one already-existing
environment directory. It does not select the host root, create an environment
directory, or define its deletion. It consumes the directory lifecycle here.

Without one decision, Environment creation would have to guess a public path,
privileged directory owner, failure state, backing-project namespace, and the
component allowed to create or delete host data.

## Existing constraints

Any accepted solution must preserve these contracts:

- every variable physical-path segment is a stable id, never a slug or label;
- tenant, project, and environment rename does not move workload data;
- volumes and Entry destinations remain below their environment directory;
- backing environments use the same environment-owned volume behavior as
  tenant environments;
- the persistent Agent receives no workload-materialization mount;
- filesystem side effects occur only through a durable Task after validation;
- the Controller authorizes the exact scope and the Agent executes the closed
  host procedure;
- deletion enters ADR 0053's shared tombstone, ordering, checkpoint, receipt,
  and parent-last finalization engine; and
- superseded roots and slug-derived paths are removed without a compatibility
  reader, alias, symlink, or dual-write period.

## Recommended contract

### 1. Select one configured root

The Controller startup configuration gains:

```yaml
storage:
  volume_root: /var/lib/groundplane/vol
```

`/var/lib/groundplane/vol` is the default. `storage.volume_root` is a local
host setting, not desired state, a Blueprint field, or an operator-facing
Controller resource. The Controller-owned Agent runtime policy receives the
same selected value so both processes validate assignments against one root.
A Task never supplies or overrides the root.

The configured value must be non-empty, absolute, UTF-8, already clean, and
not `/`. It has no trailing separator. Startup opens the path from `/` with
descriptor-relative resolution that rejects symlinks and magic links in every
component. Crossing onto the selected root's filesystem is allowed so an
operator can mount dedicated storage there. The selected root itself must
already exist as a directory owned by numeric `0:0` with exact mode `0700`.

The packaged installation creates the default root with those attributes.
An operator selecting a custom root must provision it before starting the
Controller. The Controller does not create an arbitrary configured base path,
repair its metadata, follow a symlink, or fall back to the default.

The root is immutable for the Controller process lifetime. At startup the
Controller recomputes every persisted environment path using the configured
root. If any existing record selects another root or path, startup fails
closed before accepting mutations or dispatching Agent work. A different root
may be selected normally only when no environment records exist. Moving an
installation with existing environments is an explicit offline migration and
is outside the MVP.

### 2. Derive tenant and backing paths from stable ids

Tenant environment directories have exactly this shape:

```text
<volume-root>/<tenant-id>/<project-id>/<environment-id>
```

Backing environment directories have exactly this shape:

```text
<volume-root>/platform/<project-id>/<environment-id>
```

`platform` is a fixed namespace sentinel, not a mutable project slug. Tenant,
project, and environment ids use their registered kind-prefixed ULID grammar.
No additional label, name, slug, adapter kind, or service name participates in
the directory identity.

The Controller derives the path after reading the durable Project and its
owner. A tenant Project requires its durable tenant id. A backing Project must
have no tenant id and uses the `platform` branch. Any impossible ownership or
id combination is Internal and causes no filesystem side effect.

Volume names and Entry destinations remain validated relative descendants of
the environment directory under their existing product contracts. This ADR
does not introduce volume rename or change the meaning of those descendants.

### 3. Treat `volume_dir` as a redundant exact invariant

`environment.volume_dir` is Controller-generated, immutable, and read-only to
operators. Environment create requests and Blueprint input never accept it as
an authored value. API and Console reads may expose it as a generated field.

The durable Environment record persists the exact generated value so a Task,
audit record, and observed report can bind the host identity they used. That
field is redundant rather than authoritative by itself. Repository and service
boundaries enforce all of the following:

- create derives the value from the configured root and durable owner ids;
- update and rename cannot change it;
- record assembly recomputes and compares it;
- render and Task construction reread ownership and compare it; and
- the Agent independently derives and compares it from its runtime policy and
  the typed scope in the assignment.

A syntactically valid absolute path is not authorization. A stored mismatch is
durable corruption and fails Internal without touching either possible path.

### 4. Make creation a durable provisioning operation

Environment creation generates one environment id before its first durable
transaction. The request's idempotency record binds that id and every retry
reuses it.

The initial transaction creates the Environment record and indexes, the Task,
the active Environment create operation, and the idempotency evidence. The
Environment record carries the closed provisioning state and owning Task id:

```text
provisioning | ready | failed
```

The Environment is visible while `provisioning` or `failed`. Reads expose its
state and Task reference. Every descendant mutation, render, deploy, backup,
or materialization requires `ready`; while creation is active it fails with
the accepted active-operation conflict, and while creation has failed it fails
with `resource.in_use` until the operator retries or deletes the Environment.
Rename is also blocked until `ready`.

The creation Task dispatches exactly one directory `ensure` procedure. Success
requires all derived ancestors and the Environment leaf to exist with exact
metadata and to be durably synced. The terminal transaction compares the
captured Environment, Task, active-operation, and idempotency revisions, sets
the Environment to `ready`, completes the Task, and releases the active
operation.

A deterministic host or invariant failure sets the Environment to `failed`,
fails the Task, and releases the active operation in one transaction. It does
not delete the record, generate a new id, or guess that partial directories are
safe. Task retry re-enters `provisioning` for the same Environment id and exact
path and reruns the idempotent `ensure` procedure. Environment delete is also
allowed from `failed` and cleans any partial owned path through the deletion
procedure.

Cancellation or connection loss after a helper may have acted is an unknown
host outcome. The Task remains nonterminal until observation inspects the exact
path descriptor-relatively. Exact expected directories and metadata prove the
`ensure` side effect; absence proves it did not finish; unsafe or mismatched
state is Internal and moves the Environment to `failed`. Later etcd state alone
never proves a filesystem side effect.

### 5. Execute through one task-scoped directory helper

The Agent owns an `EnvironmentDirectoryRunner` port. Its Docker implementation
performs one closed invocation: create a helper, attach its private stdin,
start it, wait for its terminal result, and forcibly remove it. It is separate
from the persistent Agent container lifecycle and from ADR 0020's per-file
materializer runner.

The runner selects the host source exclusively from the Agent runtime
`storage.volume_root` policy. For each invocation it creates a helper from the
exact Controller-approved, digest-pinned Agent image with:

- user `0`, restart policy `no`, network mode `none`, and disabled networking;
- a read-only root filesystem, default seccomp policy, all capabilities
  dropped, and `no-new-privileges`;
- no Docker socket, devices, secrets, credentials, or additional mounts;
- no request data in environment variables, labels, or command arguments;
- exactly one read-write `rprivate` bind of the configured host volume root at
  `/run/groundplane/volumes`; and
- bind mountpoint creation disabled.

The helper command is a closed entrypoint, never a shell. Once Docker returns a
container id, every success, error, cancellation, and attach failure attempts
bounded terminal cleanup and explicit removal. Success requires a zero helper
exit and successful removal. Cleanup failures are Internal.

The typed stdin request contains only:

```text
task_id
step_id
operation: ensure | remove
project_kind: tenant | backing
tenant_id: present only for tenant
project_id
environment_id
expected_volume_dir
expected_uid: 0
expected_gid: 0
expected_mode: 0700
```

The Controller plan, Agent, runner, and helper each validate the closed enums,
id grammar, ownership shape, and exact derived path. The helper receives
`expected_volume_dir` only for cross-checking and diagnostics suppression; it
never opens or reconstructs the host absolute path. It operates below its
fixed mount using the typed relative scope.

The same socketless helper accepts one separate
`ManagedVolumeDirectoriesEnsure` execution step for a sealed Environment
Compose artifact. That step carries only `artifact_id`; the helper derives
the uniquely named managed volume leaves from the authenticated artifact
table. It requires the Environment namespace to exist with the metadata above
and creates only direct root-owned mode `0755` children. It never creates
missing Environment ancestors through this operation and never accepts a
caller-authored relative or absolute leaf path.

### 6. Enforce a descriptor-relative filesystem boundary

The helper opens `/run/groundplane/volumes` once and treats the descriptor as
its only filesystem root. Below it, every existing directory is opened with
Linux resolution equivalent to:

```text
RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS |
RESOLVE_NO_MAGICLINKS | RESOLVE_NO_XDEV
```

Unsupported kernels fail closed. The configured root may itself be a mount
point, but no nested mount transition is accepted below it.

For `ensure`, the helper creates each missing namespace, tenant, project, and
environment component one at a time with `mkdirat`, numeric owner `0:0`, and
exact mode `0700`. It opens and verifies each new component before continuing
and syncs its parent after creation. Existing components must be directories
with the same owner and exact mode and must not be symlinks, magic links, or
nested mounts. A mismatch is Internal; the helper never repairs or replaces
it automatically.

The Environment lifecycle operation does not accept arbitrary relative paths.
Tenant requests traverse
exactly three validated id components. Backing requests traverse the literal
`platform` followed by exactly the project and environment ids. Extra, empty,
dot, dot-dot, slash-containing, or backslash-containing components fail before
mutation.

The fixed root bind is a capability boundary against accidental path
selection, not a sandbox from a compromised Agent. ADR 0016's persistent
Agent still owns a root-equivalent Docker socket. No requirement or test may
claim stronger containment.

### 7. Keep rename filesystem-free

Tenant, project, and environment rename changes labels, document placement,
indexes, URLs, and presentation only. It does not dispatch a directory helper,
move a path, rewrite `volume_dir`, or recreate a Compose project. Rendered
labels may change while physical identity remains fixed.

### 8. Integrate removal with ADR 0053 shared deletion

Environment deletion is the Environment-specific public entry into ADR 0053's
shared deletion engine. It uses the accepted `environment.delete` Task and
tombstone transaction; this ADR defines no parallel tombstone, lock, plan,
receipt, or public DELETE protocol. The shared tombstone blocks new
descendants and mutations while the Controller processes contained resources
in stable-id postorder.

The directory `remove` step runs only after workload containers, task-scoped
helpers, and other accepted consumers of the environment path are terminal or
removed, and after every contained resource has completed its own required
external cleanup. It removes exactly the derived Environment leaf beneath the
configured root.

Removal walks descriptor-relatively and never follows a directory symlink or
crosses a nested mount. Ordinary symlinks, hard links, sockets, FIFOs, devices,
files, and directories inside an owned volume are removed as directory entries
without opening or following their targets. A nested mount, substituted
ancestor, ownership mismatch on the stable-id ancestors, or path derivation
mismatch fails Internal for operator inspection. Recursive removal is bounded
and checkpointed under ADR 0053's shared deletion tombstone rather than held in
one unbounded etcd transaction.

After removing the Environment leaf, the helper syncs the project directory.
It then removes now-empty generated ancestors as follows:

- remove the project-id directory when it is empty;
- for a tenant path, remove the tenant-id directory when it is empty; and
- for a backing path, retain the shared `platform` sentinel permanently.

`ENOTEMPTY` during optional ancestor cleanup is benign because another durable
Environment owns the remaining sibling. Any ancestor actually removed is
followed by a parent sync.

Only after the host removal checkpoint is durable does the ADR 0053 shared
finalization delete the Environment record, indexes, tombstone, and active operation and
complete the Task. Because the tombstone blocks mutation, a final CAS conflict
is an invariant failure handled exactly as ADR 0053 specifies. Reconciliation
repeats the idempotent `remove` observation or operation; it never deletes a
different path inferred from current labels.

Project and tenant deletion reuse the same Environment cleanup action for contained
Environment leaves before their durable parents finalize. Deleting one
Environment never removes the configured volume root or the `platform`
sentinel.

### 9. Fail closed during recovery and configuration changes

Controller startup validates the configured root before starting schedulers,
Agent dispatch, or mutation listeners. It then validates every durable
Environment's derived path.

Recovery follows the durable operation state:

- an in-flight `provisioning` Environment resumes or observes its exact create
  Task and path;
- a `failed` Environment is not automatically retried;
- a `ready` Environment with a missing directory is data-loss drift and is
  reported Internal rather than silently recreated;
- a deletion tombstone resumes from its durable host checkpoint; and
- a directory with no matching durable Environment or deletion operation is
  reported as an orphan and is never automatically removed or adopted.

Changing `storage.volume_root` never triggers online path moves, symlink
creation, record rewriting, or directory adoption. An operator must return to
the recorded root or perform a separately specified offline migration.

### 10. Replace old roots and slug paths cleanly

Acceptance requires one synchronized cutover from both `/infra/vol` and
slug-derived paths. The implementation changes configuration, derived models,
hierarchy validation, runtime Agent policy, runners, API/Console projections,
fixtures, and authoritative documentation together.

There is no `/infra/vol` fallback, dual-root scan, compatibility validator,
slug-path reader, automatic move, or symlink alias. Pre-acceptance development
data must be removed or moved offline before starting the accepted build. The
Controller refuses old durable `volume_dir` records rather than silently
claiming their directories.

## Persistence and API effects

The Environment durable record gains the closed provisioning
state and create Task reference in addition to its generated `volume_dir`.
The exact Environment create transaction joins the record, unique and
membership indexes, Task, active-operation index, and idempotency evidence and
must remain within ADR 0053's shared transaction limits and the persistence
constraints inherited from ADR 0013.

The public Environment projection exposes `volume_dir`, provisioning state,
and the current or failed create Task id. `volume_dir` remains read-only.
Existing Environment create, retry, rename, and delete actions retain their
Console/CLI/API parity; this ADR does not add a local filesystem command or an
operator endpoint for arbitrary path management.

The typed deletion checkpoint records at least the target environment id,
derived path identity, phase, last completed stable-id batch where applicable,
and whether the Environment leaf was observed absent after removal. It never
stores a slug-derived path or treats an unchecked absolute string as authority.

## Implementation order

1. Accept or replace every owner choice in this ADR.
2. Synchronize authoritative root and lifecycle documentation without retaining
   `/infra/vol` or slug-derived examples.
3. Add and validate `storage.volume_root`, copy it into Agent runtime policy,
   and fail startup on durable-path mismatch.
4. Centralize tenant and backing derivation in one typed pure function used by
   Controller persistence, rendering, Task construction, and Agent validation.
5. Make `volume_dir` generated and immutable at repository and API boundaries.
6. Add the directory-helper protocol, Docker runner, descriptor-relative helper,
   observation, and cleanup behavior.
7. Add the Environment provisioning state machine and idempotent retry.
8. Add ADR 0053-integrated environment directory removal and reconciliation.
9. Cut over Console/store fixtures, API DTOs, and tests in the same contract
   change.
10. Only then connect Proposed ADR 0020 materialization to ready Environment
    directories; ADR 0020 still requires separate owner acceptance.

## Alternatives considered

### Keep `/infra/vol` fixed

It matches current scaffolds but conflicts with the canonical Blueprint path
and prevents operators from selecting a dedicated storage filesystem. It is
replaced rather than retained as an alias.

### Fix `/var/lib/groundplane/vol` without configuration

A single fixed path is simpler, but a self-hosted single-machine control plane
must commonly place persistent workload data on a dedicated disk. One validated
startup setting permits that without making the path desired state.

### Use tenant and project slugs for readability

Slugs are renameable labels. A slug path either moves data during rename or
silently disconnects durable state from its old directory. Stable ids are the
accepted physical identity.

### Omit `volume_dir` from durable records and always recompute it

Pure derivation removes redundancy but loses the exact host identity captured
by an in-flight Task and makes configuration drift easier to apply silently.
Persisting and independently recomputing the value turns disagreement into a
fail-closed invariant.

### Let Docker create missing bind sources

Implicit mountpoint creation does not prove stable ownership, owner/mode,
symlink safety, durability, or the etcd operation that authorized it. The
directory helper makes creation explicit, observable, and retryable.

### Write directories directly from the native Controller

This gives the product brain a second workload host-mutation path and bypasses
the accepted Controller-plan/Agent-execution topology. The Agent already owns
Controller-authorized Docker workload procedures; a task-scoped helper keeps
the execution boundary consistent.

### Persistently mount the volume root into the Agent

ADR 0016 explicitly forbids workload-materialization roots in the persistent
Agent. A short-lived helper limits ambient exposure to one closed operation,
although it does not contain a malicious Agent with the Docker socket.

### Move directories automatically when configuration changes

Online movement introduces partial copies, cross-filesystem rename behavior,
running bind mounts, rollback, free-space, and crash-recovery semantics outside
Environment creation. The MVP fails closed and requires a later explicit
offline migration contract.

### Delete unknown or orphan directories automatically

An unmatched directory may be the only remaining copy after lost durable
state. Automatic adoption or deletion risks silent data loss. Recovery reports
the orphan for explicit operator handling.

## Consequences

- Every Environment has one deterministic host identity across all label
  renames and Controller restarts.
- Tenant and backing Environments share one derivation contract without using
  a fake mutable tenant slug.
- Operators can select a dedicated volume filesystem through one startup
  setting, but cannot change it online while Environments exist.
- Environment creation becomes an observable, retryable Task rather than an
  etcd record that assumes a host directory exists.
- The persistent Agent's accepted mount allowlist remains unchanged.
- One short-lived directory helper sees the shared volume root for the duration
  of a closed operation; descriptor-relative policy constrains mistakes, not a
  compromised Agent.
- Root, ancestor, and leaf metadata become strict invariants. Groundplane does
  not silently repair pre-existing unsafe host state.
- Ready-state directory loss is surfaced as data-loss drift rather than hidden
  by automatic recreation.
- Environment deletion has a crash-recoverable host checkpoint integrated with
  ADR 0053's shared tombstone rather than an unsafe plain recursive delete.
- Existing `/infra/vol` development data and slug-derived fixtures require a
  clean offline cutover.
- Accepted ADR 0020 consumes this exact prerequisite environment root.

## Verification required before implementation is complete

- Config tests cover the default, a valid custom mounted root, relative paths,
  dirty paths, `/`, trailing separators, missing roots, final and ancestor
  symlinks, wrong type, wrong uid/gid, and every wrong mode.
- Startup tests reject any persisted `volume_dir` that differs from the exact
  configured-root derivation and accept a root change only with no Environment
  records.
- Pure derivation tests cover tenant and backing scopes, every wrong id kind,
  missing or extra owner ids, slashes, backslashes, dot components, and mutable
  label changes.
- Repository tests prove create is the only writer of `volume_dir`, rename
  preserves it byte-for-byte, public mutation cannot supply it, and corrupt
  stored paths fail Internal.
- Creation transaction tests prove one environment id across identical retry,
  mismatch rejection, active-operation exclusion, descendant blocking, and
  atomic transitions to `ready` or `failed`.
- Crash tests cover failure before helper creation, after each ancestor mkdir,
  after leaf mkdir, after sync, after helper success but before acknowledgement,
  and before the terminal etcd transaction.
- Agent reconnect tests prove observation resumes the exact Task and cannot
  authorize a different path or root.
- Runner tests assert one fixed root bind, disabled mountpoint creation, no
  socket/network/devices, a read-only root filesystem, all capabilities
  dropped, request-free argv/env/labels, pinned image, and bounded cleanup.
- Helper tests cover ancestor substitution races, symlinks, magic links,
  nested mounts, wrong metadata, unsupported `openat2`, duplicate ensure,
  cancellation, parent sync failures, and sibling isolation.
- Rename tests cover tenant, project, environment, and backing-project labels
  and prove no helper dispatch or path change.
- Deletion tests cover active workloads, partial descendant batches, ordinary
  in-tree symlinks and special entries, nested-mount refusal, repeated remove,
  empty and non-empty ancestor cleanup, backing `platform` preservation,
  final CAS recovery, and cancellation at every checkpoint.
- Recovery tests prove a missing ready directory is not recreated, an orphan is
  neither adopted nor deleted, and an in-flight provisioning or deletion Task
  resumes only its recorded stable-id path.
- Contract scans reject `/infra/vol`, slug-derived volume paths, compatibility
  roots, and caller-authored `volume_dir` after the synchronized cutover.
- System tests create, retry, rename, materialize, stop, and delete both tenant
  and backing Environments on a real Docker host and dedicated volume-root
  mount.

## Accepted owner choices

On 2026-08-22 the owner accepted the recommendation to proceed without
additional approval rounds. The accepted choices are:

1. `storage.volume_root` with default `/var/lib/groundplane/vol` and no
   `/infra/vol` fallback.
2. Exact startup validation, `0:0` mode `0700`, pre-provisioned base root, and
   root immutability while any Environment record exists.
3. Tenant path `<root>/<tenant-id>/<project-id>/<environment-id>` and backing
   path `<root>/platform/<project-id>/<environment-id>`.
4. Controller-generated, persisted, read-only `volume_dir` with independent
   exact derivation checks at persistence, plan, and Agent boundaries.
5. Visible `provisioning | ready | failed` Environment creation lifecycle,
   descendant blocking, same-id retry, and deletion of failed creation.
6. The task-scoped, socketless directory helper with the configured root as its
   only read-write bind and the explicit threat boundary for a Docker-capable
   persistent Agent.
7. Descriptor-relative no-symlink, no-magic-link, and no-nested-mount policy,
   with exact root-owned `0700` ancestors and leaves.
8. Rename as a filesystem no-op for every label above the Environment.
9. ADR 0053-integrated recursive Environment removal, empty ancestor cleanup,
   permanent backing `platform` sentinel, and fail-closed recovery.
10. Clean synchronized replacement of `/infra/vol`, slug paths, and permissive
    caller-supplied durable paths, with no compatibility or automatic online
    migration.
11. ADR 0020 as a dependent Proposed decision that receives an existing ready
    Environment root but is not accepted by this ADR.
