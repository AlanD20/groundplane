# Setup Scripts

## Purpose and scope

Setup Scripts prepare storage and TLS material or run ordered migrations before
their consuming Services start. They use the existing Script runner and normal
pre-deploy hooks, so an application does not need a sleeping initializer Service.

A Script remains associated with one real logical Service. It runs once for the
selected Release, not once per Service replica. First-start setup is an ordinary
pre-deploy hook for the selected candidate Release. A manual run remains associated
with the serving Release. This feature does not add a Jobs system, a workflow engine,
a dependency graph between Scripts or a synthetic initializer Release.

The shared Script lifecycle and outcomes remain owned by
[the MVP contract](../mvp.md). Desired-state syntax and reserved mount paths are
owned by [the Blueprint contract](../blueprint.md), while operator actions are
owned by [the API and CLI contract](../api-cli.md). The rationale for independent
execution context and numeric order is recorded in
[ADR 0076](../decisions/0076-explicit-script-execution-context.md).

## Functional requirements

### Selection and order

- `order` is an integer from 0 through 65,535. Its default is `0`.
- Within one selected Service and hook phase, Scripts run by ascending `order`,
  then by the existing ASCII slug comparison.
- Service dependency order and Release Group member order continue to control
  ordering between Services.
- `order` only sorts Scripts that were already selected. It does not select an
  additional Script or Service, make a manual Script automatic, define a
  prerequisite, or add cross-Task dependencies.

### Execution context

Every Script has one complete execution context:

- If `execution` is omitted, the Script inherits the captured Service and Release
  context. `{mode: inherited}` selects that behavior explicitly.
- `{mode: explicit}` requires an immutable repository reference in
  `image@sha256:digest` form and a canonical numeric `user` in `uid:gid` form.
  It may grant exact Volumes and Entries.
- Supplying `execution` through an edit replaces the whole context. Grant lists
  are not merged. Unknown fields, mixed modes, malformed numeric users and
  mutable image references fail validation.

Inherited mode remains available when a migration needs the Service's normal
networking. Explicit mode receives only the chosen image's defaults, the fixed
runner process, the Script body and declared resources. Its working directory is
`/`. It receives no network, inherited Service environment, Service runtime
options, host paths, Docker socket, published ports or serving aliases. A Script
must not depend on changes to the container's writable image layer surviving the
run.

### Explicit resource grants

- An omitted Volume or Entry grant list means no grants; it never means inherit all.
- In a Blueprint, Volume grants use immutable Compose keys and Entry grants use
  `x-gp-entry` keys. Human API requests use stable ids.
- A granted Volume must belong to the Script's Environment. A granted Entry must
  also be exposed to the associated Service, so a Script cannot acquire another
  Service's credentials.
- Each Volume grant has a canonical absolute container target and an explicit
  read-only decision. A setup Script may receive read-write access even when the
  consuming Service mounts the Volume read-only.
- Volume targets must not overlap one another, the Script body mount or Entry file
  targets. Root targets, path traversal, host binds and the reserved kernel,
  runtime, interpreter and Docker-managed paths in
  [the Blueprint contract](../blueprint.md) fail validation. Application paths
  such as `/etc/tls` remain valid when they do not violate those rules.
- One explicit context may grant at most 32 Volumes and 64 Entries.
- Explicit managed Volume mounts disable Docker's automatic image-to-empty-Volume
  copy. Initial content is written by the authored Script, not by container creation.

### Immutable execution and lifecycle

Before publication, the Agent resolves the explicit repository digest to an
immutable Docker image id already present on that host. A missing image fails
before effects. Groundplane does not pull, build or resolve the reference again at
execution time.

The immutable runner snapshot records the explicit Script image separately from the
real Service Release image. The first selects the one-off runner image; the second
continues to bind the consumer Release. Neither identity may substitute for the
other. Inherited snapshots continue to select only the applicable Release image.

The extension preserves body source-generation references, fixed-revision
compare-and-set publication, single-runner cleanup, Abort, the 900-second timeout
and unsafe-retry barriers. The snapshot digest and execution-plan hash cover the
complete explicit context. Publication checks the selected Script revision and
the exact prepared body, snapshot and declared resource sources. Missing or
foreign resources, changed captured context and source digest mismatches fail
before execution.

Body generations remain body-only. Editing `order` or `execution` affects future
captures and never rewrites a published runner snapshot or reconstructs an
existing operation from live Script, Docker or container state.

All selected pre-deploy hooks must reach cleanup before any candidate consumer
starts. An exact reapply runs no hooks. Authors own idempotent and atomic setup or
migration behavior because rollback cannot undo arbitrary Script writes.

Script output is drained and discarded. Task detail retains typed outcomes and
cleanup or Abort evidence, but Groundplane does not add output-log storage or a
secret-bearing output stream.

### Operator surfaces

The existing Script create, edit and read actions, and the bodyless run action,
retain matching Console, CLI and API surfaces.

- The Console exposes `order` and inherited or explicit execution controls.
- CLI add and edit accept `--order` and `--execution-file`; edit also accepts
  `--inherit-execution`. An execution file contains one Blueprint-shaped YAML
  mapping, is at most 65,536 bytes, and may be read from standard input with `-`.
- The CLI resolves scoped Volume slugs and immutable Entry reconciliation keys
  through normal paginated lookup. `--id` opts into stable ids. API-owned Entries
  require ids.
- Entry list and show expose the existing non-secret immutable reconciliation key
  for Blueprint-owned Entries. This grants no new Entry edit or secret-reveal
  authority.
- API create and edit accept `order` and `execution`. A supplied execution value
  replaces the complete context.

## Non-functional requirements

- **Isolation:** explicit execution can access only its declared
  image, user and managed resources. Ambient Service or host access is absent.
- **Reproducibility:** image, Script and resource sources are captured and checked
  before execution. Candidate replay uses the captured context rather than
  current mutable state.
- **Recovery safety:** inherited behavior, durable outcomes, cleanup, Abort,
  holds for unknown outcomes and refusal to retry after start authorization remain
  intact. Lost acknowledgements must not cause duplicate Script starts.
- **Compatibility:** existing Scripts with omitted `order` and `execution` retain
  inherited behavior. Generated protobuf, OpenAPI and both clients are updated
  from their source definitions rather than edited directly.
- **Security:** secret values remain confined to existing Entry materialization;
  output is discarded and no broader Entry authority is introduced.

Shared coding, generation and verification rules live in
[standards.md](../standards.md) and [delivery.md](../delivery.md). Temporary-path
rules are in [agents.md](../agents.md#repository-local-temporary-state); current
operational permissions and safety pauses are in [head.md](../head.md).

## Technical design

Desired Script shape and pure validation live in
[`internal/core`](../../internal/core), including `Script`, `ScriptExecution` and
the authored execution specification. [`internal/controller/blueprintparser`](../../internal/controller/blueprintparser)
parses and exports Blueprint keys, while
[`internal/controller/desiredrevision`](../../internal/controller/desiredrevision)
resolves authored resource names into stable ids.

Script-owned Controller code handles API intent, full-context replacement, exact
resource selection and host-local image preparation. The relevant modules are
[`internal/controller/scriptdefinition`](../../internal/controller/scriptdefinition),
the Controller's [`script_*` modules](../../internal/controller), and the existing
Script records and revision/source-reference checks in [`internal/infra/etcd`](../../internal/infra/etcd).
Persistence continues to use the existing Script record and immutable runner/source
publication protocol.

[`internal/common/executionplan`](../../internal/common/executionplan) carries the
closed execution plan. The Agent Script runtime and Docker one-off runner in
[`internal/agent`](../../internal/agent) and
[`internal/infra/docker/scriptrunner`](../../internal/infra/docker/scriptrunner)
execute it. Composition roots only connect these modules; the existing runner's
execution restrictions remain in force.

Agent wire definitions remain in [`proto`](../../proto). Typed Controller handlers
generate OpenAPI and both clients. Console Script controls live in
[`console/src/features/script`](../../console/src/features/script). Generated Go,
OpenAPI clients and Console build output remain generated artifacts.

## Acceptance

The feature is accepted only when all of these observable conditions are proved:

1. Blueprint, persistence, frozen hook replay, API, CLI and Console preserve
   `order`; equal-order hooks use ASCII slug order and API values round-trip.
2. Explicit planning and execution use the captured image id and exact grants,
   with no ambient network, environment, mount or host authority. Foreign
   resources, unsafe targets, mutable image text, changed captured context and
   source digest mismatches fail before effects.
3. Manual runs and pre-deploy hooks use fixed-revision preparation and publication;
   source removal and unknown publication states preserve the existing recovery
   and retry fences.
4. Blueprint authoring/export and API, CLI and Console editing preserve the whole
   execution choice and resolve keys, slugs and ids under the stated ownership rules.
5. On a normal first Apply, every selected setup or migration hook cleans up before
   its consumer starts. Exact reapply starts no hook.
6. Setup or migration failure, Abort, timeout, lost acknowledgement, reconnect and
   unknown outcome keep durable evidence and never permit an unsafe duplicate start.
7. A real isolated setup proves the intended writable-setup/read-only-consumer
   pattern, including certificate preparation and migration behavior, without a
   sleeping initializer Service.

Package, generated-client and Console proof establishes local implementation only.
Live first-Apply and recovery journeys establish operator qualification. Passing
one does not imply the other.

## Current status

All requirements above are agreed. Task 4 is not accepted because required live
qualification remains deferred.

| Area | Recorded implementation and proof |
| --- | --- |
| Hook order | Grammar, persistence, frozen replay and all operator surfaces have focused local race, generated-client and browser-fixture proof. The browser writes were not live QA. [Evidence](../acceptance/script-execution.md) |
| Explicit model and plan | Pure context validation, mount/user policy, independent consumer image binding and the minimal exact-grant plan passed the recorded package race and vet checks. [Model](../acceptance/script-execution.md), [plan](../acceptance/script-execution.md) |
| Immutable sources and grants | Private metadata, primary and resource fences, local image preparation, exact projection, stored-source publication and frozen replay are connected locally. [Storage](../acceptance/script-execution.md), [primary fences](../acceptance/script-execution.md), [resources](../acceptance/script-execution.md), [preparation](../acceptance/script-execution.md) |
| Authoring and surfaces | Blueprint grant resolution/export and Script API, CLI and Console controls have recorded local race, vet, generated-client, Console and browser proof. [Authoring](../acceptance/script-execution.md), [surfaces](../acceptance/script-execution.md) |
| Runtime and composition | The zero-network panic correction and mixed-context two-Service plans have local runner, Agent and Controller proof for exact grants, the global cleanup barrier, failure, Abort and no duplicate start after lost acknowledgement. [Networkless runner](../acceptance/script-execution.md), [composition](../acceptance/script-execution.md) |
| Mutation admission | Journal-backed admission has local race and vet proof. Its guarded live trial remains deferred. [Evidence](../acceptance/safe-updates.md) |

Remaining qualification is a real first Apply and exact reapply, certificate setup,
migration failure, Abort, reconnect, unknown-outcome and guarded native trial/write
proof. Live mutations are paused until the recorded
[storage incident](../acceptance/storage-integrity-incident.md) is qualified.
The broader Script and Blueprint suite also retains pre-existing recovery and
fixture failure groups, and two broader CLI failure groups, for the later
qualification work tracked in [tasks/todo.md](../../tasks/todo.md). None of those
open items changes the requirements in this document.
