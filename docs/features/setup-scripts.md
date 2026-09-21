# Setup Scripts

Setup Scripts prepare storage and TLS material or run ordered migrations before
their consuming Services start. They use the existing Script runner and normal
pre-deploy hooks, so an application does not need a sleeping initializer Service.

A Script remains associated with one real logical Service. It runs once for the
selected Release, not once per Service replica. First-start setup is an ordinary
pre-deploy hook for the selected candidate Release. A manual run remains associated
with the serving Release. This feature does not add a Jobs system, a workflow engine,
a dependency graph between Scripts or a synthetic initializer Release.

The shared Task outcomes are described in [Tasks](tasks-and-logs.md).
Desired-state syntax and reserved mount paths are
owned by [the Blueprint contract](../blueprint.md), while operator actions are
owned by [the API and CLI contract](../api-cli.md). The rationale for independent
execution context and numeric order is recorded in
[Configuration materialization and Scripts](../decisions/configuration-materialization-and-scripts.md#explicit-context-and-deterministic-order).

## Configuration and behavior

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

## Design and availability

[Configuration and Script decisions](../decisions/configuration-materialization-and-scripts.md)
explain immutable captures, start authorization and cleanup. Operator-authored
Scripts own the atomicity of their side effects; GP cannot undo arbitrary writes.

See [current limitations](../capabilities.md) and the [QA matrix](../qa-matrix.md).
Implementation and historical checks are not fresh runtime qualification.
