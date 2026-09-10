# Spec: setup-scripts

Task4 of the approved production initiative. Product, Blueprint and API contracts
remain authoritative; ADR0076 closes the execution-source and ordering change.
Implementation and operator qualification are separate checkpoints.

## Objective

Run storage/TLS preparation and ordered migrations through normal Script hooks,
before their consumers start, without a sleeping initializer Service. A Script
still belongs to one real logical Service and runs once per selected Release,
not once per replica. This is not a second Jobs or workflow system.

## Contract

- `order` is an integer0through65535, default0. Within each selected Service
  and hook phase, order ascending precedes the existing ASCII slug tie-break.
  Service dependency topology and Release Group member order stay authoritative
  across Services. Order is not a prerequisite selector and does not cause an
  unselected Script or Service to execute.
- Omitted `execution` inherits the existing sealed Service/Release context.
  `{mode: inherited}` explicitly restores that choice. `{mode: explicit}`
  requires an immutable repository `image@sha256:digest` and canonical numeric
  `user: uid:gid`, plus optional exact Volume and Entry grants.
- Explicit execution has no network, inherited Service environment, Service
  runtime options, host paths, Docker socket, published ports or serving aliases.
  Working directory is`/`. It receives only its image defaults, fixed runner
  process, body and declared resources. No writable-image persistence is assumed.
- Blueprint Volume grants name immutable Compose keys; Entry grants name
  `x-gp-entry`keys. Human API grants use stable ids. Volumes must belong to the
  same Environment; Entries must also be exposed to the associated Service.
  Omitted grant lists mean no grants, never inherit-all.
- Each Volume grant has a canonical absolute target and explicit read-only
  decision. Targets may not overlap one another, the body mount or Entry file
  targets; root, traversal, reserved kernel/system paths and host binds fail.
  The exact reserved path list is in`docs/blueprint.md`; it protects the body,
  interpreter, runtime/kernel and Docker-managed files without excluding
  application subtrees such as`/etc/tls`. Grant limits are32Volumes and64Entries
  per explicit context.
  Explicit managed mounts disable Docker's automatic image-to-empty-Volume
  copy; initialization writes belong to the authored Script, not container create.
- The Agent resolves the explicit digest reference before publication to an
  immutable host-local Docker image id. Missing images fail before effects;
  no pull, build or late re-resolution occurs. The runner snapshot seals this
  source separately from the real Service Release and retains both identities.
- Existing source-generation references, fixed-revision/CAS publication,
  single-runner cleanup, Abort,900-second timeout and unsafe-retry barriers remain.
  Body generations remain body-only; order/context edits affect future captures,
  never rewrite a published runner snapshot. Manual run still needs a serving
  Release. Initial setup is the selected candidate's pre-deploy hook.
- All selected pre-deploy hooks reach cleanup before any candidate consumer
  starts. Exact reapply does not rerun hooks. Authors own idempotent, atomic
  output and migration semantics; rollback cannot undo arbitrary Script writes.
- Script output remains drained and discarded per the MVP; typed outcomes and
  cleanup/Abort evidence remain visible in Task detail. No output-log storage or
  secret-bearing stream is introduced by this context extension.

## Surfaces and implementation boundaries

Existing Script create/edit/read and bodyless run actions remain1:1. Console
adds order and inherited/explicit context controls. CLI adds`--order`and
`--execution-file`to add/edit, and`--inherit-execution`to edit; the file uses
Blueprint-style shape in one YAML mapping, at most65,536bytes, with`-`for stdin.
Scoped Volume slugs and immutable Entry reconciliation keys resolve by normal
paginated CLI lookup (`--id`opts into ids; API-owned Entries require ids).
Entry list/show exposes the existing non-secret immutable reconciliation key
for Blueprint-owned records, not new edit or reveal authority.
API create/edit accept`order`and`execution`; a supplied
execution replaces the complete context rather than merging grant lists.

Pure desired shape/validation lives in`internal/core`; the Blueprint parser and
desired reconciler resolve authored names to stable ids. Script-owned Controller
modules own API intent, exact grant projection and image preparation. Persistence
keeps the existing Script record and immutable runner/source protocol. The shared
execution plan and existing Docker one-off runner carry the closed result. New
feature behavior does not grow the composition root or add an adapter escape.
Generated protobuf/OpenAPI/clients are regenerated from their sources.

## Ordered implementation and proof

1. Deliver order across grammar, persistence, frozen hook replay and all three
   surfaces, with failing-first sort and API round-trip tests.
2. Prove explicit image/source and resource isolation in the pure runner plan,
   then wire fixed-revision preparation and publication for manual and hooks.
3. Complete explicit context authoring/export, API/CLI/Console and source fences.
4. Verify normal first apply, exact reapply, setup/migration failure, Abort,
   unknown outcome and retry without duplicate Script starts.

Use repository-pinned Go/Node tools with all caches and evidence under`.tmp`.
Focused proof:

```sh
go test -race -count=1 ./internal/core ./internal/controller/blueprintparser ./internal/controller/desiredrevision
go test -race -count=1 ./internal/common/executionplan ./internal/controller/blueprintrelease ./internal/controller/releaseoperation
go test -race -count=1 ./internal/controller ./internal/infra/etcd ./internal/agent -run Script
make generate
npm --prefix console test
npm --prefix console run build
```

Use adjacent tests with rationale comments, typed concrete inputs/results,
`errs.New/Wrap`, and module-pinned120-column formatting. Runtime/source tests
must reject foreign resources, extra ambient access, mutable image text, changed
captured context and rehashed mismatches; they must preserve existing inherited
and recovery semantics. Browser proof uses an isolated session.

Always preserve source fences, secret isolation and execution bounds. Ask before
production/network scope changes or a new external dependency. Never commit
credentials, run setup out of band, weaken cleanup/retry guards or call deferred
QA passing. Live QA awaits the recorded storage incident qualification; local
proof and independent work continue under the owner's instruction.
