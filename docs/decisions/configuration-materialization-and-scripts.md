# Configuration materialization and Scripts

- Decision status: Accepted
- Scope: Environment storage roots, generated files, Script runners, retained
  Script sources and native router configuration

## Stable Environment storage

The workload volume root is one validated host setting, not Blueprint desired
state. Environment paths are derived only from stable ids: tenant-owned paths
use Tenant, Project and Environment ids, while Platform backing paths use the
fixed Platform namespace plus Project and Environment ids. Renaming a label is
a filesystem no-op.

`environment.volume_dir` is persisted for audit and Task binding but is never
authority by itself. Controller and Agent independently recompute it from the
configured root and durable ownership. A mismatch is corruption and causes no
filesystem effect. Changing the root with existing Environments requires an
explicit offline migration; there is no dual-root reader, automatic move or
symlink compatibility path.

Environment creation and removal are durable operations. A socketless,
task-scoped helper receives the configured root as its only writable bind and
walks below it descriptor-relatively without symlinks, magic links or nested
mount transitions. Creation proves exact root-owned directory metadata.
Deletion is coordinated by the aggregate deletion engine and removes the
Environment leaf only after contained workload and data cleanup.

## Generated files use pinned sources and a narrow writer

Generated environment files and file Entries are written only below one
authorized Environment directory. Durable plans retain metadata, exact source
generation references, length and digest, never secret plaintext. Plaintext is
resolved immediately before dispatch, streamed through bounded transient
buffers and cleared by each owner.

The Agent invokes one short-lived materializer with only the selected
Environment mounted. The helper opens a fixed root, walks descriptor-relatively,
rejects unsafe ancestors or destination types, writes a same-directory
temporary, verifies digest and metadata, atomically replaces the destination
and syncs it. Cleanup and durability failures take precedence over an earlier
operation error because an abandoned plaintext temporary is not acceptable.

Generated env files and secret file Entries remain mode `0600`; plain file
Entries remain read-only mode `0444`. File Entry uid and gid are explicit
numeric desired input and are never inferred from an image. Durable
per-Environment writer ownership serializes materialization and exact retry.
The helper reduces accidental exposure but does not contain an Agent that owns
the root-equivalent Docker socket.

## Scripts run as sealed one-off workloads

A Script has a stable id, an immutable Blueprint reconciliation key when
Blueprint-authored, a renamable Environment-scoped slug, and immutable body
generations. A published operation selects exact bodies, the applicable
serving or candidate Release, a resolved runner snapshot and every source
generation at one fixed revision.

Manual runs and lifecycle hooks use the same typed `RunScript` plan step. A
Script never enters a serving container and never uses Docker exec. The Agent
creates a sealed one-off runner from the pinned image and complete resource
projection. The body is supplied through one private read-only file rather
than argv, environment or standard input. Arbitrary adapter commands and
generic exec are not added.

Runner checkpoints separate authorization, body preparation, container
creation, process outcome and proven cleanup. Controller acknowledgement is
required before each irreversible boundary. Retry may transfer the same
operation only while non-execution is proved; once start is authorized,
recovery continues that exact execution and never starts a replacement.

The MVP does not expose Script stdout or stderr. The Agent drains and discards
it and exposes only bounded terminal metadata. Timeout, abort and invariant
failure do not terminalize the Task until the runner, body and private
directory are proved absent.

## Explicit context and deterministic order

Omitted Script execution context inherits the real Service Release. An
explicit context is a complete replacement that selects a digest-pinned image,
numeric uid/gid and exact same-Environment Volume and Entry grants. It receives
no ambient Service environment or network. Authors needing the Service network
use inherited mode; the decision does not create a general Jobs or network
permission system.

Each Script has an independent numeric order. Within a Service and phase,
selection sorts by order and then current slug. Dependency topology and Release
Group order remain outer ordering. Editing context or order affects only future
captures and does not rewrite an existing operation.

## Source references are prepared and released in bounded phases

Script operations retain exact bodies, runner snapshots, Services, Releases,
Networks, Volumes, Entry values, Secret values and materialization proofs. The
source set is a closed typed union with symmetric operation membership and
per-source counts. There is no generic reference kind or host-path fallback.

Large source sets are reserved privately in bounded, crash-resumable batches
before Task publication. The publication transaction exposes one active root;
it does not rewrite every membership. References remain through cleanup and
retry availability, then release through a separately recorded normal or
retry-expiry path. Task pruning is never reference-release authority.

A referenced source cannot be deleted. Its finalizer must prove both the count
and membership prefix empty. Missing symmetry, underflow or an orphan root is
corruption and retains evidence rather than attempting repair.

## Router configuration owns the complete native file

The router template is a complete operator-authored Caddyfile. Groundplane
reserves only `{gp.*}` references. `{gp.routes}` renders all Routes, while
`{gp.route:HOST:PATH:FIELD}` resolves one immutable Route match and exposes its
host, path or stable Service upstream. The former `{routes}` spelling is not an
alias, and native Caddy placeholders retain their native meaning.

The complete candidate receives native Caddy validation in a bounded disposable
container before serving bytes are replaced or a new router starts. File-only
updates retain the existing runtime only when the effective service definition
is unchanged, then use native reload. An invalid candidate leaves the previous
serving configuration intact. This grants no filesystem or arbitrary-command
authority to the registered Component.

## Source navigation

Environment path derivation and host mutation live in
[`internal/common/environmentpath`](../../internal/common/environmentpath/path.go)
and
[`internal/infra/docker/environmentdirectoryhelper`](../../internal/infra/docker/environmentdirectoryhelper/directory_linux.go).
Materialization is owned by
[`internal/infra/entrymaterializer`](../../internal/infra/entrymaterializer/materializer_linux.go).
Script planning and execution are in
[`internal/controller/taskplanning`](../../internal/controller/taskplanning/script_plan.go)
and [`internal/infra/docker/scriptrunner`](../../internal/infra/docker/scriptrunner/runner.go),
with retained source authority in
[`internal/infra/scriptsourcereference`](../../internal/infra/scriptsourcereference/repository.go).
Router template planning is in
[`registered-components/caddy`](../../registered-components/caddy/template.go).
