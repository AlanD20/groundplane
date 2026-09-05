# Groundplane — Project Architecture

Implementation contract for the MVP. Companion to `mvp.md` (what the
product is) and `api-cli.md` (the human surfaces); this doc is *how the
code is organized* — the enforceable rules (import matrix, error type,
Runner, banned patterns, CI gate) are in `standards.md`. The shape is
inspired by the mvmctl codebase
(internal/cli · internal/core · internal/service · internal/infra ·
internal/lib), but the exact package names may vary — the structure is
what matters, and it exists to enforce three principles:

1. **Decoupling.** Entry points are thin; surfaces (Console/CLI/API) call
   one backend; the domain has no infrastructure imports. A change in
   storage, docker, or etcd never ripples into CLI or domain code.
2. **Logic in one place.** Shared behavior lives in one narrowly named leaf
   package under `internal/common`; `common`, `util`, `helpers`, `types`, and
   `interfaces` are not catch-all modules. No copy-paste between Controller,
   Agent, and CLI.
3. **Unified implementation for scaling.** There is one Task journal with two
   closed executors, one renderer, one scheduler tick, and one materialization
   pattern. Scaling or extending the MVP means strengthening those paths, not
   adding ad-hoc alternatives.

## Language & runtime

- **Go** — one language for the Controller, the Agent, and the CLI.
  Rationale: etcd (go.etcd.io/etcd), gRPC, Cobra, and slog are all
  first-class; the deliverable is small static binaries for amd64 and arm64 (the
  qa-workload host).
- One repository, three binaries — no microservices:
  `cmd/groundplane` (CLI), `cmd/controller` (server), `cmd/agent`
  (agent).

## Repository layout (structure, not gospel)

    cmd/groundplane      CLI entry — thin: init ctx, init logging, run root
    cmd/controller       Controller entry — root daemon, systemd unit,
                         runs the API server + scheduler
    cmd/agent            Agent entry — the ECS-agent container image
    internal/app         Composition roots only: construct concrete modules,
                         register transports, run and shut down processes
    console/             The Console frontend — React 19 + Vite + shadcn/ui
                         (Base UI) + Tailwind v4, a static SPA whose dist/
                         is go:embed'd into the Controller binary. The
                         production Console (console/) is the shipped UI; its
                         components and store pattern carry over. See
                         "Console (frontend)". A build-tagged, stdlib-only Go
                         file beside dist/ exposes its embedded fs.FS only to
                         internal/app production wiring.
    internal/cli         Cobra commands — one file per noun, ZERO logic,
                         delegates to the human API; common/ holds the
                         shared error handler + output helpers
    internal/controller  Capability-owned Controller use cases, typed HTTP
                         adapters, Task orchestration, renderers, and scheduler
    internal/agent       Capability-owned gRPC client and execution modules:
                         bounded worker pool, Compose, helpers, materialization
    internal/core        Pure capability domain values, Blueprint model, and
                         transition decisions; no transport or infra imports
    internal/adapters    The adapter registry — one package per kind
                         (postgres:16, valkey:9, manual): provision ops,
                         connection URL, facts prefix, backup/restore
                         strategy, connector kinds (s3-compatible → S3,
                         MinIO, B2 later). Each adapter is a CONTRACT
                         (typed ops + templates), not core logic; adding
                         a kind = adding a package + one registry entry.
    component-sdk/       Separate public Go module: pure typed Component
                         Capability descriptors, immutable snapshots, grants,
                         planners, and intents; no Groundplane internals or I/O
    registered-components/
                         Separate public Go module: the closed build-time Caddy,
                         Cloudflare Tunnel, and CoreDNS integrations; imports
                         only component-sdk and approved standard-library leaves
    internal/infra       Concrete technology adapters. Shared etcd mechanics
                         stay in infra/etcd; aggregate persistence moves to
                         infra/etcd/<capability> as it is refactored. The CLI
                         never imports infrastructure.
    internal/common      Namespace of narrowly named cross-binary leaf
                         packages: logging, config, ids, execution-plan
                         validation, and other genuinely shared behavior. It is
                         not one importable common project or a grab bag.
    pkg/api              Public API types + stable error codes (the RFC
                         7807 `code` values); the OpenAPI document is
                         generated from the controller handlers
                         (code-first) — see "API contracts"
    proto/               The agent channel contract — .proto is the single
                         source of truth; a contract change is a build error

The dependency rule is one-way and strict:

    named common leaves ← consumers that genuinely share their behavior
    infra   ←  only the daemons (controller, agent); never the CLI
    core    ←  pure domain — no infra, no common platform deps
    adapters ← core + named pure leaves; never infra
    registered-components ← component-sdk + approved stdlib only
    component-sdk ← approved stdlib only

`internal/common` never imports `internal/infra`, and `internal/infra`
may import `internal/common` — never the other way. Nothing below `cmd`
imports upward.

## Shared leaf packages

`internal/common` is a directory namespace, not a package every binary imports.
Each child has one meaningful name and one cohesive behavior. The Controller,
Agent, and CLI share those leaf implementations without creating a generic
home for unrelated code.

### Unified logging (locked)

- **standard `log/slog`** — no third-party logging framework, no per-binary
  loggers. One `SetupLogging` call at each entry point, once.
- **Level priority**: `--debug` > `--verbose` > `GROUNDPLANE_LOG_LEVEL`
  env > config-file `log.level` > default **WARN**.
- **Two destinations, both always configured — file AND stdout/stderr**:
  - a **stderr text handler** at the configured level (human-readable;
    respects `--no-color` on the CLI);
  - a **rotating file handler, always at DEBUG** (10 MB × 3 backups)
    for the Controller and its managed Agent container —
    `/var/log/groundplane/` —
    so a production incident never needs a rerun with `--debug`.
- **Both destinations are configurable**, not hard-coded: `log.level`,
  `log.file.path`, `log.file.enabled`, and `log.console.enabled` live in
    the Controller startup config and Controller-owned Agent runtime config,
    so an operator can point logs anywhere, turn the console handler off for
    a quiet boot, or disable the file handler entirely. The CLI stays
    console-only (nothing to persist).
- **Structured attributes** (key=value), never log secret values —
  consistent with the locked reveal rule: values are never cached, never
  logged. Task progress goes to the etcd journal; resource log *output* uses
  bounded, short-lived Controller buffers and is never persisted. Service and
  Environment streams are non-resumable and use ADR 0059's fixed serving-
  release source selection. A private `LogReady` handshake gates SSE headers
  after initial Docker setup. Workload output is forwarded unchanged except
  UTF-8 normalization and 32 KiB truncation; MVP has no secret-aware redaction.

### Configuration (locked: one reader for Controller + managed Agent)

- **One config reader in `internal/common`** — YAML in, validated struct
  out, defaults first, strict key checking (unknown keys are rejected,
  same as the Blueprint schema rule). Nothing in `cmd/`, `internal/agent`,
  or `internal/controller` parses config itself.
- **Three config inputs, one loader**:
  - `/etc/groundplane/controller.yaml` — etcd endpoint, controller key
    path, listeners, and runtime bootstrap policy; it is exposed as one exact
    revision-fenced document through API/CLI/Console, validated by the startup
    parser and atomically replaced without claiming hot reload;
    path, listen addresses, scheduler cadence, log config, and the immutable
    machine `environment_pool`, `system_pool`, and `runner.network_pool` IPv4
    CIDRs. MVP recovery restores per-source data to an original surviving
    target; it does not promise a startup-config export or empty-host bundle.
  - the Agent runtime document — channel location, Agent id, pull interval,
    max concurrent tasks, labels, and log config. It is Controller-owned in
    etcd and injected only into the Controller-managed container. There is no
    `/etc/groundplane/agent.yaml` bootstrap file; `agent config set` writes to
    etcd. ADR 0011 defines the accepted encrypted channel-token and runtime-file
    mechanics.
  - `~/.config/groundplane/config.yaml` — the CLI's global config (the
    `-c/--config` default from api-cli.md): controller address, default
    scope flags, output preferences.
- **Merge order everywhere**: built-in defaults < config file < env vars
  < flags (the CLI's locked scope resolution applies to config the same
  way).
- **Validation at load**: malformed config fails fast with the exact
  field, never a half-started daemon.

### Local Agent lifecycle (locked topology)

`groundplane-controller.service` is the only Groundplane runtime supervisor.
The native Controller creates, starts, stops, replaces, rolls back, and removes
the local Agent OCI container through Docker. There is no
`groundplane-agent.service` and no bootstrap Compose file. The Agent may use
the Docker socket to execute validated workload procedures, but it never uses
Docker to mutate its own container. Agent enrollment is the operator-facing
`POST /agents` task; credentials remain internal to the Controller-managed
runtime boundary. The container runs as root with host networking and restart
policy `no`. It mounts only the Docker socket read-write,
`/var/lib/groundplane/agent` read-write at the same path, the read-only
`/run/groundplane/controller` channel directory, the read-only runtime config,
and the read-only token file; there is no host-root, generic host-runtime, or
workload-materialization mount.
The image is produced only by `Dockerfile.agent` from the immutable Go 1.26.6,
Docker CLI 29.1.3, and Compose 2.40.3 inputs accepted in ADR 0043. Its explicit
`/usr/local/bin/groundplane-agent` entrypoint and empty command serve both the
persistent process and the binary's closed helper modes. The Docker CLI base's
bundled Compose 5 plugin is replaced by the accepted Compose v2 binary; there
is no host-CLI mount or second helper image.
The channel is the UDS `/run/groundplane/controller/agent.sock`; ADR 0011
defines its accepted token contract. Enrollment requires authenticated
`Ready` within 120 seconds. Missing `Ready` makes the Agent stale after
`max(3 * pull_interval, 30s)`. mTLS, CAs, and certificates are post-MVP.
ADR 0016 is the accepted topology decision.

Agent removal stops assignments, aborts active tasks with reason
`agent_removed`, revokes the token, waits for the Agent to be offline, and then
removes its container and records within a 120-second Controller Task deadline.
Runner removal stops and deletes the local
runner container and Groundplane record only; GitHub deregistration remains a
manual GitHub operation.

### Machine IPAM and Runner isolation

The startup `environment_pool`, `system_pool`, and `runner.network_pool` are
pairwise-disjoint local bootstrap inputs, not etcd resources or human API
capabilities. `runner.network_pool` is a `/24`, `/25`, or `/26`; it is not
reserved from `system_pool`. Environment
creation transactionally reserves a globally exclusive child `network_pool`;
Zone creation reserves a child subnet and materializes only that child as
`gp_net_<stable-zone-id>`. Parent pools are never Docker networks. Environment
pool replacement is metadata-only and must contain every existing Zone. Zone
network settings are immutable because bridge IPAM changes require replacement.

A Tenant owns no runtime network. Tenant- and Project-scoped GitHub Runners
share one atomic five-record quota per Tenant, but each Runner receives a
separate `/29` from `runner.network_pool`. The native Controller supervises one
rootless Docker daemon per Runner under a distinct unprivileged host UID,
subordinate UID/GID range, runtime directory, data root, and Unix socket. The
Runner OCI container mounts only that socket and runs with the same numeric UID;
it never receives `/var/run/docker.sock`, host networking, privileged mode, or
`CAP_NET_ADMIN`. Docker builds, container actions, and service containers are
therefore children of the Runner's rootless daemon rather than the host daemon.
Controller-owned UID/cgroup egress rules deny Environment, backing, platform,
and other Runner pools except the authenticated Controller endpoint while
retaining DNS and outbound internet. Registration and removal are replayable
Controller Tasks that own the daemon, Runner container, network, and local
state as one lifecycle boundary. ADR 0057 remains the Controller lifecycle
authority; the pool correction does not move that ownership.

## Module, executor, and type architecture (locked: ADR 0056)

Groundplane is a layered modular monolith. Behavior is owned by capability
inside each layer rather than accumulated in the layer root:

```text
internal/app                       composition and process lifecycle only
internal/core/<capability>         pure domain values and decisions
internal/controller/<capability>   Controller use cases and orchestration
internal/controller/handlers       typed human-API adapters
internal/agent/<capability>        Agent execution behavior
internal/infra/etcd                etcd mechanics only
internal/infra/etcd/<capability>   aggregate persistence adapters
console/src/features/<capability>  feature state, actions, and presentation
```

There is one durable Task model and journal with exactly two immutable executor
authorities. Agent Tasks run through the bounded Agent worker pool and own
workload and Platform data-plane effects. Controller Tasks run through one
serial recoverable native queue and own only the closed bootstrap/isolation and
persistence-only procedures accepted by the product contracts. Handlers and
the scheduler publish Tasks; they never perform asynchronous side effects
directly.

Concrete types are the default. An interface exists only at a real behavior or
side-effect seam, is declared by its consumer, exposes only the methods that
consumer calls, and is never used to recover implementation behavior through a
type assertion. Implementing constructors return concrete types.

HTTP, CLI, protobuf, etcd, YAML, Docker, systemd, filesystem, registry, and
object-store values are parsed once at their owning boundary. Mapping is
explicit. Reflection, `unsafe`, JSON round trips, untyped maps, double casts,
and interface-to-concrete recovery are not conversion mechanisms.

The Console follows the same ownership: the root store holds cross-feature
workspace composition only; feature request state and actions live with the
feature. Routes remain shallow, and generated OpenAPI types are the sole human
wire contract.

### Errors

- One taxonomy in `pkg/errs`: one typed error + stable dot-namespaced `code`
  (the RFC 7807 values the API returns: `service.not_found`,
  `deploy.in_flight`, `strategy.not_implemented`, …).
- **One shared error handler** (the mvmctl `HandleErrors` pattern): the
  CLI routes every error through it — non-fatal exits (BrokenPipe) return
  nil, everything else exits 1. The Controller maps the taxonomy to RFC
  7807 problem+json. Nothing else in the codebase formats errors.

### Output & ids

- `--output TABLE|JSON|YAML` (go-pretty for TABLE; table is the default;
  JSON/YAML for scripts) — shared, so the CLI is the only place output
  is decided.
- ULID ids (`<kind>_<ulid>`) live in `internal/common` — one
  implementation, used by Controller-generated ids everywhere (the lock:
  mock fixtures included).

## CLI (locked: Cobra)

- **Cobra + pflag** (`github.com/spf13/cobra`).
- Root command owns the global flags (`-c/--config`, `--host`,
  `-t/-p/-e`, `-o/--output`, `--no-color`, `--id`, `-h`); every noun is
  one command file; the tree matches `api-cli.md` exactly (flat nouns,
  verbs last, scope resolved once).
- `completion bash | zsh | fish` is Cobra's built-in completion — the
  locked completion noun falls out for free.
- `context.Context` threaded from `main` (signal handling:
  SIGINT/SIGTERM) through every command; commands carry zero logic and
  call the human API.

## API contracts (locked)

Two machine contracts, two tools — never one for both:

1. **Agent channel — gRPC + protobuf** (`proto/`). The `.proto` is the
   single source of truth for the Controller↔Agent machine surface; a
   contract change is a build error. Unchanged, by lock.
2. **Human API — REST/JSON, one OpenAPI document.** The Console, CLI,
   and scripts all speak it. The OpenAPI document is the single source
   of truth for the REST surface, generated **code-first**: handlers are
   typed Go structs (Huma or oapi-codegen), the spec is emitted from
   them at build time and served at `/openapi.json` — never
   hand-maintained, so it cannot rot. From that one spec:
   - TypeScript client types for the Console (openapi-typescript);
   - a Go client for the CLI;
   - the RFC 7807 error codes, SSE streams (logs, task events,
     activity), and cursor pagination all expressed in the document.

REST/OpenAPI is also the sole external machine integration boundary for
Components. The CLI is human-facing and is never an integration protocol.
Build-time registered integrations use the pure Component SDK rather than
recursively calling the local HTTP server; a future external integration would
require an explicit scoped API/token contract and is not an MVP feature.

This is how the 1:1 rule becomes mechanical: every operator-facing Controller
capability's Console action ↔ CLI command ↔ API endpoint derives from the same
document — drift is a build error, not a review finding. Local process/tooling
commands are the closed exceptions defined in
`api-cli.md`. The Console's store surface
(`lib/store.tsx` + `types.ts`) is the seed of the spec: the Controller's
handlers are generated to match it, and the Console's types come back
from the spec.

## Console (frontend) — locked: React + Vite, static SPA, embedded

- **React 19 + Vite + shadcn/ui (Base UI) + Tailwind v4** — the SAME
  stack used by the original design, so the contract UI migrated directly. The
  Console components, styling, and store pattern
  are not.
- **No meta-framework, no Next.js**: SSR/SSG/RSC exist for
  internet-facing apps; a localhost console served from the Go binary
  needs none of it. Vite emits a flat `dist/` embedded via `go:embed`
  into the Controller — one binary, no Node anywhere in prod.
- **One reproducible production graph**: Node `24.19.0`, npm `11.17.0`,
  `npm ci`, Vite build and fingerprint verification, then
  `go build -tags groundplane_console`. The ignored `console/dist/` tree is
  embedded directly from the build-tagged `console` package and is never copied
  or committed. Untagged Controllers are development-only and return RFC 7807
  `503` responses for otherwise valid Console requests.
- **One outer HTTP dispatcher** validates the original canonical request path
  before ServeMux, reserves `/api`, `/api/`, and operational routes for the
  Controller, and serves only GET/HEAD regular files from the injected `fs.FS`.
  SPA fallback requires positive exact `text/html` negotiation; cache and MIME
  behavior are closed by ADR 0017.
- **The store pattern stays** (the lock: the store is the API contract).
  In prod the store calls the generated OpenAPI client instead of
  fixtures, and SSE (EventSource) feeds live task states — the same
  journal shape the Console already renders. No cache layer
  (TanStack Query and friends) for a single-operator localhost tool:
  machinery without payoff.
- **Frontends carry zero logic (locked)** — the Console renders
  Controller responses and sends Controller-validated requests; the
  bundle is static files, the API is the only logic.

## Unified paths that scale

The "one implementation" rule applied to the load-bearing paths, so each
is a single well-tested loop rather than N special cases:

- **One Task journal, two closed executors** — Agent work follows Controller
  dispatch → Agent pull → bounded worker pool → acknowledgement; native work
  follows the separate serial Controller queue → acknowledgement. Both use the
  same Task, event, retry, abort, and terminal model.
- **One renderer** — desired state → compose / Corefile / Caddyfile /
  env files from a single schema-validated render path.
- **One scheduler tick** — every backup schedule evaluated in one daemon
  tick, per the locked contract.
- **One materializer** — env files at 0600 and the resolv.conf rewrite
  are Agent-side materializations of the same pattern.
- **One error path** — taxonomy → RFC 7807 (API) / single handler (CLI).

## Compose superset and execution boundary

The Blueprint deliberately builds on Docker Compose instead of cloning its
grammar. The Controller parses the Groundplane envelope, passes the Compose
body through the Compose parser, and validates the `x-gp-*` extension
namespace against the Groundplane schema. Standard Compose fields retain
their standard meaning; Groundplane only adds behavior Compose cannot express.

Environment apply accepts the closed bundle from ADR 0012 through one
multipart manifest plus deterministic file parts. Repeatable CLI
`--compose-file` flags retain layer order after the always-first root body;
repeatable `--var` flags provide explicit non-secret interpolation. The Console
mirrors those controls with a reorderable source list and key/value table. The
manifest maps normalized paths to `file-000001`, `file-000002`, and so on in
path order. The Controller supplies
`compose-go/v2` only the bundle namespace and explicit non-secret interpolation
map. It never supplies process environment, implicit `.env`, host paths,
symlinks, or network fallbacks. The immutable normalized revision is the
durable reconciliation source. The same revision retains the canonical
submitted bundle stream for audit and reparse. Canonical Compose and
materializations stay in the ephemeral render-plan category below.

ADR 0051 defines one clean-start desired-state authority. The Controller first
validates the complete bundle and derives a lossless normalized projection.
The encoded projection, including its schema and framing, must fit exactly
2 MiB. Rejection happens before durable staging or Task publication.

Validated audit and projection streams are staged privately in immutable
60-KiB chunks. All revision records use `GDR1`: a strict 44-byte envelope with
magic, kind, codec version, flags, payload length, and raw SHA-256 payload
digest. A chunk payload adds its family, zero-based sequence, logical offset,
logical length, raw chunk digest, data length, and bytes. Dynamic key segments
use `~` plus unpadded base64url of the raw segment bytes.

The audit stream is at most 1,049,100 bytes and 18 chunks. The projection is at
most 2 MiB and 35 chunks, so one revision has at most 53 chunks. A staging
transaction writes at most 12 chunks, uses at most 26 operations, and measures
at most 797,580 encoded bytes. Sealing reads every chunk at one fixed MVCC
revision and creates a compact integrity root. The seal uses at most 57
operations and 132,800 encoded bytes.

Final Environment Blueprint publication compares the sealed root, descriptor,
current Environment head, dependency evidence, hierarchy, operation, Task, and
idempotency fences through ADR 0051's dedicated executor. Its comparison,
success, and failure arms each allow at most the configured etcd maximum of 256
operations, and its actual protobuf-encoded request allows at most 1 MiB. The
derived 512 selected-arm and 768 full logical counts are diagnostics only, not
rejection limits. Ordinary non-Blueprint `Store.Transact` retains its
96-selected-operation protection. Staging, sealing, and other
non-final-publication limits are unchanged. Publication changes the Environment
desired head, creates the Task and replay records, removes the private locator,
and marks the descriptor published. It never copies file or projection chunks.
There is no singleton current projection or flat desired-resource mutation path.

Every desired Add, Edit, Remove, and Blueprint Apply derives and publishes a
complete new revision through this seam. Tasks pin the final desired schema,
Environment id, revision id, sealed-root digest, projection digest, and render
generation. A worker reads only its pinned revision, while current-head claim
fences prevent stale execution. Private staging expires after five minutes
without progress, becomes abandoned only when no public marker exists, and is
deleted in bounded batches of at most 32 records and 143,616 encoded bytes.
Published chunks belong to revision retention, not staging cleanup. Startup
accepts one final `desired_schema` marker and fails closed on legacy or mixed
desired authority. The MVP has no compatibility reader or in-place migration.

ADR 0051 makes an immutable normalized revision the durable reconciliation
source. The same revision retains the canonical submitted bundle stream for
audit and reparse. Canonical Compose and materializations remain ephemeral.

The Controller validates the complete bundle and derives a lossless normalized
projection before staging. The encoded projection, including its schema and
framing, must fit exactly 2 MiB. Rejection happens before durable staging or
Task publication.

Validated audit and projection streams are staged privately in immutable
60-KiB chunks. All revision records use `GDR1`: a strict 44-byte envelope with
magic, kind, codec version, flags, payload length, and raw SHA-256 payload
digest. A chunk payload adds its family, zero-based sequence, logical offset,
logical length, raw chunk digest, data length, and bytes. Dynamic key segments
use `~` plus unpadded base64url of the raw segment bytes.

The audit stream is at most 1,049,100 bytes and 18 chunks. The projection is at
most 2 MiB and 35 chunks, so one revision has at most 53 chunks. A staging
transaction writes at most 12 chunks, uses at most 26 operations, and measures
at most 797,580 encoded bytes. Sealing reads every chunk at one fixed MVCC
revision and creates a compact integrity root. The seal uses at most 57
operations and 132,800 encoded bytes.

Final Environment Blueprint publication compares the sealed root, descriptor,
current Environment head, dependency evidence, hierarchy, operation, Task, and
idempotency fences through ADR 0051's dedicated executor. Its comparison,
success, and failure arms each allow at most the configured etcd maximum of 256
operations, and its actual protobuf-encoded request allows at most 1 MiB. The
derived 512 selected-arm and 768 full logical counts are diagnostics only, not
rejection limits. Ordinary non-Blueprint `Store.Transact` retains its
96-selected-operation protection. Staging, sealing, and other
non-final-publication limits are unchanged. Publication changes the Environment
desired head, creates the Task and replay records, removes the private locator,
and marks the descriptor published. It never copies file or projection chunks.
There is no singleton current projection or flat desired-resource mutation path.

Every desired Add, Edit, Remove, and Blueprint Apply derives and publishes a
complete new revision through this seam. Tasks pin the final desired schema,
Environment id, revision id, sealed-root digest, projection digest, and render
generation. A worker reads only its pinned revision, while current-head claim
fences prevent stale execution. Private staging expires after five minutes
without progress, becomes abandoned only when no public marker exists, and is
deleted in bounded batches of at most 32 records and 143,616 encoded bytes.
Published chunks belong to revision retention, not staging cleanup. Startup
accepts one final `desired_schema` marker and fails closed on legacy or mixed
desired authority. The MVP has no compatibility reader or in-place migration.

The initial extension families include:

- `x-gp-resource` for stable ids and ownership;
- `x-gp-slug` for the mutable public slug of a Volume while its Compose key remains
  immutable;
- `x-gp-release` for authored release defaults; runtime image and slot authority
  comes only from immutable Release records and serving projections;
- `x-gp-release-groups` for a map of explicit coordinated multi-service
  releases declared only at the document root, keyed by canonical group
  name, whose services must exist in the resolved enabled Compose project;
  omitted `order` normalizes to member order, explicit order is an exact
  member permutation, and group-level
  `on_failure: switch_back | leave_active` defaults to `switch_back`;
- `x-gp-attach` and `x-gp-fact` for backing resources and computed values;
- `x-gp-entry` and `x-gp-exposure` for environment/file sources, explicit
  numeric file ownership, and visibility;
- `x-gp-depends_on` for service-local dependency conditions;
- `x-gp-requires` for Controller-level dependencies across Compose projects;
- `x-gp-route` for ingress and DNS intent;
- `x-gp-components` for environment-owned component instances, generated
  services, and derived Router/CoreDNS projections;
- `x-gp-backup` for backup policy and selected sources;
- `x-gp-scripts` for Environment-scoped Script desired state and typed
  lifecycle hooks.

Compose ignores `x-*` fields. That is useful because a generated Compose
document remains valid to Docker Compose, but it also means extensions never
become executable by accident. The Controller must compile each supported
extension into one of:

- a standard Compose field;
- a generated materialized file;
- a Controller dependency edge;
- a typed Agent step;
- Docker labels used for ownership and drift detection.

There are two related but different contracts:

1. **Controller grammar.** The full Blueprint and all `x-gp-*` semantics are
   interpreted only by the Controller. This is where ids, facts, secrets,
   release records, adapters, and cross-project dependencies are resolved.
2. **Agent execution contract.** The Agent understands the generated Compose
   document, task step catalog, ownership labels, render generation, and
   execution metadata. It does not re-interpret the full Blueprint or make
   decisions the Controller did not provide.

The Controller creates one typed `ExecutionPlan` and sends an execution bundle
containing its `plan_id` and `plan_hash`, render generation, one canonical
Compose file per generated project, typed Compose steps, per-resource expected
labels, and dependency ordering. Protobuf carries the plan over the live
channel; generated `x-gp-execution` carries the same plan inside the Compose
bundle for inspection and replay. The Agent verifies the hash, applies the
bundle through Docker Compose and the Runner, and reports
observed state. Both sides therefore understand how an operation is applied,
but semantic ownership stays on the Controller and execution ownership stays
on the Agent.

`x-gp-depends_on` is compiled to native Compose `depends_on` only for
services in the same Compose project. `x-gp-requires` becomes a Controller
task-DAG edge and handles backing-service readiness, attach provisioning,
external network joins, CoreDNS, routers, and other cross-project cases.
Dependency cycles, missing references, and unsupported conditions fail during
validation before a task is dispatched.

## State translation and materialization

The Controller keeps five distinct categories of state:

| Category | Examples | Owner |
| --- | --- | --- |
| desired | one lossless normalized topology and audit stream per immutable revision | revision chunks selected only by the Environment desired head |
| durable records | ids, Volume and Service runtime projections, generated credentials/facts, releases, recovery points | Controller in etcd |
| compiled execution | canonical metadata, plan digest, artifact identities, and reconstructable non-secret inputs | Controller in etcd while replay authority is required |
| transient execution material | secret bytes, rendered secret files, streaming logs, helper frames | bounded memory or constrained runtime files only |
| observed | containers, health, local image ids, networks, files | Agent reports |
| task | lifecycle, step progress, result, timeout, abort | Controller journal |

The canonical environment env file is generated as
`secrets/.env.<environment-id>` and explicitly attached to every service
with Compose `env_file`. Service-specific files append the service name.
The filename and environment volume directory are id-based, so label renames
do not move data or recreate the Compose project. The Compose default `.env`
file is only an interpolation mechanism and is not treated as implicit
container injection.

A managed Volume has a stable `vol_` id, a mutable Environment-scoped slug,
and an immutable authored Compose key. Service mounts and Backup targets store
the id. Rendering uses the key and creates `gp_vol_<volume-id>` backed by
`<environment.volume_dir>/<key>`. The Agent creates the leaf through a private
same-filesystem sibling, an exclusive ownership marker, fsync, and
`renameat2(RENAME_NOREPLACE)`. Removal publishes the desired revision before
runtime destruction, proves consumer detachment, and traverses the leaf
descriptor-relatively in bounded resumable calls. Volume runtime states and
removal locks never become desired authority.

Service `runtime_intent` is Controller-owned operational state outside the
Blueprint. Start, Stop, and Destroy set it to `running`, `stopped`, and
`absent`; Remove deletes the desired service and its intent. Bundle apply never
authors or overwrites the field.

Lifecycle intent and Task publication share one etcd transaction under the
Service, owner/name, hierarchy, projection, deletion, idempotency, and active
Task fences. Applied tenant Services persist an immutable per-attempt render
input containing the exact hierarchy identity, Blueprint revision, applied
projection, render generation, and artifact id. Retry copies that input while
preserving the plan id and plan hash; Task pruning deletes it atomically with
the journal primary. Never-applied Services use a deterministic Controller
no-op Task. The per-Service active index is released in every terminal
acknowledgement transaction. ADR 0042 fixes the timeout and Compose-operation
details.

The renderer delegates runtime primitives to Compose whenever possible:
`deploy.replicas`, service `secrets`/`configs`, service `labels` and
`annotations`, networks, aliases, healthchecks, and same-project
`depends_on`. Groundplane adds durable desired state, cross-project ordering,
release/slot switching, secret-store lifecycle, and reconciliation around
those primitives. Compose labels use the `com.groundplane.*` namespace;
Compose's reserved `com.docker.compose.*` labels and local `deploy.labels`
are not used as the ownership contract.

Each immutable candidate Release owns one `WorkloadSeal` value:
`{requested_reference, local_image_id, replica_count}`. The requested reference
is provenance and tag selection; generated workload Compose and execution use
the exact sealed `sha256:<64-hex>` host-local Docker image id. Optional registry
or manifest metadata is display-only and never workload authority.

For a newly authored requested reference, the Controller makes one ephemeral,
bounded, correlated read-only request over the existing authenticated Agent
channel before publication. The Agent alone reads workload Docker state, uses
`ImageInspect`, and returns one all-or-nothing validated result for at most 64
unique selectors. The request selects either a new requested reference or an
already sealed local image id; it exposes no arbitrary Docker operation and is
not an operator action or Task. The Controller never reads workload Docker
state. Resolution never builds, pulls, or pushes and never requires
`RepoDigests`, so a locally built image remains valid.

ADR 0052 owns the exact machine bounds. In summary, the correlation id is the
32-character lowercase-hex encoding of 16 nonzero random bytes, is fresh within
the authenticated connection, and is discarded with that connection or active
request. Requested-reference selectors are explicitly tagged or
digest-qualified Docker named references of at most 512 ASCII bytes; local-id
selectors are exactly `sha256:` plus 64 lowercase hex characters. Request and
result envelopes are each at most 65,536 bytes, both peers enforce the 1..64
unique-selector and string bounds before Docker inspection, and success returns
one result per selector in order. A closed failure names a present zero-based
ordinal and returns no partial success. Each peer caps the complete exchange at
30 seconds; timeout, disconnect, late response, or malformed active response
provides no usable identity and prevents staging and publication without
automatic re-resolution.

Rollback copies the complete candidate seal from the Controller-selected
historical Release input. Prior topology and compensation copy the complete
seal from the exact currently serving Release input. Retry and recovery reuse
those same seals. Historical, prior, retry, and immediate pre-mutation checks
inspect the sealed local id directly, never the mutable requested reference.
Retagging cannot change selected bytes or invalidate an old image that remains
present; a missing sealed id fails before any workload mutation. No path derives
a missing image id or replica count from current desired state, a projection,
containers, or a mutable tag.

Pinned Groundplane-managed Agent, etcd, Component, and backing images remain
release/installation assets outside operator registry integration. Their image
authority is separate from `WorkloadSeal`. A rollback changes the Release
projection and causes the renderer to regenerate sealed local image ids, slot
aliases, router targets, and labels from exact immutable Release inputs.

Workload Services expose no host ports in the accepted MVP contract. A narrow
loopback-only mapping for an operator-owned recreate Service is merely a
proposed break-glass seam; until separately accepted, it has no schema, API,
renderer, or runtime implementation.

Release execution does not move a mutable Compose network alias. Blue-green
renders two physical slot workloads and one stable Controller-owned Caddy proxy
endpoint and is limited to sealed `replica_count == 1` for both candidate and
prior sets. Native Compose omission of `deploy.replicas` normalizes to the
explicit count one only at the authored-input boundary. Every durable Release
seal contains a positive explicit count; absent or zero durable counts are
invalid and never default to one. Recreate renders the exact sealed count
`N >= 1`; an addressable
recreate may retain the stable proxy, but never retains the slot pair. Internal
consumers and the Environment router target the stable endpoint when present.
The immutable render input contains the candidate `WorkloadSeal` and optional
prior `WorkloadSeal` as separate exact logical sets; it does not duplicate their
members as image, prior-image, replica, or workload-manifest-digest authority.
It also seals the internal `singleton`, `blue`, or `green` slot labels, proxy
JSON and digests, generations, and Release ids. The `singleton` label names the
recreate slot; it does not imply one physical replica.
Blue-green to recreate removes the exact sealed prior workload set before
applying the exact-count candidate set. Recreate to blue-green creates and
health-proves the candidate slot set, atomically switches the proxy, then
removes the obsolete prior workload set. Any transition to or from blue-green
requires both the prior/current serving Release and candidate/destination
Release seals to have `replica_count == 1` before mutation. `switch_back`
restores and proves the exact prior topology
before candidate cleanup. Recovery retries use the same fixed-revision input
and run probes plus enabled compensation only.

The managed stable proxy's compiled OCI index reference, Agent-selected host
platform child manifest, and local Docker image/config id are distinct identities.
Their preparation and historical-retention authority is separate from the
workload seal, and current compiled catalog authority never substitutes for a
sealed historical proxy identity.

A Release Group is one ordered coordinated action over 2 through 32 logical
Services, not a simultaneous or atomic switch. For group deploy, a request tag
overrides the persisted group tag; absence of both is invalid. The selected
deploy tag resolves once against each member's declared image in one
all-or-nothing Agent batch before publication and seals each exact local Docker
image id; registry or manifest metadata is optional and never substitutes for
that id. Each member keeps its own ledger. Group rollback never consults the
persisted default and copies each selected historical member seal rather than
resolving its tag again.
An optional rollback request tag independently filters each member's eligible
history and selects its newest Release that completed successfully and reached
serving with that exact tag and a tag different from its current serving
Release; omission selects each member's newest such eligible different-tag
Release. Any member without an
eligible source rejects the whole group before publication. Group `on_failure`
overrides member defaults. Selected lifecycle hooks execute once per logical
Service in their declared order, never per replica; there is no group-hook
resource. Migrations remain bound once to their designated logical Service.
Each selected Script execution receives one task-scoped runner even when its
target is replicated; no runner is created merely because a logical Service is
a group member.

An initial Blueprint apply implicitly selects both `pre-deploy` and
`post-deploy` Scripts for each newly introduced or materially changed,
effectively running logical Service that is not a Release Group member. Its
candidate requested references resolve once through the Agent before
publication, and every runner consumes the applicable Release's sealed
host-local Docker image id; there is no deferred Compose-result image authority.
Exact reapply and an apply with no selected changed candidate run no hooks. The
single assignment materializes Entries and files; ensures Volumes; performs
required Attach adapter provisioning and grants plus Network resource
preparation without applying consumer Compose memberships; and runs all pre
hooks in sealed dependency-topology/current-Service-slug/current-Script-slug
order with cleanup after each. Only after that barrier does the candidate
workload phase apply each targeted consumer's prepared Network membership and
apply/start the candidate. No hidden consumer Compose start or recreate may
precede the barrier. It next runs post hooks with the same cleanup barrier,
waits for health, runs Component actions, and reaches Controller-only atomic
promotion. This Blueprint-only ordering does not change standalone Attach or
Detach execution. The existing total cap of 16 executions and
1 MiB of body bytes spans pre, post, and possible `on-failure` execution rather
than resetting by phase; the fixed 900 seconds applies to each execution. All
bounds are validated before publication.

A pre-hook failure leaves serving and current-successful projections unchanged
and cleans its runner before `on-failure`; earlier materialization, Volume,
Attach, or Network changes remain accounted effects. Retry never crosses a
Script `start_authorized` or unknown-state barrier and never automatically
starts a replacement. TLS-first setup is an ordinary project-authored
pre-deploy Script using `/bin/sh` and externally supplied `openssl` from its
selected workload image, a sealed numeric Service user, a declared read-write
Volume, and read-only consumer mounts. Entries cannot own its output subtree;
the author owns idempotent stage, validation, ownership/mode, and atomic publish
logic. No generic PKI capability, tool guarantee, arbitrary-output atomicity,
or live automatic certificate rotation is introduced.

Recreate deploy, rollback, restart, and exact reapply preserve the authored
replica count. The addressable renderer and health model support exact `N`, but
lifting remaining singleton execution guards and proving DNS, WebSocket/Valkey
fan-out, and replicated Script targeting are pending runtime alignment.

The applied Blueprint projection embeds one owned
`core.ServiceDependencyPlans` value for deploy and rollback. Release render
inputs copy that exact value and must equal the embedded projection. Group
publication rejects a declared order that violates an edge between selected
members; dispatch never reorders members. An external prerequisite remains a
native phase-specific Compose `depends_on`, and each member apply depends on
the prior declared member's successful proxy-switch step.

Attach facts are computed from durable attach records. A fact mapping can
render into a service environment value, the all-services env file, a
service-specific env file, or a generated file. Facts are not automatically
injected by the existence of an attachment. Secret values are resolved only
through an explicit single-fact reveal or into a secret Entry destination;
Attach list projections expose only fact keys and secret classification.
by the Controller and materialized by the Agent at mode `0600`.

Attach execution never derives an Environment render from live topology. The
create/detach transaction stores one immutable, non-secret render-input record
under the plan id: pinned hierarchy labels and ids, Blueprint revision,
render generation, owned Compose identities, backing adapter, artifact id, and
the complete external-network membership union. Provision and grant steps run
before the targeted consumer Compose apply. Detach runs revokes, the targeted
Compose apply without the removed membership, then adapter deprovisioning.
The `manual` adapter produces only the targeted Compose apply. Retries retain
the plan id and therefore reconstruct byte-identical plans.
Each Attach also retains its resolved stable backing Network id independently
of the Task snapshot. That long-lived binding is the source for computing a
later Environment network union after historical Task retention has elapsed.
Union construction includes every provision-intent Attach, including failed
provisions, excludes detach-intent Attaches, and deduplicates Network/Service
edges before storing the sorted immutable render input.
Successful detach acknowledgement removes the Attach primary, encrypted facts,
all membership indexes, and outgoing grant edges in the same transaction that
terminalizes the Task. Failed detach acknowledgement retains the durable
failed record and sealed plan for retry; there is no second success deletion.
Attach create and initial detach publication both advance the consumer
Environment primary revision without changing its value. This revision is the
serialization fence around list-derived network-union snapshots, preventing
two pending mutations from committing stale unions before the Agent writer
fence serializes execution.

Terminal Tasks publish a 90-day retain-until index in the same transaction as
their terminal record. Daily maintenance deletes expired idempotency evidence
first, then starts crash-safe Task pruning only after the Task marker is absent
and no retry is active. A resource-specific hook may make the sole narrow
exception when it proves exact atomic ownership transfer to the active retry:
the shared records and active-operation marker identify the same operation and
exact new Task owner, the expired candidate owns no retry-shared resource, and
all shared authority remains operation- and new-owner-protected. Environment
deletion is the accepted implementation, using its tombstone, operation lock,
cleanup intent, and active marker as that proof; the old attempt may then prune
without starving the global retention index.

Every terminal Agent-executor Task publishes the sole generic durable replay
receipt at `/v1/runtime/task-terminal-receipts/{task_id}` in the same etcd
transaction and exact modification revision as its terminal Task. The receipt
contains only schema 1, Task id, Agent id and durable generation, assignment id
and generation, 32-byte plan hash, terminal result, and the 32-byte domain
digest of the exact canonical terminal Task primary. It contains no TaskAck
digest or Task revision. Native Controller Tasks and pending aborts without a
terminal Agent assignment have no receipt.
Backup has no second receipt key, record, reader, or pruning path. Delivery
application separately owns Ack evidence: Agent Ack terminalization publishes
Task plus receipt atomically and then pending delivery with the Ack digest;
Controller timeout/abort publishes Task plus receipt atomically and then
`awaiting_ack` without it. A late exact Ack changes only delivery state.
`awaiting_ack` uses TaskAbort/retirement only; receipt handshakes use pending
and applied only. Unknown terminal outcome requires a fixed-revision Task and
receipt at the same ModRevision with exact digests. Compacted MVCC history,
locks, and later domain state are never replay authority.

Accepted ADRs 0047 and 0048 define the sole Backup artifact and Agent protocol
contract: schema 1 only; canonical `environment-config-v1`, `volume-tar-v1`,
and `postgres-custom-v1`; immutable four-part source/stored evidence; exact
upload, Head, and point-commit checkpoints; two-pass canonical-primary Config
publication; and acknowledged stage inventory/disposition before true Ready.
There is no old protobuf decoder, mixed binary mode, staging salvage, or
compatibility contract. These accepted decisions are implementation authority,
not evidence that the checked-in schema or runtime already implements them.

Production MVP also requires a safe Valkey data source and verified restore.
Until its bounded source contract lands, runtime rejection with
`strategy.not_implemented` is correct: a whole shared-instance RDB is not a
per-Attach artifact, and archiving a live Valkey data directory is forbidden.
MVP recovery is per source to its original surviving target, without
cross-source atomicity or an empty-host restore/export/import promise. Future
full-host DR must inventory metadata, keys, startup configuration, exact
images, data backups, and a recovery bootstrap before reconciliation.

The collector starts each expired Task with a private phase-and-cursor intent
transaction. Under the task and relevant revision/absence fences, that
transaction removes the retain-until index, immutable operation-history index,
owner indexes, service lifecycle render input, and any present
Component/Route/Entry removal intents, while leaving the Task primary as the
checkpoint ownership anchor. Every intent carries completion booleans for the
checkpoint cursor, checkpoint dedupe, and Task-primary phases; only event and
event-dedupe records carry remaining counts. Every Task traverses the cursor
and dedupe checkpoint phases: Backup records drain in bounded
compare-and-swap batches, while non-Backup prefixes advance the same booleans
empty. A nonempty checkpoint batch fences the private intent and each record's
ModRevision; an empty-prefix transition fences prefix absence. After both
checkpoint booleans are true, a fenced transition deletes the Task primary
only when both checkpoint prefixes remain absent. Event and dedupe records
then drain through 47-record batches within the 96-operation ceiling, with
prefix-empty verification when a count is zero. A Controller failure resumes
from the persisted phase and cursor rather than restarting or skipping a
phase.
For any Agent-executor Task, pruning start validates its generic terminal
receipt at the Task's exact revision, compares it, and records that revision in
the private intent. The receipt survives subordinate cleanup, Task-primary
deletion, and event/dedupe draining. Only the final transaction, after every
phase and empty-prefix proof completes, compares and deletes the receipt with
the pruning intent; transaction failure leaves both available for resumption.
Attempt-scoped Component intents expire with their Task. Plan-scoped Attach
render inputs expire only with the final retained retry attempt. Backup retry
is currently fail-closed while retained checkpoint state exists; this is an
implementation gap against ADR 0024's accepted checkpointed-retry behavior,
not a pending retention decision. See ADR 0035.

The renderer is pure with respect to its inputs: desired state plus durable
records produce the same render plan. Applying the plan is not pure and is
performed only through a task. A failed render never reaches the Agent; a
failed Agent step records the observed partial state and makes retry/reconcile
explicit.

## Extensibility: adapters and components

Groundplane has two extension seams: backing-service adapters and registered
Components. The Component seam is governed by ADR 0061. Groundplane owns
generic typed Component Capabilities; a registered integration declares what
it provides and the exact capabilities it may consume. Caddy, Cloudflare
Tunnel, and CoreDNS are the closed MVP catalog. There is no runtime or custom
plugin loading.

Registered integration code lives in a separate Go module and can import only
the public Component SDK and an approved standard-library allowlist. It receives
bounded immutable planning views and returns typed immutable intents. It never
receives Groundplane aggregates, repositories, etcd, Docker, filesystem,
protobuf, executors, secret values, or arbitrary command authority. The root
composition package is the only importer of concrete registrations.

REST/OpenAPI is the external machine boundary. The SDK is its pure in-process
planning counterpart, not a second backend. Groundplane validates and applies
every intent through the same capability use case used by the human API and
publishes all side effects as durable Tasks.

The public backing-service resource is a facade over the existing
`project(kind=backing) -> environment(main) -> service(adapter)` hierarchy. It
uses the backing project id as its public identity and exposes the three ids
needed for operations. This avoids duplicating domain state while keeping the
operator/API model aligned with the product noun.

The interfaces stay narrow and uniform anyway — that is the insurance
policy: if third-party loading ever becomes a goal, the existing seam is
the wrap point. Nothing is designed or built for it now.

### What is extensible (the seams)

| Seam | How to add one | Cost |
| --- | --- | --- |
| **Backing-service adapters** (postgres:16, valkey:9, manual) | package in `internal/adapters` + one registration line | small — the design's cheapest seam |
| **Connector kinds** (backup destinations) | `s3-compatible` today; new kind = package + registry entry | small |
| **Registered Components** (Caddy, Cloudflare Tunnel, CoreDNS in the MVP) | typed implementation in `registered-components` using only existing Component Capabilities, plus one composition registration | medium — source-reviewed and rebuilt; no runtime loading or core implementation branch |

### What is NEVER extensible

Schema validation, task sequencing, the serialization locks, the render
*execution* path (validate-before-reload, the shared renderer engine),
reconciliation, encryption (age, the controller key), labels, and the
task pipeline itself. Extensions widen the catalog of what the pipeline
can *do*; they never reshape how the pipeline *runs*.

### The adapter seam (the contract shape)

An adapter is **declarative, typed knowledge** — a fixed shape to fill
in, in one package:

- metadata: key (`postgres:16`), label, image, facts prefix (`pg16_`),
  URL scheme (`pgsql://` vs `redis://`), connection port (`5432`),
  `requires {database, role}`,
  exposed fact fields, `manual?`, and `supports_grants?`;
- provision/detach/rotate ops as typed parameterized steps
  (`create_database`, `create_role`, `grant`, …) with placeholders
  (`<db>`, `<role>`, `<generated>`) filled by the Controller;
- the facts template;
- the backup/restore strategy (dump → encrypt → upload → verify → prune).

Adding a kind = implement the shape + register it. The Console already
models the shape exactly: `lib/types.ts` `Adapter` (key, image, prefix,
urlScheme, requires, envVars, provision, backup/restore strategy,
manual).

The durable Service record, rather than an Attach request or Docker discovery,
owns the selected stable `backing_network_id` for every adapter-backed Service.
Ordinary tenant Services have no such binding. Each Attach copies and pins that
id, and publication plus plan reconstruction reject any mismatch.

Consequences:

- **Core never switches on kind.** The task pipeline runs whatever steps
  the registry's contract describes. Postgres and Valkey are the same
  pipeline with different step tables.
- **The registry is the only entry point.** One map, keyed by adapter
  key, in `internal/adapters`. Adding a kind is a package plus one
  registration line; the Console, CLI, and API learn the kind from the
  registry (labels, requires, steps) — no switches anywhere.
- **Execution on the Agent, through the image the adapter resolves.** The
  `postgres:16` adapter resolves only the digest-pinned first-party managed
  PostgreSQL 16 OCI index fixed by ADR 0047. Its `pg_dump`, `pg_restore`, and
  `psql` procedures run through the release-pinned helper and private gate in
  that existing workload container; mutable `postgres:16-alpine`, caller image
  input, and generic exec are not authority. When another backing
  image lacks the tooling (e.g. a Kafka adapter with no dump tool), the
  adapter names a second image — a sidecar the Agent runs with a typed
  operation request and gets a typed result.

**The guardrail**: the Agent executes only the **known step catalog** —
  compose_up, compose_down, wait_healthy, switch_alias, switch_route,
  write_file, reload, join_network, provision_network, run_script, exec, sql,
  dump, restore, encrypt, upload, verify, prune, ack. A step
kind that doesn't exist (e.g. `snapshot-filesystem`) is an *additive core
extension*: it must be added to the catalog and the contract shape,
never worked around by an adapter-side bypass. This keeps the locked "no
  arbitrary command execution" rule true for adapters and components:
  the adapter composes, the pipeline executes, and `run_script` is the
  explicit operator-authored automation exception with its own task boundary.

**Not hooked**: task sequencing, serialization locks, schema validation
internals, rendering, reconciliation, encryption, labels, the task
pipeline.

### The Component Capability seam

A registered Component is pure translation between one typed implementation
configuration and Groundplane-owned capabilities. Registration declares its
provided capabilities, grants, allowed owner scopes, definition digest, typed
planner, and generic managed-configuration requirements.

Implementation configuration may include an operator-authored managed-file
template as a durable decision. CoreDNS carries the required full Corefile
template through the generic Component config vertical; its SDK-only registered
renderer validates the template and substitutes the single `{groundplane}`
marker with Controller-owned directives. This does not add an
implementation-named persistence route or Agent procedure.

The existing generic Component read service owns a consumer-defined
managed-config projector port. `GET /components/{id}/config` returns a typed,
non-null managed-file array through that port; it contains the durable template
and live rendered output but no planner or persistence record. The CoreDNS
projector consumes only durable Component config, the already-persisted
read-only host resolver baseline, the current host-resolution projection, and
the registered renderer. It has no baseline capture/write capability, so GET
cannot mutate state. Other implementations project an empty array. PUT remains
the single configuration mutation and the Agent/protobuf contract is unchanged.

Groundplane constructs a fixed-revision capability-scoped planning session.
The planner returns immutable intents for existing Service, Route, Volume,
Secret, Script, Backup, Network, Entry, Task, and component-specific
capabilities. Each owning module independently validates and contributes its
transaction fragment; one Controller transaction publishes the desired change,
management ownership, replay evidence, candidate, Task, and queue entry.

Component management ownership is separate from Tenant/Project/Environment/
Platform product ownership. Operator-owned mutation requires authorization
from the exact human request. Secret capabilities expose only metadata and
opaque references; Groundplane alone materializes approved values transiently.

Execution remains Groundplane-owned. Agent procedures are generic to the
capability and closed against raw shell, host paths, arbitrary argv, or plugin-
selected executors. Controller and Agent compile the same immutable registered
catalog. Each application Task carries only the Component id, definition and
catalog digests, action id, artifact identity and digest, and generation. The
Agent resolves the action id to a fixed catalog recipe and verifies catalog
identity, ownership, image digest, labels, mounts, artifact digest, and
generation before applying generic safe effects. A Task never supplies an
executable, arbitrary arguments, shell text, host path, or arbitrary URL. An
implementation-specific config format stays inside its registered translator
and approved runtime artifact; it never creates a technology-specific
Controller, persistence, or Agent path.

The generic Route module owns desired Route state whether or not an HTTP-router
implementation is enabled. A desired-only Controller Task records an `unserved`
result without an Agent effect. Enabling the Environment's one logical router
applies all stored Routes. Disabling it removes the entry point and preserves
the Routes. Caddy provides `http-router` and consumes the authorized Route set;
it does not own Route creation. Component-managed resources retain their product owner and record
separate Component management ownership. Ordinary collections exclude them;
the owning Component detail groups them by capability.

## Release execution authorities

Release execution uses three concrete durable authorities rather than one
generic operation switch. `ReleaseLedger` owns immutable intents,
fixed-revision reads, and bounded publication. The group execution authority
owns declared-order progress and compensation state. The checkpoint authority
owns monotonic per-Release effect evidence and Service projections.

`internal/controller/releaseoperation` owns the concrete Release Group
rollback-preview and rollback-publication use cases alongside the existing
deploy and rollback orchestration, hook selection, and publication flow. The
concrete `ReleaseLedger` remains the only historical selection authority: both
paths call its sole `SelectRollback` operation for every member at one fixed
revision and preserve exact group order. The handler process port accepts and
returns pure core domain inputs and preview results; HTTP handlers parse the
presence-aware optional tag and canonical revision string and map only at the
wire boundary. No `pkg/api` input enters the controller module, and the
Console, CLI, handlers, and group store never implement a second selector or
derive eligibility from public history.

Rollback preview returns a concrete `ReleaseGroupRollbackPreview` value and is
strictly read-only: no durable preview, idempotency claim, Task, or mutation is
created. Rollback without a preview revision selects at the current revision.
With a preview revision, the same controller use case reselects at that fixed
revision and publishes only while the Release Group desired record,
Environment mutation epoch, and existing Release publication fences still
match. Compaction or changed authority fails closed with `state.conflict` before
publication; writes outside those authorities do not stale the selection. The
wire revision is a positive canonical decimal int64 JSON string and is the only
preview token; no manifest digest or client-selected Release ids are accepted.

ADR 0064 adds one shared pure `internal/common/executionplan` candidate
procedure used by ordinary Release and Blueprint. Its immutable plan projection
contains exact candidate and forward anchors plus the complete closed lawful
`serving_predecessor` and `candidate_absence` restoration alternatives. The
Controller claim selects exactly one from the already-sealed nullable applied
predecessor and persists that choice plus its canonical digest with the
assignment, writer, and execution epoch. The Agent receives a selection; it
never derives one from Docker or current Controller projections.

The private release-recovery record at
`/v1/records/release-recoveries/{task_id}` is continuation authority for the
same nonterminal Task. It stores the immutable primary report, assignment,
operation, plan, selected restoration authority, exact recovery steps, cursor,
phase, deadline, and evidence revision. It is neither desired state nor a
second Task. Recovery-only dispatch cannot execute forward work, and final
restoration proof closes the original Task and every ownership fence in one
transaction. Assignment execution epoch is the shared event and terminal-ACK
attempt fence; connection loss alone does not select recovery.

Task publication contributes a closed transaction fragment, including the
Task operation-history index. The ledger never writes Task-owned keys
directly. A maximum 32-member publication is exactly 75 mutations plus 11
compares, for 86 aggregate operations under the repository ceiling. There is
no compatibility path or second schema.

## Script immutable-input reference authority

ADR 0062 replaces only ADR 0040's bulk reference publication and close with
prepared reference generations. The Controller resolves one closed
`ScriptSourceIdentity` union for bodies, runner snapshots, Services, Releases,
Networks, Volumes, Entry value generations, reusable Secret values, and
materialization proof generations. It writes byte-identical forward and reverse
memberships plus one exact execution-count record per source.

Preparation is private and writes at most 16 memberships per transaction under
one cursor. Prepared memberships immediately fence removal. One final
transaction publishes the ordinary Task, operation, executions, snapshots,
sealed plan, and active reverse root. Retry keeps that source set unchanged.
After every execution is `cleanup_proven` and retry is impossible, the source
root becomes `releasing`; bounded transactions remove exact membership pairs
and decrement counts before the ordinary final Task transaction.

The key families are closed:

```text
/v1/staging/script-operation-source-sets/{operation_id}
/v1/records/script-operation-source-sets/{operation_id}/root
/v1/records/script-operation-source-sets/{operation_id}/executions/{execution_id}/{source_suffix}
/v1/runtime/script-source-references/{source_suffix}/{operation_id}/{execution_id}
/v1/runtime/script-source-counts/{source_suffix}
```

One Controller Script-reference module owns every mutation in that keyspace.
Source modules, hierarchy deletion, and retention compare its counts and exact
prefix absence but never release references. Generic Task pruning requires the
source root to be absent and never infers cleanup. Missing symmetry, count
mismatch, digest mismatch, or underflow is Internal corruption and fails
closed.

Network source identity records the Zone's actual owner, so a consumer runner
may retain a backing-owned external Network. Volume authority remains the
stable `vol_` id plus applied-projection capture revision. A Release owns its
immutable render input and retention children. Entry values retain their
existing `cfg_` generation; because reusable Secret rotation is deferred, the
create-only `sec_` id is also its sole MVP value-generation id.

## Testing

- Unit tests per package; the domain (`internal/core`) has no infra
  imports so it tests in isolation.
- Blueprint fixtures must round-trip through the Controller parser and render
  valid Compose output, preserving every native Compose field and every
  supported `x-gp-*` extension. The Console's Desired-state view is a
  contract fixture, not a second legacy grammar: it must show the same
  Compose-plus-extensions shape the Controller accepts.
- Render tests must cover id-based environment paths and env files, per-service
  release projections, external backing-network joins, fact mappings,
  cross-project dependency ordering, route hostname/path validation, backup
  source identity, and rename stability.
- Revision tests must cover `GDR1` envelope and chunk corruption, exact 2 MiB
  projection acceptance and one-byte-over rejection, staging resume and
  abandonment, fixed-revision sealing, compact publication, stale-head claims,
  and cleanup bounds.
- Volume tests must cover slug/key separation, stable-id mounts, qa-workload/WebSocket
  mount preservation, ownership-marker recovery, no-replace publication,
  symlink and mount-boundary rejection, complete deletion impact pagination,
  consumer detachment, bounded recursive removal, retry replay, and Backup
  source consequences.
- System tests drive the CLI against a real Controller+Agent on a host.
- The earliest TLS-first proof is a clean initial Blueprint apply showing the
  selected host-local image identity is sealed before publication, every
  matching pre hook completes and cleans up in deterministic order, a valid
  author-published bundle exists in the declared Volume, and no selected
  application workload starts before that proof. This contract is not a
  production claim until that end-to-end test passes.
- The Console store is the API contract: the fixture store mirrors what the
  Controller holds in etcd, and the acceptance test stays — every
  operational step must work from the CLI with no Console session.
- `make architecture-check` enforces imports, module placement, concrete-first
  interfaces, conversion rules, generated provenance, and the non-growing
  oversized-file baseline defined by ADR 0056.
- Staticcheck 2026.1 runs through the repository-pinned Go tool dependency in
  the production-tag CI graph.
## Shared hierarchy deletion engine

`internal/controller/hierarchydeletion` is the sole orchestration owner for
Tenant, Project, and Environment deletion. Its closed target, operation, phase,
and action types are the domain contract. Handlers only translate and return
the published Task. The Controller Task handler resumes `Service.Execute`.

The etcd adapter owns atomic publication, fixed-revision membership scans,
coordination epochs, compare-and-swap transitions, bounded plan batches,
receipts, summaries, parent-last finalization, retry transfer, and retention.
Descendant adapters dispatch existing capability Tasks with stable action ids;
unknown actions fail closed. Environment deletion is a fourth target of this
engine, not a parallel implementation or compatibility path.

The module boundary is deliberate: membership returns closed procedure inputs;
the domain builds a deterministic `PlannedAction` DAG; and the etcd adapter's
`BindPlan` converts it to exact durable `Action` procedures. Binding derives
retry-stable child Task operation ids and hashes canonical storage-aware
transaction templates with closed symbolic runtime slots. `AppendPlan` accepts
only bound actions. Controller code never owns etcd keys, and the adapter never
reorders or changes domain plan identity.

Durable state uses `/v1/runtime/deletions`, `deletion-locks`,
`deletion-replay-targets`, `deletion-cleanup-fences`, `deletion-intents`,
`deletion-actions`, `deletion-receipts`, `deletion-progress`,
`deletion-summaries`, and `deletion-scan-cursors`. Aggregate publication is at
most 64 comparisons plus mutations and 900 KiB; plan batches contain at most
47 actions, filling the store-wide 96-operation ceiling as
`1 + 47 comparisons + 47 mutations + 1 checkpoint` while remaining below
1 MiB.
