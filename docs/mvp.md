# Groundplane - MVP

Status: v2 (revised model), 2026-08-15. This revision locks in: per-service
attaches with an explicit new-or-existing credential choice (network access is
per Service while a credential may be reused by several Services), per-service deploys (`service + tag +
strategy` chosen at deploy time, tag defaulting to the current one for
redeploy) with rollback tracking the previous tag, clickable/editable service
cards, ingress as opt-in components (Caddy and/or Cloudflare Tunnel, both off by
default; the tunnel token is a secret exposed only to the cloudflare-tunnel
service; DNS is a manual process the token cannot do), connector credentials
as secret references or direct values, the GitHub runner registered on the
host via an operator-provided short-lived GitHub registration token, secrets
as **project-scoped** resources with platform fallback and connectors as
**environment-scoped-only** resources;
the tenant-level pages are gone), a **deterministic per-environment env file**
(`secrets/.env.<environment-id>`) attached explicitly to every service for all-services
env secrets, file-secret paths **relative to the environment's volume
folder**, one unified environment-entry model (env var | file, exposure,
secret flag), and backup encryption with a **per-environment age keypair
generated lazily on first backup enable**, backups toggleable **off per
environment**, selectable **sources** (attach databases, any volume subset,
and the environment's **config** — entries only, never backing environments,
with a warning to that effect on config backups), the public recipient in desired
state and the private identity wrapped at rest by a root-only controller key
(exportable for old-era point restore, rotatable per environment), the **environment Settings
tab** (encryption key, rename, delete), and **stable ids for every referenced
entity** — environment ids are static (`env_…`, the name is only a label),
env vars and deployments gained ids, and every Volume separates its stable id,
mutable slug, and immutable Compose key. `volume_dir` derives from stable ids,
so a label change never renames or orphans data.

**MVP trust boundary (locked).** Hosting assumes a trusted operator
organization; the MVP remains generic for many Tenants, Projects, and
Environments. Tenant, Project, Environment, and credential-owning Attach scopes
remain enforced in records and APIs, but the MVP makes no tenant-network
security guarantee and does not claim strict peer isolation. Backing networks are
shared bridges by design: attached consumer peers and the backing service may
reach one another. Strict peer-isolation enforcement and multi-tenant security
guarantees are post-MVP.

**Backing services (locked).** Shared infrastructure is renamed to
**backing services** (the Twelve-Factor term: "any service the app consumes
over the network as part of its normal operation" — datastores, caches,
queues). A backing service follows the SAME hierarchy as tenant projects:
**backing project → one environment ("main") → one adapter-backed service**.
The adapter knowledge (provision ops, connection URL, fact prefix, backup
strategy) lives on the service; volumes, age key, and the backup policy live
on the environment like any other. The Controller logic (environments,
services, attaches, backups) is shared — only a few adapter fields differ.
Routes: `/platform/backing-services`.

Backing-service creation is one protected atomic operation. The operator
supplies the backing Project labels, one compiled adapter key (`postgres:16`
or `valkey:9`), the `main` Environment network pool, and one new Zone name,
subnet, and `internal` decision. The Controller creates exactly one backing
Project, one `main` Environment, one dedicated backing-owned Zone, one
adapter-backed Service, one adapter-defined data Volume, and one Agent Task.
The Zone subnet must be inside the Environment pool and globally unreserved.
There is no existing-Zone branch, uploaded backing Blueprint, plugin catalog,
implicit subnet, or partial facade. Adapter defaults are versioned compiled
product behavior, and the complete aggregate commits or remains absent.

**Backing-service facts (locked).** Attaching a backing service exposes
**facts**, never injected env vars: prefixed keys (`pg16_URL`, `pg16_HOST`,
`pg16_PORT`, `pg16_DATABASE`, `pg16_ROLE`, `pg16_PASSWORD`) whose **values**
carry the credential-owning Attach's identity - the provisioned database and role are
`<service-name>_<first-6-of-attach-id>` (e.g. `api_5d3f9a`), derived from
the **attach's** id (the first 6 characters of its random tail — not the
timestamp portion). Randomness is mandatory: the shared instance serves
EVERY tenant's EVERY environment, so only the random tail keeps
every database/role instance-unique with negligible collision probability
(30 random bits), no matter how many attaches or how many projects name
their service `api`. Adapter identities are bounded to PostgreSQL's 63-byte
identifier limit, so a credential-backed Attach rejects a Service name longer
than 56 bytes rather than truncating the locked identity. The attach **name**
is only the spec key (an operator
decision, unique per environment, hyphenated: `api-db`) — nothing derives
from it. A grant (an attach whose role may also access
another attach's database) surfaces as an **additional prefixed fact set**
over the granted database with the same role. The operator then creates
live environment mappings from these facts (the app chooses the destination
name). A mapping names the owning attach and optionally the granted attach;
omitting the grant selects the owning attach's fact set. Each mapping is created with an
**exposure**: a specific service or all services in the environment. There
is no auto-injection and no alias system.

Attach facts are not Secret-store resources. They remain owned by the
credential-owning Attach and are exposed only through explicit Attach fact
mappings. Operator-supplied platform credentials such as runner registration
tokens, tunnel tokens, and connector credentials use the separate Secret
store: choosing an existing value persists its Secret reference, while direct
input atomically creates a Secret-store item and persists only that reference.

Groundplane is a small self-hosted tool that lets one person run many projects on
their own machine without hand-maintaining Docker Compose. Two small processes
split into a control plane and an execution plane:

- **Controller (control plane)** - stores the desired state, serves the Console,
  and decides. It owns the baked-in tasks: it sequences deploy steps, renders
  and validates component configs, schedules backups, and tracks history and
  observed state. It tells the Agent **what** to do and **how** to do it — the
  exact procedure, with validated parameters. State, desired-state docs, task
  queue, and the encrypted secret store live in a **single-node etcd** on the
  host (secrets additionally encrypted at rest app-side with a host key). The
  Console and human API default to loopback with no auth in the MVP. The
  Controller may additionally bind explicitly configured trusted private
  interfaces for same-operator access. Wildcard, public, and Cloudflare-Tunnel
  exposure are forbidden until a future human-API authentication decision. Its native
  `groundplane-controller.service` also owns the local Agent OCI container's
  complete lifecycle through Docker: create, start, stop, replace, rollback,
  and removal (ADR 0016).
- **Agent (execution plane)** - runs on the same machine as a **container**
  (ECS-agent pattern) created and reconciled by the native Controller. There
  is no `groundplane-agent.service`. It applies what the Controller commands
  through Docker Compose and streams back progress, logs, and observed state.
  The Agent has no decision authority, carries no scripts of its own, and
  never updates or recreates itself: it is a dumb, safe executor of
  Controller-supplied procedures.

**Platform components (locked) — the groundplane-infra project.** The
platform-owned workload containers live in the `groundplane-infra` compose
project, rendered from Controller-owned platform state (never tenant desired
state) and applied by the Agent like any other project. CoreDNS is its sole MVP
Component and is a project member (host network, binds `127.0.0.1:53`). The
Agent is a separate runtime resource whose container the native Controller
creates and reconciles directly through Docker, so the Agent never owns its
own lifecycle. Starting
`groundplane-controller.service` is the only Groundplane service bootstrap;
there is no Agent unit or bootstrap Compose file. Agent enrollment is the
operator-facing `agent join` task after the Controller is running. The
Agent container runs as root with host networking and Docker restart policy
`no`; the Controller is its only restart authority. Its bind mounts are
limited to the Docker socket read-write, `/var/lib/groundplane/agent`
read-write at the same path, the read-only Agent-channel directory, the
read-only runtime config, and the read-only token file. It never receives the
host filesystem root, a generic host runtime mount, or workload materialization
roots. The
Controller itself is the one **systemd unit that never becomes a container**:
docker recovery is systemd's job (`docker.service` restarts itself), and the
Controller reconciles the Agent container when Docker returns. It updates
itself through a
staged-binary Agent task that validates before exec with the old binary as
fallback — an operator surface like any other action (`groundplane
component update controller --platform`, `POST /components/{id}/update`).
The Controller orchestrates and validates; the Agent performs workload host
mutations, while the Controller alone mutates the Agent container lifecycle.
The Console shows this on the **Platform page**: an overview
(components, bootstrap chain, agents), and **one page per Component with
its own settings** (`/platform/components/coredns`). Agent configuration
remains on the Agent resource and never creates a Component projection.
The native Controller has its own `/platform/controller` page for the exact
startup YAML document, while the Agent has its own `/platform/agents/{id}`
page for durable runtime configuration. The Platform Host page is health and
capacity only and never duplicates either settings editor.
Platform Component settings live here, never in the
platform Settings page, which stays small. The **Tasks section on the Platform
Components page** is a second placement of the complete Platform workspace
journal: it requests the same `workspace=platform` records as Platform
Activity, with the same order, page boundaries, and cursor. Component work such
as DNS saves → Corefile reloads, Agent config changes, and Component updates
appears there because it is Platform-owned, not because the section filters by
Component or `target`. The MVP has no target or Component Task index.

**etcd is host-level, not a component (locked).** etcd is the bootstrap state
store, so it never lives inside Agent-applied `groundplane-infra` and never
depends on the Task journal it stores. The native Controller synchronously
creates and starts one private `groundplane-etcd` OCI container before opening
the store, then reconciles that container for its entire daemon lifetime. The
container uses a digest-pinned multi-architecture image, host networking with
client and peer listeners bound only to loopback, persistent
`/var/lib/groundplane/etcd` state, Docker restart policy `no`, and strict
Groundplane ownership labels. The Controller is its only lifecycle authority;
there is no etcd systemd unit, distro etcd service, Agent Task, configurable
endpoint, or alternative hosting path. etcd status remains visible on the Host
page and through `controller etcd show`, not as a platform component.

**Host execution authority is closed (locked).** Workload and Platform
execution supports exactly `linux/amd64` and `linux/arm64` hosts. One release
publishes both variants under the same immutable release identity. The MVP
does not add multi-host scheduling or emulation. ADR 0060 defines the exact
binary, OCI index, platform-child, and two-host acceptance rules.

data-plane host mutations are Agent Tasks. The native Controller executor owns
only the bootstrap and isolation substrate that cannot depend on the Agent:
the Agent container lifecycle, isolated Runner runtimes, and persistence-only
native procedures. Those are Controller Tasks in the same durable journal;
they are not direct handler or scheduler side effects. The `/etc/resolv.conf`
rewrite pointing at `127.0.0.1` remains
**Agent materialization** (same category as env files at 0600; the Agent
writes it as the final step of applying the DNS service, and restores the
previous content if CoreDNS is removed). This rule is what keeps future
platform infra trivially deployable on hosts that carry **only an Agent**:
add a component, the Controller renders it, the Agent on that host applies
it. ADR 0056 defines the exact two-executor boundary; neither executor infers
authority from Task kind or target.

**DNS resolver (locked).** CoreDNS (`coredns/coredns`, pinned) is the host
resolver: the Controller renders its **Corefile exactly like the Caddyfile**
— validated before reload, and CoreDNS's `reload` plugin swaps configs
gracefully, so updates are **zero-downtime** and an invalid Corefile is
rejected while the old instance keeps serving. Static entries serve the
router's internal names; the **upstream** is either **auto** (resolvers read
from the host's `/etc/resolv.conf` at render time) or pinned to explicit
resolver addresses; **per-zone forwarders** route specific domains to their
own resolvers (each rendered as a `forward <domain> <resolvers>` line before
the catch-all — the tailnet delegation is one of these, managed
automatically); when
Tailscale runs on the host, the **tailnet delegation** forwards the tailnet
domain (`ts.net`) to `100.100.100.100` so MagicDNS names resolve through the
same resolver. **Auto-detection only sets the toggle's default** — when
Tailscale appears on the host the delegation switches on by default, and
the operator can override it any time on Platform → CoreDNS. The
**Agent** rewrites `/etc/resolv.conf` to point at `127.0.0.1` as
materialization (see "Everything host-level is actioned by the Agent").

The CoreDNS configuration includes one required operator-authored
`corefile_template`. It is bounded UTF-8 with LF line endings, no NUL byte, a
final LF, and exactly one `{groundplane}` marker. The registered renderer
replaces that marker with Controller-owned bind, static-host, per-zone
forwarder, catch-all forwarder, and reload directives; the surrounding
Corefile remains the persisted operator decision. The canonical default keeps
Prometheus, log, and errors enabled around the managed root server block.

**Groundplane "zone" vs DNS "zone" (locked terminology).** The tool uses
"zone" for two unrelated things and they never mix: a **network zone** is a
docker network a service joins (the topology columns, `networks.<name>` in
desired state — the Console calls these network zones, services bridge them,
and backing services own theirs); a **DNS
zone** is a domain in the resolver's namespace (a `forward <domain>` / hosts
entry in the Corefile). Every "zone" above and below in the router/DNS
sections means a DNS zone — network zones never appear in DNS names, and
DNS zones never appear in topology.

**Environment DNS → the Caddy entry router (locked).** Split-horizon on the
host: when an environment enables the **Caddy** component, the Controller adds
that environment's public route hostnames as **static A records in the
Corefile's `hosts` block pointing at the environment's Caddy private IPv4**
— so on the host and LAN those hostnames resolve to the internal router,
while public DNS continues to point at the edge (Cloudflare). The hostnames
come from the environment's route/tunnel hostnames (never guessed); exact
names only — wildcard subdomains (e.g. `*.app.example.com`) are deferred
(CoreDNS's `template` plugin can synthesize them later). Nothing in the
model ever depends on the public internet for internal traffic.

**Multi-host is the future foundation, not the MVP.** The containerized
Agent is exactly the unit a multi-host fleet needs (Controller-managed
enrollment and image replacement, Controller-served config, and a task
contract already shaped for labels and targeted dispatch). **Placement** —
which agent runs an environment — does not exist in the MVP contract and is
out of scope; the MVP is one host. Remote channel transport and mTLS belong to
that post-MVP topology.

The Controller decides, the Agent applies.

**Component Capabilities (locked).** Groundplane owns generic typed
capabilities for Services, Routes, Volumes, Secrets, Scripts, Backups,
Networks, Entries, Tasks, HTTP routing, outbound tunnel lifecycle, DNS resolution, and
managed configuration. A registered Component declares which capabilities it
provides and the exact operations it may consume. It receives bounded immutable
planning inputs and returns immutable intents; it never receives Controller
internals, repositories, etcd, Docker, filesystem access, secret plaintext,
raw Task steps, or arbitrary execution.

The MVP registry is closed at build time to Caddy (`http-router`), Cloudflare
Tunnel (`edge-tunnel`), and CoreDNS (`dns-resolver`). There is no custom
or runtime plugin loading. Integrations compile in separate modules against the
public Component SDK. REST/OpenAPI is the external machine boundary; the CLI is
only a human client. Groundplane validates and atomically applies every intent
through the same first-class capability implementation used by the human API,
then executes host effects through durable Tasks and typed Agent procedures.

Component management ownership does not replace Tenant, Project, Environment,
or Platform ownership. A Component may create and manage only resources allowed
by its grants; an operator-owned mutation requires authorization from that exact
operator action. Secret capabilities expose only metadata and opaque references.
ADR 0061 fixes this boundary. Its remaining visible product choices are listed
in `docs/capabilities.md` and must not be inferred from the current Caddy-
specific implementation.

Component configuration is a closed discriminated union at every API and
Blueprint boundary. A disabled or unconfigured Component projects
`config: null`; enabling or configuring it requires exactly one complete typed
variant for Caddy, Cloudflare Tunnel, or CoreDNS. No flattened cross-kind
configuration fields or compatibility shape is accepted. CoreDNS configuration
requires `corefile_template` alongside its structured resolver fields and
responses preserve empty resolver and forwarder collections as `[]`, never
`null`; Tunnel credentials use an explicit `existing` or `new` mode and the
corresponding fields are exclusive.

`GET /components/{id}/config` also returns a non-null `managed_files` array of
generic typed `{path, template, rendered}` projections. For CoreDNS the sole
entry is `/etc/groundplane/coredns/Corefile`: `template` is the durable
operator-authored Corefile and `rendered` is derived live by the registered
renderer from that config, the already-persisted read-only host resolver
baseline, and the current host-resolution projection. The GET is
side-effect-free and never captures or persists a missing baseline. Components
without a managed-file preview return `[]`. `PUT /components/{id}/config`
remains the sole configuration mutation.

**One backend, many frontends (locked).** The Controller is the single
backend: it holds **all** logic — schema validation, action sequencing,
config rendering (desired state → Corefile / Caddyfile / compose), task
dispatch, scheduling. The Console is a frontend, never the product. There
are exactly three frontends over the one Controller API:

- **Console** — the production user interface. Everything implemented here is what the API
  must do: every drawer, dialog, action, and view is a first-class feature
  of the backend, not UI sugar. The production Console is **React 19 +
  Vite + shadcn/ui + Tailwind** as a static SPA, `go:embed`'d into the
  Controller binary. Its components and store are the maintained product
  surface (see docs/architecture.md).
- **CLI** — `groundplane <resource> <action>` (e.g. `groundplane env deploy
  storefront production app-api --tag sha-…`). Mirrors every operator-facing
  Controller capability 1:1. Only the closed local process/tooling commands
  and machine bootstrap actions named in `api-cli.md` are exempt; Agent
  enrollment is an operator-facing Console, CLI, and API action. The
  full command tree and API structure are defined in
  `docs/api-cli.md` (companion to this document).
- **API** — HTTP on loopback by default (127.0.0.1, no auth in the MVP), with
  optional explicitly configured trusted private listen addresses. It is never
  exposed through the application Cloudflare Tunnel. Every
  operator-facing Console action maps to exactly one API endpoint and CLI
  command. Machine bootstrap endpoints are explicitly outside this parity
  set.

The Console store (`console/src/lib/store.tsx`) is the stand-in for the Controller
API: **anything the UI does today is the API contract** — if it isn't in
the store, it doesn't exist in the product. Frontends carry zero logic:
they render Controller responses and send Controller-validated requests.
The acceptance test (the selected workload completes Gate A and Gate B through
Groundplane, including replica-aware release/Tunnel traffic and source
restore/recovery proof, with zero hand-written shell scripts) applies equally to all three
frontends — the same operations must work from the CLI with no Console
session involved.

**Implementation (locked).** One Go repository with three binaries —
Controller, Agent, CLI. The CLI is built on **Cobra** (nouns as command
files, the `completion bash|zsh|fish` noun falls out of Cobra's built-in
completion). All binaries share one **common project** (`internal/common`)
that holds the unified implementations: one **unified logging** setup
(standard `log/slog`, one call per entry point, level priority `--debug` >
`--verbose` > `GROUNDPLANE_LOG_LEVEL` > config-file `log.level` > default
WARN, **file AND stdout/stderr both always available** — a stderr text
handler at the configured level plus a rotating file handler always at
DEBUG for the daemons, both destinations configurable from their owned
configuration, never log secret values), **one config reader** used for the
Controller startup file, the Controller-owned Agent runtime document, and the
CLI file (defaults < file < env < flags, strict key checking, fail-fast
validation; there is no Agent bootstrap YAML file),
one error taxonomy + one shared error
handler (RFC 7807 codes on the API, single handler on the CLI), shared
output, and ULID ids. `internal/common` is importable by every binary
including the CLI and carries no heavy dependencies (no etcd, docker,
systemd, age); the daemon-only platform integrations live in
`internal/infra`, which the CLI never imports. The structure exists for
three reasons:
**decoupling** (thin entry points, pure domain without infra imports),
**logic in one place** (shared logic is never re-implemented per binary),
and **unified implementation for scaling** (one durable Task journal with
closed Agent and Controller executors, one renderer, one scheduler tick —
every consumer goes through the same well-tested path). The extension model is
part of this contract:
adapters, connector kinds, and platform components are in-repo packages
behind one narrow registry — core never switches on kind — so adding one
is a package plus a registration line. Third-party plugin loading is
explicitly out of scope (compiled-in; the narrow interface is the
insurance policy, not a feature). The two machine contracts have two
tools: the agent channel is **gRPC + protobuf** (locked), the human API
is **REST/JSON documented by one OpenAPI document, generated code-first
from the typed handlers** — the Console's TypeScript client and the
CLI's Go client both derive from it, so the 1:1 rule is a build error,
not a review finding. Full layout in
`docs/architecture.md`.

The product goal is narrow: navigate Tenant -> Project -> Environment -> Services,
then deploy, roll back, and back up without pain. This document is the contract
the Console follows and must stay authoritative.

### Minimum hosting floor

The first hosting milestone has two sequential gates. This is a documentation
contract correction; runtime alignment, clean frontend/API/CLI cutover,
capability-ledger acceptance, and acceptance proof remain pending.

**Gate A — disposable first-hosting acceptance.** Groundplane bootstraps on an
already provisioned supported Linux/Docker host with a Groundplane release,
explicit non-overlapping
environment, system, and dedicated Runner network pools, and all operator
workload images already present in the host Docker daemon. One trusted operator
acceptance exercises one selected Tenant, Project, Environment, and Blueprint;
the MVP product remains generic for many tenants, projects, and environments.
The floor
requires PostgreSQL and Valkey backing with fact mappings, explicit
uid/gid-aware Volumes, Entries, and files, identity dependencies, generated CA
and leaf TLS material before identity starts, and the exact existing
HTTP/WebSocket/internal-callback and deny-by-default policy. Caddy plus
Cloudflare Tunnel is the public path; provider DNS and ingress configuration are
external prerequisites. The selected acceptance workload must persist and
reconcile operator-authored native `deploy.replicas` counts (N is at least one).
The Gate A proof exercises more than one WebSocket replica with Valkey fan-out
and a stable logical target. API blue-green, scheduler, and queue remain
singleton in this floor unless an application explicitly opts in; a replicated
Service uses recreate, preserves its count through deploy, rollback, restart,
and exact reapply, and may be a Release Group member. Blue-green with N greater
than one is rejected before mutation and is post-MVP; N==1 remains supported.
There is no advertised zero-downtime replicated recreate. A single ordered
Release Group action covers migration once and the selected service releases.
Health, logs, Tasks, rollback, restart, and exact reapply are included.
TLS first-provision workflow and exact Caddy policy rendering are named proof
gates. The generic initial-Blueprint pre-hook contract below is pending runtime
alignment and must first prove TLS material is validly published before any
selected application candidate starts; this document makes no production
claim. Gate A is disposable acceptance only; production cutover requires Gate
B and its backup/restore/retention proof.

**Gate B — production MVP operations acceptance.** Every actual persistent source has a
proven backup, restore, and retention path with existing per-source safety and
original-target rules. Restore tests may use a disposable environment; there is
no new scratch-target API. PostgreSQL Attach/config/Volume sources are the
  current bounded restore scope. Valkey data backup/restore is required by this
  floor, but the namespace-safe per-consumer source versus an explicit
  shared-instance RDB source still needs a closed contract and proof;
the current runtime rejects `strategy.not_implemented` until implemented, and
live data-directory archival is not authorized. Gate B is Gate A plus these
required source backup, verified original-target restore, and retention proofs.

The floor exclusions apply to both gates: no replica scaling beyond the
WebSocket proof and operator-authored counts above, a second private acceptance
workload, or GP-managed CI Runner provisioning is required. Under the explicit
assumption that the existing external build process supplies host-local
workload images, GP-managed CI Runner provisioning is not a Gate A or Gate B
dependency; its detailed Runner design remains subsequent work without changing
that contract. Extra
router providers or runtime plugins, registry integration or hosting, tenant
peer-firewall isolation, automated empty-host DR, advanced monitoring/alerting,
or unrelated repository refactors. The current no-host-port rule remains locked.
A proposed bounded seam
for a topology-specific break-glass need—explicit loopback-only native Compose
mapping on operator-owned recreate Services—requires owner resolution before any
implementation and is not current behavior.
These exclusions do not waive the existing full CI, API/CLI/Console parity,
security, generated-artifact, or exercised destructive-path requirements.

## Mission: replace the operational shell scripts

Groundplane is not a general orchestrator. It is built to express and run a
representative multi-service workload that was previously operated with Docker
Compose files, shell scripts, timers, and state files. The **acceptance test for
the MVP** is that this private workload completes the Gate A hosting journey
(including replica-aware release and Cloudflare Tunnel traffic) and Gate B
backup/restore/retention and recovery proof through Groundplane with **zero
hand-written operational shell scripts**. Every required
step becomes declarative desired state, an operator-authored Script resource, or
a **baked-in action** provided by the Controller. The private workload, its
credentials, topology, and deployment driver remain outside Git.

### Baked-in actions

Every operational shell script becomes either a **built-in action primitive
owned by the Controller** or an explicit operator-authored **Script** resource.
Built-in actions have named typed parameters that the Console and API expose
and the Agent implements against Docker. Script resources intentionally allow
free-form bodies for operator automation; they are still explicit desired
state, task-dispatched, scoped to one logical Service, fixed to a 900-second
timeout, and never stored as Agent-owned commands. MVP Script output is drained
and discarded; only typed terminal metadata is retained. Adapters and platform
components may not smuggle arbitrary shell through their contracts. Each
operation has:

- **typed parameters** validated by the Controller (service, image tag,
  deploy strategy, retention days, …) — no `$1` positionals;
- **ordering and guards** built in: **deploy serialization lives at the
  Controller** — it refuses to dispatch a second Deploy/Rollback for a
  service that already has one in-flight (a new deploy for that service is
  allowed only after the previous one is aborted or completes); the agent
  never runs two schedulers, never switches the router before the
  healthcheck passes, and renders/validates Caddy config before reloading
  it. The abort/intercept mechanism is the **Agent channel (locked)** below;
  retried tasks reconcile against labeled-container state before
  re-applying steps (idempotency, never applied twice).;
- **idempotent, recorded results**: each run lands in deploy history /
  observed state, so `active-slot` and `image-history` stop being ad-hoc files.

Scripts use the closed `RunScript` Agent step. The Controller seals the
  immutable body generation, logical Service and Release snapshot, resolved
runner projection, and operation identity; the Agent executes it only in a
task-scoped one-off container. Manual scripts and deploy/rollback hooks are in
the MVP. Their immutable runner sources use ADR 0062's prepared reference
generations, so accepted hook and plan bounds never depend on one oversized
publication or close transaction. Scheduled scripts are deferred; if added later, the Controller
scheduler will turn them into the same typed Script operation rather than
introducing a second execution mechanism.

### Baked-in actions become Tasks

Every action is dispatched as a **Task** under a proper task contract, stored
in etcd:

- **Task contract** - `{ id, operation_id, retry_of?, idempotency_key,
  executor, type, target, params, steps, timeout, plan_hash, status, ack }`.
  `executor` is an explicit immutable decision: `agent` for workload-host
  procedures or `controller` for native control-plane procedures such as local
  Agent enrollment and removal. Each executor has its own durable FIFO queue
  and claim record; neither may infer authority from `type` or `target`, and an
  Agent can never claim a Controller Task. `type` is the closed journal kind:
  operator-facing action kinds (deploy, rollback, backup, restore, attach,
  detach, run, script, provision, create, update, remove, start, stop, destroy,
  rotate) plus the Controller-published internal `backup_prune` kind. Internal
  prune Tasks are observable through the existing Task and Activity list/detail
  surfaces but have no operator action, API mutation, CLI command, or Console
  control. `remove` deletes desired state; `destroy` preserves desired state and
  sets runtime intent to `absent`.
  **Attach
  and detach are tasks**: attach runs the adapter's provision procedure,
  detach runs its deprovision (revoke grants → drop role → optionally drop
  database) and removes the desired-state record as part of the task —
  never a plain record delete.
- **Pull model** - the Agent **pulls** Agent Tasks from the Controller (no
  push), while the native Controller claims Controller Tasks from its separate
  durable queue. On claim the task moves from `pending` to `running`; on
  completion the executing authority **acks** the task id back to the Controller
  (`acked`). `acked` is an acknowledgement event, not a final state. Final
  states: `completed`, `failed`, `timed_out`, `aborted`.
- **Timeout + dead-agent detection** - every task carries a timeout set in
  Controller config. If the Controller sees no ack within the timeout, the
  agent is considered dead or the task stuck — the task is marked
  `timed_out`. Only `failed`, `timed_out`, and `aborted` attempts may be
  retried. A retry creates a new task id, points `retry_of` at the immediately
  retried attempt, and preserves the same `operation_id`, target, parameters,
  execution plan, `plan_hash`, timeout, and idempotency key. Pending, running,
  and completed attempts fail with `task.not_retryable`; a second active retry
  for the operation fails with `task.retry_in_flight`. Before applying any
  step, the Agent reconciles observed labels and the operation's durable step
  checkpoints, so transport retry never means blind re-execution.
  Backup is the narrow retry payload exception: it preserves the captured run
  snapshot but rebuilds only failed and unstarted point-attempt payloads with
  fresh point ids. Old-identity restore remains non-retryable.
- **Streaming** - the ack/state flow doubles as the live progress feed:
  task states and step output stream back to the Controller's GUI (the
  Console progress views simulate exactly this).
- **Heartbeat** - two signals with distinct roles (see Agent channel):
  the **`Ready` liveness message** is authoritative for dead-agent
  detection (it proves the task loop is alive); **HTTP/2 keepalive** only
  proves the socket and drives reconnect timing. The `Ready` cadence comes
  from agent config (pull interval). The Agent becomes stale after
  `max(3 * pull_interval, 30s)` without `Ready`; new assignments stop and any
  active tasks are timed out through the normal task state machine.
- **Concurrency is Controller-managed** - the Agent executes up to
  `max_concurrent_tasks` (set in the agent config the Controller generates).
  Deploy/rollback/restore are tasks like anything else; nothing is
  hard-serialized by the Agent.
- **Agent enrollment** - `groundplane agent join` calls `POST /agents`, which
  returns `202 {task_id}`. The Controller creates the local Agent record,
  internally generates its channel token, injects the runtime token and
  Controller-owned config into the Agent container, and starts it through
  Docker. No token, CA, certificate, bootstrap file, or combined enrollment
  artifact crosses the human API or stdout. There is no pending approval
  state or approval command. The token authenticates the local Agent channel
  until that Agent is removed. Enrollment must receive authenticated `Ready`
  within 120 seconds. The exact accepted socket, token, storage, and
  runtime-file mechanics are in ADR 0011. mTLS and certificates are post-MVP.

  Initial enrollment input is host bootstrap configuration, not human API
  input. `controller.yaml` owns `agent.image` plus `agent.runtime`.
  `agent.image` may be empty while the Controller runs, but enrollment rejects
  it until it is a digest-pinned OCI reference; mutable tags and host-derived
  image selection are forbidden. Initial runtime defaults are exactly a
  2-second pull interval, three concurrent Tasks, and an empty labels map.
  They are copied into the durable Agent config at enrollment and subsequent
  changes use the Agent config singleton. CPU count, architecture, hostname,
  and other host observations never alter these defaults.

  The production Agent image is built by `Dockerfile.agent` from immutable
  upstream image digests. It contains the statically built Agent, Docker CLI
  29.1.3, and exactly Docker Compose 2.40.3. The Agent binary is the explicit
  entrypoint with an empty image command; the persistent Agent and every
  short-lived helper use that same image. Tags are build handles only and the
  Controller still accepts only the resulting RepoDigest. ADR 0043 fixes the
  complete packaging and version contract.

**Agent channel (locked): gRPC + protobuf.** The Controller and the Agent
communicate over **one long-lived gRPC bidirectional stream per agent,
initiated by the Agent** over `/run/groundplane/controller/agent.sock`. The
pull model is preserved: the Agent signals
`Ready` and the Controller assigns work; nothing is ever pushed to an
unconnected Agent. The local MVP channel uses a Controller-provisioned token:
32 random bytes encoded as 43 unpadded base64url characters, encrypted at rest,
with SHA-256 digest lookup. The `.proto` file is the single source of truth for the machine
contract, so both sides compile against it and a contract change is a build
error rather than a silent runtime mismatch.

Message contract (sketch — exact fields live in the proto):

```
Agent → Controller:
  Connect(agentId, runtimeToken)       # authenticates the Controller-created Agent
  Ready{capacity}                      # the PULL = "give me work"; idle Ready is the heartbeat
  TaskEvent{task_id, step, state, chunk}# step progress + streamed output
  ObservedState{containers, health}    # drift-detection feed for reconciliation
  TaskAck{task_id, result, exit}        # completed / failed

Controller → Agent:
  TaskAssignment{task identity, typed ExecutionPlan, timeout}
  TaskAbort{task_id, reason}           # demand-execution-action: cancel in-flight work NOW
  ConfigUpdate{agentConfig}            # sent after drain at a fully idle Ready; no process restart
  Shutdown
```

Semantics bound to the channel:

- **Abort/intercept** — the Agent's executor is a worker pool
  (`max_concurrent_tasks` workers); each worker owns a `context.Context`,
  and the current step runs with it. `TaskAbort` cancels that context, the
  step returns, the task lands as `aborted` in the Controller, and only
  then may the Controller dispatch the next Deploy/Rollback for that
  service (serialization lock, see Baked-in actions). Cancellation
  propagates to the Docker operation (container stop) the same way.
  The public abort action targets that existing Task and returns its id; it
  never creates a second abort Task. Pending work terminalizes atomically,
  while running Agent or native Controller work returns only after its exact
  cancellation acknowledgement has durably committed. ADR 0041 fixes the
  response, replay, race, and terminal-state semantics.
- **Heartbeat — two signals, distinct roles.** (a) **`Ready` liveness is
  authoritative for dead-agent detection**: the agent sends an idle `Ready`
  at its pull-interval cadence, and a missing `Ready` past the timeout marks
  in-flight tasks `timed_out` — a keepalive cannot prove the task loop
  isn't wedged, `Ready` can. (b) **HTTP/2 keepalive pings prove only socket
  liveness** and drive reconnect timing (exponential backoff) on a dropped
  stream — a dead socket is detected faster by keepalive than by waiting
  for the next `Ready`.
- **Progress → Console/CLI** — the Agent streams `TaskEvent`s to the
  Controller, which writes the state transitions to the etcd activity
  journal; the Console and CLI read them over the human API. Log *output*
  (high volume) is proxied through the Controller with a bounded, short-lived
  buffer per active request — never persisted in etcd. Resource log streams
  are non-resumable and use the fixed-source contract in ADR 0059; there are
  no direct agent→UI pipes. The private `LogReady` handshake gates SSE headers
  after initial Docker setup, including zero-source setup. Workload-authored
  Docker stdout/stderr is forwarded unchanged except UTF-8 normalization and
  32 KiB line truncation. Groundplane never adds Secret-store values, generated
  credentials, or generated env-file material to log frames; workloads are
  responsible for not printing secrets, and MVP has no secret-aware redaction.
- **Config** — `ConfigUpdate` rides the open stream and is applied on the
  next `Ready`, no restart.
- **Agent removal** — removal first stops new assignments, aborts every active
  task with reason `agent_removed`, revokes the channel token, waits for the
  Agent to be offline, removes its container, and deletes its runtime material,
  config, and record. Its Controller Task deadline is 120 seconds. A removed
  token is never accepted again.
- **Controller-owned Agent replacement** — an Agent update fences assignments
  and proceeds only when the Agent is idle. The native Controller rotates the
  token and generation, replaces the container at configured `agent.image`,
  requires authenticated Ready within 120 seconds, and otherwise rotates again
  and rolls back the previous digest. The Agent never receives a self-update
  command and never recreates itself. The bodyless Task has a 300-second
  deadline; bundles and release-signature policy are post-MVP (ADR 0010).
- **Multi-host** — the MVP channel token authenticates the one local Agent and
  the Controller dispatches by labels. Remote transport identity, mTLS, CA
  lifecycle, and certificates are post-MVP decisions.

The human surface stays **REST/JSON** (Console + CLI over the same HTTP
API); gRPC+protobuf is the **machine** surface (Agent). "One backend, many
frontends" is unchanged: the Controller is still the single backend, and
the Agent is simply another client of it.

**Images (locked).** Workload deployments use only images already present in the
host Docker daemon, including preloaded third-party workload images. The
Controller asks the Agent to resolve each newly selected requested reference
once before publication and seals its exact local Docker image id with the
Release. Requested reference remains provenance; optional registry or manifest
metadata never substitutes for the local id. Rollback, prior restoration,
retry, and immediate pre-mutation validation inspect their already sealed local
id directly rather than resolving a mutable tag again. Retagging therefore does
not replace historical bytes; a missing sealed image fails before any workload
mutation. Locally built images with no `RepoDigests` are valid. Deploy has no
registry pull or image build side effect. Pinned Groundplane-managed Agent,
etcd, Component, and backing-service images remain release or installation
assets, not operator registry integration. Public/private registry integration,
registry-backed deployment, and build-on-deploy are post-MVP.

The full catalog of scripts and the task that replaces each one:

| Legacy workflow | What it does | Groundplane replaces it with |
| --- | --- | --- |
| scripts/deploy.sh | blue/green API deploy: start inactive slot, healthcheck, render + validate + reload router | **Deploy** action on a **Service** (per-service: service + tag + strategy chosen at deploy time); the Agent runs the same ordered steps (inactive slot -> /up healthcheck -> router switch -> record active slot) |
| scripts/workers-deploy.sh | recreate operator-configured scheduler, queue, and WebSocket replicas after the API switch | independent **Deploy** actions by default; one of the environment's explicit release groups may coordinate them when the operator chooses |
| scripts/identity-deploy.sh | bring up the private identity stack, optional migrate | **Deploy** of the identity services, including its TLS-provisioned prerequisites |
| scripts/migrate.sh | run Laravel migrations against prod_backend_net before the switch | the migrate step of **Deploy** (expand/contract, one-shot `artisan migrate`) |
| scripts/rollback.sh | previous immutable image into the inactive slot, then workers | **Rollback** action — per-service, the previous tag is tracked from deploy history and pre-selected; a traffic switch, never a cold start |
| scripts/register-runner.sh | register the GitHub Actions runner with a short-lived token | **Runners page** per tenant (repo- or org-scoped); operator-provided short-lived GitHub registration token (never generated by Groundplane); runner container deployed on the host; GitHub self-hosted only |
| scripts/provision-identity-db.sh | create least-privilege roles + databases on the backing postgres | **Attach** on the backing postgres project - each Service joins through its own Attach and explicitly creates a credential or reuses an existing credential-owning Attach |
| scripts/provision-identity-tls.sh | generate the identity CA and server certs | provisioning the identity stack's internal TLS as a declared prerequisite |
| scripts/generate-identity-secrets.sh | rotate identity secrets | **Secrets** provisioning on the Environment |
| scripts/artisan.sh | run artisan commands in a service | **Scripts** - first-class per environment, executed against a service (`when: manual \| pre-deploy \| post-deploy \| pre-rollback \| post-rollback \| on-failure`) |
| scripts/logs.sh | tail logs across the stack | **Logs** action on a Service / Environment |
| scripts/sync-provision.sh | rsync the infra tree to the Pi | eliminated — the Agent already lives on the host, so there is nothing to sync |
| backup/r2-backup.sh + systemd timer | encrypted database/config/storage artifacts to R2 at 03:15 | **Backup policy resource** (frequency, retention, encryption, target) + a typed source procedure, scheduled by the Controller |
| cleanup/slot-cleanup.sh + systemd timer | remove the inactive slot after 7 days, keep 3 images | **Retention policy** in the Environment desired state |
| setup/install, setup/storage, setup/swap, setup/runner, ... | one-time host provisioning (users, dirs, storage ownership) | the Agent's **provision host** step on first run |

State files also move into the tool:

- `active-slot`, `image-history`, `slot-*.inactive-since` -> Controller state
  (deploy history as per-service records — each deployment stores
  `service · tag · strategy · when · status`, so rollback can look up the
  previous tag of any service — plus the active slot as observed state).
- `router/Caddyfile.*` -> rendered by the Agent from templates as part of the
  **Caddy ingress component** (validated and reloaded, never a blind write); the
  Cloudflare Tunnel component is wired by the Agent from the token secret.
- `secrets/.env.*` -> stored encrypted in the Controller, materialized by the
  Agent on the host (0600), referenced through the same `--env-file` /
  `env_file:` mechanism as today.
- systemd timers (backup, cleanup) -> **the Controller scheduler**: the
  Controller runs as its own systemd service (`groundplane-controller.service`),
  so the scheduling loop lives inside the daemon — a tick in the service that
  evaluates all schedules from desired state (still systemd calendar
  expressions, only the engine changes) and dispatches due actions to the
  Agent. No separate timer is needed; N policies cost nothing extra, no root
  unit churn, and next-run times are visible in the Console.

## Model

    Tenant ── many Projects ── many Environments ── many Services
       │              │
       │              ├── tenant Project   (an application; may be a microservice architecture)
       │              └── backing Project  (a backing service — datastore/cache, built once, attachable anywhere)
       │
       └── resource and credential-scoping boundary

A Tenant's public record is `{id, slug, name, description}`. `description` is
an optional operator-authored summary and may be empty; it is persisted and
editable rather than a Console-only decoration.

A Project's public record is `{id, tenant_id, slug, name, description, kind}`.
`description` is an optional operator-authored create-time summary, may be
empty, and is immutable after creation. Project `edit` changes the display
`name` only; Project `rename` changes the scoped `slug` only.

Two kinds of **Project**, both following the SAME hierarchy
(project → environment(s) → service(s)):

- **Tenant project** - belongs to exactly one Tenant. It may be a microservice
  architecture with many Services (a storefront is one). Its Environments attach
  backing services as needed.
- **Backing project** - a **backing service** with no owning Tenant: the
  Twelve-Factor term for "any service the app consumes over the network as
  part of its normal operation" (datastores, caches, queues). Built once, it
  supports any project: the shared PostgreSQL is the MVP example, and the
   template is dynamic (databases, caches, message brokers, anything). A
   backing project holds **exactly one environment** ("main") with **one
   adapter-backed service**: the adapter knowledge (provision ops, connection
   URL, fact prefix, backup strategy) lives on the service, and volumes and
   the age key live on the environment like any other — so
   the Controller runs the same logic for both kinds and only the adapter
   fields differ. **The backing environment itself never runs a backup
   (locked): backups are per consumer** — every attached environment backs up
   its own attach through the adapter (`pg_dump` of exactly that database;
   shared attaches are backed up once, never per service). The backing
   service&apos;s Backups tab reports what consumers are backing up (how many
   enabled, per-consumer schedule/retention/encryption/records) instead of
   running an instance-level policy.

The template is **dynamic**: any number of Tenants, Projects, Environments,
Services, Network Zones, and backing Projects. Nothing is hard-coded to two
tenants and one database.

- **Tenant** - a resource and credential-scoping boundary within the trusted
  operator organization. Shared backing bridges may permit peer
  reachability; the MVP does not claim tenant-network security. May register
  org-scoped Runners, managed on its Runners page.
- **Project** - an application (tenant) or a backing service (backing). A
  tenant Project may be a microservice architecture with many Services; a
  backing Project is one environment with one adapter-backed Service.
- **Environment** - one deployable instance of a Project (staging, production).
  An environment has a **static, unique id** (`env_…`, generated at creation)
  and a **name that is only a label** — every environment-scoped reference
  (secrets, age identity, backups, deploy history, activity, `volume_dir`)
  keys off the id, so renaming never breaks anything. The URL keeps the name
  for readability; the deterministic env file name uses the environment id.
  Every Environment also reserves one required IPv4 `network_pool`. The pool is
  Controller IPAM metadata, not a Docker network: every Zone subnet must be
  contained by it, while Environment pools remain globally non-overlapping on
  the MVP host. It may be resized only when the replacement contains every
  existing Zone subnet and overlaps neither another Environment reservation nor
  the system pool. The reservation remains fenced until Environment network
  cleanup succeeds. The
  environment's **Settings tab** shows its identity (id, name, volume folder,
  deterministic env file, network pool and allocation). Allocation is the
  deterministic sum of reserved Zone CIDR addresses at the Environment read's
  fixed revision; total and available addresses count the complete parent CIDR
  without assigning host-address semantics. The tab also shows the backup **encryption key** (per-environment
  age: recipient, repeatable no-store current-identity export, rotate), and the destructive
  actions: **Rename** (label-only, id unchanged) and **Delete** (typed
  confirmation; removes the environment plus everything scoped to it —
  env entries, age identity, backups, recovery points). Delete retains its
  fence, Connector credentials, and required key material until every remote
  point/orphan object is checkpointed, deleted, and verified absent.
- **Service** - one container or shared runtime: a **stable name** (`api`,
  referenced by aliases, depends_on, routes, Caddyfile) and a **unique
  creation id** (`svc_<ULID>`; the attach id's random tail, not the service
  id, disambiguates provisioned values on backing services — see Attach). The
  id never appears in the
  service name, in references, or in env var keys. Fields: image, zones
  (network
  membership), `strategy` (the **declared default** — `blue-green` /
  `recreate` implemented, `rolling` rejected in the MVP; the Deploy action
  chooses the strategy per deployment and overrides this default), healthcheck
  (`http <path>`, `tcp <host:port>`, or `pgrep <cmd>` with
  interval / timeout / start_period / retries), resources (`mem`/`cpus`),
  command, mounts (env-scoped volumes), native `secrets`/`configs`, generated
  `env_file`, aliases, depends_on, expose ports
  (internal
  only — no host port publishing), restart, logging, and native
  `deploy.replicas`. Per-service
  `environment` (service-scoped env vars, see Env var exposure below) and
  `env_file` entries are layered over the environment's inherited all-services
  file; service-specific values remain scoped. Each service also has durable
  Controller-owned `runtime_intent: running | stopped | absent`. This is
  operational state outside the Blueprint and is never authored or overwritten
  by a bundle apply. Start sets `running`, Stop sets `stopped`, and Destroy sets
  `absent` while retaining the desired service. Remove deletes the desired
  service and its runtime intent. A Service created directly or first
  introduced by Blueprint starts with `runtime_intent: running`; subsequent
  Blueprint applies preserve the current intent. Each lifecycle action is
  bodyless and atomically commits the new intent with one durable Task; a
  failed, timed-out, or aborted Task does not roll intent back. A Service in
  the current applied projection uses a 120-second Agent Task pinned to that
  exact projection and Blueprint revision. Start performs targeted Compose
  apply, Stop performs targeted Compose stop with a fixed 30-second grace, and
  Destroy removes only the target containers. A Service not yet present in an
  applied projection uses an observable 30-second Controller no-op Task. One
  active lifecycle Task is allowed per stable Service id, and retry reuses the
  exact immutable render input. A service card is
  clickable: full spec, edit, delete — edits save to desired state and apply
  on the next deploy (redeploy defaults to the current tag). Full spec includes
  a canonical native Compose document derived from the current immutable
  Environment desired revision rather than reconstructed from the flat Service
  record.
  Direct create and edit publish a new immutable normalized desired revision
  but do not deploy it; the next explicit deploy applies it. Remove never
  cascades: live Routes, Attaches, Release Groups, dependent Services,
  Service-scoped Entries, Scripts/hooks, and active operations must be cleared
  first. Remove keeps the current desired head and visible Service until its
  targeted runtime cleanup succeeds, then atomically promotes the candidate
  revision and deletes desired plus runtime state. Volumes and immutable
  release history are retained. ADR 0058 fixes the complete revision and retry
  contract.
- **Network Zone** - a named internal network that groups Services for
  communication (frontend, backend, egress, identity-private). Its public
  record is `{id, environment_id, name, subnet, internal, owner_kind,
  owner_id}`. `owner_kind` is `environment` or `backing_project`; platform
  component networks are generated internal artifacts and never Zone resources.
  A Zone's required IPv4 subnet must be inside its Environment pool and must not
  overlap any other reserved Zone, Docker network, or protected host route. The
  name, subnet, `internal` flag, and owner are immutable because Docker cannot
  mutate bridge IPAM in place; replacement is add a new Zone, move Services,
  then remove the old Zone. The physical bridge name is
  `gp_net_<stable-zone-id>`. A Service joins **any number of zones** and appears
  in every zone it joins (a service on two zones is a bridge).
  **Zone removal (locked):** an ordinary Zone is removed only after its Service
  memberships are reconciled; a service on two zones keeps its remaining zone,
  and only a service that loses its **last** zone becomes unzoned. Unzoned
  services render in a dedicated **"no zone" column** in the topology (dashed,
  "no network" — they can talk to nothing until edited back into a zone). The
  column is **ALWAYS rendered** — even in an environment with zero zones — so
  unzoned services can never silently disappear from the topology. Removing a
  backing-owned Zone is the sole dependency-cascade exception: confirmation
  lists every affected Attach, Service, and provisioned database and requires
  the Zone name; the Task runs each normal detach, including credential
  revocation and per-Attach database/role deprovisioning for managed adapters,
  before removing the network. A failure retains visible drift and a retryable
  Task. The Console obtains that list from a fixed dependency-impact read; its
  opaque impact token fences the later cascade dispatch against a stale list.
  An ordinary Zone selected by an enabled Caddy Component is
  `resource_in_use` until Caddy is disabled or moved to another Zone; Zone
  deletion never invents an automatic ingress reconfiguration.
- **Attach** - how exactly one **Service** (not a zone) consumes a backing
  service. The Service joins that backing service's dedicated network as a
  consequence, never by picking a Zone. During creation the operator chooses
  either **Create new credential** or **Use existing credential**. A new
  credential provisions a database + role on a managed PostgreSQL instance;
  an existing credential references a ready credential-owning Attach in the
  same consumer Environment and on the same Backing Service, provisions
  nothing, and exposes the owner's facts to the additional Service. An Attach carries a
  **name** — an operator decision or Controller-generated suggestion from the
  current tenant slug, project slug, environment name, and service name,
  hyphenated (`api-db`),
  unique per environment — that is ONLY its spec key. The selected name is
  stored and does not recalculate on later renames. For a new credential, the
  provisioned identifiers stay random
  (`<service>_<first-6-of-credential-attach-id>`, see facts below). A Service
  may attach the same Backing Service multiple times and chooses the credential
  source independently for each Attach.
  Attached infra renders inline in the tenant topology as a linked
  chip on the service that attaches and is listed as a consumer on the backing
  service page.

  **Attach exposes facts, not env vars.** The moment an attach exists, the
  Console shows its prefixed fact set — key prefix from the adapter, values
  carrying the attach's identity:

      pg16_HOST     = postgres
      pg16_PORT     = 5432
      pg16_DATABASE = api_5d3f9a          # <service-name>_<first-6-of-attach-id> — random tail, instance-unique
      pg16_ROLE     = api_5d3f9a          # one role per attach
      pg16_PASSWORD = <generated>         # reveal-once, rotatable
      pg16_URL      = pgsql://api_5d3f9a:••••••@postgres:5432/api_5d3f9a

  Because the identifiers embed the attach's unique id (the first 6
  characters of its random tail — never the timestamp portion), two
  attaches of the same service can never collide, and two projects may
  both have a service named `api`, yet their databases can never clash on
  the shared instance. The keys stay stable and prefixed; uniqueness
  lives in the values.

  **Valkey authentication.** A Valkey backing instance chooses immutable
  `authentication`: `username_password` (default), `password`, or `none`.
  Every Attach inherits it. Named mode issues a named user/password; password
  mode issues an independently revocable password on the shared default user.
  No-auth is an explicit instance-wide choice: any reachable client can access
  it. Its bindings generate no credentials and expose only HOST, PORT and a
  credential-free URL. Named mode additionally exposes ROLE; authenticated
  modes expose PASSWORD and a secret URL. All modes share data and Pub/Sub;
  none provides keyspace isolation. Consumer credentials exclude administrative
  commands and never expose Groundplane's separate bootstrap administrator.
  ACLs survive restart. Password detach revokes only that owner's password
  for future AUTH, not already-authenticated default-user connections. The
  existing owner/reuse protections also apply to credential-free fact bindings.
  Mode changes require a new instance. See ADR 0068.

  **Grants.** An attach may also access a database owned by another attach of
  the same backing project under its **single role**. Grants reference the
  target attach id/name, never a mutable database string (e.g. the identity
  service's attach provisions `identity_c4d5e6` and is granted access to
  `api_abcdef`). Each grant surfaces as an **additional prefixed fact set**
  over the granted database (same keys, same role, granted database): the
  identity service's facts are its own set (`pg16_*` → `identity_c4d5e6`)
  plus a set for `api_abcdef` — the operator names the env vars they create,
  so the key overlap is harmless.
  Grant support is an explicit adapter capability: PostgreSQL 16 supports it;
  Valkey 9 and `manual` reject grants before task publication.

  **Credential ownership.** The credential-owning Attach is the durable
  credential identity; no separate public Credential resource exists. A new
  credential owns itself. An existing-credential Attach stores a direct
  reference to that owner; reference chains are rejected. Only owners store
  encrypted fact values, adapter provisioning identity, grants, and Backup
  database-source identity. A dependent Attach resolves fact reads through
  its owner and contributes only its Service/network membership. It rejects
  grants. The owner cannot detach while dependent Attaches reference it. A
  dependent detach removes only its own membership; owner detach performs the
  normal grant revoke and adapter deprovision after all dependents are gone.

  **Networks have one explicit backing owner, never an implicit consumer
  owner.** Every Backing Service owns its own dedicated backing Environment,
  Zone, and network. PostgreSQL and Valkey therefore never share a Groundplane
  backing network. Attaching a consumer joins the selected Backing Service's
  network as `external`; a Service attached to both joins both networks.

  **The backing-service contract.** Every backing project declares a **generic
  spec** whose `adapter` (`postgres:16`) selects the provisioning knowledge.
  The `postgres:16` adapter resolves its workload image only from
  Groundplane's immutable managed PostgreSQL 16 release record; no Project,
  Blueprint, operator setting, Controller configuration, or Agent configuration
  may select or override its repository, tag, digest, platform, helper, or
  adapter contract version. `name` is the **unique service name**,
  enforced unique by the schema — it is the DNS-resolvable name consumers
  connect to on the joined network (`@postgres:5432`), so two backing projects
  can never collide. A separate display `label` is presentation only. The
  Controller looks the adapter up by key and runs the image resolved by that
  accepted adapter contract. The
  auto-provisioning logic lives in the **adapter**, and adapters are part of
  the project — an extensible registry in the tool's codebase (e.g.
  `adapters/postgres:16`), so adding a new backing-service kind is dropping
  in an adapter, never editing core logic. Adapters are **contracts, not
  dependencies**: declarative typed steps the Controller sequences through
  the one task pipeline — a new kind is a package plus one registration
  line (see docs/architecture.md, "Extensibility").

  **The `manual` adapter (locked).** `manual` is a first-class adapter with
  **no auto-provisioning**: attaching a service to a manual backing service
  only **joins the network** — no database/role creation, no facts, no
  credentials, no grants, and no Groundplane-managed backups (the backing
  project's backup policy starts off). The operator runs and manages the
  service themselves; Groundplane only wires connectivity. Reachability is
  purely the service name on the zone. The Console surfaces this everywhere
  an adapter appears: the attach dialog explains network-only attach, the
  backing page shows no provision operations and no connection/rotate for
  consumers, and the Backups tab is marked "not applicable".

  Each adapter knows how to provision that kind, how to render its connection
  URL (`pgsql://` vs `redis://`), how to dump and restore it, and which env
  vars it exposes (`pg16_URL`, `pg16_HOST`, `pg16_DATABASE`, `pg16_ROLE`,
  `pg16_PASSWORD`, ...).

  An adapter's **`provision`** is its list of auto-provisioning operations:
  typed, parameterized steps that run on attach, rotate, and repair (create
  database, create role, generate password, grants for postgres; set
  requirepass / ACL for valkey; ...). Placeholders such as `<db>`, `<role>`,
  and `<generated>` are filled by the Controller from the attach facts and
  the generated password, then the Agent executes the concrete procedure.
  The operations are shown in the Console before you attach, so attaching
  never surprises you.

  Attaching a Service dispatches to its adapter: the Controller generates the
  exact provisioning procedure from the adapter and the Agent executes it,
  then the **facts** (prefixed keys, service-id values) are available in the
  Console immediately — each attach is its own fact set, so the same backing
  project attached twice produces two independent sets, and each grant adds a
  further set. The operator creates live env mappings from the facts; the
  names are the app's choice (see Env var exposure). Auto-provisioning is a
  first-class citizen, typed and discoverable on
  the backing service page, and identical for every consumer of the same kind.

  **Backing services are created explicitly.** There is no lazy creation.
  The **New backing service** flow takes the backing Project slug, name, and
  optional description, one compiled adapter key, an Environment pool, and one
  new Zone with an explicit name, subnet, and `internal` decision. Only a
  running backing service accepts attachments.
  (No backup policy on the form — the backing environment never backs up,
  and it therefore **never lazily generates an age key** either: the key
  appears only when backups are first enabled, which a backing environment
  never does. Its Backups tab reports consumer policies instead.)
  The compiled adapter fixes the Service name, image, health check, exposed
  port, restart policy, replicas, managed Volume, mount target, bootstrap
  Entries, facts prefix, and procedures. The create request has no image,
  Volume, Compose, Blueprint, existing-Zone, or adapter-options field.
  Consumers join the backing-owned Zone as an external network when they
  attach. A created instance stays running with zero consumers. **Destroy**
  removes only the Service runtime and keeps the Project, Environment, Zone,
  Volume, Entries, credentials, Attach history, and data. **Stop** and
  **Start** also keep the Volume and data. Permanent removal uses Project
  deletion after its dependency checks pass.
- **Router and edge tunnel** - the Environment has exactly one logical
  `http-router` Component Capability binding and one optional `edge-tunnel`.
  Caddy is the sole MVP HTTP-router implementation; Cloudflare Tunnel is the
  sole MVP edge-tunnel implementation. Both are off by default and their
  lifecycle decisions are independent.
  Enabling either Component requires an explicit nonempty, duplicate-free
  ordered `zone_ids` list of ordinary Zones in the same Environment. Both
  Components support multiple memberships, independently selected by the
  operator. Enable and Configure offer existing-Zone selection and inline
  Zone creation. Inline creation uses the ordinary Zone-create operation;
  a created Zone remains an operator-owned Environment resource if subsequent
  enablement fails or is cancelled. It is never silently deleted on disable.
  The first Caddy Zone is its explicitly presented primary Zone. The Controller
  transactionally reserves the first available usable address in that Zone,
  excluding the network, gateway, broadcast, and other reserved addresses; the
  address remains stable while the primary Zone is unchanged, until successful
  disable. Additional Caddy interfaces use dynamic addresses like ordinary
  Services. Host/LAN DNS continues using the primary pinned address. Changing
  only secondary memberships does not replace that reservation. Groundplane
  never infers a Zone by name or assumes `.2`. Every Route target Service must
  explicitly share at least one selected router Zone and expose its required
  `target_port`; unreachable Routes fail
  validation rather than silently changing memberships.
  Route resources are authoritative Environment desired state. They may be
  created, edited, or retained while no HTTP router is enabled and then report
  observed state `unserved`. Create and edit return the Route with a Controller
  Task. Route observed state is closed: `unserved` means no provider is enabled;
  `pending` means desired state awaits provider application; `served` means the
  latest successful pinned provider observation matches the desired generation;
  and `degraded` means the latest provider apply failed while desired state is
  not confirmed. The Task records a desired-only result without an Agent effect when no
  router is enabled. Enabling Caddy reconciles every stored Route. Disabling
  Caddy removes the entry point and releases its address but preserves every
  Route. Re-enabling Caddy allocates an address and reconciles those same
  Routes.
  Caddy's optional template contains
  exactly one `{routes}` marker, where the Controller inserts deterministic
  longest-path-first blocks using stable Service aliases and explicit target
  ports. The complete Caddyfile is validated before reload. There are no
  per-Route snippets or `{host}`/`{slot}` substitutions. Exact internal Route
  hostnames use Caddy's internal CA; the enable/reconcile Task installs that
  Environment root into the host trust store. LAN clients may install the
  exported public root manually.
  Tunnel config stores its ordered `zone_ids` and one authorized Project or
  platform env-var Secret reference as stable `secret_id`. It joins exactly
  its selected Zones, never a router-inferred or default bridge. At least one
  selected Zone must be non-internal; the first such Zone explicitly carries
  default-gateway priority for connector egress. Internal memberships retain
  their isolation. Removing any Zone selected by an enabled router or tunnel
  is rejected until the Component membership is changed or disabled.
  Direct token input creates a Project Secret
  and immediately replaces the write-only token with that stable reference.
  Only the generated cloudflare-tunnel Service receives it as `TUNNEL_TOKEN`.
  Groundplane never fetches, parses, or displays the token and never calls the
  Cloudflare API. It starts and stops the connector and reports its health.
  Groundplane does not configure Tunnel DNS, public hostnames, ingress rules,
  origin targets, or protocol; those remain in the provider-managed Tunnel.
  Caddy's normal proxying preserves
  WebSocket upgrades, including the private workload's WebSocket upgrade paths and signed
  `/apps/*` endpoints; there is no separate WebSocket Route mode.
  Component-managed Services, Volumes, Entries, Networks, and other resources
  do not appear in ordinary management collections. The owning Component page
  groups them by capability. Topology views may show them with a managed marker,
  and their stable-id detail remains readable. Only the owning Component may
  mutate them. Their Tasks and Activity remain visible under the normal
  Environment, Tenant, or platform scope.
- **Runner** - the GitHub Actions self-hosted runner. It is a registration, not
  a Project, and it is **managed on the tenant's Runners page** (repo-scoped per
  Project or org-scoped per Tenant). Only GitHub self-hosted runners are
  supported for now. A Tenant may own at most five Runner records in total;
  Project-scoped Runners count against the same Tenant quota. A direct Runner
  has exactly one Tenant owner index; a Project Runner has exactly one Project
  owner index. The combined quota record is not a second ownership index.
  Pending or failed creation and pending removal retain the quota until
  successful finalization.

  Each Runner uses its own generated `/29` bridge allocated from the dedicated
  `runner.network_pool`, which is disjoint from both `environment_pool` and
  `system_pool` and is provisioned as a `/24`, `/25`, or `/26` at bootstrap. It
  never joins another Runner, Environment, backing, or platform
  network and publishes no host port. The Runner container receives only its
  dedicated unprivileged rootless Docker daemon socket, never the host Docker
  socket. The native Controller allocates one persisted host identity slot per
  Runner: one unprivileged UID plus distinct fixed 65,536-id subordinate UID and
  GID blocks from finite machine-local configured ranges. The per-slot runtime
  allocation record and Runner record retain that exact slot through every
  retry and release it only after successful local cleanup. Pool exhaustion is
  `resource.in_use`; there is no operator-facing identity-pool API. Docker
  builds, container actions, and service containers remain inside that Runner's
  user namespace. UID/cgroup egress policy denies all Groundplane private pools
  except the authenticated Controller endpoint while allowing DNS and outbound
  internet. Host networking, privileged mode, and `CAP_NET_ADMIN` are forbidden.

  The operator provides a **fresh short-lived GitHub registration token**
  (obtained manually from GitHub repo/org Settings) for create and each
  failed-create Retry action. Groundplane never generates, fetches, persists,
  hashes, logs, or reuses the token. It is cleared after that attempt's
  registration step. Loss before registration is proven fails the Task with
  `registration_token_required`; Retry reuses the Runner id, quota, host slot,
  and subnet but requires a fresh token. Generic Task Retry cannot retry Runner
  creation. After pairing with GitHub, the runner pulls its Groundplane agent
  config (pull interval, max concurrent tasks, labels) from the Controller.

  Create atomically publishes the provisioning Runner, quota and allocation
  claims, replay evidence, and Controller Task before host mutation. Removal
  atomically publishes its tombstone, cleanup intent, replay evidence, and
  Controller Task while keeping every allocation fenced. Successful
  finalization releases all local resources and durable claims; failure retains
  them for retry. Removal never deregisters the Runner through GitHub. Status
  (`online`/`offline`) is a Controller-observed projection of the supervised
  local runtime, never desired state or an operator mutation.
- **Route** - a provider-neutral domain or path that sends traffic to a Service.
  A Route remains valid without an enabled HTTP-router provider and is observed
  as `unserved`; it is served only when the Environment has an enabled provider.
  Cloudflare Tunnel is an edge tunnel and never a router. A Route stores stable `target_service_id`, required `target_port`,
  `exposure: public|internal`, host, and path. Public Routes require a lowercase
  ASCII DNS host; internal Routes may omit it. Paths default to `/`, are
  absolute, contain no query or fragment, and may use only an optional terminal
  `*`. Duplicate `(environment_id, host, path)` tuples are rejected; overlaps
  compile longest path first and then stable Route id. Routes never publish a
  target Service port on the host.
- **Database** - a database instance provisioned on a backing PostgreSQL project
  for one consumer (its own name, role, and credentials).
- **Volume** - persistent storage owned by an Environment. A Volume has a
  stable `vol_` id, an Environment-scoped mutable `slug`, and an immutable
  Compose `key`. Service mounts and Backup sources retain the id. Compose and
  the derived host leaf retain the key.
- **Secrets** - an Environment's variables, stored safely and never printed.
  Secrets are project-scoped resources with platform fallback. Connectors are
  **environment-scoped-only resources**, never project or platform scoped. The
  environment page's **Variables tab**
  holds the environment-scoped entries (vars, files, secrets — one unified
  entry model with the secret flag). The Console's left navigation is split into
  **platform navs** (Overview, Backing services, Activity, Secret store —
  platform secrets only, Host, Settings) and **tenant navs** (Projects, Runners,
  Activity, Settings); secrets live on project pages, while connectors live in
  environment Backup settings. An enabled backup policy selects a connector
  owned by that environment; enabling with no connector, or with a missing
  connector, is a validation error. Enabling also requires a valid frequency,
  positive retention count, and at least one selected source. A disabled policy
  may omit its connector.
  Entry removal is protected and asynchronous. An Entry that has participated
  in an applied Environment dispatches a 120-second Agent Task; a never-applied
  Entry dispatches a 30-second Controller finalizer with no host mutation. The
  Entry remains visible until successful terminal acknowledgement. Success
  removes its exact pinned plain or secret file, or rewrites every affected
  generated env file from the remaining pinned generations, before atomically
  deleting its metadata and subordinate value generations. The canonical
  all-services env file is rewritten even when it becomes empty; an empty
  service-specific env file is removed. Failure, timeout, or abort preserves
  both desired and applied state for retry. Entry removal does not run Compose,
  restart, or recreate a Service, so environment values already loaded by a
  running process change only on its next deploy or reconciliation. Reusable
  Secret deletion, not Entry deletion, enforces Component reverse references.
- **Deploy** - a **per-service action**: pick ONE logical service, the immutable
  image **tag** for it, and the **strategy** for this release
  (`blue-green` / `recreate` implemented; `rolling` is rejected in the MVP). The
  strategy is chosen **at deploy time**, per deployment — it is not a
  property of the environment, and it overrides the service's declared
  default for this release only (see Release strategy rules). The Deploy
  task's typed parameters are `{ service, tag, strategy, on_failure }`, where
  `on_failure` defaults to `switch_back` and may be set to `leave_active` for
  this release. Native omission of `deploy.replicas` means one and normalizes
  once at the authored-input boundary. The accepted Release then captures a
  positive explicit count; a missing or zero durable count is invalid and is
  never defaulted or reconstructed from current desired state. For blue-green
  with N==1, the generated Compose
  project contains two physical singleton slot workloads and one stable
  Controller-owned Caddy proxy for the logical service. Routed Caddy and
  internal consumers target only that stable proxy. The Controller starts the
  inactive slot, waits for exact candidate health, then atomically reloads the
  proxy from a sealed JSON configuration to replace its upstream. The old
  healthy slot is retained for rollback. Blue-green is rejected when N is
  greater than one or when the Service has no addressable internal TCP port;
  replicated blue-green is post-MVP. Recreate stops the old logical set before
  creating the desired N workloads (N==1 is a singleton), so downtime is
  explicit; it retains the stable logical proxy when addressable and targets
  only the desired set. The Controller seals candidate and prior Compose
  artifacts, proves the full desired set healthy, and can restore and prove the
  exact prior artifact and captured count under `switch_back`. Blue-green to
  recreate and recreate to blue-green are both rejected before mutation unless
  the sealed prior and candidate workload counts are each exactly one. An
  accepted blue-green-to-recreate transition removes the sealed slot topology
  before applying the desired set. Restart
  and retry use only the same sealed artifacts and converge idempotently. Start,
  Stop, Destroy, and reconciliation operate on the full logical set. The Agent
  applies the procedure, health and proxy configuration are observed, and the
  run lands in
  deploy history as one record per service (`service · tag · strategy · when ·
  status`), so every service's history is independently traceable. The tag
  **defaults to the service's current tag**: redeploying the same tag is the
  common case — it re-applies spec, env-file, secret, and healthcheck changes
  while resolving that newly selected requested reference exactly once. The
  same tag may therefore pin new local bytes after an external retag; it does
  not promise reuse of the preceding Release's image id. An explicit new tag is
  entered only when releasing a differently tagged build. The Deploy dialog
  takes the **tag only, never the image name** (the image name comes from the
  service); the Controller combines those authored inputs as requested-reference
  provenance, while final Compose uses the resolved sealed local image id.
  Clicking a topology service card or a Services-tab list row opens its full
  spec in a right-side Service details drawer;
  edits save back to desired state and need a redeploy to apply.
- **Rollback** - the per-service mirror of Deploy. It **tracks the previous
  tag automatically**: for the selected service, the rollback target is the
  **most recent successful deploy whose tag differs from the current tag**
  (failed/timed-out records are never selectable, and a successful redeploy
  of the current tag never advances the rollback point — a rollback always
  returns to a tag that actually served) and it is **pre-selected** by
  default; an explicit tag may be entered
  instead. It is a traffic switch through the same strategy as the original
  deployment, never a cold start, and it never reverses migrations. The new
  rollback Release copies the selected historical Release's requested
  reference, local Docker image id, and replica count; it never resolves that
  tag to current bytes or derives missing historical authority.
- **Release group** - one operator action for an explicit ordered set of 2 through 32 logical services with one group-level
  `on_failure: switch_back | leave_active` policy. The default is
  `switch_back`. Each service retains its own release record; the group policy
  controls whether already-switched members return to their previous healthy
  releases after a later member fails or remain active for explicit rollback.
  Members execute serially in the declared order; this is not a simultaneous or
  atomic distributed switch. For a group deploy, a request `tag` overrides the
  persisted group tag; if both are empty the deploy is rejected, with no
  member-current fallback. The chosen deploy tag resolves separately against
  each member's image once in one all-or-nothing Agent batch before publication
  and pins its exact host-local Docker image id; registry or manifest metadata
  is optional and never substitutes for that local id. Each member records its
  release history independently.
  Group rollback never consults the persisted group tag. An optional rollback
  request tag makes each member independently select its newest eligible
  historical Release that completed successfully and reached serving with that
  exact tag and a tag different from its current serving Release; omission
  selects the newest such eligible Release with a different tag for each
  member. The Release Group Rollback dialog has a real **Preview** action. The
  Controller resolves every member from one fixed revision and returns each
  selected Service, Release, and tag in exact group order. A missing, expired,
  ineligible, or wrong-member source rejects the whole preview. Preview is
  read-only: it creates no Task, mutation, idempotency claim, or durable preview
  record, and it never asks the Console to reconstruct eligibility from lossy
  Release or Task history.

  The Console requires a successful preview for the current optional-tag input
  before confirmation and sends that preview's revision with rollback. Any
  exact tag-input change invalidates the preview; loading and selection errors
  remain visible. A stale revision returns `state.conflict`, invalidates the
  displayed preview, and requires a fresh preview and confirmation. Direct API
  and CLI rollback may omit the preview revision and select from current state.
  With a supplied preview revision, the Controller reselects all sources at
  that fixed revision and publishes only if the Release Group desired record,
  Environment mutation epoch, and Release publication fences still match.
  Unrelated writes outside that authority do not stale the preview. If any
  member has no eligible source, the entire group rollback is rejected before
  publication.

  Optional tag presence is exact. An omitted tag means implicit selection; a
  supplied empty, blank, or surrounding-whitespace tag is invalid and is never
  trimmed into omission. The persisted group deploy tag is never a rollback
  fallback. Rollback accepts no client-selected Release ids, compatibility
  alias, alternate eligibility control, or preview persistence. An accepted
  idempotent replay remains the original replay even if its preview revision is
  now stale; changing exact tag presence, tag value, or preview revision under
  that key is an idempotency mismatch.
  The group `on_failure` overrides member defaults for aggregate compensation.
  Blueprint lifecycle edges
  are frozen at the same revision; publication rejects a group whose declared
  order places a selected Service before its selected prerequisite and never
  silently reorders the group. A retry preserves the same candidate ids,
  slots, requested-reference provenance, workload seals, proxy/render digests,
  render inputs, and order, probes the sealed proxy state, and runs only the
  required physical compensation. It never replays forward switches.
  Release execution includes the Script lifecycle hooks selected by ADR 0040.
  A migration Script binds once to its designated logical member (for example,
  the API), while every selected Script targets its logical Service and executes
  once per logical Release, never once per replica. Manual and deploy, rollback,
  and failure hooks use the same task-scoped one-off runner; there is no hook-
  free compatibility path and no group-level hook resource.
- **Backup** - three parts, and database identity is **per credential-owning
  Attach, never per consuming Service**: the
  Controller tracks every database it provisions on the shared instance, and
  the environment's Backup page lets the operator **select which db projects
  (credential-owning Attaches) to back up** - one source per provisioned
  credential. Existing-credential consumer Attaches are never additional
  sources, so a database used by api, worker, and scheduler is backed up
  exactly once. **Sources are fully
  selectable (locked):** attach databases, **any subset of volumes** (a few,
  not all — e.g. back up `app-data` but not `identity-data`), and the
  environment's **config** (env entries: vars, files, secrets — values
  included, age-encrypted; restore replaces the entries from the bundle).
  The config source backs up **THIS environment only** — it **never includes
  backing environments** (postgres/valkey and their configs) and
  never platform state; the Console warns about this wherever the config
  source appears. All selected sources
  run under the **one backup policy** — frequency, how many
  previous backups to keep, encryption (age key ref or unencrypted), and a
  reference to a connector — so a single scheduled run backs up every
  selected source (the Controller dispatches one dump per source through
  its adapter). An unencrypted policy cannot select the config source because
  config includes secret Entry values; selecting config requires
  `encryption: age`. **Backups can be switched OFF per environment (locked)** —
  staging/dev environments simply never back up (the policy and selected
  sources survive the toggle, they just don't run), and the Backups tab
  shows a clear disabled state. The
  **strategy** (how to back up this kind) is closed for the MVP: PostgreSQL
  Attach -> `postgres-custom-v1` (the exact PostgreSQL 16
  `pg_dump --format=custom --compress=none` stream), config -> the canonical
  `environment-config-v1` USTAR Entry archive, and Volume -> the canonical
  `volume-tar-v1` USTAR filesystem projection, then stage, optionally
  age-encrypt, upload,
  verify with HeadObject, prune past retention. The **frequency** uses one
  bounded UTC grammar evaluated directly by the Controller: daily is
  `*-*-* HH:MM:SS`; weekly is `Mon|Tue|Wed|Thu|Fri|Sat|Sun *-*-* HH:MM:SS`.
  Hours are `00` through `23`; minutes and seconds are `00` through `59`; each
  separator is exactly one ASCII space. There are no systemd timers,
  subprocess validators, timezone suffixes, ranges, lists, repetitions, or
  calendar dates. The **connector** is a separate environment-scoped resource:
  where the backups go plus the credentials to get there (initially
  `s3-compatible` for R2, later S3/MinIO/B2). A Backup Policy selects the
  exact stable Connector id owned by that same Environment; Connector identity
  never falls back through Project or platform scope.
  Each connector credential (access key, secret key) is either a **reference to
  a secret-store env var** (recommended — enter the env var name such as
  `R2_ACCESS_KEY_ID`; only this credential `secret_ref` resolves through the
  owning Environment's Project and then platform fallback; the value lives in
  the secret store once and never appears in desired state) or a **direct
  value** baked into the connector and stored encrypted; the New connector
  flow offers both per credential and
  suggests existing env secrets from the store. The Controller passes the policy +
  connector to the adapter, which makes the dump/encrypt/upload/verify/prune
  judgments.

  The policy is one Environment singleton replaced as a complete document.
  Each resolved source has a stable `spt_` id keyed by its Environment, kind,
  and stable target id. Names and slugs are input-resolution and display
  labels only. Public API source records return the stable source id and
  `target_id`; persistence stores only that stable target id. Removing a
  source from the singleton policy preserves its
  catalog identity; re-adding the same surviving target reuses that id, and old
  Recovery Points remain resolvable. Only an enabled policy owns a durable
  reverse reference to its Connector, so Connector deletion is blocked by an
  enabled policy but not by a disabled retained configuration.

  Runtime supports PostgreSQL Attach, Environment config, and Volume sources.
  Valkey backup is required by the hosting floor, but its source procedure and
  safe format remain unclosed; current runtime rejects
  `strategy.not_implemented` before Task creation. A run is bodyless and always
  captures every configured source at one policy revision. One visible Task
  processes stored order and fails fast; earlier verified points survive, and
  retry preserves that immutable run snapshot, rebuilds only failed and
  unstarted point-attempt payloads with fresh point ids, and prevalidates all
  pinned target, Connector, and key dependencies before publish. It never
  snapshots the current policy. Restore with a bounded old identity remains
  non-retryable. Consistency is
  per source, never one cross-source snapshot. Config captures one etcd revision
  in bounded typed Controller-to-Agent Entry content for backup and returns
  bounded validated Entry content Agent-to-Controller for restore; no config
  bytes enter Tasks, events, or logs.
  Volume capture stops every mounting Service and restores its prior intent.

  Config and Volume use the strict canonical USTAR framing and closed readers
  fixed by ADR 0047. The decoded source has no outer compression or archive.
  `encryption: none` stores those exact source bytes; `encryption: age` stores
  exactly the unarmored binary age v1 encoding for one X25519 recipient.
  PostgreSQL uses the first-party digest-pinned managed PostgreSQL 16 workload
  image and its release-pinned helper; no mutable tag or generic exec path is
  Backup authority.

  Each source produces a deterministic versioned format in the Agent-owned
  `/var/lib/groundplane/agent/tasks/<task>/<step>/<point>` transient area. The
  persistent Agent has no workload-root mount; helpers receive only that exact
  task-stage bind and the authorized source bind. Config and a quiesced Volume
  use computed preflight bounds; PostgreSQL uses an overflow-safe conservative
  estimate. Staging writes remain `ENOSPC`-safe. Exact bytes are known, fsynced,
  and hashed only after capture and before immutable upload. Restore checks the
  known stored size before download and decoded size again before mutation. The
  Agent uses the neutral
  `artifact.bin` object name, records `encryption: age|none` with `key_era` only
  for age, and verifies exact metadata with HeadObject before committing a
  Recovery Point. Its id and `created_at` are allocated together before
  execution; `created_at` is that point-id allocation timestamp. The point is
  publicly nonexistent until verified commit. Object keys and hashes remain private. Orphans, pruning, and
  Environment-deletion remote cleanup are durable and resumable.

  Source adapters own only typed capture/restore format procedures. The
  Agent-side `s3-compatible` Connector adapter owns AWS SDK Put, Get, Head, and
  Delete. The Controller owns policy, scheduling, locking, Recovery Points, and
  retention, and resolves Connector credentials into transient Agent slots; it
  never performs the S3 artifact transfer.

  Connector credential slots use the same bounded
  `(task_id, assignment_id, step_id, purpose)` delivery fence as other task
  secrets. The Agent accepts only the exact active assignment and purpose and
  clears a slot when consumed and again on completion, failure, cancellation,
  or reconnect. Current capture, whether age-encrypted or unencrypted, and
  `BACKUP_PRUNE` receive exactly the S3 access-key and S3 secret-key purposes.
  Capture encrypts with the public age recipient in its sealed plan and prune
  never decrypts, so neither receives a private identity. The current-age
  identity purpose is reserved for current-era restore; the operator-old-age
  identity purpose is reserved for old-era restore. Durable direct or
  Secret-backed credentials are re-resolved on valid redispatch. An
  operator-supplied old age identity is request-only and makes that restore
  non-retryable.

  **Restore is per consumer and per source, through the same adapter.** A
  backup policy enumerates **sources**: each is a dataset with a kind that
  selects its typed procedure (`postgres:16` -> `pg_restore --clean -d <db>`,
  `volume` -> validated same-filesystem exchange, `config` -> hidden batched
  Entry replacement). Recovery points are tagged
  with their source, so restore always knows which adapter and which procedure
  to run. The Console Restore dialog confirms the exact stable `target_id`,
  presents an explicit overwrite warning, calls out downtime for PostgreSQL and
  Volume restores, and accepts an optional old-era age identity. It then streams the adapter's restore steps
  (download -> decrypt -> restore/extract -> verify). A postgres dump and a
  volume archive are never conflated — they restore through different adapters
  to different targets. **Restoring a recovery point encrypted under a
  rotated-away key requires the operator to supply the previously exported
  age identity** — the Restore dialog accepts the exported identity file when
  the environment's current identity cannot decrypt the recovery point.

  Restore pins the exact verified point and its original surviving stable
  target before protected intent; omitted point means latest resolved once at
  that time. The Agent fully downloads, hashes, fsyncs, decrypts, and validates
  the artifact before mutation. PostgreSQL and Volume restores stop consumers
  and declare downtime; Volume alone stages its decoded tree as a hidden sibling
  under the target Volume parent and performs same-filesystem
  `renameat2(RENAME_EXCHANGE)` inside the helper. Config restore uses an
  Environment lock and one read-hidden fence, validates the complete archive
  in two passes, and fully replaces the Environment Entry set by rolling the
  canonical Entry primaries and owner indexes forward in bounded batches.
  Present artifact Entry ids are upserted, omitted Entries are deleted, and
  there is no Environment-wide active-Entry-generation read model. One final
  transaction publishes the preallocated materialization generation and
  removes read hiding. Every procedure performs source-specific post-restore
  verification and never restores to a replacement target.

  **A live data directory is never a backup source.** PostgreSQL and Valkey
  data directories live in backing Environments and are never offered as
  Volume sources. PostgreSQL backup is only the typed consumer-Attach
  `pg_dump` procedure. No live Valkey data-directory archival is authorized;
  the bounded Valkey source procedure and restore proof remain a Gate B delivery
  item, and `strategy.not_implemented` is rejected before Task publication until
  that contract is closed. The source must resolve the namespace-safe
  per-consumer versus shared-instance RDB boundary; no whole-instance RDB is
  claimed as a per-consumer format.

### Microservice communication

A tenant Project such as storefront is a microservice architecture: one Environment
runs many Services, and those Services reach each other and shared state through
named Network Zones, not through published host ports.

- public traffic: Caddy Router -> API and WebSocket endpoints; an independently
  provider-managed Tunnel may reach that router, but Groundplane does not
  author the provider ingress mapping.
- internal data: API, workers, PostgreSQL, and Valkey on the backend zone.
- outbound: workers on the egress zone.
- identity: a private identity zone with an internal status-event proxy back to
  the active API slot.

### Console display goals (inspired by Neon, PlanetScale, Vercel, Coolify, Dokploy)

- A **workspace switcher** at the top of the sidebar — the primary selection
  (Vercel team / Neon org style). It holds **Platform** (the platform
  workspace) and each **Tenant**; the sidebar and every page recontextualize
  to the selection, so you always know which isolation boundary you are
  inside.
- **Activity IS tasks (locked).** There is exactly one record set: **tasks**.
  The activity journal is the same records — every journal entry is a task
  (pending/running/acked → completed/failed/timed_out/aborted, with its
  steps), and every task lands in the journal. The Console renders the
  journal three ways, never a second data model: the **Activity pages**
  (platform + per-tenant) as the scoped journal, the **Tasks tabs** as the
  per-environment live queue, and the **Tasks section on the Platform
  Components page** as the complete Platform-owned workspace journal. Platform
  Activity starts with every Platform and Tenant Task, while the Components
  placement requests `workspace=platform`; neither filters by `target` or
  Component. Platform Activity may narrow by Tenant, Project, or Environment.
  Tenant Activity starts with its Tenant subtree and may narrow by Project or
  Environment. A filter can only narrow the starting visibility. The MVP has
  no target or Component Task index. Every Task always exposes
  **Inspect** as the authoritative detail and action surface. A row's primary
  navigation opens an operation/resource surface only when the immutable owner
  and live typed target resolve: an Environment-owned Task opens that
  Environment's Tasks tab (`?tab=tasks`), while a live Platform Component target
  opens its Component surface. If the target was deleted or cannot resolve, the
  row falls back to the owning Platform Components or Tenant Activity journal;
  that fallback is not presented as the original operation surface. If even the
  owning Tenant label no longer resolves, Inspect remains available without a
  fabricated link. `operation_id` is opaque reference identity and is never
  parsed for routing. Navigation metadata never selects journal membership.
  This is why the Environment tab bar's deep-linkable tabs exist.
- **Task ownership is immutable (locked).** Every Task freezes
  `workspace_type: platform|tenant`, the Tenant id exactly for a tenant
  workspace, and Project/Environment ids when the initiating capability is
  owned at those levels. Tenant-project Environments belong to their Tenant
  workspace; backing-project Environments belong to Platform. Environment and
  descendant actions use that Environment owner, Tenant/Project Runner and
  deletion actions use their initiating Tenant or Project owner, and Agent,
  platform Component/Secret, and Controller maintenance actions use Platform.
  The journal never derives ownership from a mutable `target`. Public Tasks
  carry UTC `created_at`, `updated_at`, nullable `started_at` and `finished_at`,
  plus `actor: operator|system`; the single-token MVP stores no user profile or
  token identity. Retries copy ownership, while abort retains the same Task.
  Workspace and Environment indexes are written atomically with the Task.
  Project pages use a bounded scan of immutable ownership at the same fixed
  revision. All scopes page at one fixed revision; `/activity` is an exact
  `/tasks` list alias.
- The sidebar renders per workspace: the **Platform workspace** shows platform
  nav (Overview, Backing services, Activity, Secret store — platform secrets
  only, Host, Settings); a **Tenant workspace** shows the tenant-level items
  (Projects, Runners, Activity, Settings). Inside a project
  (`/t/<tenant>/<project>/…`) a **Project section** (Environments and Secrets,
  the project-scoped resources) renders ABOVE the tenant items,
  and disappears again on tenant-level pages. No flat pages that mix every
  tenant.
- Inside a tenant, **Projects -> environment tabs** (Render/Neon branch style):
  a project lists its environments, each environment being the topology page
  with the tab bar (Overview, Services, Blueprint, Router, Releases, Tasks, Backups,
  Volumes, Environments, Scripts, Settings last). **Services is a responsive
  list, not a card grid**; each row exposes service identity, runtime intent,
  observed health, image, zones, resources, and backing connections, and opens
  the right-side Service details drawer. Overview topology retains its cards.
  **Releases is the
  per-Service release ledger** (immutable intents and execution summaries plus
  separate serving/current-successful projections — rollback selects exact
  history); **the Tasks tab is the environment's live task queue**
  (pending → running → completed|failed|timed_out|aborted, per-task steps,
  abort for in-flight work) — tasks are environment-scoped, and this tab is
  the Console surface the `task` CLI noun maps to. The **breadcrumb's project
  crumb dropdown always lists the environments** (jump staging <-> production
  from anywhere, including the Secrets page) plus the project's
  Secrets page. Secrets resolve by fallback environment -> project -> platform;
  connectors never inherit because they exist only at environment scope.
- **Forms and Service details open in right-side drawers; centered modals are
  used only for confirmations and other read-only views** — anything taking input (new tenant,
  project, environment, backing service, runner, secret, connector, zone,
  route, attach, backup policy, volume, env entry, script, rename, deploy,
  rollback) slides in from the right; remove/delete/destroy/restore/reveal
  confirmations and other spec views stay centered. Service details use the
  right-side drawer even when read-only; their destructive confirmations remain
  centered.
- The **Platform Overview** is a single management page: all tenants' projects,
  backing services, and platform activity.
- A **dashboard** shows the whole platform at a glance: status, counts, projects
  grouped by tenant, backing services, recent deploys.
- A **zone topology** instead of dense tables: Network Zones as columns, Service
  cards placed inside every zone they join, so microservice communication is
  obvious. Backing services render as attached, dashed cards that link out
  to the backing project, and each service card carries a chip for every attach
  that grants it access (`postgres · api_abcdef`), so the attach is visible
  on the service, not the zone.
- A dedicated **Backing services** area listing backing projects, with
  Neon-style connection cards (consumer, database, role, copyable connection).
  A consumer row is the **triple (environment, service, attach)**: the same
  environment appears once per attaching service, and twice for the same
  service when it holds two attaches — the cards show
  `project / env · service` so usage is never undercounted or misattributed.
- Attach + link out: a tenant Environment shows the backing services its services
  consume inline and links to the backing service page, which lists every
  consumer (per-service-attach).
- Every action is one click: Deploy, Rollback, Restore, Reveal. Service cards
  are clickable: full spec, edit, delete.

### Onboarding / creation flow

The Console supports the full create-to-deploy path in order: **New tenant**
(workspace switcher) -> **New project** (in the tenant) -> **New environment**
(project env tab bar, reserve its network pool) -> **Add zone** (choose an
available child subnet) -> **Add service** (container:
image, zones, healthcheck, resources, mounts, env files, environment, restart,
replicas) -> **Attach backing** (per Service: choose a new credential or an
existing credential-owning Attach; join the backing network and expose the
owner's `pg16_*` facts) -> **Add route**
(public routes never auto-deploy ingress — enable the Caddy / Cloudflare
Tunnel components on the Router tab) -> **Add volume / env entry** (a var, a file, or a secret via the secret flag) ->
**Deploy**
(per-service: pick the service, its new tag — defaults to the current tag for
redeploy — and this release's strategy). Every edit goes through the
reconcile loop and lands in the Activity journal.

## Desired state - the contract format

The Controller understands desired state as a set of YAML documents called the
**Groundplane Blueprint** — the desired-state contract. Each entity is one
schema-validated document. The Blueprint uses Docker Compose as its base
grammar and adds Controller-owned `x-gp-*` extensions instead of rewriting the
Compose grammar. The Controller strips its document envelope, validates the
Compose body and Groundplane extensions, then renders an executable Compose
document and typed Agent procedure.

The Environment **Blueprint** surface shows the Controller's canonical
authoring projection and supports edit, import, export, side-effect-free
validation, and apply. The editable YAML contains decisions only: stable
Environment and Task ids, generated paths and resource names, render
generations, observations, and secret plaintext are absent. The Console keeps
an edit as a local draft until Apply. Its bundle importer retains explicit
root-file selection, reorderable Compose sources, and non-secret interpolation.
The CLI mirrors are `environment blueprint show|validate|apply <name>`; validate
and apply accept `--bundle-dir DIR --root RELATIVE_PATH [--compose-file
RELATIVE_PATH]... [--var KEY=VALUE]...`. `GET
/environments/{id}/blueprint` returns the canonical single-file authoring
projection and its revision, `POST /environments/{id}/blueprint/validate`
returns a typed create/update/retained diff without writing state, and `PUT
/environments/{id}/blueprint` atomically publishes the desired revision and
reconcile Task. Validate and apply require `If-Match` from the loaded Blueprint
revision so a stale editor never overwrites newer desired state.

### Blueprint apply reconciliation

A successful Blueprint apply is one Environment update Task with one operation
id and one Agent assignment carrying the complete sealed execution plan. It is
not a desired-state-only publication: the apply Task is the sole execution for
that apply. It creates no child Deploy Task, hidden Deploy request, second
operation, or new operator action or endpoint.

From the sealed candidate projection, the Controller implicitly selects newly
introduced or materially changed logical Services whose effective
`runtime_intent` is `running`, regardless of replica count, and derives their
candidate Releases solely from that projection. An existing Service whose
intent is `stopped` or `absent` remains stopped or absent: its desired change
is retained for a later explicit Release action and is not implicitly applied
or hooked. A Service that is a member of a Release Group is also excluded from
implicit Blueprint candidate Release and hook execution; Blueprint publishes
its desired change only, while explicit group Deploy/Rollback retains its
declared serial order and `on_failure` policy.

Matching `pre-deploy` and `post-deploy` Scripts are selected against the sealed
candidate Service and applicable Release inputs. Within each phase, Services
follow the sealed dependency topology with current Service slug bytes as the
tie-break, and Scripts for each Service follow current Script slug bytes.
Manual Scripts never execute during apply. Exact reapply, or an apply with no
selected changed candidate, executes no hooks. The complete selection across
pre-deploy, post-deploy, and possible `on-failure` execution is limited to 16
hooks and 1,048,576 aggregate UTF-8 body bytes; phases do not receive separate
budgets, and a violation is rejected before Task publication.

The one Agent assignment executes these phases in order:

1. materialize the sealed environment files and file entries;
2. ensure the candidate Volume leaves, perform required Attach adapter
   provisioning and grants, and prepare Network resources, without applying
   those memberships to a consumer Compose Service;
3. execute all selected pre-deploy `RunScript` steps, requiring
   `cleanup_proven` after each before the next starts;
4. only then apply each targeted consumer's prepared Network memberships as
   part of the candidate workload Compose apply and start that candidate,
   without a readiness gate or an earlier hidden consumer start or recreate;
5. execute all selected post-deploy `RunScript` steps with the same cleanup
   barrier;
6. execute `WaitHealthy` for each candidate;
7. execute the sealed registered Component actions.

Compose apply does not intrinsically wait; readiness occurs only when the
typed plan reaches its `WaitHealthy` step. Only after the assignment succeeds
does the Controller perform the Controller-only atomic promotion. That
transaction promotes the candidate Releases, applied Environment projection,
Component state, and Routes together, then records the parent Task success.
Before promotion, a failure must prove exact predecessor restoration for each
selected Service that previously served, and exact first-candidate absence for
each selected Service that did not. A configured-only Service in an applied
Environment projection is not a serving predecessor. Mixed recovery preserves
unrelated runtime and the exact applied projection. The desired head is never
rolled back, and no failed or unproven candidate may be published as a serving
Release or Route. If the proof is not available, the Task remains
nonterminal/recovery-required rather than claiming success or false serving
state.

A pre-deploy hook failure therefore performs no application candidate workload
mutation and leaves the serving and current-successful projections unchanged.
Its runner cleanup is proven before `on-failure` begins. Earlier file
materialization, Volume, Attach, and Network effects remain accounted resource
effects; the Task must not claim the whole assignment made no mutation.

Retry transfers the same operation only while every selected Script execution
is durably `not_started`. If any Script reaches `start_authorized` or later, or
its state is unknown, the existing retry action returns `script.retry_unsafe`;
recovery continues the authorized execution instead of starting a replacement.

The existing Task detail step projection records the captured non-secret Script
id and Script slug for every selected `RunScript` step. Existing events carry
that step's `step_id` transitions, and the same parent Task detail carries the
terminal success or failure. This durable evidence joins each selected Script
to its parent Blueprint Task without exposing bodies or secret values.

Blueprint input is a closed bundle: one root envelope, ordered Compose source
paths, a closed relative file namespace, and an explicit non-secret
interpolation map. The Controller never reads an implicit `.env`, process
environment, submission-host path, symlink, network resource, or unlisted
file. Its manifest lists `{path, part, size, sha256}` in path order; binary
parts are deterministically named `file-000001`, `file-000002`, and so on.
A bundle has at most 64 files, 256 KiB per file, 768 KiB total bytes, and
240 UTF-8 bytes per normalized relative path. Parsing also permits at most
100,000 aggregate YAML nodes, 1,000 aliases, resolution depth 16 across
aliases/includes/extends, and 512 resolved Compose resources. Limit failures
return `validation.failed` with HTTP 422 before any write or Task.

Normalized records are the reconciliation source of truth. The immutable
submitted bundle generation is retained for audit and deterministic reparse.
Canonical Compose, generated materializations, and every
secret-bearing artifact are regenerated and remain ephemeral. Native bind
sources are accepted only for bundled non-secret content materialized read-only
below the stable environment directory; arbitrary or writable host binds are
rejected.

| Document | Content |
| --- | --- |
| tenants.yaml | Tenants |
| projects.yaml | Project registry: tenant vs backing |
| backing/<name>.yaml | Backing projects (one environment, one adapter-backed service): adapter + image, host paths, **contract (prefix + exposed env vars + provisioning actions)**; consumer environments own backup policies and keys |
| environments/<tenant>/<project>/<env>.yaml | The topology: zones, services, backing attaches, routes, components, volumes, secret references, policies |
| connectors/<tenant>/<project>/<environment>/<name>.yaml | Environment-owned storage destinations (s3-compatible, etc.) |

**The spec grammar — see `docs/blueprint.md`.** Every spec file opens
with a header: `kind` + `schema` as top-level fields, then a `metadata`
section holding the **recreate keys** (tenant / project / environment —
the operator-chosen names, scoped-unique, never ids). The one law:
a spec is a pure input of decisions; anything the Controller can derive
(volume_dir, env file names, the pinned router IPv4, rendered artifacts)
is never hand-authored in it. Controller-owned release projections and
`x-gp-*` execution metadata may appear in generated output, but are
regenerated from durable records. Placement: the document lives at the
path of its recreate triple and the header must match it (a mismatch is
rejected at write). Renaming is an explicit id-preserving operation, not
a new resource.

Every Environment desired mutation derives one complete lossless normalized
revision from a pinned prior head. The Controller privately stages immutable
`GDR1` audit and projection chunks, seals their integrity root, and then
publishes only the new head, Task, and replay authority. There is no singleton
current projection or flat desired-record authority. The encoded normalized
projection, including durable schema and framing, must fit exactly 2 MiB.
Exceeding that ceiling returns `validation.failed` before staging, Task
publication, or any host effect.

An environment document and its mapping to Compose:

| YAML key | Compose / Agent meaning |
| --- | --- |
| kind / schema / metadata | the Controller envelope; stripped before Compose parsing |
| standard Compose keys | the base topology grammar; Groundplane preserves their Compose meaning |
| `x-gp-*` | Controller extensions; validated by the Controller and compiled to Compose fields or Agent tasks |
| `x-gp-network-pool` | required Environment IPv4 reservation; an operator decision, never a Docker network |
| networks.<name> | native Compose network definition with exactly one required child subnet and immutable `internal`; the Console calls this a network zone |
| services.<name>.networks | native Compose network membership (a service on two zones is a bridge) |
| services.<name>.healthcheck | the container healthcheck: `http <path>` (curl), `tcp <host:port>` (socket connect), or `pgrep <cmd>` (process alive), with `interval` / `timeout` / `start_period` / `retries` |
| services.<name>.resources | `mem` / `cpus` limits (`mem_limit` / `cpus`) |
| services.<name>.command | entrypoint/command overrides (workers: `artisan queue:work`, WebSocket: `websocket:start`) |
| services.<name>.environment | **service-scoped env vars** (created with exposure: this service) — layered over the environment's inherited all-services file; wins on name conflict |
| services.<name>.x-gp-release.default_strategy | the service's **declared default** deploy strategy; the Deploy action chooses the strategy per deployment (`blue-green` and `recreate` implemented; `rolling` is rejected in the MVP) — the spec default is overridden at deploy time, never rewritten |
| services.<name>.env_file | generated env files attached to THIS service; the canonical all-services file is explicitly attached to every service |
| services.<name>.mounts | native Compose volume/file mounts; Groundplane-managed file entries use native `secrets` or `configs` grants when their semantics fit |
| services.<name>.aliases | per-zone network aliases (blue/green: `storefront-{slot}-api`, WebSocket: `app-websocket` / `storefront-websocket`) |
| services.<name>.depends_on | ordering, including health conditions (`identity-portal -> identity-clamav: service_healthy`) |
| services.<name>.expose | internal ports reachable on the zone (`cms:3000`, `websocket:8080`) |
| services.<name>.restart | restart policy (`unless-stopped` / `always` / `no`) |
| services.<name>.logging | json-file log limits (`maxSize` / `maxFile`) |
| services.<name>.deploy.replicas | native Compose instance count (WebSocket horizontal scaling); omission normalizes once to one, then every durable Release carries a positive explicit sealed count that the Controller persists and reconciles against Compose's ordinary containers |
| services.<name>.labels / annotations | native container identity and supplemental metadata; Groundplane emits `com.groundplane.*` labels and does not use `deploy.labels` for local ownership |
| environment.volume_dir | the environment's controller-managed volume folder; generated from stable ids |
| `x-gp-entry` | authored environment variables/files with `exposure: [all]` or a service list; the Controller renders all-services entries into the canonical env file and service entries into native `environment`/`env_file` |
| files.<name> | plain (non-secret) file entries: materialized at `<volume_dir>/<path>`, exposure per service |
| `x-gp-attachments` | attach to a Backing Service per **Service**, keyed by the Attach's **name** (`api-db`, an operator decision unique per Environment): `service` is singular; `credential.mode` is `new` or `existing`; existing names one credential-owning Attach in the same Environment and Backing Service; the backing network join is a consequence; only a new credential provisions `<service-name>_<first-6-of-attach-id>` and may declare `grants` |
| routes[].exposure: public | remains valid desired state without an enabled router and reports `unserved`; Caddy serves it when the `http-router` capability is enabled |
| `x-gp-components.http-router` | environment-owned HTTP entry point, off by default; selects the registered `caddy` implementation for MVP and uses ordered portable `settings.zone_ids`, first Zone primary |
| `x-gp-components.http-router.implementation_config.caddyfile_template` | optional editable Caddy implementation template with exactly one `{routes}` marker; validated as a complete file before reload |
| `x-gp-components.edge-tunnel` | environment-owned outbound tunnel connector, off by default; selects `cloudflare-tunnel` and stores ordered `settings.zone_ids` plus `settings.secret_id`; the selected Project or platform env-var Secret is materialized only as cloudflared's `TUNNEL_TOKEN`; Groundplane starts/stops the connector and reports health but does not configure DNS, public hostnames, ingress rules, origin targets, or protocol |
| secrets.env_file | generated secret files the Agent materializes on the host (0600), referenced explicitly through service `env_file:` |
| volumes.<key> / `x-gp-slug` | the immutable Compose key plus an optional mutable Groundplane slug; the Controller renders a managed local Docker Volume backed by the Environment-owned path |
| x-gp-scripts.<reconciliation-key> | per-environment Scripts: `{ slug, service, script, when }`; the map key is immutable reconciliation identity, `slug` is renamable, `script` is the full body, and `when` is `manual`, `pre-deploy`, `post-deploy`, `pre-rollback`, `post-rollback`, or `on-failure` |
| backups / retention | Controller scheduling |

**Scripts.** Scripts are a first-class per-environment concept (like volumes,
secrets, and the router): `{ slug, service, script, when }` under one immutable
`x-gp-scripts` reconciliation key. The `script` field holds the **full script
  body** - one line or many, entered in a multi-line editor - executed against a
  logical Service in a task-scoped one-off container. Scripts double as
**hooks**: pre hooks finish before candidate mutation; post hooks run after the
candidate is applied and started but before readiness observation and the
strategy's proxy switch or recreate acknowledgement. This permits a
migration-dependent healthcheck without deadlocking the Release.
`pre/post-rollback` plus `on-failure` hooks cover rollbacks and failed
deploys/rollbacks. One Blueprint apply publishes Script desired state and
  implicitly selects only a newly introduced or materially changed logical
  Service whose effective `runtime_intent` is `running`, regardless of its
  replica count; stopped or absent existing Services retain desired changes for a
later explicit Release action and are not implicitly applied or hooked. Release
Group members are excluded from implicit candidate Release and hook execution;
explicit group Deploy/Rollback retains declared serial order and `on_failure`.
For the remaining selected Services, the apply creates candidate Releases from
the sealed candidate projection and executes matching `pre-deploy` and
`post-deploy` Scripts in the same Environment update Task, operation id, and
Agent assignment. Within each phase Services run in dependency-topological
order with slug-byte ordering as the tie breaker; each Service's Scripts run in
slug-byte order, once per selected logical Service Release rather than once per
replica. Exact reapply, or an apply with no selected changed candidate, runs no
hooks. Manual Scripts do not execute during apply. The complete selection
across pre-deploy, post-deploy, and possible `on-failure` execution is limited
to 16 hooks and 1,048,576 aggregate UTF-8 body bytes, rejected before Task
publication; phases do not receive separate budgets, and the bounds count
logical hooks, not replicas.

The sealed Blueprint plan orders materialization; managed-Volume ensure; Attach
adapter provisioning and grants plus Network resource preparation without
consumer Compose membership application; all pre-deploy `RunScript` steps with
a cleanup barrier after each; then targeted consumer Network-membership and
candidate workload Compose apply/start without readiness; all post-deploy
`RunScript` steps with the same barrier; `WaitHealthy`; and Component actions.
There is no hidden consumer Compose start or recreate before the barrier, and no
candidate workload mutates before every selected pre hook is clean. This
Blueprint-only split does not change standalone Attach or Detach ordering.
Success atomically promotes the candidate
Releases, applied projection, Components, and Route observations. Failure never
rolls back the desired head and is accepted only after proving exact predecessor
restoration or first-candidate absence; it never publishes a false serving
Release or served Route. Task detail and events expose non-secret durable Script
identity and terminal metadata linked to the parent Blueprint Task. Retry may
transfer the operation only while every selected execution is durably
`not_started`; `start_authorized` or an unknown state rejects with
`script.retry_unsafe`. ADR 0040 owns execution, cleanup, and retry. ADR 0062
owns prepared immutable-input reference generations and their bounded release.

Blueprint candidate completion uses one closed terminal transaction (ADR 0067),
separate from desired-state publication. Its complete physical request must fit
256 operations per comparison/success/failure arm and 1 MiB before Script
source release begins; ordinary transactions and release batches retain their
existing limits. Source closure stores the original terminal report with exact
Task and assignment authority. After interruption, the Controller finishes that
report without redispatching closed hooks or advancing the execution epoch,
including at the maximum positive epoch. Conflicting reports or new events
cannot change closing execution authority. Final completion atomically removes
the temporary report and source root with the ordinary terminal state and
receipt. Unknown commit status requires exact durable terminal replay, never
inferred success from workload health or fabricated reports for old Tasks.

TLS-first setup uses this generic contract: a project-authored `pre-deploy`
Script runs `/bin/sh` plus `openssl` from its target Service's externally built,
selected host-local image as the sealed numeric Service user. It stages,
validates, sets ownership and modes, and atomically publishes a certificate
bundle into a declared read-write Volume; consumers mount that Volume read-only,
and Entries do not own the same output subtree. An existing valid bundle is an
author-owned idempotent no-op. Groundplane does not supply tools, assume they
exist in every image, validate PKI policy, or guarantee arbitrary Script output
atomicity. Live automatic certificate rotation is absent; a future rotation
requires an explicit operation and separate contract.

Every prepared, active, retry-open, or releasing Script execution holds exact
references to its body, runner snapshot, Service, Release, Networks, Volumes,
Entry value generations, reusable Secret values, and materialization proof
generations. Removing any referenced source returns `resource.in_use`; an edit
may publish a new immutable generation but never overwrites or prunes the
referenced generation. This active-operation fence is distinct from an ordinary
late-bound desired Secret reference, which remains non-blocking.

Every Script runner consumes the applicable sealed Release's exact
`local_image_id` and Service definition plus typed env/file Entry bindings from
one fixed-revision projection. A newly authored candidate requested reference
is resolved through the Agent once before publication. Manual runs use the
Release selected by `serving_release_id`; active
Deploy, Rollback, and Blueprint apply plans use their exact sealed candidate or
predecessor Release and candidate or applied projection. Private assignment
artifacts carry the exact pinned Entry-generation bytes. Historical, retry,
recovery, and failure paths validate their stored local id directly; no runner
re-resolves a tag or discovers identity from a Compose result. The Controller
durably acknowledges
`start_authorized`, `body_prepared`, `container_created`, `outcome_recorded`,
and `cleanup_proven`; an Agent reconnect resumes that execution rather than
starting a replacement. Recovery may inspect only the deterministic runner
name before an id is captured, then uses only the validated immutable container
id. Terminal reasons are exactly `normal_exit`, `start_failure`,
`runtime_failure`, `timeout`, `abort`, `abort_before_start`,
`expiry_before_start`, `no_serving_release`, `parent_failure_before_start`, and
`recovery_invariant_failure`.

**Labels & reconciliation.** The Controller owns desired state and reconciles
it. Every container Groundplane manages is labeled
(`com.groundplane.managed=true`, stable owner/resource ids, release, slot,
plan id, and render generation); labels are the ownership contract. The Controller watches etcd
(watchers) and restores drift on labeled containers — a managed container
stopped or edited by hand is converged back to desired state; unlabeled
containers are never touched. Reconciliation runs on desired-state change,
task completion, and a periodic tick.

**Stable identifiers (locked).** Every entity that can be referenced has a
stable, static id — **never derived from a name or slug, and never a slug
itself** (mock fixtures included). Ids are **ULID-style**: a 48-bit
millisecond timestamp prefix makes them chronologically sortable (etcd
ranges, deploy history, and the activity journal order for free) plus 80
bits of CSPRNG randomness (collision-safe); a short kind prefix keeps them
readable (`tnt_…`, `prj_…`, `env_…`, `svc_…`, `dep_…`). Tenant and Project
slugs are URL labels derived hyphenated from their display names. An
Environment instead has one scoped-unique `name` that is both its human and
URL label; it has no second display name or slug field. All of these labels
are **freely renamable** — nothing references one, so rename never breaks
anything:

| Entity | id | notes |
| --- | --- | --- |
| tenant | `tnt_…` | URL uses the slug (globally unique); data uses the id; slug renamable |
| project | `prj_…` | URL uses the slug (unique within the tenant); data uses the id; slug renamable |
| environment | `env_…` (generated) | `name` is its only human/URL label (unique within the project); there is no separate slug — rename never breaks references |
| service | `svc_…` | the name stays the compose label (unique within the environment) |
| env var entry | `ev_…` | key = the label the container reads; id = the reference (secrets are entries with the secret flag) |
| deployment | `dep_…` | deploy history + rollback reference deployments by id |
| volume | `vol_…` | mounts keep referencing by name; the Docker volume object is `gp_vol_<volume-id>` |
| script / attach / route / secret / connector / runner | `scr_…` / `att_…` / `rte_…` / `sec_…` / `con_…` / `run_…` | every referenced entity carries `<kind>_<ulid>` |
| backup source / recovery point | `spt_…` / `rp_…` | sources are one-kind-per-prefix too: `spt_` = configured source, `rp_` = recovery point produced by a run |
| component | `cmp_…` | one id space for both owners (environment and platform) — `owner` on the record disambiguates, the id shape doesn't |
| network | `net_…` | the Compose network name is `gp_net_<net id>`, so it doubles as the id in its own generated name |
| task / operation / plan / step | `task_…` / `op_…` / `plan_…` / `step_…` | `operation_id` is stable across retries of the same logical action; `task_id` is per attempt (`retry_of` points at the prior `task_…`) |
| config (materialized file) | `cfg_…` | the on-disk materialized path embeds the id: `/var/lib/groundplane/materialized/cfg_…` |

The CLI mirrors the Console: it accepts Tenant/Project slugs and the
Environment name for humans (resolved server-side within the scope chain —
tenant → project → environment), and ids with `--id` for scripts. Human
labels are scoped-unique, so resolution is never ambiguous.

Every entity's id follows the same shape: `<kind>_` + 26-char ULID (`tnt_`,
`prj_`, `env_`, `svc_`, `dep_`, `ev_`, `vol_`, `att_`, `rte_`, `sec_`,
`con_`, `run_`, `scr_`, `spt_`, `rp_`, `cmp_`, `net_`, `task_`, `op_`,
`plan_`, `step_`, `cfg_`, …). No hyphens, no shas, no slugs — the
delimiter is always an underscore, and the ULID body is the same
timestamp+randomness scheme everywhere. This table is the canonical list —
a new entity gets a new row here in the same commit that adds it, not a
guessed abbreviation. Mock fixtures use static ULIDs, not generated ones.

**Environment-scoped volumes.** Every Environment owns a Controller-managed
`volume_dir`, for example
`/var/lib/groundplane/vol/<tenant-id>/<project-id>/<environment-id>`. Stable ids
define that directory, so Tenant, Project, and Environment label changes do not
move it.

Every Volume has three separate identities. `id` is an immutable `vol_` ULID.
`slug` is a mutable Environment-scoped label. `key` is the immutable authored
Compose top-level key and direct child leaf. `name` is not a Volume field or
alias. The slug uses the ordinary 1-through-63 lowercase scoped-slug grammar.
The key is 1 through 255 ASCII bytes from `[A-Za-z0-9._-]`, is neither `.`
nor `..`, and contains no separator or NUL. The slug remains reserved through
deletion finalization, and the key remains reserved for the Environment
lifetime, including retained replay targets. The derived path is always
`<environment.volume_dir>/<volume.key>`, and
the Docker object is `gp_vol_<volume-id>`. Neither path is accepted as input.

Native Compose mount sources resolve the immutable key to the stable Volume id.
The normalized desired revision stores `volume_id`, target path, and read-only
state for each Service mount. It also retains the key for reproducible Compose
rendering. Slug edits therefore do not rewrite a mount, a Backup source, a
Docker object, or host data. The Agent preserves each authored mount target and read-only flag, including
shared application storage and ordinary mounts used by WebSocket Services.

The Agent creates a Volume as a private same-filesystem sibling, records an
exclusive ownership marker, fsyncs it, and publishes the leaf with
`renameat2(RENAME_NOREPLACE)`. A managed leaf starts as root-owned mode `0755`
beneath the private root-owned mode `0700` Environment directory. The helper
does not adopt an existing leaf or repair an ownership or mode mismatch. A
container that needs another data owner initializes its mounted data explicitly.
Descriptor-relative traversal never follows a symlink or crosses a mount
boundary.

Add publishes the desired identity, `creating` runtime projection, Task, and
replay response atomically through the Environment desired head. Edit changes
only the slug and still publishes a revision-bound Task. Remove first returns
the complete fixed-revision dependency impact in bounded pages. Destruction
requires the final impact token and the immutable key, detaches all consumers,
and then removes at most 128 directory entries per 30-second helper call.
Create and removal checkpoints make retries resume the same operation. Create
uses `intent_acknowledged`, `private_sibling_ready`, `leaf_published`,
`marker_removed`, and `runtime_active`. Removal uses `intent_sealed`,
`revision_staged`, `desired_published`, `consumers_detached`,
`directory_absent`, and `runtime_finalized`. The API, CLI, and Console have
the same list, detail, add, slug-edit, impact, and confirmed-remove operations.
There is no `--force`, implicit empty-directory exception, or browser-side
rewrite.

A current Backup source stores the Volume id as `target_id`. Volume removal
removes that source from active policy selection but preserves historical
Recovery Points and their Connector fences. Removing the final active source
disables scheduling while retaining the configured policy.

**No host port
publishing**: all service ports are `expose` (internal to the zone); ingress
goes through the enabled components only (Caddy, and optionally the Cloudflare
Tunnel edge) — with neither enabled, everything is internal/loopback. **The
single documented exception is the Caddy entry router** when its component is
enabled: a statically pinned IPv4 on the frontend bridge network (its
internal names served by the host DNS resolver) — reachable by the host by
default; LAN reachability is the operator's own networking.

**Environment entries: vars, files, secrets — one model (locked).** The
environment page has ONE tab for environment-level configuration (merged from
the old Environments + Secrets tabs). Every entry is created with three
choices:

- **Type** — an **env variable** (key=value) or a **file** (name, path
  relative to the environment's volume folder, content, and explicit numeric
  `uid`/`gid`; ownership is never inferred or defaulted).
- **Exposure** — **all services** or **a specific service**. All-services
  vars land in `env_vars`; service-scoped vars land in that service's
  `environment` (wins on conflict). Files carry the same exposure marker and
  materialize inside `<volume_dir>/<path>`.
- **Secret flag** — a checkbox that decides which side of the storage line
  the entry lives on: **plain** (value sits in desired state, visible in the
  Blueprint — `APP_ENV: staging`) or **secret** (value lives only in the
  Controller's encrypted value store; desired state holds just the Entry and
  its source reference; masked + reveal-on-demand; materialized at 0600;
  rotatable).
  Masking is a symptom of storage, not the definition: the difference is
  where the value lives and how it is materialized. A **file** entry works
  the same: plain config file (desired state) vs encrypted file secret.

A secret environment value never creates a second public Secret resource.
The Entry owns one subordinate encrypted value, revealed through
`GET /entries/{id}/value`; deleting the Entry atomically deletes that
ciphertext. Project and platform Secret resources are separate reusable
fallback inputs, revealed through `GET /secrets/{id}/value`. Neither value
endpoint has a CLI command.

Every Entry edit creates one immutable `cfg_...` value generation. Durable
Tasks reference that exact generation, so a retry or Controller restart cannot
observe a later edit. Plain generations remain desired-state plaintext with a
verified digest; secret generations contain only Controller-key ciphertext and
envelope metadata. Reusing a generation id with different bytes is rejected.

The Environment Entries card also provides an atomic bulk editor for literal
environment variables. It accepts one `KEY=value` pair per line, splits only
on the first `=`, preserves empty values and additional `=` characters, and
ignores blank lines and lines whose trimmed form begins with `#`. The operator
chooses one explicit plain or secret storage class and one common exposure for
the submission. Matching keys retain their stable Entry ids and are updated;
new keys are created; omitted Entries are unchanged. Duplicate or invalid keys,
an incompatible existing storage class, or any invalid value rejects the whole
submission before the Environment desired revision advances.

A service layers its own `environment` on top of the canonical all-services
env file plus the env files it attaches (deterministic
`secrets/.env.<environment-id>` and per-service files for secrets; the
entries are attached explicitly through Compose `env_file`, never implicitly
injected by Compose).

**Secrets are attached per service.** The canonical all-services file is
explicitly attached to every container; service-specific files are attached
only to their exposed service. A service reads the env file(s) it is attached
to plus its own `environment`. Compose supports a **list** of env files
(`env_file`), so a service can attach several
(e.g. the app env file and a shared one). The environment page's tabs sit
together: **Volumes** lists the named volumes and which services mount them,
**Environments** (formerly Environments + Secrets) shows every env entry —
vars, files, and secrets with their exposure — and which services read which
env file.

**Deterministic env files per environment (locked).** Every environment owns a
deterministic env file, `secrets/.env.<environment-id>` (for example
`secrets/.env.env_01J...`), that holds the environment's all-services set —
the `env_vars` plus every env secret created with **all services** exposure.
The Agent materializes it as one physical file and the Controller attaches it
explicitly to **every** service's `env_file` list. It is stable across label
renames. All-services secrets are never staged anywhere else; staging and
production never share a file, so values cannot collide. An env secret created
with a **specific service** exposure lands in
`secrets/.env.<environment-id>.<service>`, attached to that service only.
The environment-scoped file is the only per-environment secret file;
project and platform scopes use their own fallback files
(`secrets/.env.<project-id>`, `secrets/.env.edge`). Compose's default `.env`
file is for interpolation; Groundplane uses explicit service `env_file`
entries for container injection.

**Secret kinds.** Both environment Entries and reusable project/platform
Secrets support exactly two value kinds. The Console's **New secret** flow on
a project or platform Secret Store creates either kind; on the environment
page the same two kinds live inside the unified Entry dialog and are
differentiated by the **secret flag**:

- **env variable** - a key=value entry that lives in an env file
  (`secrets/.env.<environment-id>`, `secrets/.env.<project-id>`,
  `secrets/.env.edge`, the `secrets.env_file` reference). This is what the
  app's containers read via `env_file:`. The tunnel token, R2 keys, and backup
  age recipient are env variables in `secrets/.env.edge`
  (`R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`,
  `BACKUP_AGE_RECIPIENT`).
- **file secret** - a whole file (age private keys, TLS certs/keys, CA bundles)
  referenced by its materialized path (`config/identity-tls/ca.pem`), stored
  encrypted and written by the Agent to that path at 0600.

Each entry's ref in the Console reflects its kind: env vars show their env
file, file secrets show their path. The project-scoped Secrets page and the
platform Secret store always create secrets; only the environment page has the
plain/secret choice.

Reusable Secret metadata is `{id, scope, project_id?, key, kind, ref,
updated_at}`; its value is write-only on create and revealed only through the
typed value endpoint. Env-var refs are Controller-derived from stable ids:
`secrets/.env.<project-id>` for Project scope and `secrets/.env.edge` for
platform scope. Keys are unique within a scope. A key lookup resolves the
Project before platform fallback; a stable `sec_...` reference selects that
exact in-scope Project or platform record. There is no stop-propagation marker.

**File secret paths are volume-relative (locked).** A file secret's path is
**always relative to the environment's volume folder** (`environment.volume_dir`
= `/var/lib/groundplane/vol/<tenant-id>/<project-id>/<env-id>`): the Agent materializes
`config/identity-tls/ca.pem` at `<volume_dir>/config/identity-tls/ca.pem`.
File secrets therefore live in the **same dedicated folder the environment uses
for volume mounting** — isolation by construction (they can never traverse
outside it), and backups/cleanup stay trivial (archive the folder). A
project/platform-scoped file secret is inherited the same way: each
inheriting environment materializes it inside its own volume folder.

Understanding = validate then reconcile:

Blueprint replacement never implies deletion. An existing owned resource
omitted from the submitted Blueprint is retained and appears as `retain` in the
validation diff. Destruction is available only through that resource's
explicit protected Remove action. Renames are never inferred from one removed
and one new name.

1. **Validate** - a typed schema; malformed or unknown keys are rejected at
   write time in the Console.
2. **Render** - desired state renders to Compose configuration for the Agent.
3. **Reconcile** - diff desired vs Agent-reported observed state; apply; Agent
   reports back; converge.
4. **Actions are procedures over this loop** - Deploy, Rollback, Backup,
   Restore, Attach, Run, Logs are baked-in step sequences the Controller
   sequences and the Agent executes.

### Compose extensions and state translation

The Blueprint is a Compose superset, not a second container grammar. Native
Compose keys are parsed by the Compose implementation. Groundplane-specific
behavior uses `x-gp-*` extensions. Compose ignores those fields; the
Controller does not.

The Controller owns the semantic interpretation of extensions. The Agent and
Controller share the execution contract, but the Agent receives a compiled
procedure and does not independently resolve desired state, secrets, facts,
releases, or cross-project dependencies. Execution metadata may be included
in the generated Compose document for validation, but the Agent applies only
the typed procedure it was assigned.

The extension families are:

| Extension | Controller meaning | Execution result |
| --- | --- | --- |
| `x-gp-resource` | stable resource identity and ownership | Docker labels and task context |
| `x-gp-release` | authored default strategy and failure policy | immutable candidate Release, sealed local image id, aliases, and serving/current-successful projections |
| `x-gp-release-groups` | named explicit coordinated releases with exact service membership, order, and failure policy | one task, lock, and per-service release records for the group |
| `x-gp-network` | stable network identity and ownership | native Compose network definition or external join |
| `x-gp-attach` | backing project, attach, grant, and fact references | external network join and adapter tasks |
| `x-gp-fact` | computed adapter fact source | service env, env file, or generated file |
| `x-gp-entry` | environment variable/file source and exposure | `environment`, `env_file`, or mounted files |
| `x-gp-exposure` | service/all-services visibility | generated env-file membership |
| `x-gp-depends_on` | dependency between services in one environment | native Compose `depends_on` where possible |
| `x-gp-requires` | prerequisite resource or task, including another Compose project | Controller task DAG |
| `x-gp-route` | provider-neutral route target and ingress behavior | selected HTTP-router capability state |
| `x-gp-components` | registered implementation choices, typed capability config, and enabled decisions | validated Component Capability intents and Groundplane-owned resources |
| `x-gp-backup` | backup policy and selected source references | scheduler state and Agent backup tasks |
| `x-gp-execution` | generated render generation and execution procedure | Agent validation and application |

`x-gp-depends_on` is limited to services in the same environment and may
compile to Compose health conditions such as `service_healthy` or
`service_completed_successfully`. `x-gp-requires` is Controller-level and
can cross Compose files and projects. It is never incorrectly represented as
cross-project Compose `depends_on`. Its MVP grammar is closed: the root value
is a list whose items require a `backing-attach` target kind and label,
`exists`, `ready`, or `completed_successfully`, plus a non-empty subset of
`start`, `deploy`, `rollback`, and `always`. The Controller resolves the
label once at a fixed pre-stage revision, seals both the authored label and
stable Attach id in the desired projection, and reconstructs retries from that
sealed id without re-resolution. Requirements are edges in the same Environment
Blueprint Task DAG; missing, unsupported, duplicate, self, and cyclic
requirements fail before staging, and deterministic order is stable target ids
then stable step ids. Condition observation and gating remain Controller-owned;
the Agent never resolves labels.

The Controller translates the documents into these state categories:

| Category | What it contains | Source of truth |
| --- | --- | --- |
| desired | lossless normalized services, networks, volumes, entries, routes, attaches, and policies | the immutable revision selected by the Environment desired head in etcd |
| durable records | ids, service `runtime_intent`, generated credentials, facts, releases, recovery points | Controller records in etcd |
| render plan | Compose files, env files, network joins, router files, task DAG | ephemeral Controller output |
| observed | containers, local image ids, health, networks, files, router state | Agent reports |
| task | pending, running, completed, failed, timed out, aborted | task journal in etcd |

Task progress is a bounded durable journal, not an output stream. Every public
Task event contains exactly `sequence`, `step_id`, `state`, `attempt`,
`ordinal`, and `received_at`; Agent payloads, diagnostics, output chunks,
etcd revisions, and storage keys are never exposed. `task events` and the
Console consume the same sequence-resumable SSE endpoint. Its event id is the
positive decimal journal sequence, `Last-Event-ID` is parsed strictly, replay
starts after that sequence without duplicates, and a terminal Task is drained
at one authoritative revision before the stream closes. The Console may use
events to refresh promptly, but the Task detail snapshot remains the source of
truth for displayed state.

The final workload Compose `image` is the exact local Docker image id from the
Release selected by `serving_release_id`. A deploy or rollback publishes a new
per-service Release; the renderer regenerates the sealed local image id, slot
aliases, labels, and router targets. Requested image name and tag remain Release
provenance, not runtime authority. The environment never has one global Release
state: Release history and serving/current-successful projections are per
Service.

Attach state is also split. Desired state contains the Backing Service, Attach
name, one consumer Service, credential mode and source, and owner-only grants.
Durable records contain the Attach id, Service id, resolved backing network id,
and credential-owner Attach id. Only credential owners contain generated
database/role/password and adapter facts. Rendering adds the union of all
Attach membership edges as external networks on consumer Services. New
credentials provision before the consumer deployment may proceed;
existing-credential Attaches perform only network reconciliation. Detach is a
task that always removes the consumer membership and, for an unreferenced
credential owner, revokes grants and deprovisions through the adapter. It is
never a plain record deletion.

Fact values are never auto-injected merely because an attach exists. A typed
fact mapping chooses the destination: a service `environment` value, the
canonical all-services env file, a service-specific env file, or a generated
file. Secret fact values are resolved only during Controller materialization
and are written by the Agent at mode `0600`.

### Rename and materialization invariants

Tenant, Project, and Environment labels are renameable. The Controller
performs a rename against the stable id and atomically replaces only the
target's scoped slug index for a Tenant/Project or scoped name index for an
Environment. Human-facing URLs and hierarchy paths are derived from the
current stable-id-owned records at presentation time; rename does not rewrite
descendants, desired state, or generations. Tenant/Project display names are
also unchanged.

The following are id-based and do not change on rename:

- environment volume directory;
- Compose project name;
- Docker network and ownership names;
- canonical environment env file;
- backup object identity and existing recovery-point keys;
- secret, attach, release, task, and activity references.

The canonical all-services file is
`secrets/.env.<environment-id>`. It is explicitly attached through
`env_file` to every service. Service-specific files use the same stable
environment id plus the service name. Compose's default `.env` file is not
used as an implicit container injection mechanism; all container env files
are explicit.

### Required Controller cases

The following cases are part of the MVP state model and must have tests at
the Controller/Agent boundary:

| Case | Required behavior |
| --- | --- |
| apply twice | same ids and durable generated values; no duplicate resources |
| edit service | validate, render, and reconcile; unchanged release tag may be redeployed |
| deploy | per-service release record, strategy validation, health gate, router switch only after health |
| rollback | select the previous successful different tag for that service; never reverse migrations |
| deploy failure | preserve observed state and failure record; no false active release |
| attach | validate running backing service, provision, join external network, publish facts |
| manual attach | join network only; no database, role, password, facts, or Groundplane backup |
| detach | task-ordered revoke/deprovision/network removal; consumer index updated |
| dependency cycle | reject before any side effect |
| missing dependency | reject before any side effect |
| incomplete render | service environment, files, logging, attach references, backup sources, and router settings must survive Blueprint-to-Compose translation |
| volume add | atomically publish stable id, slug, immutable key, create Task, runtime projection, and replay response before any host effect |
| volume slug edit | preserve the key, path, Docker object, Service mount ids, and Backup target ids |
| volume remove | display a complete fixed-revision impact, require its final token plus immutable key, detach consumers, and resume bounded descriptor-relative destruction |
| volume create recovery | resume only from matching ownership-marker evidence; never adopt an unexpected leaf |
| env exposure change | atomically regenerate files and remove stale service references |
| backup key rotation | rotate the environment age key for future backups; credential and secret rotation remain deferred |
| rename | preserve ids, owners, descendants, and data; derive presentation paths from current labels without changing physical identity |
| zone removal | remove network membership, warn about backing consumers, and render the no-zone column when needed |
| route validation | keep hostname and path/pattern typed separately; reject a path as a tunnel hostname and reject a missing target service |
| router enable/disable | validate config, apply health-gated component, update CoreDNS, restore previous config on failure |
| router identity | allocate and persist one pinned router address per environment; never use one fixture address for every environment |
| backup run | create one recovery point per selected source, verify upload, prune by policy, update status |
| backup source edit | preserve source ids and existing recovery-point references when policy settings change |
| restore | use the selected recovery point and required key era; never restore an arbitrary snapshot |
| backing environment | never create an age key or run a consumer backup policy for the backing environment itself |
| drift | repair only labeled Groundplane resources; never touch unmanaged containers or networks |
| retry | reconcile observed labels and task checkpoints before reapplying a step |
| agent loss | time out the task, preserve the procedure, and permit a safe retry after reconciliation |

Validation is side-effect free. Rendering failures, invalid Compose output,
invalid Corefile/Caddyfile output, missing secrets, invalid paths, unsupported
strategies, and unsatisfied dependencies all stop before Agent dispatch.

## Secrets & generated credentials

Desired state never contains a secret value. The YAML holds references; the
values live in the Controller's **encrypted secret store**, and the Agent
materializes them on the host (0600) into the env files the app reads.
Reusable Secret create values are valid UTF-8 and at most 255 KiB before the
Controller-key age wrap; the durable ciphertext ceiling is 256 KiB.

Reusable Secret deletion is a Controller-owned Task with a 30-second claim
deadline. Dispatch atomically hides the Secret behind a durable tombstone and
returns `202 {task_id}`; completion atomically removes metadata, indexes, and
ciphertext, while failure, timeout, or abort removes the tombstone and restores
the Secret. Retry reacquires the tombstone atomically. Existing desired-state
references do not block deletion and fail later resolution when neither the
selected Secret nor normal platform fallback remains.
An exact prepared, active, retry-open, or releasing Script value-generation
reference is not a desired-state reference and blocks deletion with
`resource.in_use` until ADR 0062's bounded release completes.

- **Secret references.** A secret is referenced by its env file
  (`secrets/.env.edge`, `secrets/.env.<project-id>`, the deterministic per-environment
  `secrets/.env.<environment-id>`) or its file path
  (`config/identity-tls/ca.pem`, relative to the environment's volume folder) —
  never an inline value and never a bare
  `secrets/<name>` reference. The Controller resolves the reference at
  materialization time and the Agent writes the concrete value (env file,
  age recipient, connector credentials, TLS file) where it is needed.

- **Encryption at rest + backup encryption (locked).** Two distinct layers:

  - **At rest — one controller key.** The Controller runs as root and wraps
    every secret value with a single key file at `/etc/groundplane/controller.age`
    (0600, root-only) before the value lands in etcd. Nothing else on the host
    can read the secret store; only a root compromise exposes it. This key is
    platform-wide and never per-environment — per-environment wrapping
    keys would be reachable by the same compromise and buy nothing. The key is
    a recovery prerequisite, but MVP does not define an export bundle that
    restores an entire host.
  - **Backups — one age keypair per environment, generated LAZILY the first
    time backups are enabled** (a staging/dev environment that never backs up
    gets no key; the Settings tab shows "not generated yet" until then).
    Backups are encrypted with the environment's
    **public recipient** (safe in desired state — the backup policy references
    it); the **private identity** is stored only under the controller-key wrap,
    delivered to the Agent only for current-era restore through its task-scoped
    transient secret slot,
    repeatably exported with `no-store` for old-era point restore, and rotatable per
    environment (rotation affects new backups; prior recovery points need the
    previously exported identity). The host therefore CAN restore backups
    on-box (decision A, MVP), while the export gives the operator an off-host
    copy that survives a full host compromise.

**Control-plane metadata in etcd (locked) — the future DR boundary.** etcd
stores desired-state documents, the task queue, encrypted secret metadata,
Agent and component settings, connectors, and Runner records. It is not the
whole host: workload data and volumes, workload images, rendered artifacts,
the controller age key, startup configuration, and host/bootstrap prerequisites
remain external. Corefiles, Caddyfiles, env files, resolv.conf, and Compose
projects are derived and can be re-rendered from control-plane metadata, but
this does not restore external workload data or images. MVP does not promise an
automated
empty-host export/import or a complete host restore. A future DR procedure must
provide metadata, the controller key, startup config, images and data backups,
and recovery bootstrap before reconcile; the host and its prerequisites are
outside this current source contract.

The startup config remains a native bootstrap file rather than etcd desired
state, but it is an operator-facing document. `GET /controller/config`,
`PUT /controller/config`, `groundplane controller config show|set`, and the
Controller Console page expose the same exact YAML bytes. Replacement uses the
startup parser, an exact SHA-256 revision fence, a required idempotency key,
mode `0600`, and an atomic same-directory rename followed by directory sync.
The key is durably bound to the exact expected revision and YAML intent: an
exact retry replays its stored response, while reuse for another intent fails.
Comments and formatting survive. The running process continues using its
startup snapshot; every differing document reports `restart_required: true`
until the Controller restarts. Agent config remains a separate immediately
reconciled etcd singleton.

- **Provisioning credentials are generated by the Controller.** When a backing
  project is attached, the adapter's `create_role`/password step uses a
  Controller-generated password containing exactly 32 cryptographically random
  bytes encoded as 43 unpadded base64url characters (URL-safe because it lands
  in a connection string). The role (and its database) is named
  `<service-name>_<first-6-of-attach-id>` — the first 6 of the attach id's
  random tail, so two attaches of the same service can never collide and
  every database on the shared instance stays unique (30 random bits;
  collisions negligible at any scale). The attach name is only the spec
  key — it never influences the provisioned identifiers. The password is
  stored encrypted, keyed by role, and used for
  both the `CREATE ROLE ... PASSWORD` provisioning and the rendered fact
  (`pg16_PASSWORD`, `pg16_URL`), which the operator turns into their env var
  of choice.
  Today this is the manual two-file dance between `secrets/.env.backend` and
  `secrets/.env.app` (`provision-identity-db.sh`); Groundplane makes it one
  source of truth.
- **Retrieval is on demand.** The Console shows the generated credential once
  at attach time and re-reveals it anytime with **one click by default**
  (never cached, never logged). The typed confirmation is a **Console-wide
  browser-profile preference** (Settings -> Preferences -> "Require typed
  confirmation to reveal secrets", default off) that operators can turn on —
  and it is
  **Console-only by design**: it guards against accidental clicks in the UI,
  not access. The Console calls the resource-specific API value endpoint, and
  direct API callers receive the value because the security boundary is
  localhost + root daemon + no auth. The CLI deliberately has no reveal
  command. Copy returns the real value; the display stays masked; nothing is
  cached or logged.
- **Rotation is declared-deferred (except the age key).** The only rotation
  in the MVP is the backup encryption age key per environment (`rotate-key`).
  Secret rotation and credential/role-password rotation
  (Regenerate -> `ALTER ROLE ... PASSWORD` -> re-render the connection env
  var -> Agent rewrites the env file -> the app picks it up on next deploy)
  are locked decisions, declared-deferred — no Console, CLI, or API surface
  until after the MVP.
- **Variable names are the app's choice.** Attach produces facts (host, port,
  database, role, password reference) under prefixed keys — never the final
  env var name. The operator creates env vars from the facts with names the
  app reads: `DB_URL=pgsql://...` for
  Laravel, `DATABASE_URL=postgresql://...` for a CMS, or decomposed
  `PGHOST`/`PGPORT`/`PGUSER`/`PGPASSWORD`. Changing the name is editing the
  env var, not the attach.
- **Env var exposure.** Every env var is created with an exposure: **a
  specific service** or **all services** in the environment. All-services
  vars land in the environment's `env_vars` (inherited by every service, e.g.
  `APP_ENV`); service-scoped vars land in that service's `environment` and
  appear only in its env file. On a name conflict the service-scoped value
  wins. Materialization: each service's env file = `env_vars` + its own
  `environment` + the env files it attaches. The same entry dialog also
  creates **files** (name, volume-relative path, content) and carries the
  **secret flag** that moves a var or file into the encrypted secret store
  (see Environment entries above).

## Private acceptance workload

The MVP is exercised against a private, representative multi-service workload.
Its tenant and project identities, domains, credentials, source paths, network
layout, service inventory, deployment driver, and captured QA state are not part
of Groundplane's public product contract and MUST remain in an ignored local
workspace. Public fixtures use generic names and documentation-only addresses.

## Out of scope for the MVP

- Cross-tenant connectivity grants.
- Revision-bound Plans, approval workflows, and an Operations journal.
- Runner pools and GitHub Actions automation.
- Package manifests and additional shared-infrastructure instances beyond the
  dynamic template.

### Host read model (locked)

`GET /host`, `groundplane host show`, and the Platform Host page expose one
non-persistent live snapshot. Linux identity and resource sources, uptime and
binary-unit formatting, one-minute normalized CPU load, whole percentages,
Docker version behavior, and safe etcd state are fixed by ADR 0044. The API
never exposes configured etcd endpoints or raw probe errors.

The etcd node label is `single-node`; the managed loopback endpoint is either
`healthy` or `failed`. Docker version or etcd DB size is `unavailable` when its bounded
probe cannot supply that field. The Controller row is healthy while serving
and reports `groundplane-controller.service` plus the shared build version.

Before Agent enrollment, Host reports the Agent as `stopped` with the
Controller bootstrap runtime config. After enrollment it reports the durable
Agent config and the same status mapping as Agent detail; labels are sorted
`key=value` strings for stable display. Host health is not a monitoring store,
Task journal, or replacement for external metrics.

### Agent read model (locked)

Agent list and detail responses expose the Controller-owned local Agent as
`id`, immutable `enrollment_task_id`, `host`, `status`, nullable `version`,
keyed `labels`, nullable durable `ready_at`, nullable live `last_report_at`, and
`in_flight`. Both timestamps are absolute UTC RFC3339 values. `ready_at` is the
first authenticated Ready committed for the Agent and never changes;
`last_report_at` is current-process session telemetry. Relative report ages are
presentation only and are never stored or returned by the API. `in_flight` is
the durable count of non-terminal Task assignments for the Agent, not a value
inferred from stream capacity.

The public status projection is closed: `provisioning` and `updating` are `pending`;
`ready` with a current-generation, authenticated, non-stale Ready session is
`healthy`; `ready` without that session is `degraded`; and `deleting` is
`stopped`. `ready_at` remains null while provisioning. Version and
`last_report_at` remain null until the current Agent generation has reported
Ready. In the one-host MVP, `host` is the Controller host's current hostname.
Agent configuration labels are a string-to-string map; CLI label input uses
repeatable `key=value` values.

Agent config replacement commits the complete desired config synchronously.
When a connected Agent reports Ready with a different durable config pending,
the Controller stops dispatching new work and lets existing assignments drain.
It sends the replacement on the first Ready whose free capacity equals the
previous configured maximum. The Agent then replaces its idle worker pool,
resets its pull cadence, and immediately reports Ready under the new maximum;
dispatch resumes only after that report. No running Task is aborted and the
Agent process is not restarted. A disconnected Agent receives the current
config during its next authenticated connection.

## Connector addressing and credential contract

For the MVP, an Environment Connector has kind `s3-compatible` and records the
complete operator decision: `endpoint`, `bucket`, optional normalized `prefix`,
`region`, required Boolean `path_style`, and exactly the credential keys
`access_key` and `secret_key`. `path_style: true` selects path-style addressing;
`false` selects virtual-hosted-style addressing. Each credential selects exactly
one reusable `secret_ref` or direct write-only `value`. A reference resolves in
the owning Environment's Project and then through platform fallback and must
name an `env_var` Secret. Direct values are encrypted before persistence and
are never returned. Connector CRUD does not probe the remote endpoint. ADR 0045
locks validation, persistence, deletion, and redaction behavior.
The `secret_ref` remains a late-bound key and creates no Secret reverse
reference. Secret deletion is not blocked by Connector records; the next use
or redispatch selects the remaining fallback or fails closed when no valid
`env_var` value remains.
## Durable hierarchy aggregate deletion

Tenant, Project, and Environment deletion are asynchronous Controller-owned
aggregate operations. `DELETE` publishes exactly one durable `remove` Task and
returns `202 Accepted`; it never performs a synchronous cascade. The operation
kinds are `tenant.delete`, `project.delete`, and `environment.delete`. All three
use the same durable deletion engine.

The Controller atomically publishes the parent Task, target tombstone,
operation lock, replay target, cleanup fence, and deletion intent. The resource
remains listable and showable until final success and exposes nullable
`deletion_task_id`. While it is present, edits, renames, descendant creation,
and new operations beneath the aggregate fail with conflict. Console controls
are disabled from this projection; the Console does not optimistically remove
rows or manufacture Activity.

Planning freezes exact descendant membership, reverse references, and the
hierarchy coordination epoch at one etcd revision. Mutations that could create
or re-parent a child compare the ancestor tombstone and epoch, so a child cannot
appear outside the plan. The deterministic child-first DAG is written in
bounded batches. Action identity is stable across restart and retry. External
effects are idempotent by action id and followed by a durable authority receipt.

Membership freezes typed procedure inputs, not fabricated storage digests.
After the domain assigns the deterministic DAG ids and ordinals, the etcd plan
binder derives every child Task `op_<ULID>` identity and seals canonical
compare, mutation, and postcondition templates. Dynamic commit revision,
checkpoint generation, action/completion identity, and terminal-time operands
are closed typed slots; fixed operands are literal. Only bound actions may be
persisted, and execution must resolve the same template before applying it.

Environment resources and Agent cleanup precede Environment finalization;
Environments and Project-owned resources precede Project finalization;
Projects and Tenant-owned resources precede Tenant finalization. Project
Secrets and Tenant/Project Runners are contained descendants. Exact reverse
references are fences. Parent records and indexes are removed only after every
sealed-plan action is terminal-successful.

Environment membership includes every Component record and index, including
the default `caddy` and `cloudflare-tunnel` Components even when disabled.
Their explicit `component.remove` actions precede Environment Agent cleanup;
cleanup is not complete merely because runtime containers are absent.

The root finalizer is the last immutable plan action but is not executed by
the Controller Task runner. After ordinals `0..plan_count-2` have completion
proofs, the operation enters `finalizing`. The Task acknowledgement transaction
then applies the root compare/mutation set, writes its completion and the
all-action summaries, retains recovery evidence, terminalizes the Task and its
idempotency marker, and removes the parent record and indexes in one CAS. This
avoids both a missing-parent/running-Task crash window and a circular summary
dependency.

An attempt has a six-hour deadline. Failure, abort, or timeout retains the
parent and tombstone with its Task and receipts. Operator retry creates a new
Task attempt while preserving operation/action identities and successful
receipts. Task and deletion recovery records are retained for 90 days.
