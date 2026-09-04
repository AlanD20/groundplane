# Groundplane — CLI & API structure

Companion to `mvp.md` (the authoritative contract). Defines the shape
of the human-facing surfaces: the **CLI** and the **API**. One resource
model, three surfaces — Console, CLI, API — all speaking the same nouns and
verbs. The agent channel (gRPC/protobuf) is a machine surface and is
defined in the vision doc, not here.

---

## 1. Principles

- **Nouns first, verbs last.** `groundplane <resource> <action> [target]`.
  The CLI tree is the API tree is the Console navigation.
- **Scope is resolved once.** Tenant / project / environment scoping comes
  from global flags, then the config file, then positionals — never
  repeated per command.
- **Slugs for humans, ids for machines.** Every entity carries a display
  name, a **hyphenated slug** derived from it, and a **stable id**. Slugs
  are renamable at any time (nothing references them — ids do), and
  uniqueness is scoped: tenant slugs are globally unique, project slugs
  within their tenant, environment names within their project, service
  names within their environment. The CLI accepts **scoped URL labels**
  (resolved to ids through API collection reads within the scope chain) and
  ids with `--id`; the
  API addresses entities by id. Renaming a slug never breaks references —
  a stale slug simply fails to resolve, and the operator updates the few
  scripts that used it.
- **Every asynchronous operation is a task.** Deploy, rollback, restore,
  attach, run, backup, destructive delete, and abort return a task id and
  stream events. Validated synchronous updates, including rename and singleton
  config replacement, return the updated representation; any resulting
  reconciliation is still recorded as a task.
- **Snake_case everywhere.** JSON field names match the desired-state YAML
  keys (the render step stays near-1:1).
- **Edits are partial; singleton replacements are explicit.** Entity edits use
  `PATCH`. `PUT` is reserved for complete singleton replacement: an
  environment Blueprint, backup policy, component config, or Agent config.
  There is no generic whole-entity replacement route.

## 2. The resource model (the nouns)

   tenant ──> project ──> environment ──> zone · service · route · volume ·
       │            │          │              component (owner=environment)
       │            │          │              · env entry · script · attach · backup
       │            │          └─> service ──> deploy/rollback/logs/attach
       │            ├──> secrets (project-scoped) · connectors (environment-scoped)
       │            └──> backing project (kind=backing: one env "main",
       │                                    one adapter-backed service)
       └──> runners

     platform: tenants · projects · backing-services · activity ·
             secret-store · host · agents · tasks ·
             component (owner=platform) · settings

## 3. CLI

The CLI is implemented with **Cobra** — nouns as command files, the tree
below verbatim. Layout and the shared common project (unified logging,
errors, output) are in `docs/architecture.md`.

    groundplane [global flags] <resource> <action> [target...] [flags]

Global flags (any position):

    -c, --config FILE      config file (default ~/.config/groundplane/config.yaml)
        --host ADDR        controller address (default http://127.0.0.1:8080)
    -t, --tenant SLUG      scope: tenant
    -p, --project SLUG     scope: project
    -e, --env NAME         scope: environment
    -o, --output TABLE|JSON|YAML   (table is default; json/yaml for scripts)
        --no-color         plain output
        --id               treat targets as ids instead of slugs (for scripts;
                           ids never change — slugs can be renamed)
    -h, --help

`route add` uses `--hostname` for the public Route hostname so it cannot
shadow the global `--host` Controller address.

Scope resolution order: flags > config file > positional scope path
(`groundplane service deploy app-api --project storefront --env production`
is the canonical form; `-t acme -p storefront -e production` are synonyms).
Slugs resolve within their scope; targets default to slugs and switch to
ids with `--id`.

**Flat nouns (locked).** Every resource is a top-level noun — never nested
under its parent. `service`, `zone`, `route`, `volume`, `entry`, `script`,
`backup`, `component`, and `router` are siblings of `environment`, not children of it; the
parent is selected by the scope flags above, exactly like the API's URL
nesting. No resource ever lives two levels deep in the command tree —
guessing "is this flat or nested?" is a CLI design failure, and flat-everything
eliminates it. `component` itself is one noun spanning two owners
(`environment` and `platform`), while `runner` spans `tenant`/`project` — not
a nesting exception. Connectors are environment-only. The only subcommands are non-resource facets of their
noun: `backup policy` and `agent config`.

Verb inventory (locked):

    CRUD:      list · show · create · add · edit · remove     (alias: delete)
    Lifecycle: start · stop · destroy
    Toggle:    enable · disable                                (component)
    Ops:       deploy · rollback · backup · restore · attach · detach ·
               run · logs · retry · apply
    Config:    show · set       (`config show|set` addresses the Agent or
                                 component config singleton; backup policy
                                 uses `policy show|set`)
    System:    join · update · abort · serve · version · completion
    Backup-only: points · rotate-key · export-key    (recovery-point
                 listing and age-key lifecycle — narrow to this one
                 resource, no natural fit in the categories above)

`create` is reserved for scope-creating entities (tenant, project,
environment, backing-service); **`add`** is for everything added inside an
existing scope (service, zone, route, volume, entry, file, script,
release-group, runner, secret, connector). Per-zone DNS forwarders are not a standalone
resource — they're a setting inside the `coredns` component's config
(`component config coredns --platform`), alongside the other resolver
settings in `mvp.md`'s DNS section. Secret values are revealed through the
Console and the resource-specific API value endpoints; there is no CLI reveal
command. See section 4.

**Rotation is declared-deferred for the MVP.** Secret rotation and
credential/role-password rotation have no CLI or API surface yet — the only
rotation that exists is the backup encryption age key (`backup rotate-key` /
`POST /environments/{id}/rotate-key`).

Command tree:

    groundplane
    ├── tenant            list | show <slug> | create | edit | rename | delete <slug>
    ├── project           list | show | create | edit | rename | delete
    ├── environment (env) list | show | create | edit <slug> --network-pool CIDR
    │                    rename | delete
    │                    blueprint show <slug>
    │                    blueprint validate | apply <slug>
    │                              --bundle-dir DIR --root RELATIVE_PATH
    │                              [--compose-file RELATIVE_PATH]... [--var KEY=VALUE]...
    │                    logs [--tail N] [--follow]
    ├── service (svc)     list | show <name> | add | edit | remove
    │                    deploy <name> [--tag <tag>] [--strategy blue-green|recreate]
    │                              [--on-failure switch_back|leave_active]
    │                    rollback <name> [--tag <tag>]
    │                    start | stop | destroy
    │                    logs <name> [--tail N] [--follow]
    │                    attach <name> <backing> [--name <attach-name>]
    │                           (--new-credential | --credential <attach>) [--grant <attach>...] | detach <attach>
    ├── attach            list | rename <attach> <name> | fact <attach> <key> [--grant <attach>]
    ├── zone              list | show | removal-impact | add | remove
    ├── route             list | show | add | edit | remove
    ├── volume            list | show <slug|--id id>
    │                    add --slug <slug> [--key <compose-key>]
    │                    edit <slug|--id id> --slug <new-slug>
    │                    remove <slug|--id id>
    ├── entry             list | show <id> | add --type env|file [entry fields]
    │                              [--literal VALUE | --value-file PATH|- | --secret-ref REF
    │                               | --fact-attach ID --fact-key KEY [--fact-grant-attach ID]]
    │                    bulk-edit [--file PATH|-] (--plain | --secret)
    │                              [--all | --service NAME...]
    │                    edit | remove
    ├── script            list | show | add | edit | run <name> | remove
    ├── release-group     list | show <name>
    │                    add <name> [--tag <tag>] [--on-failure switch_back|leave_active]
    │                    edit <name> [--tag <tag>] [--on-failure switch_back|leave_active]
    │                    deploy <name> [--tag <tag>]
    │                    rollback <name> [--tag <tag>] | remove <name>
    ├── backup            policy show
    │                    policy set --frequency <UTC-calendar> --keep N
    │                              --encryption age|none --connector <name|id>
    │                              --source attach:<name|id>|volume:<slug|id>|config...
    │                              [--id]
    │                    policy set --off
    │                    run
    │                    restore <source> [--point <id>] [--age-identity <path>] [--id]
    │                    points [--cursor <token>]
    │                    rotate-key | export-key [--file <path>|-]
    ├── component         list [--env NAME | --platform]
    │                    show <id> | enable <id> | disable <id>
    │                    config show <id> | config set <id> [--template-file <path|->] | update <id>
    ├── backing-service (bs)
    │                    list | show
    │                    create --slug --name [--description]
    │                      --adapter postgres:16|valkey:9
    │                      --network-pool --zone-name --zone-subnet
    │                      [--zone-internal]
    │                    start | stop | destroy
    ├── secret           list | add | show | remove              (scope = project)
    ├── connector        list | add | show | remove              (scope = environment)
    ├── runner           list | add | show | retry | remove      (scope = tenant|project)
    │                    add --registration-token-file <path|->
    │                    retry <id> --registration-token-file <path|->
    ├── agent            list | show | join | config show | config set
    │                    update (<id> | --all) | remove <id>
    ├── task             list [--limit N] [--cursor VALUE] [--tenant NAME [--project NAME [--env NAME]] | --workspace platform|<tenant>]
    │                    show <id> | events <id> | retry <id> | abort <id>
    ├── activity         list [--limit N] [--cursor VALUE] [--tenant NAME [--project NAME [--env NAME]] | --workspace platform|<tenant>]
    │                    (exact alias of `task list` — the journal IS tasks)
    ├── host             show
    ├── controller       config show | config set --file PATH | serve | key show | etcd show
    ├── agent-run        run               (foreground, for debugging)
    ├── version
    └── completion       bash | zsh | fish

`host show` is the operator-facing CLI mirror of `GET /host`, including the
Controller's view of etcd status. `controller key show` and `controller etcd
show` are same-host diagnostics: they read the local Controller startup
configuration, fingerprint its local age key or probe the Controller-managed
loopback etcd endpoint directly, never call the REST API, and are excluded from the 1:1
operation manifest with the other local process/tooling commands.
`controller config show` and `controller config set --file PATH` are ordinary
human-API operations and are not local-command exceptions. They mirror
`GET /controller/config` and `PUT /controller/config`; set first reads the
current exact revision and publishes the supplied YAML only against that
revision. The response includes `path`, exact `content`, `revision`, and
`restart_required`.
The key fingerprint is exactly `sha256:` plus 64 lowercase hexadecimal
characters over SHA-256 of the canonical age recipient's UTF-8 text.

Host values are one live, non-persistent snapshot using ADR 0044's exact Linux
sources, binary units, rounding, and safe dependency-state projection. The CLI
uses the generated `host.show` operation; the Console loads the same document
without fixture fallback. Neither surface adds endpoint details, relative
report ages, scheduler claims, or locally inferred health.

Machine bootstrap requires three pairwise-disjoint IPv4 CIDRs in the local
Controller startup config: `environment_pool`, from which operators reserve
Environment pools; `system_pool`, for Agent and other generated infrastructure
networks; and a `/24`, `/25`, or `/26` `runner.network_pool`, divided into
per-Runner `/29`s. Runner space is not reserved from `system_pool`. Bootstrap
validates all three against protected host routes and existing Docker networks. These are local
machine/bootstrap inputs, not mutable Controller resources, and therefore have
no Console, REST, or ordinary CLI operation.

**Agent and Platform Components (locked split).** The top-level `agent` noun
is the sole Agent surface: enrollment (`join`), per-instance config
(`config show|set` — pull interval, max concurrent, labels), and updates
performed by the Controller. It has no Component projection. CoreDNS is the
sole MVP Platform Component and uses `component show`, `component config`, and
`component update` with its stable Component id.

`agent join` calls `POST /agents` and receives `202 {task_id}`. The Controller
task creates the local Agent record, internally generates and injects its
channel token and runtime config, and owns the Agent container lifecycle
through Docker. No token, CA, certificate, or bootstrap artifact is returned
through the human API or printed by the CLI. There is no public join-token or
REST pair endpoint, pending approval state, or `approve` command. The Agent
never updates or recreates itself; `agent update` asks the native Controller to
replace the selected idle Agent at configured `agent.image` and, if necessary,
roll back the previous digest. Exactly one id or explicit `--all` is required;
`--all` resolves the singleton and calls the same per-id endpoint.
This topology is accepted in ADR 0016; exact local token mechanics remain
Proposed in ADR 0011.

## 4. API (REST/JSON, human surface)

    Base:      http://127.0.0.1:8080/api/v1        (versioned from day one)
    Auth:      none in the MVP human API. Loopback is the default; startup may
               add explicit trusted private listen addresses. Wildcard,
               public, and Cloudflare-Tunnel exposure are forbidden. The local
               Agent machine channel uses a Controller-provisioned token;
               mTLS, CAs, and certificates are post-MVP.

Conventions:

- **One stable operation identity.** Each parity operation uses one exact
  lowercase `noun.verb` identity for both the Console action id and OpenAPI
  `operationId`, for example `host.show`, `task.events`, and
  `component-config.set`. Multiword nouns and verbs use kebab case. CLI paths
  remain nouns and leaf verbs separated by spaces.
- **Success statuses are semantic, not handler defaults.** Reads and
  synchronous updates return `200`; creates return `201`; every Task dispatch
  returns `202`; a truly synchronous bodyless delete returns `204`. A delete
  that performs operational or destructive work is a Task and therefore
  returns `202`, never `204`.

- **Flat collections, filter params — never nested paths.** Every entity is
  addressed by its own id; the parent is a query filter, exactly like the
  CLI's scope flags (and the Console sidebar's filters). `POST /services`
  takes `environment_id` in the body; `GET /services?environment={id}`
  lists. This keeps the CLI↔API 1:1 rule mechanical: one CLI noun ↔ one
  collection. No nested URL forms exist — flat is the only shape.
- **Direct Service desired mutations are a typed projection, not raw Compose.**
  `POST /services` takes `{environment_id, name, image, zones, strategy,
  on_failure, healthcheck, resources, expose, restart, replicas}`. `PATCH
  /services/{id}` takes the same direct-edit fields without identity or owner.
  The body is the complete Console-editable subset: an empty healthcheck
  object removes it, empty lists clear zones/expose, and Service name is
  immutable. Blueprint-native fields with no specialized Console control are
  preserved outside this mutation rather than silently dropped. Both responses
  return the complete Service projection plus Controller-owned runtime intent.
  `GET /services/{id}` returns that projection as a Service detail plus
  `native_compose`, a canonical normalized single-Service Compose document
  derived on demand from the Environment's current immutable desired revision.
  Collection and mutation responses omit this potentially large read-only
  field. The detail also embeds `release_ledger`, the first 50 records from the
  same fixed-revision service-filtered ledger used by `GET /releases`, including
  its continuation cursor. `service show` and the clickable Console Service
  card display both; `release list` continues beyond the embedded page.
  Create and edit publish a new immutable normalized Environment desired
  revision but do not deploy it. `DELETE /services/{id}` publishes a targeted
  Remove Task and keeps the current head and Service visible until successful
  acknowledgement. It rejects all live reverse references without cascading,
  retains Volumes and immutable release history, and promotes the exact
  candidate revision only after local runtime cleanup succeeds. ADR 0058 is
  the sole direct Service desired-mutation contract.
- **Workload images are host-local inputs.** Deploy and rollback resolve the
  selected tag against each Service's declared image already present in the
  host Docker daemon and pin its exact local identity before mutation. They do
  not build, pull, or push. Groundplane-managed pinned Agent, etcd, Component,
  and backing images are release/installation assets and do not create an
  operator registry-integration contract.
- **Every mutation is idempotent at the human API boundary.** Every `POST`,
  `PUT`, `PATCH`, and `DELETE` requires `Idempotency-Key` except the bodyless,
  read-like `POST /environments/{id}/export-key`, which persists neither marker
  nor response. Console and CLI
  generate one ULID per user intent and reuse it only for transport retry. A
  key is 16–128 ASCII characters matching `[A-Za-z0-9._:-]+`. An identical
  committed direct mutation replays its exact stored response. A Task-backed
  duplicate preserves the original Task id and 202 response; while active it
  returns `idempotency.in_progress` (409). Reuse for a different canonical
  request returns `idempotency.mismatch` (400). Schema, reference, and domain
  failures discovered before the atomic claim write no marker. There is no
  optional or compatibility mode. Accepted Volume DELETE is the bounded
  operation-scoped exception: an equal request always replays its original
  `202` root Task response while removal state is retained, including between
  failed attempts. Retry appends a successor Task but never replaces the
  original replay response.
- **The only nesting is singleton sub-resources** — resources that are
  strictly 1:1 with a parent and never listed standalone: backup policy
  (`/environments/{id}/backup-policy`), component config
  (`/components/{id}/config`, whether owner=environment or
  owner=platform), router projection (`/environments/{id}/router`), and
  agent config (`/agents/{id}/config`). Environment Blueprint
  (`/environments/{id}/blueprint`) is the revision-fenced desired-state
  authoring singleton with read, validate, and apply operations. That is the
  standard singleton pattern, not hierarchy nesting.

Backup Policy `keep` is a JSON integer in the inclusive range
`1..9007199254740991` (`Number.MAX_SAFE_INTEGER`). The CLI accepts the same
base-10 integer range and rejects an out-of-range value before making an HTTP
request.

The existing Backup Policy singleton read/write projection adds the nullable
server-derived `next_run_at` field to both `GET
/environments/{id}/backup-policy` and the successful `PUT` response. It is not
a replacement-request field and does not add an endpoint or action. The value
is an RFC3339 UTC timestamp at exact whole-second precision (`...Z`), derived
from the current policy and durable policy-revision cursor; it is `null` if and
only if the policy is disabled or unconfigured. A valid enabled policy always
returns a value. `backup policy show` exposes this field as `next_run_at`, and
the Console Environment Backup settings renders the same field from the same
projection.

The scheduler compares UTC instants, not rendered strings. For an enabled
same-frequency replacement, the new revision carries the prior cursor's
cadence and uses the exclusive boundary
`max(previous last_evaluated_at, replacement commit instant)`, so an occurrence
at or before the replacement commit cannot run under the new snapshot. Create,
enable, and frequency-change revisions seed that exclusive boundary at their
commit instant. Due selection remains the latest missed occurrence in
`(boundary, now]`; cursor advancement skips earlier occurrences and a held
Environment lock records `skipped_overlap`.
- **Operational actions are POST sub-resources on the entity** and return a
  task: `POST /services/{id}/deploy` `{tag?, strategy?, on_failure?}` →
  `202 {task_id}`. Omitted tag uses the current service tag; omitted strategy
  uses the service default; omitted `on_failure` uses `switch_back`.
  `POST /services/{id}/start`, `/stop`, and `/destroy` set the durable
  Controller-owned `runtime_intent` to `running`, `stopped`, and `absent`
  respectively. A new Service starts with `runtime_intent=running`, and
  Blueprint reconciliation never overwrites the current value.
  These three actions accept no body. Applied Services dispatch a 120-second
  Agent Task pinned to the exact applied projection; Stop has a fixed
  30-second grace. Never-applied Services dispatch a 30-second Controller
  no-op Task. Intent and Task publication are atomic, one lifecycle Task may be
  active per Service, terminal acknowledgement releases that fence, and retry
  reuses the immutable render input. Task failure does not roll intent back.
  `runtime_intent` is not a Blueprint field. `DELETE
  /services/{id}` is Remove: it deletes desired state rather than acting as an
  alias for Destroy.
  Rename is the deliberate synchronous exception to the operational-action
  rule: `POST /tenants/{id}/rename`, `POST /projects/{id}/rename`, and
  `POST /environments/{id}/rename` return the updated entity with `200`.
  Follow progress via `GET /tasks/{id}` and stream events via
  `GET /tasks/{id}/events` (SSE). **Attach and detach are tasks like any
  other**: `DELETE /attaches/{id}` returns `202 {task_id}` running the
  adapter's deprovision (revoke grants → drop role → optionally drop
  database); the desired-state record is removed as part of that task —
  never a plain record delete.
- **Streams are SSE**: logs (`GET /services/{id}/logs?follow=true`), task
  events, activity — `Accept: text/event-stream`.
- **Resource Logs are transient and non-resumable**: Service and Environment
  log streams accept only `tail=0..1000` and `follow=true|false`, defaulting
  to `tail=200` and `follow=false`; they emit bounded `event: log` JSON
  records without an SSE `id` or `Last-Event-ID` resume. The Controller takes
  one fixed serving-release snapshot, the Agent selects matching managed
  Docker containers (all replicas, excluding stable proxies), and the source
  set remains fixed for the connection. The private `LogReady` handshake gates
  SSE headers after initial Docker setup, including zero-source setup. Workload
  stdout/stderr is forwarded unchanged except UTF-8 normalization and 32 KiB
  line truncation; workloads must not print secrets and MVP has no secret-aware
  redaction. ADR 0059 defines schema, bounds, pre/post-header errors, and
  termination.
- **Task-event streams resume by durable sequence.** Each event uses its
  canonical decimal `sequence` as the SSE `id` and a JSON `data` object with
  exactly `{sequence, step_id, state, attempt, ordinal, received_at}`. State is
  `pending | running | completed | failed | aborted | timed_out` and
  `received_at` is RFC 3339. The request accepts no `Last-Event-ID` header or
  exactly one canonical unsigned decimal value; absent and `0` replay all.
  Agent output chunks, durable payload hashes, etcd keys, and etcd revisions
  are never part of this human stream. Reconnect and terminal-drain behavior is
  fixed by ADR 0023.
- **Retry = a new task with the same operation identity.** A retry creates a
  new task id whose `retry_of` is the immediately retried attempt, while
  preserving `operation_id`, target, parameters, execution plan, plan hash,
  timeout, and operation idempotency key. Only `failed`, `timed_out`, and
  `aborted` tasks are retryable. `pending`, `running`, and `completed` tasks
  fail with `task.not_retryable` (409). If the same operation already has a
  pending or running retry, the request fails with `task.retry_in_flight`
  (409). Safety comes from the Agent's reconcile-based idempotency: it checks
  labeled-container state and durable step checkpoints before re-applying any
  step. The Controller's resource serialization locks still apply.
- **Abort = terminalize the addressed task, not a second task.** `POST
  /tasks/{id}/abort` and `task abort <id>` require an explicit Task id and
  return `202 {task_id}` containing that same id. Pending and running Tasks
  are abortable; an already-aborted Task is an idempotent success, while
  completed, failed, or timed-out Tasks fail with `task.not_abortable` (409).
  Running Agent and native Controller work returns only after cancellation
  durably commits. ADR 0041 fixes the exact race and replay behavior.
- **Internal prune Tasks are excluded from generic Task mutations.**
  `backup_prune` remains visible through Task and Activity list/detail, but
  `POST /tasks/{id}/retry`, `task retry <id>`, `POST /tasks/{id}/abort`, and
  `task abort <id>` reject it without an idempotency claim or Task mutation.
  The retry pair returns `task.not_retryable` (409), and the abort pair returns
  `task.not_abortable` (409). There is no dedicated prune operator action.
- **Task states**: `pending`, `running`, then exactly one of `completed`,
  `failed`, `timed_out`, or `aborted`. `acked` is a streamed acknowledgement
  event, not a durable terminal state. Every task exposes `operation_id`,
  optional `retry_of`, `plan_hash`, `status`, `steps`, and `events`.
- **Task owner and journal projection**: every Task also exposes immutable
  `workspace_type: platform|tenant`, `tenant_id?`, `project_id?`, and
  `environment_id?`, plus `actor: operator|system`, UTC `created_at` and
  `updated_at`, and nullable UTC `started_at` and `finished_at`. `tenant_id` is
  present exactly for a tenant workspace; `environment_id` requires
  `project_id`. The API accepts stable ids. CLI hierarchy flags resolve Tenant,
  Project, and Environment labels to stable ids; `--workspace <tenant>` also
  resolves the Tenant label and `platform` is literal. The single-operator
  token maps all authenticated human requests to `operator`;
  Controller-initiated work is `system`.
- **Task list scopes and Activity parity**: `GET /tasks` and `GET /activity`
  accept identical `limit`, `cursor`, and zero or one of
  `environment=<env-id>`, `project=<prj-id>`, or
  `workspace=platform|<tenant-id>`. Environment and workspace filters use
  immutable owner indexes; Project uses a bounded fixed-revision primary scan
  over immutable ownership. No scope lists all Tasks. Pages are Task-id ascending at one fixed
  etcd revision; the cursor binds the logical `tasks` collection, exact scope,
  limit, order, revision, and last id. Both routes return identical pages and
  errors, and their cursors are interchangeable. Owner indexes are created with
  the Task and removed with it during retention pruning.
  Platform Activity uses no scope and may narrow by Tenant, Project, or
  Environment. Tenant Activity uses its Tenant workspace and may narrow by
  Project or Environment. These controls never broaden starting visibility.
- **Errors**: RFC 7807 problem+json — `{type, title, status, detail,
  code}`; `code` is a stable, dot-namespaced machine-readable key (e.g.
  `service.not_found`, `deploy.in_flight`, `strategy.not_implemented` —
  `rolling` is declared-deferred and rejected with exactly this code).
  Every MVP problem uses `type: "about:blank"`; `code` is the stable machine
  identity until resolvable per-code problem documentation exists. Malformed
  request syntax or transport decoding uses `validation.failed` with status
  400. A syntactically decoded request that fails parameter, body-schema, or
  semantic validation uses `validation.failed` with status 422.
  Other request failures use `request.not_found` (404),
  `request.method_not_allowed` (405),
  `request.not_acceptable` (406), `request.unsupported_media_type` (415),
  or `request.failed` for an otherwise unmapped HTTP failure, including a
  route body that exceeds its raw transport ceiling (413). Every error
  response, including framework-generated failures, uses
  `application/problem+json`; framework-native error envelopes never cross
  the Controller boundary. Failures produced by `net/http` before handler
  dispatch, including header parse, header timeout, and header-size failures,
  are the explicit exception because application formatting cannot run before
  dispatch.
- **Pagination**: cursor-based `{items, next_cursor}` in stable id-ascending
  order. The default limit is 50 and the accepted range is 1–200; no total
  count is returned. A cursor binds the collection, scope, filters, order,
  limit, fixed etcd revision, and last id. Malformed or query-mismatched
  cursors use `validation.failed` (400). A compacted 24-hour revision uses
  `cursor.expired` (409) with restart-from-first-page guidance; the server
  never silently restarts a cursor.
- **Persistence failures**: `slug.conflict`, `name.conflict`,
  `state.conflict`, `resource.in_use`, and `cursor.expired` use 409;
  `storage.unavailable` is retryable and uses 503. These responses never
  expose etcd keys or revisions.
- **Filtering**: `?kind=backing`, `?status=`, `?tenant=`, `?project=`,
  `?environment=` — the same filters the Console sidebar uses.
- **Secret reveal is a Console/API-without-CLI exception by design**: there
  is no CLI reveal command. The Console calls `GET /entries/{id}/value` for
  an environment Entry or `GET /secrets/{id}/value` for a reusable project or
  platform Secret, gated by a typed-confirmation dialog. That dialog is
  click-accident protection, not access control: the localhost endpoints have
  no confirmation mechanism, and a direct host caller receives the value.
  Environment ciphertext is subordinate to its Entry and is deleted
  atomically with it; it never appears as a second Secret resource.

Resource map (collections are flat; entities by id; singletons by parent
id; actions on the entity):

Accepted ADRs 0047 and 0048 do not add an operator action or alternate wire
shape. The `postgres:16` backing-service create input has no image override;
the adapter resolves the immutable managed release. Config restore retains the
one `POST /environments/{id}/restore` action and fully replaces Entries through
read-hidden canonical-primary publication rather than an Environment-wide
active-generation read model. Restore remains unavailable in the Console until
the C16 runtime and surface land together.

Tenant responses contain `{id, slug, name, description}`. `description` is an
optional string and defaults to empty on create; CLI create/edit expose it as
`--description`.

Project responses contain `{id, tenant_id, slug, name, description, kind}`.
`description` is optional, defaults to empty, and is exposed by CLI create as
`--description`; it is not PATCH-editable because the Console has no matching
edit action.

Backing-service responses contain exactly `{project_id, environment_id,
service_id}`. The backing Project id is the facade's stable public identity;
the other ids address its sole `main` Environment and sole adapter Service.
Project labels and metadata remain on the ordinary Project resource rather
than being duplicated into this read-only facade.

Environment responses contain `{id, project_id, name, network_pool,
network_capacity, volume_dir, provisioning_state, create_task_id,
deletion_task_id}`.
`network_capacity` contains `{total_addresses, allocated_addresses,
available_addresses, zone_count}` derived at the Environment read's fixed
revision. `name` is the Environment's single
scoped-unique human and URL label; there is no second display name or `slug`
field. `volume_dir` is Controller-generated and read-only.
`provisioning_state` is closed to `provisioning | ready | failed`, and
`create_task_id` is the current or failed directory-creation Task, otherwise
null after successful provisioning.
`deletion_task_id` is the current parent `environment.delete` Task while
deletion is active or retryable, and is null otherwise. The Environment stays
visible and fenced until the shared parent-last finalization succeeds.
Environment deletion follows the accepted ADR 0053 parent-last durable
deletion contract: the Environment remains visible and fenced until cleanup
and finalization succeed, and its pool and owned cleanup records are released
only after durable acknowledgement. The contract is authoritative; C04's
end-to-end deletion implementation and acceptance evidence remain pending.
The DELETE operation is `environment.delete`, accepts no request body, and
targets a stable Environment id in either a Tenant or Platform scope; it does
not use a separate deletion schema or impact-preview route.

Volume responses contain `{id, environment_id, slug, key, path, state,
create_task_id?, origin_task_id?, current_task_id?}`. `id` is stable. `slug`
is the only mutable field. `key` is the immutable Compose key and managed
directory leaf. `path` is Controller-derived as
`<environment.volume_dir>/<key>`. `state` is
`creating | active | create_failed | deleting`. A Volume has no `name`,
editable path, aggregate size estimate, or denormalized Service-name list.

Reusable Secret responses contain `{id, scope, project_id?, key, kind, ref,
updated_at}` and never contain value bytes. `POST /secrets` accepts
`{project_id?, platform?, key, kind, path?, value}` with exactly one owner;
`path` is required only for `kind: file`, while env-var `ref` is
Controller-derived. `value` is valid UTF-8 and at most 255 KiB before
encryption. The CLI reads create bytes from `--value-file PATH|-`
(`-` is stdin), never from argv, and intentionally has no reveal command.
Secret removal dispatches a 30-second Controller Task. Its atomic tombstone
hides the Secret immediately; terminal failure, timeout, or abort restores it,
and success removes metadata, indexes, and ciphertext in the terminal Task
transaction.

Entry removal requires a stable Entry id and `Idempotency-Key`, accepts no
body or query, and returns `202 {task_id}`. An Entry already represented by the
current applied Environment uses a 120-second Agent Task to remove its exact
pinned file or rewrite affected generated env files before finalization. A
never-applied Entry uses a 30-second Controller Task with no host mutation. The
Entry remains visible through pending, running, failed, timed-out, and aborted
states and disappears only after successful terminal acknowledgement. Removal
does not apply Compose or recreate a Service; values already loaded in running
process environments remain until a later deploy or reconciliation. Component
credentials are reusable Secrets and do not create Entry reverse references.

All mutating endpoints require `Idempotency-Key` and durable exact replay except
the bodyless, read-like backup-key export `POST`. Export accepts no idempotency
key, persists no command marker or response, and produces a fresh no-store
attachment on every authorized request.

| CLI noun | Endpoints |
| --- | --- |
| tenant | `GET /tenants` and `GET /tenants/{id}` → `200` · `POST /tenants` → `201` · `PATCH /tenants/{id}` and `POST /tenants/{id}/rename` → `200` · destructive `DELETE /tenants/{id}` → `202 {task_id}` |
| project | `GET /projects` (`?kind=tenant\|backing`) and `GET /projects/{id}` → `200` · `POST /projects` → `201` · `PATCH /projects/{id}` and `POST /projects/{id}/rename` → `200` · destructive `DELETE /projects/{id}` → `202 {task_id}` |
| backing-service | `GET /backing-services` and `GET /backing-services/{project_id}` → `200` · protected `POST /backing-services` body `{slug,name,description?,adapter:"postgres:16"\|"valkey:9",network_pool,zone:{name,subnet,internal}}` → `201 {backing_service,task_id}` atomically creates the backing Project, `main` Environment, dedicated backing-owned Zone, adapter Service, adapter data Volume, and Agent Task; the Zone subnet must be inside the Environment pool and globally unreserved · bodyless `POST /backing-services/{project_id}/start\|stop\|destroy` → `202 {task_id}` and delegates to the facade's immutable adapter Service; there is no Backing-service delete endpoint, existing-Zone create branch, uploaded Blueprint, or omitted subnet default |
| environment | `GET /environments` (`?project=`), `GET /environments/{id}`, and `GET /environments/{id}/logs?tail=0..1000&follow=true|false` → `200` · `POST /environments` body `{project_id, name, network_pool}` → `202 {task_id}` and atomically publishes the provisioning Environment, globally exclusive pool reservation, and directory-creation Task · `PATCH /environments/{id}` body `{network_pool}` → `200` only when the replacement contains every Zone and overlaps no reservation · `POST /environments/{id}/rename` → `200` · `GET /environments/{id}/blueprint` → `200 {environment_id,revision,document}` plus `ETag` · side-effect-free `POST /environments/{id}/blueprint/validate` with `If-Match` and a multipart closed bundle → `200 {revision,changes}` · revision-fenced `PUT /environments/{id}/blueprint` with `If-Match`, ordered sources, explicit interpolation, and deterministic file-part identities → `202 {task_id}` · destructive `DELETE /environments/{id}` → `202 {task_id}`, retaining the pool fence until physical network cleanup succeeds and retaining Connector credentials/key material until every Recovery Point and orphan object is checkpointed, deleted, and verified absent |
| service | `GET /services` (`?environment=`), `GET /services/{id}`, and `GET /services/{id}/logs?tail=0..1000&follow=true|false` (non-resumable SSE) → `200` · `POST /services` → `201` · `PATCH /services/{id}` → `200` · `DELETE /services/{id}` and `POST /services/{id}/deploy\|rollback\|start\|stop\|destroy` → `202 {task_id}`; native `replicas` is an integer `N >= 1`; recreate preserves `N` through deploy, rollback, restart, and reapply, while blue-green rejects `N > 1` before mutation; lifecycle actions set `runtime_intent` to `running\|stopped\|absent`; `service show <name>` includes the release ledger and runtime intent |
| release-group | `GET /release-groups` (`?environment_id=` with opaque revision cursors) and `GET /release-groups/{id}` → `200` · `POST /release-groups` → `201` · `PATCH /release-groups/{id}` → `200` · destructive `DELETE /release-groups/{id}` and `POST /release-groups/{id}/deploy\|rollback` with optional `{tag}` → `202 {task_id}`; create body includes an exact ordered 2..32 Service membership, optional persisted `tag`, and `on_failure: switch_back\|leave_active`, default `switch_back`; request tag wins over group tag and an empty result is rejected; each member resolves the tag against its own declared image and retains its own digest/history; the group failure policy overrides member defaults; execution is ordered and coordinated, not simultaneous or atomic; selected hooks run once per logical Service, never per replica, with no group-hook resource; retry preserves the same operation/candidate lineage |
| attach | `GET /attaches?environment={id}` (the Environment filter is required; items include one `service_id`, stable backing ownership, `credential:{mode:"new"}` or `credential:{mode:"existing",attach_id}`, plus fact keys and secret classification, never values) · `GET /attaches/{id}/facts/{key}?grant_attach_id={id}` → `200 {value}` as the explicit Console/CLI/API reveal operation; dependent existing-credential Attaches resolve through their credential owner · `POST /attaches` body `{service_id,backing_service_id,name?,credential:{mode:"new"|"existing",attach_id?},grant_attach_ids?}` → `202 {task_id}`; `credential.mode` is required, `new` rejects `attach_id`, `existing` requires a ready credential-owning Attach in the same consumer Environment and Backing Service, and only `new` accepts grants · `POST /attaches/{id}/rename` body `{name}` → `200` · `DELETE /attaches/{id}` → `202 {task_id}`; dependent detach removes only its network edge, owner detach deprovisions and is `resource.in_use` while dependents exist; there is no attach detail endpoint |
| zone | `GET /zones?environment={id}` (required), `GET /zones/{id}`, and `GET /zones/{id}/removal-impact` → `200`; the impact read returns the exact affected Attaches, Services, provisioned databases, and an opaque impact token · `POST /zones` body `{environment_id, name, subnet, internal}` → `201`, deriving `{owner_kind, owner_id}` · `DELETE /zones/{id}?impact_token={token}` → `202 {task_id}`; ordinary removal rejects an enabled Caddy reservation, removes the managed network, strips that Zone from every Service membership, and releases the subnet only on successful acknowledgement, while backing-owned removal requires the current impact token and runs the Attach cascade · Zone fields are immutable and there is no PATCH endpoint |
| route | `GET /routes?environment={id}` (required) and `GET /routes/{id}` → `200` · `POST /routes` body `{environment_id, host?, path?, exposure, target_service_id, target_port}` → `202 {route,task_id}` · `PATCH /routes/{id}` body `{exposure}` → `202 {route,task_id}` · `DELETE /routes/{id}` → `202 {task_id}`; target identity, port, host, and path are immutable; status is exactly `unserved` (no enabled provider), `pending` (desired generation awaits provider application), `served` (latest successful pinned provider observation matches desired generation), or `degraded` (latest provider apply failed and desired is unconfirmed); without an enabled HTTP router, mutation Tasks complete as desired-only with no Agent effect |
| volume | `GET /volumes?environment={id}` (required) and `GET /volumes/{id}` → `200` · `POST /volumes` body `{environment_id, slug, key?}` → `201 {volume,task_id}` · `PATCH /volumes/{id}` body `{slug}` → `200 {volume,task_id}` · `GET /volumes/{id}/deletion-impact?cursor={opaque}&limit={1..40}` → a fixed-revision page no larger than 768 KiB, with `impact_token` only on the final page · destructive `DELETE /volumes/{id}?impact_token={token}&confirm_key={immutable-key}` → `202 {task_id}`; add, edit, remove, and Blueprint changes publish through the sole Environment desired head |
| entry / script | `GET /<resources>` (`?environment=` required) and `GET /<resources>/{id}` → `200` · `POST /<resources>` → `201` · `PATCH /<resources>/{id}` → `200` · destructive `DELETE /<resources>/{id}` → `202 {task_id}`; Entry additionally exposes atomic literal-env upsert through `POST /entries/bulk` → `202 {task_id,entries}` and `GET /entries/{id}/value` → `200` for the Console/API-without-CLI reveal exception |
| script run | bodyless protected `POST /scripts/{id}/run` → `202 {task_id}`; any request body, parameters, substitutions, argv, or runtime Environment override is invalid |
| backup | `GET /environments/{id}/backup-policy` → `200` and protected full-replacement `PUT /environments/{id}/backup-policy` → `200` retain ADR 0046's accepted absence, completeness, frequency, encryption, stable-source, ordering, and replay contract · `POST /environments/{id}/backup-run` is bodyless, rejects disabled policy, captures all configured sources, and returns `202 {task_id}` for one stored-order fail-fast Task; `source_ids` and partial manual runs do not exist · `GET /environments/{id}/recovery-points?cursor=` → `200 {items,next_cursor?}` uses an opaque fixed-revision cursor and Recovery Point id descending; each item is exactly `{id, source_id, source_kind, target_id, created_at, size_bytes, encrypted, key_era?, status:"verified"}`, with `created_at` equal to the point-id allocation timestamp and `key_era` present if and only if `encrypted` is true; `target_id` is the stable original target, survives source removal, and must still resolve for restore; environment, source-format, Connector, object-key, digest, and any verification timestamp are private; the point remains publicly nonexistent until verified commit · `POST /environments/{id}/restore` accepts `{source_id, recovery_point_id?, age_identity?}` and returns `202 {task_id}`; API references are stable ids, omitted point is latest resolved before protected intent, and `age_identity` is one optional-LF UTF-8 identity line bounded to 4 KiB and never persisted; it is accepted only for an old age era; the Console restore dialog confirms `target_id`, warns that the target is overwritten, calls out PostgreSQL/Volume downtime, and accepts the optional old-era identity · bodyless `POST /environments/{id}/rotate-key` requires an existing key and returns `202 {task_id}` · bodyless `POST /environments/{id}/export-key` returns the current identity plus one LF as `text/plain; charset=utf-8`, `Content-Disposition: attachment; filename="groundplane-<environment-id>-age-era-<era>-identity.txt"`, and `Cache-Control: no-store`; CLI `backup export-key [--file <path>|-]` writes the exact attachment bytes, with omitted `--file` or `--file -` selecting stdout, and rejects the global `--output` formatter for this raw-byte command · backup/restore deadlines are six hours; rotation is 120 seconds; PostgreSQL Attach, config, and Volume are the only runtime source kinds; there is no point-delete endpoint |
| component | `GET /components` (`?environment=` or `?platform=true`), `GET /components/{id}`, and `GET /components/{id}/config` → `200` · `PUT /components/{id}/config` → `200` · `POST /components/{id}/enable\|disable\|update` → `202 {task_id}`; every detail, action, and config route is addressed by stable Component id; Components have no create, edit, or delete endpoint |
| router | `GET /environments/{id}/router` → `200`; read-only projection grouping ingress components |
| secret | `GET /secrets` (`?project=` or `?platform=true`), `GET /secrets/{id}`, and `GET /secrets/{id}/value` → `200` · `POST /secrets` → `201` · destructive `DELETE /secrets/{id}` → `202 {task_id}` with a 30-second Controller finalizer deadline; value retrieval is the Console/API-without-CLI reveal exception |
| connector | `GET /connectors` (`?environment=` required) and `GET /connectors/{id}` → `200` · `POST /connectors` → `201` · destructive `DELETE /connectors/{id}` → `202 {task_id}` only when neither an enabled Backup Policy nor any Recovery Point reverse-reference retains it; disabled-policy configuration alone does not block deletion |
| runner | `GET /runners` (`?tenant=` or `?project=`) and `GET /runners/{id}` → `200` · protected synchronous `PATCH /runners/{id}` body `{slug}` → `200` replaces only the Tenant-unique label and replays the exact stored Runner response · `POST /runners` body `{slug,tenant_id|project_id,github_url,labels?,registration_token}` → `202 {task_id}` and atomically publishes the provisioning Runner, its sole owner index, owning Tenant's combined five-Runner quota claim, dedicated `/29`, persisted host UID/subordinate-ID slot, replay evidence, and Controller Task · `POST /runners/{id}/retry` body `{registration_token}` → `202 {task_id}` only for failed creation, reusing every allocation with a fresh token · `DELETE /runners/{id}` → `202 {task_id}` and atomically publishes the tombstone, cleanup intent, replay evidence, and Controller Task while retaining every fence; create/retry tokens are transient and never persisted, generic Task Retry rejects Runner-create Tasks, `online` is read-only observed state, and successful removal deletes only local resources and durable claims, never GitHub registration |
| task | `GET /tasks` (zero or one of `?environment=<env-id>`, `?project=<prj-id>`, or `?workspace=platform\|<tenant-id>`), `GET /tasks/{id}`, and `GET /tasks/{id}/events` (SSE) → `200`; the activity journal IS this record set; `POST /tasks/{id}/retry\|abort` → `202 {task_id}` |
| activity | `GET /activity` accepts the exact Task-list query and returns the exact same fixed-revision page; Task and Activity cursors are interchangeable |
| host | `GET /host` → `200` (includes etcd status; etcd is host-level, not a component) |
| agent | `POST /agents` → `202 {task_id}` creates the local Agent and Controller-managed container without returning credentials and requires authenticated `Ready` within 120 seconds · `GET /agents`, `GET /agents/{id}`, and `GET /agents/{id}/config` → `200` · `PUT /agents/{id}/config` → `200` · bodyless `POST /agents/{id}/update` → `202 {task_id}` uses configured `agent.image`, requires an idle Agent, rotates generation/token, waits 120 seconds for Ready, and rolls back the prior digest inside a 300-second Task · `DELETE /agents/{id}` → `202 {task_id}` has a 120-second Controller Task deadline, stops assignments, aborts active tasks with `agent_removed`, revokes the token, waits for offline, then removes the container and record; Agent is not a Component and has no Component projection |

Backup restore targets only the original surviving target; proof may use a
disposable Environment, but there is no scratch-target or whole-host import
API. PostgreSQL Attach, config, and Volume remain the implemented runtime
source kinds. Valkey data recovery is required for production MVP, but the
runtime must continue to reject `strategy.not_implemented` until a safe source
contract lands: a shared-instance RDB is not a per-Attach backup, and live
data-directory archival is forbidden.

Blueprint multipart requests begin with one `application/json` field named
`manifest`, with exactly `{root, compose_sources, interpolation, files}`.
Its `files` array is sorted by normalized path and contains
`{path, part, size, sha256}`; `part` is exactly `file-000001`,
`file-000002`, and so on. The manifest is followed by each declared part once,
in that exact order, as `application/octet-stream`; file parts never carry a
filename. Missing, duplicate, undeclared, or reordered part identities are
malformed requests.

After logical bundle validation, the Controller derives a lossless normalized
projection. Its complete durable schema and framing must fit exactly 2 MiB.
Exceeding that limit returns `validation.failed` with HTTP 422 before private
staging, Task publication, idempotency authority, or any host effect.

A deletion-impact cursor is at most 4 KiB and binds the Volume, fixed MVCC
revision, Environment head, next stable item, count, and rolling digest. Each
item is at most 16 KiB. A page returns the largest ordered prefix that fits both
the requested 1-through-40 item limit and the 768-KiB response limit. Only the
complete final page carries `impact_token`. Compaction or any changed bound
dependency returns `state.conflict` and requires a new sequence from page one.

Interactive `volume remove` follows every deletion-impact cursor at the pinned
revision and validates the joined count and rolling digest. It displays all
mount, active Backup-policy, and historical Recovery Point consequences, then
requires the operator to type the immutable key. Non-interactive use requires
both `--impact-token` and `--confirm-key`. There is no `--force` flag. The
Console uses the same page sequence, final token, key confirmation, Task, and
six removal checkpoints.

Entry requests use one discriminated source model:

```json
{
  "type": "env",
  "key": "DATABASE_URL",
  "source": {"kind": "fact", "attach_id": "att_01J...", "grant_attach_id": "att_01K...", "fact": "pg16_URL"},
  "exposure": ["api"],
  "secret": true
}
```

`POST /entries` carries the stable owner as `environment_id`; mutable
Environment labels never enter the request body. Literal Entry values are
valid UTF-8 and at most 256 KiB. The CLI permits `--literal` only for
non-secret desired-state values. A secret literal is read from
`--value-file PATH|-` so plaintext does not enter argv or shell history.
`--fact-grant-attach` maps the optional `grant_attach_id` field without
changing the flat JSON source shape.

`PATCH /entries/{id}` carries the complete mutable desired state as exactly
`{source, exposure}`. Entry type, key or path, secret storage class, and file
ownership are absent because they are immutable. `entry edit` requires one
explicit source and either `--all` or one or more `--service` values; it reads
a secret literal only through `--value-file PATH|-`.

`POST /entries/bulk` and `entry bulk-edit` are the one atomic bulk-upsert
operation for literal environment variables. The CLI reads `KEY=value` lines
from `--file PATH|-`, splits on the first `=`, preserves empty values and
additional `=` characters, and ignores blank lines and trimmed `#` comment
lines. It requires exactly one of `--plain` or `--secret`; all items share the
selected exposure. The API body is
`{environment_id,entries:[{key,value}],exposure,secret}` and returns
`202 {task_id,entries}`. Matching keys retain their ids, new keys are created,
omitted Entries are unchanged, and the full request fails before publication
on duplicate or invalid keys or an existing storage-class mismatch. The
operation accepts 1 through 200 Entries and at most 1 MiB of key/value bytes;
each value retains the normal 256 KiB Entry limit.

Literal, `secret_ref`, and `fact` sources are mutually exclusive. Fact
references remain live and are resolved by the Controller during render.
For `type: file`, `path`, numeric `uid`, and numeric `gid` are all required;
both ownership fields remain required when their explicit value is zero.
Their accepted range is 0 through 4294967294, matching the Agent's numeric
ownership policy and reserving the invalid all-ones sentinel.
`type: env` rejects `uid` and `gid`. The CLI mirrors this with mandatory
`--uid` and `--gid` on `entry add --type file`.

## 5. The 1:1 rule

### Component Capability integration boundary

Component implementations never receive a private Controller API. The existing
resource operations are generic Groundplane Capabilities, and the same
validated use cases apply operator requests and registered Component intents.
`/routes` therefore remains the HTTP-router-neutral Route resource; no
`/caddy/*`, `/nginx/*`, or `/traefik/*` backend route exists.

The external machine boundary is REST/OpenAPI. The CLI is a human-facing
generated client and is never invoked by a Component. MVP registrations are
statically linked pure planners through `component-sdk`; no install, register,
upload, remote-plan, or plugin-token endpoint exists.

`GET/PUT /components/{id}/config` uses a strict generated discriminated union
for the closed registered catalog, never a free-form object. Component reads
project implementation identity, provided capabilities, granted capability
operations, product owner, desired state, and observed state without exposing
planner data or backend records. The Router read is capability-based
(`http_router`, `edge_tunnels`), even though Caddy and Cloudflare Tunnel are
the only MVP implementations.

Component reads include `config: null` while a Component is disabled or
unconfigured. An enabled Component is configured and has a non-null `config`
containing exactly one complete variant: Caddy (`zone_id` and optional
`caddyfile_template`), Cloudflare Tunnel (`secret_id`), or CoreDNS
(`upstream_auto`, `upstream_resolvers`, `forwarders`, `tailnet_delegation`, and
required `corefile_template`). CoreDNS `component config set` requires
`--template-file <path|->`; `-` reads bounded stdin. CoreDNS resolver and
forwarder arrays are always JSON
arrays, including when empty. The CLI prints the same union and does not
canonicalize invalid input before the Controller validates it.

The `GET /components/{id}/config` response is the generator-safe envelope
`{config:<variant>|null,managed_files:[...]}`; its nested `config` is null under
the same disabled or unconfigured rule, and `managed_files` is always an array.
Each generic managed-file projection contains `path`, the durable authored
`template`, and the Controller `rendered` output. CoreDNS returns
`/etc/groundplane/coredns/Corefile`, derived side-effect-free from durable
config, the already-persisted host resolver baseline, and current host
resolution through the registered renderer. Other Components return `[]`
until they provide the generic projection. `component config show` prints both
fields; `component config set` and the existing PUT remain the sole mutation.

Cloudflare Tunnel config reads return only `{secret_id}`. Config replacement
accepts exactly one write shape: `{credential:{mode:"existing",secret_id}}` or
`{credential:{mode:"new",secret_name,token}}`. `new` creates a Project env-var
Secret whose `key` is `secret_name`, then persists only its stable id. The token
is write-only. The selected Secret may be owned by the Environment's Project or
the Platform. Deleting a Secret referenced by an enabled Cloudflare Tunnel
returns `resource.in_use`.

Registered planners may return authorized typed intents for Services, Routes,
Volumes, Secrets, Scripts, Backups, Networks, Entries, and Tasks. Groundplane
assigns ids, validates ownership and grants, and atomically publishes the
result. Secret operations expose metadata and opaque references only.

An Environment has one logical `http-router`. Routes remain valid desired state
when it is disabled and report `unserved`. Create and edit return
`202 {route,task_id}`. With no enabled router, that Controller Task has no Agent
effect. Enable applies all stored Routes; disable removes the entry point and
preserves them.

Ordinary resource collections exclude Component-managed resources. The owning
Component detail returns them grouped by capability, and topology reads may
include them with an explicit managed marker. A caller with a known stable id
may read the resource detail. Direct mutation is forbidden. Controller and
Agent never appear in Component catalog or projection responses.

Every operator-facing Controller capability has exactly one Console action,
one CLI command, and one API endpoint. If one of those three surfaces is
missing, it is a contract bug. The acceptance test ("the same operations work
from the CLI with no Console session involved") enforces the operator
direction.

The following are outside the parity set because they are not
operator-facing Controller capabilities:

- local process commands: `controller serve` and `agent-run run`;
- same-host diagnostics: `controller key show` and `controller etcd show`;
- local CLI tooling: `version` and `completion`.

These exceptions are closed and explicit. Adding a new exception is a product
contract change, not a convenient way to bypass the Console.

### Agent read representation

`GET /agents` and `GET /agents/{id}` return the Controller-owned Agent read
model: `id`, immutable `enrollment_task_id`, `host`, `status`, nullable
`version`, `labels` as a JSON object, nullable durable `ready_at`, nullable live
`last_report_at`, and durable `in_flight` Task assignment count. Both timestamps
are absolute UTC RFC3339 values. `ready_at` is the first authenticated Ready and
remains null while provisioning; `last_report_at` is current-process session
telemetry. Status is projected exactly as follows: provisioning to `pending`;
ready with a current-generation authenticated non-stale Ready session to
`healthy`; ready without one to `degraded`; deleting to `stopped`.
`GET /agents/{id}/config` returns `pull_interval_seconds`,
`max_concurrent_tasks`, and the keyed `labels` object. CLI `--label` values are
repeatable `key=value` pairs.

`PUT /agents/{id}/config` is complete replacement and returns the committed
config with `200`. For a connected Agent, the Controller pauses new dispatch,
drains existing assignments under the old concurrency limit, sends the update
at the first fully idle Ready, and resumes only after the Agent re-advertises
capacity under the replacement. Disconnected Agents receive the replacement
on their next authenticated connection.

## Connector request contract

`POST /api/v1/connectors?environment=<environment>` and its `connector add`
command require `name`, kind `s3-compatible`, `endpoint`, `bucket`, `region`,
Boolean `path_style`, and exactly the credentials `access_key` and `secret_key`.
`prefix` is optional. Each credential supplies exactly one `secret_ref` or
direct write-only `value`. Connector responses return `path_style` and redacted
credential source metadata, never direct values. See ADR 0045 for the complete
validation and persistence contract.
## Durable hierarchy delete operations

| Console action | CLI command | REST endpoint | Operation kind |
|---|---|---|---|
| Delete Tenant | `groundplane tenant delete <tenant>` | `DELETE /api/v1/tenants/{id}` | `tenant.delete` |
| Delete Project | `groundplane --tenant <tenant> project delete <project>` | `DELETE /api/v1/projects/{id}` | `project.delete` |
| Delete Environment | `groundplane --tenant <tenant> --project <project> environment delete <environment>` | `DELETE /api/v1/environments/{id}` | `environment.delete` |

All require `Idempotency-Key`, return `202 Accepted` with the authoritative
Controller Task (`type=remove`), and reconcile through Task show, event-watch,
retry, and abort. There is no second cascade endpoint. Hierarchy projections
include nullable `deletion_task_id`; non-null means tombstoned but readable
until parent-last finalization. Different-key concurrency, descendant mutation
under a tombstone, exact reverse-reference fences, and expired recovery cursors
use the normal RFC 9457 conflict response.
