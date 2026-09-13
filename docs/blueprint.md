# Groundplane Blueprint - the spec format

HTTP router `settings.alias` optionally declares one lowercase DNS label
(1–63 characters, alphanumeric ends). The alias exists on the first selected
Zone only and must not collide with another Service name or alias there.
Empty or omission declares no custom alias. It does not rename the managed
Service or change ports; disable/re-enable and updates preserve it.

The Groundplane Blueprint is the desired-state contract. It is a
Compose-compatible superset: Docker Compose supplies the base grammar and
Groundplane adds a namespaced `x-gp-*` grammar for identity, facts,
attachments, dependencies, releases, and Controller-owned operations.

This document is the grammar contract. `mvp.md` is the product contract,
`api-cli.md` is the human API contract, and `architecture.md` is the
implementation contract.

## The layers

Groundplane has one authored document and several derived representations:

```text
Blueprint envelope + Compose body + x-gp-* extensions
                         |
                         v
              typed Controller desired state
                         |
                         v
           durable records + render/task plan
                         |
                         v
      standard Compose + generated files + execution metadata
                         |
                         v
                    Agent applies
                         |
                         v
                  observed host state
```

The authored Blueprint is the operator's intent. Controller records hold
values created once and carried forward. Rendered Compose files, env files,
Corefiles, Caddyfiles, and host paths are generated artifacts. The Agent
never becomes a second decision-maker.

## The laws

1. **Compose is the base grammar.** Standard Compose keys retain their
   Compose meaning. Groundplane does not clone or reimplement services,
   networks, volumes, healthchecks, mounts, logging, or other standard
   Compose structures.
2. **`x-gp-*` is Groundplane's extension namespace.** Compose ignores these
   fields; the Controller validates and interprets them. Groundplane is not
   limited to Compose's expressiveness: an extension can describe any typed
   Controller behavior, including cross-project dependencies, backing-service
   provisioning, facts, backups, and reconciliation policy.
3. **The Controller is the only interpreter.** The Agent and Controller share
   the execution contract, but the Agent receives a compiled procedure. It
   does not independently resolve desired state, secrets, facts, or global
   dependencies.
4. **The generated Compose document is executable.** The Controller expands
   every Groundplane-only decision into standard Compose fields or a typed
   Agent task before application. The result is validated with Compose's
   parser/config validation. Each generated Compose project has one
   canonical Compose file; internal fragments are compiled before dispatch.
5. **Stable ids are machine identity.** Names and slugs are labels and
   lookup conveniences. Physical directories, Compose project names,
   generated env-file names, Docker labels, and durable references use ids.
6. **No secret values cross the desired-state boundary.** A Blueprint may
   reference a secret, fact, or generated credential, but never contain its
   value.
7. **Every operation is idempotent.** Applying a document twice converges to
   the same Controller state. A retry reconciles against labels and observed
   state before repeating a step.

## Envelope and placement

Every Blueprint document starts with a Controller envelope:

```yaml
kind: environment
schema: 1
metadata:
  tenant: acme
  project: storefront
  environment: production
```

The envelope is stripped before the remaining document is passed to the
Compose parser. This keeps the Controller's document discriminator and
addressing rules separate from the Compose grammar without duplicating the
Compose grammar.

`kind` values are `environment`, `backing`, `connector`, `project`, and
`tenant`. `schema` versions the Groundplane envelope and extension grammar.

Connector documents are environment-scoped resources:

```yaml
kind: connector
schema: 1
metadata:
  name: r2-backups
  tenant: acme
  project: storefront
  environment: production
connector:
  kind: s3-compatible
  endpoint: https://<account>.r2.cloudflarestorage.com
  bucket: groundplane-backups
  prefix: backups/
  region: auto
  credentials:
    access_key: {secret_ref: R2_ACCESS_KEY_ID}
    secret_key: {secret_ref: R2_SECRET_ACCESS_KEY}
```

Connectors are environment-scoped only. The tenant slug/project
slug/environment name chain resolves the owner to its stable environment id
before storage. A
backup policy resolves its connector name strictly within that environment.
An enabled policy with no connector, or with a missing connector, is a
validation error; a disabled policy may omit the field. There is no project or
platform connector and no fallback. Connector credentials are references or
encrypted direct values and are never emitted into Compose.

Environment documents live at:

```text
environments/<tenant-slug>/<project-slug>/<environment-label>.yaml
```

The placement is human-facing. The Controller resolves the document to the
stable tenant, project, and environment ids before storing it. A rename is an
explicit Controller operation, not an accidental delete/create caused by a
new path.

### Canonical authoring projection

`GET /environments/{id}/blueprint` reconstructs one canonical single-file YAML
document from the selected immutable desired projection and current
Blueprint-owned extensions. It is an authoring projection, never a runtime
export. It includes only fields accepted back by the parser and therefore
excludes stable Environment and Task ids, generated Docker names and paths,
render generations, observations, Task state, and secret plaintext. Stable
resource references that are themselves authored decisions, such as
`secret_id` and `secret_ref`, remain present.

The canonical projection preserves semantics, not the submitted source layout:
comments, aliases, anchors, and multi-file boundaries are not reproduced.
Import still accepts the closed multi-file bundle. Multiline Script bodies are
emitted as YAML literal block scalars under `x-gp-scripts`.

Explicit Script contexts export Volume and Entry grants through their immutable
Blueprint keys, preserving targets and each read-only decision. Export never
substitutes a mutable Volume slug, emits a generated resource id as a key, drops
a grant, or adopts an API-owned Entry. A missing or non-Blueprint Entry key makes
that context unrepresentable and returns `validation.failed`; repair the Script's
grants through its normal edit action before exporting that Blueprint.

The response carries the selected desired revision and an equivalent `ETag`.
Validation and apply require that revision through `If-Match`. Revision `0` is
the explicit initial authoring revision before the first desired publication.
A mismatch is `state.conflict` and performs no write.

`POST /environments/{id}/blueprint/validate` parses the exact same closed
bundle accepted by apply and returns a typed `create | update | retain` diff.
`retain` means the submitted document omitted an existing resource and
Groundplane will preserve it. Validation never publishes a desired revision,
Task, marker, materialization, or runtime effect.

## Compose base document

After the envelope, an environment Blueprint uses standard Compose keys for
standard container topology:

```yaml
x-gp-network-pool: 10.0.0.0/23

services:
  api:
    image: storefront-app:sha-9f3c1ad
    networks: [frontend, backend]
    healthcheck:
      test: [CMD, curl, -f, http://localhost/up]
      interval: 10s
      timeout: 3s
      retries: 5
    environment:
      APP_ENV: staging
    env_file:
      - /var/lib/groundplane/secrets/.env.env_01J...
    expose: ["8080"]

networks:
  frontend:
    internal: true
    ipam:
      config: [{subnet: 10.0.0.0/25}]
  backend:
    internal: true
    ipam:
      config: [{subnet: 10.0.0.128/25}]
```

The Blueprint may use the Compose vocabulary directly: `services`,
`networks`, `volumes`, `environment`, `env_file`, `healthcheck`, `command`,
`restart`, `logging`, `deploy`, `depends_on`, `expose`, mounts, aliases, and
resource limits. The Controller may fill or override generated fields while
rendering the final document.

Valid native Compose fields are preserved even when Groundplane has no
specialized UI for them yet. The Controller keeps the parsed Compose value in
the render model and applies only explicit Groundplane product policies. A
valid Compose field is never silently dropped because the Console does not
currently expose it.

Native Compose and Groundplane-managed fields are additive. The operator owns
authored native values; the Controller owns values generated from
`x-gp-*`. Conflicting managed network, alias, route, resource, or attachment
definitions fail validation rather than being silently overwritten. Duplicate
entries at the same environment/service scope also fail validation.

The intentional environment layering rule is not such a conflict: a service's
native `environment` value may override a key from the inherited Groundplane
all-services `env_file`, following the documented Compose precedence. That
override is explicit in the service document and is visible to the operator.

Groundplane's domain term **network zone** maps directly to a Compose
`network`. There is no second zone topology grammar. A zone's name, internal
flag, subnet, and service membership become the corresponding Compose network
definition and service `networks` entries. Groundplane may add ownership and
stable-id metadata with `x-gp-*`, but Docker networking remains Compose's
native model.

`x-gp-network-pool` is required and is a pure operator decision. It reserves
one IPv4 CIDR for the Environment but never renders a Docker network. Every
managed Zone has exactly one explicit IPv4 subnet in
`ipam.config[0].subnet`; that subnet must be contained by the Environment pool
and globally non-overlapping. Network name, subnet, `internal`, and ownership
are immutable after creation. A topology that needs different network settings
adds a replacement Zone, moves Service memberships, and removes the old Zone.
Changing `x-gp-network-pool` is valid only when the replacement still contains
every existing Zone and conflicts with no reservation.

Every managed Zone has one Controller-derived owner. A normal Environment Zone
stores `owner_kind: environment` and that Environment id. A backing service's
`create_network` Zone stores `owner_kind: backing_project` and that Project id.
Platform component networks are generated artifacts, not Zone resources. A
Blueprint never authors stable ids or ownership fields. A backing service
chooses either `create_network` (the default isolated network) or
`join_network` (an existing network in the same backing system):

```yaml
networks:
  backing:
    internal: true
    ipam:
      config: [{subnet: 10.200.0.0/28}]

services:
  postgres:
    networks: [backing]
  valkey:
    networks: [backing]
```

Consumers join the owner's network as a native Compose external network:

```yaml
networks:
  backing:
    external: true
    name: gp_net_net_01J...
```

The physical `name` is `gp_net_<stable-zone-id>`. The Controller rejects a
second owner or a consumer that attempts to redefine an owned network.
Backing-Zone removal is an explicit cascade Task whose confirmation names every
Attach, Service, and provisioned database; it runs each normal detach before
removing the network.

A backing service is also a normal Compose `service`. Its backing-project
environment is rendered as its own Compose project, with its adapter contract
and provisioning metadata carried by Groundplane extensions. A consumer
environment joins the backing service's Compose network as an external
network. `backing` is a Groundplane relationship and resource scope; it is
not a replacement for Compose's `services` or `networks` keys.

The Controller's internal `env_vars` collection is populated from authored
`x-gp-entry` values with all-services exposure. Compose has no
environment-level `env_file` key, so the renderer emits the canonical
environment file as an explicit `env_file` entry on every service.
Service-level `environment` and additional `env_file` entries remain native
Compose fields.

Backups are not a Compose topology field. A backup is a Controller-owned
policy plus selected sources, adapter procedure, connector, encryption state,
and recovery-point records. Groundplane may annotate the relevant Compose
resource with `x-gp-backup`, but the Controller compiles the policy into
typed Agent tasks rather than pretending that Compose schedules or performs
backups.

The exact Compose specification version supported by the implementation is a
Controller compatibility constant. A schema bump is required when a change
cannot be accepted without changing interpretation.

### MVP Compose surface

The MVP passes through the Compose fields Groundplane needs for the qa-workload
topology: `image`, `networks`, `volumes`, `environment`, `env_file`,
`healthcheck`, `command`, `restart`, `logging`, `deploy` resource limits and
replicas, `depends_on`, `expose`, aliases, service mounts, `secrets`,
`configs`, `labels`, and `annotations`. The renderer
must preserve these fields rather than projecting them through a lossy
Groundplane-only model.

The MVP deliberately applies product policy to some otherwise valid Compose
features:

| Compose feature | MVP behavior |
| --- | --- |
| `ports` | rejected for tenant services; Caddy's pinned bridge address is the documented router exception |
| `build` | rejected; workload deploys require images already present in the host Docker daemon and never pull or build as a side effect |
| native Compose `secrets` / `configs` | preserved as native Compose; Groundplane-managed values use `x-gp-entry` when scope, encryption, and volume-relative paths are required |
| `profiles` | preserved as native Compose; the Controller never silently activates or drops a profile |
| `include` / `extends` / multiple source files | accepted where supported, then resolved into the one canonical project file before Agent execution |
| other valid Compose fields | preserved by the Compose render model unless an explicit MVP safety/product rule rejects them |
| YAML anchors and aliases | parser features; resolved before validation and never used to bypass Groundplane validation |
| variable interpolation | resolved with Controller-controlled inputs; secret values are never used for interpolation in a persisted document |
| `deploy.rolling` | rejected as a release strategy in the MVP, even though standard Compose may accept related deploy fields |
| service `runtime_intent` | Controller-owned operational state outside Blueprint; bundle apply never authors or overwrites it |

These are product constraints, not limitations of `x-gp-*`. A future schema
can add support without replacing the Compose grammar.

### Closed bundle input

An environment apply submits one closed `BlueprintBundle` containing:

- one normalized relative root path whose document carries the Groundplane
  envelope;
- ordered Compose source paths, with the stripped root Compose body as the
  primary source;
- every source and companion file in a closed normalized relative path-to-byte
  namespace; and
- an explicit interpolation map containing no secret values.

The CLI adds ordered sources with repeatable `--compose-file RELATIVE_PATH`
flags and explicit non-secret interpolation with repeatable `--var KEY=VALUE`
flags. The stripped root Compose body is always the first source. The Console
uses a reorderable source list and a non-secret key/value table.

The human API encodes the same input as `multipart/form-data`: one
`application/json` part named `manifest` followed by one binary part for every
manifest path. The versioned manifest contains root, ordered sources,
interpolation, and a `files` array of `{path, part, size, sha256}` in normalized
path order. `part` is exactly `file-000001`, `file-000002`, and so on; every
declared `application/octet-stream` part occurs exactly once. Filenames,
undeclared parts, missing parts, reordered identifiers, and duplicate parts
are rejected.
Archives, unlisted parts, duplicate or absolute paths, `..`, symlinks, implicit
directories, ambient environment variables, implicit `.env`, host filesystem
fallback, remote includes, and network resources are rejected.

Limits apply to decoded bundle content: 64 files, 256 KiB per file, 768 KiB
aggregate bytes, and 240 UTF-8 bytes per path. Parsing permits at most 100,000
aggregate YAML nodes, 1,000 alias references, resolution depth 16 across YAML
aliases and Compose include/extends, and 512 aggregate resolved services,
networks, volumes, configs, and secrets. Exceeding a limit is
`validation.failed` with HTTP 422 and produces no state change.

The complete encoded lossless normalized projection, including its durable
schema and framing, has an exact 2 MiB ceiling. The Controller checks this
ceiling after normalization and before private revision staging or Task
publication. Expansion through aliases or normalization cannot bypass it.

Bundle-relative include, extends, `env_file`, `label_file`, configs, and
non-secret companion files retain their Compose meaning. A native bind source
must name bundled non-secret content, be read-only, and materialize below the
stable environment directory. Native secret grants are preserved, but secret
content enters only through a Groundplane secret reference and is never a
bundle file or interpolation value.

The Controller parses the root envelope, resolves stable ownership, then loads
the ordered sources through `compose-go` using only the closed namespace. It
stages the canonical audit stream and lossless normalized projection as
immutable `GDR1` chunks owned by one candidate revision. Publication changes
the Environment desired head and creates one Environment update Task, one
operation id, and one Agent assignment for the sealed candidate plan.
Canonical Compose, generated files, and secret materializations are recreated
from that revision and are never separate desired authority. The apply Task is
the sole execution for the apply and creates no child Deploy Task, hidden
Deploy request, second operation, or new operator action or endpoint.

A Blueprint apply is non-destructive. If the submitted bundle omits a
currently desired Service, Zone, Volume, or other owned resource, the candidate
normalized revision carries that resource forward from the pinned prior
revision. This rule preserves resources created through direct API, CLI, or
Console actions. If the Controller cannot preserve an omitted resource without
ambiguity, apply fails with `resource.in_use` and publishes nothing. Only the
explicit Remove workflow for that resource may publish a revision without it.
Omission never implies deletion or rename.

### Apply execution contract

The Controller derives candidate Releases only from the sealed candidate
projection. Implicit candidate selection includes newly introduced or materially
changed logical Services whose effective `runtime_intent` is `running`,
regardless of their authored replica count. Stopped or absent existing Services
remain stopped or absent, retain their desired changes for a later explicit
Release action, and are not implicitly applied or hooked. A Service that is a
Release Group member is excluded from implicit Blueprint candidate Release and
hook execution; Blueprint publishes its desired change only, while explicit
group Deploy/Rollback retains declared serial order and `on_failure` policy.

Matching `pre-deploy` and `post-deploy` Scripts are selected from the same
sealed Service and Release inputs. Within each phase, Services execute in
sealed dependency-topology order, breaking incomparable ties by current Service
slug bytes; Scripts for each Service execute by numeric `order` then current Script
slug bytes.
Manual Scripts never execute during Blueprint apply. Exact reapply, or an apply
with no selected changed candidate, executes no hooks. The complete selection
across pre-deploy, post-deploy, and possible `on-failure` execution is limited
to 16 hooks and 1,048,576 aggregate UTF-8 body bytes; phases do not receive
separate budgets, and a violation is rejected before Task publication.

The single Agent assignment carries these phases in order: materialize sealed
files and Entries; ensure candidate Volume leaves, perform required Attach
adapter provisioning and grants, and prepare Network resources without applying
those memberships to a consumer Compose Service; execute all selected
pre-deploy `RunScript` steps, with each runner reaching `cleanup_proven` before
the next; only then apply each targeted consumer's prepared Network memberships
as part of the candidate workload Compose apply and start that candidate without
readiness; execute all selected post-deploy `RunScript` steps with the same
cleanup barrier; execute `WaitHealthy`; and execute the sealed registered
Component actions. There is no hidden consumer Compose start or recreate before
the pre-hook barrier. This Blueprint-only split does not change standalone
Attach or Detach ordering. Compose apply does not intrinsically wait.

Only after the assignment succeeds does the Controller perform the
Controller-only atomic promotion of candidate Releases, the applied projection,
Component state, and Routes. A failure must prove exact predecessor restoration
or exact first-candidate absence before terminalizing; the desired head is never
rolled back, and an unproven candidate never becomes a serving Release or
Route. If proof is unavailable, the Task remains nonterminal/recovery-required.
Retry transfers the operation only when every selected Script execution is
durably `not_started`; `start_authorized` or later, or unknown Script state,
returns `script.retry_unsafe` through the existing retry action and recovery
continues the authorized execution.

A pre-deploy hook failure cannot have mutated an application candidate
workload. It leaves the serving and current-successful projections unchanged,
and runner cleanup completes before `on-failure`. Earlier materialization,
Volume, Attach, or Network effects remain durably accounted; their existence is
not permission to report the whole assignment as mutation-free.

The plan seals one immutable candidate Release procedure for each selected
Service. It declares the exact forward mutation anchors and the complete lawful
restoration alternatives `serving_predecessor` and `candidate_absence`; it
never fabricates `baseline` or derives restoration from a mutable current
Service projection. Claim selects exactly one alternative per Service from its
captured native Release authority and stores the complete sorted member map plus
its canonical digest with the assignment and execution epoch. An explicit native
absence witness selects absence even when an applied Environment record exists.
Missing native authority is an error, not permission to use that Environment
record. The exact nullable applied witness stays independently fenced. Mixed
recovery restores serving members from their captured native artifacts and removes
only first candidates, leaving unrelated runtime untouched. Independent observation
checks those same native artifacts. Host state can prove a selected target but
cannot choose it.

No mutation-capable candidate step begins until its running event is durably
accepted for that epoch. A reconnect remains forward only while the Controller
proves no effect-possible event or Script checkpoint exists. Once such evidence
exists, or a recovery record exists, dispatch is permanently recovery-only and
contains only the selected probe/compensation cursor. An unproven terminal ACK
privately records the original failure and recovery continuation while the same
public Task remains running. First-candidate recovery removes the exact
plan-owned candidate Service/Release set from its exact Compose project and
terminalizes only after typed candidate-absence proof. Desired head and failed
staged candidates remain.

That final restoration proof may close only selected Script executions still
exactly `not_started`, using Controller-owned
`parent_failure_before_start` with explicit runner/body absence. Script source
references then follow the existing bounded `normal_completion` path while the
Task, recovery record, assignment, timeout, writer, and operation stay live;
their final source transaction and the original Task terminalization are
atomic. A later or unknown Script checkpoint fails closed.

Before Blueprint candidate source release starts, the Controller validates the
complete terminal envelope against physical transaction limits ([Blueprint terminal contract](features/blueprints.md#atomic-terminal-publication)).
Normal source closure atomically captures the original Agent terminal report,
including its epoch, recovery digest, exact Task/assignment revisions and
observation time. Every release batch compares this temporary continuation.
Reconnect finishes it before effect classification or epoch increment, without
Script artifact resolution or Agent dispatch. A maximum-epoch closing report
can finish; a reconnect needing a new epoch still rejects exhaustion. Conflicting
reports and new events reject without writes; identical event retransmissions
remain read-only. Final completion deletes the report with the source root and
uses the existing generic terminal receipt for exact replay.

The existing Task detail step projection records each selected Script's
captured non-secret id and slug, while existing events carry its `step_id`
transitions; the parent Task terminal state records success or failure. This is
the durable evidence link, with no Script body or secret value exposed.

### Use native runtime primitives

Groundplane renders native Compose primitives first and adds Controller logic
only where Compose has no durable or cross-project concept:

```yaml
services:
  api__blue:
    deploy:
      replicas: 3
      resources:
        limits:
          cpus: "2"
          memory: 1G
    labels:
      com.groundplane.managed: "true"
      com.groundplane.kind: service
      com.groundplane.service-id: svc_01J...
      com.groundplane.plan-id: plan_01J...
      com.groundplane.slot: blue
      com.groundplane.render-generation: "42"
    secrets:
      - source: database-password
        target: database-password
    configs:
      - source: app-config
        target: /etc/app/config.yaml

secrets:
  database-password:
    file: /var/lib/groundplane/materialized/sec_01J...

configs:
  app-config:
    file: /var/lib/groundplane/materialized/cfg_01J...
```

`deploy.replicas` is the native desired replica count. Omission retains native
Compose meaning and normalizes to the explicit value `1` exactly once when the
authored input becomes Controller desired state. Every candidate Release then
seals a positive explicit count; a missing or zero count in durable Release
state is invalid and is never defaulted from current desired state or a Compose
projection. The Agent invokes Compose with the sealed count; Compose apply does
not intrinsically wait for running or healthy state. A typed plan adds
`WaitHealthy` when readiness is required. The Controller still stores the
desired count, observes each replica, and reconciles out-of-band removal because
local Compose only converges when invoked; it is not a long-running replica
controller.

Every replica receives service `labels`. Groundplane uses its own
`com.groundplane.*` namespace for managed identity, logical service, slot,
environment, release, and render generation. The `com.docker.compose.*`
namespace is reserved by Compose. `deploy.labels` are not used for local
Compose identity because they target a platform service object and do not
become local container labels. Service `annotations` carry supplemental,
non-identifying metadata such as render generation; labels remain the
ownership and lookup contract.

Groundplane entries compile to native Compose resources by type:

| Entry | Native Compose output |
| --- | --- |
| plain env variable | service `environment` or generated `env_file` |
| secret env variable | generated `env_file`, materialized by the Agent |
| plain file | top-level `configs` plus an explicit service grant |
| secret file | top-level `secrets` plus an explicit service grant |
| fact reference | resolves to one of the four destinations above |

Local Compose file-backed secrets/configs are transport and mount primitives,
not an encrypted secret manager: current Compose uses host bind mounts or
pre-start copies. Groundplane therefore materializes source files from its
encrypted store, controls cleanup and rotation, and uses native `secrets` or
`configs` grants for service visibility. Swarm-specific `external`, driver,
and template-driver resources are capability-checked and are not assumed to
exist in the local runtime.

## Groundplane extensions

Groundplane extensions use the `x-gp-*` prefix. They are typed fields, not
an arbitrary shell escape hatch. The Controller rejects an unknown extension
where the selected schema does not permit it.

The initial extension families are:

| Extension | Meaning | Compiled into |
| --- | --- | --- |
| `x-gp-resource` | stable resource kind/id and ownership | Docker labels and task context |
| `x-gp-slug` | mutable public slug on a top-level Volume | normalized Volume identity; no Compose runtime field |
| `x-gp-release` | authored default strategy and failure policy only | immutable candidate Release intent, execution summary, and separate serving/current-successful projections |
| `x-gp-release-groups` | named explicit coordinated releases of multiple logical services | one task and lock with per-service release records |
| `x-gp-adapter` | backing-service adapter contract and provisioning knowledge | adapter tasks and fact records |
| `x-gp-network` | stable network identity and ownership | native Compose network definition or external join |
| `x-gp-attach` / `x-gp-attachments` | Backing Service, one consumer Service, credential source, grants, and fact sources | external network joins and owner-only provisioning tasks |
| `x-gp-fact` | a reference to a computed adapter fact | `environment`, env files, or generated files |
| `x-gp-entry` | environment variable/file source and exposure | `environment`, `env_file`, or mounted files |
| `x-gp-exposure` | whether a value reaches one service or all services | env-file membership and service environment |
| `x-gp-depends_on` | service dependency within one environment | Compose `depends_on` where representable |
| `x-gp-requires` | Controller resource/task prerequisite | Controller task DAG |
| `x-gp-route` | provider-neutral route target and ingress behavior | selected HTTP-router Component Capability state |
| `x-gp-components` | registered implementation choice, enablement, and typed capability configuration | validated Component Capability intents and Groundplane-owned resources |
| `x-gp-backup` | backup policy and selected source references | scheduler state and Agent backup tasks |
| `x-gp-scripts` | Environment-scoped Script desired state | immutable Script body generations and typed Script execution steps |
| `x-gp-execution` | generated render generation and execution procedure | Agent validation and application |
| `x-gp-managed` | render generation and ownership metadata | execution bundle validation |

### Concrete extension fields

These are the initial field contracts. The authored form contains decisions
and references; Controller-generated execution artifacts carry stable ids and
sealed Release authority separately rather than adding mutable runtime state to
the authored extensions.

`x-gp-scripts` is an Environment-level map whose key is the immutable authored
reconciliation key. Duplicate YAML keys are invalid. Reconciliation keys and
slugs are each unique within the Environment, stored without normalization,
1 through 63 ASCII bytes, and match
`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`. The map key is immutable identity;
`slug` is a renamable label whose raw ASCII bytes break equal-order ties.
Map values contain the four required fields shown below and the optional
`order`and`execution`fields defined afterward; unknown fields are invalid:

```yaml
x-gp-scripts:
  migration-hook:
    slug: migrate
    service: api
    when: pre-deploy
    script: |
      php artisan migrate --force
```

`service` names one logical Service in the same Environment and resolves to its
stable id. `when` is exactly `manual`, `pre-deploy`,
`post-deploy`, `pre-rollback`, `post-rollback`, or `on-failure`. `script` is
required, non-blank valid UTF-8 without NUL, and at most 65,536 encoded bytes.
There are no authored parameters, arguments, timeout, interpreter,
working-directory or environment overrides. Numeric user selection exists only
inside the complete explicit execution context below. Literal secret values are
invalid. `x-gp-scripts` clean-replaces the superseded `scripts.<name>` and
task-hook Script forms; neither superseded form is accepted.

`order`is an integer0through65535, default0. Selected hooks for each logical
Service and phase execute by order ascending, then current Script slug bytes.
Cross-Service dependency topology and Release Group order are unchanged. Order
does not select a prerequisite or cause an unselected/manual hook to execute.

Omitted`execution`inherits the existing sealed Service/Release projection.
`execution: {mode: inherited}`explicitly selects the same behavior and accepts
no other fields. An explicit context is a complete replacement:

```yaml
execution:
  mode: explicit
  image: registry.example/setup@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  user: "0:0"
  volumes:
    - volume: storage
      target: /data
      read_only: false
  entries: [SETUP_INPUT]
```

`image`must be a repository reference pinned by a lower-case SHA-256 digest,
already available on the Agent host; local Docker ids and mutable tags are not
authored image authority. `user`is canonical decimal`uid:gid`, both32-bit unsigned
integers, with no leading zero except zero itself. There are at most32Volume
grants and64Entry grants. Optional omitted lists grant nothing; duplicates fail.
`volume`names the immutable Compose Volume key in this Environment;`entries`
names immutable`x-gp-entry`map keys, each exposed to the associated real Service.
Volume targets are canonical absolute container paths, excluding root, traversal,
reserved kernel/system paths, the Script-body target, overlapping mounts and
Entry file targets. Reserved targets include`/proc`,`/sys`,`/dev`,`/run`,
`/var/run`,`/bin`,`/sbin`,`/usr`,`/lib`,`/lib64`, the Docker-managed files
`/etc/hosts`,`/etc/hostname`,`/etc/resolv.conf`and`/groundplane-script-body`.
Ancestors and descendants of these paths are excluded too; component-prefix
siblings are not. Application subtrees such as`/etc/tls`are allowed. Targets
must be valid UTF-8 without control characters. Each Volume's`read_only`boolean
is explicit. Explicit mounts disable automatic copying of image files into an
empty Volume; initialization belongs to the authored Script. Host paths,
Docker sockets, networking and all unknown execution fields are forbidden.
An explicit runner has no network or inherited Service runtime/environment;
its working directory is`/`. Image defaults and only selected Entry values are
available. The Controller seals the declared grants and separately resolved
execution-image identity before any Task is executable (ADR0076).

TLS-first provisioning targets a real consumer with a`pre-deploy`Script using
`/bin/sh`and author-supplied`openssl`. It can inherit the consumer context or use
the explicit setup image/user and read-write Volume grants above while consumers
mount output read-only. The Script owns staging, validation, ownership
and modes, idempotent no-op for an existing valid bundle, and atomic publication
into a declared read-write Volume. Consumer mounts of that Volume are
read-only, and `x-gp-entry` must not own the same output subtree. Groundplane
does not supply the tools, assume they exist in every image, validate PKI
policy, or guarantee arbitrary Script output atomicity. Live automatic
certificate rotation is absent; any future rotation is a separate explicit
operation contract.

`x-gp-resource` is generated metadata and is never required in an authored
Blueprint:

```yaml
x-gp-resource:
  kind: service
  id: svc_01J...
  parent:
    environment_id: env_01J...
```

The Controller emits the same identity into Docker labels. The Agent uses it
to reject a task aimed at a different resource.

`x-gp-slug` is valid only on a top-level Volume entry. The Compose map key is
the immutable authored key. The optional extension supplies the mutable public
slug:

```yaml
volumes:
  app-data:
    x-gp-slug: application-data
```

The slug uses 1 through 63 lowercase ASCII letters, digits, and `-`, and it
starts and ends with a letter or digit. If a new Volume omits `x-gp-slug`, its
Compose key must satisfy that grammar and becomes the initial slug. The key is
1 through 255 ASCII bytes from `[A-Za-z0-9._-]`, is neither `.` nor `..`, and
contains no separator or NUL. Unknown `x-gp-*` Volume fields are invalid.

For an existing key, changing `x-gp-slug` edits only the slug and keeps the
stable id, host path, and mounts. Changing the map key is a remove plus create,
not a rename, and Blueprint apply rejects it. Service mount sources resolve
the immutable key to `volume_id`; durable mounts and Backup sources never
reference the mutable slug. Omitting an existing Volume is non-destructive and
cannot remove its data. The operator must use the explicit Volume removal
workflow first.

`x-gp-release` carries only the Service's authored defaults:

```yaml
x-gp-release:
  default_strategy: blue-green       # authored: blue-green | recreate
  on_failure: switch_back            # authored default: switch_back | leave_active
```

The authored standard Compose `image` field supplies the image name and initial
tag or digest-qualified requested reference. A Deploy-selected tag combines
with that authored name as provenance for a new candidate Release. Before
publication, the Agent resolves that candidate requested reference once to an
exact local Docker image id. Final workload Compose uses only that sealed local
id; a mutable name or tag and optional registry metadata are never runtime
authority.

One Blueprint operation may select at most 64 unique typed workload-image
selectors across its complete candidate and prior set. The Controller counts
the whole set before any desired-revision, Release-ledger, or private-source
staging and before idempotency or Task publication. A larger set returns
`validation.failed` with HTTP 422 and publishes or stages nothing; it is never
chunked into multiple Agent exchanges.

The release ledger is per logical service, never one environment-wide
`release` field. Conceptually, its immutable intent, separate execution
summary, and Service projections contain:

```yaml
intent:
  id: dep_01J...
  service_id: svc_01J...
  candidate_workload:
    requested_reference: docker.io/example/storefront-app:sha-9f3c1ad
    local_image_id: sha256:<64-lowercase-hex>
    replica_count: 1
  tag: sha-9f3c1ad
  strategy: blue-green
  slot: green
  on_failure: switch_back
  originating_task_id: task_01J...
execution_summary:
  release_id: dep_01J...
  outcome: completed
  final_serving_release_id: dep_01J...
service_projection:
  serving_release_id: dep_01J...
  current_successful_release_id: dep_01J...
```

The checkpoint transition is Task-driven:

```text
pending -> running -> candidate_healthy -> switching -> serving -> post_hooks -> completed
             |              |             |          |             |
             +--------------+-------------+----------+-------------+-> failed / timed_out / aborted
                                              |
                                              +-> compensating / recovery_required -> recovering
```

There is no stored `active` or `superseded` Release status. Runtime evidence
advances `serving_release_id`; successful completion separately advances
`current_successful_release_id`. Under `leave_active`, a failed candidate may
remain the serving Release while the older completed Release remains current
successful. Steady-state rendering therefore follows `serving_release_id`, not
an assumption that current-successful always serves. Rollback creates a new
Release whose candidate workload seal is copied from the Controller-selected
historical Release; it never mutates history or resolves the historical tag to
current bytes.

Tasks have immutable attempt ids and a stable operation identity:

```yaml
task_id: task_01J...          # this execution attempt
operation_id: op_01J...      # same across retries
retry_of: task_01J...        # set on a retry
plan_hash: sha256:...
```

A retry creates a new task record with the same operation target, parameters,
plan hash, and idempotency key. The Agent reconciles labels, checkpoints, and
observed state before repeating any step. Recomputing a different operation is
an explicit new action, not a retry.

Service deployment is independent by default. Optional entries in
`x-gp-release-groups` make coordination explicit; they are never inferred from
shared image names or service names. The map key is the canonical group name;
an entry never repeats that identity in a `name` field. One environment may
declare multiple groups:

```yaml
x-gp-release-groups:
  realtime:
    services: [api, worker, scheduler]
    order: [api, worker, scheduler]
    tag: sha-9f3c1ad
    on_failure: switch_back
  maintenance:
    services: [api, scheduler]
    order: [scheduler, api]
    on_failure: leave_active
```

The map key must be valid UTF-8, contain at least one non-whitespace character,
and contain no control characters. The MVP does not impose a narrower slug
grammar. Every `services` entry must be non-blank, unique, and name a service
in the resolved enabled Compose project. An omitted `order` normalizes to the
listed `services` order; an explicit `order` must be an exact permutation with
no blank, duplicate, additional, or omitted member.

A group is one operator action with one task lock and `on_failure: switch_back |
leave_active`, which defaults to `switch_back`; members execute serially, not as
a simultaneous or atomic distributed switch. A request `tag` overrides the
persisted group tag for deploy; if both are empty publication rejects the group
deploy, with no member-current fallback. The selected deploy tag resolves
once against each member image in one all-or-nothing Agent batch before
publication and pins its exact host-local Docker image id; registry or manifest
metadata is optional and never substitutes for that local id. Each member
preserves its release history independently. Group rollback never uses the
persisted default and copies each selected historical Release's complete
workload seal instead of resolving its tag again. An optional rollback request
tag independently selects
each member's newest eligible historical Release that completed successfully and
reached serving with that exact tag and a tag different from its current
serving Release; omission selects the newest such eligible different-tag
Release for each member. If any member has no eligible source, publication
rejects the whole group rollback. Group `on_failure` overrides member defaults
for aggregate compensation. If a later
member fails, `switch_back` returns already-switched members to their previous
healthy releases; `leave_active` keeps them active for explicit rollback. Each
member still retains its own release records and observed state. Hooks remain
supported by ADR 0040: they target logical Services and execute once per logical
Release, never per replica; a migration binds once to its designated logical
member and there is no group-level hook resource.

`x-gp-release-group` is not part of the schema. The Controller rejects that
singular spelling as an unknown Groundplane extension; there is no alias or
compatibility decoder.

`x-gp-release-groups` is root-only. An override or included Compose document
that declares it is invalid.

For a releasable Service, the generated Compose project contains two physical
slot workloads and one stable Controller-owned proxy for the logical service:

```yaml
services:
  api:
    # Separate managed authority: compiled OCI index reference. The Agent
    # verifies its selected host-platform child manifest and local Docker
    # image/config id independently; neither is a workload seal.
    image: docker.io/library/caddy@sha256:<compiled-oci-index-digest>
    networks: [frontend, backend]
    x-gp-resource:
      kind: service-proxy
      logical_service_id: svc_01J...

  api--blue:
    # Requested-reference provenance: storefront-app:sha-old
    image: sha256:<sealed-old-local-image-id>
    networks:
      frontend: {}
      backend: {}
    x-gp-resource:
      kind: service-slot
      logical_service_id: svc_01J...
      slot: blue

  api--green:
    # Requested-reference provenance: storefront-app:sha-new
    image: sha256:<sealed-new-local-image-id>
    networks:
      frontend: {}
      backend: {}
    x-gp-resource:
      kind: service-slot
      logical_service_id: svc_01J...
      slot: green
```

The logical `api` endpoint belongs only to the stable proxy. Routed Caddy and
internal consumers never address a slot directly. Slot names are stable for
healthchecks and diagnostics. A switch is a typed atomic reload of the proxy's
sealed canonical JSON configuration, not a mutable alias or an image mutation
hidden inside Compose.

`recreate` renders the authored N physical workloads for the Service (N==1 is a
singleton). An addressable Service may retain its stable logical proxy, which
targets only the desired set; recreate never retains or creates a blue/green
workload pair, and the durable Release `slot` remains absent. A portless recreate
has the same topology without a proxy. The Controller seals both candidate and
prior topology artifacts, stops and removes the prior set before replacement,
and proves the exact desired set healthy. There is explicit downtime and no
duplicate Script execution per replica.

Changing strategy is an explicit topology transition. Any transition to or
from blue-green is rejected before mutation unless the sealed prior and
candidate workload counts are both exactly one. An accepted transition from
blue-green to recreate removes the sealed prior slot topology before applying
the desired set and proving the new serving state. An accepted transition from
recreate to blue-green creates the candidate slot topology, proves candidate health,
atomically switches the stable proxy, then removes the obsolete set. Retries
probe and compensate only against
the same sealed candidate and prior artifacts, so interruption at any boundary
converges idempotently instead of stranding both topologies. A Service with no
internal TCP port cannot safely select `blue-green`; publication rejects that
combination.

The blue-green deploy procedure is:

1. Select the inactive slot and render its candidate sealed local image id.
2. Run matching `pre-deploy` Scripts through the typed Release hook executor,
   with cleanup proven after each.
3. Start or recreate only that slot.
4. Run matching `post-deploy` Scripts through the same executor and cleanup
   barrier.
5. Observe its exact Release labels and pass its sealed healthcheck.
6. Atomically reload the stable proxy to the healthy slot and prove the sealed
   config digest, generation, Release id, and upstream.
7. Record the candidate as serving, then completed, for the logical Service.
8. Retain the old healthy slot for rollback.

If a pre-switch step fails, the stable proxy remains on the prior slot. If a
later member or post-switch checkpoint fails, the release's `on_failure`
policy decides whether the Agent atomically reloads each already-switched
proxy to its exact prior healthy slot in reverse order or leaves the new slot
active for an explicit rollback. The default is
`switch_back`. Either outcome preserves the failed release record. Rollback
uses the same procedure with the Controller-selected historical Release's
stored `local_image_id`; it never resolves that Release's requested reference.
No policy reverses
database migrations automatically. Recreate uses the same hook boundary:
run matching pre-deploy Scripts before candidate mutation, remove the prior
workload, apply and start the candidate, run matching
`post-deploy` Scripts, observe readiness, then acknowledge the recreate.
Blueprint apply uses the Apply execution contract above. The plan applies and
starts candidates only after all selected pre-deploy Scripts have cleaned up,
runs matching `post-deploy` Scripts,
waits for health only at `WaitHealthy`, then performs Component actions before
Controller-only atomic promotion. Explicit Deploy and Rollback retain their
typed hook paths, and explicit group Deploy/Rollback retain declared serial
order and `on_failure` policy.

`x-gp-adapter` belongs on a backing service. Networks, healthcheck, resources,
volumes, and restart policy remain native Compose fields. The managed
`postgres:16` adapter is the closed exception to authored native `image`: it
resolves the immutable Groundplane-managed PostgreSQL workload OCI index from
the embedded release record, and Blueprint cannot select or override its
repository, tag, digest, platform, helper, or contract version:

```yaml
services:
  postgres:
    networks: [postgres_net]
    x-gp-adapter:
      key: postgres:16
      prefix: pg16                   # optional adapter-prefix override
```

The adapter registry, not the Blueprint, supplies `requires`, provision and
detach operations, fact templates, and backup/restore strategy. `manual` is a
registry adapter with network-only attach behavior.

Valkey authentication is immutable backing-instance policy explicitly required at
backing creation (`username_password`, `password`, or `none`, with no default), not a consumer
Blueprint decision. `x-gp-attachments` inherits it and cannot downgrade it.
No-auth bindings still own/reuse HOST, PORT and credential-free URL facts and
network membership, but generate no credential or adapter provisioning step.
Their `credential.mode` keeps its existing new-binding/direct-owner meaning.
Password-only owners have distinct passwords on one shared default identity;
detaching an owner revokes only its password. See ADR 0068.

`x-gp-attachments` is authored at environment scope. Its key is the attach
name; it is not used to derive any database, role, password, network, or
container name:

```yaml
x-gp-attachments:
  api-db:
    backing_project: postgres
    backing_service: postgres
    service: api
    credential:
      mode: new
    grants: []
  worker-db:
    backing_project: postgres
    backing_service: postgres
    service: worker
    credential:
      mode: existing
      attach: api-db
```

`service` is singular. `credential.mode` is required and is exactly `new` or
`existing`. `new` rejects `credential.attach` and may declare up to eight
`grants`. `existing` requires `credential.attach`, rejects `grants`, and names
a ready credential-owning Attach in the same Environment and Backing Service.
Reference chains are rejected. Every Service still receives its own Attach and
network-membership edge, while all existing-credential dependents expose the
owner's fact sets. Duplicate edges collapse to one rendered network membership.

The attach name may be supplied manually or omitted. When omitted, the
Controller joins the current tenant slug, project slug, environment name, and
service name after lowercasing ASCII letters and replacing each run of `-`,
`_`, or `.` separators with one hyphen. It selects the lowest available name
from `base`, `base-2`, `base-3`, and so on. Attach names are lowercase ASCII
hyphen labels of 1-255 bytes; a numeric suffix truncates the base from the right
when needed. The selected name is stored on the attach as a mutable label;
future tenant, project, environment, or service renames do not recalculate it.
Renaming an attach is a separate explicit operation. Grants reference attach
ids/names, never mutable database strings.

After resolution, the Controller records and generated form add
`attach_id`, `backing_project_id`, `backing_environment_id`,
`backing_service_id`, `backing_network_id`, `credential_attach_id`, the
external network name, and the provisioned facts. Only a credential owner
stores fact values and adapter identity; dependents resolve those through the
owner.
The consumer Compose service receives the external network through standard
Compose `networks` syntax.

An Attach receives its durable id before reconciliation starts. A new
credential receives its generated identity before provisioning. Lifecycle is
`pending -> provisioning -> ready`, with terminal `failed` and
`detaching -> detached` paths. A failed provisioning task keeps the same
Attach id, database, role, password, and fact references. Retrying reruns
idempotent adapter repair against that identity. Existing-credential retries
preserve the direct credential-owner reference and reconcile only the network.
An owner cannot detach while dependent Attaches reference it.

Credential-backed identities preserve the exact Service name and append `_`
plus the lowercase first six characters of the Attach ULID's random tail. They
are limited to 63 bytes; therefore a Service name longer than 56 bytes is
rejected for a credential-backed Attach instead of being silently truncated.
Passwords contain exactly 32 cryptographically random bytes encoded as 43
unpadded base64url characters.

`detached` is the successful terminal outcome, not a retained desired record.
The Controller removes the Attach and its membership/grant indexes atomically
with the successful detach Task acknowledgement. A failed detach remains
visible and retryable.

`x-gp-entry` defines an environment entry when its source is not a literal
standard Compose value. It covers variables and files, plain and secret,
including fact-derived values:

```yaml
x-gp-entry:
  DATABASE_URL:
    kind: env
    source:
      fact:
        attach: api-db
        # grant: reporting-db # optional; selects that grant's additional fact set
        key: pg16_URL
    exposure: [api]
    secret: true
```

One `x-gp-entry` is one destination. Reusing a fact under different env keys,
file paths, or exposure scopes uses multiple explicit entries. Facts are never
auto-injected because an attach exists. A fact source is live: the Blueprint
stores the `{attach, grant?, key}` reference and the Controller resolves it again on
each render or materialization. A rotation or repair therefore updates every
declared destination without rewriting the Blueprint.

A secret literal on an environment Entry is stored as one subordinate
encrypted value owned by that Entry; it never creates a second public Secret
resource. Deleting the Entry atomically deletes its ciphertext. A
`secret_ref` instead names a reusable project or platform Secret resolved by
stable id or key. A key uses the project-then-platform fallback chain; a stable
id must name either the selected Project's record or a platform record. There
is no environment Secret scope or stop-propagation marker. Neither form places
plaintext in the Blueprint.

Every file Entry declares numeric `uid` and `gid`, including explicit zero
values. Ownership is never inferred from an image or defaulted by the
Controller. Env Entries omit both fields.

`exposure: [all]` means the entry is written to the canonical
`secrets/.env.<environment-id>` file and that file is attached to every
service. A service list creates service-specific generated files. The
Controller may instead compile a non-secret literal directly into native
Compose `environment` when that is the simplest representation.

Entry deletion is a desired-state transition, not a Service lifecycle action.
The Agent removes an applied file Entry or rewrites affected generated env
files from the remaining immutable generations before the Controller removes
the Entry record. The canonical all-services file remains present even when
empty; a service-specific file with no remaining values is removed. This
transition never runs Compose or recreates a Service, so a running process
retains its existing environment until a later deploy or reconciliation.

`x-gp-depends_on` is an environment-local extension over native Compose
`depends_on`:

```yaml
services:
  api:
    x-gp-depends_on:
      migrate:
        condition: service_completed_successfully
        phases: [deploy]
```

`condition` accepts Compose health/start conditions. `phases` accepts
`start`, `deploy`, `rollback`, or `always`. When no phase is specified, the
dependency applies to normal Compose startup. The Controller compiles the
service-local portion to native Compose and uses the phase for task ordering.
For a Release Group, its normalized order remains exact and serial: publication
rejects the operation when that order places a selected member before one of
its selected-member prerequisites for the active phase. The Controller never
silently reorders the group. A phase dependency outside the selected group is
sealed as prerequisite work in the same Release plan and never creates a child
Task or a second Release path.

`x-gp-requires` is for Controller resources, including resources in another
Compose project:

```yaml
x-gp-requires:
  - target:
      kind: backing-attach
      name: api-db
    condition: ready
    phases: [deploy]
```

`x-gp-requires` is a root list. Every item requires `target.kind`,
`target.name`, `condition`, and a non-empty `phases` list. In the MVP,
`target.kind` is exactly `backing-attach`; `condition` is exactly
`exists`, `ready`, or `completed_successfully`; and every phase is exactly
`start`, `deploy`, `rollback`, or `always`. There are no legacy aliases:
`healthy`, unknown values, duplicate requirements, duplicate phases, missing
targets, requirements on an Attach created by the same Blueprint Task, and
cyclic requirement graphs are rejected before staging.

The Controller resolves each authored Attach label to its stable Attach id at
one fixed pre-stage revision. The authored label remains in the normalized
projection for Blueprint GET, while the stable id is sealed for Task planning,
retry, and replay. The requirement is an edge in the same Environment Blueprint
Task DAG, ordered by stable target id and then stable step id; it never creates a
child Task or a second operation and never becomes cross-project Compose
`depends_on`. Only the Controller observes and gates conditions. The Agent
receives resolved ids and never resolves labels.

`x-gp-route` keeps hostnames and paths distinct:

```yaml
x-gp-routes:
  - hostname: app.example.com
    path: /app/*
    target: api
    target_port: 8080
    exposure: public
```

`x-gp-components` is the pure desired input for registered Environment
Components. The Router page is a UI grouping for ingress capabilities; it is
not a separate runtime model. Each key is a Component Capability. The
`implementation` selects one closed registered implementation, `settings`
contains portable capability decisions, and `implementation_config` contains
the strict typed variant for that implementation. The old implementation-keyed
form is invalid:

```yaml
x-gp-components:
  http-router:
    implementation: caddy
    enabled: true
    settings:
      zone_ids: [net_01J..., net_01K...]
    implementation_config:
      caddyfile_template: |
        {gp.routes}
```

Each registered Component has a stable id, implementation key, provided
capabilities, exact grants, enabled decision, strict typed config, and
health/reconcile state. The Blueprint contains decisions only: it never authors
generated Services, ids, addresses, artifacts, management ownership, grants,
Tasks, or observations.

Groundplane parses the selected typed variant, constructs a fixed-revision
capability planning view, and invokes the build-time planner through the public
Component SDK. The planner returns immutable resource and execution intents.
Groundplane independently validates and atomically publishes them through the
same Service, Route, Volume, Secret, Script, Backup, Network, Entry, and Task
capabilities used by the human API. The planner never imports backend code or
receives secret values. The Agent receives only sealed generic procedures and
does not run plugin planning code. Controller and Agent compile the same
immutable catalog. A Task identifies a catalog action and sealed artifact by
id and digest. The Agent verifies the matching catalog and definition digests,
Component ownership, image digest, labels, mounts, artifact digest, and
generation, then resolves the action to its fixed registered recipe. A Task
never contains an executable name, arbitrary arguments, shell text, host path,
or arbitrary URL.

The Tunnel token is materialized through a service-specific opaque secret
binding and is never placed in Compose or `x-gp-*`. The Cloudflare Tunnel
typed config contains ordered stable `zone_ids` and stable `secret_id`; direct token input is a
Console/API mutation that creates a Project Secret and is not Blueprint
desired state. Tunnel lifecycle is independent of `http-router`. Groundplane
starts or stops the connector and reports health, but does not configure its
DNS, public hostnames, ingress rules, origin targets, or protocol. A future source-registered
Traefik or Nginx implementation must reuse the HTTP-router capability, generic
Route API, and generic catalog action procedure; it does not add a new router
architecture or technology-specific backend endpoint. Caddy PROVIDES the
`http-router` capability and CONSUMES existing GP Routes; registered components
may produce authorized Route intents, but Caddy does not need to create Routes.

The Caddy component receives a pinned bridge address while enabled. Redeploys
retain that address. Disabling Caddy releases the allocation; re-enabling it
allocates a new address and regenerates the CoreDNS entries, Caddy origin, and
any generated tunnel-origin guidance. The address is durable component state
while enabled, not a permanent environment identity.

`zone_ids` is required for both Caddy and Tunnel: a nonempty ordered list of
distinct stable references to ordinary Zones in the same Environment. Caddy's
first Zone is primary and holds its one pinned address; secondary interfaces
are dynamic. A Route target must share at least one selected router Zone.
Tunnel joins exactly its independently selected Zones, requires at least one
non-internal Zone, and selects the first non-internal Zone as outbound gateway.
There is no implicit router-following attachment or default bridge fallback.
Removing any selected Zone while its Component remains enabled rejects.
The optional `caddyfile_template` is a complete Caddyfile, at most32KiB of
valid UTF-8 without NUL. Empty defaults to `{gp.routes}`; that aggregate marker
may occur once and expands to complete grouped site blocks. The old `{routes}`
marker is invalid. Full custom files use `{gp.route:HOST:PATH:FIELD}`, where
HOST/PATH select a declared Route by its immutable unique match and FIELD is
exactly `host`, `path` or `upstream`. An internal catch-all has an empty HOST.
The first and last separator colons delimit HOST and FIELD; colons in a valid
PATH remain part of the path. The root matcher `/` renders `/*`; upstream
renders the stable Service name and explicit target port. No runtime addresses,
serving slots or generated Route ids belong in this portable template input.

Every Route must be accounted for by its upstream reference or the aggregate
marker. Unknown/malformed GP references reject. Native Caddy placeholders such
as `{host}` and `{uri}` are untouched. A full custom policy can intentionally
deny request paths; it does not create Routes or change their target grants.
When editing Routes separately, the aggregate default provides an intermediate
valid template if a custom file needs corresponding reference changes.

For example, with a declared root Route targeting `api` at8080:

```caddyfile
http://{gp.route:api.example.com:/:host} {
  route {
    respond /internal/* 404
    reverse_proxy {gp.route:api.example.com:/:path} {gp.route:api.example.com:/:upstream}
  }
}
```

The existing Component config read returns the current template and rendered
managed-file preview without mutation. It is not draft validation or a live
observation. Complete staged output must pass Caddy validation before serving-file
replacement or candidate-router start, and again before reload;
invalid output retains the previous serving config. File-only edits reload the
existing router without recreating unchanged Component runtimes. Actual image,
mount, user and network changes still reconcile the affected runtime. Primary-Zone IPAM and
operator-owned LAN/provider configuration are unchanged.

`x-gp-backup` is environment policy, not Compose topology:

```yaml
x-gp-backup:
  enabled: true
  frequency: "*-*-* 03:15:00"
  keep: 3
  encryption: age
  connector: r2-backups
  sources:
    - kind: attach
      ref: api-db
    - kind: volume
      ref: app-data
    - kind: config
```

`frequency` uses the same bounded UTC grammar on every surface. Daily is
`*-*-* HH:MM:SS`; weekly is
`Mon|Tue|Wed|Thu|Fri|Sat|Sun *-*-* HH:MM:SS`. Fields use exact widths and
exactly one ASCII space; ranges, lists, repetitions, timezone suffixes, and
calendar dates are invalid.

`keep` is a YAML integer in the inclusive range `1..9007199254740991`
(`Number.MAX_SAFE_INTEGER`). Quoted strings, fractions, and values outside
that range are invalid; the upper bound keeps the authored value exact across
the Console, CLI, JSON API, and Blueprint surfaces.

The Controller evaluates the frequency grammar directly;
there is no systemd timer or validation subprocess.

The Controller resolves source ids, selects typed source procedures, schedules
run, and creates recovery-point records. No backup value is expected to be
understood by Docker Compose. A configured source has a stable source id. If
the source is removed from the active policy, no new points are created, but
existing recovery points remain addressable until retention pruning or the
owning Environment deletion cascade removes them. There is no operator point
delete in the MVP.

Source identity is `(environment_id, kind, resolved_target_id)`: attach and
volume labels resolve to their stable ids, while config resolves to the owning
Environment id. Authored refs are mutable resolution labels only. Durable and
public API source records use the resolved stable `target_id`; the Console may
display the target's current name. Active membership is the singleton policy's ordered source-id
set; removing a source preserves its catalog record, and re-adding the same
surviving target reuses the same `spt_` id. An enabled policy
requires a valid frequency, in-range `keep`, at least one source, and an existing
Connector owned by the same Environment. A disabled policy may remain
unconfigured and owns no active Connector reference fence.

`MaximumBackupPolicySources` is 12. Each stable source tuple owns exactly three
durable records: the source primary, its Environment ownership index, and its
`(environment_id, kind, target_id)` identity index. A maximum Blueprint may
therefore create 36 source-catalog records in its final publication; this is
not a three-source limit. The policy's ordered source ids remain the only
active-membership authority.

`MaximumEnvironmentBlueprintAttachCandidates` is 2. Only Attaches newly
introduced by the candidate count; retained and pre-existing Attaches do not.
New Attach grants and existing-credential references may target retained ready
credential-owning Attaches selected at the Blueprint fixed revision. Each such
reference carries the exact retained primary revision and deletion state into
the final transaction. Publication creates the reverse membership and rewrites
the retained primary unchanged so direct detach and Blueprint publication have
exactly one winner.
An Attach or Volume source may target an identity introduced by the same
Blueprint. The Controller validates that target against the sealed candidate
projection and publishes its identity in the same transaction as the Backup
Policy. It never pre-creates a candidate target. An Attach source still selects
only a credential-owning Attach, never an existing-credential dependent.

Connector creation remains outside the Environment Blueprint grammar. The
authored connector label must resolve to an already-existing Connector owned by
this Environment at the fixed validation revision, and final publication
fences its exact primary, owner index, and deletion state.

Every final Environment Blueprint publication is all-or-nothing. One dedicated
envelope contains the Environment desired head, Environment update Task and
queue/index authority, ADR 0021 marker, every candidate Release and Script
execution, every required staged physical Script source and candidate Attach or
Volume identity, and, when present, the Backup Policy, enabled-only Connector
reference, every missing three-record source tuple, and lazy age key. Direct
Backup Policy replacement retains [Backup lifecycle](features/backups/lifecycle.md)'s source pre-ensure behavior;
Blueprint creates no source record before its final publication transaction.
The applied Environment Compose projection is not in this envelope: it retains
its exact predecessor or absence until successful terminal Task acknowledgement.
Pre-deploy and post-deploy Script Network and Volume memberships use the
already-prepared immutable runner snapshot's key, positive revision, canonical
digest, and exact member ids instead of treating the candidate projection as a
staged source.

The exact legal operation shapes are:

| Blueprint shape | Comparisons | Success | Failure | Selected success | Selected failure | Full request |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| QA: eleven Release candidates, two hooks, two staged physical sources | 30 | 42 | 30 | 72 | 60 | 102 |
| maximum non-Backup Script | 44 | 98 | 44 | 142 | 88 | 186 |
| maximum non-Backup Script plus two candidate Attaches | 117 | 147 | 117 | 264 | 234 | 381 |
| maximum Backup-only | 152 | 90 | 152 | 242 | 304 | 394 |
| combined Backup plus Script maximum | 174 | 175 | 174 | 349 | 348 | 523 |

All counted operations are distinct and atomic. Removing or coalescing one
would discard desired-head, Task, marker, Release, Script, source, Attach, or
Backup authority. The Backup-only shape remains accounting evidence, not a
separate publication envelope.

Each comparison, success, and failure arm may contain at most 256 operations,
matching configured etcd semantics. The Controller also measures the actual
protobuf request and permits at most 1 MiB (1,048,576 bytes). A byte-fitting arm
of 256 is valid; 257 in any arm is invalid. The theoretical 512 selected-arm
and 768 full-logical-request counts are diagnostics only and are never
rejection limits.

Arm or byte overflow is rejected locally before etcd as
`validation.failed`/HTTP 422 and publishes no public state. A fitting
comparison that loses at etcd remains an atomic conflict: its fixed-revision
failure arm is selected and no success mutation commits. Staging, sealing,
Script source preparation/release, and other ordinary transaction limits remain
unchanged. Candidate terminal completion uses the distinct closed envelope
below, not final-publication authority. Ordinary `Store.Transact` retains its
96-selected-operation protection.

Focused proof must cover exact `30/42/30`, `44/98/44`, `117/147/117`, and
`174/175/174` construction; the Backup-only `152/90/152` accounting;
256-arm acceptance and 257-arm rejection; rejection above the 1 MiB protobuf
ceiling; and the unchanged ordinary non-Blueprint 96-operation protection.
Known local validation or encoding failure and a confirmed comparison failure
publish nothing. A retryable or deadline result whose commit status is unknown
remains unresolved until ADR 0021's durable evidence identifies the one atomic
result as wholly old or wholly new; marker absence alone never proves the prior
head won.

**Blueprint candidate terminal envelope.** [Blueprint terminal contract](features/blueprints.md#atomic-terminal-publication) preserves one atomic Task completion with all candidate promotion or
failure records, applied projection, materialization cleanup, Environment
fences, retention/idempotency records and final Script source fragment. It uses
the same 256-operation per-arm and exact physical 1 MiB request ceilings, through
a separate closed persistence method; it does not raise the ordinary 96 limit.

Read-only preparation composes the entire envelope before source release can
write. Source/root/closing-report comparisons whose revisions will change
reserve the maximum positive ModRevision encoding width. This budget projection
cannot execute. The final transaction re-reads exact current authority and
requires drained memberships and empty reverse prefixes. A lost compare writes
no terminal fragment and preserves the original closing report; changed sealed
authority remains fail-closed. If a commit succeeds but its response is lost,
the existing durable terminal receipt must produce exact read-only replay.

`encryption` is `age` or `none`. Because the config source includes secret
Entry values, any policy selecting config must use `encryption: age`.

The age key remains lazy. A first successful enabled `age` Blueprint creates
era 1 in that same final transaction, and exact replay reuses it. Disabled or
`none` candidates create no key. Disabling Backup or changing to `none` retains
existing current and historical key identities required by Recovery Points.

A successful source run commits one immutable recovery point only after the
typed dump/archive, transient staging, optional encryption, immutable upload,
remote verification, and local
record write all succeed:

The closed decoded formats are `environment-config-v1` as canonical USTAR,
`volume-tar-v1` as canonical USTAR, and `postgres-custom-v1` as the exact
PostgreSQL 16 custom dump with compression disabled. There is no outer
compression. `encryption: none` stores the exact decoded source; `age` stores
the exact unarmored binary age v1 encoding for one X25519 recipient.

```yaml
id: rp_01J...
source_id: spt_01J...
source_kind: attach
target_id: att_01J...
created_at: 2026-08-16T03:15:00Z
size_bytes: 44040192
encrypted: true
key_era: 2
status: verified
```

Environment id, source format, Connector id, object key, and digest are private.
`key_era` is present if and only if `encrypted` is true. `target_id` is the
stable original target, survives source removal from the active policy, and
must still resolve at restore time. Point reads use opaque fixed-revision
cursors and Recovery Point id descending. The point id and `created_at` are
allocated together before execution, with `created_at` equal to that allocation
timestamp; the point stays publicly nonexistent until verified commit. Only
verified points exist; failed
attempts remain Task history. Every point retains a separate Connector reverse
reference. Retention is the only ordinary point deletion path and runs per
source after its next successful backup; lowering `keep` does not prune during
policy replacement.

Restore takes a source id and recovery-point id, verifies that the selected
point belongs to that source, obtains the required age identity for its key
era, fully stages and verifies the bundle, then applies it to the original
surviving stable target. PostgreSQL and Volume restore stop consumers and
declare downtime; Volume uses same-filesystem `renameat2(RENAME_EXCHANGE)`.
Volume restore alone places its decoded tree outside the Agent task root, as a
hidden same-filesystem sibling under the target Volume parent. Config restore
uses an Environment lock, bounded typed validated Entry content from Agent to
Controller, and one read-hidden fence. It rolls canonical Entry primaries and
owner indexes forward in bounded batches, upserts present artifact ids, deletes
omitted Entries, and publishes the preallocated materialization generation
before removing read hiding. There is no Environment-wide active Entry
generation. Every source has explicit post-
restore verification. Restore never targets a replacement resource, restores a
mutable current-policy snapshot, or includes a backing environment.

Manual run has no Blueprint or request-time source subset. It captures every
configured source at one policy revision, processes stored order in one visible
fail-fast Task, and preserves earlier verified points. Capture consistency is
per source: config is one etcd revision, Volume stops mounting Services, and no
cross-source snapshot is claimed. Retry preserves that captured run, validates
its pinned dependencies, allocates fresh point ids for failed/unstarted
sources, and never reloads the current policy. PostgreSQL Attach, config, and
Volume are the current bounded runtime sources. Valkey data backup/restore is
required, but the namespace-safe per-consumer source versus
an explicit shared-instance RDB source remains unclosed; current runtime
rejects `strategy.not_implemented` until that contract and proof land. Live
data-directory archival is not authorized.

`x-gp-execution` is generated and may be carried alongside the Compose file
so the Agent can validate exactly what it is applying:

```yaml
x-gp-execution:
  task_id: task_01J...
  plan_id: plan_01J...
  plan_hash: sha256:...
  render_generation: 42
  operation: reconcile
  project_id: prj_01J...
  steps:
    - id: step_01J...
      kind: compose_up
      services: [api]
    - id: step_01J...
      kind: wait_healthy
      service: api
```

The Controller creates one typed `ExecutionPlan`. Protobuf carries that plan
over the live Agent channel and is authoritative for dispatch. Generated
`x-gp-execution` is the file-local serialization for validation, inspection,
replay, and debugging. Both carry `plan_id` and `plan_hash`; the Agent rejects
a bundle whose two projections do not match. Neither is an independently
editable task source. Arbitrary shell commands remain forbidden.

Extensions may appear at the document, service, network, volume, or other
Compose mapping scope where their schema defines that scope. They never
replace a standard Compose key when the standard key can express the same
thing.

Example:

```yaml
x-gp-requires:
  - target:
      kind: backing-attach
      name: api-db
    condition: ready
    phases: [deploy]

services:
  api:
    image: storefront-app:sha-9f3c1ad
    x-gp-resource:
      kind: service
      id: svc_01J...
    x-gp-depends_on:
      migrate:
        condition: service_completed_successfully
```

`x-gp-requires` is Controller-level. It can cross Compose project and file
boundaries. `x-gp-depends_on` is environment-local service ordering and may
compile to native Compose `depends_on`. A dependency cycle, missing target,
or unsupported condition is rejected before any task is dispatched.

The Agent may validate execution-scoped extensions such as resource ids,
task ids, generation numbers, and ownership. It does not resolve
`x-gp-requires`, provision attachments, choose releases, or reinterpret
facts. Those decisions arrive in the typed task procedure.

## State translation

The Controller translates every Blueprint into five state categories:

| Category | Examples | Persistence |
| --- | --- | --- |
| desired | lossless normalized services, networks, volumes, env entries, routes, attaches, and backup policy | immutable revision selected by the Environment desired head in etcd |
| durable records | ids, generated credentials, facts, release history, recovery points | etcd |
| render plan | Compose projects, network joins, env files, router files, task DAG | ephemeral |
| observed | containers, local image ids, health, networks, files, router state | Agent reports + journal |
| task state | pending, running, completed, failed, timed out, aborted | etcd/journal |

For each Service, the Controller combines the desired Service definition with
the immutable Release selected by `serving_release_id`. The final workload
Compose `image` is that Release's sealed local Docker image id. Requested image
name and tag remain provenance, not runtime authority. A failed candidate kept
serving by `leave_active` is therefore rendered even while
`current_successful_release_id` still names an older completed Release.

This is the steady-state rule. An active Deploy, Rollback, or Blueprint apply
uses only its exact sealed candidate or predecessor Release and projection
until terminal promotion; it never recomputes a plan from a moving current
pointer. Serving and successful authority advance independently from their
respective proved checkpoints.

A Service first introduced by a bundle starts with the Controller-owned
`runtime_intent` `running`. Reconciliation replaces only the desired Service
subrecord and preserves the current runtime intent for every existing stable
Service id; a Blueprint can never restart a stopped or destroyed Service.

For each attach, the Controller combines the desired attach reference with
the durable attach id, generated database/role/password, adapter facts, and
network ownership. Facts are computed sources; an operator mapping decides
whether a fact becomes a service environment value, an all-services env-file
entry, a service-specific env-file entry, or a generated file.

## Generated environment files

Every environment has one canonical all-services env file:

```text
secrets/.env.<environment-id>
```

The filename is id-based and does not change when the environment label is
renamed. The Controller attaches it explicitly through `env_file` to every
service that belongs to the environment. It contains the all-services set:
plain variables plus secret variables whose exposure is all services.

Service-specific values are placed in generated files such as:

```text
secrets/.env.<environment-id>.<service-name>
```

The Agent materializes these files at mode `0600`. Secret values never appear
in Compose YAML or `x-gp-*`; non-secret literals may use native Compose
`environment` when appropriate. The Controller computes file membership from
exposure and removes stale references atomically.

Compose's project `.env` interpolation file is a separate concern. Groundplane
does not rely on Compose's default `.env` behavior for container injection;
every injected file is named explicitly in the service's `env_file` list.

Precedence is deterministic: service `environment` overrides env-file values
with the same key; service-specific generated values override the inherited
all-services file; project and platform fallbacks are resolved before
materialization.

## Identity and rename rules

Every referenced entity has a stable id. The Environment label, Project slug,
Tenant slug, and Volume slug may be renamed without changing their ids. A
Volume slug edit also preserves its immutable Compose key and host leaf.

On rename, the Controller updates the authored document location, metadata,
API/Console labels, and any intentionally human-facing derived output. It
does not recreate the environment, move its volume directory, change its
Compose project identity, or change its generated env-file name.

Physical paths use ids:

```text
/var/lib/groundplane/vol/<tenant-id>/<project-id>/<environment-id>
```

Backup object keys and Docker resource names also use stable ids. Labels may
be included for display, but storage and ownership do not depend on labels.

The rename operation is addressed by stable id. It atomically changes the
label, document metadata, human-facing path, and any generated presentation
that intentionally uses the label, then renders a new generation. The old
document path is removed as part of the same Controller transaction. Replaying
the new path alone is a create/upsert at that address; it is not inferred to
be a rename without the explicit rename operation.

## Required validation

The Controller rejects a document before state mutation when any of these
conditions apply:

- envelope kind, schema, or placement is invalid;
- an invalid Compose field or unsupported Compose specification version is used;
- an unknown or incorrectly scoped `x-gp-*` extension is used;
- a service, network, volume, attach, fact, route, or secret reference is missing;
- a service or network reference is invalid, or a dependency cycle exists;
- a route targets a missing service or uses a hostname as a path, or vice versa;
- a public route has an invalid hostname/path or missing target service;
- an attach targets a non-running backing service, uses an incompatible adapter, or grants an unknown attach;
- a secret or fact would be exposed beyond its declared scope;
- a file path escapes the environment volume directory;
- a Volume key or `x-gp-slug` is invalid, a key changes, or a Service mount
  cannot resolve its key to one stable Volume id;
- the encoded lossless normalized projection exceeds exactly 2 MiB, including
  its durable schema and framing;
- a backup source, connector, encryption key, or restore identity is invalid;
- a final Environment Blueprint publication exceeds an arm or encoded-request
  limit;
- a requested release strategy is deferred (`rolling` in the MVP);
- a release-group map key is empty, a group contains fewer than two services,
  or its `on_failure` policy is outside the closed enum;
- Compose config validation fails after Groundplane expansion.

Validation is side-effect free. Provisioning, materialization, deployment,
and deletion happen only through tasks after validation succeeds.

## Lifecycle and reconciliation

The Controller follows this loop:

1. Parse the envelope and Compose body.
2. Validate native Compose and `x-gp-*` extensions.
3. Resolve authored names and immutable Volume keys to stable ids.
4. Load durable records: releases, facts, credentials, keys, and recovery points.
5. Derive the complete lossless normalized projection from one pinned prior
   Environment revision.
6. Build the render plan and cross-project dependency DAG.
7. Render Compose, env files, Corefile, Caddyfile, and typed Agent steps.
8. Validate every rendered artifact and the exact normalized projection ceiling.
9. Stage and seal the canonical audit stream and normalized projection as
   immutable `GDR1` chunks.
10. Publish the Environment desired head, revision-pinned Task, and replay
    authority in one compact transaction.
11. Dispatch tasks to the Agent in dependency order.
12. Compare Agent observations with the expected labels and render generation.
13. Record convergence, drift, failure, or rollback state.

Managed Docker resources carry stable labels in the `com.groundplane.*`
namespace, including managed/kind, tenant/project/environment ids, service id,
release id, slot, plan id, and render
generation. Unmanaged resources are never touched.

Task states and resource observations are distinct. A failed task does not
silently become desired state; it leaves a durable failure record and the
next retry reconciles against what was actually applied.

## Complete Controller-generated environment projection example

```yaml
kind: environment
schema: 1
metadata:
  tenant: acme
  project: storefront
  environment: production

x-gp-requires:
  - target:
      kind: backing-attach
      name: api-db
    condition: ready
    phases: [deploy]

services:
  api:
    # Requested-reference provenance: storefront-app:sha-9f3c1ad
    image: sha256:<sealed-serving-local-image-id>
    networks: [frontend, backend]
    env_file:
      - /var/lib/groundplane/secrets/.env.env_01J...
      - /var/lib/groundplane/secrets/.env.env_01J....api
    healthcheck:
      test: [CMD, curl, -f, http://localhost/up]
      interval: 10s
      timeout: 3s
      retries: 5
    x-gp-resource:
      kind: service
      id: svc_01J...
    x-gp-release:
      default_strategy: blue-green
      on_failure: switch_back
    x-gp-depends_on:
      migrate:
        condition: service_completed_successfully

networks:
  frontend:
    internal: true
  backend:
    internal: true

x-gp-attachments:
  api-db:
    backing_project: postgres
    backing_service: postgres
    service: api
    credential:
      mode: new

x-gp-entry:
  DATABASE_URL:
    kind: env
    source:
      fact:
        attach: api-db
        key: pg16_URL
    exposure: [api]
    secret: true
```

The example is not a hand-maintained Compose project. The Controller owns
the generated file, rewrites it after releases, attaches the id-based env
files, joins the external backing network, and sends the resulting procedure
to the Agent.

## Connector addressing input

Every `s3-compatible` Connector input carries a required Boolean `path_style`
alongside `endpoint`, `bucket`, optional `prefix`, and `region`. It also carries
exactly `access_key` and `secret_key` credentials; each credential contains
exactly one of `secret_ref` or direct `value`. These are pure desired-state
choices. No addressing behavior, credential source, region, or prefix is
derived during apply. See ADR 0045 for normalization and validation.
