# Blueprint reference

A Blueprint is the authored desired configuration for one Groundplane
Environment. It combines a native Compose project with a small, closed set of
`x-gp-*` fields for decisions Compose cannot express. This reference owns the
accepted authoring grammar. Feature guides own behavior, lifecycle, and current
implementation status.

- [Start with one Service](#start-with-one-service)
- [Envelope and references](#envelope-and-references)
- [Identity, omission, and removal](#identity-omission-and-removal)
- [Closed bundle input](#closed-bundle-input)
- [Native Compose with Groundplane policy](#native-compose-with-groundplane-policy)
- [Groundplane extension overview](#groundplane-extension-overview)
- [Groundplane field reference](#groundplane-field-reference)
- [Connector documents](#connector-documents)
- [Validation and safety summary](#validation-and-safety-summary)

For runnable CLI commands, export, Apply recovery and Task handling, use
[Blueprint authoring and application](features/blueprints.md). This page explains
what to put in the document. Sections marked **not available** retain accepted
syntax without presenting it as usable today.

## Start with one Service

Create the Tenant, Project and Environment first. Choose an available Environment
pool inside the host's configured allocation pool and preload the workload image.
Save this complete example as `blueprint/blueprint.yaml`:

```yaml
kind: environment
schema: 1
metadata:
  tenant: acme
  project: storefront
  environment: production

x-gp-network-pool: 10.42.0.0/24

services:
  api:
    image: registry.example/storefront-api:2026-09-29
    expose: ["8080"]
    networks: [application]
    x-gp-release:
      default_strategy: recreate

networks:
  application:
    internal: true
    ipam:
      config:
        - subnet: 10.42.0.0/25
```

Replace the labels, CIDRs and image with your own. `expose` describes an internal
container port; it does not make the application reachable from outside GP.
Add a [Route](#x-gp-routes) and an enabled [HTTP router](#x-gp-components) when
you need HTTP access. This example has no health check; running does not mean
application health has been verified.

## Envelope and references

The root document requires the envelope above and `x-gp-network-pool`.
`kind` is `environment` and `schema` is the integer `1`. Metadata contains the
current Tenant slug, Project slug and Environment name, matching the Environment
addressed by the API route. Creating parents is a separate action; metadata
does not create or rename them.

Only the root file contains the envelope and root `x-gp-*` fields. Additional
Compose sources can contain native definitions and Service/Volume extensions.
Unknown Groundplane fields, misspelled fields and fields in the wrong scope fail
validation. Generated ids that identify new resources, Docker names, derived
host paths, observations and Task state do not belong in authored input.

References to existing resources are operator decisions. Use the form required
by each field; a name and an id are not interchangeable everywhere:

| Reference | Authored value |
| --- | --- |
| Entry exposure, Script `service`, Route `target`, release-group members | Compose Service names in this Environment |
| Explicit Script `volume` | Immutable top-level Compose Volume key |
| Explicit Script `entries` | `x-gp-entry` keys |
| Attach `backing_project` / `backing_service` | Backing Project slug / Service name |
| Attach `credential.attach` | Direct owner key in this document's attachment map |
| Entry `source.fact.attach` | Attach name; optional `grant` selects a declared grant |
| Entry `source.secret_ref` | Reusable Secret key or allowed stable Secret id |
| Component `settings.zone_ids` / `secret_id` | Existing stable Zone / Secret ids |
| Backup `connector`, Attach `ref`, Volume `ref` | Same-Environment Connector name, Attach name, Volume slug |
| Connector credential `secret_ref` | Reusable env-var Secret key, resolved Project before Platform |

Quoting strings such as image tags, numeric-looking env values and schedule
expressions avoids unintended YAML types. Supply numbers and booleans as their
actual types where required. Interpolation is for native Compose input, not for
storing secret values or replacing the Groundplane envelope.

Apply never pulls or builds workload images. Every required image must already
exist in the host Docker daemon. Existing Service runtime intent is separate
from desired configuration: applying a Blueprint must not restart a Service an
operator deliberately stopped or destroyed merely because its definition is
still present. [Component boundaries](decisions/component-boundaries.md) and
[desired-state publication](decisions/desired-state-publication.md) explain the
trust and execution design.

## Identity, omission, and removal

An Environment Blueprint declares operator-managed resources. Removing an Entry
key removes that Entry from the Environment on Apply. Directly created Entries
appear in canonical export too. Persistent Volumes are different: omitting one
is rejected until its protected Remove action completes. Other omission paths
are [not yet fully implemented](issues/runtime-qualification.md#incomplete-features);
Validate must describe the actual candidate effect, not imply a removal that
Apply will retain or reject.

Omitted native Compose `configs` and `secrets` definitions are not copied from
the prior Blueprint. Companion file bytes are carried forward only when the
candidate Compose project still references them.

Stable ids are Controller-owned. Authored map keys provide reconciliation
identity where a field says so, while slugs are renamable labels. In particular:

- a Volume's Compose map key is its immutable authored key;
- an `x-gp-entry` map key is its Blueprint reconciliation key, independent of
  its destination environment key or file path;
- an `x-gp-scripts` map key is immutable authored identity, while its `slug` is
  renamable; and
- changing an immutable key is not a rename and is rejected when it would imply
  destructive replacement.

For Entries, source and exposure may change without changing identity. Kind,
destination, file ownership, and secret-storage class are not in-place identity
edits. A direct env Entry initially uses its destination key for Blueprint
identity; a direct file Entry uses `file:` followed by its relative path.
Generated Component configuration is not an operator-authored Entry.

## Closed bundle input

An Environment import is one closed `BlueprintBundle` containing:

- one normalized relative root path carrying the Groundplane envelope;
- an ordered list of Compose source paths beginning with that root;
- every Compose source and companion file in a declared, normalized
  path-to-bytes namespace; and
- an explicit interpolation map containing only non-secret values.

The CLI reads every regular file under `--bundle-dir`, including nested and
hidden files. Use a dedicated directory containing only intended Blueprint
inputs. There is no ignore-file mechanism; symlinks and special files fail.
`--root` is relative to that directory. Repeat `--compose-file` for additional
layers, in precedence order; the root is already the first layer.
Repeat `--var KEY=VALUE` for explicit non-secret interpolation values.
See the [multi-file example](features/blueprints.md#multi-file-bundles).

The API represents the same bundle as multipart input. Its
[upload encoding](api-cli.md#upload-encoding) does not change YAML meaning.

Compose `include`, `extends`, `env_file`, `label_file`, config files, and
read-only bind sources may refer only to declared bundle members. Paths are
relative, slash-separated, normalized, and traversal-free. The Controller does
not consult the process environment, an implicit `.env`, the host filesystem,
symlinks, remote URLs, a network fallback, or undeclared files.

The decoded input limits are:

| Limit | Maximum |
| --- | ---: |
| files | 64 |
| one file | 256 KiB |
| all file bytes | 768 KiB |
| one UTF-8 path | 240 bytes |
| aggregate YAML nodes | 100,000 |
| YAML alias references | 1,000 |
| alias/include/extends resolution depth | 16 |
| resolved Compose resources | 512 |

Paths in the bundle manifest are normalized, for example `config/app.env`,
not `/config/app.env`, `./config/app.env` or `../app.env`. File references use
Compose's base directory: layered sources are based on the root's directory;
an included project uses its own directory unless `project_directory` says
otherwise. All references must remain inside the declared bundle.

Each Compose source contains exactly one YAML document. Duplicate paths,
duplicate source entries, cycles, undeclared references, invalid interpolation
keys, and interpolation values containing NUL are rejected before publication.
Interpolation keys match `^[A-Za-z_][A-Za-z0-9_]*$`; duplicate CLI `--var` keys
are invalid. Submitted values must be NUL-free UTF-8.

## Native Compose with Groundplane policy

Use Compose for container topology: Services, networks, Volumes, images,
commands, environment, health checks, mounts, aliases, dependencies, resource
limits, logging, and restart policy. The accepted grammar keeps native Compose
decisions subject to Groundplane policy. Parsing support and current Apply
support differ where noted below.

This example is intentionally ordinary Compose plus a few Groundplane
decisions. It declares one locally available workload, an internal Zone, a
managed Volume, one scoped Entry, and a release hook:

```yaml
kind: environment
schema: 1
metadata:
  tenant: acme
  project: storefront
  environment: production

x-gp-network-pool: 10.42.0.0/24

services:
  api:
    image: registry.example/storefront-api:2026-09-29
    command: ["/app/server"]
    environment:
      APP_ENV: production
    expose: ["8080"]
    healthcheck:
      test: [CMD, /app/healthcheck]
      interval: 10s
      timeout: 3s
      retries: 5
    networks: [application]
    volumes:
      - app-data:/srv/app/data
    deploy:
      replicas: 1
      resources:
        limits:
          memory: 512M
    x-gp-release:
      default_strategy: recreate
      on_failure: switch_back

networks:
  application:
    internal: true
    ipam:
      config:
        - subnet: 10.42.0.0/25

volumes:
  app-data:
    x-gp-slug: storefront-data

x-gp-entry:
  LOG_LEVEL:
    kind: env
    source:
      literal: info
    exposure: [api]
    secret: false

x-gp-scripts:
  migrate-schema:
    slug: migrate-schema
    service: api
    when: pre-deploy
    order: 10
    script: |
      /app/migrate --non-interactive
```

Groundplane adds these restrictions to native Compose:

| Compose input | Groundplane consequence |
| --- | --- |
| `build` | Rejected. Images are preloaded; Apply never builds or pulls. |
| nonempty service `ports` | Rejected for tenant workloads. Use `expose` plus a Groundplane Route and router Component. |
| host `devices` | Rejected. |
| bind mounts | Accepted only read-only and only from declared bundle content. |
| top-level local bind Volumes | Accepted only read-only from declared bundle content. Network filesystems are rejected. |
| `configs.file` and other companion files | The file must be in the closed bundle. |
| authored Compose secret sources | `file`, `content`, and `environment` sources are rejected. Use `x-gp-entry` or a Secret reference. |
| `include`, `extends`, and multiple sources | Accepted within the closed bundle and resolved before execution. |
| YAML anchors and aliases | Parsing conveniences only; they cannot bypass validation and are absent from canonical export. |
| interpolation | Uses only the submitted non-secret map. There is no ambient environment or `.env`. |
| `profiles` | Preserved; Groundplane does not silently activate a profile. |
| rolling release | Declared but deferred in the MVP. `x-gp-release.default_strategy: rolling` is rejected. |
| runtime intent | Controller-owned operational state; it is not authored here. |

**Current Apply limits:** native `configs` and Compose `secrets`, including
external definitions, are rejected after parsing. Use Entries for configuration.
External networks and network extensions are unavailable. Volumes must be
GP-managed: `external` Volumes and any `driver_opts` are rejected, including
read-only local bind Volume definitions that satisfy the accepted grammar.
Service-level read-only binds from bundle files remain a separate supported
input. These limits do not remove the accepted syntax above.

`x-gp-network-pool` is required and must be a canonical IPv4 CIDR. It reserves
address space for the Environment but does not itself create a Docker network.
Every managed native Compose network has one explicit subnet inside the pool;
network name, subnet, `internal` setting, and ownership are immutable. Subnets
must not overlap other reserved Zones. See
[Network and shared access](decisions/network-and-shared-access.md).

Current Zones are IPv4 bridge networks. Use one `ipam.config` item with only
`subnet`; do not supply gateway, address range, auxiliary addresses, driver
options, attachable mode or IPv6. If Compose would create an implicit `default`
network, declare that network with its explicit subnet too.

## Groundplane extension overview

Extension scope is strict. Unknown fields, known fields in the wrong scope, and
Controller-generated fields in authored input are rejected.

| Field | Authored scope | Decision it adds |
| --- | --- | --- |
| `x-gp-network-pool` | Environment root | IPv4 allocation boundary |
| `x-gp-requires` | Environment root | Controller resource prerequisites |
| `x-gp-attachments` | Environment root | consumer-to-Backing-Service bindings |
| `x-gp-entry` | Environment root | scoped environment variables and files |
| `x-gp-routes` | Environment root | provider-neutral HTTP routes |
| `x-gp-scripts` | Environment root | manual and Release-hook Scripts |
| `x-gp-components` | Environment root | registered Component Capability choices |
| `x-gp-backup` | Environment root | backup policy and source selection |
| `x-gp-release-groups` | Environment root only | explicit coordinated Service releases |
| `x-gp-release` | Service | default release strategy and failure policy |
| `x-gp-depends_on` | Service | Environment-local lifecycle prerequisites |
| `x-gp-slug` | top-level Volume | renamable public Volume label |
| `x-gp-adapter` | sole Service of a Backing Blueprint | built-in or custom adapter; upload not available |

`x-gp-resource`, `x-gp-execution`, and `x-gp-managed` are generated execution
metadata and are forbidden in authored input. The reserved `x-gp-network`
extension is not yet resolved by the runtime renderer; do not author it for an
operable Environment. Ordinary Zone decisions use native Compose networks.

## Groundplane field reference

### `x-gp-release`

`x-gp-release` carries Service defaults, not current Release state:

```yaml
services:
  api:
    image: registry.example/storefront-api:2026-09-29
    x-gp-release:
      default_strategy: blue-green
      on_failure: switch_back
```

When authored, `default_strategy` is `blue-green` or `recreate`; `rolling`
remains deferred.
When the extension is present, `default_strategy` is required. Blue-green supports one
replica; use recreate for more than one. `deploy.replicas` defaults to one.
`on_failure` is `switch_back` or `leave_active` and defaults to
`switch_back`. The standard Compose `image` field is the requested image
reference. Selection, local image resolution, immutable Release records,
serving state, and retry checkpoints are Controller-owned; see
[Services and releases](features/services-and-releases.md) and
[Release packaging](decisions/release-packaging.md).

Current Blueprint Apply prepares native workload candidates with `recreate`.
Setting `blue-green` here supplies a default for explicit Service Deploy;
it does not make Apply perform a blue-green rollout. Use the explicit Deploy
workflow when that strategy is required.

### `x-gp-release-groups`

A root-only map makes coordination explicit:

```yaml
x-gp-release-groups:
  realtime:
    services: [api, worker, scheduler]
    order: [api, worker, scheduler]
    tag: sha-9f3c1ad
    on_failure: switch_back
```

The map key is the group name. A group contains 2 through 32 distinct enabled
Services. When `order` is omitted, `services` order is used; an explicitly
empty or null `order` is invalid. A supplied order must contain every member
exactly once. `tag` is optional. `on_failure` uses the same values and default
as Service releases. Declaring a group does not run its Deploy action; groups
are never inferred from names or shared images.

### `x-gp-depends_on` and `x-gp-requires`

Use `x-gp-depends_on` for a Service in the same Environment:

```yaml
services:
  api:
    x-gp-depends_on:
      migrate:
        condition: service_completed_successfully
        phases: [deploy]
```

`condition` is `service_started`, `service_healthy`, or
`service_completed_successfully`. Omit `phases` for native Compose startup
ordering only. Otherwise use a nonempty, duplicate-free subset of `start`,
`deploy`, `rollback`, and `always` for Controller lifecycle ordering.
Dependencies may not be declared twice through native `depends_on` and
`x-gp-depends_on`, and dependency graphs must be acyclic.

Use root `x-gp-requires` for Controller resources, including a resource outside
the Compose project:

```yaml
x-gp-requires:
  - target:
      kind: backing-attach
      name: api-db
    condition: ready
    phases: [deploy]
```

In schema 1, `target.kind` is `backing-attach`; `condition` is `exists`,
`ready`, or `completed_successfully`; and `phases` is a required nonempty
subset of `start`, `deploy`, `rollback`, and `always`. Requirements are Task
prerequisites, not cross-project Compose dependencies. The target must already
be resolvable at the fixed pre-stage revision; do not require an Attach created
by the same Apply.

### `x-gp-attachments`

Each map key names one consumer binding:

```yaml
x-gp-attachments:
  api-db:
    backing_project: data
    backing_service: postgres
    service: api
    credential:
      mode: new
    grants: []

  worker-db:
    backing_project: data
    backing_service: postgres
    service: worker
    credential:
      mode: existing
      attach: api-db
```

`service` names exactly one consumer Service. Credential mode is `new` or
`existing`:

- `new` rejects `credential.attach` and may list at most eight direct grants;
- `existing` requires `credential.attach`, rejects grants, and names a direct
  credential owner declared in the same `x-gp-attachments` map, on the same
  Backing Service; and
- credential-owner reference chains are rejected.

An Attach makes facts available; it does not inject them. Use `x-gp-entry` to
select a fact and its destination. Declare every consumer Service used in these
snippets. `backing_project` and `backing_service` locate existing shared
infrastructure; an attachment declaration does not create the Backing Service.

For the current implementation of a Custom backing adapter with hooks, first
complete a standalone Attach before referencing its new facts or credential
owner. The accepted same-Apply behavior is not available yet. Facts from an
already-ready Attach and existing credential reuse remain valid. Hook behavior
and the implementation limit are owned by
[Backing services](features/backing-services.md).

Valkey authentication is an immutable Backing-instance choice made explicitly
when that Backing Service is created: `username_password`, `password`, or
`none`, with no default. A consumer Blueprint inherits that choice and cannot
downgrade it. `credential.mode` still selects ownership/reuse even when the
Backing Service uses no authentication.

### `x-gp-entry`

A new or edited Entry has one destination and exactly one source kind. Canonical
export has the secret-literal preservation exception described below:

```yaml
x-gp-entry:
  DATABASE_URL:
    kind: env
    source:
      fact:
        attach: api-db
        key: pg16_URL
    exposure: [api]
    secret: true

  TLS_CA:
    kind: file
    path: trust/ca.pem
    uid: 10001
    gid: 10001
    source:
      secret_ref: sec_01J00000000000000000000000
    exposure: [api]
    secret: true
```

`kind` is `env` or `file`. Sources are:

- `source.literal`, a string of at most 256 KiB;
- `source.secret_ref`, a reusable Secret key or allowed stable id, requiring
  `secret: true`; or
- `source.fact` with `attach`, optional `grant`, and fact `key`.

`secret` defaults to false. A confidential fact must have a secret destination;
the fact key is the exact adapter-published name, including its prefix. Optional
`grant` selects another Attach that this credential owner is granted access to.
Do not put credentials in native Compose `environment`, `env_file`, companion
files or interpolation values to avoid the protected Entry/Secret path.

For an `env` Entry, the map key is also the destination environment key and
`path`, `uid`, and `gid` are omitted. A `file` Entry uses a relative path inside
the Environment's managed volume directory and always supplies explicit numeric
`uid` and `gid`, including when either is zero. Ownership is never inferred
from an image.

`exposure` is a nonempty list of Service names, or the sole value `all`.
Groundplane never exposes an Entry merely because a Service has an Attach.
Script Entry grants are also limited to Entries already exposed to that
Script's associated Service.

A secret literal can be declared explicitly:

```yaml
x-gp-entry:
  API_TOKEN:
    kind: env
    source:
      literal: ""
    exposure: [api]
    secret: true
```

A secret literal in a Blueprint has an empty `source.literal`; nonempty secret
plaintext is rejected. A new key-only secret Entry creates an empty protected
value generation. Canonical export shows its key and source kind, never the
selected value. Reapplying the same key-only declaration preserves an existing
generation, including one later set through the Entry action. Prefer
`secret_ref` when the same credential should be reusable as a Secret resource.

See [Storage and Entries](features/storage-and-entries.md) for lifecycle and
materialization behavior.

### `x-gp-scripts`

The map key is immutable Script reconciliation identity. Each Script supplies
a unique renamable `slug`, target `service`, hook `when`, and body:

```yaml
x-gp-scripts:
  migrate-schema:
    slug: migrate-schema
    service: api
    when: pre-deploy
    order: 10
    script: |
      /app/migrate --non-interactive
```

A Script created through the API also appears in canonical Blueprint export.
Its key is `direct-` followed by the lowercase suffix of its stable Script id;
renaming its slug does not change that key. Omitting a Script key removes the
Script from the next active Script set.

Declare at most 64 Scripts. Their targets must be enabled Services with a
positive effective replica count.
Keys and slugs are 1 through 63 lowercase ASCII bytes matching
`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`. `when` is `manual`,
`pre-deploy`, `post-deploy`, `pre-rollback`, `post-rollback`, or
`on-failure`. The body is nonblank, NUL-free UTF-8 up to 65,536 bytes. `order`
is `0..65535`, defaults to zero, and sorts before raw slug bytes. A manual
Script does not become a hook merely because its order matches one.

Omitted `execution`, or `execution: {mode: inherited}`, uses the sealed target
Service/Release context. Inherited mode accepts no other execution fields.
Explicit mode is a complete replacement:

```yaml
execution:
  mode: explicit
  image: registry.example/setup@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  user: "10001:10001"
  volumes:
    - volume: app-data
      target: /data
      read_only: false
  entries: [DATABASE_URL]
```

Place `execution` beside `slug`, `service`, `when`, `order` and `script` in one
Script definition; this snippet is not a separate root section. Replace the
illustrative digest with the exact digest of an available image.

Explicit execution requires a repository reference pinned by a lowercase
SHA-256 digest and an explicit numeric `uid:gid`. The image must already be
local. It has no network, Docker socket, host path, inherited environment, or
inherited Service runtime. Omitted `volumes` and `entries` grant nothing.

There are at most 32 Volume grants and 64 Entry grants. `volume` is the
immutable Compose Volume key; each target is a non-root absolute container path
and every `read_only` decision is explicit. Targets cannot overlap each other,
Entry file targets, `/groundplane-script-body`, Docker-managed host files, or
reserved system trees such as `/proc`, `/sys`, `/dev`, `/run`, `/bin`, `/sbin`,
`/usr`, `/lib`, and `/lib64`.

Groundplane supplies execution isolation, not application semantics. The Script
author owns idempotency, staging, validation, and atomic publication of output.
See [Setup and migration Scripts](features/setup-scripts.md).

### `x-gp-routes`

Routes keep host and path separate and always name a target Service and port:

```yaml
x-gp-routes:
  - hostname: app.example.com
    path: /api/*
    target: api
    target_port: 8080
    exposure: public
```

`target` and `target_port` are required; the port is an integer from 1 through
65535. `path` defaults to `/`; it is absolute, contains no query or fragment,
and permits only a terminal `*`. `exposure` is required and is `public` or
`internal`. A public Route requires a lowercase ASCII DNS hostname; internal
Routes may omit it. Duplicate host/path matches are invalid. A public Route is
only served when an enabled HTTP-router Component can reach a Zone shared with
the target. Routes do not publish host ports. See
[Components](features/components.md) and the
[router template contract](features/router-template.md).

### `x-gp-components`

The map key is a Component Capability. `implementation` selects a registered,
build-time implementation; `settings` contains portable decisions and
`implementation_config` contains that implementation's strict typed variant:

```yaml
x-gp-components:
  http-router:
    implementation: caddy
    enabled: true
    settings:
      zone_ids: [net_01J00000000000000000000000]
      alias: storefront-router
    implementation_config:
      caddyfile_template: |
        {gp.routes}
```

Current Environment choices are:

| Map key | `implementation` | Settings while enabled |
| --- | --- | --- |
| `http-router` | `caddy` | Nonempty ordered `zone_ids`; optional `alias`; optional `implementation_config.caddyfile_template` |
| `edge-tunnel` | `cloudflare-tunnel` | Nonempty ordered `zone_ids` and `secret_id`; no alias or Caddy template |

`enabled` is a boolean and defaults to false. Zone references are existing stable
ids, not Compose network names; use the Zone list to obtain them. The first Zone
has the primary-address role. Tunnel needs a reachable non-internal Zone for
egress. `dns-resolver` / `coredns` is Platform-owned and cannot be configured in
an Environment Blueprint.

The Caddy template above is a minimal routes-only illustration. Follow
[router templates](features/router-template.md) to author the complete serving
policy. Omitting a previously configured optional Environment Component disables
it; unchanged configuration can retain its runtime. See
[Components](features/components.md) for lifecycle and implementation settings.
Blueprint input never contains a
Component's generated Services, addresses, grants, artifacts, Tasks, or
observations. Secret material is referenced by stable `secret_id`; it is not
embedded in Component configuration.

### `x-gp-backup`

**Backup/Restore is not operationally complete and remains deferred.** This
section defines policy input, not a qualified data-recovery procedure.

Backup policy is Environment desired state, not Compose topology:

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
      ref: storefront-data
    - kind: config
```

Source kinds are `attach`, `volume`, and `config`; `config` refers to this
Environment's Entries and has no `ref`. Attach `ref` is a credential-owning
PostgreSQL Attach name; Volume `ref` is its public slug (`x-gp-slug`), not its
immutable Compose key. Valkey and Custom Attach backup sources are unsupported.

`frequency` is a whole-second UTC daily or weekly expression, for example
`"*-*-* 03:15:00"` or `"Sun *-*-* 03:15:00"`; it is not a cron expression.
`encryption` is `age` or `none`; Config sources require `age`. Select 1 through
12 distinct sources in explicit order for a configured policy.
`keep` is an integer from 1 through
`9007199254740991`. An enabled policy requires a Connector. `enabled: false`
alone is the unconfigured disabled form. Omitting `x-gp-backup` also disables
and unconfigures the policy; it does not retain the previous settings or sources.
Frequency, encryption, retention,
restore, and recovery-point behavior belong to [Backups](features/backups.md).

### `x-gp-slug`

`x-gp-slug` appears only on a top-level native Compose Volume:

```yaml
volumes:
  app-data:
    x-gp-slug: storefront-data
```

The Compose map key is immutable authored identity. The optional slug is a
renamable label of 1 through 63 lowercase ASCII letters, digits, and hyphens,
starting and ending with a letter or digit. When omitted for a new Volume, the
map key must satisfy that slug grammar and becomes the initial slug. Changing
the slug retains the stable Volume id, storage path, and mounts.

### `x-gp-adapter` in a Backing Blueprint

Backing Blueprint upload is not available yet. The example below describes the
accepted Service field, not a file the current Environment Blueprint parser can
apply. A Backing Service Blueprint does not use tenant or project metadata; its
own envelope is not implemented yet. Do not add `x-gp-adapter` to a tenant
Environment Blueprint to work around that limit.
Create shared infrastructure through the [Backing Service action](features/backing-services.md)
instead.

`x-gp-adapter` is valid only on the sole Service of a Backing Blueprint. Native
Compose still owns its networks, health check, resources, Volumes, and restart
policy:

```yaml
services:
  postgres:
    networks: [postgres-net]
    x-gp-adapter:
      key: postgres:16
      prefix: pg16
```

The registered built-in adapter owns provisioning operations, fact templates,
and backup/restore procedures. A `custom` adapter uses the operator's native
image and is network-only unless hooks are explicitly configured:

```yaml
services:
  shared:
    image: registry.example/shared-service:1.0
    networks: [shared-net]
    x-gp-adapter:
      key: custom
      hooks:
        attach:
          command: [/opt/provision-consumer]
          timeout_seconds: 60
        detach:
          command: [/opt/remove-consumer]
          timeout_seconds: 60
        before_stop:
          command: [/opt/check-stop]
          timeout_seconds: 30
        after_start:
          command: [/opt/check-start]
          timeout_seconds: 30
        inputs:
          - key: PORT
            value: "8080"
          - key: ADMIN_TOKEN
            secret_ref: sec_01J00000000000000000000000
          - key: PASSWORD
            generate: password
        facts:
          - key: USERNAME
            secret: false
          - key: PASSWORD
            secret: true
```

Each hook command is a nonempty literal argument array, not an implicit shell
command, and its timeout is 1 through 900 seconds. Each input selects exactly
one of `value`, `secret_ref`, or `generate: password`. `HOST` is reserved.
Generated values belong to an Attach and are unavailable to lifecycle hooks.
Declared fact keys are the only accepted hook outputs; fact declarations
require an attach hook. The full input/output protocol and lifecycle limits are
in [Backing services](features/backing-services.md).

## Connector documents

**Connector document upload is not available.** The model below uses the same
envelope version, but is not part of an Environment Compose bundle or accepted
by Blueprint Apply. Create a Connector with the Console, `connector add`, or
the Connector API. This retained document shape describes the accepted intent:

```yaml
kind: connector
schema: 1
metadata:
  tenant: acme
  project: storefront
  environment: production
  name: r2-backups
connector:
  kind: s3-compatible
  endpoint: https://example.r2.cloudflarestorage.com
  bucket: groundplane-backups
  prefix: storefront/production
  region: auto
  path_style: false
  credentials:
    access_key:
      secret_ref: BACKUP_ACCESS_KEY
    secret_key:
      secret_ref: BACKUP_SECRET_KEY
```

`metadata.name`, the complete tenant/project/environment chain,
`connector.kind`, and explicit boolean `path_style` are required. Each
credential supplies exactly one of `secret_ref` or `value`. A direct value is
stored encrypted and is not returned in canonical authored output. Connector
scope, credential resolution, and backup use are documented in
[Secrets and Connectors](features/secrets-and-connectors.md).

Connector credential `secret_ref` is an env-var Secret **key**, resolved in the
owning Project before Platform fallback. This differs from Entry references,
which may select a stable Secret id. Credentials contain exactly `access_key`
and `secret_key`; neither is an ambient host credential.

## Validation and safety summary

The Controller is the only Blueprint interpreter. It checks the envelope,
bundle and grammar, then resolves and prepares the candidate operation.
Apply rejects input before publication when, among other cases:

- the envelope does not address the routed Environment;
- a bundle path, reference, interpolation value, YAML document, or complexity
  bound is invalid;
- Compose syntax is invalid or a forbidden host/runtime capability is used;
- a generated, unknown, or incorrectly scoped `x-gp-*` field appears;
- an image, Service, Zone, Volume, Attach, Entry, Route, Script, Component,
  Connector, or backup source reference cannot be resolved;
- a dependency cycle or invalid release-group order exists;
- an Entry would exceed its declared exposure or a file path escapes its
  managed location;
- an explicit Script context has an unpinned image, nonnumeric user, undeclared
  grant, overlapping target, or forbidden runtime access; or
- the requested strategy or extension is declared but not implemented.

Validation failure creates no desired revision, Task, marker, materialization,
or runtime effect. After publication, partial failure remains visible through
the Task and retry uses captured immutable input rather than rereading a newer
Blueprint.

**A successful Validate is a preview, not a reservation or proof of Apply
success.** Both paths share the parser, but current Validate does not execute
every Apply preparation check, including local image resolution and executable
resource preparation. Apply can still reject that candidate. Concurrent changes
can also invalidate its revision or dependencies. See the
[operator guide](features/blueprints.md#read-edit-validate-and-apply).

## Implementation references

The reference intentionally does not duplicate parser algorithms or state
translation. Current implementation entry points are:

- [closed bundle model and limits](../internal/core/bundle.go)
- [authored envelope and extension types](../internal/core/envelope.go)
- [Environment parser](../internal/controller/blueprintparser/parser.go)
- [source and extension safety checks](../internal/controller/blueprintparser/service_source_validation.go)
- [closed file-reference checks](../internal/controller/blueprintparser/bundle_references.go)
- [Script execution type](../internal/core/script_execution_spec.go)
- [canonical authoring projection](../internal/controller/blueprintparser/authoring.go)
- [Apply orchestration](../internal/controller/blueprint/apply.go)

Those paths explain how the accepted contract is implemented; they do not add
operator-visible fields beyond this reference and the authoritative feature
contracts.
