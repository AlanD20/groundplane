# Groundplane Blueprint operator reference

A Blueprint is the authored desired configuration for one Groundplane
Environment. It combines a native Compose project with a small, closed set of
`x-gp-*` fields for decisions Compose cannot express. This reference owns the
accepted authoring grammar. Feature guides own behavior, lifecycle, and current
implementation status.

The current envelope schema is `1`:

```yaml
kind: environment
schema: 1
metadata:
  tenant: acme
  project: storefront
  environment: production
```

The metadata must match the Environment addressed by the API route. Human
labels locate that Environment; the Controller resolves and stores stable ids.
Do not put generated ids, Docker names, host paths, observations, Task state, or
render metadata in an authored Blueprint.

See [Blueprint authoring and application](features/blueprints.md) for the
operator journey, [Component boundaries](decisions/component-boundaries.md) for
the trust boundary, and [Desired-state publication](decisions/desired-state-publication.md)
for publication and recovery semantics.

## Read, validate, and apply

Use `environment blueprint show`, `environment blueprint validate`, and
`environment blueprint apply`, or the Environment Blueprint editor in the
Console.

1. Read the current Blueprint and its desired revision.
2. Edit locally. A draft has no effect until submitted.
3. Validate the same bundle that will be applied, using the exact quoted
   revision in `If-Match`.
4. Apply with the same revision. Apply publishes one reconcile Task.
5. Follow the Task to a terminal outcome. Acceptance is not runtime success.

Revision `0` means the Environment has no desired head. A stale `If-Match`
fails without changing desired state. Validate performs the same input parsing
and planning checks as Apply, but creates no desired revision, Task, or host
effect.

`show` returns canonical single-file YAML reconstructed from normalized desired
state. It preserves decisions, not the submitted file layout: comments, anchors,
aliases, source ordering outside Compose precedence, and multi-file boundaries
are not round-tripped.

Apply never pulls or builds workload images. Every required image must already
exist in the host Docker daemon. Existing Service runtime intent is separate
from desired configuration: applying a Blueprint must not restart a Service an
operator deliberately stopped or destroyed merely because its definition is
still present.

Automatic latest-wins supersession is an accepted design but is not connected
end to end. The selector exists; durable unit state, late planning, safe
handoff, and runtime integration remain incomplete. Do not depend on a newer
Apply automatically cancelling older work.

## Identity, omission, and removal

An Environment Blueprint declares operator-managed resources. Removing an Entry
key removes that Entry from the Environment on Apply. Directly created Entries
appear in canonical export too. Persistent Volumes are different: omitting one
is rejected until its protected Remove action completes. Other omission paths
are [not yet fully implemented](issues/runtime-qualification.md#incomplete-features);
Validate must describe the actual candidate effect, not imply a removal that
Apply will retain or reject.

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

The CLI accepts a bundle directory, ordered repeatable Compose sources, and
explicit interpolation variables. The API represents the same logical bundle
as multipart input. Wire encoding details belong to
[the API and CLI contract](api-cli.md); they do not change Blueprint meaning.

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

Each Compose source contains exactly one YAML document. Duplicate paths,
duplicate source entries, cycles, undeclared references, invalid interpolation
keys, and interpolation values containing NUL are rejected before publication.

## Native Compose with Groundplane policy

Use Compose for container topology: Services, networks, Volumes, images,
commands, environment, health checks, mounts, aliases, dependencies, resource
limits, logging, and restart policy. Groundplane keeps native Compose decisions
unless an explicit safety or product rule below rejects them.

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
    image: registry.example/storefront-api:2026-09-21
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

`x-gp-network-pool` is required and must be a canonical IPv4 CIDR. It reserves
address space for the Environment but does not itself create a Docker network.
Every managed native Compose network has one explicit subnet inside the pool;
network name, subnet, `internal` setting, and ownership are immutable. Subnets
must not overlap other reserved Zones. See
[Network and shared access](decisions/network-and-shared-access.md).

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
| `x-gp-adapter` | sole Service of a Backing Blueprint | built-in or custom Backing adapter |

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
    image: registry.example/storefront-api:2026-09-21
    x-gp-release:
      default_strategy: blue-green
      on_failure: switch_back
```

When authored, `default_strategy` is `blue-green` or `recreate`; `rolling`
remains deferred.
`on_failure` is `switch_back` or `leave_active` and defaults to
`switch_back`. The standard Compose `image` field is the requested image
reference. Selection, local image resolution, immutable Release records,
serving state, and retry checkpoints are Controller-owned; see
[Services and releases](features/services-and-releases.md) and
[Release packaging](decisions/release-packaging.md).

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
exactly once. `on_failure` uses the same values as Service releases. Groups are
never inferred from names or shared images.

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
  credential owner on the same Backing Service; and
- credential-owner reference chains are rejected.

An Attach makes facts available; it does not inject them. Use `x-gp-entry` to
select a fact and its destination.

For a Custom backing adapter with hooks, first complete a standalone Attach.
One Blueprint Apply cannot create a hook-based credential owner and consume its
newly produced facts in already-sealed Entries. Facts from an already-ready
Attach and existing credential reuse remain valid. Hook syntax and output are
owned by [Backing services](features/backing-services.md).

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

- `source.literal`, at most 256 KiB;
- `source.secret_ref`, which requires `secret: true`; or
- `source.fact` with `attach`, optional `grant`, and fact `key`.

For an `env` Entry, the map key is also the destination environment key and
`path`, `uid`, and `gid` are omitted. A `file` Entry uses a relative path inside
the Environment's managed volume directory and always supplies explicit numeric
`uid` and `gid`, including when either is zero. Ownership is never inferred
from an image.

`exposure` is a nonempty list of Service names, or the sole value `all`.
Groundplane never exposes an Entry merely because a Service has an Attach.
Script Entry grants are also limited to Entries already exposed to that
Script's associated Service.

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

`path` defaults to `/`. `exposure` is `public` or `internal`. A public Route is
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

The accepted capabilities and implementation-specific settings are documented
in [Components](features/components.md). Blueprint input never contains a
Component's generated Services, addresses, grants, artifacts, Tasks, or
observations. Secret material is referenced by stable `secret_id`; it is not
embedded in Component configuration.

### `x-gp-backup`

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
      ref: app-data
    - kind: config
```

Source kinds are `attach`, `volume`, and `config`; `config` refers to this
Environment's Entries and has no `ref`. `keep` is an integer from 1 through
`9007199254740991`. An enabled policy requires a Connector. `enabled: false`
alone is the unconfigured disabled form. Frequency, encryption, retention,
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

A Connector document uses the same envelope version but is not part of an
Environment Compose bundle and is not accepted by the Environment Blueprint
Apply command:

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
      secret_ref: sec_01J00000000000000000000000
    secret_key:
      secret_ref: sec_01J00000000000000000000001
```

`metadata.name`, the complete tenant/project/environment chain,
`connector.kind`, and explicit boolean `path_style` are required. Each
credential supplies exactly one of `secret_ref` or `value`. A direct value is
stored encrypted and is not returned in canonical authored output. Connector
scope, credential resolution, and backup use are documented in
[Secrets and Connectors](features/secrets-and-connectors.md).

## Validation and safety summary

The Controller is the only Blueprint interpreter. It rejects the entire input
before publication when, among other cases:

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
