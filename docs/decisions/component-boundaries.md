# Desired-state and Component boundaries

Status: Accepted for the MVP. These decisions define architecture; they do not
by themselves prove implementation or qualification.

Groundplane is a typed modular monolith with a closed source registry. Desired
state enters through reproducible bounded inputs, registered Component code
plans only through capability-scoped values, and the Controller remains the
sole authority for validation, persistence, Tasks, and execution selection.

Product authority: [Blueprints](../features/blueprints.md),
[Components and routing](../features/components.md), and
[architecture](../architecture.md).

## Closed Blueprint inputs

An Environment Blueprint is a logical closed bundle, not one YAML byte slice or
an implicit directory. It contains a root Groundplane document, an ordered set
of Compose sources, the complete normalized relative file namespace, and an
explicit non-secret interpolation map.

The Controller parses the envelope, resolves stable hierarchy ids, and then
loads Compose through `compose-go` using only the submitted namespace and
Controller-derived identity. It must not consult process environment, implicit
`.env`, host paths, symlinks, network resources, or remote fallbacks. Native
`include`, file-based `extends`, `env_file`, `label_file`, configs, secrets, and
other permitted Compose references remain available when their content is in
the bundle and satisfies the content policy.

The human API uses one manifest plus one binary part per declared file. Part
identity, file digest, source order, interpolation values, path normalization,
and the accepted file/byte/parser limits are verified before desired-state
publication. CLI and Console expose the same ordered sources and explicit
variables. Bundle validation is side-effect free.

Read-only bind sources may refer only to bundled non-secret content materialized
below the stable Environment directory. Writable binds, arbitrary host paths,
sockets, devices, and traversal are rejected. Secret content cannot use ordinary
companion files or interpolation. A Blueprint secret Entry literal is key-only
and starts with an empty protected value; the separate Entry action sets or
rotates its value. Canonical export never reveals it. Secret references remain
the reusable alternative. See the [Entry grammar](../blueprint.md#x-gp-entry).

Normalized desired records are the sole reconciliation authority. The immutable
submitted bundle is retained only for audit and deterministic reparse
verification. Canonical Compose and materializations are regenerated and are
never competing persisted desired state. The Agent receives canonical execution
inputs and never reparses the authored bundle.

Current source: [Blueprint parser](../../internal/controller/blueprintparser),
[apply orchestration](../../internal/controller/blueprint), and
[Console bundle builder](../../console/src/lib/blueprint-bundle.ts). Exact
bundle-generation persistence mechanics remain subject to the unresolved
general-storage choices in [storage and idempotency](storage-and-idempotency.md#storage-model-and-open-decisions).

## Modular monolith

Groundplane remains one repository and one product backend, with Controller,
Agent, and CLI binaries. It does not introduce microservices, runtime plugins,
a generic orchestration framework, service discovery, an event bus, or a
dependency-injection container.

There is one durable Task journal with two explicit executors:

- the Agent executor owns workload and platform data-plane effects, bounded
  helpers, scripts, backups, adapters, and observations; and
- the serial native Controller executor owns Agent lifecycle, isolated Runner
  lifecycle, persistence-only finalizers, and the closed procedures that must
  remain available without the Agent.

Executor authority is immutable Task state, not inferred from kind or queue.
Handlers and schedulers publish intent and fences before side effects. Neither
executor may claim the other's Tasks, and the native executor is not a generic
host-command escape hatch.

Desired state, operational intent, Task state, reconstructable execution
identity, transient secret material, observed evidence, external runtime state,
and browser state each have one owner. Docker labels, task output, Console
state, and historical events never become alternate desired-state stores.

Backend code is organized by capability within strict layers. `internal/app`
composes and manages process lifecycle; core owns pure domain decisions;
Controller modules own use cases; infrastructure modules own technology and
aggregate persistence; Agent modules own execution. Wire packages carry wire
types, not product policy. The Console mirrors capability ownership and uses
the generated API contract rather than a parallel generic repository layer.

Concrete types are the default. Interfaces are consumer-owned and narrow,
used for multiple real behaviors, side-effect seams, standard-library
contracts, or deliberate process ports. Persistence keys, raw revisions,
transport responses, and implementation-recovery assertions do not leak
through capability interfaces. External values cross explicit parsers into
validated internal types; maps, reflection, JSON round trips, `unsafe`, and
double assertions are not mapping mechanisms.

Architecture checks enforce import direction, composition-only application
wiring, generated-artifact ownership, Component isolation, unsafe-conversion
rules, and non-growing file-size ratchets. Those checks must encourage deeper
modules rather than pass-through file splitting. Refactoring replaces old
implementations directly; there are no forwarding shims or dual paths.

Current source: [composition](../../internal/app),
[Controller capabilities](../../internal/controller),
[Agent capabilities](../../internal/agent),
[etcd capability adapters](../../internal/infra/etcd), and
[architecture check](../../cmd/architecture-check).

## Registered Components

The MVP registry is closed at build time. Caddy provides `http-router`,
Cloudflare Tunnel provides `edge-tunnel`, and CoreDNS provides `dns-resolver`.
Controller and Agent are runtime resources, not Component registrations. There
is no runtime registration, plugin upload, plugin subprocess, remote planner,
custom schema loading, or arbitrary execution.

Registered integrations compile in a separate module against the pure public
Component SDK. They cannot import the root module's internal packages. Only the
composition root imports concrete registrations, so the result is still one
statically linked Controller and Agent rather than microservices.

A Component Capability is a Groundplane-owned typed contract. It may describe
generic Services, Routes, Volumes, Secrets, Scripts, Backups, Networks,
Entries, Tasks, routing, tunnels, DNS, managed configuration, host resolution,
or trust. Repositories, etcd keys, Docker, filesystem, systemd, protobuf,
encryption keys, raw Task steps, and executors are never capabilities.

Registrations declare immutable implementation and catalog identity, managed
images, typed configuration, provided and consumed capabilities, permitted
owners, bounded planners, and managed-configuration needs. Groundplane supplies
only immutable granted snapshots. A planner performs no I/O, id allocation,
state mutation, Task publication, transaction construction, executor selection,
or command execution.

Planner results are typed intents. Groundplane independently validates catalog
identity, grants, ownership, authorization, revisions, limits, image authority,
idempotency, and cross-resource invariants before the owning capability modules
publish one transaction. Secret and Entry inputs expose metadata and opaque
references only; registration never grants plaintext.

Agent execution uses closed generic procedures resolved from its compiled
catalog. Tasks carry immutable catalog, action, artifact, and generation
identity, never arbitrary executable names, argv, shell, host paths, or URLs.
A new effect requires a reviewed generic Agent capability.

Routes remain generic Environment resources. The enabled `http-router`
provider is selected by capability; a Route without a provider remains valid
and unserved. Caddy is the sole MVP implementation. Tunnel lifecycle and
network selection remain independent of router placement. Component-managed
resources retain normal product ownership but are managed only through their
owning Component surface.

Blueprint Components are keyed by capability, with portable settings and a
strict implementation-specific configuration variant. The former broad
registry, untyped maps, implementation switches in generic modules,
technology-specific persistence, and arbitrary Agent procedures are migration
sources to remove, not compatibility surfaces.

Current source: [Component SDK](../../component-sdk),
[registered catalog](../../registered-components/catalog),
[registered implementations](../../registered-components), and
[Controller Component capability](../../internal/controller/component).

The architecture is accepted; capability acceptance still depends on generated
surfaces and live behavior recorded in the feature status. Tests for these
boundaries must be behavioral or architectural, not static source-text checks.
