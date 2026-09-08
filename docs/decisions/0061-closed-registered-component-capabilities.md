# ADR 0061: Closed registered Component Capabilities

Status: Accepted

Date: 2026-08-29

## Context

Groundplane's MVP ships Caddy, Cloudflare Tunnel, and CoreDNS integrations.
The existing `internal/components` registry gives those integrations broad
`internal/core` values and permits Controller, persistence, and Agent code to
switch on implementation names. That is not an extension boundary: it bakes
technology knowledge into Groundplane and allows a component package to reach
backend implementation types.

The MVP does not load user code or runtime plugins. It does need a boundary
that lets the shipped integrations deeply compose Groundplane behavior without
receiving privileged access. The same boundary must allow a future
source-registered HTTP router, such as Traefik or Nginx, to reuse the Route and
configuration-activation contracts without adding a second Controller path.

## Decision

### Closed source registry

The MVP registry is closed at build time. Its registered integrations are:

- Caddy, providing the `http-router` Component Capability;
- Cloudflare Tunnel, providing the `edge-tunnel` Component Capability;
- CoreDNS, providing the `dns-resolver` Component Capability.

There is no runtime registration, plugin upload, plugin subprocess, remote
planner, custom schema loading, or arbitrary component execution in the MVP.
Adding an implementation requires source integration, review, and rebuilding
Groundplane.

Controller and Agent are Groundplane runtime resources, never registered
plugin code or Component resources. Component catalog and projection reads do
not include Controller or Agent entries. Their existing Host and Agent
resources remain the only public presentations.

### Component Capabilities

Groundplane owns generic, typed capabilities. The term **Component
Capability** describes the subset a registered integration provides or
consumes; it is distinct from the MVP implementation ledger in
`docs/capabilities.md`.

The reusable product capabilities include Services, Routes, Volumes, Secrets,
Scripts, Backups, Networks, Entries, and Tasks. Component-specific capabilities
include HTTP routing, outbound tunnel lifecycle, DNS resolution, managed
configuration, host resolution, and host trust.

etcd, repositories, persistence keys, Docker, Compose helpers, the filesystem,
systemd, Controller and Agent packages, protobuf messages, encryption keys,
raw Task steps, and executors are never Component Capabilities.

A new implementation may satisfy an existing capability without changing
Groundplane core. A genuinely new capability is an explicit Groundplane
contract change with types, validation, authorization, persistence semantics,
API/CLI/Console parity when operator-facing, Agent procedure when required,
documentation, and acceptance evidence.

### Machine and in-process boundaries

REST/OpenAPI is the external machine integration boundary. The CLI is a human
operator client generated from that API; a Component never spawns it, parses
it, or treats CLI output as a protocol.

Statically linked integrations do not recursively call the Controller's HTTP
server. They compile against a pure public Component SDK and receive immutable,
capability-scoped planning inputs. The SDK is the in-process form of the same
Groundplane-owned capability contract, not access to backend services.

Two additional Go modules enforce this at compile time:

```text
component-sdk/          module github.com/AlanD20/groundplane-component-sdk
registered-components/ module github.com/AlanD20/groundplane-registered-components
root module             Groundplane core, API, Controller, Agent, and composition
```

`registered-components` imports only `component-sdk` and an explicit standard
library allowlist. It cannot legally import the root module's `internal/**`
packages. Only the root composition package imports concrete registrations.
The result remains one statically linked Controller and one statically linked
Agent, not microservices or runtime plugins.

### Typed catalog, grants, and planning

Each registration declares:

- an immutable implementation key and definition digest;
- its immutable managed OCI images, including the complete canonical
  two-platform index, child, config, and authenticated variant identity;
- the typed configuration variant it accepts;
- capabilities it provides;
- exact capabilities and operations it may consume;
- allowed product-owner scopes;
- bounded planner entry points;
- any generic managed-configuration contract it requires.

Configuration is a closed discriminated union generated into OpenAPI and
Blueprint boundary types. Component SDK, public, desired, durable, and Agent
models never use `any`, `interface{}`, `map[string]any`, raw JSON objects,
reflection, unsafe conversion, or implementation-recovery assertions.

Groundplane constructs one fixed-revision planning session containing only the
granted immutable snapshots. A planner returns typed immutable capability
intents and declarations. It cannot perform I/O, mutate state, allocate ids,
publish Tasks, select an executor, construct a transaction, or execute a
command.

Groundplane independently validates every returned intent against the sealed
catalog, grants, owner scope, authorization, revisions, limits, idempotency,
and cross-resource invariants. The owning capability modules then contribute
their existing persistence and Task-publication fragments to one atomic
commit. The plugin never runs inside the transaction.

Every managed image emitted by a planner must exactly match an image declared
by that implementation's compiled registration. Catalog action recipes may
reuse those declared images but cannot introduce another image authority; the
catalog digest binds each full declared image once and binds each recipe to its
declared repository. Missing, duplicate, extra-platform, malformed, or
unregistered images are rejected before Controller projection.

### Ownership, authorization, and secrets

Product ownership remains Tenant, Project, Environment, or Platform.
Component management ownership is a separate durable relationship recording
which Component created and may reconcile a resource. It never replaces the
resource's authoritative primary or product owner.

A Component may manage its own resources only through granted capabilities.
Mutation of an operator-owned resource requires authorization derived from the
exact operator request or Blueprint operation; it is not reusable plugin
authority. Groundplane assigns stable ids and owns all validation, persistence,
idempotency, lifecycle, and cleanup.

Secret and Entry capabilities expose metadata and opaque references only.
Registration or ownership never grants plaintext. Groundplane alone resolves
approved references transiently into an exact service binding or
materialization, and it clears those bytes at the existing secret boundary.

### Generic use cases and execution

The same capability use case backs the human REST action and Component-intent
application. There is one Route, Service, Volume, Secret, Script, Backup,
Network, Entry, and Task implementation, not a technology-specific path plus a
generic facade.

`/routes` remains the generic HTTP Route resource. MVP Routes remain owned by
their Environment and do not persist a router implementation id. Groundplane
resolves the Environment's single enabled `http-router` provider and pins the
Component id, definition digest, catalog digest, and input revision in each
immutable application Task and observation. Caddy translates the already
validated router input; it does not own HTTP handlers, repositories,
authorization, Task sequencing, or generic Route validation.

Agent execution uses generic, closed procedures. No procedure is named for an
implementation when another registered implementation of the same capability
would require the same effect. Managed configuration never accepts raw shell,
host paths, Task-supplied executable names, or arbitrary argv. The exact MVP
action is resolved from the Agent's statically compiled catalog. A Task carries
only `component_id`, `definition_digest`, `catalog_digest`, `action_id`,
`artifact_id`, `artifact_digest`, and generation. The Agent verifies those
values against its local catalog, the Component's ownership, the pinned image
digest, labels, mounts, artifact digest, and generation before it executes the
catalog recipe. The Task never carries an executable name, command arguments,
shell text, host path, or arbitrary URL. A new effect requires a reviewed
generic Agent capability before any Component can use it.

### Enforcement

`make architecture-check` must reject:

- a registered integration importing anything outside the Component SDK and
  approved standard library;
- a concrete registration imported outside the root composition package;
- a planner signature containing Groundplane aggregates, repositories,
  persistence records, public transport structs, protobuf, or executor types;
- untyped configuration or intent models;
- direct I/O, process execution, CLI execution, secret reveal, or arbitrary
  command declarations in registered integrations;
- implementation names in generic capability Controller, core, persistence,
  or Agent-procedure packages;
- a capability intent applied without independent Groundplane validation and
  atomic publication through its owning module.

## Clean replacement

The existing `internal/components` registry, whole-Environment renderer input,
free-form config maps, Caddy-specific Route persistence/plans, cross-component
config inspection, and implementation-specific Agent procedures are migration
sources. They are removed as callers move; no compatibility registry, implicit
Caddy fallback, dual config decoder, or old Agent payload remains before the
MVP is accepted.

This ADR amends ADR 0056's single-module and `internal/components/<kind>`
placement. It preserves ADR 0056's modular-monolith, explicit composition,
state ownership, concrete-first, and two-executor rules.

## Locked product behavior

An Environment owns exactly one logical `http-router` entry point. Caddy is its
sole MVP implementation. Routes remain valid desired state when no router is
enabled and report observed state `unserved`. Route create and edit return
`202 {route, task_id}`. Route removal returns `202 {task_id}`. With no enabled
router, the Controller Task records the desired-only result and performs no
Agent effect. Enabling a router applies every stored Route. Disabling the
router removes the entry point but preserves the Routes. Re-enabling the
router reconciles those same Routes.

Caddy retains the typed implementation setting `caddyfile_template`. The
setting is intentionally not portable to another HTTP-router implementation.
Both Environment ingress Components select explicit ordered `zone_ids` in
their own Environment. Caddy's first Zone retains its one pinned address;
secondary interfaces are dynamic. Route reachability requires a shared selected
Zone. Tunnel consumes granted generic Networks/read snapshots, joins its exact
independent selections, and gives its first non-internal Zone gateway priority.
It neither consumes HTTP-router/read nor automatically follows router placement.
Cloudflare Tunnel stores and resolves an authorized Secret reference,
starts or stops the remotely managed connector, and reports health. Its
lifecycle is independent of the HTTP-router provider. Groundplane does not
configure Tunnel DNS, public hostnames, ingress rules, origin targets, or
protocol.

Component-managed resources do not appear in ordinary resource collections or
management pages. The owning Component detail groups them by capability. A
topology view may show them with an explicit managed marker, and a caller with
a known stable id may read their detail. Direct mutation remains forbidden.
Tasks and Activity remain visible under their normal product scopes.

Blueprint Components are keyed by capability. `settings` contains portable
capability configuration. `implementation_config` is the strict typed variant
selected by `implementation`. Implementations never accept the old
implementation-keyed form or an untyped configuration object.

## Consequences

- Registered integrations can deeply compose Groundplane behavior without
  accessing its implementation.
- Generic capabilities remain reusable by future source-registered
  implementations.
- Existing product resource modules remain authoritative; this is not a
  repository-wide framework rewrite.
- Adding an implementation is intentionally more constrained than adding an
  ordinary internal package.
- C07, C12, and C13 require re-acceptance after the provider-neutral contract,
  generated surfaces, Agent procedure, and live host behavior are proven.
