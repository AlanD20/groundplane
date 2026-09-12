# Managed Components and network routing

## Purpose and scope

Provide the host DNS resolver and each Environment's HTTP router and outbound
tunnel through normal Groundplane resources. The MVP has three compiled
implementations: CoreDNS (`dns-resolver`), Caddy (`http-router`) and Cloudflare
Tunnel (`edge-tunnel`). It has no runtime plugins or operator-uploaded planners.

[mvp.md](../mvp.md) owns the product model. The shared SDK, grants, immutable
catalog and execution boundary are defined in
[the Component decision](../decisions/0061-closed-registered-component-capabilities.md).
Read that contract when changing an integration, not for an ordinary config edit.

## Functional requirements

- An Environment has at most one enabled logical HTTP router. Routes belong to
  the Environment, not its router implementation. Routes remain valid without
  a router and report `unserved`; enabling applies stored Routes, while disabling
  preserves them. Desired-only Route Tasks have no Agent effect.
- Caddy and Tunnel each have an explicit ordered Zone list. Caddy retains its
  reserved address on the first Zone; secondary memberships are dynamic. Tunnel
  chooses the first non-internal Zone for its gateway and does not follow Caddy's
  membership automatically. Route targets must share reachable Zone membership.
- Tunnel config authorizes an opaque Secret reference and connector lifecycle.
  It does not configure provider DNS, public hostnames, ingress or origin policy.
- CoreDNS is a Platform Component. It serves exact internal Route names using
  router private addresses, structured upstreams and per-domain forwarders.
  Tailnet detection sets a default that the operator may override. Network Zones
  and DNS zones are different concepts; network identity never becomes a DNS name.
- CoreDNS requires one full Corefile template containing exactly one
  `{groundplane}` marker. The renderer owns its managed directives. Invalid
  configuration must retain the serving configuration. The Agent owns resolver
  file materialization and restores the predecessor when DNS is removed.
- Component config is a closed typed union. Disabled or unconfigured Components
  project `config: null`; config reads return a non-null `managed_files` array.
  Preview uses durable desired inputs without writes and is not live-file proof.
- Component-owned resources retain their product owner but are excluded from
  ordinary collections. The Component detail groups them by capability; direct
  known-ID reads remain possible, but direct mutation is forbidden. Their Tasks
  remain in the normal owning workspace journal.

The exact requests and desired-state variants belong to [api-cli.md](../api-cli.md)
and [blueprint.md](../blueprint.md). Complete Caddyfile behavior and the router's
optional network alias are in [Router templates](router-template.md).

## Non-functional requirements

Registered code compiles only against the public SDK and approved standard
library. It gets immutable scoped inputs and returns typed intents, never
repositories, Docker, filesystem access, secret plaintext or arbitrary execution.
Groundplane validates authorization, revisions, ownership, images and limits
before atomic publication. The Agent executes only compiled generic recipes.
Controller and Agent runtime resources are not Components.

## Technical design

The owning capability modules validate the planner's intents and contribute to
one publication. [architecture.md](../architecture.md#the-component-capability-seam)
owns the shared planning and execution seam. Implementations live in
[registered-components](../../registered-components/); public types live in
[component-sdk](../../component-sdk/).

### Task-owned Component candidates

One Agent Task owns one immutable Component candidate set for an Environment.
The active Component records remain unchanged while that Task is pending or
running. An Environment-scoped active-intent index rejects another candidate
based on the same active state; the MVP serializes reconciliation rather than
merging concurrent graphs.

Publication captures the exact current record and revision, replacement and
address transitions. A new primary router address is reserved atomically with
the candidate and Task; existing addresses remain reserved during execution.
Successful terminal publication promotes all candidates and releases only
superseded addresses. Failure, Abort or timeout retains active Components and
releases only new candidate addresses. Terminal replay checks retained intent
without repeating Component or address mutations. Adding terminal status and
time does not mutate the candidate payload. This private intent is not another
public Component model.

## Acceptance

Prove fixed-revision preview without writes; typed config and 1:1 surface parity;
grant, catalog and ownership rejection before effects; deterministic Route
rendering; retained addresses and config after failure; and terminal replay
without duplicate publication. On authorized QA, prove DNS, routing and tunnel
behavior without modifying provider or host policy outside the approved scope.
Implementation-named core paths or arbitrary execution fail the architecture gate.

## Current status

The closed boundary and Environment Components have implementation evidence.
CoreDNS reference-host qualification remains open. Caddy template and reload
qualification is tracked in [Router templates](router-template.md); the
[capability index](../capabilities.md) records product-wide limits. Dated checks
do not establish current host health or permission to bypass the QA integrity pause.
