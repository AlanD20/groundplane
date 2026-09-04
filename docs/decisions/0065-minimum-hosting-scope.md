# ADR 0065: Minimum hosting scope and trust boundaries

- Status: Accepted for the documentation contract; runtime alignment pending
- Date: 2026-09-04
- Capability: MVP hosting floor

## Context

The product documents overstate the MVP in several places: tenant-network
security, registry-backed workload deployment, whole-host disaster recovery,
simultaneous release-group switching, and singleton-only release semantics do
not describe the bounded first-hosting milestone. The accepted floor must remain
generic and must not claim implementation or acceptance evidence that is not
present.

## Decision

The MVP remains generic for many Tenants, Projects, and Environments. The first
hosting acceptance selects one trusted operator organization and one
Tenant/Project/Environment; this is an acceptance boundary, not a product limit.
Existing resource and credential-owning Attach scoping is retained. Shared
backing bridges may permit peer reachability; the MVP makes no tenant-network
security guarantee. Strict peer-firewall isolation is post-MVP.

Workload deployment resolves and pins an exact image identity already present in
the host Docker daemon before mutation. It does not pull from or push to a
registry and does not build as a deploy side effect. Pinned Groundplane-managed
Agent, etcd, Component, and backing images remain release or installation
assets. Registry integration and registry hosting are post-MVP.

Groundplane owns generic Route resources. A registered Component may submit an
authorized Route intent through the SDK. Caddy is the current `http-router`
provider: it PROVIDES `http-router` and CONSUMES existing GP Routes to render,
provision, and reconcile service traffic; it need not create Routes. A Route is
valid without a provider and is then `unserved`. Future nginx, Traefik, or Envoy
implementations are source registrations, not runtime plugins.

MVP backup and restore covers PostgreSQL Attach, config, and Volume sources,
each restored to its original surviving existing Environment target. There is no
cross-source atomic snapshot. Valkey data backup/restore is required by the
hosting floor, but the namespace-safe per-consumer source versus an explicit
shared-instance RDB source, its safe format, and proof are unclosed; the current
runtime rejects `strategy.not_implemented` until that bounded work lands. No
whole-instance RDB is claimed as a per-consumer format, and live data-directory
archival is not authorized. Full automated empty-host
DR/export/import is post-MVP. A future DR flow must include metadata, the
controller key, startup config, images and data backups, and recovery bootstrap
before reconcile.

Runner networking uses a dedicated `runner.network_pool`, disjoint from both
`environment_pool` and `system_pool`, provisioned at `/24`, `/25`, or `/26` and
subdivided into per-Runner `/29`s. ADR 0057 remains the Controller executor
authority and its broader Runner lifecycle contract is retained; this does not
claim Runner implementation. Under the explicit assumption that an existing
external build process supplies host-local workload images, GP-managed Runner
provisioning is not a Gate A or Gate B dependency. This is a first-hosting
sequencing assumption, not an owner approval of a changed Runner scope.

An `x-gp-release-groups` map is the sole release-group grammar. The singular
extension is rejected, with no alias or compatibility parser. A group is one
operator action over 2 through 32 logical Services, executed serially rather
than as a simultaneous or atomic distributed switch. For group deploy, a
request tag overrides the persisted group tag; when both are empty the deploy
is rejected, with no member-current fallback. The selected deploy tag resolves
independently against each member image and pins its exact immutable host-local
Docker image identity before workload mutation; registry or manifest digest
metadata is optional and never substitutes for that local identity. Each member
retains its own release history. Group rollback never uses the persisted
default. An optional rollback request tag independently selects each member's
newest eligible historical Release that completed successfully and reached
serving with that exact tag and a tag different from its current serving
Release; omission selects the newest such eligible different-tag Release for
each member. Any member without an eligible source rejects the whole group
before publication. Group `on_failure` overrides member defaults for aggregate
compensation. Existing
Script hooks remain supported and ordered by ADR 0040; a migration binds once to
the designated logical member, while each selected hook executes once per
logical Service Release, never per replica. No group-level hook resource exists.

The minimum hosting floor has two gates. Gate A is disposable first-hosting
acceptance on an
already provisioned supported Linux/Docker host with a Groundplane release,
preloaded workload images, explicit pools, one selected Tenant/Project/Environment and
Blueprint, backing PostgreSQL/Valkey facts, explicit uid/gid-aware resources,
identity dependencies and generated CA/leaf material before identity start,
exact HTTP/WebSocket/internal-callback and deny-default policy, Caddy plus
Cloudflare Tunnel public path, and one ordered group action with one migration.
Native `deploy.replicas` is persisted and reconciled at an operator-authored
N>=1: Gate A proves more than one WebSocket replica with Valkey fan-out and
stable logical routing; API, scheduler, and queue remain singleton defaults.
Replicated Services use recreate and preserve their count through deploy,
rollback, restart, and reapply. Blue-green N>1 is rejected in the MVP; exact
replica rendering/health mechanisms remain implementation proof work. Gate A
also proves health, logs, Tasks, rollback, restart, exact reapply, TLS first
provision, and Caddy policy rendering.

Gate B is Gate A plus production MVP operations acceptance with backup, proven
restore, and retention for all actual persistent sources under existing
per-source safety and original-target rules. Restore tests may use a disposable
Environment and need no new scratch-target API. Passing Gate A is not production
cutover authorization and
does not permit abandoning current backups; Gate A is not all-MVP acceptance.
The current no-host-port
rule remains locked. A proposed bounded floor seam for a topology-specific
break-glass need—explicit loopback-only native Compose mappings on operator-owned
recreate Services—requires owner resolution before implementation and is not
current behavior.

## Consequences

These are clean pre-release contract corrections: no aliases, dual schemas,
fallback old behavior, runtime claim, or acceptance claim is added. Existing
implementation and status evidence remain unchanged until separately aligned and
proven. The exclusions apply to both gates: optional replica scaling beyond the
Gate A WebSocket proof, extra router providers, runtime plugins, registry
integration, peer-firewall isolation, automated empty-host DR, advanced
monitoring, and unrelated refactors do not block either gate. Full existing
CI, parity, security, generated-artifact, and exercised destructive-path checks
remain mandatory.

## Rejected alternatives

- A strict tenant security guarantee on shared backing bridges.
- Registry pull/build side effects during deployment.
- An etcd snapshot plus key being advertised as a complete host restore.
- A simultaneous or atomic distributed release-group switch.
- A new group-level hook resource or runtime-loaded router plugin.
