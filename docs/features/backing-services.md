# Backing services and Attaches

## Purpose and scope

Backing services provide shared datastores and caches without creating a second
resource model. A Backing Service is the facade over one Platform-owned
hierarchy:

```text
backing Project -> Environment named main -> adapter-backed Service
```

The MVP creates PostgreSQL 16 and Valkey 9 Backing Services. An Attach joins one
consumer Service to one Backing Service network and, for a managed adapter,
owns or reuses one provisioned credential and its facts. The `manual` adapter
remains network-only where the authored backing-service contract permits it; it
does not gain managed facts, grants, credentials, or Backup support.

Backing creation does not accept an uploaded Blueprint, an existing Zone, an
operator-selected managed image, or runtime plugins. Existing-Zone selection
is deferred post-MVP; [ADR 0055](../decisions/0055-revision-bound-network-observation-and-backing-blueprint-scope.md)
records the constraints that a future decision must resolve without making
that path current behavior.

The exact public operations are owned by [the API and CLI
contract](../api-cli.md). Desired-state Attach syntax is owned by [the
Blueprint contract](../blueprint.md).

## Functional requirements

### Creation and lifecycle

Backing creation is one protected atomic operation. The operator supplies the
Project slug, name and optional description, one compiled managed adapter key,
the `main` Environment network pool, and one new Zone's name, subnet, and
`internal` decision. Valkey also requires an explicit immutable authentication mode.

The Controller publishes exactly one backing Project, one `main` Environment,
one dedicated backing-owned Zone, one adapter Service, one adapter-defined data
Volume, and one Agent Task. The Zone subnet must be canonical IPv4, inside the
new Environment pool, and globally unreserved. A conflict commits none of the
aggregate; protected replay returns the original ids and response.

Start, Stop, and Destroy change only the adapter Service's runtime intent under
the current MVP and API contract. Destroy removes runtime, not the Project,
Environment, Zone, Volume, Entries, Attach history, credentials, or data. There
is no Backing Service DELETE endpoint.

Permanent Backing deletion is outside the current MVP, by owner decision on
2026-09-12. Start recreates runtime using the retained configuration and data.
A future permanent Delete needs a separate Console action, CLI command and API
operation with impact preview, confirmation and explicit approval. The
[deferred safeguards](../decisions/0053-durable-hierarchy-and-backing-facade-deletion.md#8-deferred-backing-service-permanent-deletion)
do not authorize a route or a change to Destroy.

### Attach identity and credential reuse

An Attach connects exactly one consumer Service to exactly one Backing Service
network. Each consumer Service needs its own Attach record, even when several
Services intentionally share a credential. Create makes an explicit choice:

- `credential.mode: new` creates one credential-owning Attach and may include
  up to eight grants;
- `credential.mode: existing` names a ready credential owner in the same
  consumer Environment and Backing Service, rejects grants, and cannot point
  through another dependent.

The credential owner stores adapter identity, encrypted fact values, grants,
and any eligible database Backup-source identity. A dependent stores a direct
owner reference, resolves facts through that owner, and contributes only its
own Service/network edge. A dependent detach removes only that edge. An owner
cannot detach while a dependent references it; after dependents are gone, its
detach revokes grants and deprovisions through the adapter.

Every Backing Service owns a dedicated Zone and physical network. PostgreSQL
and Valkey do not share one backing network. A Service attached to both joins
both. Rendered network membership is a sorted, deduplicated union, but the
Attach records remain distinct durable facts.

Managed backing creation publishes a stable network alias: `gp-` plus the
lowercase backing Service id with `_` replaced by `-`. Owner and grant HOST/URL
facts use this alias, not the adapter-wide Service name. Joining two same-name
backing networks must not make endpoint selection ambiguous. Slug changes do
not change the endpoint.

### Facts, grants, and reveal

Attach facts are not reusable Secret resources and are never injected
automatically. A managed credential owner stores its fact schema separately
from an encrypted fact-value envelope. Its provisioned database and role
identity is the exact Service name plus `_` and the first six characters of
the Attach id's random tail. A credential-backed Attach rejects Service names
longer than 56 bytes rather than truncating the resulting 63-byte-bounded
adapter identity.

A grant exposes an additional prefixed fact set for another owner-selected
Attach. Omitting a grant in an Entry fact reference selects the credential
owner's own set; naming a grant selects that granted set. Credential reuse and
grants are independent relationships.

Attach list results expose stable backing ownership and fact keys with their
secret classification, never fact values. Fact values are returned only by the
explicit fact-reveal operation. The Controller derives Environment scope from
the durable Attach and requires ready facts; the caller cannot override scope.
Non-secret database and role facts may be used for labels. Password and
credential-bearing URL facts remain masked until explicit reveal. Plaintext
facts must not enter Attach primaries, Tasks, Activity, list responses, or
desired-state projections.

### Adapter behavior and Valkey authentication

The accepted managed create keys are `postgres:16` and `valkey:9`. Adapter
defaults are compiled product behavior and the operator cannot override their
managed image, mount, bootstrap, health, or procedure decisions through the
Backing Service create request.

Valkey authentication is required, explicitly selected, immutable instance policy:

- `username_password` gives each credential owner a named
  ACL identity and password;
- `password` gives each owner an independent password on the shared `default`
  user;
- `none` explicitly enables access without AUTH and produces HOST, PORT, and a
  credential-free URL, but no ROLE, PASSWORD, or consumer credential.

No mode is selected by default. The Console starts unselected; API and CLI
creation reject omission. Missing, empty, null or unknown values cannot publish
resources or Tasks. Every Attach inherits the selected mode. No Attach can weaken it. All modes
share one instance keyspace and Pub/Sub channels; an ACL identity is not tenant
or data isolation. The Console must explain the reachability risk before a
no-auth instance is created.

ACL state is persistent data. Management must use authenticated, discrete
commands, carry the final secret token through stdin rather than argv or the
environment, save ACL changes to the owned data Volume, and prove success
before publishing ready facts. Existing ACL state is retained on restart; it
must not be regenerated in a way that erases Attach users. Consumer identities
must not receive administrative commands. The exact accepted modes and fact
forms remain in [ADR 0068](../decisions/0068-valkey-authentication-modes.md).

## Non-functional requirements

- Aggregate creation, Attach creation, detach, retry, and runtime destruction
  preserve stable ids, immutable plans, operation fences, and protected replay.
- Credential and fact plaintext stays in encrypted subordinate storage or a
  bounded execution/reveal path. It is never logged or stored in public Task
  evidence.
- Adapter execution fails closed when its selected procedure, image authority,
  encrypted identity, or persistent ACL state cannot be validated. It must not
  substitute a mutable tag, a same-major runtime, or an advisory shell string.
- The rejected release-registry design formerly recorded as ADR 0054 provides
  no registry, wire, helper, image, platform, or acceptance authority. In
  particular, its ARM64-only scope conflicts with the product's AMD64 and ARM64
  requirement.
- Valkey data recovery remains a production requirement. Shared-instance RDB
  capture is not a safe per-Attach artifact, and `valkey-cli --pipe` does not
  restore an RDB image. Until an exact source, consistency, artifact, and
  restore-publication contract is accepted and implemented, Valkey Backup and
  Restore fail before Task publication with `strategy.not_implemented`; live
  data-directory archival is not authorized.

## Technical design

[ADR 0037](../decisions/0037-backing-service-network-selection.md) owns atomic
Backing Service creation and runtime lifecycle. [ADR 0031](../decisions/0031-durable-attach-facts-and-grants.md)
owns durable Attach credential ownership, reverse references, task behavior,
facts, grants, and Backup-source identity. [ADR 0068](../decisions/0068-valkey-authentication-modes.md)
owns the accepted Valkey authentication modes. ADR 0053 owns the accepted
hierarchy-deletion engine and retains the separate deferred Backing extension;
neither changes the current runtime-only Destroy action.

Facts and encrypted values have separate durable records. A dependent fact
read follows `credential_attach_id` to the direct owner while preserving the
consumer Attach as the public reference. Reverse references serialize owner
detach against dependent creation. Creation publishes the primary, indexes,
immutable render input, Task, operation locks, queue entry, and idempotency
evidence atomically.

Standalone Attach and Detach capture native runtime, current Entry bindings and
running intent at one fixed revision. Their immutable input retains that capture;
the Environment mutation epoch fences publication. Network changes target the
selected active physical workloads, keep historical Release labels and exclude
Components, stable proxies, inactive slots and dependencies. Retained Component
ownership never authorizes Component startup, recreation or lifecycle steps in
an Attach or Detach plan. A stopped or configured-only
consumer validates Compose without starting a container. The complete Attach
union replaces the old managed network overlay; ordinary-name containers are
not substitutes for native workloads. Captured runtime may contain one serving
slot at the current desired generation because native Deploy does not advance
desired state. This does not permit incomplete fresh desired slot topology.

The rejected ADR 0054 attempted to define a compiled release ledger, runtime
registry mutation rules, an ARM64-only image chain, new wire messages, and one
static Valkey helper in a single proposal. It was rejected because that scope
conflicted with the accepted C10 implementation and dual-platform requirement,
introduced unavailable release inputs, and coupled independent release,
runtime, wire, and Backup decisions. Git history retains that proposal; none of
its detailed clauses is a routed current contract.

## Acceptance

Feature acceptance requires cross-surface proof of atomic creation, exact
replay, new-Zone ownership, lifecycle intent, Attach new/existing credential
rules, owner/dependent detach races, grant resolution, masked list output,
explicit fact reveal, and restart-stable facts. PostgreSQL and each Valkey
authentication mode need create, attach, reveal, stop/start, detach, retry, and
failure-path proof against the same durable identities.
Destroy must preserve durable configuration and data so Start can recreate
runtime. No permanent Backing deletion or impact-preview surface is exposed.

Production Gate B additionally requires source-specific Backup, verified
original-target Restore, and retention proof for every persistent source.
PostgreSQL Attach recovery is routed through [Backups](backups.md). Valkey does
not pass Gate B until its unresolved safe source and restore contract is closed
and proved.

## Current status

The [capability index](../capabilities.md) records qualification for PostgreSQL
and Valkey creation plus the core Attach lifecycle, with focused isolated proof
for Valkey authentication modes. Required explicit selection has HTTP, CLI,
domain and local Console/browser proof; omission no longer chooses a mode.
Live Groundplane qualification of the Valkey
authentication extension remains. Valkey Backup and Restore are not
implemented or qualified, so this feature is not production-accepted.
