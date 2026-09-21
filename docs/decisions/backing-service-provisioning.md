# Backing Service provisioning

- Decision status: Accepted for current creation, Attach and runtime lifecycle
- Deferred: existing-Zone selection and permanent Backing deletion

## The facade reuses the ordinary hierarchy

A Backing Service is a facade over one Platform-owned Project, its `main`
Environment, one backing-owned Zone and one adapter-backed Service. Built-in
adapters also own one managed data Volume; Custom does not invent a Volume.
The stable Backing Service id is the backing Project id; there is no duplicate
facade primary.

Creation is one atomic aggregate publication followed by one Agent Task. The
operator selects only the closed public choices, including adapter and explicit
network pool/subnet decisions. For built-in adapters, the image, Service
definition, health check, Volume, bootstrap Entries, fact schema, Attach
procedures and Backup strategy come from the compiled versioned adapter catalog
and are frozen at publication; the operator cannot override them.

Custom is the accepted, bounded exception: the operator selects its image and
may declare typed Attach, Detach, before-stop and after-start hooks. Custom does
not infer a data Volume, credentials, ports, health check or Backup strategy,
and its image and hooks do not create a plugin or arbitrary host-execution
surface. Exact hook configuration and result rules belong to
[Backing services and Attaches](../features/backing-services.md#unified-provisioning-and-optional-custom-hooks).
There is no uploaded Backing Blueprint or sequence of public child mutations.

The generated endpoint alias derives from the immutable Service id, not its
name. Tenant and Platform ownership remain distinct: a Platform-owned render
does not fabricate an empty Tenant identity.

Start, Stop and Destroy delegate to the ordinary immutable Service lifecycle.
Destroy means runtime absent only. It preserves Project, Environment, Zone,
Volume, Entries, credentials, Attach history and data. Start recreates the
runtime from frozen desired input. Permanent deletion is unavailable as
described in [resource deletion](resource-deletion.md#permanent-backing-deletion-remains-deferred).

## Attaches separate network membership from credential ownership

Each Attach connects exactly one consuming Service to one Backing Service.
Network membership belongs to that consumer edge. Credential facts, grants,
provisioning identity and database Backup-source identity belong to one
credential-owning Attach.

A dependent Attach may reuse an owner's credential only within the same
consumer Environment and Backing Service. It stores the owner identity and
resolves facts through that owner; it provisions no duplicate credential,
stores no duplicate encrypted facts and is not a second database Backup source.
Reference chains are forbidden. Grant selection remains distinct from
credential reuse.

For built-in adapters, creating an owner performs typed compiled provisioning
and grants before network reconciliation. For Custom, an Attach or Detach hook
uses the same sealed input, declared output and result-validation boundary, but
executes only inside the selected backing container. Without a provisioning
hook, Custom Attach is network-only; Custom has neither database grants nor
managed Backup support.

Creating a dependent changes only the selected consumer's network membership.
Detaching a dependent removes only that edge. Detaching an owner is blocked
while dependent credential references remain; once clear, it revokes any
built-in grants, removes the edge and runs the matching typed deprovisioning
procedure when one exists. Retry preserves the exact credential owner, facts,
adapter identity and network plan. Backing facts are Attach-owned encrypted
data, not reusable Secret resources.

## Network selection is intentionally narrow

Current Backing creation always receives an explicit globally non-overlapping
Environment pool and creates one new dedicated backing-owned Zone. It cannot
select an existing Zone, infer a pool, or accept a competing external-network
grammar.

Existing-Zone reuse is deferred. A future design must keep the Controller as
admission authority, bind an authenticated Agent observation to one fixed
durable revision, use actual Moby facts rather than expected labels, and compare
that authority again in final publication. Missing, stale, collided, drifted or
wrong-owner evidence must fail closed without falling back to an older good
observation. The public surface must expose stable product identities rather
than Docker ids, private labels, revisions or observation tokens.

The deferred design still needs explicit owner/dependency rules, freshness and
size limits, reconnect behavior, snapshot retention, Blueprint scope and
end-to-end acceptance. Earlier candidate field names, wire numbers, record
paths and count limits are not accepted requirements and create no
implementation authority.

## Consequences

- The current create flow is deterministic and bounded: built-in adapter and
  infrastructure details are compiled, while Custom variation is limited to
  its selected image and declared typed hooks.
- Credential reuse avoids duplicate databases and backups while preserving one
  explicit Attach per consuming Service.
- Custom remains suitable for network-only or operator-defined provisioning,
  but it is not a source of managed Backup.
- Dedicated Zones keep current publication independent of stale runtime
  observation.
- Accepted design does not imply that every path is implemented or qualified;
  current capability status remains separate.

## Source navigation

Facade creation and projection live in
[`internal/controller/backingservices`](../../internal/controller/backingservices/creation.go),
with atomic publication in
[`internal/infra/etcd/backing_service_creation.go`](../../internal/infra/etcd/backing_service_creation.go).
Attach ownership and lifecycle are in
[`internal/controller/attachments`](../../internal/controller/attachments/repository.go),
and built-in adapter steps are defined in
[`internal/common/executionplan`](../../internal/common/executionplan/attach_mutation.go).
Custom creation is separated in
[`internal/adapters/custom_creation.go`](../../internal/adapters/custom_creation.go);
its hook contract and typed plan are owned by
[`internal/common/backinghook`](../../internal/common/backinghook/contract.go) and
[`internal/common/executionplan`](../../internal/common/executionplan/backing_hook.go).
