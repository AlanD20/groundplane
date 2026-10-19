# Managed Components and routing

GP exposes CoreDNS for host DNS, Caddy for an Environment's HTTP routing,
and Cloudflare Tunnel for its outbound edge tunnel. An Environment can instead
select Envoy for HTTP, TCP and UDP forwarding.
There is no runtime plugin loader or operator-uploaded planner. Controller and
Agent are host processes, not Components.

Removing a Zone disconnects its existing containers and removes its bridge.
It does not redeploy Services, apply pending Service edits or delete their data.
Other Zone memberships remain connected; losing the last membership removes
network reachability without replacing the workload.

## Routes and the HTTP router

An Environment can enable one logical HTTP router. Routes belong to the
Environment, not Caddy: disabling the router preserves them and reports them as
`unserved`. Enabling it applies the stored Routes.

The accepted proxy choice is per Environment: Caddy for HTTP-only routing or
upstream Envoy for HTTP, TCP and UDP. Only one may be enabled at a time; different
Environments choose independently. Configure or Enable selects the implementation
in the existing router slot, using the same Blueprint-backed action in Console,
CLI and API. Changing the engine of an existing Service proxy requires that
Service's explicit Recreate strategy; GP does not silently replace a blue-green
or rolling proxy. Routes and stable resource identities are retained. Both enabled
router replacement and disabled-router selection capture affected Service work
before accepting the change. Selected HTTP-only Recreate switching in both
directions passed [H103](../acceptance.md#historical-evidence-register). Disabled-router
selection, failed switching and interruption remain unqualified.

Envoy supports UDP forwarding with Recreate. Blue-green and rolling reject UDP
before effects; choose Recreate and accept its interruption. Caddy remains
HTTP-only. Transport declarations alone do not prove live forwarding, and Envoy
does not imply Caddy's automatic certificate behavior.

Enable and Configure write the selected Component into the Environment Blueprint.
They add its declaration if absent, or edit its existing declaration in place;
other Component declarations and Environment resources remain unchanged.
No earlier Blueprint Apply is needed: enabling the router also creates its
selected networks if they have not yet been applied.

A Route selects a Service and target port, public or internal exposure, host and
path. Public Routes require a lowercase ASCII DNS host; internal Routes may omit
the host. Paths default to `/`, must be absolute, have no query or fragment and
allow only a terminal `*`. Duplicate host/path matches are rejected. Overlaps
prefer the longest path, then stable Route ID. Routes do not publish application
ports on the host.

Caddy has an ordered Zone list and keeps a reserved address on the first Zone.
Targets must have reachable Zone membership. Additional memberships do not change
that primary address. Use [full Caddyfile templates](router-template.md) to set
request policy; a template cannot grant access absent from GP's Routes.

## Cloudflare Tunnel

Tunnel has its own Zone selection. Its gateway is the first non-internal Zone;
it does not automatically follow Caddy's memberships. GP configures the connector
and its opaque Secret reference. The operator still configures provider DNS,
public hostnames, ingress and origin policy. Tunnel is not an HTTP router.

### Test public access

Explicit access checks are implemented locally but not yet live-qualified. In
Tunnel details, select a deployed ordinary Service, its exposed HTTP port and the
public hostname/path configured in Cloudflare. GP captures the serving Service's
reserved TCP proxy address on a shared Tunnel Zone. A Caddy Component or GP Route
is not required. For a GP-managed public Route, use its Test public access action;
wildcard Routes require a concrete matching request path.

Results separate Tunnel readiness and origin HTTP from the connector's network,
then public DNS, hostname-verified TLS and HTTP from Controller-host outbound
networking. That outbound path is not an independent external monitor. HTTP
401/403 means protected access, not successful application authentication.
The check does not read provider ingress, change DNS, follow redirects, send
application credentials or collect response bodies.

Checks capture the acknowledged runtime, selected public target and token-reference
revision. They do not certify pending configuration or prove which token version
the provider accepted. Changed configuration makes retained results stale. Opening
results refreshes stored evidence without repeating probes. An interrupted check
does not rerun automatically; Retry can finish pending temporary-helper cleanup
only. Start a new check to probe again after cleanup.

## CoreDNS

The Platform resolver serves exact internal Route names using router addresses.
It supports upstream resolvers and per-domain forwarding. Tailnet detection
provides an overridable default. A network Zone is not a DNS zone and does not
automatically become a DNS name.

The managed private registry also uses this resolver. Host Docker and Runner
daemons resolve its internal name through private DNS reachability, not public
DNS. See [Runners](runners.md).

Add exact hostname records in CoreDNS settings. A target is either an IPv4
address or a TCP Service and one of its Zones. Deploy the Service first: GP must
have applied its reserved proxy address before accepting it as a DNS target.
Service records retain stable Service and Zone IDs, not labels or workload IPs.
Recreate and blue-green deployments preserve that proxy address. UDP-only and
Backing Services cannot be selected as Service targets; use an explicit address
when managing such a record yourself. A hostname cannot override a Route name
with a different address.

DNS references block Service, selected Zone and containing hierarchy removal.
The Console offers to remove the records first, waits for the DNS Task, then
opens the resource-removal confirmation. Cancel keeps both. Saving a replacement
is insufficient: last-applied references remain protected until its Task succeeds.
An explicit-address record is operator-managed and has no resource-removal guard.

Provide a full Corefile containing exactly one `{groundplane}` marker for GP's
managed directives. The Console shows the exact generated replacement alongside
the complete desired Corefile. That preview is not proof that those bytes are
active. Invalid configuration must leave serving configuration in
place. Removing DNS restores the captured predecessor resolver configuration.

A configuration Task owns the resolver until application or recovery settles.
The Console links that Task, disables conflicting edits and refreshes ownership
automatically. Observation checks the mounted file, the live reload fingerprint
and serving probes; it does not depend on an old reload message remaining in the
query log tail.

An unresolved failure is recorded without disconnecting the Agent. The resolver
stays locked because restoration has not been proved; unrelated Tasks can proceed.
Use the failed Task's explicit Retry action to retry its sealed configuration.
GP does not repeatedly publish new resolver Tasks over uncertain effects.

### Internal-Zone DNS

An internal Zone can opt into shared
CoreDNS while retaining Docker's Service names and network aliases. Resolving a
public name does not grant a route to that address or access to another Zone.
The setting defaults off and requires a private IPv4 subnet.

Authoring and creation controls exist in Blueprint, Console, CLI and API.
Use `x-gp-allow-dns` in the [Blueprint](../blueprint.md#native-compose-with-groundplane-policy).
Changing the decision does not reconfigure an existing container in place;
workload deployment must use its configured release strategy.

Local resolver selection now captures only materialized, owned internal bridges.
CoreDNS configuration Tasks pin those private listeners and verify both UDP and
TCP replies before acknowledgement. Full Blueprint Service work waits in the
Controller for that acknowledgement, leaving the Agent available for DNS work.

Zone removal now waits in the queue for acknowledged listener withdrawal before
bridge removal, and rechecks that evidence before releasing its subnet. A failed
or timed-out withdrawal retains the resource and reservation. This local path is
not behavior-qualified. Direct Deploy and Rollback capture Network work items
within their visible Task before workload execution. They wait for
successful CoreDNS acknowledgement without occupying an Agent worker. Known
preparation or resolver failures leave workloads unchanged. Queued preparation
and resolver waiting share the release deadline. If bridge preparation loses its
result, recovery checks only those captured networks. Proven absence or matching
ownership and configuration settles that uncertainty; GP preserves the failure
and does not replay the workload. Unknown or conflicting bridges retain the
resource claims. Task details show the preparation, reconciliation and CoreDNS
waiting phases through the existing Console, CLI and API.
Selected container UDP/TCP, managed/local/upstream resolution and named-target
HTTP passed [H105](../acceptance.md#historical-evidence-register). Opt-out and
failure/recovery variants remain unqualified; a saved setting is not serving proof.

## Environment DNS

Environment **Networking → DNS records** owns records served by the shared platform
CoreDNS. Operators choose an Environment, Project or Tenant hostname namespace
under `.internal`; this changes the published name, not the Environment's
ownership or Service lookup scope. The selected namespace labels persist across
owner renames until explicitly edited.

A named Service target keeps its record when that Service is removed. GP must
first withdraw the active answer; the record then waits for a same-named Service
in the same Environment and selected Zone. Only a successfully deployed stable
proxy can supply its new address. Client caches remain subject to DNS TTL.
Stop and Destroy withdraw the Environment-owned answer without deleting the
record. Start makes it eligible again only after successful execution; failed
Start retains the waiting record. A successful new Deploy can also restore it.
This is lifecycle-based selection, not continuous application-health monitoring.

Create, edit and remove records here, through `gp environment dns`, or through
the Environment's `dns-records` API resource. Choose an IPv4 address or a Service
name and Zone. A missing Service is a valid waiting target. Full Blueprint Apply
reconciles these same records; selected-Service Apply cannot change them.

Saving records their desired configuration, not successful activation. The list
shows applied, pending, waiting-for-Service, disabled-resolver or failed status,
with the resolver Task when available. Removed answers remain protected until
CoreDNS confirms withdrawal. Edit forms retain the reviewed revision; if another
change wins, refresh and review before retrying. Namespace edits explicitly adopt
the current owner labels; target-only edits preserve the existing hostname.

Selected namespace, rename, owning-Environment lookup, withdrawal and same-name
rebinding passed [H105](../acceptance.md#historical-evidence-register), including
unchanged-router preservation. Concurrent claims and failed/interrupted withdrawal
remain unqualified. These are shared resolver answers, not private DNS views.

## Configuration and owned resources

Preview shows the saved desired configuration and managed-file rendering, not
unsaved text validation or proof of live files. Disabled or unconfigured
Components have no active config. Their owned resources appear under Component
details rather than ordinary editable collections. A known-ID read is allowed;
direct mutation bypassing the Component is not.

Component configuration requests and responses explicitly identify their
`implementation`. Caddy and Envoy share Zone and alias settings, but are not
interchangeable payloads. A request without that field, with mixed implementation
settings or with a Caddy template in an Envoy config is rejected. See the generated
[API contract](../../openapi.json) for the complete variants.

Unchanged Components should retain their runtime when a Blueprint changes
unrelated resources. A changed Component remains a candidate until its Task
succeeds; failed application retains the active configuration and addresses.

## Design and qualification

[Component boundaries](../decisions/component-boundaries.md) explains the public
SDK and closed capability model. Registered code receives scoped inputs and
returns typed requests; it has no direct database, filesystem, Docker or secret
plaintext access. [Network decisions](../decisions/network-and-shared-access.md)
explain reservations and reachability.

DNS, routing, Tunnel and restart continuity require runtime evidence for the
candidate being shipped. See [current limitations](../capabilities.md) and the
[QA matrix](../qa-matrix.md).
