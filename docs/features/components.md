# Managed Components and routing

GP includes three compiled integrations: CoreDNS for host DNS, Caddy for an
Environment's HTTP routing, and Cloudflare Tunnel for its outbound edge tunnel.
There is no runtime plugin loader or operator-uploaded planner. Controller and
Agent are host processes, not Components.

## Routes and the HTTP router

An Environment can enable one logical HTTP router. Routes belong to the
Environment, not Caddy: disabling the router preserves them and reports them as
`unserved`. Enabling it applies the stored Routes.

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

## CoreDNS

The Platform resolver serves exact internal Route names using router addresses.
It supports upstream resolvers and per-domain forwarding. Tailnet detection
provides an overridable default. A network Zone is not a DNS zone and does not
automatically become a DNS name.

Provide a full Corefile containing exactly one `{groundplane}` marker for GP's
managed directives. Invalid configuration must leave serving configuration in
place. Removing DNS restores the captured predecessor resolver configuration.

## Configuration and owned resources

Preview shows the saved desired configuration and managed-file rendering, not
unsaved text validation or proof of live files. Disabled or unconfigured
Components have no active config. Their owned resources appear under Component
details rather than ordinary editable collections. A known-ID read is allowed;
direct mutation bypassing the Component is not.

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
