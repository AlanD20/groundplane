# Full Caddyfile templates

Let an operator edit a complete Caddyfile through the existing Component Configure,
CLI config-set and API config-PUT actions. Caddy syntax owns request policy and
ordering. Groundplane substitutes only its reserved references to existing Routes.
There is no second hostname, upstream, port or template-binding resource.

[Configuration materialization and Scripts](../decisions/configuration-materialization-and-scripts.md#router-configuration-owns-the-complete-native-file) records the design.
Shared product, API and desired-state rules remain in [mvp.md](../mvp.md),
[api-cli.md](../api-cli.md) and [blueprint.md](../blueprint.md).

## Configuration and behavior

- An empty template uses `{gp.routes}`, which expands once to the deterministic
  configuration for all Routes. The former `{routes}` marker is rejected.
- A custom file uses `{gp.route:HOST:PATH:FIELD}`. `HOST` and `PATH` identify an
  existing Route by its immutable, Environment-unique match. An empty host selects
  the internal catch-all. `FIELD` is exactly `host`, `path` or `upstream`.
  Example: `{gp.route:api.example.com:/app/*:upstream}`.
- Resolve references from one fixed-revision Route input. `host` returns the
  Route's host; `path` returns its matcher, with `/` rendered as `/*`; `upstream`
  returns the stable Service name and declared target port, never a serving-slot
  container or observed address.
- A full custom file must reference every declared Route's upstream, or use the
  aggregate marker. A new Route may therefore require a template edit. Operators
  can temporarily use the aggregate default while making separate Route changes.
- Keep native placeholders such as `{host}` and `{uri}` unchanged. A custom
  Caddyfile may restrict or deny requests, but cannot create Routes or grant Service
  access. Route resources still define Groundplane references and reachability.
- Config reads return the saved template and rendered managed-file preview from
  one consistent durable view, without writes. This is not unsaved-draft validation
  or proof of the live router's configuration.
- The Agent validates the complete staged file with the pinned Caddy image before
  replacing serving bytes or starting a candidate router. In-container validation
  also precedes reload. Invalid output retains the old serving configuration;
  failed restoration is never reported as successful application.
- File-only template and Route edits reload Caddy and retain unchanged Component
  and Tunnel runtime ownership. A read-only configuration-directory bind makes
  atomic file replacements visible. Image, mount, user or network changes still
  reconcile the affected container; migration from the former file bind is such
  a runtime change, not an interruption-free reload.

## Non-functional requirements

### Optional router network alias

Portable HTTP-router settings may declare one DNS-label `alias`, attached only
to the primary selected Zone. Disable/re-enable and reconstruction retain it;
it does not rename the generated Service or change its stable ID or ports.
Empty removes the alias. Omitted CLI flags preserve the loaded declaration;
omission from a complete Blueprint declares no custom alias. Configure and
Enable share the same Console/CLI/API field, and Blueprint uses
`x-gp-components.http-router.settings.alias`.

Reject invalid labels and same-Zone collisions with native or generated Service
names and aliases before publication. Caddy emits the existing typed SDK network
attachment; no host DNS, Cloudflare configuration or execution authority changes.

### Safety and limits

- Reject missing Routes, malformed or unknown Groundplane references, invalid UTF-8,
  NUL bytes and templates over 32 KiB before any runtime effect.
- Preserve native Caddy text outside substitutions. Do not interpret the template
  as shell code or a Go template, mutate input Routes or encode serving slots.
- Preserve primary-Zone IPAM, the router alias, ports 80/443 on the pinned router
  interface and independently configured Tunnel membership and ingress.
- Preserve Route/Zone/Component ownership, secret isolation and Console/CLI/API
  parity. Host routing, firewall, provider DNS and certificate automation are out
  of scope. Production cutover requires separate operational approval.
- Use the public Component SDK. No new dependency, generic template engine,
  runtime plugin, host-execution permission or Caddy-specific Controller/Agent
  procedure is introduced.

## Design and qualification

The [Component boundary](../decisions/component-boundaries.md) keeps rendering in
[the Caddy integration](../../registered-components/caddy) and execution in
Groundplane's generic capabilities. [Configuration decisions](../decisions/configuration-materialization-and-scripts.md)
explain captured sources and safe file replacement.

The [QA matrix](../qa-matrix.md) tracks routing, invalid-config retention and
connection continuity. Current source has not been requalified after restructuring.
