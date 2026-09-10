# Spec: router-template

Module in the owner-approved `CAPABILITY_MAP.md`, 2026-09-10. This replaces the
single insertion-marker restriction with a complete operator-authored Caddyfile.
Product/API/Blueprint contracts remain authoritative; ADR0075 records the seam.

## Objective and contract

The operator edits one complete Caddyfile through existing Component Configure,
CLI config set and API config PUT. Native Caddy syntax owns policy and ordering;
Groundplane substitutes only its reserved namespace. No second hostname,
upstream, port, Route name or template-binding resource is introduced.

- Empty template uses the documented default `{gp.routes}`. That marker expands
  once to the existing deterministic all-Route configuration. `{routes}` is
  removed, not retained as an alias.
- Full templates use `{gp.route:HOST:PATH:FIELD}`. HOST and PATH identify an
  existing Route by its Environment-unique immutable match; HOST may be empty
  for an internal catch-all. FIELD is exactly `host`, `path` or `upstream`.
  Example: `{gp.route:api.example.com:/app/*:upstream}`.
- The planner resolves every reference from its fixed-revision typed Route
  input. `host` is that Route's host, `path` is its matcher (`/` becomes `/*`),
  and `upstream` is its stable Service name plus declared target port. It never
  selects a serving-slot container or an observed address.
- Full custom files must account for every declared Route using its `upstream`
  reference, or use the aggregate marker. Missing Routes, malformed/unknown GP
  references, invalid UTF-8, NUL and templates over32KiB fail before effects.
  Native Caddy placeholders such as `{host}` and `{uri}` are not GP variables.
- Routes remain authoritative for GP references and reachability. A custom
  Caddyfile can deliberately restrict their request policy, including denying
  private paths. It does not create Route resources or grant Service access.
  A new Route may require a matching complete template edit; switch temporarily
  to the aggregate default when separate normal Route edits need that flexibility.
- The existing config read returns the durable template and its rendered
  managed-file preview. This is a read-only current-decision preview, not an
  unsaved-draft Caddy syntax-validation endpoint or proof of live application.
- The Agent validates the complete staged file with the pinned Caddy image
  before its materialization can replace serving bytes or Compose can start a
  candidate router. The existing in-container validation still precedes reload.
  Invalid output retains the previous serving config; failed
  restoration cannot be reported as successful application.
- Primary-Zone IPAM, router alias,80/443 on the pinned router interface and
  independently configured Tunnel membership/ingress remain unchanged. Host
  routing, firewall, provider DNS and certificate automation are outside scope.

## Structure and style

Pure rendering and its tests: `registered-components/caddy/`. The public SDK
supplies existing immutable HTTP Routes. Generic Component read/planning modules
own preview composition; `internal/app` only wires them. Console Router editing
lives in a focused feature module, not new behavior in the oversized page/store.
Existing CLI/API source schemas remain the wire authority; regenerate clients.

Use concrete typed inputs, SDK-only Component imports and normal root-module
errors at adapters. Example style:

```go
upstream := route.BackendServiceName + ":" + strconv.Itoa(int(route.TargetPort))
```

No new dependency, generic template engine, runtime plugin, host execution or
technology-named Controller/Agent procedure is needed. Preserve native Caddy
braces; never interpret arbitrary template text as Go templates or shell code.

## Implementation and commands

1. Replace the pure renderer and all active contract/example mirrors, with
   failing-first reference/default/policy/injection tests.
2. Return current rendered files through the generic config read and expose
   them alongside the full-file editor. Preserve Enable/Configure parity.
3. Qualify the normal runtime validation/old-config restoration path and adopt
   a managed QA policy through normal operations. Remove no workaround until
   the replacement journey proves its behavior.

With Go1.26.7, Node24.19.0/npm11.17.0 and repository-local Go/temp caches:

```sh
go -C registered-components test -race -count=1 ./caddy
go -C component-sdk test -race -count=1 ./component
go test -race -count=1 ./internal/controller/component ./internal/core ./pkg/api
make generate
npm --prefix console test
npm --prefix console run build
make ci
```

Run narrower package/test selections during red/green; broad qualification is
the initiative's required task6, not a waiver of known gate failures.

## Acceptance and boundaries

Pure tests prove full-file policy survives byte-for-byte outside substitutions,
all GP references resolve or fail closed, native placeholders survive, stable
upstreams never encode slots, inputs are not mutated and primary allocation is
unchanged. Read tests prove preview uses one coherent durable view without writes.
Console/CLI/API tests prove the same complete file and rendered preview.

On disposable QA only, verify host/path HTTP and held WebSocket delivery through
both replicas, stable upstream, router alias and public Tunnel; verify explicit
deny policy, direct router ingress where the authorized topology permits it,
and old-config retention after invalid full-file edits. Preserve bounded evidence
and original workload/Component identities. Record unavailable direct-LAN/client
trust checks for task6; public Tunnel success does not prove those paths.

Always preserve Route/Zone/Component ownership, secret isolation and1:1 parity.
Never change host/Tunnel/provider policy to pass a check. Production cutover
still requires explicit operational authority.
