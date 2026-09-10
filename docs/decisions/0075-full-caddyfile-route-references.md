# ADR 0075: Full Caddyfile with reserved Route references

- Status: Accepted
- Date: 2026-09-10
- Capability: Environment HTTP router
- Scope: owner-approved `router-template` production initiative

The operator owns the complete Caddyfile policy rather than only text around a
generated block. Keep the existing typed `caddyfile_template` decision and
Configure/Enable/config-read surfaces. The Component remains an SDK-only pure
planner, and the generic managed-config procedure still validates before reload.

Only `{gp.*}` belongs to Groundplane. `{gp.routes}` is the default aggregate
renderer. `{gp.route:HOST:PATH:FIELD}` selects a Route by its existing immutable,
Environment-unique host/path tuple; FIELD is `host`, `path` or `upstream`.
The latter resolves to stable Service name plus explicit target port, never a
slot or observed IP. Parsing splits the first colon after the prefix and the
last colon before FIELD, so colons inside a valid path are unambiguous. An empty
host identifies the existing internal catch-all. Missing or malformed GP
references fail closed. The former `{routes}` marker is rejected, not aliased;
native Caddy `{host}`, `{uri}` and other placeholders remain Caddy syntax.

This uses an existing immutable desired match rather than generated Route ids
in portable Blueprints, positional indexes or a second binding/hostname table.
Resolution still occurs inside the pinned Route capability input. Full custom
files account for every Route through its upstream reference; aggregate mode
automatically accounts for all. Native policy may deny requests intentionally;
successful provider application is not a promise that every request is allowed.

Existing config GET/CLI show/Console read returns the current template and its
rendered file. It is neither a draft-validation endpoint nor an observation of
live Caddy. Use the same registered planner at a coherent durable view; GET
cannot allocate an address, stage files, capture state or create a Task.
Configure uses the raw typed Component metadata to retain unedited fields,
not a successful preview as a prerequisite for repair. During a namespace
transition, an existing old marker can remain serving until the operator
replaces it through normal Configure; it is not accepted by the new planner.
Preserve surrounding native policy (including automatic-HTTPS settings), not
just Route output, when preparing that explicit replacement.

Native syntax and reload semantics are defined by the pinned Caddy implementation:
[Caddyfile concepts](https://caddyserver.com/docs/caddyfile/concepts),
[validation and reload](https://caddyserver.com/docs/command-line), and
[reverse proxy/WebSocket handling](https://caddyserver.com/docs/caddyfile/directives/reverse_proxy).
Invalid candidate output must preserve the previous serving configuration.

Primary-Zone allocation and ADR0069 aliases are unchanged. This grants no host,
firewall, Tunnel/provider, PKI or extra Component execution authority. Registered
Components still receive no filesystem, repository, secrets or arbitrary command.
