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

The complete candidate receives a native stdin preflight before its Environment
materialization can replace serving bytes or Compose can start a new router.
Its sealed Component action identifies the exact materialization, destination,
digest and generation. The compiled container-action recipe supplies the pinned
image and stdin validation command; catalog identity includes that command.
The existing in-container file validation still runs before reload. This closes
the older ordering gap where the candidate router could start before validation.

Preflight provisions the native configuration without starting listeners, in one
bounded disposable container: no network, host/serving mounts, Docker socket,
retained logging or writable root filesystem; numeric unprivileged user; only
the existing `NET_BIND_SERVICE` capability needed by the pinned executable;
limited CPU, memory and PIDs. XDG data/config writes use bounded private tmpfs.
This creates no new arbitrary file or Secret grant. Success requires exact zero
exit, output drain and owned-container cleanup. Failure, cancellation, unknown
exit, changed bytes or missing recipe prevents the file writer from running.
Native validation is not merely Caddyfile-to-JSON adaptation; the documented
`caddy validate --config - --adapter caddyfile` provisions the modules too.

File-only Configure and Route mutation/removal retain native reload semantics.
The Controller compares complete effective Component service definitions,
image/replica metadata and resource definitions against the captured applied
artifact. Only plan/generation labels are excluded from comparison. Equal
runtime retains that exact ownership; real image, mount, user, environment or
network edits reconcile normally. An unchanged Tunnel is retained as well.
Component preparation already fences its applied-source key at publication;
native-runtime/predecessor preparation supplies the same barrier for ordinary
Blueprint paths. The frozen candidate carries the result through replay, without
consulting a later desired head or Docker observation.

The Agent validates historical Component labels separately from native Release
authority: one Component owner, one pinned-image service, canonical nonfuture
generation, and only a targeted no-dependencies non-forced Compose up. Native
retention rules remain separate. File/action generation may exceed retained
container generation but cannot precede it. Route reconciliation uses the exact
materialize/up/activate chain; it no longer requests force-recreate.

Caddy binds its managed parent directory read-only, so atomic replacement of
the Caddyfile is visible without recreating its container. Activation rejects
the former single-file bind, writable/foreign parent binds and shadowing mounts.
Replacing the old bind requires one actual mount-change reconciliation; later
file-only edits use reload. Native Caddy policy still controls WebSocket reload
behavior (for example `stream_close_delay`). This is distinct from GP native
upgrades, which preserve all ingress and application containers. Invalid native
files must not reach the serving write or Compose step.

Primary-Zone allocation and the [router alias contract](../features/router-template.md#optional-router-network-alias)
are unchanged. This grants no host,
firewall, Tunnel/provider, PKI or operator-supplied execution authority. Registered
Components still receive no filesystem, repository, secrets or arbitrary command.
