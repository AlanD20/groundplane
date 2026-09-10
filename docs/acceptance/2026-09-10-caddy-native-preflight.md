# Caddy native preflight correction

Task3 live inspection found an existing safety gap: Environment Configure
materialized the Caddyfile and applied Compose before running native validation.
The earlier platform managed-config rollback tests did not cover this path.
Bad-file QA was paused before an invalid candidate was submitted.

The failing-first `TestMaterializationRejectsComponentFileWithoutPreflight`
demonstrated the unsafe writer call. The Agent now correlates the exact sealed
Component action to its materialization and validates the complete owned bytes
before invoking the file writer. Ordinary non-Component materialization stays
unchanged. Missing/duplicate/mismatched authority, changed bytes, source-close
failure, native rejection and cancellation fail before the write. Production
wiring selects the compiled Caddy recipe, whose stdin command is now part of
the catalog digest. No API or protobuf field is added.

The separate terminating validator requires a zero exit, complete output drain
and container cleanup. An initial test caught a connection-close/output-drain
race; normal completion now waits for EOF under the bounded cancellation guard.
The existing long-running CoreDNS readiness validator remains unchanged.

The pinned QA image was exercised without serving mounts or network, with a
read-only root, unprivileged65534:65534, private16MiBtmpfs XDG state,256MiBmemory,
half a CPU,64PIDs, no retained logs and only`NET_BIND_SERVICE`. An initial
capability-free probe failed to exec with`operation not permitted`; retaining
the existing validator boundary's single capability fixed it. Native complete
policy and `tls internal` provisioning both exited0. The invalid global
directive exited1. Probe containers were automatically removed; no real router
file or certificate state was mounted. These are isolated image checks, not
yet the normal Configure end-to-end rejection result.

Focused Agent/materialization, compiled wiring, native-validator and registered
catalog/Caddy race tests pass. Native failure, cancellation and failed cleanup
cannot report success. A Route-specific failing regression also proves the
transitive file→Compose→activation prerequisite, alongside Blueprint's direct
file prerequisite. Affected vet passes and generated artifacts are unchanged.
Architecture still reports the accumulated mandatory task6 corrections; the
exact latest output is `caddy-preflight-architecture.log`. Evidence uses
`caddy-preflight-*` and `caddy-qa-preflight-*` under
`.tmp/production-mvp-20260910/`. Required full CI remains task6.

Next: guarded deployment, managed full policy, direct/public HTTP and deny
matrix, replicated held WebSockets, then a normal invalid Configure while
proving the prior serving bytes and runtime survive. Restore the saved valid
desired file normally after that failure. Valid Environment Configure can still
recreate ingress Component containers; no zero-downtime Configure claim is made.
