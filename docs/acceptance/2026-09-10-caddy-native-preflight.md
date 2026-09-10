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

## Live normal Configure rejection

Guarded I from85a472476 completed as`task_01M252Z2VPKGZJ04QGFSFDM4W9`,
running`0.0.0-qa.native20260910.9`. Host reports Controller SHA256
`44be776461eeb499d2dc9a9179f5aa5802b931b6540597b970bcd004ff1c7c53`,
Agent image digest`68adb0af63b85d04e16e9e932debc32b02390dceaf97a45399388dc4e2b91316`
and qualified release`712f73626abdb60c67ee108a2fc03ec1278d83dd629accff24c35a7de35db2ea`.
The fresh activation sample completed3000held WebSocket deliveries. The longer
whole-build HTTP sample completed1200requests with two502failures, both before
activation during the reproduced public Tunnel timeout. These are not zero-failure
whole-path results; mandatory task6 retains the exact build/Tunnel failure.

Normal CLI Configure submitted an otherwise valid aggregate template containing
one invalid native global directive. Task`task_01M253SD6A7THATCAN842SR3KG`
failed at its first step at07:33:52UTC; all later steps remained unexecuted.
Caddy retained digest`86af0b07deb16cc9b5bff2af79825665038692973c2c642692b15369aee373e8`,
container2f69cbba8619and Tunnelb8827c3b7c94. Public/login and/up both returned200
with TLS verification0. This proves the previously missing normal native-rejection
boundary. The failed desired candidate remains repairable through normal Configure;
no file, container or durable record was patched out of band.

Evidence: `qa-native-update-i-deploy.log`, `caddy-preflight-qa-installed.txt`,
`caddy-preflight-upgrade-first-failure.txt`, both continuity logs,
`caddy-invalid-native-configure.json` and `caddy-invalid-native-retention.txt`
under the same ignored evidence directory. The installed-version evidence uses
Host's executing-binary SHA; a separate attempted filesystem hash used an absent
path and is not claimed as independent proof.

Next: deploy the separate retained-runtime reload correction, replace the failed
desired file through normal Configure with managed full policy, then prove the
direct/public HTTP/deny matrix and held WebSocket continuity across a file-only
reload. Migrating the old single-file mount is an actual runtime change.
