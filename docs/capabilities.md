# Capability status and limits

**GP is not fully qualified for production.** The architecture cleanup and test
migration are integrated; local delivery checks do not qualify live hosting,
upgrades or recovery. Earlier operator-journey passes apply only to their recorded
builds and cases, not automatically to the restructured source.

The [QA matrix](qa-matrix.md) owns behavioral cases and variants.
[Acceptance](acceptance.md#historical-evidence-register) owns historical results. This page summarizes gaps;
it does not redefine the [product contract](mvp.md) or authorize implementation.

## Available implementation and remaining proof

| Area | Implementation position | Remaining qualification or limitation |
| --- | --- | --- |
| Host, Controller and Agent | Native Controller, owned Agent and independent etcd lifecycle; staged updates and recovery paths exist. | Candidate-bound fresh installation, component-specific updates, failed activation, in-flight interruption and ingress continuity. Historical restart/upgrade passes are bounded. |
| Hierarchy and networking | Tenant/Project/Environment lifecycle, Zone reservations and aggregate deletion exist. | Current partial-failure, concurrency and physical-cleanup proof. Conflicting old Tasks are not repaired by admission guards. |
| Services and Release Groups | Lifecycle, immutable Releases, singleton blue-green, recreate and serial groups exist. | Exact-count replicas, grouped hooks, tag selection and recovery after configuration changes require complete current-case evidence. Rolling and replicated blue-green are unsupported. |
| Observation and logs | Serving observations, Task streams and transient workload logs exist. | Current live health/Route parity and log-follow interruption checks. The last recorded Environment log-stream rerun is outstanding. |
| Blueprints | Authoring, validation, bounded publication and immutable execution paths exist. Entry export and reconciliation include direct Entries; omission plans their removal. | One live running-Service Entry omission passed; full Apply/reapply and other removal variants remain unqualified. Validate does not run every Apply preparation check. Native configs/secrets, external networks and Volume driver options are rejected by current Apply; see the [syntax reference](blueprint.md#native-compose-with-groundplane-policy). Other resource omissions can still be retained or rejected rather than removed. Latest-wins is not connected end to end; `x-gp-network` is reserved, not operable. |
| Volumes and Entries | Managed storage, configuration materialization and deletion exist. | Mounted-consumer cleanup, source retirement, pinned-file recovery and interruption need candidate-specific proof. |
| Scripts | Ordered hooks and explicit image/user/resource contexts exist. | First Apply, exact reapply, cleanup, Abort and unknown-outcome recovery on current runtime. Scripts cannot undo arbitrary writes. |
| Backing services | PostgreSQL, explicit Valkey authentication modes and Custom creation/hooks exist. | Current live provisioning, hook timeout/retry and credential-retention evidence. New Custom hook owners must Attach before a Blueprint consumes their facts. Permanent backing deletion is not supported. |
| Components and ingress | CoreDNS, Caddy and Cloudflare Tunnel integrations exist. | Real DNS, template/reload, restart and selected public/private ingress continuity. Provider setup is not performed by GP. |
| Secrets and Connectors | Scoped Secret lifecycle and Environment-owned Connector metadata exist. | Current execution pin/deletion races and provider use. Connector creation does not validate remote storage. |
| Backup/Restore | Policy, source, key and Recovery Point scaffolding plus partial execution exist. | Incomplete and deferred. Restore is not operationally qualified; Valkey has no safe accepted source/artifact contract. |
| GitHub Runners | Direct-rootless runtime, private registry/CoreDNS wiring and explicit image Fetch through API/CLI/Console are integrated. H75 qualifies a Project-owned trusted workflow through Deploy, build/push/fetch failure preservation, same-boot listener restart and owned removal on Ubuntu amd64. | Full isolation, token-failure, quota/concurrency, reboot, private-address change, registry-restart and arm64 variants remain unqualified. Moved-tag/replay checks are local only. Creation needs a fresh operator registration token; registered restart does not. |
| Console, CLI and API | Production surfaces and generated clients exist. | Current generation/build cleanliness and complete operator-action parity. Packaging or schema generation alone does not prove parity. |

## Release boundary

The owner deferred Backup/Restore from the immediate hosting assessment. That is
not a Gate B pass or data-loss acceptance. Failed-rollout recovery remains part
of hosting safety and is distinct from restoring a database backup.

Select an exact candidate and required deployment-specific cases before QA.
Record failures and missing variants openly; the owner decides repairs, further
scope and operational permission. A single-host reboot has unavoidable downtime,
and recreate intentionally interrupts workloads. Do not advertise high
availability or unconditional zero interruption.

[Remaining qualification](issues/runtime-qualification.md) records the actionable
proof gaps and known unresolved observations. Historical incidents do not describe
the current host or impose a permanent QA pause.
