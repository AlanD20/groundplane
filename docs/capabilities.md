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
| Host, Controller and Agent | Native Controller, owned Agent and independent etcd lifecycle; staged updates and recovery paths exist. Software preparation supports Controller, Agent or Both from source refs or component releases, with registry-pinned outputs and a separate Apply. [H97](acceptance.md#source-software-preparation-and-activation) qualifies selected source builds and normal activation on Ubuntu amd64. | Published artifact QA is deferred. Partial Both activation, broader interruption/replay, held connections, other operating systems and arm64 remain unqualified; historical passes are bounded. |
| Hierarchy and networking | Tenant/Project/Environment lifecycle, Zone reservations and aggregate deletion exist. | Current partial-failure, concurrency and physical-cleanup proof. Conflicting old Tasks are not repaired by admission guards. |
| Services and Release Groups | Lifecycle, immutable Releases, singleton blue-green, recreate and serial groups exist. | Exact-count replicas, grouped hooks, tag selection and recovery after configuration changes require complete current-case evidence. Rolling and replicated blue-green are unsupported. |
| Observation and logs | Serving observations, Task streams and transient workload logs exist. | Current live health/Route parity and log-follow interruption checks. The last recorded Environment log-stream rerun is outstanding. |
| Blueprints | Authoring, validation, bounded publication and immutable execution paths exist. Entry export and reconciliation include direct Entries; omission plans their removal. | One live running-Service Entry omission passed; full Apply/reapply and other removal variants remain unqualified. Validate does not run every Apply preparation check. Native configs/secrets, external networks and Volume driver options are rejected by current Apply; see the [syntax reference](blueprint.md#native-compose-with-groundplane-policy). Other resource omissions can still be retained or rejected rather than removed. Latest-wins is not connected end to end; `x-gp-network` is reserved, not operable. |
| Volumes and Entries | Managed storage, configuration materialization and deletion exist. | Mounted-consumer cleanup, source retirement, pinned-file recovery and interruption need candidate-specific proof. |
| Scripts | Ordered hooks and explicit image/user/resource contexts exist. | First Apply, exact reapply, cleanup, Abort and unknown-outcome recovery on current runtime. Scripts cannot undo arbitrary writes. |
| Backing services | Version-independent PostgreSQL, Valkey and MySQL adapter families select explicit catalog versions: PostgreSQL 16, Valkey 9 and MySQL 8.4 initially. [H89](acceptance.md#upstream-postgresql-patch-and-backup-tooling) qualifies an earlier upstream PostgreSQL patch. [H96](acceptance.md#mysql-data-restoration-and-native-lifecycle) adds MySQL provisioning, a credential-owning Attach and native Stop/Start with preserved data. | MySQL grant variants, Detach and patch recovery remain unqualified. Family migration is a breaking pre-release change, not an old-state compatibility path. Custom hook timeout/retry and other credential variants remain unqualified. Permanent backing deletion is not supported. |
| Components and ingress | CoreDNS, Caddy and Cloudflare Tunnel integrations exist. | Real DNS, template/reload, restart and selected public/private ingress continuity. Provider setup is not performed by GP. |
| Secrets and Connectors | Scoped Secret lifecycle and Environment-owned Connector metadata exist. | Current execution pin/deletion races and provider use. Connector creation does not validate remote storage. |
| Backup/Restore | Config, Volume, PostgreSQL and MySQL capture/Restore, key handling, retention and native execution paths exist. Database archives capture observed versions; Restore reviews the exact target and requires acknowledgement of version differences. [H84/H85](acceptance.md#source-restore-retention-and-deletion-boundary), [H87](acceptance.md#bounded-backup-fault-recovery) and [H89](acceptance.md#upstream-postgresql-patch-and-backup-tooling) qualify selected earlier cases. [H96](acceptance.md#mysql-data-restoration-and-native-lifecycle) adds MySQL known-row R2 Restore, original Dump/Restore interruption recovery and changed-target review rejection. | Cross-version acknowledgement and other archive/provider/checkpoint and arm64 variants remain unqualified. Original incidents are not retrofitted; the Config incident remains recovery-required. Valkey has no accepted source/artifact contract. |
| GitHub Runners | Direct-rootless runtime, private registry/CoreDNS wiring and explicit image Fetch through API/CLI/Console are integrated. H75 qualifies a Project-owned trusted workflow through Deploy, build/push/fetch failure preservation, same-boot listener restart and owned removal on Ubuntu amd64. | Full isolation, token-failure, quota/concurrency, reboot, private-address change, registry-restart and arm64 variants remain unqualified. Moved-tag/replay checks are local only. Creation needs a fresh operator registration token; registered restart does not. |
| Console, CLI and API | Production surfaces and generated clients exist. | Current generation/build cleanliness and complete operator-action parity. Packaging or schema generation alone does not prove parity. |

## Release boundary

The scoped object-identity correction passed actual Cloudflare R2 encrypted
PostgreSQL Backup and known-row Restore on amd64 in
[H91](acceptance.md#upstream-postgresql-patch-and-backup-tooling). R2 retention,
multipart, Config/Volume sources and arm64 remain unqualified. The earlier H90
upload remains an orphan with its original VersionId authority; the new pass
does not establish reconciliation of that incident.

The earlier hosting assessment excluded Backup/Restore; it did not establish
a Gate B pass or data-loss acceptance. Failed-rollout recovery remains part
of hosting safety and is distinct from restoring a database backup.

Select an exact candidate and required deployment-specific cases before QA.
Record failures and missing variants openly; the owner decides repairs, further
scope and operational permission. A single-host reboot has unavoidable downtime,
and recreate intentionally interrupts workloads. Do not advertise high
availability or unconditional zero interruption.

[Remaining qualification](issues/runtime-qualification.md) records the actionable
proof gaps and known unresolved observations. Historical incidents do not describe
the current host or impose a permanent QA pause.
