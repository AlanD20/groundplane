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
| Environment DNS | Blueprint/export and Console/CLI/API share Environment-owned records, captured namespaces, stable targets and durable resolver updates. Internal-Zone DNS opt-in uses the shared CoreDNS instance. H105/H106/H108 qualify selected namespace, container resolution, opt-out and named-Service lifecycle journeys. | Broader supersession, concurrent claims and interrupted withdrawal/retention remain unqualified. Resolution is shared, not an Environment-private DNS view. |
| Services and Release Groups | Lifecycle, immutable Releases, singleton blue-green, recreate, rolling and serial groups are integrated. H98 qualifies selected scaling, strategy transitions and supersession recovery; H109 qualifies image-only rolling rollback using current configuration. | Other counts, grouped hooks, held connections and broader interruption boundaries remain unqualified. Replicated blue-green is unsupported. |
| Observation and logs | Serving observations, Task streams and transient workload logs exist. | Current live health/Route parity and log-follow interruption checks. The last recorded Environment log-stream rerun is outstanding. |
| Blueprints | Public Apply uses resource-level reconciliation, protected omissions and latest-wins settlement. H110/H111 qualify selected interruption/recovery; H116 covers direct-Attach roundtrip, reuse and Abort; H117 covers same-Apply Custom facts; H118 covers combined runtime cleanup and reservation release. | Hook interruption, explicit captured-hook Retry, source retirement and every omission/fault variant remain unqualified. Validate does not run every Apply preparation check. Native configs/secrets, external networks and Volume driver options remain unavailable to Apply; see the [syntax reference](blueprint.md#native-compose-with-groundplane-policy). Persistent Volumes and external-consumer backing Zones still need separate protected removal. |
| Volumes and Entries | Managed storage, configuration materialization and deletion exist. | Mounted-consumer cleanup, source retirement, pinned-file recovery and interruption need candidate-specific proof. |
| Scripts | Ordered hooks and explicit image/user/resource contexts exist. | First Apply, exact reapply, cleanup, Abort and unknown-outcome recovery on current runtime. Scripts cannot undo arbitrary writes. |
| Backing services | Version-independent PostgreSQL, Valkey and MySQL adapter families select PostgreSQL 16, Valkey 9 and MySQL 8.4 initially. H89/H96 cover selected upstream patch and MySQL restoration/lifecycle. H114/H115 qualify selected consumer access checks; H116/H118 cover credential reuse and dependent-before-owner Detach. | Wrong-credential/network/grant faults, interrupted check cleanup, patch recovery and Custom hook interruption/retry remain unqualified. Family migration is breaking pre-release work, not an old-state compatibility path. Permanent backing deletion is unsupported. |
| Components and ingress | One Environment router slot selects Caddy HTTP or Envoy HTTP/TCP/UDP. H99/H103 qualify selected actual transport replies and HTTP engine switching; H112 proves unsupported UDP strategy refusal before effects. H117 qualifies selected Tunnel private/public diagnostic stages and helper absence; H118 covers router/Route cleanup. | Broader failed/interrupted engine switching and recovery/source variants remain unqualified. H101 covers selected default DNS diagnostics, not forwarding/timeout races. Public checks use Controller outbound networking, not an independent external monitor. GP does not configure provider DNS/ingress. |
| Secrets and Connectors | Scoped Secret lifecycle, Connector CRUD and explicit storage permission checks are integrated. H104/H114 record successful bounded R2 draft permission/cleanup checks. Save remains separate from Check. | Saved/edited/Secret-backed check variants, interruption, cleanup recovery and source-pin/deletion races remain unqualified. A permission check does not qualify Backup/Restore. |
| Backup/Restore | Config, Volume, PostgreSQL and MySQL capture/Restore, key handling, retention and native execution paths exist. Database archives capture observed versions; Restore reviews the exact target and requires acknowledgement of version differences. [H84/H85](acceptance.md#source-restore-retention-and-deletion-boundary), [H87](acceptance.md#bounded-backup-fault-recovery) and [H89](acceptance.md#upstream-postgresql-patch-and-backup-tooling) qualify selected earlier cases. [H96](acceptance.md#mysql-data-restoration-and-native-lifecycle) adds MySQL known-row R2 Restore, original Dump/Restore interruption recovery and changed-target review rejection. | Cross-version acknowledgement and other archive/provider/checkpoint and arm64 variants remain unqualified. Original incidents are not retrofitted; the Config incident remains recovery-required. Valkey has no accepted source/artifact contract. |
| GitHub Runners | Rootless runtime, private registry/CoreDNS wiring and explicit image Fetch are integrated. H75 covers a selected trusted Project workflow; H115 qualifies read-only checks inside an existing registered Runner for GitHub, rootless Docker, GP API and registry access. | Full isolation, token/dependency failures, quota/concurrency, reboot, private-address changes, registry restart and arm64 remain unqualified. The diagnostic does not build/push images or prove CI/CD deployment. Creation needs a fresh registration token; registered restart does not. |
| Console, CLI and API | Production surfaces and generated clients exist. | Current generation/build cleanliness and complete operator-action parity. Packaging or schema generation alone does not prove parity. |
| Host terminal and Task diagnostics | Trusted-operator root PTY, independent tabs, owning-browser transport, input-inactivity settings and Console/CLI/API access are delivered. H101/H102 cover selected Keep/reconnect/Terminate, expiry, ownership and shutdown; H111 covers interrupted-Apply details. [H119](acceptance.md#bounded-recovery-storage-reclamation-and-terminal-tabs) adds tab isolation, shared capacity, replay, background output and selected process termination. | Browser-close/route-change presentation, the maximum timeout boundary and every error family remain separately unqualified. Console reachability remains the operator-owned host-control trust boundary. |
| Task retention and storage maintenance | Finite per-operation recovery retries, explicit captured Retry, individually compressed diagnostics, guarded retention and history compaction, threshold/window defragmentation and native settings/status are integrated. H119 proves local fault boundaries and selected installed reclamation, immutable outcomes and data/runtime preservation. | Broader installed retry/capture faults, other platforms and traffic during defragmentation remain unqualified. Held authority may block cleanup or compaction; maintenance cannot bypass it. |

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
