# Product capability status

This is the product-wide implementation and qualification index. It does not
define behavior. Product requirements live in [mvp.md](mvp.md), human surfaces
in [api-cli.md](api-cli.md), desired state in [blueprint.md](blueprint.md), and
implementation boundaries in [architecture.md](architecture.md).

Status is deliberately conservative:

- **Recorded qualification** means dated evidence proves the named scope. It is
  not a statement about current host health.
- **Implemented** means the production path exists, but required operator or
  failure-path qualification remains.
- **In progress** means required production behavior is still being built.

Missing implementation or evidence does not narrow the product contract.
The feature entries below record remaining implementation and qualification.

[The product QA matrix](qa-matrix.md) breaks these areas into behavioral cases,
failure/recovery sequences and reviewed execution evidence. This coarse capability
index is not a substitute for case-by-case qualification.

## Product-wide index

| Capability | Current position | Remaining gap or qualification limit |
| --- | --- | --- |
| Host, Controller, Agent and updates | Host health and the Controller-owned Agent lifecycle have recorded qualification. Native Controller and coordinated Agent update paths have implementation and bounded QA evidence. | Whole-build Tunnel continuity and the remaining recovery cases are unresolved. Live mutation proof is paused pending storage integrity. See [Safe updates](features/upgrade-safety.md). |
| Tenants and Projects | Lifecycle, persistence, protected mutations and aggregate deletion have recorded qualification. | Recheck live state only when a later journey depends on it; dated evidence is not current host health. |
| Environments | Creation, identity, network-pool editing, logs and hierarchy deletion are implemented with recorded bounded evidence. | Complete Environment qualification remains coupled to its Service, Route, Script, Blueprint and recovery paths below. |
| Services, Releases and rollback | Direct lifecycle and historical singleton deploy/rollback paths have recorded qualification. Service observation is implemented across Agent, Controller and operator surfaces with focused local proof. | Deploy and qualify observation; exact-count recreate, replicated rollout and recovery, and alignment with Release Groups and logical Scripts remain open. |
| Zones, Routes and HTTP routing | Durable Zone and Route lifecycles and operator surfaces are implemented. The Route summary has recorded 2026-09-12 local evidence. | Deploy the Route summary, complete Caddy template/reload qualification, and prove HTTP, WebSocket, callback and deny-default policy. See [Router templates](features/router-template.md). |
| Volumes and Entries | Lifecycle, desired-revision integration, materialization and removal have recorded qualification. | Reprove only the source-specific persistence and restore paths exercised by production qualification. |
| Scripts and deploy hooks | Ordered hooks, explicit contexts, immutable sources and grants, all human surfaces, and runtime composition are implemented with focused local evidence. | Real first Apply and exact reapply, resource preparation, hook failure, Abort, reconnect, unknown-outcome recovery and guarded native-write proof remain. See [Script execution](features/setup-scripts.md). |
| Backing services and Attaches | PostgreSQL and Valkey creation plus Attach lifecycle have recorded qualification. Valkey authentication modes have focused and isolated-container evidence. | Live Groundplane qualification of the Valkey authentication extension and its backup/restore behavior remains. |
| Components and CoreDNS | The closed Component boundary, Environment Caddy/Cloudflare components and CoreDNS template/config surfaces are implemented. | CoreDNS reference-host qualification remains. Caddy template and retained-reload work has the limits recorded in [Router templates](features/router-template.md). |
| Secrets and Connectors | Project/platform Secret lifecycle and Environment S3-compatible Connector lifecycle have recorded qualification. | Credential use during real backup, restore and production recovery is part of the recovery gap, not proved by metadata lifecycle evidence. |
| Backups and restores | Policy, source catalog, scheduling, points, retry foundations, age-key rotation/export and parts of the execution protocol are implemented. | Terminal delivery, actual-source capture and upload, PostgreSQL runtime, Config/Volume transfer, staging recovery, Restore, remote cleanup, failure injection and end-to-end proof remain. Restore stays unavailable until its production path and overwrite confirmation are complete. |
| GitHub Runners | Durable lifecycle and earlier isolation/negative-path cleanup have recorded evidence; this does not prove the complete accepted runtime contract. | The [Runner contract](features/runners.md) retains missing host/runtime, approved token-handoff implementation, parity and qualification work. Ready-state creation and real jobs also require a fresh GitHub token. |
| Tasks and Activity | Durable Tasks, retry, abort, events, retention and hierarchical Activity have recorded qualification. | Feature-owned Task producers still require their own failure and recovery proof. |
| Release Groups | Ordered singleton deploy, failure and switch-back behavior has recorded evidence; current surfaces and runtime are implemented. | Replicated members, persisted/request tag precedence and grouped Script-hook behavior remain unqualified. |
| Blueprint | Canonical show/validate/apply, desired resources, immutable execution inputs and bounded final publication are implemented across substantial source paths. | Generated/full verification, remaining Script and recovery failures, non-Entry removal coverage, and complete reference-host application proof remain. |
| Console delivery and parity | The production SPA packaging and API-first serving path have recorded qualification. | Full operator-surface parity, generation cleanliness and the remaining feature actions must pass the final gates; packaging evidence does not qualify every Console action. |

The acceptance index contains the dated evidence behind recorded qualification:
[acceptance.md](acceptance.md). Keep each claim bounded to the source revision,
artifact and topology named there.

## Acceptance

[The MVP acceptance gates](mvp.md#acceptance-gates) define hosting and recovery
proof. They remain incomplete. Each row above also retains the requirements of
its owning feature document; a focused suite or one live journey does not
qualify unrelated capabilities.

Live mutations remain paused pending storage and source-integrity qualification.
Current operational authority comes from explicit user instructions. Historical
host observations do not lift the pause.

## Completion

MVP completion requires its accepted feature requirements, operator journeys,
failure and recovery cases, generated artifacts and full CI gates to pass.
Every relevant safety, data, parity and security issue must be closed. A working
fixture, historical host result or source inspection alone is not completion.
