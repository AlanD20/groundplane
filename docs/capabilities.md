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

Missing implementation or evidence does not narrow the product contract. See
[head.md](head.md) for the current work checkpoint and [tasks/todo.md](../tasks/todo.md)
for the remaining production initiative.

## Product-wide index

| Capability | Current position | Remaining gap or qualification limit |
| --- | --- | --- |
| Host, Controller, Agent and updates | Host health and the Controller-owned Agent lifecycle have recorded qualification. Native Controller and coordinated Agent update paths have implementation and bounded QA evidence. | Whole-build Tunnel continuity and the remaining recovery cases are unresolved. Live mutation proof is paused pending storage integrity. See [Safe updates](features/upgrade-safety.md). |
| Tenants and Projects | Lifecycle, persistence, protected mutations and aggregate deletion have recorded qualification. | Recheck live state only when a later journey depends on it; dated evidence is not current host health. |
| Environments | Creation, identity, network-pool editing, logs and hierarchy deletion are implemented with recorded bounded evidence. | Complete Environment qualification remains coupled to its Service, Route, Script, Blueprint and recovery paths below. |
| Services, Releases and rollback | Direct lifecycle and historical singleton deploy/rollback paths have recorded qualification. | Exact-count recreate, replicated rollout and recovery, observed Service state, and alignment with Release Groups and logical Scripts remain open. |
| Zones, Routes and HTTP routing | Durable Zone and Route lifecycles and operator surfaces are implemented. The Route summary has recorded 2026-09-12 local evidence. | Deploy the Route summary, complete Caddy template/reload qualification, and prove HTTP, WebSocket, callback and deny-default policy on the approved QA topology. See [Router templates](features/router-template.md) and [Kobwnewe hosting](features/kobwnewe-hosting.md). |
| Volumes and Entries | Lifecycle, desired-revision integration, materialization and removal have recorded qualification. | Reprove only the source-specific persistence and restore paths exercised by production qualification. |
| Scripts and deploy hooks | Ordered hooks, explicit contexts, immutable sources and grants, all human surfaces, and runtime composition are implemented with focused local evidence. | Real first Apply and exact reapply, certificate setup, migration failure, Abort, reconnect, unknown-outcome recovery and guarded native-write proof remain. See [Script execution](features/setup-scripts.md). |
| Backing services and Attaches | PostgreSQL and Valkey creation plus Attach lifecycle have recorded qualification. Valkey authentication modes have focused and isolated-container evidence. | Live Groundplane qualification of the Valkey authentication extension and its backup/restore behavior remains. |
| Components and CoreDNS | The closed Component boundary, Environment Caddy/Cloudflare components and CoreDNS template/config surfaces are implemented. | CoreDNS reference-host qualification remains. Caddy template and retained-reload work has the limits recorded in [Router templates](features/router-template.md). |
| Secrets and Connectors | Project/platform Secret lifecycle and Environment S3-compatible Connector lifecycle have recorded qualification. | Credential use during real backup, restore and production recovery is part of the recovery gap, not proved by metadata lifecycle evidence. |
| Backups and restores | Policy, source catalog, scheduling, points, retry foundations, age-key rotation/export and parts of the execution protocol are implemented. | Terminal delivery, actual-source capture and upload, PostgreSQL runtime, Config/Volume transfer, staging recovery, Restore, remote cleanup, failure injection and end-to-end proof remain. Restore stays unavailable until its production path and overwrite confirmation are complete. |
| GitHub Runners | Durable lifecycle, isolation and negative-path cleanup have recorded evidence. | Ready-state creation and real job execution still require a short-lived GitHub token. The approved production initiative retains external image building, so Runner completion is not a cutover prerequisite. |
| Tasks and Activity | Durable Tasks, retry, abort, events, retention and hierarchical Activity have recorded qualification. | Feature-owned Task producers still require their own failure and recovery proof. |
| Release Groups | Ordered singleton deploy, failure and switch-back behavior has recorded evidence; current surfaces and runtime are implemented. | Replicated members, persisted/request tag precedence and grouped Script-hook behavior remain unqualified. |
| Blueprint | Canonical show/validate/apply, desired resources, immutable execution inputs and bounded final publication are implemented across substantial source paths. | Generated/full verification, remaining Script and recovery failures, non-Entry removal coverage, and complete reference-host application proof remain. |
| Console delivery and parity | The production SPA packaging and API-first serving path have recorded qualification. | Full operator-surface parity, generation cleanliness and the remaining feature actions must pass the final gates; packaging evidence does not qualify every Console action. |
| Portable Kobwnewe hosting | Route visibility is implemented locally. | Service observation is unfinished, the portable non-secret bundle is absent, and deploy/reapply/update/rollback plus worker, scheduler, replicated Reverb, Identity and certificate journeys remain. See [Kobwnewe hosting](features/kobwnewe-hosting.md). |

The acceptance index contains the dated evidence behind recorded qualification:
[acceptance.md](acceptance.md). Keep each claim bounded to the source revision,
artifact and topology named there.

## Minimum hosting gates

The current initiative builds on earlier manual QA; it has not completed either
gate below.

**Gate A** proves on the disposable acceptance host:

- supported Linux/Docker bootstrap and Groundplane installation;
- the generic Tenant, Project, Environment and Blueprint model;
- PostgreSQL and Valkey facts, uid/gid-aware Volumes, Entries and files;
- Identity TLS before start;
- exact HTTP, WebSocket, internal-callback and deny-default routing policy;
- the Cloudflare Tunnel-to-Caddy path;
- more than one WebSocket replica with Valkey fan-out;
- migrations once per Release Group; and
- deploy, rollback, restart recovery, exact reapply, logs and Tasks.

External image building is retained for this initiative. Groundplane-managed
Runner provisioning is therefore not a Gate A or Gate B prerequisite, although
the broader Runner capability remains unfinished.

**Gate B** adds backup, verified original-target restore and retention for every
actual persistent source used by the workload. It also requires the relevant
full CI, generated parity, security, production build and operator-surface gates.

Passing a focused suite, local browser check, isolated activation or one traffic
sample does not complete either gate. Every exercised destructive path must keep
its failure, retry and recovery proof.

## Current qualification pause

Live mutations on disposable QA `10.25.0.2` are paused by the
[2026-09-10 storage incident](acceptance/2026-09-10-build-capacity-incident.md).
Recovered free space and healthy processes did not prove persisted integrity.
Before further mutation, qualify the virtual disk and each affected persistent
source, including Valkey AOF state, and preserve the incident evidence.

Production, upstream devices, BIP, provider DNS/ingress, host firewall and any
other host remain outside QA authority. Production work requires the explicit
target, data-transfer authority and cutover approval listed in
[tasks/todo.md](../tasks/todo.md).

## Completion

The production initiative is complete only when its feature requirements are
implemented, Gate A and Gate B pass within their named scope, every relevant
safety/data/parity/security issue is closed, required generated artifacts are
clean, and the final CI and Console gates pass. A working fixture, green focused
suite, historical host result or source inspection alone is not completion.
