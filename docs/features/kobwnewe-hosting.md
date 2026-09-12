# Reproducible Kobwnewe hosting

## Purpose and scope

Host the complete Kobwnewe application through normal Groundplane operations,
using reproducible non-secret inputs and runtime observations that distinguish
desired configuration from the workload actually serving traffic.

This is the hosting feature in the [production initiative](../../CAPABILITY_MAP.md),
not authority for production cutover. Keep the external image builder and the
Identity isolation proxies. Replace the QA-only permanent initializer and
public-policy proxy through normal operations only after qualification.

## Functional requirements

- Provide an Environment bundle, its non-secret companion files and a concise
  binding/runbook document. Record where its inputs came from.
- Expose target labels, network pools/subnets, existing Component Zone ids,
  reusable Secret references, workload/setup image identities, public/private
  hostnames and operator access as deliberate inputs. Groundplane extensions
  are not Compose-interpolated: `--var` does not bind root metadata or `x-gp-*`.
  Use the existing CLI bundle-authoring path.
- Use ordered pre-deploy Scripts on real consumers for storage and TLS preparation,
  with explicit pinned images, users and grants where needed. Preserve atomic
  output, validated results and idempotent no-ops. Document certificate renewal,
  trust and restart responsibilities as operator work, not background PKI automation.
- Keep workers, schedulers, two Reverb replicas, shared backing Attach credentials
  and Identity's private status/reviewer paths. Bind delivery, notification,
  authentication and retention settings explicitly; disabled or log-only QA
  transports do not satisfy production acceptance.
- Use the full Caddyfile template and declared Route substitutions. Preserve deny
  policy, WebSocket paths, stable upstreams and primary router IPAM. Provider DNS,
  ingress and host/LAN routing remain operator-owned.
- Existing Service list/show in Console, CLI and API must expose current-serving
  workload observations separately from `runtime_intent`. Exclude retained inactive
  blue/green containers. Missing, invalid, disconnected or stale evidence means
  unavailable, not healthy based on desired state or a historical successful Task.
- Overview Route hints summarize the API states `unserved`, `pending`, `served`
  and `degraded`. An applied provider generation does not prove public reachability;
  for example, `public · provider applied` is not a claim that HTTP is reachable.

## Non-functional requirements

- A portable bundle must not embed installation ids, generated network names,
  secret values or QA-only application settings. Make necessary installation
  bindings explicit instead of describing such a bundle as zero-edit portable input.
- Preserve Secret boundaries, source ownership and cleanup rules. Do not change
  existing application or ingress state as a side effect of verification.
- Observation is bounded and read-only. It adds no public mutation, Task family,
  dependency or generic query engine. Never infer health from absent evidence.
- Production, provider and host-network changes require explicit approval. Live QA
  mutations wait for storage/source-integrity qualification; local proof does not
  waive required CI or live acceptance.

## Technical design

Feature behavior belongs in capability modules under the shared
[architecture](../architecture.md). Pure Route summaries belong in focused Console
modules, not the root store or oversized Environment page.

The [Service observation design](../decisions/0077-serving-workload-observation.md)
separates a named cross-binary value
contract, Docker reads in infrastructure, exchange handling in Agent/channel and
source checks/public projection in the Controller's Service module. Its local
implementation and qualification limits are recorded below. Generated contracts remain
derived from their sources.

The bundle depends on [full Caddyfile templates](router-template.md) and
[explicit Script contexts](setup-scripts.md). Its grammar and authoring surfaces
remain in [blueprint.md](../blueprint.md) and [api-cli.md](../api-cli.md).

## Acceptance

- Observation checks cover empty, unknown, failed and stale results; wrong
  generations; retained-slot exclusion; missing replicas; reconnect; and absence
  of mutation. Console checks cover real rendering and narrow/wide layouts.
- Validate bundles through the real closed parser and normal desired preparation,
  not YAML syntax alone. Include the non-secret companion files and binding runbook.
- After the integrity pause ends, prove first Apply, exact reapply, deploy, update,
  rollback, workers/schedulers, replicated Reverb, Identity isolation and certificate
  preparation/renewal on disposable QA. Preserve bounded evidence and existing state.

Use the [verification policy](../delivery.md#verification-ladder), repository-pinned
tools and repository-local caches/evidence. The Blueprint parser, Agent/channel
observation paths and Console tests are the relevant starting points. Current
qualification tasks are in [tasks/todo.md](../../tasks/todo.md).

## Current status

The Route-summary correction has [local test and browser evidence](../acceptance/router-and-visibility.md)
and is committed, but is not deployed. Service observation's protocol, read-only
Docker observer, Agent/channel, Controller source/freshness and existing
API/CLI/Console surfaces are implemented with focused local proof; see
[Service status](services-and-releases.md#current-status). The portable bundle
remains incomplete. Full CI, deployment and live hosting qualification remain open.

[head.md](../head.md) records the next work and the
[storage-integrity pause](../acceptance/storage-integrity-incident.md).
