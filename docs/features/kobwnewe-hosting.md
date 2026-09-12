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

### Portable authoring and input ownership

The retained QA export is evidence, not the deployment template. It contains
installation-bound Component ids, staging/log-only delivery settings, a permanent
`qa-initialize` Service and a `public-policy` proxy. Author the replacement with
eleven real Services: the four application processes, Identity intake, portal,
worker, scheduler and ClamAV, and the two intentional Identity isolation proxies.
Keep Reverb at two replicas and preserve shared Attach credentials. Only the
designated application and Identity consumers run their respective migration hooks.

Bind these inputs before submitting the closed bundle:

| Input | Binding rule |
| --- | --- |
| Target and networking | Set envelope labels, the Environment pool, Zone subnets, trusted proxy address and existing Component Zone ids explicitly. `--var` does not interpolate the envelope or `x-gp-*` fields. |
| Images and users | Select externally built workload images and a digest-pinned setup image already on the Agent. Verify numeric workload uid/gid; a `www-data` name does not prove its numeric identity. The setup image must contain the shell, filesystem tools and OpenSSL used by its Script. |
| Credentials | Use reusable Secret references or declared Attach facts, never secret bundle files or interpolation values. Preserve application/Identity database separation, the intentional cross-database grant and shared Reverb credentials. |
| Delivery and authentication | Explicitly select production OTP/mail providers, enabled push/realtime delivery and any social providers. Bind their actual required credentials and endpoints; staging test identities, fake social auth and log-only delivery do not qualify. |
| Identity policy | Bind signing key ids/public-key sets, evidence-key versions, reviewer access, retention and deletion policy. A deletion-backlog age threshold is not an evidence-retention duration. |
| TLS and operator access | Select the issuing authority, names, trust distribution, certificate lifetime, renewal and consumer restart responsibilities. Do not turn the disposable QA issuer into a production default. |

The 2026-09-12 input audit inspected the backend at `2ae08b7` and infrastructure
at `7871583`, read-only. Backend `config/services.php` owns OTP, Identity, social
auth and Firebase keys; `config/mail.php` owns mail transports;
`config/realtime.php`, `config/broadcasting.php` and `config/reverb.php` own
the application/realtime bindings. Infrastructure `rpi/identity.compose.yaml`
and its non-secret `.env.app.example`/`.env.identity.example` describe the earlier
deployment inputs. They are navigation sources, not frozen runtime authority;
recheck them against the selected image revisions before qualification.

The current TLS choice requires the owner: consume externally issued certificates
or create a dedicated private CA. The QA initializer creates a disposable 30-day
CA and seven-day leaf certificates, so copying it would silently select an issuer
and renewal policy. Until that choice is resolved, certificate-dependent bundle
implementation is paused. No certificate, trust store or host was changed.

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
