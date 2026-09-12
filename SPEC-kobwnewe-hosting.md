# Spec: kobwnewe-hosting

Task5 of the owner-approved production initiative. Product and surface contracts
remain authoritative; this module does not authorize production cutover.

## Objective

Host the complete Kobwnewe application through normal Groundplane operations,
with reproducible non-secret inputs and enough runtime evidence to distinguish
desired intent from observed behavior. Keep the external image builder and the
intentional Identity isolation proxies. Remove the QA-only permanent initializer
and public-policy proxy through normal operations after qualification.

## Outcomes and boundaries

- Check in an Environment bundle plus its non-secret companions and a concise
  binding/runbook document. Record source provenance. Do not copy installation
  ids, generated network names, secret values or QA-only application settings
  into a purported portable production deployment.
- Make target labels, network pool/subnets, existing Component Zone ids,
  reusable Secret references, workload/setup image identities, public/private
  hostnames and operator access explicit inputs. Groundplane extensions are
  not Compose-interpolated; never imply that `--var` binds root metadata or
  `x-gp-*`. Existing CLI bundle authoring remains the operator path.
- Use normal ordered pre-deploy Scripts on real consumers for storage and TLS
  preparation, with explicit pinned images/users/grants where needed. Preserve
  idempotent no-op, validated output and atomic publication. Renewal is explicit
  operator work with a documented trust/restart boundary, not background PKI
  automation or an out-of-band initializer.
- Keep workers/schedulers and two Reverb replicas, backing Attach credential
  sharing and Identity's private status/reviewer paths. Production delivery,
  notification, authentication and retention inputs must be bound deliberately;
  disabled or log-only QA transports are not production acceptance.
- Use the managed full Caddyfile template and declared Route substitutions.
  Preserve deny policy, WebSocket paths, stable upstreams and primary router
  IPAM. Provider DNS/ingress and host/LAN routing remain operator-owned.
- Existing Service list/show in Console, CLI and API expose bounded actual
  runtime observation, separately from `runtime_intent`. Missing, invalid,
  disconnected or stale evidence is unavailable, never inferred healthy from
  desired state or a successful historical Task. Retained inactive blue/green
  containers must not masquerade as the current serving workload.
- Overview Route hints summarize the API's closed Route states (`unserved`,
  `pending`, `served`, `degraded`), not unconditional claims that ingress is
  missing. A served provider generation is not proof of public reachability.

## Implementation order

1. Correct Route summary presentation with failing-first tests and local
   browser proof, using the existing API/CLI Route state contract.
2. Close the read-only Service observation seam and authoritative mirrors;
   implement bounded Agent observation, Controller projection and all three
   existing read surfaces. Keep generated artifacts source-derived.
3. Check in and validate the bundle, its setup Scripts and binding runbook.
4. Adopt on disposable QA only after storage/source integrity is qualified.
   Prove first/exact Apply, deploy/update/rollback, worker/scheduler behavior,
   replicated Reverb, Identity isolation and certificate preparation/renewal.

New behavior belongs in capability modules, not the frozen app/root store or
oversized Environment page. Pure summaries stay in focused Console modules;
the shared observation contract is a named cross-binary leaf, Docker reads live
in infra, transport in Agent/channel, and Service projection in its Controller
module. No new public mutation, Task family, dependency or generic query engine.

## Proof and commands

Use adjacent Go tests with rationale comments, typed results and `errs.New/Wrap`;
use existing Console Node tests, semantic components and explicit unknown states.
For example, a UI summary may say `public · provider applied`, never `public
reachable` merely because a Route reports `served`.

Run the smallest affected selection first, then the owned packages and surfaces:

```sh
go test -race -count=1 ./internal/controller/blueprintparser
go test -race -count=1 ./internal/controller/agentchannel ./internal/agent -run Observation
npm --prefix console test
npm --prefix console run build
make generate
```

All tools use the repository's pinned Go/Node versions and repo-local `.tmp`
caches/evidence. Test empty/unknown/failure/stale observations, wrong generation,
retained slot exclusion, missing replicas, reconnect and read-only behavior.
Browser checks cover real rendering and narrow/wide layouts. Bundle proof uses
the real closed parser and normal desired preparation, not YAML syntax alone.
Required CI and live qualification remain task6, not waived by local tests.

Always preserve Secret boundaries and existing application/ingress state. Ask
before production/provider/host-network changes. Never publish credentials,
manufacture health, bypass source/cleanup fences or call deferred QA passing.
Live mutations are still paused behind the recorded storage-integrity incident;
independent implementation continues under the owner's instruction.
