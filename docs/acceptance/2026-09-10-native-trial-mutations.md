# Native trial mutation isolation

Upgrade-safety correction discovered during task4's strict-storage audit.
Local proof only; no QA deployment or storage-integrity claim.

## Reproduction and correction

Before the correction, a real Script PATCH route reached its mutation port
during a simulated trial hold (`200`, one write, zero admission checks), and a
scheduler pass reached ordinary expiry, publication and pruning. Pausing Agent
assignments did not prevent these Controller writes.

The native release service now checks the validated journal through a narrow
HTTP/scheduler admission port. Every unfinished phase holds ordinary mutations.
GET/HEAD and the existing read-only POSTs remain usable. Exact Controller Update
acceptance replay remains in its protected idempotent service; new acceptance
still checks the active journal. Only the matching native Task's exact POST Abort
path receives its existing Abort semantics. Adjacent paths, other Tasks, Retry
and other verbs receive no exception. Journal errors fail closed, with private
unexpected causes excluded from HTTP detail. Ordinary writes and maintenance
resume after settlement. App changes only wire the port.

## Writer audit

- Controller Tasks are serial. Startup rejects an unfinished journal without
  its single matching durable claim; this Task owns the executor until settled.
  The native Task/Agent recovery protocol is unchanged and remains operational.
- The ordinary scheduler is now held before expiry, stale-Agent maintenance,
  due Backup publication and idempotency/Task pruning. No prune deadline advances
  while held. It retries normally after journal-read failures or settlement.
- Local Agent reconciliation and native recovery still need their unchanged
  identity/config/token records and runtime convergence. They do not author
  Script contexts, order or desired Environment revisions.
- Existing startup ensures cover the Runner pool, Task schema and platform
  Components/resolver. They retain their existing formats and do not author
  the new Script metadata. Environment Volume auditing is read-only.
- Startup unpublished-Script-source recovery can adjust Script primary counts.
  A strict pre-extension wire decoder now proves those count-only rewrites do
  not add default order/context fields. Nonzero order or an explicitly authored
  context fails that predecessor decoder, establishing why the trial hold is
  necessary. The pinned predecessor shape is from deployed source85a472476.

This audit is scoped to this extension. Equal manifest epochs and additive fields
alone do not prove every future startup write or arbitrary downgrade compatible.
New schema-writing startup behavior must carry its own recovery proof.

## Evidence

All logs are in ignored `.tmp/production-mvp-20260910/`, with pinned Go1.26.7
and repository-local, bounded build/cache settings.

- `native-mutation-admission-red.log`: actual HTTP and scheduler failures above,
  plus the not-yet-defined journal admission method.
- `native-mutation-admission-green.log`: the first wire attempt exposed the
  existing HTTP writer's concrete error type; corrected without weakening kinds.
- `native-mutation-admission-fixed-green.log`: affected HTTP, scheduler, native
  coordinator and app race tests pass. The filtered Controller Task package has
  no matching tests; its full suite follows below.
- `native-mutation-http-final-race.log`: final HTTP/scheduler regressions pass,
  including journal failure privacy and resumed Script authoring.
- `native-mutation-recovery-race.log`: full native coordinator and Controller
  Task executor race suites pass, including protected replay and Abort/recovery.
- `native-trial-script-compatibility.log`: actual count-adjustment wire checks
  and strict predecessor rejection of authored new metadata pass under race.
- `native-mutation-admission-vet.log`: Controller, native coordination,
  Controller Task execution and app vet pass. `git diff --check` passes.

Deferred live proof: guarded candidate trial under held HTTP/WebSocket traffic,
attempt Script/Blueprint mutations before Healthy, verify rejection and unchanged
desired state, then prove settled write admission and failed-trial predecessor
reads. All live mutations remain behind the existing storage-integrity hold.
