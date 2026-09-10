# Native Controller coordinator: local evidence

Scope: ADR0074's native Task, activation handoff and recovery module. This is
local implementation proof, not a completed operator surface or QA deployment.

## Implemented boundaries

- Frozen canonical Task input pins release manifest, predecessor executable and
  optional Agent identity/image/generation. Its plan hash and exact 600-second
  claim budget are validated before host effects.
- Task startup restores all-generation admission. Preparation validates the
  actual running and installed predecessor, guard bootstrap and staged bytes;
  active work drains for at most 120 seconds and is never aborted.
- The predecessor commits activation and launches the detached watchdog, but
  never acknowledges its own replacement. The candidate qualifies only after
  its executing-inode digest, all HTTP listeners, channel listener, fresh etcd
  health and authenticated Agent readiness agree.
- Abort and activation have one journal-CAS winner. Partial/expired recovery
  retains Task identity and admission until the predecessor is ready. Successful
  rollback records a failed update; lost successful ACK replay does not roll back
  a qualified candidate.
- A `stopping` journal phase fences the watchdog's pending stop against startup
  guard recovery. The guard can restore bytes but cannot qualify a process that
  may still be stopped. Obsolete watchdogs exit cleanly after a newer journal.
- Release-directory ancestors must have trusted ownership and no group/world
  write access; leaf-only privacy cannot protect against whole-directory swaps.

## Regressions and proof

The watchdog/guard race was reproduced as `stale rollback stopped the recovered
Controller`. The new stop claim and guard preservation fix it. The obsolete
watchdog test originally returned `state.conflict`, which would trigger endless
systemd retries; it now exits without unit effects. A group-writable parent of
a private release leaf was initially accepted and is now rejected.

Go1.26.7, `GOTOOLCHAIN=local`, repository-local `.tmp/production-mvp-20260910`
caches and temporary directories:

- Full race tests for `internal/common/controllerupgrade`,
  `internal/controller/controllerupgrade` and
  `internal/infra/controllerrelease`: pass.
- Focused real-listener tests in `internal/controller` and `internal/app`,
  including existing HTTP Serve and Agent-channel transport limits: pass.
- Vet for those packages and `internal/app`: pass.

Socket tests initially encountered sandbox `operation not permitted`; their
successful rerun used local listener permission. The restricted filesystem view
also remaps root ownership to65534; ownership tests were verified in the actual
filesystem namespace (root uid0). All test-created state remains repo-local.
The old Agent-channel helper's hard-coded `/tmp` directory was replaced with
the ignored repository `.tmp` location.

## Remaining integration

Compose the native coordinator/readiness callbacks, private release store,
guarded bootstrap/staging and Console/CLI/API surfaces. Verify A→B, bad binary,
active work, interrupted update and rollback on disposable QA under continuous
application HTTP/WebSocket traffic. No new Controller or Agent has been installed
by this increment. Full CI, runtime qualification and production approval remain
separate initiative gates.
