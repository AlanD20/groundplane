# Explicit Script preparation and projection

Task4 local checkpoint; not operator-surface or live qualification.

## Implemented boundary

`ScriptRunnerPreparationService` owns resource eligibility, the authenticated
Agent image-lookup port and private Entry hashing. Its opaque result binds the
Script identity/revision, fixed capture, desired projection and exact Entry
metadata. Changed contexts, source captures or Entry generations cannot reuse
it. Explicit sources require a coherent fixed read; malformed resources are
rejected before image lookup, and unavailable/malformed image responses before
Entry value reads. Inherited preparation performs no new image lookup.

The manual, normal Release hook and same-Blueprint hook producers now consume
that result instead of caller-supplied bindings. Explicit runners start from a
fresh minimal Service configuration: independent local image, numeric user,
working directory `/`, exact managed mounts with `volume-nocopy`, and selected
Entries. Consumer environment, network, user, runtime and host mounts do not
enter that branch. The snapshot still binds the real consumer Release image
separately. Frozen hook replay retains both identities without re-resolution.
The inherited projection policy moved intact into the context-owned module;
the oversized common projection file shrank from644to521lines. The app changes
only replace the prepared input and wire the Script-owned preparation service.

## Local evidence

All evidence is in ignored `.tmp/production-mvp-20260910/`, using pinned
Go1.26.7, bounded compilation and repository-local temporary/cache paths.

- `script-context-preparation-red.log`: preparation tests fail before its API
  exists. `script-context-preparation-green.log`: closed image responses,
  no-secret-read failures, inherited behavior and changed-source rejection pass.
- `script-context-projection-red.log`: the context producer tests fail before
  the prepared input/projector exists. `script-context-projection-green.log`:
  minimal projection and existing/staged exact Volume authority pass under race.
- `script-context-preparation-wiring-race.log`: affected Script tests pass in
  Controller, app, Blueprint Release and etcd; the selected releaseoperation
  package has no Script-named tests, so its full suite was run separately.
- `script-context-producer-race.log`: the real Controller builder emits a
  machine-valid explicit plan, actual stored-source publication accepts it,
  frozen hook replay retains it, and all inherited serving-Release manual
  journeys pass, including plain/secret Entry assignment and retention.
- `script-context-release-callers-race.log`: full releaseoperation and
  blueprintrelease race suites pass. `script-context-preparation-vet.log`:
  Controller, both Release services and app vet pass. `git diff --check` passes.

The image-channel result is supplied hermetic evidence, not a live Docker
lookup. The manual publication fixture seeds Release history; the inherited
journeys separately exercise actual serving-source discovery. The frozen hook
test proves reconstruction, not a full candidate consumer runtime journey.

## Remaining qualification

Explicit context authoring/export and Console/CLI/API parity remain pending.
Actual explicit first apply, exact reapply, setup/migration failure, Abort,
unknown outcome and retry qualification remain required. Live mutations stay
paused behind the recorded storage-integrity incident. Audit native-trial write
admission before introducing live schema writes: strict predecessor decoding
does not become compatible merely because fields are additive. The previously
recorded broader Blueprint recovery failures, etcd copy-lock warnings and full
architecture/CI corrections remain task6 work; this checkpoint does not waive
them or claim complete task4/production readiness.
