# Explicit Script machine-plan foundation — 2026-09-10

This is local task4 foundation proof, not stored/public context support or live
setup acceptance. ADR0076 and`SPEC-setup-scripts.md`remain authoritative. The
Controller does not yet emit explicit contexts from operator inputs.

## Implemented boundary

The immutable runner snapshot can capture a canonical explicit context, its
digest and positive Script primary revision, with separate consumer-Release and
runner image ids. Candidate validation still matches the real consumer image
in its selected Compose artifact. A different setup image does not bypass that
check; inherited runners retain their existing image equality requirement.

The shared Controller/Agent plan validator accepts only the full minimal explicit
projection: numeric user,`/`, fixed process, ownership labels, body and exact
grants. All ambient fields remain absent, including Service environment, networks,
additional groups and runtime selection. The snapshot may not carry inherited
network or secret-source tables. Selected Entries remain digest/reference bindings,
not plaintext pairs. Full projection equality also excludes future ambient fields.

Volume/Entry grants are bounded, unique and sorted by stable id before hashing.
Mounts must be exact managed Volumes with the declared target/read-only policy
and no other options except mandatory no-copy. Missing/extra grants, host binds,
subpaths, reserved paths and whole-component overlaps fail. Entry file targets
cannot overlap Volumes or one another. Disabling automatic image-to-empty-Volume
copy keeps initialization in the Script; see the
[Docker volume contract](https://docs.docker.com/engine/storage/volumes/#mounting-a-volume-over-existing-data).

## Evidence

All evidence is under`.tmp/production-mvp-20260910/`, using pinned Go1.26.7 and
repo-local temporary/cache paths with two-way compilation bounds.

- `script-context-image-red.log`: failing-first missing machine context types.
- `script-context-image-behavior-red.log`: after generated types, candidate
  setup image rejected and seven invalid authority/ambient cases accepted.
- `script-context-grants-red.log`: rehashed missing/extra/changed resource grants,
  unsafe Entry paths and mount options accepted before the exact-grant guard.
- `script-context-canonical-red.log`: malformed/unsorted/duplicated grants and
  first-excess32/64 bounds accepted before canonical context validation.
- `script-context-grants-green.log`: explicit image/context/grant checks pass.
- `script-context-managed-name-red.log`: the corrected fixture from the existing
  renderer exposed a shortened physical-name assumption. The guard now uses
  the existing`gp_vol_`plus full lower-case stable id, and rejects abbreviation.
- `script-context-plan-race.log`: complete shared execution-plan and policy
  packages pass`-race -count=1`, including inherited/candidate image regressions,
  inherited network/secret rejection and stale context-digest checks.
- `script-context-plan-vet.log`: shared-plan vet still reports the previously
  recorded protobuf copy-lock assignments in`plan.go:322`and
  `service_lifecycle.go:27`. No clean package-wide vet claim; task6 retains them.
- `script-context-consumer-race.log`: complete Blueprint-release, release-operation
  and Docker Script-runner packages pass race tests.
- `script-context-plan-final-race.log`: after the managed-name correction, all
  five packages above pass again, including the abbreviated-name rejection.
- `script-context-generated.log`: full`make generate`passes; OpenAPI and both
  public clients are unchanged. A subsequent`make proto`reproduces identical
  generated hashes (`script-context-proto-repro.log` records the generator).

The protobuf Go output was regenerated, not edited. No new user action, source
write, Agent deployment or live QA mutation was performed by this foundation.

## Required next proof

Wire actual fixed-revision Script metadata, eligible-resource preparation and
authenticated digest-to-local-image resolution before exposing authoring. Manual
and Blueprint publication must compare actual source context/revisions, keep
prepared resource memberships and reject rehashed source substitution or a CAS
race before Task effects. Context edits must not create a body generation or
rewrite any published snapshot. Complete Console/CLI/API/Blueprint/export parity
and live first-apply/reapply/Abort/failure/unknown-outcome acceptance remain.
Storage-integrity qualification still blocks live mutation QA.
