# Explicit Script resource selection — 2026-09-10

Local task4 increment. The actual explicit runner/image preparation and human
authoring surfaces remain pending; no live QA state changed.

## Implemented

Controller Entry preparation validates the entire explicit grant set before
resolving any value. Omitted grants select nothing. Volumes must appear exactly
once in the same Environment projection; they need not be mounted by the real
consumer Service. Entries must belong to that Environment and be exposed to the
associated Service. Missing/duplicate resources, invalid metadata, unsafe file
targets and overlapping Entry/Volume targets reject preparation before even the
first secret read. Selected file ownership and secret/plain modes are retained.
Temporary resolved plaintext is cleared after hashing, as before.

The storage source comparison now verifies the fixed projection scope/read
revision, Volume availability and exact Entry coverage, generation, kind,
destination, uid/gid, mode and secret classification. The comparison uses stored
metadata and does not decrypt values. The existing private artifact preparation
continues to own the plaintext digest; source-reference preparation retains the
stored value-generation digest/ciphertext authority.

Manual source memberships now retain only declared explicit Entry generations
and their selected Secret dependencies. They no longer require all exposed
Entries, and an absent declared grant fails even when the binding list is empty.
Inherited Entry selection and source membership retain their previous behavior.

## Evidence

Logs are under`.tmp/production-mvp-20260910/`with pinned Go1.26.7, bounded
compilation and repository-local temporary/cache paths.

- `script-context-entries-red.log`: empty explicit grants still read exposed
  secrets; invalid ownership/targets reached value resolution. A missing uid
  also reached the old unchecked dereference.
- `script-context-entries-green.log`: exact/empty grants, pre-read rejection,
  preserved file metadata, cleared plaintext and inherited selection pass.
- `script-context-entry-sources-red.log`: source preparation required unrelated
  exposed Entries and accepted a missing grant with an empty projection.
- `script-context-resource-sources-red.log`: source comparison accepted twelve
  resource/projection and Entry-metadata substitutions before the new guard.
- `script-context-resources-green.log`: focused Controller/storage resource,
  context and maximum Blueprint publication tests pass.
- `script-context-resources-race.log`: Controller and etcd`Script`-named tests,
  maximum final publication shapes and injected combined-maximum failure tests
  pass with`-race -count=1`.
- `script-context-entries-vet.log`: Controller vet passes.
- `script-context-resources-vet.log`: combined vet still reports only the three
  previously recorded etcd protobuf copy-lock warnings at
  `script_source_reference_codec.go:153/218/237`.

Earlier broad Blueprint recovery/fixture failures, architecture/full CI,
native-trial mutation isolation and live storage-integrity qualification remain
required. Next: authenticated explicit image preparation and a minimal actual
Controller runner projection, then Blueprint/API/CLI/Console context authoring.
