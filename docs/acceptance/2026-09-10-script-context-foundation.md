# Explicit Script context foundation

The pure desired model now validates the closed inherited/explicit choice,
repository SHA-256 image authority, canonical32-bit numeric uid/gid, stable
Volume/Entry references, duplicate grants, overlapping targets and the32/64grant
limits. The shared policy owns numeric parsing, component-aware path overlap
and the exact mount restrictions documented in`docs/blueprint.md`.

Tests were added before the model existed. Full`internal/core`and shared
`internal/common/scriptpolicy`race suites and scoped vet now pass. Boundary
proof includes accepted maximum counts/ids, rejected one-over counts, mutable/
tag-plus-digest/local/uppercase images, noncanonical users, invalid text and
reserved path ancestry. Legitimate`/etc/tls`,`/var/lib/app`and prefix siblings
remain accepted. Logs are under`.tmp/production-mvp-20260910/`:
`script-context-core-red.log`,`script-context-core-green.log`,
`script-context-core-race.log`and`script-context-core-vet.log`.

This is an internal foundation, not delivered explicit execution. No stored
Script, API/CLI/Console input or runner path uses the new context yet. Boundary
decoders must still enforce field presence, including explicit`read_only:false`,
and reject extra members in inherited mode. Fixed-source preparation must prove
same-Environment ownership, Service Entry exposure, file-target isolation and
image availability. Separate Release-image authority, immutable capture and
publication fences remain the next implementation increment. No live mutation
or full-CI/production claim is made.
