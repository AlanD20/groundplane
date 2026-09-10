# Explicit Script metadata/source comparison — 2026-09-10

Local task4 checkpoint following the machine-plan foundation. Operator context
authoring and actual explicit runner preparation remain unavailable; this is not
task4 completion or live setup qualification.

## Implemented

`core.Script`and its private stored metadata now retain optional execution
context. Core/write-boundary validation rejects incomplete context. Explicit
grants, inherited reset and omission round-trip without allocating a new body
generation or copying the body into mutable metadata.

The publication boundary compares the captured canonical context to the selected
fixed-read Script record: mode, image, numeric user, exact grants, primary revision
and real consumer Release image. It also verifies the context digest. Canonical
comparison does not reorder authored lists. A valid rehashed machine plan cannot
invent explicit authority when the real stored Script is inherited.

This comparison runs before manual source preparation, in ordinary Release hook
publication, before Blueprint hook staging and when deriving Blueprint source
memberships. The manual final-primary comparison permits reference-count-only
updates but rejects a concurrent context edit. The context extension has not
changed publication/Abort/cleanup ownership or prepared-reference protocols.

## Evidence

Files below are in`.tmp/production-mvp-20260910/`. Go1.26.7, two-way compilation
bounds and repository-local temporary/cache paths were used.

- `script-context-record-red.log`: missing desired metadata field, before wiring.
- `script-context-record-green.log`: context round-trip, inherited reset,
  malformed metadata and existing Script execution checks pass.
- `script-context-source-red.log`and`script-context-source-green.log`: failing-first
  source comparison followed by passing changed revision/image/user/grant cases.
- `script-context-publication-behavior-red.log`: a real Controller-produced
  inherited plan, changed into a fully valid/rehashed explicit machine plan,
  was incorrectly accepted by the actual manual publication repository.
- `script-context-publication-fixture-green.log`: the source guard rejects that
  plan before any durable write; affected Script and Blueprint-hook tests pass.
  Three publication fixtures now use real minimal protobuf snapshots instead of
  checkpoint-only invalid byte sentinels. This is fixture correction, not a
  relaxation of the new decoder or evidence of full runtime plans in those tests.
- `script-context-sources-race.log`: affected core and etcd Script/Blueprint-hook
  selections pass`-race -count=1`, including exact manual publication/no-write
  rejection, body-generation stability and reference-count/context-edit fences.
- `script-context-blueprint-race.log`: invented/missing context rejects before
  the real Blueprint preparation method writes its durable hook stage.
- `script-context-sources-vet.log`: the three previously recorded protobuf
  copy-lock warnings at`script_source_reference_codec.go:153/218/237`remain.
  No clean etcd-wide vet or full-CI claim is made.

The private metadata field is not in the API input or authored`ScriptSpec`yet.
No live Script context or QA state was created or changed.

## Next boundaries

Prove the final same-Blueprint primary metadata/revision fence after reference
preparation, including bounded publication at maximum hook count. Then prepare
only eligible explicit resources and authenticated digest-to-local-image seals,
build the actual minimal Controller projection, and prove valid manual and
candidate publication/replay. The Controller must not silently inherit for an
explicit Script; public authoring stays unavailable until this path is complete.

Before enabling new stored fields in a live native release, audit trial-phase
mutation isolation: the predecessor's strict durable decoder cannot read an
unknown metadata field. Additive Go/protobuf shape alone is not rollback proof.
This is a required compatibility check, not a diagnosed live failure. Existing
architecture/vet gates and storage-integrity qualification remain mandatory.
