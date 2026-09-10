# Numeric Script hook ordering

Task4's first vertical increment implements`order`from0through65535, default0.
Within one selected Service and hook phase, numeric order precedes the ASCII
slug tie-break. It does not change Service topology, Group member order, phase
barriers or hook selection. Manual Scripts remain manual.

Blueprint parsing/reconciliation/export, the stored Script primary, frozen hook
render input, release selection/replay and Console/CLI/API all preserve order.
Metadata edits leave the immutable body generation unchanged. A PATCH containing
zero is distinct from omission; default-zero creation retains the prior mutation
intent identity. Release selection reads the existing fixed revision and frozen
replay sorts captured order rather than rereading an edited Script.

New owned Script definition/API/Console modules replace the corresponding root
helpers and card. Generated OpenAPI and both clients were regenerated, not edited.

## Evidence

All evidence below is under`.tmp/production-mvp-20260910/`.

- Failing-first parser, hook ordering, route and CLI checks reproduced missing
  behavior. YAML and HTTP reject negative/overflow, fractional, quoted and null
  values. HTTP's initial null cases reached the mutation service; closed JSON
  decoding now rejects those before mutation. Direct decoding also rejects
  unknown/duplicate members and multiple JSON documents.
- Full affected parser, desired-revision, Blueprint release and Script-definition
  packages pass with the race detector: `script-order-modules-race.log`.
- Focused Controller, etcd, app and CLI Script/ReleaseHook race tests pass:
  `script-order-surfaces-race.log`. The dedicated fixed-revision selection and
  Blueprint selection tests pass in`script-order-fixed-revision-race.log`.
- Dedicated API JSON tests pass with the race detector:
  `script-order-json-race.log`. Protected mutation-intent comparison proves order
  changes cannot replay a different saved mutation and explicit-zero edits are
  not omitted.
- Scoped vet passes in`script-order-vet.log`. OpenAPI/protobuf/client generation
  was run twice with matching generated output.
- All67Console tests and the final production build pass in
  `script-order-console-tests-final.log`and`script-order-console-build-final.log`.

An isolated browser served the production Console on127.0.0.1:5181. Its fixture
forwarded GET reads only to authorized QA; Script create/edit writes were held
in local memory and all other mutation methods were rejected. Browser checks
proved default0, create65535, retained edit value, keyboard save/reset to0 and
disabled Save with an accessible error for1.5/65536. At320,768,1024and1440pixels,
the dialog and Script row did not overflow horizontally. Narrow-screen visual
inspection and keyboard focus passed. There were no console warnings/errors;
the observed POST/PATCH requests targeted only the local fixture. The task-owned
tab and server were closed; the owner's QA tab was preserved.

## Remaining boundary

This is local implementation proof, not a live deployment or completion of task4.
Explicit image/user/resource context is not implemented by this increment.
Real first-start, exact-reapply, migration, failure, Abort and uncertain-outcome
qualification remain required after storage/source integrity is qualified.
The architecture gate still reports accumulated oversized/frozen-module and
test-placement failures in`script-order-architecture.log`; existing broader vet
failures remain recorded in the head checkpoint. Task6 must correct these, not
waive them. No full-CI or production-readiness claim is made.
