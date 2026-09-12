# Script execution evidence

This document consolidates the 2026-09-10 local evidence for numeric hook order,
explicit execution contexts, source authority, operator surfaces and runtime
composition. Requirements live in [Script execution](../features/setup-scripts.md).
The earlier manual lifecycle evidence is in [hosting-floor.md](hosting-floor.md).
No evidence below claims live explicit-context qualification or production
readiness.

## Scope and test environment

Local work used pinned Go 1.26.7, `GOMAXPROCS=2`, `-p 2`, `-race -count=1`,
repository-local caches/temporary paths and module-pinned formatting. Console
checks used exact Node 24.19.0/npm 11.17.0. Logs and read-only overlays remain
under `.tmp/production-mvp-20260910/`.

Live mutations remained paused behind the storage/source-integrity incident.
No Controller/Agent deployment, QA mutation, provider/network change or
production action was performed by these local increments.

## Numeric hook order

`order` is an integer from 0 through 65,535, default 0. Within one Service and
hook phase, order precedes ASCII slug. It does not change Service topology,
Release Group member order, phase barriers, hook selection or manual Scripts.
Blueprint, persistence, frozen replay and API/CLI/Console preserve the value;
metadata edits do not allocate a new body generation.

Failing-first parser, release, route and CLI checks established missing order
and invalid null/quoted/fractional/overflow handling. The following recorded
results passed:

- full parser, desired-revision, Blueprint release and Script-definition race
  suites: `script-order-modules-race.log`;
- focused Controller, etcd, app and CLI Script/hook selections:
  `script-order-surfaces-race.log`;
- fixed-revision selection: `script-order-fixed-revision-race.log`;
- API JSON and protected-intent checks: `script-order-json-race.log`;
- scoped vet: `script-order-vet.log`;
- repeated OpenAPI/protobuf/client generation with identical output; and
- 67 Console tests and build:
  `script-order-console-tests-final.log` and
  `script-order-console-build-final.log`.

An isolated Console fixture on `127.0.0.1:5181` kept Script writes in memory,
blocked other mutations and used QA only for authorized reads. It proved default
0, create 65,535, edit/reset, invalid 1.5/65,536, keyboard/focus and
320/768/1024/1440px layout. No live Script changed. Architecture remained red
in `script-order-architecture.log`.

## Explicit model and machine plan

The desired model validates a closed inherited/explicit choice, SHA-256 image
authority, canonical 32-bit uid/gid, stable Volume/Entry references, duplicate
and overlapping grants, and 32 Volume/64 Entry grant limits. Exact path policy
allows legitimate `/etc/tls`, `/var/lib/app` and prefix siblings while rejecting
reserved/overlapping ancestry.

The immutable runner snapshot binds Script primary revision, context digest,
consumer Release image and independent runner image. Explicit projections contain
only numeric user, `/`, fixed process, ownership labels, body and declared
grants. They exclude Service environment, networks, additional groups, runtime
selection and inherited source tables. Managed Volume mounts use the complete
`gp_vol_<lower-case-stable-id>` identity, exact target/access and `volume-nocopy`.

Full `internal/core`, `internal/common/scriptpolicy` and shared execution-plan
race suites passed. Evidence includes:

- `script-context-core-{red,green,race}.log` and
  `script-context-core-vet.log`;
- `script-context-image-{red,behavior-red}.log`;
- `script-context-grants-{red,green}.log`;
- `script-context-canonical-red.log`;
- `script-context-managed-name-red.log`;
- `script-context-plan-race.log` and
  `script-context-plan-final-race.log`;
- `script-context-consumer-race.log`; and
- `script-context-generated.log` plus
  `script-context-proto-repro.log`.

The complete shared-plan vet was not green: protobuf copy-lock assignments at
`plan.go:322` and `service_lifecycle.go:27` remained. Generation reproduced
identical protobuf and unchanged public clients.

## Metadata, source and final-publication fences

Stored Script metadata retains optional context without allocating a body
generation on context edit/reset. Publication compares the fixed-read Script
mode, image, numeric user, exact grants, primary revision, consumer Release image
and context digest before manual preparation, Release hooks and Blueprint
staging. A valid rehashed plan cannot invent explicit authority for an inherited
Script.

After source preparation, manual and Blueprint hooks share a final Script-primary
fence. Reference-count-only bookkeeping may advance, but context, identity, body
generation and Script-set generation must match. Edits before the final read
reject; edits after it lose CAS without publishing Task, queue, Release marker,
source root or Environment head. Duplicate references share one comparison.

The maximum fixture contained 32 Releases, 16 explicit hooks, 17 physical
sources, candidate Attaches and Backup: 192 comparisons, 176 success mutations
and 192 failure reads, within the unchanged 256-per-arm Blueprint limit. Ordinary
transactions remain capped at 96.

Evidence:

- `script-context-record-{red,green}.log`;
- `script-context-source-{red,green}.log`;
- `script-context-publication-behavior-red.log` and
  `script-context-publication-fixture-green.log`;
- `script-context-sources-race.log` and
  `script-context-blueprint-race.log`;
- `script-context-blueprint-primary-{red,green}.log`;
- `script-context-blueprint-final-behavior-red.log`,
  `script-context-blueprint-final-green.log` and
  `script-context-blueprint-focused-race.log`.

Etcd vet retained copy-lock warnings at
`script_source_reference_codec.go:153`, `:218` and `:237`.

## Resource selection and preparation

Explicit Entry preparation validates the full grant set before reading any
value. Volumes must occur exactly once in the Environment projection; Entries
must belong to that Environment and be exposed to the consumer. Omitted grants
select nothing. Missing/duplicate resources, unsafe targets and overlaps reject
before secret access. Temporary plaintext is cleared after hashing.

Manual source membership retains only declared explicit Entry generations and
their selected Secret dependencies. Source comparison checks fixed projection,
read revision, Volume availability and every Entry identity/generation/kind/
destination/uid/gid/mode/secret decision without decrypting values.

`ScriptRunnerPreparationService` owns resource eligibility, authenticated Agent
image lookup and private Entry hashing. Its opaque result binds Script/revision,
fixed capture, desired projection and Entry metadata. Explicit runner projection
starts from a fresh minimal Service; inherited projection remains unchanged.

Recorded passing evidence includes:

- `script-context-entries-{red,green}.log`;
- `script-context-entry-sources-red.log`;
- `script-context-resource-sources-red.log`;
- `script-context-resources-{green,race}.log`;
- `script-context-preparation-{red,green}.log`;
- `script-context-projection-{red,green}.log`;
- `script-context-preparation-wiring-race.log`;
- `script-context-producer-race.log`;
- `script-context-release-callers-race.log`; and
- `script-context-preparation-vet.log`.

The image result was hermetic evidence, not a live Docker lookup. Fixture release
history and in-memory seams do not replace live first-Apply qualification.

## Blueprint authoring and operator surfaces

Blueprint `execution` authors immutable Compose Volume and `x-gp-entry` keys,
not stable ids, secrets, host image ids or receipts. Exact scalar presence is
preserved, including `read_only: false`; null/coerced/mixed/unknown choices fail.
Aliases and merge precedence cannot erase decisions. Existing Entry
reconciliation precedes Script reconciliation. Export maps stable grants back to
immutable keys and fails rather than dropping an API-owned or missing key.

Script create/edit/read carries the same closed choice through API, generated
clients, CLI and Console. CLI `--execution-file FILE` accepts at most 65,536
bytes, with `-` for stdin; `--inherit-execution` is exclusive and supports
bodyless reset. `--id` avoids label resolution. Console retains unavailable
saved grants for repair and never reveals Entry values.

Full owned package/app authoring race/vet passed in
`script-context-authoring-full-race.log`,
`script-context-export-get-race.log`,
`script-context-authoring-final-race.log`,
`script-context-authoring-final-app-race.log` and
`script-context-authoring-vet.log`.

Surface evidence includes:

- JSON/schema/HTTP failing-first and corrected logs
  `script-context-json-red.log`, `script-context-api-definition-red.log`,
  `script-context-api-intent-red.log`, `script-context-api-schema-red.log`,
  `script-context-api-routes-corrected.log` and
  `script-context-api-schema-final.log`;
- Entry key/projection checks and generated clients;
- CLI file/command/client failing-first and passing selections;
- `script-context-surfaces-race.log`, `script-context-cli-full-race.log`,
  scoped vet, generation, all 28 Console test-file suites and production build.

The first CLI final-race run failed only on JSON member order; the corrected
expectation passed. Two untouched broad CLI failures remain:
`TestEnvironmentApplySendsMultipartSingletonReplacement` expects the removed
`environment apply`, and
`TestComponentActionsAndConfigUseStableID/config_set` expects PUT without the
current config-read/full-Corefile flow.

An isolated loopback fixture on port 5182 kept mutations in memory and returned
405 for unrelated writes. It proved explicit create, inherited reset, unavailable
grant repair, injected 409 recovery, generation stability, keyboard/focus and
320/768/1024/1440px layout. `script-context-browser-final.txt` records the
58.4ms cached navigation; this is not production performance or live QA proof.

## Networkless runner and full composition

Source `ac1937643` exposed a zero-network panic from unconditional
`networks[1:]`. The runner now skips the primary only when present and never
invents a network. `script-explicit-network-red.log` and
`script-explicit-runner-red.log` reproduce the panic; the latter used a
read-only predecessor overlay and in-memory Docker client.

`script-explicit-runner-race.log` passes create, start, output drain, wait and
owned cleanup without network connect. Options prove pinned image, `0:0`, `/`,
no ports/endpoints/restart/log storage and exact writable Volume with image copy
disabled. `script-explicit-agent-barrier-baseline.log` and
`script-explicit-runner-vet.log` passed.

The final two-Service Controller plan preserves Network/Volume preparation,
both pre-hooks, then either consumer Apply. Agent runtime with a fake engine
proves exact grants, checkpoint/cleanup barrier, failure/Abort stop-before-Apply,
and lost acknowledgement recovery without duplicate create/start. Mutation-test
overlays intentionally removing Entry filtering or cleanup failed as expected.

| Check | Result | Log |
| --- | --- | --- |
| Agent explicit/Docker runtime race selection | Pass | `script-explicit-agent-final-race.log` |
| Controller Script/Blueprint preparation race selection | Pass | `script-explicit-controller-final-race.log` |
| Agent/Controller vet | Pass | `script-explicit-composition-vet.log` |
| Unfiltered Entry mutation | Expected failure | `script-explicit-filter-mutation.log` |
| Missing cleanup mutation | Expected failure | `script-explicit-cleanup-mutation.log` |

The engine and acknowledgement persistence were in-memory doubles. This does
not prove real Docker mount/network enforcement or source-to-host behavior.

## Preserved broad failures and remaining qualification

The broader Script/Blueprint race run retained five pre-existing failure groups:

- `TestBlueprintRequirementGateClaimEpochAllowsOnlyExactPrerequisiteAcknowledgements`;
- `TestBlueprintRequirementGateNonSuccessPrerequisiteNeverRefreshesEpoch`;
- `TestBlueprintCompletedHooksReleaseSourceFenceBeforeNextPublication`;
- `TestBlueprintMixedProducerRecoversServingAndFirstCandidate`; and
- `TestBlueprintHookRecoveryTerminalReport`.

They reproduce against previous changed-file versions through a read-only Go
overlay. Evidence is `script-context-blueprint-final-race.log`,
`script-context-blueprint-baseline-race.log` and
`script-primary-baseline/overlay.json`. Corrupt Release fixtures, incomplete
normalized Compose coverage and missing Agent predecessor authority remain
mandatory corrections; no broad-green or full-CI claim exists.

Still required after storage integrity is qualified:

- real first Apply and exact reapply;
- certificate setup/renewal and ordered migrations;
- failure, Abort, reconnect, lost acknowledgement, unknown outcome and Retry;
- real Docker resource isolation and cleanup before consumer start;
- guarded native trial/write qualification;
- the broad CLI, Script/Blueprint, architecture, Staticcheck and copy-lock
  corrections; and
- final CI and production qualification.
