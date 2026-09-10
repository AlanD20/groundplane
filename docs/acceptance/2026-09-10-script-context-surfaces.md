# Script execution context surfaces — 2026-09-10

Task4 local implementation checkpoint, based on main `2ec08a914`. This is not
live execution qualification or production readiness. QA mutation remains paused
behind the recorded storage-integrity incident.

## Delivered contract

Existing Script create/edit/read now carries the closed inherited/explicit
execution choice in API, generated clients, CLI and Console. No new capability,
endpoint, Task or runtime override was added. Context replacement preserves the
existing Script/Service association and body-only generation ownership.

- JSON and OpenAPI reject null/mixed contexts, unknown or duplicate fields,
  missing explicit image/user/read-only decisions and oversized grant lists.
  Optional grant lists may be omitted but not null; inherited mode has only mode.
- Script-owned validation and protected mutation intent bind image, numeric
  user, exact Volume targets/access, Entry ids and inherited reset. PATCH
  omission preserves context; supplied lists never merge. Create omission retains
  its prior protected intent semantics.
- CLI `--execution-file FILE` accepts one closed YAML mapping of at most
  65,536 bytes, with `-` for stdin. Scoped Volume slugs and immutable Entry keys
  resolve through paginated metadata reads. `--id` avoids label lookups and
  supports API-owned Entries. `--inherit-execution` is mutually exclusive with
  the file and supports a bodyless context-only PATCH.
- Entry list/show metadata exposes its existing immutable Blueprint
  reconciliation key, including file Entries. No value access or writable key
  was added. The existing Entry projection moved to its capability module;
  oversized API/Controller/store files shrink.
- Console forms expose the exact pinned image, numeric user and resource grants,
  retaining unavailable saved grants for explicit repair. Entry options are
  limited to current Service exposure and never reveal values. The list shows
  effective mode and numeric order. Create/edit error/loading states preserve
  input; bodyless Run, Abort and Task behavior are unchanged.

The authoritative MVP prose is aligned with accepted ADR0076: separate explicit
setup authority, numeric intra-Service order, exact resource boundaries and
unchanged consumer Release identity. Older inherited-only paragraphs were not a
second supported contract.

## Executable proof

All logs are under `.tmp/production-mvp-20260910/`. Go used pinned 1.26.7,
`GOMAXPROCS=2`, repo-local temporary/cache directories, `-p 2 -race -count=1`.

- `script-context-json-red.log` and `script-context-api-definition-red.log`
  reproduce rejected valid context/lost desired context before implementation.
  `script-context-api-intent-red.log` proves changed execution choices previously
  matched the same protected acceptance. `script-context-api-module-green.log`
  passes full public API and Script-definition packages. Final formatting-only
  verification passes in `script-context-definition-final-race.log`.
- `script-context-api-schema-red.log` reproduces the absent closed union.
  Initial HTTP proof caught stale Huma schema validation caches: inherited mode
  rejected while oversized grants were admitted. Calling the pinned framework's
  schema precomputation after exact schema mutation fixes both. Actual HTTP
  boundary and schema/client shape pass in `script-context-api-routes-corrected.log`
  and `script-context-api-schema-final.log`; the earlier failing log is retained.
- `script-context-entry-key-red.log` reproduces omitted reconciliation metadata;
  `script-context-entry-key-green.log` passes the Entry read and owned-module
  checks. Console metadata preserves key, source reference, exposure and zero
  uid/gid (`script-context-entry-console-green.log`).
- `script-context-cli-file-red.log` and `script-context-cli-command-red.log`
  capture missing file/command support. `script-context-cli-green.log` passes
  Script CLI/client checks. `script-context-client-red.log` uses a build overlay
  of the prior client to prove dropped request/response context and missing
  explicit-access validation; `script-context-client-green.log` passes.
- Added ID-only/no-lookup and absent/ambiguous-key checks pass with all selected
  Script CLI tests in `script-context-cli-final-corrected-race.log`. The preceding
  final-race log records a test expectation's JSON member-order mismatch;
  only that expectation changed. Missing/ambiguous keys reject before mutation.
- `script-context-surfaces-race.log` passes Script/Entry-selected API,
  Script-definition, Entry, Controller and app checks. Full API-client race
  tests pass in `script-context-cli-full-race.log`. Affected API, Controller,
  Entry, Script-definition and CLI vet pass (`script-context-surfaces-vet.log`
  and `script-context-cli-final-vet.log`).
- `script-context-generate.log` and `script-context-entry-key-generate.log`
  record regenerated OpenAPI and Go/TypeScript clients; protobuf is unchanged.
  Console request/projection/validation checks and all 28 test-file suites pass
  in `script-context-console-full-tests.log`; TypeScript and production Vite
  build pass in `script-context-console-build.log`.

## Isolated browser proof

The built Console used `script-context-ui-server.mjs` on loopback port5182 in
an isolated browser context. That fixture has no outbound proxy and only mutates
in-memory Script fixtures; every other mutation returns405. No QA/runtime,
production, provider or owner-tab mutation occurred.

Actual browser requests proved:

1. POST201 creates a pre-deploy hook at order10 with pinned execution image,
   `0:0`, one stable Volume grant at `/etc/tls` with `read_only:false`, and one
   secret-classified file Entry id. Reopen preserves all choices.
2. Keyboard-submitted PATCH200 resets to `{mode:"inherited"}` without lingering
   image/user/grants; body generation remains1.
3. Saved unavailable Volume/Entry grants remain visible and removable. A named
   user is rejected locally with disabled Save. Removing both grants submits
   explicit empty lists, not inherited access.
4. A deliberate409 keeps the draft and an accessible error. Resubmission200,
   reload and reopen preserve zero grants and generation1. The only error during
   the injected failure is its expected HTTP409; after reload there are no
   console errors or warnings.
5. Drawer horizontal client/scroll widths match at320,768,1024and1440pixels.
   Screenshots were visually inspected at320and1440. Tab/Shift-Tab reaches Save,
   focus remains in the modal, and Escape returns focus to the opener. Final
   read-only DOM/timing evidence is `script-context-browser-final.txt`; the local
   cached navigation loaded in58.4ms, not a production performance claim.

The task-owned tab was closed, preserving the existing QA tab. Browser screenshot
file export was unavailable through the tool's workspace boundary; images were
inspected inline, not represented as saved local artifacts.

## Remaining qualification

The broad CLI run fails in two untouched paths:
`TestEnvironmentApplySendsMultipartSingletonReplacement` uses the absent
`environment apply` command; `TestComponentActionsAndConfigUseStableID/config_set`
expects a direct PUT instead of the current metadata read and complete CoreDNS
choice. These failures are retained for mandatory task6 correction, not waived;
no full CLI or full-CI green is claimed. Existing architecture, copy-lock and
broader Script/Blueprint recovery failures retain their prior evidence.

Next: initial explicit setup before consumer start, exact reapply, ordered
migrations, failure/Abort and unknown-outcome proof; then remaining hosting and
recovery qualification. Live storage/source integrity, native guarded-write
qualification and task4 live execution remain deferred. Surface proof alone
does not qualify runtime safety or finish task4.
