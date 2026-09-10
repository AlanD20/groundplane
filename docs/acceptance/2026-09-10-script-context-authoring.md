# Explicit Script context: Blueprint authoring

Local task4 increment, following context preparation and the native-trial write
guard. No Controller/Agent binary was deployed and no live QA mutation occurred.
The recorded storage-integrity qualification still blocks live mutation QA.

## Behavior

- Blueprint `execution` is a closed inherited/explicit desired choice. Explicit
  grants use immutable Compose Volume and `x-gp-entry` keys; no generated source
  revision, host-local image id, secret value or runner receipt enters authoring.
- The YAML boundary retains scalar types and presence, including explicit
  `read_only: false`. Null, strings used as booleans, unknown fields, mixed modes,
  missing images/users and invalid grants fail. Aliases and merge precedence are
  resolved without allowing null/coercion to erase decisions.
- Script reconciliation receives the intended allocated Volume identities and
  reconciled Entry metadata. Existing Entry reconciliation now precedes Script
  reconciliation in the app workflow. No live resource lookup is added to the
  pure compiler. Missing/foreign/unexposed Entry grants, unknown/duplicate Volume
  keys and Volume/file overlaps fail before the Script set is prepared.
- Context-only replacement does not allocate a body generation. A replacement
  Entry under the same authored key selects its new candidate id. An inherited
  reset removes the complete explicit authority; omitted Scripts still retain
  their current records under the existing non-destructive apply contract.
- Current Blueprint-owned Script export maps stable grant ids back to immutable
  keys, preserving image/user/target/read-only decisions. A missing key or an
  API-owned Entry that has no Blueprint key fails export rather than silently
  dropping authority or inventing an adoption. Existing Script edit repairs it.
- The existing Entry projection and authoring functions, plus their key-retention
  test, moved from app to the Entry capability module without behavior changes.
  Script grammar moved out of the oversized parser and core envelope files.
  Both touched oversized production app files shrink.

## Proof and bounds

Repository-pinned Go1.26.7, race detector, `GOMAXPROCS=2`, `-p=2`, `-count=1`,
and repository-local cache/temporary paths were used. Logs live under
`.tmp/production-mvp-20260910/`:

- `script-context-authoring-red.log`: the valid explicit/inherited authored
  contexts fail the old closed parser.
- `script-context-resolution-red.log` and `script-context-export-red.log`:
  candidate-resource and export inputs are absent before their implementations.
- `script-context-alias-red.log`: a valid merged explicit context fails before
  the typed presence/merge correction. Its green check also rejects merged null.
- `script-context-authoring-full-race.log`: full core, Blueprint parser, desired
  revision and Script definition packages pass at the first integration checkpoint.
- `script-context-export-get-race.log`: actual app `GetBlueprint`, real parser
  and real Script reconciler round-trip an edited explicit context, without
  new identity/body allocation or loss of the read-write grant. This uses a
  bounded metadata repository seam, not a live Controller or Docker runtime.
- `script-context-authoring-final-race.log` and
  `script-context-authoring-final-app-race.log`: full owned pure/capability
  packages and affected app Script/Blueprint suites pass on the final increment.
- `script-context-authoring-vet.log`: scoped vet passes for core, parser, desired
  revision, Script definition, Entry and app.

Existing generic Blueprint Console/CLI/API actions carry this YAML grammar;
there is no new action, route or public JSON DTO in this increment. Generated
clients are therefore not manually edited. Script-specific create/edit/read
context fields and controls remain the next vertical increment.

This is not complete task4 acceptance. Initial explicit setup with real Volume/
Entry grants must still prove the candidate-consumer start barrier, exact
reapply, failure, Abort and unknown/retry handling. The independent source and
minimal-runner proofs remain in the named preceding reports. Deferred live QA,
the native trial/write check, known broad Script/Blueprint recovery failures
and required final CI remain open; none are relabelled passing here.
