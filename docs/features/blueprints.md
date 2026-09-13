# Blueprint authoring and application

## Purpose and scope

Let operators inspect, validate and apply reproducible Environment desired state.
A Blueprint contains decisions, not observations, generated paths, credentials or
runtime artifacts. [blueprint.md](../blueprint.md) owns the complete grammar and
execution semantics; this document routes authoring and publication design.

## Functional requirements

- The revisioned authoring singleton returns canonical single-file YAML, the
  selected desired revision and `ETag`. Revision `0` means no desired head.
  Validate and Apply require one exact quoted `If-Match`; stale input fails
  without writes. [api-cli.md](../api-cli.md) owns the exact endpoints and commands.
- Reconstruct authoring from normalized Compose and typed desired records, not
  uploaded audit layout, rendered artifacts or partially loaded Console state.
  Comments, anchors, aliases and file boundaries are not preserved. Script bodies
  use literal YAML block scalars. Import still accepts a closed multi-file bundle.
- Validate parses the same bundle as Apply and returns a non-destructive
  `create | update | retain` diff without publishing any Task, revision or effect.
  Apply accepts the latest desired input and publishes one reconcile Task.
  [Latest-wins reconciliation](../decisions/0078-latest-wins-blueprint-reconciliation.md)
  selects resource-level changes against verified applied inputs, supersedes
  obsolete work, and seals each private execution unit after its safe handoff.
- Omitted existing resources are retained. Omission never deletes or renames;
  ambiguous preservation fails closed. Only a resource's explicit protected
  Remove action may remove it. The earlier Entry-omission deletion proposal is
  superseded by this product-wide non-destructive rule.
- The Console keeps drafts locally and supports view, edit, single-file or
  closed-bundle import, export, validate and apply. The CLI uses
  `environment blueprint show|validate|apply`, not a second `environment apply`.

### Entry identity and values

An authored `x-gp-entry` key persists as `BlueprintKey`, separate from its env
key or file path. New keys allocate stable Entry ids; existing source/exposure
changes retain identity and choose exact immutable value generations. Destination,
kind, numeric ownership and secret-storage changes are not in-place identity
edits. Direct and Component-owned Entries have no Blueprint key and are preserved.

Values and Entry-to-Environment routing may be prepared before publication, but
remain inert until the sealed projection becomes the desired head. Task execution
uses its captured generation, never a later source resolution. Reads use the
current or cursor-pinned projection; a routing index is not a second desired
authority. Explicit removal emits the required file/generated-env cleanup;
omission alone does not authorize it.

## Non-functional requirements

Input, staged records, rendered artifacts and complete transactions are bounded.
Do not raise limits or truncate restoration evidence to make a large bundle fit.
Validate source revisions, ownership and immutable digests before visibility or
execution. Failed publication leaves no partial public revision. Retries and
queued Tasks resolve their own sealed revision, not the latest desired head.

## Technical design

[Latest-wins reconciliation](../decisions/0078-latest-wins-blueprint-reconciliation.md)
owns the replacement for prebuilt whole-Environment execution and aggregate
applied-state promotion. The implementation below predates that change; it must
not be used to justify rejecting newer valid input or running obsolete units.
Keep its safety fences until their resource-level replacements are connected.

[Closed bundles](../decisions/0012-closed-blueprint-bundles.md) owns external input
closure. [Staged publication](../decisions/0051-blueprint-staged-revision-publication.md)
owns chunking, sealing, final publication, retries, cleanup and record/transaction
bounds. [Direct desired mutations](../decisions/0058-revision-authoritative-direct-desired-mutations.md)
uses that same revision authority; audit input is not execution input.

### Bounded native predecessor references

Preparation captures and validates each current and optional retained native
Release at one fixed revision. Exact native artifact bytes go into the candidate's
existing `PriorRuntime` field before staging. Complete historical inputs remain
transient validation evidence, not repeated aggregate-marker payloads. Historical
Release records are not rewritten.

These captured native records select each Service's restoration target and supply
the exact artifact used by both recovery and independent observation. The applied
Environment artifact remains independently fenced metadata, not a fallback
runtime source; see [per-Service restoration](services-and-releases.md#per-service-restoration).

The marker keeps sorted Service/read/projection identities, compact serving,
target and retained-Release summaries, and the canonical prior-runtime digest.
The candidate manifest owns the render input and its lifetime; references never
depend on a separately prunable predecessor input. The executed Environment
artifact remains marker-owned. No new blob namespace, collector or higher limit
is introduced.

Publication compares original Release sources/projections and exact staged
candidate intent/input. Claim and recovery validate manifest and witness digests,
scope, lineage, ownership and the full restoration pair at one revision, then
compare those sources on mutation. Reconstruction and Agent admission retain
exact artifact bytes. Missing, foreign or substituted evidence fails closed.
Per-input, aggregate-marker, assignment, native aggregate, applied-artifact and
complete transaction bounds all remain enforced.

The inspected QA installation had no inline serving witnesses; its existing
absent-serving shape needed no migration. This is not a universal historical
store claim. Old inline-serving history requires an explicit bounded offline
disposition, never inferred witnesses or an ad-hoc compatibility reader.

### Atomic terminal publication

The existing aggregate implementation uses the envelope below. ADR 0078 replaces
its all-members promotion rule with per-unit applied results; exact authority,
source-release safety and physical transaction limits still apply. Until that
integration is complete, retain the existing aggregate writer and terminal guards.

One atomic terminal commit currently owns the original Task state, assignment/lifecycle
removal, idempotency/retention, applied artifact, every candidate's serving and
current-successful projection, terminal evidence, Environment fences and final
Script-source fragment. The existing implementation cannot promote members
separately. Never infer completion from healthy containers.

Only the owning Task repository constructs the closed terminal envelope after
validating complete authority. Its store capability is separate from ordinary
transactions and desired publication. It uses the configured 256-operation
comparison/success/failure arm ceilings and exact 1 MiB serialized physical
request ceiling. Ordinary 96-operation transactions, staging and source-release
batches keep their existing limits. Callers cannot choose a budget.

Before irreversible source release, read-only preparation composes the complete
envelope, reserving maximum positive ModRevision width for source keys that will
change. Include root deletion, reverse-prefix absence, execution guards and
closing-report deletion. The store validates namespace and exact physical size
using its normal preparation. This budget projection is not executable or
persistable. Only then may bounded source release proceed; final commit re-reads
exact authority rather than using projected revisions.

### Closing continuation

For a candidate Task with hooks, entry into normal source release atomically saves
the original terminal report at
`/v1/records/blueprint-closing-reports/{task_id}`. It binds exact Task/assignment
revisions, operation, plan, Agent generation, execution epoch, original outcome,
recovery digest and Controller-normalized observation time. Normalization cannot
replace that report. It is temporary continuation authority, not a second receipt;
final completion deletes it with the source root while preserving generic
terminal-delivery requirements.

Every release batch compares the report. Conflicting reports and new events
reject without writes; exact event retransmissions stay read-only. Reconnect
consumes it before classifying effects or advancing epochs, resumes acknowledgement
and returns Controller-completed work without Agent dispatch, capacity use or
closed-Script resolution. Never reactivate references, invent a report for old
closed Tasks or create a replacement plan. A valid report can finish at the
maximum positive execution epoch; without it, exhausted epochs reject and never
wrap. Persistence receives validated copied operations, not a mutable envelope.

## Acceptance

Prove strict grammar, canonical export, exact revision conflicts, non-destructive
omission and fixed-generation Entry identity. Exercise actual producer publication,
claim, terminal acknowledgement and replay for ten and the maximum 32 candidates,
including maximum hook/source fragments. Measure all arm/byte limits; reject
oversize and compare loss without writes. Prove missing/substituted witness
rejection, interrupted release, public reconnect, no repeated Script effects and
uncertain terminal-commit reconciliation from exact durable authority.

## Current status

Latest-wins reconciliation is accepted but not connected to publication or Agent
execution. Its pure resource/conflict selector is the first implementation slice;
it is not an authorization to enable automatic cancellation. Resource fingerprint
capture, private unit persistence, late plan preparation, safe supersession and
operator-surface/live proof remain required.

Authoring and substantial publication paths are implemented. Bounded marker and
terminal-envelope regressions have local proof, including changed-epoch rejection
and lost-response replay without changing the original timestamp. Local
storage and Agent-admission tests do not prove live gRPC, Docker or full-bundle
Apply. [Script evidence](../acceptance/script-execution.md#preserved-broad-failures-and-remaining-qualification)
retains broader failing fixtures. Full integration, remaining source/recovery
paths and CI are not claimed complete; see [tasks](../../tasks/todo.md).
