# ADR 0038: Blueprint Entry reconciliation

Status: accepted

## Context

The Blueprint parser accepts `x-gp-entry`, but apply currently preserves every
existing Entry and does not reconcile that desired input. Direct Entry CRUD
already locks stable ids, immutable value generations, source metadata, and
Task-backed omission. The product contract does not say how an Entry in a new
Blueprint revision matches an existing stable Entry or how a changed source is
frozen atomically with the rest of Blueprint apply.

## Recommended decision

Each `x-gp-entry` item should carry an operator-authored stable logical key
unique within its Environment. Apply resolves that key through a durable index:

- a new key allocates one Entry id and freezes its first immutable generation;
- an existing key keeps its Entry id and creates a generation only when the
  canonical source or exposure changes;
- omission creates the already-defined Task-backed removal intent;
- destination, storage class, and ownership changes are replacements, never
  in-place identity edits.

Blueprint apply should resolve every literal, Secret, or Attach source before
its atomic commit, then publish Entry metadata, immutable generations, indexes,
removal intents, the Blueprint revision, and its reconcile Task together. No
value may be re-resolved later from mutable source state.

## Consequences

Blueprint Entry reconciliation remains unimplemented until this stable-key and
atomic-generation contract is accepted. Direct Entry CRUD and materialization
continue independently.

## Implementation closure (2026-08-29)

The accepted implementation is governed by ADR 0051's sole Environment desired
head. The earlier proposal to place every Entry primary, generation, index,
removal intent, revision, and Task in one final transaction is superseded by
this staged protocol:

1. The authored `x-gp-entry` map key is persisted as `BlueprintKey`; it is
   identity input and is separate from a replaceable env key or file path.
2. Direct and component-owned Entries have no `BlueprintKey` and are preserved
   by Blueprint reconciliation.
3. Source or exposure changes preserve the Entry id and select a new immutable
   value generation. Destination, numeric ownership, kind, or secret-storage
   changes replace the Entry identity and remove the old output in the same
   Task.
4. Immutable value generations and the derived Entry-to-Environment lookup may
   be prepared before publication. They are inert until the sealed projection
   that references them becomes the Environment head.
5. Omission removes only Blueprint-owned Entries from the next projection and
   emits explicit Agent removal materializations for stale file and
   service-specific generated-env destinations. The canonical all-service env
   file is rewritten from the next projection.
6. Entry list/show reads resolve Blueprint-owned records from the current or
   cursor-pinned projection. The lookup index is routing data, never a second
   desired-state authority.

The real-host evidence is recorded in
`docs/acceptance/c08-blueprint-entry-reconciliation.md`.
