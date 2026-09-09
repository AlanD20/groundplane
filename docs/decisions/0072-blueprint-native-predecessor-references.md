# ADR 0072: Bounded Blueprint native predecessor references

- Status: Accepted
- Date: 2026-09-09
- Capabilities: C06 Releases and C17 Blueprint Apply
- Extends: ADR 0070's candidate-owned native witnesses to Blueprint Apply

## Context

A running multi-Service Blueprint update duplicated complete historical render
inputs inside its aggregate publication marker. A real two-Service regression
produced a 412,657-byte marker, exceeding the unchanged 262,144-byte ceiling.
The existing per-candidate `ReleaseRenderInput.PriorRuntime` already provides
bounded, immutable, manifest-owned storage for exact predecessor artifacts.

## Decision

Blueprint preparation captures and validates each current and optional retained
Release at one fixed revision, then copies its exact native artifact bytes into
that candidate's existing `PriorRuntime` field before staging. Complete historical
inputs remain transient publication-validation evidence; they are not duplicated
in the marker. Existing historical Release records are not rewritten.

The marker stores sorted Service/read/projection identities, a compact serving
Release/target/retained-Release summary, and the canonical prior-runtime digest.
The candidate manifest binds the render input and its lifetime. These references
never depend on a separately prunable predecessor's render input. No new blob
namespace, collector, compatibility reader, or larger limit is introduced.

Publication still validates and compares the original current/retained Release
sources and projections. It additionally binds the exact staged candidate intent
and render input. Claim and terminal native recovery read those candidate values
at their fixed revision, verify manifest and witness digests, scope, lineage,
artifact ownership and the complete restoration pair, and compare their source
revisions on mutation. Missing, foreign, substituted or mismatched evidence fails
closed. Plan reconstruction and Agent admission retain exact artifact bytes.

The executed Environment artifact remains marker-owned. Assignment retains its
existing native aggregate and separate applied-artifact bounds. Individual render
inputs, aggregate marker, assignment, and full transactions keep their existing
limits; a request that exceeds them still rejects before unsafe execution.

## Existing QA history

Read-only inspection at etcd revision 3016 found all 16 QA publication markers,
without truncation: 12 absent-serving predecessor entries, zero inline serving
witnesses, and zero inline native artifact fields. Their largest marker is
104,866 bytes. Absent-serving entries retain exactly their existing persisted
shape and need no migration. The failed oversized marker was never published.

This is an explicit disposition of the authorized QA installation, not a claim
that every historical installation has this shape. An old marker containing
inline serving snapshots is not accepted through an ad-hoc fallback or inferred
witness. Such history requires an explicit bounded offline disposition before
upgrading that installation. Ordinary immutable historical inputs stay readable.

## Evidence and remaining proof

`docs/acceptance/2026-09-09-blueprint-bounded-publication.md` records publication,
assignment, terminal/replay, native recovery, source fencing, and maximum-member
proofs. A relevant full-bundle QA update remains required for live acceptance;
the hermetic Agent admission check is not gRPC or Docker execution evidence.
