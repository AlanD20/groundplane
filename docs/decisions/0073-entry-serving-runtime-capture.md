# ADR 0073: Entry mutations capture serving runtime

- Status: Accepted
- Date: 2026-09-09
- Capabilities: C08 Entries and C06 Releases
- Extends: ADR 0070's fixed-revision serving selection to Entry updates

## Context

Live Entry creation completed against a stale blue workload while the stable
proxy served green. Desired Blueprint state is not the current per-Service
runtime: an ordinary Deploy or Rollback can advance the latter independently.

## Decision

Entry create, edit and bulk-upsert capture each serving native Release at the
desired read's fixed revision, through the existing lifecycle capture/renderer.
Current and captured retained workloads keep their exact Release, image, proxy
and ownership metadata. Historical Entry decorations are removed before current
Entry generations are attached, so deleted files do not return from history.
The existing mixed-runtime merger preserves persistent-resource authority.

The same immutable desired candidate stores the captured runtime. Reconstruction
uses that candidate's runtime and the baseline revision's prior Entry decorations;
it never reselects a live slot. The existing plan hash seals the resulting pair.
Execution still selects only exposed workload instances, without dependencies
or stable-proxy restarts. Entry DELETE remains materialization-only and does
not use this capture.

The private Task parameter `entry_runtime_epoch_revision` records the captured
Environment mutation epoch. Direct publication requires it to equal the existing
publication fence's epoch; that exact epoch is already compared at final commit.
Release planning excludes active Release/Environment operations, and ordinary
Release publication and terminalization advance this epoch. Thus both a change
before publication and a race at commit reject the captured selection. Immutable
Release history is read during capture, not introduced as a new mutable source.
No second publisher, per-Service publication transaction, new storage namespace,
or transaction/record limit is introduced. Blueprint domain fragments remain
forbidden on direct mutations.

An old unstarted Entry plan without capture authority is not rewritten or given
a latest-runtime fallback. A fresh operation must capture its own authority.

## Proof

The regression starts with a published/acknowledged native Blueprint and modeled
subsequent Deploy/Rollback records using real immutable storage codecs. It first
failed because the green workload was absent, then because reconstruction used
the stale baseline, and finally because pre-publication epoch drift was accepted.
All three corrections pass through the actual desired publisher and sealed
revision reader. Commit-time epoch replacement rejects; replay is read-only.
Real Docker/CLI evidence remains distinct from this hermetic proof.
