# ADR 0070: Ordinary Release native predecessor authority

- Status: Accepted
- Date: 2026-09-08
- Capabilities: C06 Releases
- Replaces: ADR 0066's applied-artifact selection assumption for ordinary Releases

## Context

An ordinary group deployment can replace the latest Environment applied artifact
without replacing a separately serving Service. Using that artifact to prove the
Service's predecessor therefore rejects a valid subsequent Deploy. Re-rendering
the old image against the new desired configuration would also restore the wrong
runtime after failure.

## Decision

Ordinary Deploy and Rollback capture the exact per-Service serving Release at
the publication's fixed read revision, using the existing immutable lifecycle
renderer. The Environment mutation epoch fences that selection. The staged
`ReleaseRenderInput` owns the canonical current and optional retained inactive
runtime artifacts; its manifest digest binds them. No full historical input tree
or duplicate witness is added to the aggregate publication marker.

The plan binds explicit predecessor artifact, Release, target, and optional
inactive-artifact references. Historical labels remain exact. Only that named
predecessor may have historical ownership; it cannot become a forward candidate
through ComposeApply. The normal scoped predecessor removal and sealed recovery
procedure remain lawful. An inactive predecessor requires the shared restoration
pair that restores both captured artifacts.

Claim reads and validates the immutable staged witnesses, compares their source
revisions, and persists them in the sorted native assignment authority. It does
not choose a Service's predecessor from the latest Environment artifact. Agent
admission checks witness bytes against plan-bound artifacts, not merely matching
IDs. Terminal recovery matches immutable intent/render lineage and those exact
bytes before accepting the strategy's proof.

Existing immutable release history remains readable for historical rendering.
An old unstarted ordinary plan without complete predecessor authority is not
rewritten or given a fallback; a fresh ordinary operation must capture authority.
Record and transaction ceilings are unchanged. Oversized full Blueprint
publication is a separate unresolved defect, not waived by this correction.

## Verification obligations

- Unrelated Environment application does not erase a serving predecessor.
- Candidate configuration changes do not change historical recovery bytes.
- Missing, foreign, or altered witnesses and source replacements reject.
- Deploy/Rollback Agent admission binds both active and retained artifact bytes.
- Reconnect and terminal recovery retain the exact authority and source fences.
- A fresh gateway Deploy and hostname/WebSocket checks must pass on QA before
  claiming the hosting blocker resolved; unit checks alone do not prove it.
