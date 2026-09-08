# ADR 0067: Blueprint terminal transaction

- Status: Accepted
- Date: 2026-09-06
- Capabilities: C06 Releases, C21 Environment Blueprint
- Extends: ADR 0062 final publication accounting; ADR 0064 terminal authority

## Context

The actual ten-candidate producer publishes and executes successfully but its
terminal transaction contains 84 comparisons and 52 mutations. The ordinary
96-selected-operation guard rejects completion. Bounded Script source release
can already have started before this rejection. Reconnecting that assignment
then incorrectly requests active Script artifacts from closed executions.

## Decision

Preserve one atomic commit for the original Task terminal state, assignment and
lifecycle removal, idempotency and retention records, applied Compose artifact,
all candidate serving/current-successful projections and terminal evidence,
Environment fences, and final Script source-release fragment. Do not promote
members in separate transactions or infer completion from healthy containers.

Introduce a closed Blueprint Task terminal envelope, constructible only by the
owning Task repository after validating the complete existing authority. Its
store method is distinct from both ordinary Transact and desired publication.
Use the configured 256-operation comparison/success/failure arm ceilings and
the exact 1 MiB serialized physical request ceiling. Ordinary transactions,
staging, and source-release batches retain their existing limits. No global
limit increase, caller-supplied budget, or interface-recovery assertion.

Validate the complete terminal shape before irreversible source release.
Preparation composes the complete Task, candidate, materialization, Environment
and source-finalization envelope without writing source state. For source keys
whose revisions will change during release, the budget-only projection reserves
the maximum positive ModRevision encoding width. It includes root deletion,
reverse-prefix absence, execution guards and closing-report deletion. Projected
envelopes cannot expose executable operations or enter terminal persistence.
The store validates the physical namespace and exact serialized request using
the same read-only preparation as an actual commit. Only after that succeeds
may the existing bounded source-release protocol advance; completion still
re-reads and commits exact final source authority, never projected revisions.
Closed-source reconnect must preserve the exact Task/assignment/execution
authority and resume finalization without reauthorizing Script start or retry.
It must not reactivate references or manufacture a replacement execution plan.
The existing live Task is evidence, not a target for manual record repair.

### Closing continuation

For Blueprint candidate Tasks with hooks, the transition into normal source
release atomically records the original terminal report at
`/v1/records/blueprint-closing-reports/{task_id}`. This temporary continuation
record binds the exact Task and assignment ModRevisions, operation, plan, Agent
generation, execution epoch, original status/result, recovery digest, and
Controller-normalized observation time. Recovery normalization must not replace
the stored original report. This is not a second terminal receipt: final Task
completion deletes it atomically with the source root and retains the existing
generic terminal-receipt protocol unchanged.

Every release batch compares this closing record. Conflicting terminal reports
and new Task events reject without writes; identical event retransmissions keep
their existing read-only deduplication behavior. Reconnect consumes the report
before classifying effects or incrementing the execution epoch, re-enters normal
acknowledgement, and returns Controller-completed work without dispatching it,
resolving closed Script artifacts, or consuming Agent capacity. Existing closed
Tasks without this report cannot be repaired by inventing one.

A valid closing report can finish at the maximum positive execution epoch:
Controller-only completion does not need a new attempt. Reconnect without that
report still rejects epoch exhaustion without writes; it must never wrap.

The persistence consumer receives a closed terminal envelope with validated
copied operations, not a caller-selected budget or a mutable envelope. The Task
repository requires only ordinary persistence and terminal-envelope persistence;
desired-state publication remains a separate store capability.

## Acceptance evidence

- Actual producer completion and exact applied/serving state for ten and the
  maximum 32 candidates, including maximum hook/source fragments.
- Exact complete arm and byte counts, boundary rejection without writes, and
  unchanged ordinary 96-operation rejection.
- Atomic compare-loss behavior, successful terminal replay, and uncertain-commit
  reconciliation using existing durable Task/assignment authority.
- Interruption after source release, reconnect through public assignment
  handling, no repeated Script effects, and exact terminal completion.

The actual ten-candidate/one-hook fault proof confirms terminal compare loss
against a changed Environment epoch preserves Task/assignment revisions and
the original closing report without terminal writes. Because that epoch is
sealed authority, the changed epoch remains a conflict rather than being
adopted on retry. A separate successful atomic commit with an injected lost
response is recognized by a fresh repository through exact read-only terminal
ACK replay, preserving the original timestamp and removing continuation keys.
These are actual producer/persistence-seam tests, not live etcd fault injection.

Product, Blueprint and architecture mirrors are updated with the replacement.
The final formatted owning race selection passes actual producer cardinality,
hook recovery/reconnect, physical budgets, source references, fault/replay,
Task events and Controller-only dispatch. Full Script source authority and
Blueprint producer suites pass; production app/channel/etcd builds pass.
This is source acceptance for QA verification, not deployment, full-repository
gate, or floor-MVP completion. The broader quarantine-dispatch fixture still
fails identically on the unchanged base and is not claimed green.
