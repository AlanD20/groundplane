# Disposable QA manual Script legacy migration

Scope: the three previously identified terminal executions of
`scr_01M2104ECJZ050QH7RMDSCFSWG` / `qa-manual-cas-proof` on QA10.25.0.2.
The owner explicitly approved this one-time migration on September9.
This is not a Controller compatibility or automatic repair path.

## Applied result

At fixed read revision3010, all three Tasks retained their expected completed,
failed, or aborted result and exact Agent assignment. Each execution was
`cleanup_proven`, with matching container/body identity and container, body,
and execution-directory absence. Each had zero/absent new source-set metadata,
no source operation records or preparation, and one equal owned body-reference
pair. No assignment, assignment index, active retry, queue, or pruning claim
was present.

Each separate transaction compared12 exact read/absence authorities and made
four mutations: clear that execution's active flag, advance its timestamp,
decrement the matching Script primary by one, and delete its two owned body
references. No aggregate was reset, no source root was invented, and no Task,
plan, snapshot, outcome, or cleanup evidence was rewritten. The shared count
went3 ->2 ->1 ->0. Each migration used16 operations, below both its own32-operation
cap and the ordinary Store's unchanged96-operation ceiling.

| Original Task | Result | Migration revision |
| --- | --- | --- |
| `task_01M2104XFFZEZH8EEQ6BEAQYY6` | completed | 3011 |
| `task_01M2106S9M61XWETZ4SRS8NPXP` | failed | 3012 |
| `task_01M210864YX5YA5GJ254TNVT4X` | aborted | 3013 |

Post-write validation passed for each transaction. Repeating the migration
validated all three with zero mutations at revision3013.
Normal `script remove qa-manual-cas-proof` then returned
`task_01M23D4XMY7BGB5XJ9JNHBEXFS`, which completed at
2026-09-09T15:38:55.019775476Z. Normal listing confirms the Script is absent
and preserves `aaa-initialize-storage`, `migrate-app`, and `migrate-identity`.

## Evidence and recovery boundaries

The one-time tool and its tests remain in ignored repository-local
`.tmp/qa-manual-migration-20260909/`; no runtime fallback was added.
Tool SHA256: `d67f95196ce85e8b981b90bfa19540f69d779940c700dff8fea706cc73089b9c`.
Three focused test methods pass, covering all three outcomes, preservation of
unrelated counts and history, exact reference release, replay, cleanup and
ownership rejection, transaction fences, and a revision-fenced inverse.
The initial test run failed because the new migration implementation was absent;
this was test-first tool development, not a new production bug reproduction.

Before each write, exact before-images and the transaction were saved and synced
under `/var/lib/groundplane/.tmp/qa-manual-migration-20260909` on QA, with parent
mode0700 and file mode0600. Receipts include post-images and a CAS-fenced inverse.
These private files must not be published. The inverse only applies while its
record revisions remain unchanged; normal Script deletion has since changed
that authority, so it must not be replayed to resurrect deleted state.

This closes the historical QA-record disposition and normal deletion proof.
The newly integrated prepared-source runtime remains undeployed; fresh live
manual completed/failed/aborted execution acceptance still needs that deployment.
No Controller/Agent restart, application change, full CI, or full-floor
acceptance is claimed here.
