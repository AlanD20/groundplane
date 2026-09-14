# ADR 0079: Restore a failed Task's pinned configuration

- Status: Accepted by the owner on 2026-09-14; implementation and live proof pending.
- Scope: failed-operation recovery for hosting and upgrades, not Backup/Restore.
- Supersedes only ADR 0064's blanket prohibition on recovery file materialization.

## Context

Successful Attach and Entry changes can make immutable Release input obsolete as
a description of working runtime. Even an exact retained Compose artifact can
refer to a file whose contents a later failed Task overwrote. Restarting that
artifact does not restore the working application.

## Decision

The Controller captures the acknowledged per-Service runtime and exact
pre-operation configuration sources before admitting a Task that can change them.
Its immutable plan binds the selected files, prior presence or absence, content
digest, destination, ownership and permissions. Sources remain resolvable through
execution and recovery; secret plaintext is never added to Task history or logs.

Recovery may restore only those pinned files affected by that Task, alongside
the selected prior runtime. It must retain the exact assignment, execution epoch,
writer and source fences. A stale recovery cannot overwrite a newer operation's
configuration. Reconnect reuses the same plan and sources; missing authority
fails closed rather than rereading desired state or guessing from the host.

All successful writers that affect this runtime must maintain its acknowledgement.
Prepared, failed, skipped and compensated candidates are not successful input.
Runtime and file source retention must not depend on an earlier Task's survival.
Keep original Release and Task history immutable; do not backfill missing
acknowledgements from historical Release input.

This permits no database restore, migration reversal, Script replay, unrelated
file restoration, latest-Blueprint application or new desired-state versioning.
ADR 0064's other recovery fences and ADR 0078's separately defined supersession
exception remain unchanged. Supersession implementation is outside this repair.

## Acceptance

Prove Deploy, successful Attach/Entry change and Detach, then failed Deploy:
the last working endpoints, configuration, authenticated requests and data survive
without manual repair. Independently check restored bytes and file metadata,
unrelated workloads, missing-source rejection, stale ownership and reconnect.
Cases are SVC-15 and JOURNEY-02 in the product QA matrix.

Restoring only runtime references is insufficient; restoring latest desired input
can repeat the faulty configuration. Neither is a valid substitute. This decision
does not complete implementation, qualify production or resume deferred backups.
