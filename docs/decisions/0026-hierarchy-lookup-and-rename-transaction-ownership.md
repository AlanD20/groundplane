# ADR 0026: Hierarchy lookup and rename transaction ownership

- Status: Accepted
- Date: 2026-08-21

## Context

Tenant, Project, and Environment slugs are mutable human labels. Stable ids are
the durable identity and ownership boundary: a normal Project stores its Tenant
id, an Environment stores its Project id, and every durable cross-resource
reference uses ids. A parent rename therefore must not rewrite descendants,
desired resources, submitted Blueprint bundles, generations, physical paths,
Compose identities, backup keys, or other durable references.

The earlier Proposed form contradicted ADR 0013's approved stable-id choices by making
normalized document paths and generation records part of the rename
transaction. That would denormalize ancestor slugs into descendant state,
create an unbounded aggregate, and make a label update contend with unrelated
reconciliation data.

ADR 0013's approved deletion subset also fixes exclusion. A destructive operation first
creates `/v1/runtime/deletions/<target-kind>/<stable-id>`; while that tombstone
exists every mutation of the target returns `resource.in_use` (`409`). A rename
transaction that compares only the primary and slug indexes could otherwise
commit after deletion begins.

This decision fixes the internal rename contract. Environment storage lifecycle
is separately accepted in ADR 0025; renaming its label does not change that
storage identity. Current cross-surface implementation and qualification are
tracked through [Hierarchy](../features/hierarchy.md), not the original
Tenant/Project implementation slice that introduced this decision.

## Decision

### Stable ids are authoritative

Tenant slugs are globally unique. Ordinary Project slugs are unique within the
stable Tenant id stored by the Project. A rename changes exactly one label; it
preserves the target id, display name, owner id, kind, and every other field.

Children continue to refer to their unchanged stable owner ids. Rename performs
no descendant scan or write. Human-facing hierarchy paths and URLs are
presentation projections derived from the current Tenant, Project, and
Environment records when read. No durable descendant slug path, document-path
index, or ancestor-slug cache is part of the model.

Display-name Edit and slug Rename remain distinct operations. Rename accepts
only the replacement slug. A concurrent Edit can commit first; the rename
coordinator rereads and preserves that current display name rather than
restoring a stale value.

### Slugs are rejected, never normalized

Hierarchy slugs are 1-63 bytes and match exactly:

```text
[a-z0-9]+(?:-[a-z0-9]+)*
```

The Controller rejects uppercase, Unicode, whitespace, punctuation,
underscores, leading/trailing hyphens, and consecutive hyphens. It does not
lowercase, transliterate, truncate, or otherwise normalize input. Slug indexes
encode the exact accepted bytes according to ADR 0013.

### The use case owns bounded CAS retries

The Controller hierarchy service owns at most three context-aware attempts.
Each attempt:

1. reads the target by stable id;
2. validates its durable identity, kind, owner, slug, and revision metadata;
3. prepares a copy that changes only `slug`;
4. asks the repository to compare and commit against the read primary
   revision; and
5. validates that the returned record preserved every non-slug field.

Only `state.conflict` is retried. Before each read, before each commit request,
and before each retry, caller cancellation or deadline wins unchanged. The
third failed CAS is returned as `state.conflict`; there is no fourth attempt.
Slug conflicts, tombstones, not-found results, corruption, and storage failures
are never retried as CAS contention.

The repository revision is private. It is not accepted from HTTP or CLI callers
and is not exposed as a product concurrency token.

### The repository commits one bounded transaction

For an actual slug change, the repository reads the old scoped slug index, the
unchanged owner-membership index when present, the new scoped slug index, and
the deletion tombstone at the same MVCC view as the primary. It rejects missing
or mismatched accepted indexes as Internal corruption.

The single etcd transaction compares:

- the primary modification revision read by the coordinator;
- the old scoped slug index and its owner id;
- the unchanged owner-membership index when present;
- absence of the new scoped slug index; and
- absence of `/v1/runtime/deletions/<kind>/<id>`.

It then performs exactly three mutations: put the target record, delete the old
slug index, and put the new slug index. It does not mutate the owner-membership
index or any descendant. If the tombstone is created after preparation, its
absence compare fails and diagnosis returns `resource.in_use`, so rename cannot
commit after deletion begins.

Rename-to-current is a semantic no-op. The repository still validates the
primary, current slug index, stable owner index, and tombstone absence at one
consistent read, but performs no transaction and returns the unchanged primary
revision.

After a failed compare, diagnosis uses a fresh linearizable multi-read and
orders outcomes as follows:

1. missing primary becomes the kind-specific `not_found`;
2. an extant deletion tombstone becomes `resource.in_use`;
3. the requested slug owned by another id becomes `slug.conflict`;
4. a changed primary becomes `state.conflict` for the coordinator to retry;
5. missing or mismatched old/owner indexes become Internal corruption; and
6. any otherwise unexplained failed compare becomes `state.conflict`.

A requested slug already indexed to the same id after a concurrent successful
rename is contention, not a collision; the retry observes rename-to-current and
returns success without another write.

### Label cutover is clean

The old slug index is deleted in the same transaction that publishes the new
one. There is no alias, redirect, dual index, compatibility reader, or
normalization fallback. After success the old slug does not resolve and the new
slug resolves to the same stable id.

Stable-id API mutation paths and scoped CLI label-to-id lookup remain the public
surface contract recorded in `api-cli.md`. This ADR does not implement those
transport paths. It only ensures the internal operation they will invoke has a
single safe transaction owner.

## Consequences

- Rename work is constant-sized regardless of descendant count.
- Stable ownership and physical identity remain unchanged.
- Presentation paths reflect current ancestor labels without stored path
  rewrites or stale denormalized metadata.
- Concurrent Edit and Rename can both succeed without lost fields through the
  bounded reread/CAS loop.
- Deletion start is a hard transaction boundary for rename.
- Old labels fail immediately after a successful rename.
- Environment rename waits for the separate volume lifecycle decision rather
  than inheriting a partial implementation.

## Alternatives considered

### Rewrite descendants and normalized document paths

Rejected. It contradicts stable-id ownership, creates an aggregate whose size
depends on descendant count, and makes label changes rewrite reconciliation and
audit state.

### Keep denormalized ancestor slugs on descendants

Rejected. Such fields require transactional fan-out or tolerate stale values.
Presentation code can derive the path from the current stable-id hierarchy.

### Retain the old slug as an alias

Rejected. Aliases make uniqueness temporal and conceal stale automation.

### Expose expected revisions publicly

Rejected. Persistence revisions are an internal implementation detail. The
Controller owns bounded retries and semantic error mapping.

## Verification

- Slug boundary tests cover the exact grammar and reject normalization.
- Tenant and ordinary-Project tests prove stable id, display name, owner, kind,
  owner index, children, and physical fields remain unchanged.
- Stale-slug tests prove clean removal and new-slug resolution.
- Collision tests prove the original record and indexes remain unchanged.
- CAS tests prove retry success, same-id convergence, exact three-attempt
  exhaustion, and no fourth transaction.
- Tombstone tests cover both a pre-existing tombstone and one created between
  preparation and commit.
- Cancellation tests cover cancellation before work and before a retry.
- Concurrent rename tests run under the race detector.
- Contract scans reject descendant slug-path fields, descendant rewrite loops
  and compatibility aliases. Environment rename preserves its storage identity.
