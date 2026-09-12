# Backup policy and lifecycle

Read this document for policy persistence, scheduling, locks, immutable run and
restore identity, Recovery Point retention, remote cleanup, and age keys. The
[feature entrypoint](../backups.md) owns operator purpose and status.

## Policy persistence

The Backup Policy is an Environment singleton keyed by stable Environment id.
Its durable record contains:

- `environment_id`, `enabled`, `frequency`, `keep`, and `encryption`;
- optional `connector_id`;
- the ordered active `source_ids`; and
- `updated_at`, the UTC commit instant.

It has no independent policy id or list route. `next_run_at` is a response-only
field derived from policy and durable schedule coordination. It is an RFC3339
whole-second UTC value and is null exactly when the policy is disabled or
unconfigured. An enabled valid policy always has a value.

Frequency uses exactly one of these Controller-evaluated UTC forms:

```text
*-*-* HH:MM:SS
Mon|Tue|Wed|Thu|Fri|Sat|Sun *-*-* HH:MM:SS
```

Numeric fields have exact widths. There is exactly one ASCII space. Ranges,
lists, repetitions, timezone suffixes, and calendar dates are invalid.

`PUT` is a full replacement under the normal protected idempotency contract.
It records and replays the exact `200` response. Concurrent writers compare the
policy revision and never merge. A retry does not allocate a different source
id or key.

A disabled replacement may be wholly unconfigured. Disabling a configured
policy retains its submitted Connector, source, frequency, retention, and
encryption choices. Enabling requires a same-Environment live Connector, valid
frequency, `keep` in `1..9007199254740991`, and 1..12 valid sources. The one
range applies equally to JSON/YAML input, public projection, durable policy,
run snapshot, and retention. Floating point and quoted integer encodings are
invalid. A Config source requires `age` encryption.

### Stable source catalog

Every resolved source is a durable Environment-owned record with stable
`spt_` id, Environment id, kind, stable target id, and creation time. Attach and
Volume input labels resolve before publication; the API stores and returns
`target_id`. Config uses the owning Environment id as target.

The identity tuple is `(environment_id, kind, target_id)`. Replacement reuses
an existing tuple and rejects duplicates. Active membership exists only in the
policy's ordered `source_ids`; source records do not duplicate it. A removed
source remains while a current policy or Recovery Point refers to it, and
re-adding the same surviving target reuses its id. Config can occur at most
once. An Attach must be the consumer-owned credential Attach; a dependent is
not a second source. A Volume must be Environment-owned; a backing Environment
data directory is not eligible.

Direct replacement may ensure missing immutable catalog tuples before its final
transaction. An abandoned request may therefore leave an unreferenced record,
which is safe to collect later. The final transaction compares Environment,
Connector and owner index, selected source targets, and deletion tombstones,
then replaces policy and Connector indexes atomically.

Blueprint publication has no pre-ensure allowance. Its one final desired-state
transaction publishes the desired head, Environment update Task and marker,
policy, enabled Connector reference, every missing source tuple, lazy key when
needed, and candidate Attach/Volume identities. Failure publishes none of
them. At most 12 sources mean at most 36 source-catalog mutations because each
tuple has a primary, owner index, and identity index. The candidate Attach
limit counts only new candidate identities; existing or retained targets do not
consume it. Connector creation remains separate and the Blueprint must resolve
one pre-existing same-Environment Connector at its fixed validation revision.

### Connector reference

An enabled policy owns exactly one reverse reference:

```text
/v1/indexes/backup-policies/by-connector/{connector_id}/{environment_id}
```

A disabled policy owns none, even when it retains the Connector id. Policy
replacement changes the policy and old/new indexes atomically. Connector
deletion compares this prefix absent in the same transaction that publishes
its tombstone and finalizer Task. A Connector referenced only by disabled
policies may be deleted; later enablement must select an existing
same-Environment Connector.

## Environment coordination and scheduling

Every consumer Environment owns one record at:

```text
/v1/runtime/environment-coordination/{environment_id}
```

It contains only `environment_id`, a monotonic `schedule_clock_floor`, and an
optional `current_backup_schedule_state` with `policy_digest`, `frequency`,
`enabled_at`, `last_evaluated_at`, and `updated_at`. It is at most 4 KiB and
has no collections. Its etcd modification revision is the Environment mutation
fence.

Every persistence mutation that can change a Backup source, target,
provisioning, runtime intent, or restore evidence reads the domain facts,
coordination, and operation lock at one revision. It compares the exact
coordination revision and absent lock and rewrites the complete coordination
bytes unchanged in the same transaction. An operation-owned restore or
deletion batch instead compares the decoded owned lock and advances the fence.
Only policy replacement and scheduler evaluation change schedule fields.
Backing resources retain their own locks and revision fences. Renaming an
Environment label is outside this fence because backup identity uses stable ids
and it changes no captured persistence evidence.

Immediately before replacement seals its response and replay evidence, after
any initial key work, the Controller chooses:

```text
F = max(schedule_clock_floor, current last_evaluated_at, now UTC)
```

Disable clears current schedule state but retains `F`. Create, enable, or
frequency change seeds `enabled_at`, `last_evaluated_at`, and `updated_at` at
`F`. Same enabled frequency retains `enabled_at`, bounds progress at `F`, and
binds the state to the canonical replacement through `policy_digest`.

`last_evaluated_at` is an exclusive UTC instant; occurrences are whole-second
UTC instants. Evaluation uses instants, not strings. The scheduler reads policy
and coordination at one fixed revision and checks the digest. When
`now <= schedule_clock_floor` it writes nothing. Otherwise it considers only
the latest occurrence in `(last_evaluated_at, now]`. No-due evaluation,
dispatch, and overlap skip each advance coordination atomically. A due outcome
is identified by:

```text
(environment_id, policy_revision, scheduled_at_utc)
```

Dispatch atomically advances coordination, records the exact policy revision,
publishes the Task, and acquires the Environment operation lock. A held lock
records immutable `skipped_overlap` without a Task. Older missed occurrences
are skipped. This prevents duplicate dispatch from repeated ticks or backward
clock motion. `next_run_at` is the earliest occurrence strictly after the
durable boundary, including an overdue occurrence not yet evaluated.

## Locks and mutation fences

The Environment-wide operation lock is:

```text
/v1/runtime/environment-operation-locks/{environment_id}
```

It excludes backup, restore, key rotation, Recovery Point prune, and
Environment deletion. Manual contention returns the normal state-conflict
problem. Connector deletion, policy edit, source-target change, and relevant
Entry, Volume, Attach, or Service lifecycle mutation compare the lock absent in
their protected transaction.

Each PostgreSQL source additionally owns one shared exclusion on its
consumer-owned Attach. The Attach's ordinary backing references transitively
protect the backing Project, Environment, and Service, so the run does not
duplicate them. Each Volume source owns one Volume exclusion. Config needs no
target exclusion beyond Environment lock and coordination revision.

Operation publication acquires required locks, exclusions, protected intent,
Task, and replay evidence atomically. Ordinary terminalization releases exact
ownership only after subprocesses, streams, helpers, staging, and required
checkpoints are safe. No durable ordering depends on an in-process mutex.

## Immutable run and retry

Manual run is bodyless, requires an enabled policy, and captures the complete
ordered source list from one policy revision. Callers cannot submit a subset.
One visible Agent Task processes that order and fails fast. A successful point
from an earlier source survives a later failure.

Publication accepts one caller-selected fixed etcd revision and validates
policy, ordered sources, targets, backing resources, Connector, and key facts at
that revision. The immutable snapshot includes revisions, formats, encryption
and key era, object identities, and whether the Connector has direct
credentials. Its encrypted-credential revision is positive exactly when at
least one direct credential exists; otherwise it is zero and publication
compares the envelope absent. Secret references remain in pinned Connector
metadata and resolve late.

Config additionally stores the original `config_snapshot_id` and fixed
`read_revision`. Publication atomically creates the building snapshot cursor
and Task/snapshot references. Retry reuses this exact Entry and generation
authority rather than selecting a new revision. Absent run primary owns its
deterministic membership and subordinate keys; readers validate their value and
commit revision against the primary.

Generic or Backup retry creates a new Task and later a fresh assignment, but
reuses the original run snapshot. Before publication it validates every pinned
target, Connector, and key dependency and resolves any uncertain prior point or
orphan. It allocates fresh point ids only for failed and unstarted sources.
Verified sources are omitted. A committed point whose retention failed resumes
retention without another point.

Durable source failures retain current phase and one closed code:

```text
capture staging upload head_verification point_commit cleanup retention
aborted timed_out
```

Successful progress follows the closed source transition table. After staging,
the only path is upload, Head verification, point commit, cleanup, and
retention as applicable. `aborted` and `timed_out` are allowed from an active
phase. Provider responses, exception text, request details, object locators,
and credentials do not enter run records.

Restore selection, target, dependencies, evidence, and encryption identity are
also immutable. Restore using a request-only old age identity is not eligible
for generic retry. After the first destination mutation a restore is protected:
locks, holds, finals, helper state, pending intents, and cursors cannot be
released into generic retry. A new execution may continue only through the
atomic adoption procedure in the Agent protocol; it never resnapshots or
repeats already applied work.

## Recovery Points and object ownership

A Recovery Point id and `created_at` are allocated together before execution;
the timestamp is exactly the UTC millisecond encoded by its ULID. It is absent
from public reads until the verified point commit. A private `verified_at` may
record commit time.

The public projection is exactly the stable point/source/target identity,
creation time, size, encryption/key-era fields, and `status: verified` defined
by the API contract. `key_era` is present exactly when encrypted. Target id
remains the original Attach, Environment-config target, or Volume even after
the source leaves the policy. Restore fails when that target no longer exists.
Environment id, format, Connector id, object key, VersionId/ETag, and hashes
remain private.

Point commit atomically creates its Connector reference:

```text
/v1/indexes/recovery-points/by-connector/{connector_id}/{recovery_point_id}
```

Connector deletion compares absence of enabled-policy, Recovery Point, and
orphan references. An orphan retains its reference until exact remote absence
and atomic local cleanup.

Public lists use fixed-revision cursor pages ordered by raw Recovery Point id
descending. Retention uses the same raw-id order; the ULID timestamp is its sole
time and there is no wall-clock tie breaker. It excludes held, restoring,
pending, unverified, and already deleting points, keeps the newest `keep`, and
tombstones older ids. Lowering `keep` is applied by the next successful source
backup, not policy replacement.

One prune dispatch seals the first 1..11 selected candidates. Delete order is
point id ascending, object key ascending, discriminator kind
`version_id` before `etag`, then discriminator value ascending. The Agent Heads
each exact object and requires the sealed metadata, four-part evidence, and
discriminator before Delete. Mismatch is retained for inspection. Already
absent is success only as proven replay of the same sealed deletion.

Prune publication atomically acquires the Environment lock with Task and
retained prune authority. Each operation retains one stable operation id; a
retry uses a new Task/plan while keeping it. A step has a one-based ordinal,
exact point and Connector evidence, and task-scoped credential slots. After
remote absence, one transaction removes the point and its indexes and advances
authority to `verified_absent`. Only after every candidate reaches that state
does terminalization remove dispatch and authorities and release the lock.

Failure/timeout/abort removes verified-absent tombstones, returns survivors to
operation-owned `pending` with no Task owner, removes dispatch, and releases the
lock atomically. A later dispatch reacquires them. No object listing or
in-memory completion set is authority.

## Environment deletion

Environment deletion is an owner cascade under the same lock. It adopts every
retained unassigned prune tombstone and durably checkpoints exact remote
cleanup. It deletes and verifies all point and orphan objects before releasing
Connector credentials, keys, references, policy/source/point records, or the
Environment deletion fence. Failure retains the material needed to resume.

A deletion retry is an atomic ownership transfer, not a new snapshot. Its
publication compares the terminal old Task and exact old ownership, then
transfers the tombstone, lock, checkpoint, and immutable intent to the new Task
without an unlocked interval. It never restarts enumeration from current
Environment state.

Environment-scoped Backup authorities have explicit membership indexes.
Recovery Point/orphan indexes use inverted point ids where newest-first order
is needed; run, restore, and rotation memberships use raw stable Task ids. A
primary and its membership are created, transferred, and removed atomically.
These indexes remain deletion authority after generic Task history is no longer
authoritative.

## Age-key lifecycle

The Environment key is a separate singleton. Public data is recipient, current
era, creation time, and rotation time. Private identity is encrypted with the
Controller key and never appears in policy, idempotency evidence, Task params,
logs, or list results.

The first successful enablement of `age` creates era 1 atomically with policy
publication. `none` and disabled publication create no key. Disabling or
selecting `none` retains an existing key for old points. Blueprint publication
uses the same lazy rule and exact replay reuses the same identity.

Rotation requires an existing key, acquires the Environment lock, creates only
the next wrapped current identity/era, and changes only future points. Its
absolute deadline is 120 seconds. Rotated-away identities are not retained.

Export is bodyless, repeatable, and read-like. It creates no Task,
Idempotency-Key marker, or stored response. It returns the exact identity plus
one LF with:

```text
Content-Type: text/plain; charset=utf-8
Content-Disposition: attachment; filename="groundplane-<environment-id>-age-era-<era>-identity.txt"
Cache-Control: no-store
```

The value never enters an ordinary Environment, policy, Secret, Task, event,
log, or replay response.

For old-era restore, the client reads a local file and submits exactly one
UTF-8 identity line with optional terminal LF, at most 4 KiB. Its derived
recipient must match the point. It is valid only for an older encrypted era,
not a current-era or unencrypted point. The Controller holds it only for
validation and transient task-scoped delivery, then clears it.
