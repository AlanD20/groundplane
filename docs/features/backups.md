# Backups

## Purpose and scope

Groundplane backs up one tenant Environment to an Environment-owned
S3-compatible Connector. An operator configures one Backup Policy, runs all of
its selected sources manually or on a Controller schedule, lists verified
Recovery Points, and restores a point over its original surviving target.

The accepted executable Backup subset currently supports exactly three source
kinds:

- one consumer-owned PostgreSQL 16 Attach;
- the Environment's complete Entry configuration and selected values; and
- one Environment-owned Volume.

A policy may contain at most 12 sources. A run processes the stored source
order in one visible Task and fails fast. Each source has its own consistency
boundary; a run is not a cross-source snapshot. Production Gate B still
requires safe recovery for actual Valkey state, but its isolated source and
artifact contract is unresolved. Groundplane does not advertise or run Valkey,
manual Attach, or any other undefined source in the meantime: it fails before
Task creation with `strategy.not_implemented`. Restore into another Environment
or target, alternate restore targets, and target recreation remain outside the
accepted behavior.

This is the feature entrypoint. Exact contracts are routed by task:

- [Policy, scheduling, Recovery Points, and keys](backups/lifecycle.md) owns
  persistence, stable source identity, locks, immutable run/retry snapshots,
  retention, remote-first deletion, restore selection, and age-key behavior.
- [Artifact formats and restore publication](backups/artifacts.md) owns stored
  bytes, Config and Volume archives, source-specific validation, and the
  target-local mutation procedures.
- [Agent execution and recovery protocol](backups/agent-protocol.md) owns the
  schema-1 authority, checkpoint, transfer, staging, terminal-delivery, and
  restart rules.
- [Schema-1 wire contract](backups/wire-contract.md) preserves the exact final
  message fields, tags, and enums not yet implemented in `proto/agent.proto`.
  Its outer-message allocations are reserved in source pending implementation.
- [Managed PostgreSQL execution](backups/postgresql.md) owns the immutable
  PostgreSQL 16 image release, root helper and private client gate, exact
  process boundary, and non-replayable restore-apply rule.

The public REST and CLI shapes remain owned by [the API and CLI
contract](../api-cli.md). Backup desired-state syntax remains owned by [the
Blueprint contract](../blueprint.md). Shared process and module boundaries
remain owned by [the architecture contract](../architecture.md).

## Functional requirements

### Policy

Each tenant Environment has one singleton Backup Policy and no separately
addressable policy id. Backing Environments cannot own a policy or backup key.
The full replacement contains enabled state, UTC frequency, retention count,
`age` or `none` encryption, an optional Connector, and an ordered source list.

A disabled policy may be unconfigured. Enabling requires a valid frequency, a
same-Environment Connector, one or more valid sources, and `keep` in
`1..9007199254740991`. Config sources require `age`. Disabling retains the
submitted choices so a client toggle reads and replaces the complete policy.
Replacement is protected and replay returns the exact original response; it
does not merge concurrent edits, allocate new source ids, or create another
key.

Source ids are stable catalog identities, not positions or target names. The
identity tuple is `(environment_id, kind, target_id)`. Removing a source keeps
its identity while a policy or Recovery Point refers to it; adding the same
surviving target again reuses it. Duplicate tuples are invalid and Config can
occur only once.

### Running and scheduling

The Controller daemon is the only scheduler. It supports the closed daily or
weekly whole-second UTC grammar described in the lifecycle document. It does
not create cron or systemd timers. After downtime it dispatches only the latest
missed occurrence. A held Environment operation lock records the occurrence as
`skipped_overlap` instead of queueing it. Repeated ticks and backward clock
movement cannot duplicate an occurrence.

Manual run is bodyless, rejects a disabled policy, and always captures the
complete stored source list. The Task snapshot pins the policy revision,
sources, targets, Connector, formats, encryption/key era, and fixed-revision
Config snapshot. An eligible retry reuses that snapshot; it never reads the
latest policy. Verified sources are not uploaded again. Failed and unstarted
sources receive fresh Recovery Point ids, while a point committed before a
retention failure resumes retention only.

Earlier sources that reached verified point commit remain successful when a
later source fails. Durable failures use closed phase and failure-code values;
provider text, credentials, object locators, and private data are not retry
evidence.

### Recovery Points and retention

A Recovery Point becomes visible only after immutable upload, exact remote
verification, and atomic point commit. It identifies the stable source and
original target and exposes only its public status and size/encryption facts.
Object identity, Connector, format, SHA-256 values, and provider responses stay
private.

Point lists are fixed-revision cursor pages ordered newest first by raw
Recovery Point id. Retention also orders raw ids and keeps the newest `keep`
verified points per source. Lowering `keep` takes effect after the next
successful backup of that source. Disabling a policy or removing a source does
not delete points. There is no ordinary operator point-delete action.

Pruning is Controller-selected and Agent-executed against exact sealed objects;
it never lists a bucket to choose or prove a point. A tombstoned point is hidden
from list and restore. Its point, Connector reference, and tombstone are removed
only after remote absence is verified. Mismatch or uncertain ownership keeps
the authority for inspection.

### Restore

Restore selects one verified point and overwrites that source's original
surviving target. Omitting a point selects the latest at one fixed revision
before intent, Task, or lock publication. Protected replay resolves before a
new latest lookup, and retry never changes the selected point. A source removed
from the current policy remains restorable while its target survives.

Before mutation the Agent downloads the complete stored object, checks the
sealed length and SHA-256, decrypts when required, checks decoded evidence, and
strictly validates the entire source format through EOF. No network or partly
validated stream reaches a live target.

- PostgreSQL restore stops all consumers, validates the dump, terminates only
  target-database connections, applies a single-transaction destructive
  restore, verifies the database, and restarts previously running consumers.
- Config restore replaces the complete canonical Entry-primary set through a
  read-hidden, bounded roll-forward and then materializes one preallocated
  render generation. Selected values come from the authenticated artifact, not
  from current Secrets or facts.
- Volume restore stops consumers, builds and verifies a same-filesystem hidden
  tree, exchanges it atomically with the live tree, deletes the old tree through
  per-path acknowledged intents, and then restarts previously running
  consumers.

Every restore surface identifies the exact `target_id` and warns that existing
data is overwritten. PostgreSQL and Volume restore also state downtime. The
Recovery Point is unchanged by any restore result.

### Age keys

The first successful enabling of `age` encryption lazily creates the
Environment's era-1 key. The Controller persistently wraps only the private
identity and publicly exposes recipient and era metadata. Plaintext identities
exist only in bounded request or task-scoped buffers.

Rotation is bodyless, requires an existing key, runs under the Environment
operation lock with a 120-second deadline, and affects only future points.
Groundplane does not retain rotated-away private identities. Export is a
repeatable, no-store, plain-text download of the current identity and creates no
Task or durable replay marker. Operators must export an era before rotation if
they need to restore its points later.

An old-era restore accepts one UTF-8 identity line of at most 4 KiB and requires
its derived recipient to equal the point. The value is never logged or
persisted. That execution is not generically retryable because the request-only
identity cannot be re-resolved; the operator starts a new request and supplies
it again.

## Non-functional requirements

- Backup, restore, and internal prune Tasks have one persisted absolute
  six-hour deadline. Reconnect, restart, adoption, and retry do not extend it.
  Each prune step is capped at 1,800 seconds.
- One Environment-wide lock excludes backup, restore, key rotation, pruning,
  and Environment deletion. Source-target exclusions protect the selected
  Attach or Volume. Mutations compare the same coordination and lock evidence
  atomically.
- The Controller authorizes, schedules, checkpoints, and publishes domain
  state. The Agent owns local staging, hashing, encryption, source effects, and
  cleanup. Only the Connector adapter performs S3 Put, Get, Head, and Delete;
  the Controller never transfers artifact bytes.
- Agent work uses closed typed payloads. Generic parameter maps, shell strings,
  pipelines, arbitrary executables, credentials, Entry values, private
  identities, artifact bytes, and provider diagnostics do not enter Tasks,
  events, acknowledgements, public records, or logs.
- Every irreversible effect follows a durable acknowledged intent. A lost
  acknowledgement replays exact evidence; it never repeats publication or
  destruction from lifecycle state alone.
- Staging and transfer memory, record counts, transaction operations, artifact
  sizes, Config values, Volume entries, credentials, and diagnostics are
  explicitly bounded in the technical documents. Bounds fail closed.
- Staged plaintext and encrypted files are removed before ordinary success.
  Cleanup failure takes precedence over an otherwise successful result.
- Environment deletion is remote first. It retains Connector credentials,
  keys, points, orphans, locks, and deletion checkpoints until every exact
  remote object is proved absent.

## Technical design

The Controller seals one immutable authority from a consistent read and
publishes it with the Task, locks, protected intent, and replay evidence. The
Agent accepts only the exact assignment and uses a bounded checkpoint cursor.
Checkpoint acknowledgement is the commit boundary: domain mutation,
deduplication evidence, next cursor, and required fences commit atomically.

Capture completes and fsyncs canonical source bytes, derives optional age bytes,
and hashes both before upload. Put cannot begin before `artifact_prepared` is
acknowledged. Head cannot begin before `upload_completed` is acknowledged.
`upload_verified` acknowledges exact Head evidence; Recovery Point publication
is then Controller-owned. There is no Agent `point_committed` checkpoint.

Uncertain upload, Head, point commit, target mutation, staging recovery, and
terminal delivery retain enough immutable authority to reconcile the exact
attempt. Absence, age, disconnect, bucket listing, or local file validity never
substitutes for the prescribed proof.

The five technical documents above are normative parts of this feature.
Implemented message fields and tags live in `proto/agent.proto`; the routed wire
contract remains authoritative for accepted shapes still absent from it.
Ordering, digest, boundedness, transaction, and recovery invariants live in the
Agent execution document.

## Acceptance

Completion requires focused and race-enabled proof of:

- policy replacement, source-id reuse, Connector deletion fences, schedule
  catch-up/overlap, manual idempotency, immutable retry, and lock races;
- golden and corrupt fixtures for Config and Volume archives plus PostgreSQL 16
  dump/list/apply, exact age sizes, disk/inode preflight, and cleanup failures;
- immutable Put, uncertain Put, Head mismatch, orphan reconciliation, atomic
  point publication, retention, prune replay, and remote-first Environment
  deletion;
- current- and old-era cryptography, rotation/export secrecy, and old-era
  non-retryability;
- pre-mutation format verification, PostgreSQL/Volume downtime, Config
  read-hiding and roll-forward, Volume exchange and every per-path cleanup
  boundary, and post-restore verification;
- Unix-socket Agent sessions, stream caps/fairness, reconnect, process restart,
  lost checkpoint/terminal/staging acknowledgements, protected adoption, and
  every fail-closed ambiguity;
- managed PostgreSQL release provenance, exact host/container/process security,
  helper crash recovery, the one-reap rule, and non-replay of an acknowledged
  restore apply; and
- end-to-end API, CLI, Console, actual-source S3 backup/restore/retention, and
  minimal-host recovery.

Acceptance evidence must identify the exact source and target and must not rely
on handwritten backup scripts. A passed sub-layer does not establish the full
feature.

## Current status

The policy singleton, stable source catalog, protected replacement, enabled
Connector fence, lazy current key, public policy surfaces, Recovery Point list,
manual and scheduled Task publication, scheduler, key rotation/export, runtime
persistence foundation, immutable retry composition, and exact-key prune
execution have implementation evidence recorded in
[the capability ledger](../capabilities.md#product-wide-index).

The feature remains **Scaffolded**, not accepted as an operational Backup
vertical. The checked-in Agent protocol is still the pre-implementation shape.
The [wire contract](backups/wire-contract.md#outer-message-allocation) allocates
the missing outer-message fields; source reservations protect those numbers
but add no runtime behavior. Generic
terminal delivery, Config/Volume transfer, staging recovery, managed
PostgreSQL execution, capture/upload, Restore, production secret resolution,
remote Environment deletion, failure injection, live S3 proof, and complete
actual-source acceptance remain open. The safe Valkey source/artifact decision
needed by production Gate B also remains open. The Console therefore keeps Restore
visibly unavailable rather than sending a provisional request.

The requirements in this feature are accepted behavior. Current implementation
or qualification gaps do not narrow them.
