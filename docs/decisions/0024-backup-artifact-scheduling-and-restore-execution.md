# ADR 0024: Backup artifact, scheduling, and restore execution

- Status: Accepted
- Date: 2026-08-20
- Accepted: 2026-08-24
- Partially superseded: 2026-08-30 by ADRs 0047 and 0048, only for artifact
  framing and canonical formats, upload/Head/point-commit checkpoint detail,
  Config restore publication, Agent staging/recovery authority, and the
  Backup-only terminal receipt

All other clauses remain accepted, including product sources and actions,
Controller scheduling and locking, immutable run/retry identity, source-level
consistency, verified Recovery Points, raw-Recovery-Point-ID retention,
Connector/key ownership, remote-first cleanup, restore-to-original-target
behavior, and the six-hour Backup/Restore and 120-second rotation deadlines.
There is no compatibility contract between the superseded clauses and the sole
ADRs 0047/0048 schema-1 contract.

## Context

The MVP product contract fixes one Backup Policy per tenant Environment, an
Environment-owned S3-compatible Connector, selectable Attach, Volume, and
config sources, immutable Recovery Points, and a lazily generated per-
Environment age key. Accepted ADR 0046 owns policy persistence, stable source
identity, protected replacement, and lazy key creation. Accepted ADR 0005 owns
the AWS SDK for Go v2 Connector boundary.

Runtime still needs one exact contract for scheduling, source capture, artifact
bytes, immutable upload, point visibility, retention, restore, rotation, and
deletion cleanup. It must not introduce generic shell execution, make the
Controller an S3 artifact-transfer data plane, expose private object locators,
or leave a second scheduler outside the Controller.

This ADR accepts only the backup/restore typed-payload subset of proposed ADR
0009. ADR 0009 remains Proposed for unrelated execution payloads.

## Decision

### 1. Runtime scope and typed execution boundary

The MVP runtime supports exactly these source kinds:

- a PostgreSQL 16 Attach owned by the consumer Environment;
- this Environment's config Entries and values; and
- one Volume owned by this Environment.

Valkey backup and restore are deferred. A Valkey Attach, a manual Attach, or an
unknown source format fails before Task creation with
`strategy.not_implemented`. No public surface advertises those sources as
runnable.

Backup and restore use closed Agent protobuf payloads. A payload contains only
stable ids, captured revisions, closed format/encryption enums, bounded control
metadata, immutable object identity, digest/size evidence, and task-scoped
secret-slot ids. It never contains a generic parameter map, shell string,
redirection, pipeline, generic executable path, Connector credential, age
identity, Entry value, or artifact byte.

The Controller authorizes and records. The Agent owns local staging,
encryption/decryption, hashing, source mutation, and cleanup. Its
`s3-compatible` Connector adapter owns every AWS SDK Put, Get, Head, and Delete
artifact call. The Controller owns policy, scheduling, Recovery Points, and
retention and resolves Connector credentials into transient Agent slots; it
never performs S3 artifact transfer. Artifact streams are backpressured end to end. Config is the one
typed authority exchange: backup sends bounded validated Entry content from the
Controller to the Agent, while restore sends bounded validated Entry content
from the Agent to the Controller. Neither direction places Entry values or
archive bytes in a Task record, event, acknowledgement log, or generic payload.

Connector credential slots use the bounded
`(task_id, assignment_id, step_id, purpose)` secret-delivery fence. The Agent
accepts only the exact active assignment and purpose and clears the slot when
consumed and again on completion, failure, cancellation, or reconnect. Durable
direct or Secret-backed credentials are re-resolved on a valid redispatch. An
operator-supplied old identity is request-only and therefore cannot be
re-resolved for generic retry.

A Connector `secret_ref` remains a late-bound Secret key rather than a durable
Secret identity. Every delivery and redispatch resolves that key through the
owning Environment's Project first and then the platform fallback. There is no
Secret-to-Connector reverse reference. Secret deletion therefore does not scan
for or block on Connector references; the next credential resolution either
selects the remaining fallback or fails closed.

Each durable Task claim allocates one fresh typed `assignment_id` in the claim
transaction. That id remains unchanged across redispatch or reconnect of the
same claim. A terminal, aborted, or expired claim is never reassigned. Generic
or Backup-specific retry creates a new Task id, and that Task receives a new
assignment id when claimed. The exact active `assignment_id` fences every
Backup secret delivery, checkpoint, acknowledgement, and abort; a message from
another or stale assignment cannot consume a slot, advance durable state,
terminalize the Task, or cancel its work.

One Backup checkpoint delivery is identified by
`(task_id, assignment_id, step_id, sequence)`. Sequence is one-based and
contiguous within that assignment and step. The checkpoint carries a canonical
Controller-protected payload digest. The Controller accepts an identical replay
only while that sequence is the current cursor predecessor, its immutable
deduplication record remains at version one, and cursor, deduplication record,
and resulting domain state share one commit revision at the fixed read
revision. An older sequence after cursor advance is stale. Rewritten identical
bytes or the same identity with a different digest are state conflicts.
It acknowledges the checkpoint only after one atomic transaction commits the
domain checkpoint, deduplication evidence, next-sequence cursor, and every
required mutation-epoch or owned-lock condition and mutation. No fingerprint
derived from Secret plaintext enters a generic Task, event, public projection,
or unprotected durable record. The Agent cannot perform the next irreversible
step before receiving that checkpoint acknowledgement.

Checkpoint control-payload digest version 1 has a protobuf-independent byte
grammar. Its SHA-256 input is the ASCII domain
`groundplane.backup.checkpoint.v1`, one NUL byte, the checkpoint kind as one
unsigned 32-bit big-endian integer, then exactly the selected payload fields in
their message declaration order. A string is an unsigned 32-bit big-endian
byte length followed by its exact UTF-8 bytes. A `uint64` is eight big-endian
bytes. A SHA-256 field is its exact 32 raw bytes with no length. No absent,
default, unknown, unselected, assignment-identity, or outer digest field enters
the input. The closed payload field sequences are:

```text
artifact_prepared:             point_id, stored_size_bytes, stored_sha256
upload_completed:              point_id, stored_size_bytes, stored_sha256
upload_verified:               point_id, stored_size_bytes, stored_sha256
source_cleanup_completed:      point_id
restore_artifact_validated:    point_id, stored_sha256, decoded_sha256
volume_tree_staged:            point_id, staged_tree_manifest_sha256
volume_tree_exchanged:         point_id, live_tree_manifest_sha256
volume_replaced_tree_cleaned:  point_id
config_generation_staged:      point_id, restore_generation_id,
                               entry_generation_manifest_sha256
config_generation_activated:   point_id, restore_generation_id,
                               render_generation
postgres_restore_verified:     point_id
remote_object_absent:          point_id
```

The decoder rejects unknown protobuf fields and a kind/payload mismatch before
computing this digest. This grammar, not protobuf serialization, is the durable
deduplication contract.

Backup secret delivery has exactly four purposes: S3 access key, S3 secret
key, current age identity, and operator-supplied old age identity. Config Entry
content uses its separate typed content transfer and is never a secret purpose.
Current capture, with either `age` or `none` encryption, and `BACKUP_PRUNE`
receive exactly the S3 access-key and S3 secret-key purposes. Capture encrypts
with the public recipient pinned in its sealed plan and prune never decrypts,
so neither operation receives a private identity. Current age identity is
reserved for current-era restore, and operator-supplied old age identity is
reserved for old-era restore.
Every header, chunk, and end record repeats
`(task_id, assignment_id, step_id, purpose)`. A header declares non-zero total
bytes and the exact chunk count. Chunks are one-based and contiguous, use the
existing 32 KiB transient-frame payload ceiling, and all non-final chunks are
exactly 32 KiB. End repeats the exact chunk count. Each S3 credential slot is
bounded to 256 KiB, covering both the 255 KiB reusable-Secret ceiling and the
existing 256 KiB encrypted Connector credential-envelope ceiling. Each age
identity slot is bounded to 4 KiB, matching current identity loading and the
operator old-identity request limit. No slot is durable. The Agent clears each
chunk after consumption, clears the slot on successful consume, and clears all
slots on Task completion, failure, timeout, abort, cancellation, stream loss,
or reconnect before accepting redispatch.

### 2. Scheduler, manual run, locking, and retry

The Controller daemon owns the only scheduler. It creates no systemd timer,
cron entry, scheduler subprocess, or goroutine per policy. Each Environment has
one coordination singleton carrying both its mutation fence and optional
current schedule state. The accepted UTC frequency grammar remains the one in
ADR 0046.

Create, enable, or frequency change starts at the first occurrence strictly
after the policy commit. A due occurrence is identified by:

```text
(environment_id, policy_revision, scheduled_at_utc)
```

After Controller downtime, one tick may dispatch only the latest missed
occurrence in `(last_evaluated_at, now]`; earlier occurrences are skipped by
advancing coordination. Claiming a due occurrence, advancing coordination,
publishing the Task, writing its immutable due outcome with the exact policy
ModRevision, and acquiring the Environment operation lock are one transaction.
Repeated ticks and backward clock movement cannot duplicate it.

One Environment-wide lock excludes backup, restore, key rotation, Recovery
Point pruning, and Environment deletion. A manual
conflict fails through the existing state-conflict problem. A scheduled
occurrence that finds the lock held is durably recorded as `skipped_overlap`
and is not queued.

The Controller acquires that lock atomically before publishing. Connector
deletion, backup-policy edits, backup-source target changes, and lifecycle
mutations of an Entry, Volume, or Service relevant to the operation must
compare the lock absent in the same protected transaction. The MVP needs no
second active-operation index on Connector.

The lock lives at
`/v1/runtime/environment-operation-locks/<environment-id>`. Every consumer
Environment also owns a record at
`/v1/runtime/environment-coordination/<environment-id>`, created atomically
with the Environment. It contains `environment_id`, a monotonic
`schedule_clock_floor`, and optional `current_backup_schedule_state` with
`policy_digest`, `frequency`, `enabled_at`, `last_evaluated_at`, and
`updated_at`; it is at most 4 KiB and contains no collections. Its etcd
modification revision is the mutation fence. Every persistence mutation that
can change Backup source, target, provisioning, runtime-intent, or restore
evidence reads its domain evidence, coordination record, and lock at one fixed
revision, compares the exact coordination revision and absent lock, then
rewrites the complete coordination bytes unchanged in the same transaction.
An operation-owned restore/deletion batch instead compares the exact decoded
owned lock and advances coordination the same way. Only Backup Policy
replacement and the scheduler transform schedule fields. Backing resources
outside the consumer Environment retain their individual target locks and exact
revision fences. A scoped Environment name-label rename does not participate
because Backup identity and artifact keys use stable ids and the rename changes
no captured persistence evidence.

Immediately before sealing a replacement response and replay marker, after any
initial key preparation, policy replacement chooses `F =
max(schedule_clock_floor, current last_evaluated_at, now UTC)`. Disable clears
the optional schedule state but retains `F`; create, enable, or frequency change
seeds at `F`; same enabled frequency carries `enabled_at` and bounds progress at
`F`. The state stores a digest of the canonical policy. Scheduler evaluation
reads policy plus coordination at one fixed revision and validates that digest.
It makes no mutation when `now <= schedule_clock_floor`; otherwise no-due,
latest-catch-up dispatch, and overlap skip each atomically advance coordination
under exact policy and coordination revision compares.

`POST /environments/{id}/backup-run` is bodyless. It rejects a disabled policy
and captures the complete ordered source list from one policy revision; callers
cannot supply `source_ids` or any subset. One visible Agent Task processes
sources in stored order and fails fast. Every source that reached verified
point commit before a later failure remains successful and immutable. A Task
retry preserves the original immutable run snapshot: policy revision, ordered
source ids, target ids and revisions, Connector, encryption/key-era decisions,
formats, and checkpoints. It never snapshots the current policy. Before
publishing the retry, the Controller prevalidates every pinned target,
Connector, and key dependency and resolves any uncertain prior point/orphan
checkpoint. It allocates fresh Recovery Point ids only for the failed source
and sources not yet started; already verified sources are never uploaded
again. A source whose point committed but whose retention step failed resumes
retention only and allocates no point.

Run publication accepts one caller-selected fixed etcd revision and validates
the exact policy, ordered source, target, backing, Connector, and age-key facts
at that revision before composing Task publication. An Environment-config
source stores both its original Task `config_snapshot_id` and this immutable
`read_revision`; the same publication transaction creates the `building`
snapshot cursor and both Task/snapshot references. Retry therefore reuses the
original fixed Entry/generation manifest authority instead of selecting a new
read revision. The run primary's absence owns these deterministic subordinate
keys, while replay validates every subordinate value and commit revision.

The immutable run snapshot records whether the pinned Connector metadata uses
any direct credential slots. Its encrypted-credential revision is positive if
and only if that boolean is true; otherwise it is zero and publication compares
the encrypted envelope absent at the same fixed revision. Mixed mode records
only that a direct envelope exists. Secret references remain exclusively in
the exact pinned Connector metadata and are resolved late.
Restore snapshots use the same boolean and conditional encrypted-credential
revision rule.

Durable source failures record only the failed checkpoint. `failure_code` is
one closed value from `capture`, `staging`, `upload`, `head_verification`,
`point_commit`, `cleanup`, `retention`, `aborted`, or `timed_out`. Provider
responses, exception text, request details, object locators, and credentials
never enter the run record; operator diagnostics are separately redacted and
are not retry evidence.

Each source attempt also persists a closed immutable-current `phase`, distinct
from its coarser operator-visible lifecycle `state`. The phase vocabulary is
`capture`, `staging`, `upload`, `head_verification`, `point_commit`, `cleanup`,
and `retention`. Assignment-fenced checkpoints or Controller transitions may
advance phase only through the closed transition table. A terminal failure
retains its current phase and its `failure_code` must name that exact phase;
`aborted` and `timed_out` are the only terminal codes allowed at any active
phase. Restart never infers execution authority from lifecycle state alone.
After staging, the only successful progression is `upload` to
`head_verification` to `point_commit`. The assignment-fenced
`artifact_prepared` checkpoint is emitted after immutable artifact size and
digest are known and before any Put. Its Controller acknowledgement is the
durable pre-Put upload intent and atomically advances `staging` to `upload`;
the Agent MUST NOT invoke Put before receiving that acknowledgement. From that
acknowledgement until the `upload_completed` acknowledgement, interruption,
cancellation, a lost Put response, or explicit or uncertain Put failure leaves
an orphan candidate with lifecycle state `orphaned` and phase `upload`, using
the pinned run point, object identity, size, and digest. Even a crash before Put
therefore leaves a safe candidate whose exact Head result can prove absence.
The assignment-fenced `upload_completed` checkpoint is emitted only after the
immutable object Put returns success; the Controller acknowledgement is
required before the Agent may issue Head. Its atomic commit advances `upload`
to `head_verification`. An uncertain Head outcome after that acknowledgement
may create an orphan with lifecycle state `orphaned` and phase
`head_verification`. The assignment-fenced `upload_verified` checkpoint is
emitted only after successful matching Head; its atomic commit advances
`head_verification` to `point_commit`. Later uncertainty may create an orphan
with lifecycle state `orphaned` and phase `point_commit`. Verified point commit
requires `point_commit`; an orphan remains in its evidenced phase until
deterministic reconciliation commits or deletes it.

Manual protected intent, exact response evidence, captured plan, Task, and the
Environment lock commit atomically. The same Idempotency-Key replays the exact
Task response. A different request while the lock is held conflicts. Generic
Task retry must reacquire the same exclusion lock atomically. Backup is the
narrow payload-construction exception: it keeps the immutable run snapshot but
rebuilds only failed and unstarted point-attempt payloads with fresh point IDs;
it never resolves latest policy, source, target, or point state. Restore with a
bounded old identity remains non-retryable.

Environment deletion retry is a specialized ownership transfer, not a generic
retry or a new deletion snapshot. Its publication transaction compares the
terminal old Task and exact old deletion ownership, then atomically transfers
the retained deletion tombstone, Environment operation lock, resumable
checkpoint, and exact immutable deletion intent to the new Task and operation.
The checkpoint and intent are not resnapshotted, and there is no interval in
which the Environment is unlocked. The old claim remains terminal; the new
Task receives its own assignment id only when its new durable claim is
created.

Backup and restore Tasks have a six-hour deadline. Key rotation has a
120-second deadline. Terminal state and ordinary lock release are atomic after
all subprocesses, streams, helpers, and temporary state reach their required
checkpoint. A Backup terminal or abort after `artifact_prepared` is
acknowledged and before `upload_completed` is acknowledged cannot release
source or Environment authority unless the same atomic transition creates or
retains the exact `orphaned`/`upload` candidate.

Every terminal `backup` or internal `backup_prune` transition creates exactly
one immutable receipt at
`/v1/runtime/backup-terminal-receipts/{task_id}` in the same transaction and at
the same etcd modification revision as the terminal Task, terminal Backup run
or prune outcome, Environment mutation-epoch advance, assignment and timeout
index removal when assigned, dispatch removal when pruning, and exact lock and
exclusion release. This is the sole durable terminal replay authority. There
is no second prune-specific receipt, compatibility key, or replay dependence
on compactable MVCC history. The receipt binds the complete terminal Task and
result, the exact terminal assignment or its absence for a pending abort, the
strictly older Task revision, the prior Environment epoch revision and
canonical epoch digest, and either the terminal run digest plus ordered source
outcomes or the ordered removed-versus-retained prune outcomes. A canonical
self-digest covers the complete receipt.

An exact acknowledgement or pending-abort replay first matches the caller's
terminal status, result, and assignment against the retained Task, then
requires Task and receipt to share one current modification revision and
validates the receipt against permitted current domain ownership. The receipt
therefore remains authoritative after assignment, dispatch, lock, and
exclusion deletion, after later prune or orphan reconciliation, and after a
complete Environment owner cascade; a missing, later reconstructed,
same-revision-inconsistent, or digest-invalid receipt is corruption or a state
conflict, never inferred success. ADR 0035 owns its bounded retention-pruning
lifecycle.

### 3. Source capture and source-level consistency

One run is not a cross-source snapshot. Each source has its own consistency
boundary and capture time.

#### PostgreSQL Attach

Format `postgres-custom-v1` is the exact stdout of PostgreSQL 16 `pg_dump`
custom format with `--no-owner` and `--no-acl` for the one provisioned Attach
database. The Agent invokes the allowlisted executable directly in the
resolved backing container with typed database and role arguments. It never
constructs SQL or a shell command.

Task publication pins the source, Attach, backing Service, database identity,
and their revisions. The source exclusion record prevents detach, deprovision,
or target replacement until capture and cleanup finish. PostgreSQL provides
the source-local database snapshot; Groundplane does not claim consistency
with another source in the same run.

Each PostgreSQL source owns exactly one shared exclusion on its Attach. The
ordinary retained Attach reverse references are the transitive deletion fence
for its backing Project, Environment, and Service, so the run does not duplicate
those three backing authorities as source exclusions. Each Volume source owns
one Volume exclusion. Environment-config sources need no target exclusion
because the Environment operation lock and coordination revision own their
scope.

#### Environment config

Format `environment-config-v1` is a deterministic POSIX tar stream containing:

- `manifest.json`, canonical UTF-8 JSON describing bytewise-sorted Entry ids,
  kinds, keys or volume-relative paths, exposure ids, secret flags, uid, gid,
  and modes; and
- `values/<entry-id>`, one exact value byte sequence per manifest Entry.

All Entry metadata, current value generations, and values are read from one
linearizable etcd revision. The Controller transfers that bounded typed snapshot
to the assigned Agent, which validates and archives it without returning Entry
values or archive bytes during backup.
Tar entries are sorted bytewise, use uid/gid `0`,
closed kind-derived modes, Unix-epoch modification times, and no PAX or
implementation-specific headers. Values never enter the manifest or durable
Task data. They cross the authenticated Agent channel only in bounded,
task-scoped content frames that are cleared after consumption; config archive
bytes remain on the Agent and flow only to or from S3.

#### Volume

Format `volume-tar-v1` is a deterministic POSIX tar stream rooted at the
selected Volume. Paths are canonical relative paths sorted bytewise. Regular
directories and regular files preserve numeric uid, gid, and permission bits;
mtime is normalized to the Unix epoch. Symlinks, hard links, devices, sockets,
FIFOs, procfs magic links, and nested mount transitions are rejected.

Before capture, the Controller pins every Service mounting the Volume and its
runtime intent. The Agent stops those Services, proves them stopped, captures
the Volume, and restores exactly their prior intents after capture cleanup.
Stopping or restarting failure fails the source. A task-scoped helper receives
only the selected read-only Volume bind, has no Docker socket or Connector
credential, uses network mode `none`, and streams through stdin/stdout.

The immutable Volume source snapshot embeds those mounting Services as one
compact ordered slice of `(service_id, service_revision, prior_intent)`. It is
strictly sorted by stable Service id and unique; its count is derived from the
slice. There is no per-Service runtime child record or arbitrary service-count
cap. The protected run record's existing 256 KiB bound is authoritative.

### 4. Transient staging and immutable object contract

Ordinary capture, upload, download, decrypt, and validation staging is Agent-
owned at `/var/lib/groundplane/agent/tasks/<task>/<step>/<point>`, mode `0700`
with root ownership and files at `0600`. The persistent Agent container has no
workload-root or Environment-volume mount. A task-scoped helper receives only
the exact task-stage bind plus the one authorized source bind. Capture preflight
does not claim an exact pre-capture size: Environment config and a quiesced
Volume use computed bounds, PostgreSQL uses an overflow-safe conservative
estimate, and staging writes remain `ENOSPC`-safe. Exact staged bytes are known
and fsynced before immutable upload. Restore checks the known stored size before
download and a second decoded-size bound before mutation. There is no API
artifact-size maximum.

The Agent completes and validates the deterministic source-format file, fsyncs
it and its directory, then produces the stored artifact. For `age`, it binary-
age-encrypts into a second staged file. For `none`, the stored artifact is the
source-format bytes. It fsyncs the final stored file and computes SHA-256 and
the exact byte count over those final bytes before upload.

One source attempt preallocates a stable Recovery Point id. Its deterministic
object key is:

```text
<prefix>/<environment-id>/<source-id>/<recovery-point-id>/artifact.bin
```

An empty normalized prefix omits the leading component. Labels never enter the
key, and no caller-selected path component may precede or follow this exact
suffix. The Agent uploads with immutable create semantics; an existing object at
the key is never overwritten. Upload metadata is fixed before the request:

```text
groundplane-format-version = 1
groundplane-environment-id
groundplane-source-id
groundplane-recovery-point-id
groundplane-source-format
groundplane-encryption = age | none
groundplane-key-era       # present only for age
groundplane-sha256
```

After the `artifact_prepared` acknowledgement and until the
`upload_completed` acknowledgement, any interruption, cancellation, lost Put
response, or explicit or uncertain Put failure leaves the exact
`orphaned`/`upload` candidate. Reconciliation performs Head against its pinned
object identity before retry may allocate a new point. Confirmed absence clears
the candidate; presence proceeds to exact verification and either commits that
same point or cleans up the exact object. This is safe even when the Agent
crashed before invoking Put because absence is an expected reconciliation
result.

After immutable object Put returns success, the Agent emits
`upload_completed` with the exact point id, stored size, and stored SHA-256. It
must receive the Controller acknowledgement before issuing `HeadObject`. An
uncertain Head after that acknowledgement may create a durable orphan in
`orphaned`/`head_verification` from the committed upload evidence.
`HeadObject` must match content length and every expected metadata field. ETag
is never an integrity digest. After successful Head, the Agent emits
`upload_verified` with the same evidence; its acknowledgement advances the
source to `point_commit`, after which the Controller may commit the Recovery
Point. Later uncertainty may create a durable orphan in
`orphaned`/`point_commit`. Object keys, digests, and provider responses remain
private and never appear in ordinary point reads, Tasks, events, or logs.

A durable orphan record pins the non-secret object identity and expected
evidence. The reconciler performs exact Head verification and either commits
that same point or deletes that exact orphan. It never infers success from
object listing.
Staged plaintext and encrypted files are removed before success; cleanup
failure is Internal and takes precedence over an otherwise successful result.

### 5. Recovery Points, Connector references, and retention

Only a fully verified immutable point exists as a Recovery Point. Failed runs
exist only in Task history. Its public projection is exactly:

```text
{
  id,
  source_id,
  source_kind,
  target_id,
  created_at,
  size_bytes,
  encrypted,
  key_era?,
  status: "verified"
}
```

`key_era` is present if and only if `encrypted` is true. `target_id` is the
stable original Attach, Environment-config, or Volume target and remains on the
point after its source leaves the active policy. Restore rejects the point if
that exact target no longer survives. Environment id, source format, Connector
id, object key, and SHA-256 remain private. `GET
/environments/{id}/recovery-points` returns fixed-revision
cursor pages ordered newest first by Recovery Point id descending. Continuation
cursors preserve that revision and exact ordering.

The Recovery Point id and `created_at` are allocated together before source
execution; `created_at` is exactly the UTC millisecond timestamp encoded by the
Recovery Point ULID. The point remains
publicly nonexistent and invisible until verified commit. A private
`verified_at` may record commit time but is not part of the public projection.

Point commit atomically creates a separate Connector reverse reference:

```text
/v1/indexes/recovery-points/by-connector/{connector_id}/{recovery_point_id}
```

Connector deletion compares absence of enabled-policy references from ADR
0046, Recovery Point references, and orphan references. A Connector therefore
cannot be deleted while any point or orphan still needs its credentials and
bucket identity. An orphan retains its Connector reverse reference until the
remote object is verified absent and the orphan authority and reference are
removed atomically.

`keep` is the number of verified points retained per source. After each newly
verified source point commits, retention selects that source's newest `keep`
ids and tombstones older ids for pruning. Lowering `keep` takes effect on the
next successful backup of that source, not on policy replacement. Disabling a
policy or removing a source does not delete points. Retention is the only
ordinary MVP point-deletion path; there is no operator delete action.

Pruning hides the tombstoned point from list and restore selection, deletes the
exact immutable object, verifies absence, then atomically removes the point,
Connector reference, and tombstone. Failure retains the tombstone for bounded
reconciliation. One durable and executable prune dispatch contains at most
eleven points so its complete Task composition remains within the transaction
operation ceiling.

Prune publication acquires the Environment operation lock atomically with its
Task and retained prune authority. Its terminal transaction compares and
releases that exact lock and atomically either removes the authority after
verified remote absence or returns it to a retained pending state. Environment
deletion cannot steal the lock: it conflicts or waits while a prune owns it.
Once deletion owns the lock, no competing prune may be published; deletion
adopts and processes every retained, unassigned prune tombstone as part of its
remote-first cleanup.

Environment deletion is the exceptional owner cascade. It acquires the same
Environment lock and durably checkpoints exact remote cleanup while Connector
credentials and required age identities remain available. It deletes and
verifies every point object and orphan before releasing point references,
credentials, keys, policy/source/point records, or the Environment deletion
fence. Failure retains all required protected material and resumes from the
checkpoint; it never abandons remote objects by deleting local authority first.
A deletion retry uses the specialized atomic ownership transfer above. It
cannot release and reacquire the deletion fence, replace the immutable deletion
intent, or restart enumeration from newly observed Environment state.

Recovery Point pruning owns that lock with the closed internal operation kind
`recovery_point_prune`. Every retained prune authority carries one stable
operation id. A dispatch contains only its Environment, operation, Task,
creation time, and ordered Recovery Point ids; Connector authority belongs to
each point and step independently. On failure, timeout, or abort, one terminal
transaction removes the dispatch, releases the exact lock, removes every
verified-absent tombstone, and returns every surviving assigned tombstone to
operation-owned `pending` with no Task owner. A later retry atomically publishes
a fresh sealed plan, reacquires those pending tombstones and the lock, and
retains the stable prune operation id.
After exact remote-absence verification, each point transaction removes the
point and all three reverse indexes and advances its retained prune authority
to `verified_absent`. Only after every authority reaches that checkpoint may
one terminal transaction remove the prune authorities and dispatch and release
the exact operation lock. This bounded checkpoint keeps retry evidence durable
without requiring object listing or an in-process completion set.

The internal Agent plan for that dispatch uses the closed
`BACKUP_PRUNE` operation and contains one through eleven ordered
`BackupArtifactPrune` steps. Each step has a one-based contiguous ordinal and
pins the shared prune operation id plus the exact prune, Recovery Point,
source, Environment, and Connector revisions. It also pins the normalized
Connector endpoint, bucket, prefix, region, closed addressing decision,
protected object key, stored size, and stored SHA-256. Credentials and age
identities never enter the plan. Different steps may use different Connectors;
the Controller resolves only that step's S3 access-key and secret-key slots
under the existing `(task_id, assignment_id, step_id, purpose)` fence.

`backup` and internal `backup_prune` Tasks have an exact six-hour overall
timeout. Their public Task journal binds the sealed plan id, digest, target,
type, Agent executor, and ordered step ids. Generic Params and materialization
references are empty; source controls, object locators, credentials, private
identities, and provider data remain only in the private plan/domain records.

Each prune step has an exact 1,800-second timeout. Eleven steps therefore
consume at most five and one-half hours of the six-hour Backup Task
deadline, leaving thirty minutes for dispatch and terminal checkpoint work.
The Agent idempotently deletes only the protected exact object and performs
Head until absence is confirmed; already absent is success. It then emits the
existing assignment-fenced `remote_object_absent` checkpoint for that exact
point and cannot advance to the next step before the Controller acknowledgement
commits. No object listing participates.

### 6. Restore selection, verification, and overwrite behavior

Restore always targets one existing Recovery Point and that source's original
surviving stable target. It never restores into another Environment, Attach,
database, or Volume, and never recreates a deleted target. A removed source may
still be restored when its original target survives.

The request supplies `source_id`, optional `recovery_point_id`, and optional
`age_identity`. When the point id is omitted, the Controller resolves the
latest verified point at one fixed revision before publishing protected intent,
hashing the plan, creating the Task, or taking locks. An existing idempotency
marker replays before any fresh latest lookup. The resolved id is immutable;
Task retry never re-evaluates latest.

The point, source, original target, target revision, Connector, object evidence,
format, encryption mode, key era, and age recipient are pinned in the protected
intent. The Environment lock and any source-target lock commit atomically with
intent, Task, and replay evidence. A current-era age point uses the wrapped
current identity. An older era requires a supplied identity whose derived
recipient exactly matches the point. A wrong identity is validation failure.
`age_identity` is accepted only for an older age era; it is rejected for a
current-era or unencrypted point.

Before target mutation the Agent downloads the entire stored artifact into the
transient task area, enforces the point's expected byte count, computes and
matches SHA-256, fsyncs it, decrypts when required, and fully validates the
closed source format. Disk preflight covers stored plus decoded staging. No
stream feeds a live restore target before full stored-byte and format
verification.

PostgreSQL restore stops every consumer Service using the Attach, rejects new
use of the target, terminates connections only to that database, and runs
`pg_restore --clean --if-exists --no-owner --no-acl`. Success additionally
requires a direct fixed connection and catalog query to that database. This is
destructive in-place restore with explicit downtime; a failed apply is never
reported as restored, and affected consumers remain stopped for exact retry.

Volume restore stops every Service mounting the Volume. Only its decoded tree
is staged outside the Agent task root: the constrained helper creates a hidden
sibling under the target Volume parent so it is on the same filesystem. The
helper receives the exact Agent task-stage bind and authorized Volume-parent
bind, fsyncs the decoded files and directories, records the canonical staged-
tree manifest digest, and executes Linux `renameat2(RENAME_EXCHANGE)` itself.
Different-filesystem staging is rejected. Success requires the exchanged live
tree to match the staged manifest before prior Service intents are restored.
Failure before exchange leaves the live tree unchanged. Replaced-tree cleanup
is durably checkpointed and mandatory.

Config restore first fully validates `environment-config-v1`. Under the
Environment lock, the Agent sends the Controller bounded typed validated Entry
metadata and value content. The Controller allocates one fresh `cfg_` value
generation for every restored Entry and writes replacement Entry metadata and
those immutable value generations into a private restore generation in bounded
etcd transactions.
The private restore generation has one distinct immutable `cfg_`
`RestoreGenerationID`. It is allocated before the first staged write and is
preserved across every eligible Task retry; the `task_` identity names only the
current attempt and never keys generation data.
Ordinary Environment Entry and Service reads and all conflicting mutations
return the existing busy/state-conflict problem while this generation exists;
no partial batch is visible. One final transaction switches the active Entry
generation and records one Environment-wide materialization generation: the
exact next `EnvironmentComposeProjection.RenderGeneration` value. It is a
positive integer, not a per-Entry `cfg_` identity. The Agent then converges
files under the same operation. Success verifies exact Entry ids/value-
generation digests and that exact acknowledged render generation. Failure
before the switch leaves the old generation active; failure after the switch
leaves truthful restored desired state and a durable convergence checkpoint.
Running process environment remains unchanged until its normal deploy or
reconciliation.

Every restore surface identifies the exact target and warns that existing data
will be overwritten. PostgreSQL and Volume explicitly declare downtime. The
point remains immutable regardless of restore outcome.

### 7. Age-key rotation, export, and old-era restore

Accepted ADR 0046 owns lazy creation and the protected current-key record. No
plaintext private identity is persistently materialized on the host or in the
Console. Current identities travel to the Agent only through bounded transient
secret slots and are cleared on every completion, failure, cancellation, and
reconnect path.

`POST /environments/{id}/rotate-key` is bodyless and publishes a Controller
Task. Rotation requires an existing key, acquires the Environment operation
lock, has a 120-second deadline, creates the next wrapped current identity and
era in one transaction, and affects only future points. Groundplane retains no
rotated-away private identities. The operator must export an era before
rotation to restore its points later.

`POST /environments/{id}/export-key` is bodyless, repeatable, and never creates
a Task. As a read-like export it is exempt from `Idempotency-Key` and durable
replay: no command marker or response is persisted. It requires an existing
current key and returns that identity only with:

```text
Content-Type: text/plain; charset=utf-8
Content-Disposition: attachment; filename="groundplane-<environment-id>-age-era-<era>-identity.txt"
Cache-Control: no-store
```

The body is the exact age identity text followed by one LF. It never enters an
ordinary Environment, policy, Secret, Task, event, log, or idempotency response.

For old-era restore, the CLI reads `--age-identity <path>` locally and sends an
`age_identity` JSON string. The request accepts one UTF-8 age identity line with
an optional terminal LF, bounded to 4 KiB. It is held only for request
validation and task-scoped transient delivery, is never logged or persisted,
and is cleared after transfer. Because it cannot be resolved again, an old-era
restore is not eligible for generic Task retry; the operator starts a new
restore request and supplies it again.

### 8. Persistence and bounded execution requirements

ADR 0013 owns general etcd encoding, revision, pagination, index, and migration
rules. Backup runtime adds durable categories for:

1. Environment coordination schedule state and immutable due outcomes;
2. active Environment and source-target exclusion records;
3. immutable verified Recovery Points and Environment/source indexes;
4. Recovery Point-to-Connector reverse references;
5. orphan-upload reconciliation;
6. pruning tombstones; and
7. restore staging, cleanup, and Environment-deletion checkpoints; and
8. immutable terminal Backup and Backup-prune replay receipts.

Environment-scoped Backup authority has explicit reverse membership indexes.
Recovery Point and orphan memberships use inverted Recovery Point ids where
newest-first enumeration is required. Backup run, restore, and key-rotation
memberships use their raw stable Task ids. A primary authority record and its
Environment membership are created, transferred where applicable, and removed
atomically. These memberships are the bounded Environment-deletion enumeration
authority and persist independently when generic terminal Task retention is no
longer authoritative.

For run creation, absent primary authority owns its deterministic Environment
membership and any run-id/ordinal-derived subordinate keys. The transaction
blind-writes those derived keys rather than spending independent absence
compares; fixed-revision readers validate their key and raw value against the
primary, and deletion removes primary and derived authority atomically. Shared
source-target exclusions remain separate authorities and always require exact
absence compares.

Due claim, protected manual intent, Task publication, and lock acquisition are
atomic for their operation. Point commit and Connector reference creation are
atomic. Task terminal state and ordinary lock release are atomic. Pruning and
deletion release references only after verified remote cleanup. Config restore
uses read-hidden bounded batches plus one active-generation switch. No durable
ordering depends on an in-process mutex.

Control records obey the existing bounded Task/protobuf/etcd limits. Artifact
size is governed by disk-space preflight and backpressured streaming, not an API
maximum. Secret values, identities, credentials, artifact bytes, object keys,
signed requests, and plaintext-derived data never enter public or durable Task
surfaces.

## Consequences

- Backup runtime has one scheduler, one Environment exclusion domain, and one
  visible Task per run.
- Successful earlier sources survive a later fail-fast source and are not
  duplicated on retry.
- Capture consistency is explicit per source; the MVP claims no cross-source
  snapshot.
- Every uploaded object is immutable, pre-hashed, remotely verified, and
  represented publicly only after point commit.
- Recovery Points retain their Connector until retention or Environment
  deletion verifies remote cleanup.
- Restore fully verifies bytes and format before overwrite, exposes downtime,
  and verifies the source-specific result.
- Private age identities remain Controller-wrapped or transient; old-era
  recovery depends on an operator export.
- Valkey remains visibly unsupported rather than pretending that shared keys
  are an isolated restorable dataset.

## Verification required

Implementation completion requires schedule/catch-up/overlap race tests;
manual idempotency and checkpointed retry tests; golden fixtures for all three
source formats; disk-preflight and cleanup failure injection; immutable upload,
Head mismatch, orphan, point-reference, retention, and Environment-deletion
tests; current/old-era cryptographic tests; restore overwrite/downtime and
post-verification tests; proof that read-hidden config batches never leak; and
end-to-end API, CLI, Console, and real-host recovery acceptance. Acceptance of
this ADR is a contract decision, not evidence that runtime is implemented.
The deletion tests include atomic retry ownership transfer with no unlocked
interval, prune-versus-deletion lock races and retained-tombstone adoption,
orphan Connector-reference blocking, and atomic Environment membership cleanup.
