# ADR 0051: Stage and publish immutable Blueprint revisions

- Status: Accepted
- Date: 2026-08-25

## Implementation closure: direct Entry mutations

Direct Entry add, edit, and remove now derive a complete candidate projection
from the current Environment revision and publish it through this ADR's sole
head-CAS transaction. Each publication owns the full canonical Compose
artifact, immutable Entry value generation references, materialization and
removal steps, one Environment update Task, and the protected response marker.

Entry identity remains stable across edits. A removal publishes absence in the
new projection before runtime cleanup. Direct mutations do not write a flat
Entry as desired-state authority, create a second head, or reparse a historical
Blueprint bundle. The mutation audit is a closed typed union whose canonical
record redacts literal bytes.

The public operation shapes remain unchanged: add returns `201 Entry`, edit
returns `200 Entry`, and remove returns `202 {task_id}`. Add and edit still own
an internal reconcile Task; their resource response is bound to the marker's
Entry replay target, while the marker retains the Task identity needed for
execution and terminal replay.

## Context

Blueprint apply has three contracts that the current scaffold cannot satisfy at
the same time:

- an apply accepts up to 64 files, 768 KiB of decoded file content, and 512
  resources;
- raw multipart input must be streamed and independent of transport framing;
- desired state, the reconcile Task, and the idempotency marker must become
  visible in one etcd transaction of at most 96 operations and 1 MiB.

The scaffold stores every submitted file and every normalized resource in the
publication transaction. A maximum-size bundle therefore exceeds the etcd
operation budget before Task and idempotency records are added. It also binds
older queued Tasks to a singleton current projection. Such a Task either reads
newer desired state or refuses to run after the environment head advances.

The scaffold also reparses the submitted bundle to render a Task. That makes an
audit artifact, rather than the accepted normalized desired state, the runtime
authority. The stored service projection is intentionally lossy, so simply
pointing a Task at the historical scaffold projection would not fix the
problem.

The REST contract in `docs/api-cli.md` defines the external multipart manifest
as exactly `root`, `compose_sources`, `interpolation`, and `files`. ADR 0012
predates that locked wire shape and says the manifest carries a format version.
The implementation already distinguishes the two concepts: the external
manifest has no version field, while the internal canonical manifest used for
idempotency replay is versioned.

This ADR corrects the persistence and transport seam. It does not add missing
Service Remove or Volume Remove capabilities, and it does not make currently
rejected Blueprint extensions available.

ADR 0049 is accepted with this decision for Volume identity and resumable
runtime removal. ADR 0051 remains the sole Environment desired-revision and
desired-head authority. ADR 0049 may add revision-bound runtime, Task, lock,
checkpoint, and replay projections beside publication, but none is a desired
record, desired generation, or alternate visibility head.

## Decision

### External and internal manifests are different records

Each `files[]` entry is exactly `path`, `part`, `size`, and `sha256`, in that
order in the canonical encoding. The canonical entry is:

```json
{
  "path": "compose.yaml",
  "part": "file-000001",
  "size": 123,
  "sha256": "..."
}
```

Part names are the literal prefix `file-` followed by a six-digit, one-based
decimal ordinal. `file-000001` is the first part. `part_id`, five-digit part
names, and an omitted `size` are invalid. `format_version` is not accepted on
the external manifest. Unknown fields remain validation errors. This clause
supersedes only the external-manifest version statement in ADR 0012.

The Controller continues to build a separate internal canonical replay
manifest. That record retains `format_version: 1`. It is serialized by the
Controller, not supplied by the caller, and participates in the protected
canonical intent defined by ADR 0019. Changing multipart boundaries, header
ordering, or other transport framing does not change that intent.

### Provenance claims differ by capture surface

The CLI directory capture uses filesystem metadata and rejects a symlink at
the root, at every traversed directory, and at every captured file. It accepts
only regular files after `lstat`-style inspection. A path escaping the capture
root is rejected before a multipart part is opened.

A browser upload supplies `File` objects and relative names. Browser APIs do
not expose enough filesystem metadata to establish whether a selected object
originated through a symlink. The Console therefore validates names, content,
hashes, counts, and byte limits but makes no symlink-provenance claim. Console
copy must not say that the Controller proved the original browser selection
was not a symlink.

The Controller validates the logical multipart bundle. It does not infer
filesystem provenance that was not transmitted.

### The Controller streams the HTTP body

The Blueprint route must not bind the request to `[]byte`, `string`, or another
whole-body OpenAPI field. The HTTP adapter exposes the typed OpenAPI operation
but passes a bounded `io.Reader` for the request body to the multipart decoder.

The existing 2 MiB raw-body ceiling remains the outer transport bound. The
decoder reads directly from the limited request body into a private Controller
scratch directory while computing hashes. Scratch directories use mode 0700,
scratch files use mode 0600, and every exit path removes the directory. The
Controller does not make a durable etcd write until the complete bundle has
passed multipart, manifest, hash, path, YAML, Compose, identity, dependency,
and desired-state validation.

Scratch use is transport buffering, not persistence. It is bounded by the 2
MiB raw request limit and is never a reconciliation input.

Every scratch directory is named by an unguessable request nonce. After the
durable staging descriptor is created, the directory is atomically renamed to
`<descriptor-id>/<process-nonce>` and its fixed-size sidecar records descriptor
ID, revision ID, locator digest, creation time, and last-progress time. Scratch
deletion is directory-file-descriptor-relative and never follows symlinks.

The startup and periodic scavenger handles every crash point:

- a nonce directory with no descriptor binding is removed after one 30-second
  request-timeout interval;
- a bound directory whose durable descriptor is absent, abandoned, expired,
  or published is removed immediately;
- a bound directory whose descriptor is open or sealed is removed after its
  local progress time is stale for 30 seconds, because durable chunks, not
  scratch, are the retry source;
- malformed sidecars and unexpected filesystem object types are quarantined
  and reported, not traversed.

Thus a crash before descriptor creation, between descriptor and rename, during
staging, or after publication leaves a bounded, scavengable object.

### A revision owns both normalized desired state and audit input

Every successful Blueprint apply receives a `revision_id`. The revision owns
two immutable logical streams:

| Stream | Purpose | Runtime authority |
| --- | --- | --- |
| normalized projection | lossless typed desired state, stable identities, dependency graph, and accepted native Compose fields | yes |
| submitted bundle | exact logical external manifest and submitted file bytes | no, audit and deterministic reparse only |

The normalized projection is sufficient to plan and render without reading the
submitted bundle. It preserves every accepted value needed by reconciliation;
it is not the scaffold's reduced service summary. Its internal schema is
versioned independently from the external manifest.

This revision root supersedes ADR 0013's flat mutable desired-record authority.
Flat records may remain read models, but they are derived and carry their source
revision and generation. They are never mutation or reconciliation authority.
Every desired Add, Edit, Remove, and Blueprint Apply first derives a complete
new projection from one pinned prior revision and publishes one new revision
through the same head-CAS seam. No operator route may update a flat desired
record independently.

ADR 0049's Environment mutation epoch remains permitted only as a bounded CAS
serialization fence and publication receipt. Rewriting that epoch cannot make
a Volume or other resource desired, cannot select render input, and cannot
advance desired generation. The Environment head in this ADR is the only
operation that publishes desired state.

The audit stream is retained losslessly. Reparse tooling may compare a newly
parsed projection digest with the stored projection digest, but the Controller
must not use such a reparse to execute a queued Task.

### Runtime and Task projections may be publication-adjacent

One ADR 0051 publication may atomically create bounded runtime and Task records
required to execute the newly published desired revision. For ADR 0049 Volume
operations these records may include the Volume runtime projection, Environment
removal lock, active operation, Task and indexes, `desired_published`
checkpoint, and ADR 0021 replay target.

Each such record carries the Environment ID, desired revision ID and
generation, operation ID, and current Task identity applicable to its role.
Readers reject a mismatched tuple as corrupt or stale. Their authority is
strictly limited:

- the Volume runtime projection reports `creating`, `active`, `create_failed`,
  or `deleting`; it does not prove that the Volume is desired;
- the removal lock serializes later Environment desired writers; it does not
  retain or remove desired state;
- the Task and checkpoint authorize execution and resumable progress only for
  the pinned revision;
- the replay target resolves an ADR 0021 marker after the desired Volume is no
  longer current; it is not resource existence evidence.

Current desired reads always resolve the sole Environment head and normalized
projection first. Runtime joins cannot add an item absent from that projection,
except ADR 0049's explicitly deleting tombstone presentation after the removal
revision is already current. Runtime finalization may update or delete these
adjacent projections and release the lock, but it never switches the desired
head or creates a desired revision.

### Revision data is privately staged in immutable chunks

Validated revision data is written under a private staging namespace. Public
environment reads and Task execution cannot resolve that namespace until a
sealed root is published by the environment head.

The versioned `/v1` logical Store namespace contains:

- one descriptor keyed by its stable encoded descriptor ID;
- one separate idempotency-scope and digest locator pointing to that descriptor;
- audit chunks numbered from zero;
- normalized projection chunks numbered from zero;
- one sealed root.

Every dynamic key segment follows ADR 0013: the literal prefix `~` followed by
unpadded base64url of the segment's raw bytes. Raw text, padded base64, percent
encoding, and hexadecimal digest segments are not valid Store keys.

All new durable revision records use Groundplane Durable Record codec version
1, `GDR1`. Integer fields are unsigned, fixed-width, and big-endian. Digests
are raw 32-byte SHA-256 values. Strings are UTF-8 preceded by an unsigned
32-bit byte length. There is no JSON, base64, platform-sized integer, or map
iteration in the durable encoding.

Every `GDR1` record begins with an exact 44-byte envelope: four magic bytes,
one record-kind byte, one codec-version byte, two flag bytes, a four-byte
payload length, and a 32-byte payload digest. A chunk payload adds one family
byte, a four-byte sequence, an eight-byte logical offset, a four-byte logical
length, a 32-byte chunk digest, a four-byte data length, and the raw data. Its
fixed cost is therefore 44 + 53 = 97 bytes.

Chunk numbers are contiguous. A chunk carries at most 60 KiB, or 61,440 bytes,
of logical stream data. Its maximum encoded record size is exactly:

```text
97 + 61,440 = 61,537 bytes < 65,536 bytes
```

The audit logical stream uses the deterministic `GPAU1` framing: a 12-byte
header containing magic, version, file count, and manifest byte length,
followed by the canonical external manifest, then manifest-order file frames.
Each file frame has a four-byte ordinal and four-byte content length followed
by raw content. Path, part, size, and digest are already in the manifest.

At 64 files, framing is 12 + (64 * 8) = 524 bytes. The audit logical-stream
ceiling, including all framing, is therefore:

```text
262,144 manifest + 786,432 files + 524 framing = 1,049,100 bytes
```

It requires at most 18 chunks:

```text
ceil(1,049,100 / 61,440) = 18
```

The 2 MiB normalized projection ceiling includes its durable projection schema
and framing. It therefore requires at most 35 chunks:

```text
ceil(2 MiB / 60 KiB) = 35
```

A revision consequently has at most 53 chunks. The resource ceiling remains
512 across all normalized resource kinds. Per-file, path, YAML node, alias,
and nesting ceilings remain those in the Blueprint contract.

The 2 MiB projection ceiling reuses the existing maximum accepted raw request
size as the Controller's normalized-output allocation class. It is a new
derived-output bound because the present product contract has no normalized
projection byte bound. Acceptance of this ADR accepts that additional
validation boundary. Exceeding it returns a stable Blueprint validation error
before staging begins.

### Staging transactions have fixed budgets

A staging write transaction contains at most 12 chunks. It has at most 12
chunk-absence or matching-digest comparisons, 12 chunk puts, one descriptor
comparison, and one descriptor progress update. Its maximum is therefore 26
operations.

The exact conservative request arithmetic is:

```text
12 chunk values                 12 * 61,537 = 738,444
one descriptor value                         =   4,096
26 encoded keys                 26 *  2,048 =  53,248
per-operation protobuf framing  26 *     64 =   1,664
transaction framing                           =     128
total                                           797,580 bytes
```

The builder also measures the actual encoded etcd request. It rejects a
request above 797,580 bytes, not merely one above the store's 1 MiB limit.

Existing chunks are accepted only when family, sequence, length, and digest
match. A mismatch is an internal integrity failure; a retry never overwrites a
different immutable chunk.

### Sealing creates a compact integrity root

After all chunks exist, the Controller reads them at one fixed MVCC revision,
verifies their sequence and digests, and computes one aggregate SHA-256 digest
per logical stream. It then creates a sealed root containing:

- root schema version;
- environment ID and revision ID;
- source kind;
- render generation and normalized projection schema version;
- audit chunk count, logical byte count, and aggregate digest;
- projection chunk count, logical byte count, resource count, and aggregate
  digest;
- the validated baseline environment-head revision;
- the validated dependency and remove-precondition digest.

The sealed root is one `GDR1` record of at most 8 KiB. The staging descriptor
is one `GDR1` record of at most 4 KiB. Neither lists individual chunk digests;
readers load the contiguous range and recompute the aggregate digest.

At the maximum 53 chunks, the seal transaction uses 53 chunk revision
comparisons, one descriptor comparison, one absent-root comparison, one root
put, and one descriptor state update. It therefore uses 57 operations. The 57
keys, values, and framing have this exact conservative bound:

```text
57 encoded keys                 57 * 2,048 = 116,736
root and descriptor values      8,192 + 4,096 = 12,288
per-operation protobuf framing  57 *    64 =   3,648
transaction framing                         =     128
total                                         132,800 bytes
```

The builder measures the actual request and rejects it above 132,800 bytes.

Sealing does not make the revision public.

### Publication is one compact atomic transaction

The final publication transaction atomically performs all of the following:

- verifies the sealed root and staging descriptor;
- verifies the environment head and dependency/remove baseline have not
  changed since validation;
- verifies tenant, project, environment, active-operation, queue, Task, and
  idempotency preconditions;
- changes the environment desired-state head to the sealed revision root;
- creates the reconcile Task and its required queue, active-operation,
  history, and owner index records;
- writes the ADR 0021 idempotency marker containing the exact accepted
  response;
- removes the private staging locator and marks the revision published.

For an ADR 0049 Volume removal, the same success arm may additionally create
the bounded Volume runtime tombstone, Environment removal lock, replay target,
active operation, and `desired_published` checkpoint. These writes are
publication-adjacent execution and replay projections under the authority
limits above. They do not add a second desired mutation.

The desired-state mutation is the head change. No file chunk and no normalized
resource record is copied in this transaction.

An ordinary publication without `x-gp-backup` contains at most 32 comparisons,
a success arm of at most 32 mutations, and an ADR 0021 failure arm of at most
32 fixed-revision reads. All arms are encoded in the one etcd transaction
request, so its maximum is exactly 96 operations. The ordinary builder rejects
an internal plan that exceeds any one of those three 32-operation partitions.

The maximum ADR 0049 Volume-removal compare partition is exact:

| Compare family | Count |
| --- | ---: |
| final desired-schema marker and Controller leadership/writer fence | 2 |
| Environment, Project, Tenant, their three deletion tombstones, and Environment mutation epoch | 7 |
| current Environment head, sealed root, sealed staging descriptor, and matching locator | 4 |
| absent idempotency marker, Task, queue, active operation, operation history, and three Task-owner indexes | 8 |
| Volume runtime revision, absent removal lock, absent materialization writer, absent replay target, sealed removal evidence, impact digest, slug reservation, immutable-key reservation, and absent desired checkpoint | 9 |
| Backup policy/source aggregate and historical-consequence aggregate | 2 |
| **Total** | **32** |

The failure arm contains the corresponding 32 point reads. A failed compare is
therefore classified from the exact schema, ancestry, head, Volume runtime,
lock, replay, impact, Backup, Task, and idempotency evidence at the transaction
revision. There is no additional post-transaction read needed to decide the
compare failure.

Publication values have these maximum encoded sizes:

| Record | Maximum |
| --- | ---: |
| environment head pointer | 1 KiB |
| public Task record | 64 KiB |
| idempotency marker | 256 KiB |
| staging descriptor update | 4 KiB |
| Volume runtime projection or deleting tombstone | 8 KiB |
| Environment removal lock | 4 KiB |
| ADR 0021 replay-target record | 4 KiB |
| `desired_published` checkpoint | 4 KiB |
| each queue, history, mutation-epoch receipt, active-operation, owner index, retention index, or remaining record | 1 KiB |

The Task record contains revision and sealed-root references, plan identity,
status, and compact step metadata. Large render inputs and materializations
belong to the immutable revision projection, not the Task record.

The success-arm envelope assigns values to all 32 mutations. It charges the
head, Task, marker, descriptor, Volume runtime projection, removal lock, replay
target, and desired checkpoint at their individual maxima, and charges all 24
remaining mutation values at 1 KiB even when an operation is a delete:

```text
head                                             1,024
Task                                            65,536
idempotency marker                             262,144
descriptor                                       4,096
Volume runtime projection                         8,192
Environment removal lock                          4,096
ADR 0021 replay target                            4,096
desired_published checkpoint                      4,096
24 remaining mutation values       24 * 1,024 = 24,576
64 encoded keys                    64 * 2,048 = 131,072
per-operation protobuf framing     64 *    64 =   4,096
transaction framing                               128
success-arm envelope                            513,152 bytes
```

Deletes consume no value but are conservatively charged as 1 KiB remaining
values. The failure arm adds 32 range-read keys and their request framing but no
request values:

```text
success-arm envelope                            513,152
32 failure-read keys               32 * 2,048 =  65,536
failure-operation protobuf framing  32 *    64 =   2,048
full transaction request                         580,736 bytes
```

The builder measures the actual encoded etcd transaction and rejects it above
592 KiB, or 606,208 bytes. That exact local ceiling is 25,472 bytes above the
calculated worst case and 442,368 bytes below the store's 1 MiB ceiling. The
ordinary `Store.Transact` path remains capped at 96 selected operations and
1 MiB. `x-gp-backup` does not widen that general-purpose path.

### Backup Blueprint publication has one bounded atomic envelope

An Environment Blueprint containing `x-gp-backup` uses one dedicated bounded
publication envelope because Backup source identity and same-candidate targets
cannot be pre-published without violating desired-head atomicity. The envelope
includes the desired head, Environment update Task and indexes, ADR 0021
marker, Backup Policy and enabled-only Connector reference, every missing
source-catalog record, a lazily created current age key when required, and the
candidate Attach and Volume identities selected by the policy. The prior head
and every public record remain authoritative until this one transaction commits.

`MaximumBackupPolicySources` remains 12. Each stable source tuple has exactly
three records: source primary, Environment ownership index, and
`(environment_id, kind, target_id)` identity index. Thus the legal maximum adds
36 source-catalog mutations; it does not limit the policy to three sources.
`MaximumEnvironmentBlueprintAttachCandidates` is 2 and counts only new Attach
identities in this candidate, not retained or pre-existing Attaches. Candidate
Attach and Volume targets are validated against this same sealed publication.
They are never pre-created merely to make Backup validation pass.

Connector creation is not a Blueprint capability. The selected Connector must
already exist under the same Environment at the fixed validation revision, and
the final transaction compares its primary, owner index, and deletion fences.
Direct Backup Policy replacement retains ADR 0046's idempotent source
pre-ensure behavior. Blueprint does not use it: all missing three-record source
tuples are created only in the final publication transaction.

The exact worst legal Backup Blueprint plan has this operation shape:

| Family | Operations |
| --- | ---: |
| comparisons | 120 |
| success mutations | 74 |
| fixed-revision failure reads | 120 |
| selected successful path (compare plus success) | 194 |
| complete encoded request (compare plus both arms) | 314 |

A compare failure selects 120 comparisons plus 120 failure reads, or 240
operations. Both the 194-operation successful path and 240-operation failure
path fit the 256 selected-operation ceiling.

The dedicated builder enforces independent maxima of 128 comparisons, 128
success operations, and 128 failure operations. It rejects a plan above 256
selected compare-plus-branch operations, above 384 operations across the full
encoded request, or above 1 MiB (1,048,576 bytes) after protobuf encoding. The
backing etcd deployment keeps its configured 256-operation transaction-arm
ceiling; the builder's per-family 128-operation limits are stricter. These are
Backup Blueprint envelope limits only. Staging, sealing, ordinary Blueprint
publication, and ordinary `Store.Transact` retain their existing limits.

The failure arm reads the exact same head, root, descriptor, policy, source,
candidate target, Connector, key, Task, marker, queue, index, lock, and
deletion-fence evidence at the transaction's one MVCC revision. A failed or
unknown publication cannot expose a new head without all policy identities and
execution authority, cannot expose source identities without the head, and
cannot allocate a second age identity on replay.

Required focused proofs are:

- the maximum legal candidate with 12 selected sources, two new Attach
  candidates, candidate Volume targets, and an absent age key produces exactly
  `120/74/120`, a 194-operation successful path, a 314-operation full request,
  and an encoded protobuf request no larger than 1 MiB;
- source 13 and new candidate Attach 3 fail before publication, while retained
  and pre-existing Attaches do not consume the two-candidate limit;
- same-candidate Attach and Volume source targets validate, while missing,
  cross-Environment, dependent-credential, stale, or concurrently changed
  targets leave every public record unchanged;
- missing, deleting, or concurrently changed Connectors fail atomically, and a
  Connector definition inside the Blueprint is rejected;
- disabled or `none` policies create no age key, first enabled `age` publication
  creates exactly one era-1 identity, exact replay reuses it, and later policy
  changes retain every required key identity;
- direct policy replacement still pre-ensures and reuses stable sources, while
  Blueprint creates no source-catalog record before its final transaction;
- injected failure and compare loss at every final-publication boundary leave
  the old head, policy, Task, marker, candidate identities, source catalog, and
  key either wholly old or wholly new; and
- ordinary 32/32/32 publication and the 96-selected-operation
  `Store.Transact` ceiling remain unchanged.

If a comparison fails, the same transaction's ADR 0021 failure arm reads the
idempotency marker, Task, active operation, queue, environment head,
dependency/remove fence, Volume runtime projection, removal lock, replay
target, impact and removal evidence, Backup aggregates, and relevant owner
records. All returned records have the transaction's one MVCC revision. Those
reads classify exact replay, in-progress ownership, idempotency mismatch,
domain conflict, or an internal invariant failure. The Controller does not
assemble the failure decision from follow-up reads at different revisions.

There is no state in which a new desired head is visible without its Task and
idempotency marker, or a marker reports success without the new desired head.

### Queued Tasks resolve only their own revision

Every Blueprint Task pins the final desired-schema version, `environment_id`,
`revision_id`, sealed-root digest, projection digest, and desired generation. A
worker resolves those exact values, verifies the root and chunks, and renders
from the revision-owned normalized projection.

A worker never consults the current head to obtain render input. It must,
however, consult the final desired-schema marker and current environment head
as a claim fence. Host claim succeeds only when the marker is the pinned final
schema and the current desired generation equals the Task's pinned generation.
A queued Task whose head has been superseded retains immutable, inspectable
input but fails host claim as stale and cannot issue Agent work. The same
generation fence is checked at every pre-Agent mutation checkpoint.

Current desired-state reads first resolve the environment head and then the
published sealed root. Historical Task reads resolve the Task's pinned root.
There is no singleton projection record.

### Private staging coordinates retries without becoming a public claim

The durable descriptor and its locator are separate records with exact key
roles. A descriptor has a server-generated stable `descriptor_id` and is stored
at:

```text
/v1/private/blueprint-staging/descriptors/{~b64url(descriptor_id bytes)}
```

Its locator is stored at:

```text
/v1/private/blueprint-staging/locators/{~b64url(scope bytes)}/{~b64url(SHA-256 idempotency digest raw 32 bytes)}
```

The digest input is ADR 0021's canonical idempotency key bytes. The SHA-256
result remains raw 32 bytes until segment encoding; it is not converted to hex.
The locator value is exactly the `descriptor_id` and protected-intent digest.
It is an index, not the descriptor identity or replay authority.

These shapes do not change any transaction byte derivation. Every occurrence
of a complete encoded key is already charged at the enforced 2,048-byte Store
key ceiling. A raw 32-byte digest segment encodes to 43 unpadded base64url
characters plus the `~` prefix, or 44 bytes, and every scope and descriptor
shape is rejected before transaction construction if its complete key exceeds
2,048 bytes. The existing per-key charge is therefore conservative.

After validation, the first writer creates both in one transaction: compare
the descriptor key absent, compare the locator key absent, compare the public
ADR 0021 marker absent, put the open descriptor, and put its locator. Initial
creation therefore uses three comparisons and two mutations, five operations.
It cannot create private work after a public marker already owns the
idempotency key.

An open or sealed descriptor has exactly one matching locator, and a locator
always names an open or sealed descriptor with the same protected-intent
digest. Published and abandoned descriptors are tombstones and have no
locator. A tombstone may remain until bounded cleanup; descriptor existence by
itself is not a locator reservation. The descriptor contains the protected
canonical intent, baseline head, candidate revision and Task IDs, progress
counters, state, and timestamps. It is not an ADR 0021 marker and cannot prove
an accepted mutation.

Concurrent equal-intent requests reuse the descriptor and candidate IDs. They
may independently attempt matching create-only chunk writes. Concurrent
different-intent use of the same locator returns `idempotency.mismatch` while
the descriptor exists. Only the publication transaction creates the public
idempotency marker.

After an unknown publication outcome, the Controller enters ADR 0021's
ambiguous-outcome algorithm. It performs the required linearizable and
fixed-revision reads of the marker and related publication records:

- a matching pending marker returns `idempotency.in_progress`;
- a matching terminal marker replays its stored response;
- a mismatching marker returns `idempotency.mismatch`;
- a missing marker alone proves neither commit nor non-commit and never
  authorizes a second publication attempt.

Only ADR 0021's complete evidence set may classify the outcome or authorize a
retry. This ADR adds no shortcut based on marker absence, head appearance, or
private staging state.

A known head or dependency conflict performs one atomic transition: compare
the descriptor state and locator target, compare the public marker absent,
change the descriptor to `abandoned`, and delete the locator. Publication
similarly changes the descriptor to `published` and deletes the locator in the
atomic publication transaction. Locator reuse is allowed only after one of
those transitions commits. Chunk and scratch cleanup is eventual and does not
delay reuse. If transition cannot be confirmed, the locator remains reserved
until expiry cleanup completes the same compare-update-delete transition.

### Crash cleanup is bounded and cannot delete published data

Each successful staging batch refreshes `updated_at`. An unpublished descriptor
expires after five minutes without progress. Five minutes is the larger of ten
existing 30-second production request timeouts and twice the existing
120-second Blueprint Task timeout:

```text
max(10 * 30 seconds, 2 * 120 seconds) = 300 seconds
```

The sole Controller leader owns the private-staging GC reconciler. Cleanup
first atomically changes an expired open or sealed descriptor to `abandoned`
and removes its matching locator while comparing the public marker absent. It
then reads the abandoned descriptor and records at a fixed MVCC revision and
deletes at most 32 records per transaction. A full cleanup batch uses at most
32 revision comparisons, 32 deletes, one descriptor comparison, and one
descriptor progress update or final delete: at most 66 operations. Its exact
conservative bound is:

```text
66 encoded keys                 66 * 2,048 = 135,168
descriptor value                              =   4,096
per-operation protobuf framing  66 *    64 =   4,224
transaction framing                           =     128
total                                           143,616 bytes
```

The builder measures the actual request and rejects it above 143,616 bytes.

An abandoned tombstone owns its still-private chunks. The GC deletes those
chunks in bounded batches, removes descriptor-relative scratch, and deletes the
descriptor last. It must prove at one fixed revision that the descriptor is
abandoned, its locator is absent, and its public marker is absent before each
destructive batch.

A published tombstone does not own the chunks named by its sealed public root;
publication transferred those chunks to immutable revision ownership. The GC
proves at one fixed revision that the descriptor is published, its locator is
absent, and the matching public marker, Task, head outcome, and sealed root are
consistent. It then removes descriptor-relative scratch and deletes the
descriptor last. Revision-retention cleanup, not private-staging GC, may later
delete published chunks only after proving that no head, Task, or retained
history references the revision; it deletes chunks before the sealed root.

No published or abandoned descriptor is immortal. A sealed but unpublished
revision remains resumable until expiry, after which the atomic abandonment
transition makes its locator reusable and its private data collectable.

The staging descriptor, locator, tombstone, chunks, and scratch are never
replay records. Once publication succeeds, replay is served exclusively from
the public ADR 0021 marker. After abandonment there is no replayable outcome.

### Remove preconditions use the same immutable head

Blueprint omission remains non-destructive. Validation compares the candidate
projection with the current published projection at one fixed MVCC revision.
If a currently desired Service, Zone, Volume, or other owned resource is absent
from the candidate, apply fails with `resource.in_use` unless an explicit
Remove has already completed and published a desired revision in which that
stable resource ID is absent.

A pending, running, failed, or aborted Remove does not satisfy the precondition.
The validated environment head and remove-precondition digest are included in
the sealed root and checked again in the publication transaction. The head CAS
therefore prevents an apply from racing either a direct mutation or a Remove.

All existing and future Service, Zone, Volume, and other desired Add, Edit, and
Remove routes use this rule now, not only when the missing removals arrive.
Successful mutation publishes a new immutable desired revision derived from
the prior revision, with an appropriate `source_kind`, through the same
head-CAS publication seam. It must not mutate an existing projection or an
ADR 0013 flat record. The audit bundle reference may be retained unchanged
because the direct mutation, not that bundle, explains the new desired state.

For Volume removal, the Environment removal lock acquired beside this head
publication prevents a later desired writer from making the destructive Task
stale before ADR 0049 runtime finalization. The lock is compared by later
publication builders and released only by runtime finalization. It never
substitutes for the current desired head and cannot make an omitted Volume
desired again.

This ADR defines that integration precondition only. It does not add the
missing Console actions, CLI commands, API endpoints, Controller handlers, or
Task plans for those removals.

### Generated clients follow the corrected OpenAPI contract

The OpenAPI operation continues to describe a multipart Blueprint upload, but
the generated request model must expose the strict external manifest without
`format_version`. The Controller's streaming adapter is an implementation seam
behind that operation and must not be replaced by a generated whole-body byte
buffer.

CLI and Console API clients are regenerated from the corrected OpenAPI schema.
Generated artifacts are not hand-edited. The internal canonical replay
manifest and sealed-root schemas are not public OpenAPI types.

### Final-schema startup requires a clean authority store

The singleton current projection, per-file publication mutations, and runtime
bundle-reparse path are removed in the implementing change. There is no dual
reader, fallback to the old projection, compatibility route, or alias type.

This is a one-time pre-MVP clean replacement, not an online upgrade. The MVP
starts this final desired schema only on a fresh relevant store. There is no
legacy conversion, compatibility reader, cutover protocol, or live migration
behavior.

The final Controller uses one `desired_schema` marker whose `GDR1` payload is
the exact final schema identifier and codec version. Before creating it, the
sole Controller process holds the exclusive bootstrap leadership lease and
performs linearizable range reads at one fixed revision across every namespace
that can contain desired, Task, plan, assignment, idempotency, or runtime
authority. This includes old and final heads, revisions, projections, submitted
files, flat desired records, Task primary and index records, plans, queues,
active operations, assignments, Agent-work authority, retries, staging
descriptors and locators, and idempotency markers.

Startup has exactly these outcomes:

1. If `desired_schema` is absent and every relevant namespace is empty, one
   transaction compares the marker absent and bootstrap leadership unchanged,
   then creates the final marker. Final-schema leadership is acquired only
   after that transaction commits.
2. If `desired_schema` is absent and any relevant namespace is non-empty,
   startup fails closed with `store.schema_reinitialize_required`. The error
   reports affected namespace names and counts but not stored values. The
   operator must stop Controllers and provide a clean store or explicitly
   reinitialize those namespaces outside the Controller.
3. If `desired_schema` contains any legacy, unknown, malformed, or non-final
   value, startup fails with the same clean-store requirement.
4. If the final marker exists, every authoritative record encountered must use
   the final schema and durable codec. An unversioned or legacy authority record
   fails startup; it is not ignored, deleted, or converted.

Bootstrap exclusivity is an explicit deployment gate, not a claim that the new
schema can fence an arbitrary removed executable. Before final-schema
initialization, the installer/operator must:

1. stop, disable, and remove every scaffold Controller and Agent systemd unit
   and confirm their processes are absent;
2. install the final Controller and Agent systemd units with `ExecStart`
   pointing only to the final binaries and configs;
3. generate a fresh Agent token, remove the old token and old Agent config, and
   configure only the fresh token in the final Controller and Agent;
4. confirm the Controller endpoint is not held by another process;
5. start the final Controller as the sole endpoint listener and sole Controller
   leadership candidate, then start the final Agent.

The final Controller compares the final schema marker on every leadership
acquisition and desired publication and still fails closed on any non-final
schema or authority record. The freshly rotated Agent token makes removed
scaffold Agent configuration invalid. Running a removed Controller or Agent
binary manually with root authority, bypassing the final systemd deployment
and replacing tokens or configs, is unsupported and outside the MVP trust
model; the design does not introduce an enterprise credential or binary
revocation scheme to defend against that administrator action.

Readable audit and Task history is preserved only when it is already encoded
in the final schema and refers exclusively to final-schema roots. There is no
legacy history conversion, no legacy replay, no fallback parser, and no
automatic deletion. A requested clean-store reinitialization discards legacy
authority by operator action before startup, not by background Controller
cleanup.

## Alternatives considered

### Increase the etcd transaction operation limit

Rejected. At least two operations per submitted file already exceed the
accepted publication budget at 64 files, before 512 resources and Task records
are considered. Raising the ceiling would couple correctness to oversized
Raft proposals and would not fix historical Task authority.

### Publish chunks and then expose individual resource records atomically

Rejected. Copying staged records into public records recreates the operation
and byte problem. Publishing one sealed immutable root makes all staged data
reachable with one head mutation.

### Keep reparsing submitted bundles for reconciliation

Rejected. Audit retention and runtime authority are different concerns.
Parser or normalization changes must not alter a queued Task, and the submitted
bundle does not by itself retain every stable identity decision.

### Let old Tasks read the current projection when they begin

Rejected. It violates revision isolation and makes execution depend on queue
timing. A Task must execute the desired revision that created it.

### Put `format_version` back on the external manifest

Rejected. The locked REST contract has a strict four-field manifest. Internal
canonical replay versioning supplies the needed evolution seam without adding
caller-controlled input.

### Claim the public idempotency marker before staging

Rejected. A public pending marker without the atomic desired-state mutation and
Task would violate ADR 0021. Private expiring coordination is not accepted
mutation evidence.

### Infer browser symlink provenance

Rejected. Browser `File` uploads do not carry the filesystem metadata required
to support that claim. Path and content validation remain enforceable.

### Support old and new persistence schemas concurrently

Rejected. Dual authority would make projection selection ambiguous and retain
the broken singleton path. The repository requires clean replacement of
superseded contracts.

## Consequences

- Maximum-size Blueprint bundles fit within the existing etcd transaction
  limits without weakening atomic desired-state and Task publication.
- Raw multipart transport is streamed and remains outside canonical intent.
- Each queued Task has immutable, lossless, revision-owned render input.
- A superseded queued Task retains that input but cannot pass the current-head
  host-claim fence.
- Submitted bundles remain available for audit but are no longer runtime
  reconciliation inputs.
- Apply retry and crash recovery gain private durable work coordination while
  public idempotency semantics remain those of ADR 0021.
- Storage gains chunk, descriptor, root, cleanup, and integrity-verification
  machinery.
- Bounded Volume runtime, removal-lock, Task, checkpoint, and replay-target
  records may be committed beside a desired head, but the head and normalized
  revision remain the sole desired authority.
- The Controller must enforce three separate local transaction budgets for
  staging, sealing, and ordinary publication, plus the dedicated Backup
  Blueprint publication envelope, in addition to the store's final ceiling.
- First final-schema startup requires empty relevant authority namespaces or an
  explicit operator clean-store reinitialization.
- There is no in-place migration or compatibility reader; already-final-schema
  history remains readable, while legacy history is not converted.
- This decision introduces an exact 2 MiB normalized projection byte ceiling.
  Product, Blueprint, API, architecture, capability, Console, CLI, and test
  contracts use the same ceiling.

## Accepted projection ceiling

The irreducible choice is the normalized projection byte ceiling. Existing
contracts bound raw input, decoded files, YAML structure, and resource count,
but they do not bound the encoded lossless normalized output; aliases and
normalization can expand it. No staging batch size can supply a safe total
allocation bound without choosing one. The accepted ceiling is exactly 2 MiB
because it reuses the Controller's existing maximum-request allocation class.
The 35-chunk projection limit, 53-chunk revision limit, 57-operation seal, and
132,800-byte seal request are derived from this accepted boundary.
