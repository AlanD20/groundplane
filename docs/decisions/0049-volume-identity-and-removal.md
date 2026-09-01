# ADR 0049: Separate Volume identity from its immutable storage key and make removal resumable

- Status: Accepted
- Date: 2026-08-25

## Context

The MVP promises Volume list, detail, add, edit, and remove through the API,
CLI, and Console. The scaffold cannot implement those promises safely because
one field called `name` currently carries three different meanings:

- the operator-facing label, which Groundplane slugs make renamable;
- the Compose top-level Volume key used by service mount declarations; and
- the direct-child directory name beneath an Environment's managed
  `volume_dir`.

Renaming that value would silently change the host path. Removing it has wider
effects: Services may still mount the Volume, a Backup policy may select it,
Recovery Points may retain its historical source identity, and the directory
may contain an unbounded tree of data. A single recursive helper call or one
transaction that compares and rewrites every Service is neither bounded nor
crash resumable.

ADR 0051 establishes the sole authority for Environment desired state. Every
desired add, edit, remove, and Blueprint apply derives an immutable normalized
revision from one pinned prior revision and publishes it through the one
Environment desired-state head. Flat Volume, Service, mount, or Backup records
are read models only. This ADR must not introduce another desired generation,
visibility head, or publication transaction.

The MVP is clean-start. This ADR does not define an upgrade, compatibility,
dual-read, legacy `name` decoder, or global migration scheme.

## Decision

### Volume has three distinct identities

Every Volume has:

| Field | Meaning | Mutability |
| --- | --- | --- |
| `id` | stable Groundplane reference, `vol_` plus ULID | immutable |
| `slug` | public, Environment-scoped operator label | mutable |
| `key` | authored Compose Volume key and managed directory leaf | immutable |

`name` is removed from the Volume contract. It is not accepted as an alias in
the REST API, CLI, Console store, Blueprint extension, normalized projection,
or durable record.

`slug` uses the ordinary scoped-slug grammar: 1 through 63 lowercase ASCII
characters, starting and ending with an ASCII letter or digit, with only
letters, digits, and `-` between them. It is unique among current Volumes in
one Environment. A deleting Volume retains its slug reservation until runtime
finalization so another Volume cannot make operator resolution ambiguous.

`key` is 1 through 255 ASCII bytes from `[A-Za-z0-9._-]`, is neither `.` nor
`..`, and contains no separator or NUL. It is unique for the lifetime of the
Environment, including deleting runtime tombstones and retained idempotency
replay targets. API creation may accept an explicit valid key; when omitted,
the Controller uses the accepted initial slug. Neither edit nor retry changes
it.

The physical path is always:

```text
<environment.volume_dir>/<volume.key>
```

The path is derived, never accepted as operator input, and never renamed when
the slug changes. The rendered Docker Volume name remains `gp_vol_<volume-id>`.

### Stable references and authored Compose references are different

Durable Service mounts and Backup sources reference `volume_id`. A rename
therefore rewrites no consumer or Backup record.

The native Compose document continues to reference a Volume by its top-level
map key. Parsing resolves that immutable authored key to the stable Volume id.
The normalized projection retains both values: the id is durable reference
authority and the key is the reproducible authored/render identity. Rendering
selects the Volume by id and constructs the bind device from the immutable key,
never from the slug.

A public Service mount exposes `volume_id`, target path, and read-only state.
Console and CLI may resolve the current Volume slug for presentation, but the
label is not persisted into the mount.

Backup source records already carry a stable source id and target id. A Volume
source uses the Volume id as `target_id`. The Backup source record remains
historical authority after the source is removed from the current policy.

This stable-id rule applies equally to API mutations and Blueprint apply.

### Desired state and runtime projection are separate

ADR 0051's normalized revision contains only desired Volume identity and
stable references. It does not contain `creating`, `active`, `create_failed`,
or `deleting`.

Those values belong to a bounded runtime/task projection:

| State | Meaning |
| --- | --- |
| `creating` | desired identity is published and its create Task has not proved the managed leaf ready |
| `active` | the create Task proved the managed leaf and removed its private ownership marker |
| `create_failed` | the desired identity remains published but its create Task ended without proving the leaf ready |
| `deleting` | the ADR 0051 removal revision is published and the destructive Task has not finalized its runtime tombstone |

For a desired Volume, list and detail join the current ADR 0051 revision with
the matching runtime projection. A deleting Volume is absent from desired
state; list and detail project it from the deletion tombstone until runtime
finalization. After finalization detail returns `volume.not_found`.

Runtime projection is never render, reconciliation, rename, mount, or Backup
selection authority. Every projection carries the Volume id, operation id,
the ADR 0051 revision/generation that created it, and its current Task
identity. A mismatch is an internal integrity failure.

`create_task_id` identifies the original create Task. Removal uses two fields:

- `origin_task_id` is the immutable Task id created by the accepted DELETE;
- `current_task_id` is the mutable current retry attempt.

Changing retry ownership never changes `origin_task_id`, the operation id,
the accepted deletion impact, the immutable key, or the desired revision.

### ADR 0051 publishes every Volume desired mutation

Volume add, edit, remove, and Blueprint changes do not write an authoritative
flat Volume record. They:

1. read one pinned ADR 0051 Environment head and normalized revision;
2. validate identity, stable references, and removal preconditions at that
   revision;
3. derive a complete replacement normalized projection;
4. stage and seal that projection through ADR 0051;
5. publish the one Environment head, reconcile Task, idempotency response, and
   required runtime/operation records through ADR 0051's publication builder.

The ADR 0051 publication limits, request-size measurement, compare-failure
classification, schema epoch, staging cleanup, and stale-Task claim fences are
authoritative. This ADR adds Volume-specific comparisons and mutations to that
builder but does not restate or weaken its budgets.

The Environment removal lock is an ADR 0051 desired-writer fence. While a
Volume removal is running, no later Environment desired publication may pass
the head/active-operation comparisons and make the removal Task stale.

### Create publishes desired identity and an owned filesystem Task

`POST /volumes` validates the Environment, scoped slug, immutable key, and
idempotency intent. The ADR 0051 publication atomically makes the desired
Volume visible, creates its `creating` runtime projection, publishes the
create Task, and records the replayable accepted response.

The response is:

```text
201 { "volume": <Volume>, "task_id": "task_..." }
```

There is no state in which a desired Volume is visible without its create
Task and runtime projection, or an idempotency marker reports success without
the desired revision.

Before any `mkdir`-like host effect, the Agent acknowledges the task-owned
create intent. The helper then uses the parent `volume_dir` file descriptor
and a private sibling on the same filesystem:

```text
.gp-volume-create-<volume-id>-<operation-id>
```

It creates that sibling exclusively, creates an exclusive fixed ownership
marker containing the Volume id, operation id, Task id, immutable key, and
intent digest, and fsyncs the marker, sibling, and parent. It never opens or
adopts an unexpected existing object as the desired leaf.

After validating the marker and proving the destination leaf absent, it
publishes the sibling with `renameat2(RENAME_NOREPLACE)` and fsyncs the parent.
It then reopens the published leaf descriptor-relatively, verifies the marker,
removes the marker, and fsyncs the leaf. The same-filesystem private sibling,
exclusive marker, and no-replace rename make ownership provable after every
crash.

Create checkpoints are:

1. `intent_acknowledged`;
2. `private_sibling_ready`;
3. `leaf_published`;
4. `marker_removed`;
5. `runtime_active`.

A retry resumes from the last durable checkpoint and verifies the exact
marker or published-leaf evidence before acting. A pre-existing destination
leaf without matching task-owned evidence is never adopted and produces
`create_failed`. Unknown objects are not recursively removed by create.

Blueprint apply that introduces several Volumes uses the same per-Volume
intent and ownership proof within its ADR 0051 reconcile Task. The aggregate
`ensure directories` behavior must not adopt pre-existing leaves.

### Edit changes only the slug

`PATCH /volumes/{id}` accepts exactly `slug`. It is allowed only while the
Volume runtime projection is `active` and no Environment desired operation is
active. It derives and publishes a new ADR 0051 normalized revision. The key,
path, id, Service mount ids, Backup target ids, and Docker Volume name remain
unchanged.

The response is:

```text
200 { "volume": <Volume>, "task_id": "task_..." }
```

The ADR 0051 reconcile Task may prove that no host artifact change is required,
but it remains the Task atomically paired with the desired publication. No
special synchronous flat-record rename path exists.

### Deletion requires a revision-bound impact token

The support endpoint is:

```text
GET /volumes/{id}/deletion-impact?cursor=<opaque>&limit=<1..40>
```

`cursor` is omitted on the first request. That request pins one MVCC revision.
Each returned opaque cursor is at most 4 KiB and binds the Volume id, pinned
revision, Environment head identity, next stable item key, completed-item
count, and rolling digest. A subsequent page reads at that same revision; an
expired or compacted revision returns `state.conflict` and requires a complete
refetch from page one. A cursor cannot select a new revision, skip an item, or
change the limit-independent impact identity.

At the pinned revision the Controller reads the Volume, current ADR 0051 head,
normalized projection, mount-reference rows, current Backup policy and source
records, and historical Recovery Point/Connector consequences. It emits a
strictly ordered stream of impact items. Each item, including its JSON framing,
is at most 16 KiB. A page contains the deterministic largest stable-id prefix
of at most `limit` items whose complete encoded response fits. The server
rejects a limit outside 1 through 40.

Every response is at most 768 KiB of JSON. Common revision and Volume metadata
is at most 64 KiB, cursor/token and response framing are at most 32 KiB, and 40
fully framed items are at most 640 KiB, for a conservative maximum of 736 KiB.
The response includes `complete`, `next_cursor`, the page's items, and the
running item count and digest. Across the complete page sequence it returns:

- Volume id, current slug, and immutable key;
- every Service/mount that the removal revision will detach;
- the exact Backup source ids removed from current policy selection;
- whether the accepted policy transform disables the policy because no active
  sources remain;
- retained policy configuration and scheduling consequence;
- historical Recovery Point counts/digests whose source identity and Connector
  fences remain;
- `data_handling: recursive_destroy`;
- an opaque `impact_token` on the final page only.

Non-final pages have `complete=false`, a non-empty `next_cursor`, and no
`impact_token`. Only the final page has `complete=true`, no `next_cursor`, the
total item count/digest, and the complete `impact_token`. The Controller
computes that token only after resolving the entire ordered impact at the
pinned revision; a partial page or client-supplied running digest can never
produce one.

The token is the lowercase hexadecimal SHA-256 of a canonical versioned
encoding. It binds the Environment head revision/generation and digest, Volume
runtime revision and immutable key, every reference id and revision, the
policy revision and exact replacement transform, historical consequence
digests, the final total item count/digest, and the resulting normalized
projection digest. Collections are strictly sorted by stable id. The raw
canonical encoding is not an API.

The destructive request is exactly:

```text
DELETE /volumes/{id}?impact_token=<token>&confirm_key=<immutable-key>
Idempotency-Key: <key>
```

Both query values are required. The Controller repeats the fixed-revision
impact resolution, and the DELETE may proceed only if its canonical token is
byte-for-byte equal and `confirm_key` equals the immutable key. A slug is not
accepted as destructive confirmation. A changed mount, policy, source,
historical consequence, runtime state, or desired head returns
`state.conflict`; the caller must fetch and display a new impact.

The token is authorization evidence for the shown desired consequences, not a
snapshot of filesystem bytes. Once accepted, non-empty data is destroyed. No
`force` flag, empty-directory shortcut, implicit retention, or adoption mode
exists.

### Removal stages bounded evidence, then publishes through ADR 0051

The Controller materializes the accepted consumer and mount intent as bounded
immutable rows. Each complete encoded row value is at most 16 KiB and includes
the stable consumer, mount, Volume, pinned revision, and row digest. Rows are
sorted by stable id and ordinal. Internal manifest, row, and cursor keys are at
most 512 bytes. The manifest value and cursor value are each at most 16 KiB.
The manifest fixes the total row count, ordered-set digest, pinned impact
identity, and maximum ordinal. The crash cursor fixes the next ordinal,
completed-row count, rolling digest, and manifest digest; it cannot advance
beyond the manifest maximum.

A staging transaction deterministically selects the largest remaining ordinal
prefix of at most 44 rows whose exact serialized etcd request is at most 900
KiB and whose operation count is at most 96. For `n` selected rows it contains:

```text
one manifest comparison
n create-only row comparisons
n row puts
one cursor put
= 2n + 2 operations; at n=44, at most 90 operations
```

The 44-row cap has a conservative byte proof that includes keys and etcd
wrappers. A manifest comparison is at most 1 KiB; each row comparison is at
most 1 KiB; each row put, including its key, 16-KiB value, and wrapper, is at
most 17 KiB; the cursor put is at most 17 KiB; and the transaction envelope is
at most 16 KiB. At 44 rows the request is therefore at most
`1 + 44 + (44 * 17) + 17 + 16 = 826 KiB`, below 900 KiB. The Controller still
measures the exact serialization and shortens the prefix if necessary. Failure
of one valid row to fit these declared maxima is `internal`, not an unbounded
fallback.

Existing rows are accepted only when their immutable contents and digest
match. A transaction compares the manifest and prior cursor and advances the
cursor exactly across the selected prefix. Staging never makes desired state
public.

These rows are removal evidence and bounded execution input. They are not a
parallel normalized desired generation. The complete replacement desired
projection is staged and sealed only by ADR 0051.

The ADR 0051 publication transaction additionally:

- verifies the exact impact token evidence and sealed removal rows;
- verifies the current Environment head and Volume runtime projection;
- requires the current Environment materialization-writer key to be absent;
- acquires the Environment removal lock and active operation;
- switches the sole Environment desired-state head to the revision in which
  the Volume and its mount references are absent;
- applies the accepted Backup policy replacement in that desired revision;
- creates the `deleting` runtime tombstone;
- creates the removal Task, queue/index records, idempotency marker, and
  immutable root-response replay target;
- records the `desired_published` checkpoint.

The materialization-writer absence comparison and removal-lock acquisition are
in that same transaction. The Controller does not cancel, adopt, wait for, or
guess the outcome of an existing writer.

`policy_now` is Controller UTC `now` captured immediately before this ADR 0051
publication transaction is constructed. It is not etcd commit time. The
accepted policy replacement transform uses that exact value as its scheduling
floor.

The first Task id becomes immutable `origin_task_id`. Retry may change
`current_task_id` and current attempt ownership, but all retries use the same
operation, removal rows, desired revision, impact, and filesystem intent.

### Removal has an operation-scoped ADR 0021 extension

Volume DELETE extends ADR 0021 only for this bounded multi-attempt operation.
The publication transaction creates one operation-scoped idempotency marker
whose immutable response is the originally accepted
`202 {"task_id":"<origin_task_id>"}`. The marker remains `pending` while any
removal state is retained, including after one or more attempt Tasks become
terminal. An equal DELETE always replays that stored root response; it never
returns `idempotency.in_progress`, never substitutes `current_task_id`, and
never creates another operation. A different protected intent returns
`idempotency.mismatch`.

Attempt Tasks form an exact append-only chain. Every successor has a new Task
id, `root_task_id=origin_task_id`, `predecessor_task_id` equal to the immediately
previous attempt, and an attempt ordinal exactly one greater. It retains the
same operation id, desired revision, accepted impact digest, evidence manifest,
and traversal cursor. A terminal Task is never reopened or rewritten as the
successor; only the tombstone's `current_task_id` and current assignment
ownership advance. The root Task and every successor remain independently
queryable with their actual terminal outcome.

An attempt-terminal or successor-publication transaction verifies the removal
operation, lock, tombstone, current Task/assignment, checkpoint cursor, and
pending idempotency marker. It uses at most 24 comparisons and 24 mutations,
at most 48 etcd operations, and a measured request of at most 900 KiB. It does
not publish desired state or change the immutable replay response. ADR 0021
expiry is suppressed while the marker is pending or any removal tombstone,
lock, active-operation record, cursor, or current attempt is retained.

### Consumer detachment precedes directory destruction

Publishing the removal revision prevents new desired consumers but does not
prove that previously running containers released the bind mount. The Task
reconciles the published ADR 0051 revision and records `consumers_detached`
only after the Agent proves that no managed Service/container in the
Environment retains the Volume bind mount.

Filesystem destruction cannot begin before that checkpoint. A later desired
publication cannot race it because the Environment removal lock remains held
until runtime finalization.

### Filesystem deletion is bounded and descriptor-relative

Removal reuses ADR 0025's configured root and descriptor-relative safety
model, but not its aggregate Environment-directory delete call. Every helper
request operates beneath the already authorized Environment `volume_dir`,
opens path components without following symlinks, rejects mount-boundary and
ownership violations, and names only the immutable Volume key plus a durable
relative traversal state.

Before each helper call, the Controller durably records a `pending_path`
intent containing:

- operation id, Volume id, immutable key, and intent digest;
- exact relative component stack and traversal cursor;
- request ordinal and a budget of at most 128 directory-entry mutations;
- digest of the exact helper request;
- current Task/assignment attempt evidence.

The encoded component stack and cursor are at most 16 KiB. One helper call:

- runs for at most 30 seconds;
- performs at most 128 file, symlink-leaf, or empty-directory unlink
  mutations;
- never follows a symlink or crosses a descendant mount;
- fsyncs each changed directory before reporting progress;
- returns at most 768 KiB of canonical response data.

The completion checkpoint compares the pending intent, Task/assignment fence,
and response digest, then records the next cursor or `directory_absent` and
clears the pending call. It uses at most 16 comparisons and 16 mutations, at
most 32 etcd operations, and a measured request of at most 900 KiB.

On Controller or Agent disconnect, no new request is derived until the exact
pending call is redelivered or its durable completion is recovered. Reconnect
does not skip, merge, or renumber calls. `ENOENT` is success only when the
descriptor-relative evidence proves the exact intended leaf or pending child
is already absent.

One removal attempt has a six-hour deadline. Reaching it fails the attempt but
does not release the removal lock, discard the cursor, change desired state,
or invent cleanup. Generic Task retry resumes the same removal operation.
There is no cancellation or adoption path in the MVP.

The removal checkpoints are:

1. `intent_sealed`;
2. `revision_staged`;
3. `desired_published`;
4. `consumers_detached`;
5. `directory_absent`;
6. `runtime_finalized`.

### Runtime finalization does not publish desired state

After `directory_absent`, one bounded Controller transaction verifies the
same removal intent, desired revision, current Task attempt, directory
checkpoint, removal lock, pending idempotency marker, and immutable replay
target. It removes the runtime tombstone and current-operation records,
releases the removal lock, marks the current Task terminal, terminalizes the
idempotency marker, and creates its replay-retention index atomically. The
index expires no earlier than one full ADR 0021 TTL after
`runtime_finalized_at`; cleanup cannot observe a terminal marker without its
index or expire the replay target while removal state is retained.

It does not switch an Environment desired-state head, rewrite a Service,
change Backup policy selection, or create another desired revision. Those
changes already became atomic public desired state in ADR 0051 publication.
The transaction uses at most 26 comparisons and 26 mutations, at most 52 etcd
operations, and a measured request of at most 900 KiB.

The retained replay-target index survives primary desired removal and runtime
finalization through that full terminal ADR 0021 TTL. An equal replay returns
the originally stored root Task response both before and after finalization.
Primary Volume absence alone never authorizes a second removal operation.

### Backup policy and historical points

The removal revision deletes each current Backup source selection targeting
the Volume. The accepted Backup policy replacement transform is:

- retain frequency, keep, encryption, Connector, and other configuration;
- retain all source records required by historical Recovery Points;
- remove the deleted Volume's source ids from the current selected source
  list;
- advance the monotonic scheduling floor to
  `max(previous_schedule_floor, policy_now)`;
- when another active source remains, preserve the prior enabled value and
  compute any next schedule from that floor using the accepted scheduling
  rules;
- when no active source remains, set the active source list to empty, set
  `enabled=false`, remove `next_schedule`, retain the Connector choice and all
  policy configuration, and remove only the active-policy Connector reverse
  reference.

The representation therefore permits a disabled configured policy with zero
currently selected sources. Public policy replacement validation may still
require a source before enabling it.

Recovery Points normally persist until their ordinary retention or
Environment deletion. Deleting a source cannot generate a new retention or
cleanup trigger. Historical source records, point metadata, object locators,
orphan records, and their Connector reverse references remain, and they
continue to fence Connector deletion where the Backup contract requires it.
Removing the active-policy reverse reference does not remove or weaken any
historical Recovery Point or orphan reverse reference. The deletion impact and
token bind the scheduling floor, absence or value of `next_schedule`, exact
active-policy reverse-reference change, and retained historical fences, but
Volume removal performs no historical object cleanup.

### Blueprint grammar and behavior

The Compose top-level Volume key is immutable authored identity. Groundplane's
optional public slug is expressed only by `x-gp-slug`:

```yaml
volumes:
  app-data:
    x-gp-slug: application-data
```

`x-gp-slug` is a scalar string using the exact Volume slug grammar above.
Unknown `x-gp-*` Volume extensions are rejected. When omitted for a new
Volume, the Controller uses the Compose key only if it also satisfies the slug
grammar; otherwise validation requires `x-gp-slug`.

For an existing immutable key, changing `x-gp-slug` is a slug edit and keeps
the stable id. Changing a Compose map key is not a rename. It would imply a
destructive remove plus create and is rejected; the operator must explicitly
remove the old Volume with the deletion-impact contract and add the new one.

Service mount sources resolve the authored key to the stable Volume id in the
normalized projection. Durable references never resolve through the mutable
slug.

Blueprint omission does not destroy Volume data. An existing Volume omitted
from a Blueprint remains desired, or apply fails if the complete normalized
projection cannot preserve it without ambiguity. Destruction is available
only through the explicit Volume remove capability and its confirmation.

This clause supersedes ADR 0046 only where ADR 0046 resolves a Blueprint
Volume through a mutable Volume name. Immutable Compose-key resolution and
stable-id durable references replace that rule. All other ADR 0046 decisions
remain unchanged.

### REST, CLI, and Console clean replacement

The REST surface is:

| Capability | Endpoint | Result |
| --- | --- | --- |
| list | `GET /volumes?environment=<id>` | page of `Volume` |
| detail | `GET /volumes/{id}` | `Volume` |
| add | `POST /volumes` | `201 {volume,task_id}` |
| edit slug | `PATCH /volumes/{id}` | `200 {volume,task_id}` |
| deletion impact | `GET /volumes/{id}/deletion-impact?cursor=...&limit=...` | bounded fixed-revision page; token on final page |
| remove | `DELETE /volumes/{id}?impact_token=...&confirm_key=...` | `202 {task_id}` |

`Volume` contains `id`, `environment_id`, `slug`, `key`, derived `path`,
runtime `state`, `create_task_id`, `origin_task_id`, and `current_task_id` as
applicable. It has no `name`, editable path, aggregate size guess, or denormalized
Service-name list.

The CLI remains:

```text
gp volume list
gp volume show <slug|--id id>
gp volume add --slug <slug> [--key <compose-key>]
gp volume edit <slug|--id id> --slug <new-slug>
gp volume remove <slug|--id id>
```

All commands use the ordinary Environment scope flag. Interactive `remove`
follows every fixed-revision impact cursor to `complete=true`, rejects a page
sequence whose revision, count, or rolling digest does not join exactly,
aggregates all pages, and only then prints every mount and Backup/historical
consequence, the immutable key, and the recursive data-destruction warning. It
requires the operator to type that key before sending the final-page token and
confirmation. A non-interactive invocation must provide explicit
`--impact-token` and `--confirm-key`; there is no `--force` shortcut.

The Console has exactly one list/detail presentation and one add, edit, and
remove action matching those endpoints. The remove dialog loads every
fixed-revision impact page immediately before confirmation, validates and
aggregates the complete sequence, and does not enable confirmation until the
final-page token is present and all consequences are shown. It requires the
immutable key, dispatches the Task, and displays the six durable removal
checkpoints. It never rewrites mounts or Backup sources optimistically in the
browser store.

### Project and Environment deletion interaction

Generic Project delete applies only to tenant-kind Projects. Its Environment
cascade uses the Environment deletion contract and ADR 0025, which owns final
Environment-directory removal after child desired/runtime cleanup.

Backing Projects are not accepted by generic Project delete. Their ownership,
consumer fencing, and lifecycle remain the ADR 0037 contract. Volume removal
does not invent a second backing-Project deletion path.

Environment deletion may consume the same bounded Volume traversal primitive,
but Environment deletion remains the aggregate authority and does not call the
public per-Volume DELETE endpoint or require an operator impact token for each
child.

### Errors

The public contract uses existing typed errors:

- `validation.failed` for invalid slug, key, body, token encoding, or missing
  confirmation;
- `volume.not_found` for an absent finalized Volume;
- `name.conflict` for a scoped slug or immutable-key conflict;
- `state.conflict` for non-editable runtime state, changed impact, active
  Environment operation, materialization writer, stale desired head, or wrong
  immutable-key confirmation;
- `resource.in_use` only when a reference cannot be represented by the
  accepted recursive removal transform;
- `idempotency.in_progress` and `idempotency.mismatch` under ADR 0021, except
  that an equal accepted Volume DELETE uses the operation-scoped replay rule
  and never returns `idempotency.in_progress`;
- `storage.unavailable`, `request.failed`, and `internal` for their existing
  infrastructure meanings.

Errors never expose host paths beyond the already public derived Volume path,
private sibling names, traversal stacks, object locators, helper stderr, or
Backup credentials.

## Consequences

- Operators may rename a Volume slug without moving data or rewriting stable
  references.
- Immutable Compose keys keep Blueprint rendering and host paths reproducible.
- ADR 0051 remains the only desired-state publication and Environment head
  authority.
- Lifecycle states and deletion tombstones are explicitly runtime/task read
  projections, not a second desired store.
- Volume removal can destroy non-empty data, but only after a revision-bound
  impact preview and immutable-key confirmation.
- Removal is resumable across Controller, Agent, helper, and network crashes
  without an unbounded recursive call.
- Last-source removal disables Backup scheduling while retaining policy
  configuration and historical Recovery Point authority.
- The MVP deliberately has no legacy `name` compatibility or migration path.

## Acceptance criteria

1. No public or durable Volume contract overloads `name`; slug, key, id, and
   derived path have exactly the meanings defined here.
2. Every add, edit, remove, and Blueprint Volume change publishes through the
   sole ADR 0051 Environment head and normalized revision.
3. No `desired_visibility_head`, parallel desired generation, flat-record
   desired mutation, compatibility decoder, or migration scheme exists.
4. `creating`, `active`, `create_failed`, and `deleting` are runtime/task
   projections and are never normalized desired fields.
5. Create publication atomically exposes desired identity, Task, runtime
   projection, and replayable response; pre-existing leaves are never adopted.
6. Service mounts and Backup targets retain stable Volume ids across slug
   edits, while Compose render and the host path retain the immutable key.
7. Deletion impact is a complete fixed-revision sequence of bounded pages; only
   its final page carries the token, Console and CLI show the aggregate before
   confirmation, and changed shown consequences cannot pass DELETE.
8. Removal staging uses the deterministic largest prefix of at most 44 rows,
   90 operations, 826 conservative KiB, and 900 measured KiB; checkpoint and
   ADR 0051 publication transactions retain their stated independent bounds.
9. Dispatch proves the materialization writer absent while acquiring the
   removal lock; no cancel, adopt, wait, or outcome guess is introduced.
10. Consumer detachment is proved before descriptor-relative directory
    destruction, and every helper call is bounded to 128 mutations, 30
    seconds, and a 768-KiB response.
11. Retry appends an exact successor Task, preserves operation,
    `origin_task_id`, removal evidence, desired revision, path cursor, and the
    root `202` response, and changes only current attempt ownership and
    `current_task_id`.
12. The removal idempotency marker remains pending and unexpired through all
    terminal attempts; equal DELETE always replays the immutable root response,
    and runtime finalization atomically terminalizes the marker and creates the
    full-TTL retention index without desired publication.
13. Removing the final active Backup source disables the policy, empties active
    sources, removes `next_schedule`, advances the monotonic floor with
    `policy_now`, retains Connector choice/configuration, removes only the
    active-policy Connector reverse reference, and preserves historical/orphan
    fences to their ordinary lifetime.
14. Blueprint `x-gp-slug`, immutable Compose-key resolution, stable-id mount
    projection, explicit destructive removal, and no key rename behave
    identically through API, CLI, and Console.
15. Generic Project delete rejects backing Projects; ADR 0037 remains their
    lifecycle authority.

## Rejected alternatives

### Rename the physical directory with the public label

Rejected because live bind mounts, crash recovery, and partial rename outcomes
would make a label edit a data-moving operation.

### Keep `name` and document that it sometimes means slug and sometimes key

Rejected because it preserves the ambiguity in durable references and public
surfaces.

### Store mutable slugs in Service mounts and Backup sources

Rejected because every rename would require a cross-resource rewrite and
would make historical records unstable.

### Publish a Volume-specific desired head after filesystem deletion

Rejected because ADR 0051 is the sole Environment desired-state authority.
The accepted desired removal is published once through ADR 0051; subsequent
checkpoints are runtime/task state only.

### Delete every referencing Service in one transaction

Rejected because the consumer set is not bounded by the etcd transaction
budget and because Services are not independent desired authority under ADR
0051.

### Let the helper recursively remove the whole tree in one call

Rejected because request deadlines, disconnects, and unbounded directory size
would leave no durable progress or exact replay point.

### Refuse non-empty Volumes unless `--force` is supplied

Rejected because `force` does not prove which mounts, policy changes, and
historical consequences the operator reviewed. The revision-bound impact and
immutable-key confirmation are the destructive authority.

### Delete historical Recovery Points when their Volume is deleted

Rejected because current source deletion cannot create a new retention policy
or safely discard Connector/object cleanup authority.

### Migrate overloaded MVP `name` records

Rejected for this clean-start MVP. Adding a compatibility or migration path
would retain the ambiguous authority the clean replacement removes.
