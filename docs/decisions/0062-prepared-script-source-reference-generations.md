# ADR 0062: Prepared Script source reference generations

- Status: Accepted
- Date: 2026-08-30
- Capability: C09 Scripts
- Amends: ADR 0040 atomic reference publication and close only

## Context

ADR 0040 requires every manual Script and lifecycle hook to retain its exact
immutable runner inputs until execution cleanup is proven and retry is no
longer possible. It also says one Task-publication transaction creates every
forward and reverse source reference and every count, and that one close
transaction removes them.

Those two bulk-transaction statements cannot satisfy the rest of the accepted
contract. One release may select 16 executions. Each execution can reference a
body, snapshot, Service, Release, Networks, Volumes, Entry value generations,
reusable Secret values, and materialization generations. Expanding those
memberships can exceed the store transaction ceiling even while the sealed plan
remains below ADR 0040's accepted 4 MiB bound. Lowering the legal runner-source
set would add a new operator-visible limit and make an otherwise valid 16-hook
release invalid.

The current partial implementation also has no complete authority to close:
only Script bodies have forward and reverse records, only the Script primary
has an aggregate count, and generic Task pruning cannot own source release.
Entry removal deletes every value generation, Secret removal deletes its sole
ciphertext, and Zone, Volume, Service, Release, and hierarchy deletion do not
share one Script-source fence.

## Decision

### Narrow amendment

This ADR supersedes only ADR 0040's statements that all source memberships and
counts are created in the Task-publication transaction and removed in the
Task-terminal transaction. It retains ADR 0040's authored grammar, public
actions, selected hook order, runner projection, fixed-revision reads, source
semantics, checkpoint machine, retry barrier, timeout, cleanup, and absence
proof.

Source reservation and release are private phases of the same operation. They
create no operator action, Task type, public state, API route, CLI command, or
Console control.

### Closed Script source identity

`ScriptSourceIdentity` is a closed union with exactly these MVP members:

| Kind | Canonical identity | Retained authority |
| --- | --- | --- |
| `body` | Environment id, Script-set generation, Script id, numeric body generation | immutable body generation |
| `runner_snapshot` | snapshot id | immutable resolved runner snapshot |
| `service` | Service id | removal fence; definition bytes remain sealed in the plan |
| `release` | Release id | immutable Release intent, render input, image, and retention authority |
| `network` | Network id | immutable Zone record and physical Network removal fence |
| `volume` | Volume id | stable managed Volume identity and physical mount-source removal fence |
| `entry_value` | Entry id and `cfg_` value-generation id | immutable plain or encrypted Entry bytes |
| `secret_value` | Secret id and value-generation id | immutable reusable Secret ciphertext |
| `materialization` | materialization id and render generation | generation-matched content, ownership, or absence proof |

The union has no arbitrary kind, stringly typed fallback, host path, Docker
name, mutable label, or generic resource member. Arrays use kind first and then
the canonical identity bytes for ordering.

Dynamic non-id key segments use `~` plus unpadded base64url of the raw bytes,
matching the repository's existing dynamic-segment rule. Numeric generations
are canonical unsigned base-10 without leading zeroes. The source suffixes are
exactly:

```text
body/{environment_id}/{~script_set_generation}/{script_id}/{generation}
runner-snapshot/{snapshot_id}
service/{service_id}
release/{release_id}
network/{network_id}
volume/{volume_id}
entry-value/{entry_id}/{value_generation_id}
secret-value/{secret_id}/{value_generation_id}
materialization/{materialization_id}/{render_generation}
```

### Exact keys and envelopes

Preparation uses one descriptor:

```text
/v1/staging/script-operation-source-sets/{operation_id}
```

The active or releasing reverse root is:

```text
/v1/records/script-operation-source-sets/{operation_id}/root
```

Every selected execution/source pair has one reverse membership:

```text
/v1/records/script-operation-source-sets/{operation_id}/executions/
  {script_execution_id}/{source_suffix}
```

The matching forward membership and source count are:

```text
/v1/runtime/script-source-references/{source_suffix}/
  {operation_id}/{script_execution_id}
/v1/runtime/script-source-counts/{source_suffix}
```

Line wrapping above is explanatory; keys contain no whitespace.

`ScriptSourceReference` is the identical envelope stored at the forward and
reverse keys:

```text
schema = 1
operation_id
script_execution_id
source: ScriptSourceIdentity
source_owner_id
source_mod_revision
source_digest
```

`source_owner_id` is the stable resource owner needed by deletion. It is the
consumer Environment for bodies, snapshots, Services, Volumes, Entries, and
materializations; the Release's Environment for Releases; the actual Zone
Environment for Networks; and the Project id or the literal platform owner for
reusable Secrets. `source_mod_revision` is the exact fixed-revision evidence
used during reservation. `source_digest` is the immutable source digest when
that source has one and is empty only for a closed kind whose retained authority
has no digest.

Preparation evidence is one closed `existing | staged` union. Existing
evidence uses exactly the positive source `ModRevision`. Staged evidence is
limited to Blueprint-publishable runner snapshots, Services, Releases,
Networks, Volumes, and Entry values and carries the exact candidate
Environment id, desired revision id, render generation, positive fixed read
revision, and SHA-256 of the canonical source value. All staged members of one
preparation name the same candidate. Activation validates the candidate
identity and requires one byte-identical final put for every staged source.
Reusable Secret values and materialization proofs cannot use staged evidence.

`ScriptSourceCount` is:

```text
schema = 1
source: ScriptSourceIdentity
referenced_execution_count
```

The count unit is one selected execution/source pair. Repeated mounts of one
Volume inside one execution count once; the snapshot retains every rendered
target. Two executions using the same source count twice. A count key is absent
if and only if its count is zero and its forward prefix is empty.

`ScriptOperationSourceRoot` is:

```text
schema = 1
operation_id
membership_count
membership_sha256
phase = active | releasing
release_path = absent | normal_completion | retry_expiry
release_cursor
```

`release_path` is `absent` while the root is active. The transaction that
changes the root to `releasing` sets it exactly once. `normal_completion`
means the current Task is still nonterminal and its terminal state and
retain-until index belong to the final release transaction. `retry_expiry`
means the retry-available Task is already terminal and already has its
retain-until index; reference release must not rewrite either one.

`ScriptSourcePreparation` uses the same operation id, membership count, and
digest plus `phase = preparing | sealed | abandoning`, a preparation cursor,
and a release cursor. The digest covers the canonical sorted sequence of every
exact `ScriptSourceReference` envelope.

### Preparation and publication

The Controller derives the complete source set from the same fixed MVCC read
used to build the sealed plan. Caller-supplied Entry bindings are not authority;
Entry bindings, reusable Secret selections, and materialization generations
must be results of that fixed-revision source resolution.

Preparation writes at most 16 memberships per transaction. Each transaction:

- compares the preparation descriptor and each pinned source revision;
- requires the forward and reverse membership keys to be absent;
- writes byte-identical forward and reverse envelopes;
- increments each exact source count by one; and
- advances the preparation cursor.

The Script primary's existing `active_references` remains the Script-wide
aggregate and is incremented for each body membership. It must equal the sum of
that Script's body-generation counts. A prepared membership is already a
removal fence even though no Task is visible.

After every expected membership is reserved, one transaction seals the
descriptor only when the cursor, count, and canonical digest match. Task
publication then compares that sealed descriptor and the ordinary hierarchy,
idempotency, operation, plan, and current-pointer fences; creates the Task,
operation, executions, snapshots, and sealed plan authority; writes the active
reverse root; and deletes the preparation descriptor atomically. It does not
rewrite every source membership or count.

For final Environment Blueprint publication, those operations participate in
ADR 0051's one dedicated final-publication envelope. They do not use an
ordinary `Store.Transact` budget and do not gain a Script-specific exception.
The exact Script accounting is:

| Blueprint shape | Comparisons | Success | Failure |
| --- | ---: | ---: | ---: |
| QA: eleven Release candidates, two hooks, three staged physical sources | 31 | 43 | 31 |
| maximum non-Backup Script | 45 | 99 | 45 |
| maximum non-Backup Script plus two candidate Attaches | 86 | 132 | 86 |
| combined maximum Backup plus Script | 143 | 160 | 143 |

The QA shape has 74 operations on its selected success path, 62 on its
selected failure path, and 105 in the full logical request. The combined
maximum has 303, 286, and 446 respectively. ADR 0051's previously documented
Backup-only maximum remains `120/74/120`; it is an accounting scenario, not a
separate envelope.

Every comparison, success mutation, and failure read is distinct. Removing or
coalescing any of them breaks desired-head, Task, marker, Release, Script,
physical-source, Attach, or Backup authority. The final builder therefore
accepts each arm through 256 operations, rejects 257, and rejects a
protobuf-encoded request above 1 MiB before etcd with the existing
`validation.failed`/HTTP 422 response. The 512 selected-arm and 768 full logical
maxima are diagnostic sums only, never rejection limits. A fitting compare
failure remains one atomic conflict at its transaction revision.

Required focused proof covers the exact `31/43/31`, `45/99/45`, `86/132/86`,
and `143/160/143` shapes, 256-arm acceptance, 257-arm rejection, over-1-MiB
rejection, and the unchanged 96-selected-operation protection for ordinary
non-Blueprint `Store.Transact`. Preparation, sealing, release, abandonment, and
other non-final-publication limits in this ADR remain unchanged.

No assignment or `start_authorized` transition is legal without the active
reverse root matching the execution plan's membership digest. A failed or
abandoned preparation creates no Task. Its descriptor moves to `abandoning`
and releases the exact prepared prefix through the same bounded release rules
before disappearing. A crash resumes from the descriptor cursor; it never
guesses from a partial prefix.

### Retry, close, and bounded release

A permitted retry retains the operation id, execution ids, source root,
memberships, and counts unchanged. It transfers only current Task and assignment
ownership under ADR 0040.

References remain active for `retry_disposition=available`. Retry expiry first
performs ADR 0040's fenced `expiry_before_start` and `cleanup_proven`
transitions. Generic Task pruning is blocked while the active or releasing
source root exists.

There are exactly two completion paths for an active operation:

- **Normal nonterminal completion.** After every selected execution is
  `cleanup_proven` and retry is forbidden or abandoned, one transaction sets
  the operation's closing state and changes the source root to `releasing`
  with `release_path=normal_completion`. It compares the current Task as
  nonterminal. That transition permanently forbids assignment, start, and
  retry, and the Task remains nonterminal until the final release transaction.
- **Retry expiry.** A retry-eligible failure has already terminalized its Task,
  published that Task's retain-until index, left the operation open with
  `retry_disposition=available`, and retained every reference. At or after the
  deadline, the retention collector first advances every still-`not_started`
  execution through `expiry_before_start` and `cleanup_proven`. One transaction
  then sets `retry_disposition=expired`, sets the operation's closing state,
  and changes the source root to `releasing` with
  `release_path=retry_expiry`. It compares the already-terminal Task and its
  existing retain-until index byte-for-byte. The Task and index remain
  unchanged throughout release.

Release removes at most 16 memberships per transaction. Each transaction
compares the releasing root including `release_path`, operation, path-specific
Task fence, exact byte-identical forward and reverse membership, source count,
and Script aggregate when applicable; deletes both memberships; decrements the
count and deletes a zero count; and advances the release cursor. The normal
path compares the Task as nonterminal. The retry-expiry path compares the
already-terminal Task and existing retain-until index without writing either.
Missing membership, mismatch, underflow, overflow, or a Task, index,
disposition, or release-path state inconsistent with the selected path stops
release as Internal corruption. No partial transaction commits.

Only after the exact membership count is drained, every forward and reverse
prefix is proven empty, and every count mutation is complete may the final
transaction complete the selected path. For `normal_completion`, it deletes
the source root, closes the operation, terminalizes the Task, and publishes
that Task's retain-until index atomically. For `retry_expiry`, it deletes the
source root and closes the operation atomically while comparing, but neither
re-terminalizing, rewriting, nor republishing, the already-terminal Task or its
existing retain-until index. Removing the root unblocks ADR 0035 Task pruning,
which then uses its existing terminal Task and retention index authority.
Thus a source becomes removable only after the runner is absent, cleanup is
proven, and retry is impossible, even though bulk membership deletion no
longer shares the Task-terminal transaction.

A crash resumes the stored `release_path` and cursor; it never reselects a
path from current time or Task presentation. A replayed chunk reloads the
cursor and applies only the next still-present exact memberships, so a
committed decrement cannot repeat. A replayed finalization is success only
when the root is absent, the operation is already closed, and the selected
path's Task and retain-until-index postconditions match exactly. It never
recreates the root or republishes retention. Any other absent root, closed
operation, cursor, Task, index, or path combination is Internal corruption and
retains the remaining evidence.

### Edit, removal, hierarchy deletion, and pruning fences

An edit may publish a new immutable body, Entry, materialization, or future
Secret generation while an older generation is referenced. It never overwrites
or prunes the referenced generation.

Removal of a referenced Script, Service, Release, Network, Volume, Entry,
reusable Secret, or materialization returns `resource.in_use`. This includes
prepared, active, retry-open, and not-yet-drained releasing memberships.
Desired late-bound Secret references remain non-blocking; only an exact prepared
or active Script execution reference blocks reusable Secret removal.

Every source finalizer compares its exact count key as absent and its forward
prefix as empty in the same transaction that removes the source. The count is
the fast aggregate fence; prefix emptiness is the absence proof. One without
the other is corruption, not permission to remove.

Tenant, Project, and Environment deletion include these exact fences in their
fixed-revision membership proof. They must not prefix-delete Entry generations,
Script generations, Secret ciphertext, or source records while any Script
count remains. Network and Volume host cleanup is likewise forbidden while
referenced. Parent-last deletion remains visible and retryable rather than
omitting a blocked child.

Task journal pruning never decrements a Script source count, deletes a source
membership, or infers operation closure. It starts only after the source root
is absent. Script preparation abandonment, operation close, and retry expiry
are the only reference-release authorities.

### MVP source-family authority

- A Network source records the Zone's actual owning Environment. A consumer
  Script may therefore use a backing-owned external Zone; it is not required
  to be owned by the Script's Environment.
- A Volume source is the stable `vol_` id. Its capture revision is the applied
  Environment projection revision because the MVP has no second Volume
  primary. The snapshot retains the exact rendered mounts, and the Volume
  count fences physical removal.
- A Release source owns its immutable render input, image identity, and
  retention children as one aggregate. No separate render-input reference kind
  exists.
- Every Entry source uses its existing immutable `cfg_` value generation.
- Reusable Secret rotation is deferred in the MVP, so the create-only Secret's
  stable `sec_` id is also its sole immutable value-generation id. The Secret
  id occupies both segments of the `secret-value` suffix. A later rotation
  contract must add a distinct generation without changing historical keys.
- A materialization source is a durable generation-matched proof record, not a
  Task-owned materialization input and not an observation of a current file.

### Ownership and corruption

One Controller Script-reference module owns preparation, source membership,
counts, active roots, abandonment, and release. Source capability modules may
compare its fences but never decrement, repair, or synthesize them. The Agent,
Task pruner, hierarchy deletion engine, adapters, Components, API, CLI, and
Console have no mutation authority over this keyspace.

Missing or extra membership, unequal forward and reverse bytes, an incorrect
source tuple, digest mismatch, count mismatch, underflow, overflow, an active
root without its operation, or an operation without its active root is Internal
corruption. The Controller fails closed, retains all evidence, and performs no
compatibility scan or best-effort repair.

## Alternatives considered

### One bounded publication and close transaction

Rejected. It requires a new expanded-source cardinality limit below otherwise
accepted hook and plan bounds. That would be an operator-visible product
restriction introduced only to fit persistence mechanics.

### Count-only records without symmetric memberships

Rejected. A count cannot prove which operation owns a source, cannot be
reconstructed safely after a crash, and cannot detect a mismatched decrement.

### Task retention releases references

Rejected. Task attempts are not the stable operation, retries preserve the
same source set, and ADR 0035 intentionally does not make journal pruning a
resource-lifecycle authority.

## Safe deferrals

Scheduled Scripts, human Script output, reusable Secret rotation, arbitrary
host binds, non-Volume mount-source kinds, a generic cross-capability reference
framework, orphan repair, and count sharding remain post-MVP.

Source capture, symmetric membership, count authority, removal fences,
backing-owned Networks, materialization proof generations, retry-expiry release,
hierarchy deletion integration, and Task-pruning fences are not deferrable.

## Consequences

- Existing legal manual and lifecycle-hook plans keep their accepted bounds.
- Task publication remains one atomic visibility point, while private source
  reservation and release become bounded crash-safe phases.
- Sources cannot disappear during execution, recovery, or retry-open retention.
- Resource deletion may return the existing `resource.in_use` error for an
  exact Script operation reference; there is no new public action or state.
- ADR 0040 remains the Script execution contract except for the two bulk
  reference-transaction statements superseded here.
