# Backup recovery and execution boundaries

- Decision status: Accepted; implementation and qualification remain partial
- Scope: Backup authority, artifact publication, restore safety, Agent recovery,
  and the managed PostgreSQL execution boundary

## Recovery is driven by a sealed snapshot

Backup schedules use one five-field UTC cron format across authored state and
human interfaces. [The schedule owner](../../internal/common/backupschedule)
parses fields and finds the latest missed occurrence directly, rather than
replaying every missed minute. Controller coordination still prevents overlap
and repeated dispatch. Replacing the old stored calendar grammar advances the
storage epoch: an old Controller cannot safely read or roll back new policies.
No dual parser or old-format alias is retained.

Each Environment has at most one Backup Policy. A run freezes the policy,
Connector, ordered source catalog, relevant desired revisions, prior Service
intent, encryption era, deadlines, object targets, and restore dependencies
before work is assigned. Retry resumes that snapshot; it does not rebuild a
plan from newer desired state or mutable labels. Environment coordination owns
the scheduling floor and serializes Backup work with release, storage,
configuration, Attach, and deletion mutations.

The Controller remains the authority for locks, checkpoint acceptance, point
commit, adoption, retention, and terminal delivery. The Agent may mutate only
from one authenticated assignment and its exact resume state. Every
irreversible boundary is write-before-mutation and Controller-acknowledged.
Lost acknowledgement is resolved by the same immutable identity; it is never a
reason to infer success or repeat a destructive action.

A Recovery Point becomes visible only after the object upload, immutable
object identity, size, hashes, required metadata, and source cleanup evidence
have committed. Retention deletes in stable order and records remote absence
before removing local ownership. Environment deletion may adopt retained
prune or remote-cleanup obligations, but never abandons an object merely
because the Environment primary is gone. Age-key rotation affects future
points; an old identity is request-scoped, never stored, and loss or ambiguity
requires another explicit operator request.

## Artifacts are validated before target mutation

Recovery Point grouping uses a capture-metadata companion published atomically
with each Point. It survives producing-Task retention and is removed with the
Point. Presentation metadata is not part of the immutable Restore/prune snapshot:
adding grouping must not change an existing assignment's authority or require
rewriting Point history. [The capture owner](../../internal/infra/etcd/backupruntime/recovery_point_capture.go)
and [publication](../../internal/infra/etcd/backup_recovery_point_commit.go) enforce
the same-revision binding. A missing companion means unrecorded provenance;
timestamps are never used as a substitute identity.

The accepted source formats are the canonical Environment Config archive, the
canonical managed-Volume archive, PostgreSQL 16 `pg_dump` custom format and
MySQL 8.4 logical SQL format.
Optional age encryption wraps the source stream without adding another
compression layer. Object metadata and Recovery Point evidence bind both the
plaintext and stored representations. The current format owners are linked
below; their encodings and validation rules are not copied here.

Config capture preserves the exact desired Entry descriptors and selected
immutable values from one linearizable snapshot. Restore fully downloads,
decrypts, hashes, and validates the archive before staging a new complete Entry
generation. Publication replaces the whole set. Once the first publication
mutation is acknowledged, recovery rolls forward from the durable generation
and never restarts the restore from scratch.

The Restore Task seals the Recovery Point's complete archive evidence and one
Controller-allocated Entry/render generation. Download cannot select a newer
object, and reconnect cannot invent another generation. The Task also pins the
predecessor desired revision and head revision: restored configuration must not
be rebuilt against later Environment edits. Selected values first enter immutable
Entry generations with native receipts; replay verifies the retained ciphertext,
not a freshly sealed replacement. The extra private
validation copy uses an unlinked file charged to the same bounded staging lease;
it must not bypass capacity admission through an ambient temporary directory.
Capture and Restore share one Config transfer owner. Restore grants credit at
completed Entries so authenticated readers can advance only after durable
acceptance, without waiting on a window that requires the next Entry to fill it.
Controller restore staging protects every received frame, including metadata
and value hashes, with its existing key. A batch and its native credit commit
together; an uncertain write cannot grant more input or select new ciphertext.
The final grant follows a durable complete-generation seal. Live publication
uses that sealed input, not an acknowledged prefix or current Secret values.

The sealed predecessor also identifies managed file destinations and surviving
Services. The archive selects the replacement content; the predecessor authorizes
removal of files no longer selected. Both sides use the same dotenv encoder and
file-plan owner. Before live publication, the Controller computes the expected
file proof from the staged immutable values. The Agent writes those files through
the ordinary descriptor-confined helper, then independently verifies every output
and removal before reporting the matching proof. This updates managed files, not
the environment already loaded by running processes.

File verification and source cleanup are separate acknowledgements. The Agent
closes its private validation spool and removes the exact source stage before
acknowledging cleanup; only then can it retire the transfer journal. Startup may
discard an interrupted download before archive validation, resume the exact full
representations after validation, or finish cleanup after acknowledged file
verification. None of these dispositions permits selecting a newer object or
reapplying files after cleanup. Admission and terminal recovery must be connected
before this path is operationally available.

Volume archives represent a descriptor-confined tree containing only
directories and regular files. Mount crossings, links, devices, sockets,
special files, unsafe paths, metadata outside the supported set, and tree
changes during capture fail closed. Restore first stops the Services whose
sealed prior intent requires it, constructs and validates a hidden sibling,
journals each construction/finalization mutation, exchanges it with the live
tree atomically, then removes the replaced tree. After exchange, recovery is
forward-only and Service recovery is resumed from its cursor.

PostgreSQL capture is one continuous `pg_dump --format=custom --compress=0`
stream. Restore validates a list pass before stopping consumers, terminating
connections to the selected database, and applying one clean,
single-transaction `pg_restore`. It does not capture a cluster, WAL, roles, or
ACLs. Once restore-apply start is acknowledged, the attempt is spent: exact
input, terminal, reap, Exec inspection, and post-restore proof are required;
missing or contradictory proof is recovery-required and never authorizes a
second apply.

MySQL uses the upstream container's fixed `mysqldump`/`mysql` protocol rather
than a GP database image or operator-supplied shell command. Its schema changes
are not one atomic transaction. Recovery must inspect the original Exec and
its retained terminal/input evidence; an unknown outcome never permits another
restore-apply. [The execution owner](../../internal/infra/docker/mysql84execution)
attests image, labels and the exact data Volume around each operation.

Database archive evidence records actual server and backup-tool versions before
dump-start acknowledgement. Recovery uses those pinned versions, not fresh
probes that could relabel retained bytes. Restore preflight uses an authenticated
Agent read with the same runtime attestation. The review digest binds the Point,
Attach, consumers, exact runtime revisions, container and observed restore tools.
Version differences require explicit operator acknowledgement; execution rechecks
that target before effects. This separates operator-owned compatibility risk from
GP's non-negotiable identity, format and integrity checks.

Capture resume retains the original prepared archive alongside later upload
progress. Otherwise an acknowledged upload would erase the metadata needed to
authenticate the retained source. Unknown-size capture reserves storage as it
writes; a format maximum is not a prediction of the required disk space.

An interrupted PostgreSQL dump before preparation may resume only its original
retained bytes. The worker must match them to the original Docker Exec and
helper output evidence, then validate the archive. Incomplete bytes fail the
attempt rather than launching another dump. Before preparation, derived
ciphertext may be discarded and regenerated; after preparation it is fixed.

## Agent restart and delivery fail closed

Backup work uses closed versioned assignments with stable ids, revisions, authority
digests, assignment generations, absolute deadlines, contiguous checkpoint
sequences, and a preceding-checkpoint fence. Stable ids, not names, identify
resources. Capture and restore have a six-hour absolute budget; prune has a
thirty-minute budget. Reconnect does not extend either budget.

The single-host channel is bootstrapped over a root-only local Unix socket and
then uses the Agent token; adding TLS to that local bootstrap is not an MVP
security boundary. The authenticated process generation is distinct from the
durable Agent generation. Backup traffic uses bounded message sizes, bounded
per-assignment queues, and one fair channel writer so a Config or Volume stream
cannot starve readiness, checkpoints, abort, or terminal delivery.

S3 credentials, the current age identity, and an explicitly supplied old age
identity use separate task-scoped, chunked secret slots. Config values use the
typed Config transfer instead. Secret material is excluded from Tasks,
checkpoints, staging inventories, logs, results, and terminal receipts, and
owned buffers are cleared after use.

On startup the Agent advertises zero capacity and not-ready delivery state,
quiesces workers, inventories bounded staging state, and waits for the
Controller's exact disposition plan. It journals and applies that plan before
acknowledging it and becoming Ready. Silence is not discard authority. Any
stage that may contain an unclassified irreversible mutation blocks Ready
until it is resumed, safely retired, or declared recovery-required.

An unacknowledged plan may be superseded when native Task state changes, such
as a timeout during inspection. The replacement names the previous plan's digest
and covers the same inventory. The Agent journals the replacement; an old
acknowledgement cannot acknowledge the new decision.

A declared recovery-required disposition pins the failed Restore's immutable
native outcome and exact retained files. The Agent verifies and holds those files
with no growth, cleanup or worker authority. Unrelated work may become Ready;
the failed Environment's operation lock and recovery evidence remain protected.
Declaration is not successful recovery or permission to repeat the destructive step.

Upload intent is checkpointed before `Put`; completion is checkpointed before
the verifying `Head`; only the Controller publishes the Recovery Point.
Terminal Task results also use a durable receipt and explicit assignment
retirement handshake. An Agent never treats a disconnected stream, Task state,
or time passage as proof that terminal delivery is complete.

Session teardown quiesces Backup work; it is not an operator Abort. Redispatch
uses the retained checkpoint rather than publishing a false terminal result.
Prune credentials are resolved only for still-owned objects. An acknowledged
exact-object deletion retires that object's payload requirement, not the
remaining dispatch or its validation checks.

## Managed PostgreSQL uses a closed helper and private gate

Published releases carry an authenticated dual-platform scratch image containing
only PostgreSQL backup clients, their libraries, the helper and private gate.
The database image is unmodified upstream PostgreSQL 16 Alpine. Native `--ref`
builds publish only the host architecture's tooling; they do not fabricate an
unbuilt platform or depend on a previous published release.

The installer extracts tooling into a digest-owned directory, verifies its
inventory against the authenticated image and mounts it read-only. Clients use
their bundled loader and libraries, not those of the patched database image.
Backing creation retains the catalog. GP software updates cannot substitute new
tools for an existing Backing. Database patch Deploys preserve the same Volume
and tooling mount; verified provisioning and Deploy outcomes publish one current
PostgreSQL runtime authority consumed by Backup. See
[the record owner](../../internal/infra/etcd/backingpostgresruntime/record.go).
Container and mount attestation admits only that exact read-only tooling bind
alongside the data Volume and prevents any other workload mount
from shadowing the helper, gate, state directory, clients, socket, or runtime
identity. Container and host root are trusted boundary actors; database uid 70
is not trusted with supervisor state.

The shared protocol permits only the fixed probe, dump, restore-list,
connection-termination, restore-apply, verification, stop, execution-evidence,
recovery-inventory and evidence-retirement operations. It
has no shell, arbitrary executable or argument vector, arbitrary SQL,
password, TCP, filename, ambient environment, or diagnostic mode. A root
Docker Exec helper validates the request and launches the fixed client through
a private gate. Before client exec, the gate proves its parent and file
descriptors, then runs as uid/gid 70 with no supplementary groups, no
capabilities, `NoNewPrivs`, the selected seccomp policy, and bounded file
descriptors.

Helper state is root-only, descriptor-confined, hash-chained, fsynced, and
phase-monotonic. The original parent is the sole process allowed to reap its
child. Recovery authenticates exact process identity through pidfds and never
uses a name, negative pid, or non-parent wait as authority. Ambiguous identity,
release consumption, exec, stream, termination, or reap state retains the
record and blocks container replacement, upgrade, ordinary Backup work, and
positive Agent readiness.

Execution evidence is selected by the original nonce and canonical request
digest. It exposes only complete terminal, parent-reap and stream evidence.
Recovery also requires successful inspection of the original Docker Exec and
the selected container; reading helper state alone is not completion proof.
The resumed Restore then verifies the database and continues Service recovery,
without invoking restore-apply again.

Successful dump and restore evidence is retired after the Controller acknowledges
source cleanup. A durable retirement marker precedes deletion and survives an
interrupted cleanup. Fully reaped, stream-complete failed non-RestoreApply
operations can also be retired without being called successful. Failed RestoreApply
and ambiguous or unreaped attempts cannot be retired through that path. Absence
is not execution proof.

The Agent records a private execution marker before invoking a source's helper.
It survives source-file cleanup and successful individual sources until the
whole Task is classified. Startup inventories every helper record, including
probes and archive-list passes. A completed Task only retires its Agent marker:
its old database may already serve a newer operation. A failed Task's cleanup
must prove the retained helper namespace safe before releasing its backing lock.

Backup and Restore atomically lock the selected Backing Environments as well as
the consumer Environment. Lock acquisition advances each backing mutation epoch,
so competing publications and planners prepared before acquisition cannot cross
that boundary. Exact lock deletion releases it. Failed Tasks retain every started
PostgreSQL source's guard, including sources whose file cleanup already succeeded;
their staging acknowledgement releases those guards atomically after inspection.
This separates file cleanup from proof that no helper remains active.

The [helper runtime](../../internal/postgres16helper/main.go), private gate and
Agent execution path are implemented but unqualified. Restart reconciliation and
successful-evidence retirement are connected for retained running assignments.
Startup inventory, backing locks and execution-marker retirement are connected
in source; generated integration and runtime qualification remain outstanding.
Source implementation does not establish the security contract at runtime.
Terminal Restore uncertainty remains explicitly recovery-required,
with no automatic second attempt or new post-terminal assignment.

## Unimplemented schema-one protocol reference

This is the single owner for accepted Backup wire fields that are not yet in
the checked-in protobuf. It is deliberately a compact protocol inventory: as
each family is implemented, [`proto/agent.proto`](../../proto/agent.proto)
becomes its sole field owner and the corresponding inventory below should be
removed. The reservations and types listed here do not make Backup ready.

The outer oneof allocations are fixed:

| Payload | Agent tag | Controller tag |
| --- | ---: | ---: |
| `BackupConfigTransfer` | 32 | 32 |
| `BackupConfigCredit` | 33 | 33 |
| `BackupStagingInventory` | 34 | — |
| `BackupStagingRecoveryAck` | 35 | — |
| `BackupStagingRecoveryPlan` | — | 34 |
| `BackupVolumeManifestTransfer` | 36 | 35 |
| `BackupVolumeManifestAckCredit` | 37 | 36 |
| `TaskTerminalReceiptApplied` | 38 | — |
| `TaskTerminalAssignmentRetired` | 39 | — |
| `TaskTerminalReceiptAck` | — | 37 |
| `TaskTerminalReceiptAppliedAck` | — | 38 |
| `BackupStagingRecoveryAckReceipt` | — | 39 |
| `TaskTerminalAssignmentRetiredAck` | — | 40 |

Agent tags 32–39 and Controller tags 32–40 remain reserved until their named
payload is added. Implementing one payload releases only that tag.

The accepted additions to existing messages are:

```text
Authenticate: 3 execution_plan_schema; 4 process_generation;
  5 postgres16_managed_release_sha256
AgentConfig: clean-replace map field 3 with repeated AgentLabel{1 key;2 value}
Ready: 6 optional terminal_delivery_clean (presence required)
ObservedState: 2 snapshot_id; 3 batch_ordinal; 4 final_batch
TaskAssignment: 11 backup_authority; 12 backup_resume;
  13 backup_authority_sha256; 14 assignment_generation
TaskAck.result: 10 backup_result; TaskAck: 11 assignment_generation
TaskAbort: 4 assignment_generation; 5 plan_hash; 6 terminal;
  7 terminal_receipt_sha256; 8 durable_task_mod_revision;
  9 optional rejected_task_ack_sha256
BackupTaskResult: 1 failed_step_id; 2 recovery_required
```

Process generation is 16 bytes; protocol and digest fields are exact schema
one and 32 bytes; generations and durable revisions are positive. Agent labels
are sorted pairs with unique keys, not a map. A Backup terminal result is valid
only for the exact assignment id and generation.

The missing authority and checkpoint layouts are fixed as follows. Semicolon
notation is `tag field`; an indicated oneof is exclusive.

```text
RevisionDigest {1 mod_revision; 2 sha256}
CheckpointFence {1 authority_digest; 2 dedupe_key_mod_revision}
BackupResourceIdentity {1 kind; 2 resource_id; 3 resource}
BackupEncryptionAuthority {1 kind; 2 secret_slot_id; 3 recipient_sha256;
  4 secret_slot; 5 optional key_era}
BackupPriorRuntimeIntent {1 kind; 2 intent}
BackupServiceFact {1 service_id; 2 current_name; 3 service; 4 compose;
  5 prior_runtime_intent; 6 required_label_count;
  7 required_labels_sha256; 8 local_image_id_sha256}
BackupConnectorAuthority {1 connector_id; 2 connector; 3 canonical_endpoint_url;
  4 region; 5 path_style; 6 prefix; 7 access_key_slot_id;
  8 secret_key_slot_id; 9 access_key_slot; 10 secret_key_slot}
BackupObjectTarget {1 connector; 2 bucket; 3 object_key}
BackupS3VersionId {1 value}
BackupS3ETag {1 value}
BackupObjectIdentity {1 connector; 2 bucket; 3 object_key;
  oneof 4 version_id,5 etag}
BackupArtifactEvidence {1 source_size_bytes; 2 source_sha256;
  3 stored_size_bytes; 4 stored_sha256}

BackupTaskAuthority {1 authority_schema; 2 task_id; 3 operation_id;
  4 task_attempt; 5 plan_hash; 6 project_id; 7 project; 8 environment_id;
  9 environment; 10 task_deadline_unix_nano; 11 assignment_id;
  12 assignment_generation; 13 services; 14 steps}
BackupStepAuthority {1 step_id; 2 execution_id; 3 step_digest;
  4 step_deadline_unix_nano; 5 consumer_service_ids;
  oneof 10 capture,11 prune,12 restore}
BackupTaskResume {1 assignment_id; 2 assignment_generation; 3 steps}
BackupStepResume {1 step_id; 2 execution_id;
  oneof 10 capture,11 prune,12 restore}
BackupCaptureResume {1 checkpoint_sequence; 2 preceding_checkpoint; 3 phase;
  4 cursor; oneof 10 artifact_prepared,11 upload_verified,
  12 source_cleanup_completed,13 postgres_container_observed,
  14 postgres_dump_start,15 volume_progress,16 config_progress,
  17 upload_completed}
BackupPruneResume {1 checkpoint_sequence; 2 preceding_checkpoint;
  3 next_object_ordinal; oneof 10 object_deleted}
BackupRestoreResume {1 checkpoint_sequence; 2 preceding_checkpoint; 3 phase;
  4 cursor; oneof 10 artifact_validated,11 postgres_container_observed,
  12 postgres_restore_apply_start,13 config_progress,14 volume_progress,
  15 postgres_restore_verified,16 postgres_service_progress}
BackupCaptureCursor {1 object_attempt; 2 config_record_sequence;
  3 config_value_ordinal; 4 service_cursor; 5 cumulative_chain_sha256}
BackupRestoreCursor {1 object_attempt; 2 config_record_sequence;
  3 config_value_ordinal; 4 volume_cursor; 5 service_cursor;
  6 cumulative_chain_sha256}

BackupCheckpointRequest {1 task_id; 2 step_id; 3 execution_id;
  4 checkpoint_sequence; 5 preceding_checkpoint; 6 authority_digest;
  7 assignment_id; oneof 20 artifact_prepared,21 upload_verified,
  22 source_cleanup_completed,23 postgres_container_observed,
  24 postgres_dump_start,25 postgres_restore_apply_start,
  26 restore_artifact_validated,27 config,28 volume,
  29 prune_object_deleted,30 postgres_restore_verified,
  31 postgres_service_progress,32 upload_completed}
BackupCheckpointAck {1 task_id; 2 step_id; 3 execution_id;
  4 checkpoint_sequence; 5 committed; 6 assignment_id}
```

The old checkpoint `sequence`, `kind`, and `control_payload_sha256` meanings are
clean-replaced and reserved by name. Capture phases are capturing, artifact
prepared, uploading, head verification, point commit, source cleanup, Service
recovery, terminal. Restore phases are artifact validation, target preparation,
target mutation, publication, cleanup, Service recovery, terminal.

Capture, restore, and prune authority use these layouts:

```text
BackupCaptureAuthority {1 point_id; 2 resource; 3 target; 4 encryption;
  reserved 5 services; oneof 10 postgres,11 config,12 volume}
BackupRestoreAuthority {1 point_id; 2 destination; 3 source_object;
  4 expected_evidence; 5 encryption; reserved 6 services;
  oneof 10 postgres,11 config,12 volume;
  oneof 20 original_identity,21 adopted_identity}
BackupPruneAuthority {1 retention_policy; 2 objects}
BackupOriginalIdentity {1 original_execution_id; 2 destination;
  3 original_assignment_id}
BackupAdoptedIdentity {1 adoption_id; 2 predecessor_execution_id;
  3 predecessor_fence; 4 adoption_mod_revision;
  5 predecessor_assignment_id; 6 adopted_assignment_id}
BackupPruneObject {1 ordinal; 2 point_id; 3 point; 4 evidence; 5 object;
  6 metadata_count; 7 metadata_sha256}
BackupPruneObjectDeleted {1 ordinal; 2 point_id; 3 object}
```

Staging recovery uses:

```text
BackupStagingInventory {1 entries}
BackupRecoveredStage {1 recovery_key_sha256; 2 files}
BackupRecoveredFile {1 role; 2 size_bytes; 3 sha256}
BackupStagingRecoveryPlan {1 inventory_sha256; 2 dispositions}
BackupStagingDisposition {1 recovery_key_sha256;
  oneof 2 resume_prepared,3 discard_recovered}
BackupResumePrepared {1 assignment_resume_sha256; 2 remaining_growth;
  3 required_growth_bytes; 4 expected_files; 5 restart_config_encryption}
BackupRestartConfigEncryption {}
BackupDiscardRecovered {}
BackupStagingRecoveryAck {1 inventory_sha256; 2 applied_plan_sha256;
  3 applied_disposition_count}
```

Config encryption may restart only before the durable ArtifactPrepared receipt
has selected ciphertext and authorized upload. The Controller must independently
verify the complete retained source against the sealed snapshot and confirm
completed transfer with no Prepared receipt. Its persisted startup disposition
then retires only the exact derived ciphertext; the source and transfer journal
remain. Replaying that same disposition accepts already-removed ciphertext,
not missing source or changed files. After Prepared, reuse and authenticate the
selected ciphertext; never generate replacement bytes or issue a second Put.

Inventory is bounded to 32 stages in recovery-key order, each with one or two
unique role-ordered files. The plan contains each key exactly once and repeats
the inventoried file evidence byte-for-byte. File roles are source-plaintext
and stored-object. Remaining growth is no-growth, bounded with a positive byte
count, or exclusive-unknown.

The Config and Volume streams keep their exact framing fields:

```text
BackupConfigTransfer {1 task_id; 2 step_id; 3 execution_id; 4 transfer_id;
  5 record_sequence; 6 assignment_id;
  oneof 10 start,11 entry_header,12 value_chunk,13 entry_end,14 end,15 resume}
BackupConfigTransferStart {1 direction; 2 content}
BackupConfigEntryHeader {oneof 1 capture_entry,2 restore_entry; 3 chunk_count}
BackupConfigValueChunk {1 ordinal; 2 offset; 3 content}
BackupConfigEntryEnd {1 ordinal; 2 value_size_bytes; 3 value_sha256}
BackupConfigTransferEnd {1 direction; 2 content; 3 transcript_sha256}
BackupConfigTransferResume {3 next_ordinal; 6 value_chain_sha256;
  reserved 1 direction,2 next_record_sequence,4 metadata_accepted,
  5 metadata_chain_sha256,7 prior_ack_sequence,
  8 prior_committed_record_sequence,9 prior_ack_sha256}
BackupConfigCredit {1 task_id; 2 step_id; 3 execution_id; 4 transfer_id;
  5 direction; 6 credit_sequence; 7 assignment_id;
  oneof 10 metadata_accepted,11 value_credit,16 metadata_credit;
  12 committed_record_sequence; 13 next_ordinal;
  14 cumulative_chain_sha256; 15 ack_sha256}

BackupVolumeManifestTransfer {1 task_id; 2 assignment_id; 3 step_id;
  4 authority_digest; 5 transfer_id; 6 direction; 7 record_sequence;
  oneof 10 start,11 batch,12 end,13 resume}
BackupVolumeManifestStart {1 point_id; 2 restore_generation_id; 3 role;
  4 entry_count; 5 content_manifest_sha256; 6 full_tree_sha256;
  oneof 7 source_archive,8 no_source_archive}
BackupVolumeManifestBatch {1 first_ordinal; 2 entries;
  3 preceding_transfer_chain_sha256; 4 resulting_transfer_chain_sha256}
BackupVolumeManifestEnd {1 entry_count; 2 content_manifest_sha256;
  3 full_tree_sha256; 4 final_transfer_chain_sha256}
BackupVolumeManifestResume {1 next_record_sequence; 2 next_ordinal;
  3 preceding_transfer_chain_sha256; 4 prior_ack_sequence;
  5 prior_committed_record_sequence}
BackupVolumeManifestAckCredit {1 task_id; 2 assignment_id; 3 step_id;
  4 authority_digest; 5 transfer_id; 6 ack_sequence;
  7 committed_record_sequence; 8 next_ordinal; 9 transfer_chain_sha256;
  10 record_credit; 11 byte_credit}
```

Config direction is capture or restore. Volume directions are capture
Agent-to-Controller, restore-new Controller-to-Agent, and restore-old
Agent-to-Controller; their roles are captured, restore-new, and stopped-live-
old. Transfer chains, credit, and committed cursors survive reconnect; a
sender never exceeds granted byte and record credit.

The exact typed checkpoint families that must accompany those streams are:

```text
BackupArtifactPrepared {1 point_id; 2 evidence; 3 finals;
  oneof 10 postgres,11 config,12 volume}
BackupUploadCompleted {1 point_id; 2 evidence; 3 target;
  oneof 4 returned_object,5 unknown; 6 metadata_count; 7 metadata_sha256}
BackupPutOutcomeUnknown {}
BackupUploadVerified {1 point_id; 2 evidence; 3 object;
  4 metadata_count; 5 metadata_sha256}
BackupSourceCleanupCompleted {1 point_id; 2 evidence}
BackupRestoreArtifactValidated {1 point_id; 2 object; 3 evidence; 4 finals;
  oneof 10 postgres,11 config,12 volume}

BackupConfigCheckpoint {oneof 1 value_progress,2 transfer_completed,
  3 materialization_verified}
BackupConfigValueProgress {1 next_ordinal; 2 chain_sha256}
BackupConfigTransferCompleted {1 restore_generation_id; 2 content;
  3 committed_record_count; 4 value_chain_sha256;
  5 transfer_transcript_sha256; 6 render_generation}
BackupConfigMaterializationVerified {1 restore_generation_id;
  2 materialized_entry_count; 3 materialization_sha256}

BackupVolumeCheckpoint {oneof 1 consumers_stopped,2 construction_intent,
  3 construction_completed,4 finalization_intent,5 finalization_completed,
  6 exchange_intent,7 tree_exchanged,8 replaced_path_delete_intent,
  9 volume_replaced_path_cleaned,10 service_progress}
BackupVolumeProgress {1 construction_cursor; 2 construction_chain_sha256;
  3 optional pending_construction; 4 finalization_cursor;
  5 finalization_chain_sha256; 6 optional pending_finalization;
  7 old_full_tree_sha256; 8 new_full_tree_sha256;
  9 optional pending_exchange; 10 deletion_cursor;
  11 optional pending_delete; 12 service_cursor; 13 service_phase}
```

The remaining missing leaf layouts are also fixed. They must be introduced as
typed protobuf messages in the same cutover, not reconstructed from host state
or encoded as generic maps.

Config capture omits the restore generation and render generation: it writes an
archive, not a new live Entry generation. Config restore supplies both from its
sealed publication authority. Completed transfer proves the sealed content and
the exact final durable transfer credit in either direction.

```text
BackupStagingFinals {1 source_relative_name; 2 stored_relative_name;
  3 same_inode}

BackupConfigEntry {1 ordinal; 2 entry_id; 3 entry; 4 optional secret;
  oneof 10 environment,11 file; oneof 20 exposure_all,21 exposure_services;
  oneof 30 literal,31 secret_ref,32 fact_ref;
  40 selected_value_size_bytes; 41 selected_value_sha256}
BackupConfigEnvironment {1 name}
BackupConfigFile {1 path; 2 mode; 3 uid; 4 gid}
BackupConfigExposureAll {}
BackupConfigExposureServices {1 service_ids}
BackupConfigLiteral {}
BackupConfigSecretRef {1 secret_id; 2 authored_key; 3 secret}
BackupConfigFactRef {1 attach_id; 2 fact; 3 optional grant_attach_id;
  4 attach; 5 optional grant_attach}
BackupConfigRestoreEntry {1 entry_id; 2 metadata; 3 exposure; 4 source;
  5 optional secret; 6 selected_value}
BackupConfigEntryMetadata {oneof 1 environment,2 file}
BackupConfigEntryExposure {oneof 1 all,2 services}
BackupConfigArtifactDesiredSource {oneof 1 literal,2 secret_ref,3 fact_ref}
BackupConfigLiteralSource {}
BackupConfigArtifactSecretRefSource {1 authored_key}
BackupConfigArtifactFactSource {1 attach_id; 2 optional grant_attach_id;
  3 fact}
BackupConfigSelectedValue {1 size_bytes; 2 sha256}
BackupConfigContentAuthority {1 manifest_sha256; 2 entry_count;
  3 total_selected_value_bytes; 4 manifest_size_bytes;
  5 source_size_bytes; 6 metadata_snapshot_sha256}
BackupConfigArchiveEvidence {1 content; 2 capture_transcript_sha256}
BackupConfigCaptureAuthority {1 environment_id; reserved 2 environment;
  3 content; 4 metadata_snapshot_revision;
  reserved 5 metadata_snapshot_sha256; 6 metadata_entry_count;
  7 metadata_proto_bytes}
BackupConfigMetadataRow {1 authority_digest; 2 metadata_snapshot_sha256;
  3 ordinal; oneof 4 capture_entry,5 restore_entry;
  6 preceding_transfer_chain_sha256; 7 resulting_transfer_chain_sha256}
BackupConfigMetadataAccepted {1 metadata_transcript_sha256;
  2 initial_value_credit_bytes; 3 initial_value_record_credit}
BackupConfigValueCredit {1 value_credit_bytes; 2 record_credit}
BackupConfigMetadataCredit {1 record_credit; 2 byte_credit}

BackupPostgresCaptureAuthority {1 adapter_contract_version;
  2 database_service_id; reserved 3 consumers;
  reserved 4 managed_postgres_repository_digest; 5 database_name;
  6 role_name; 7 max_plaintext_bytes}
BackupPostgresRestoreAuthority {1 adapter_contract_version;
  2 database_service_id; reserved 3 consumers;
  reserved 4 managed_postgres_repository_digest; 5 database_name; 6 role_name}
BackupPostgresContainerObserved {1 service_id; 2 container_id;
  3 repository_digest; 4 observed_label_count; 5 observed_labels_sha256;
  6 observation_sha256}
BackupPostgresDumpStart {1 point_id; 2 execution_nonce; 3 container_id;
  4 exec_id; 5 repository_digest; 6 expected_labels_sha256;
  7 adapter_contract_version; 8 database_name; 9 role_name;
  10 max_plaintext_bytes}
BackupPostgresRestoreApplyStartCheckpoint {1 point_id; 2 execution_nonce;
  3 container_id; 4 exec_id; 5 repository_digest;
  6 expected_labels_sha256; 7 source_size_bytes; 8 source_sha256}
BackupPostgresRestoreVerified {1 point_id; 2 container_id;
  3 evidence; 4 verification_sha256}
BackupPostgresServiceProgress {1 service_cursor; 2 service_id;
  3 phase; 4 observation_sha256}
BackupPostgresArchiveEvidence {1 pg_dump_major; 2 adapter_contract_version}

BackupVolumeManifestEntry {1 ordinal; 2 relative_path; 3 kind; 4 mode;
  5 uid; 6 gid; 7 size_bytes; 8 content_sha256}
BackupVolumeArchiveEvidence {1 entry_count; 2 content_manifest_sha256;
  3 full_tree_sha256; 4 source_size_bytes}
BackupVolumeProjectionAuthority {1 artifact_id; 2 artifact_sha256;
  3 artifact_revision; 4 projection_root; 5 render_generation;
  6 compose_volume_key; 7 docker_volume_name; 8 authorized_volume_dir}
BackupVolumeCaptureAuthority {1 volume_id; 2 volume;
  3 source_size_upper_bound; reserved 4 consumers; 5 projection}
BackupVolumeRestoreAuthority {1 volume_id; 2 volume; 3 archive;
  reserved 4 consumers; 5 projection}
BackupVolumeSourceArchiveEvidence {1 source_size_bytes; 2 source_sha256}
BackupVolumeNoSourceArchiveEvidence {}
BackupVolumeConsumersStopped {1 stopped_count; 2 service_progress_sha256}
BackupVolumeConstructionIntent {1 point_id; 2 restore_generation_id;
  3 new_tree_content_manifest_sha256; 4 construction_cursor_before;
  5 archive_ordinal; 6 logical_path; 7 kind; 8 mode; 9 uid; 10 gid;
  11 size_bytes; 12 content_sha256}
BackupVolumeConstructionCompleted {1 intent; 2 construction_cursor_after}
BackupVolumeFinalizationIntent {1 point_id; 2 restore_generation_id;
  3 new_tree_content_manifest_sha256; 4 finalization_cursor_before;
  5 finalization_ordinal; 6 logical_path; 7 final_uid; 8 final_gid;
  9 final_mode}
BackupVolumeFinalizationCompleted {1 intent; 2 finalization_cursor_after}
BackupVolumeExchangeIntent {1 point_id; 2 restore_generation_id;
  3 old_full_tree_sha256; 4 new_full_tree_sha256}
BackupVolumeTreeExchanged {1 point_id; 2 restore_generation_id;
  3 old_full_tree_sha256; 4 new_full_tree_sha256}
BackupVolumeDeleteIntent {1 point_id; 2 restore_generation_id;
  3 old_full_tree_sha256; 4 new_full_tree_sha256;
  5 deletion_cursor_before; 6 deletion_ordinal; 7 logical_path;
  8 kind; 9 parent_logical_path}
BackupVolumeReplacedPathCleaned {1 point_id; 2 restore_generation_id;
  3 old_full_tree_sha256; 4 new_full_tree_sha256;
  5 deletion_cursor_before; 6 deletion_ordinal; 7 logical_path;
  8 kind; 9 parent_logical_path}
BackupVolumeServiceProgress {1 service_cursor; 2 service_id;
  3 phase; 4 observation_sha256}
```

`secret` presence is mandatory. Config metadata credit is exactly 46 records
and 393,216 bytes; initial value credit is 262,144 bytes and eight records;
later value grants are bounded by those same amounts. PostgreSQL archive
evidence is major 16 and adapter contract one. Container and Exec ids are 64
lowercase hex and the named digests and nonces are 32 bytes. PostgreSQL capture
plaintext is capped at 5,497,558,138,880 bytes without age and
5,496,216,289,016 bytes with age. Volume kinds are directory or regular file.
Volume Service phases are not-running, stop-intent, stopped, restart-intent,
restarted, health-wait, and healthy.

Terminal delivery uses one common field tuple for
`TaskTerminalReceiptAck`, `TaskTerminalReceiptApplied`, and
`TaskTerminalReceiptAppliedAck`:

```text
1 process_generation; 2 task_id; 3 assignment_id;
4 assignment_generation; 5 plan_hash; 6 terminal; 7 task_ack_sha256;
8 terminal_receipt_sha256; 9 durable_task_mod_revision
```

`TaskTerminalAssignmentRetired` and its acknowledgement use fields 1–6 of
that tuple. `BackupStagingRecoveryAckReceipt` is
`1 process_generation; 2 inventory_sha256; 3 applied_plan_sha256;
4 recovery_ack_sha256`. These messages are exact-delivery evidence, not a
second source of Task state.

The durable, non-wire `TaskTerminalReceiptRecord` remains schema one with
`2 task_id; 3 agent_id; 4 agent_generation; 5 assignment_id;
6 assignment_generation; 7 plan_hash; 8 terminal; 9 terminal_task_sha256`.
It intentionally contains no Ack digest, Task revision, delivery state,
process generation, or self-digest.

## Consequences

- Backup correctness depends on immutable authority and durable progress, not
  on best-effort cleanup or replaying a command.
- Restores intentionally prefer a visible recovery-required state over a
  second destructive attempt whose first result is unknown.
- The accepted missing protocol requires one clean coordinated cutover; there
  is no negotiation or parallel legacy decoder.
- Accepted design is not implementation or QA evidence. Capability and
  qualification documents remain the authority for availability.

## Source navigation

Implemented wire fields live in
[`proto/agent.proto`](../../proto/agent.proto). Backup plan construction and
Controller workflow live in
[`internal/controller/backup`](../../internal/controller/backup/run_plan_builder.go)
and [`internal/infra/etcd/backupplanning`](../../internal/infra/etcd/backupplanning/planner.go).
Durable run, checkpoint, retention, and point state live in
[`internal/infra/etcd/backupruntime`](../../internal/infra/etcd/backupruntime/types.go)
and [`internal/infra/etcd/backupretention`](../../internal/infra/etcd/backupretention/repository.go).

Config Restore file planning and its aggregate proof are owned by
[`backupconfigmaterialization`](../../internal/common/backupconfigmaterialization/plan.go).
Ordinary and restored environment-file content share
[`dotenvfile`](../../internal/common/dotenvfile/render.go); host writes use the
existing [`materialization runtime`](../../internal/agent/materialization/runtime.go).

Canonical format code is owned by
[`internal/common/backupconfig`](../../internal/common/backupconfig/config.go),
[`internal/common/backupvolume`](../../internal/common/backupvolume/model.go),
[`internal/common/backuppostgres`](../../internal/common/backuppostgres/postgres.go),
and [`internal/common/backupformat`](../../internal/common/backupformat/format.go).
Agent staging and checkpoint delivery are in
[`internal/infra/backupstage`](../../internal/infra/backupstage/backupstage_linux.go)
and [`internal/agent/checkpointmailbox`](../../internal/agent/checkpointmailbox/backup.go).
The PostgreSQL helper vocabulary is in
[`internal/common/postgres16protocol`](../../internal/common/postgres16protocol/protocol.go).
These links identify current owners; they do not prove the accepted protocol or
PostgreSQL confinement runtime is complete or qualified.
