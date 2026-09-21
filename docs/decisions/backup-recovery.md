# Backup recovery and execution boundaries

- Decision status: Accepted; implementation and qualification remain partial
- Scope: Backup authority, artifact publication, restore safety, Agent recovery,
  and the managed PostgreSQL execution boundary

## Recovery is driven by a sealed snapshot

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

The accepted source formats are the canonical Environment Config archive, the
canonical managed-Volume archive, and PostgreSQL 16 `pg_dump` custom format.
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

## Agent restart and delivery fail closed

Backup work uses schema-one assignments with stable ids, revisions, authority
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

Upload intent is checkpointed before `Put`; completion is checkpointed before
the verifying `Head`; only the Controller publishes the Recovery Point.
Terminal Task results also use a durable receipt and explicit assignment
retirement handshake. An Agent never treats a disconnected stream, Task state,
or time passage as proof that terminal delivery is complete.

## Managed PostgreSQL uses a closed helper and private gate

The supported PostgreSQL source is one release-authenticated, dual-platform
PostgreSQL 16 image index. The helper, private client gate, rootfs and runtime
security evidence are part of that release; operators and desired state cannot
override them. Container and mount attestation prevents any workload mount
from shadowing the helper, gate, state directory, clients, socket, or runtime
identity. Container and host root are trusted boundary actors; database uid 70
is not trusted with supervisor state.

The shared protocol permits only the fixed probe, dump, restore-list,
connection-termination, restore-apply, verification, and stop operations. It
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

This security contract is accepted but not delivered by the presence of shared
types alone. In particular,
[`internal/postgres16helper/main.go`](../../internal/postgres16helper/main.go)
currently fails closed until the supervisor and private-gate runtime exists.

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
  4 secret_slot}
BackupPriorRuntimeIntent {1 kind; 2 intent}
BackupServiceFact {1 service_id; 2 current_name; 3 service; 4 compose;
  5 prior_runtime_intent; 6 required_label_count;
  7 required_labels_sha256; 8 repository_digest}
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
  3 required_growth_bytes; 4 expected_files}
BackupDiscardRecovered {}
BackupStagingRecoveryAck {1 inventory_sha256; 2 applied_plan_sha256;
  3 applied_disposition_count}
```

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
BackupConfigProgress {1 metadata_accepted; 2 metadata_transcript_sha256;
  3 next_value_ordinal; 4 value_chain_sha256;
  oneof 10 transfer,11 materialization}
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
BackupConfigRestoreAuthority {1 destination_environment_id;
  reserved 2 destination; 3 expected_content}
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
