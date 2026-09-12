# Backup schema-1 wire contract

Read this document when changing `proto/agent.proto` or implementing the Backup
channel. It preserves the exact accepted Backup shapes that are not yet present
in the checked-in protobuf. The [Agent execution document](agent-protocol.md)
owns ordering, digest, queue, transaction, and recovery semantics.

## Current source conflict

The checked-in protobuf is still the incomplete pre-implementation Backup
shape. It lacks the schema-1 authority, resume, Config/Volume transfer, staging
recovery, generic terminal-delivery, adoption, and source-specific checkpoint
messages below. Its old `BackupCheckpointRequest` also uses the replaced
uint32/kind/control-digest shape.

There is an unresolved tag collision that must be decided before implementation:

| Outer message | Accepted Backup tags | Currently occupying those tags |
| --- | --- | --- |
| `AgentMessage` | 11 Config transfer; 12 Config credit; 13 staging inventory; 14 staging ack; 15 Volume transfer; 16 Volume ack/credit; 17 receipt applied; 18 assignment retired | 11 workload image result; 12 Volume-removal checkpoint; 13 Service-observation result |
| `ControllerMessage` | 12 Config transfer; 13 Config credit; 14 staging plan; 15 Volume transfer; 16 Volume ack/credit; 17 receipt ack; 18 applied ack; 19 staging-ack receipt; 20 retired ack | 12 workload image resolution; 13 Volume-removal ack; 14 Service-observation request |

The accepted Backup fields cannot be implemented at those exact tags while the
current occupants remain. This migration does not choose new numbers or revive
the incomplete decoder. The owner must resolve the collision and update the
wire contract once, cleanly. Until then, Backup schema 1 is unimplemented and
must not advertise readiness.

The catalogs below are the accepted Backup schema. Field notation is
`tag: type name`; `optional` and `oneof` are semantically required where shown.
Unknown fields reject recursively. Maps are forbidden in Backup authority.

## Channel and outer additions

Existing `Authenticate` tags 1/2 remain. Required additions are:

```text
Authenticate
  3: uint32 execution_plan_schema                 required, exactly 1
  4: bytes process_generation                     exactly 16 bytes
  5: bytes postgres16_managed_release_sha256      exactly 32 bytes

AgentLabel
  1: string key
  2: string value

AgentConfig
  3: repeated AgentLabel labels                   clean replacement of map

Ready
  6: optional bool terminal_delivery_clean        presence required

ObservedState
  2: bytes snapshot_id                            exactly 16 bytes
  3: uint64 batch_ordinal                         zero-based
  4: bool final_batch
```

Labels sort by key then value and keys are unique. There is no map decoder.
Observed Projects sort by stable id and one Project is never split.

Backup additions to existing assignment/ack messages are exact:

```text
TaskAssignment
  11: BackupTaskAuthority backup_authority
  12: BackupTaskResume backup_resume
  13: bytes backup_authority_sha256                exactly 32 bytes
  14: uint64 assignment_generation                1..MaxInt64

TaskAck.result
  10: BackupTaskResult backup_result
TaskAck
  11: uint64 assignment_generation                1..MaxInt64

TaskAbort
  4: uint64 assignment_generation                 1..MaxInt64
  5: bytes plan_hash                              exactly 32 bytes
  6: TaskTerminal terminal                        non-UNSPECIFIED
  7: bytes terminal_receipt_sha256                exactly 32 bytes
  8: uint64 durable_task_mod_revision             1..MaxInt64
  9: optional bytes rejected_task_ack_sha256      absent or exactly 32 bytes
```

`TaskAssignment` tag 5 remains reserved together with name
`timeout_seconds`. `TaskAck` retains result tags 5/6, assignment tag 7,
execution epoch tag 8, and release-recovery digest tag 9. Backup result is valid
only with exact assignment id/generation and a non-unspecified terminal.

```text
BackupTaskResult
  1: string failed_step_id
  2: bool recovery_required
```

## Generic terminal delivery messages

`TaskTerminalReceiptAck`, `TaskTerminalReceiptApplied`, and
`TaskTerminalReceiptAppliedAck` each have exactly:

```text
1: bytes process_generation                       exact authenticated 16 bytes
2: string task_id                                 canonical task_ id
3: string assignment_id                           canonical asgn_ id
4: uint64 assignment_generation                   1..MaxInt64
5: bytes plan_hash                                exactly 32 bytes
6: TaskTerminal terminal
7: bytes task_ack_sha256                           exactly 32 bytes
8: bytes terminal_receipt_sha256                  exactly 32 bytes
9: uint64 durable_task_mod_revision               1..MaxInt64
```

`TaskTerminalAssignmentRetired` and its Ack each have exactly:

```text
1: bytes process_generation                       exact authenticated 16 bytes
2: string task_id
3: string assignment_id
4: uint64 assignment_generation                   1..MaxInt64
5: bytes plan_hash                                exactly 32 bytes
6: TaskTerminal terminal
```

The staging-plan acknowledgement receipt is:

```text
BackupStagingRecoveryAckReceipt
  1: bytes process_generation                     exact authenticated 16 bytes
  2: bytes inventory_sha256                       exactly 32 bytes
  3: bytes applied_plan_sha256                    exactly 32 bytes
  4: bytes recovery_ack_sha256                    exactly 32 bytes
```

The durable Ack-independent receipt encoding, stored rather than sent, is:

```text
TaskTerminalReceiptRecord
  1: uint32 schema                                exactly 1
  2: string task_id
  3: string agent_id
  4: uint64 agent_generation                      durable AgentGeneration
  5: string assignment_id
  6: uint64 assignment_generation
  7: bytes plan_hash                              exactly 32 bytes
  8: TaskTerminal terminal
  9: bytes terminal_task_sha256                   exactly 32 bytes
```

It has no Ack digest, Task revision, delivery state, process generation, or
self-digest.

## Task authority and resume

```text
BackupTaskAuthority
  1: uint32 authority_schema                      exactly 1
  2: string task_id
  3: string operation_id
  4: uint64 task_attempt
  5: bytes plan_hash                              exactly 32 bytes
  6: string project_id
  7: RevisionDigest project
  8: string environment_id
  9: RevisionDigest environment
 10: int64 task_deadline_unix_nano
 11: string assignment_id
 12: uint64 assignment_generation
 13: repeated BackupServiceFact services          <=512, service-id sorted
 14: repeated BackupStepAuthority steps           1..12, plan order

BackupStepAuthority
  1: string step_id
  2: bytes execution_id                           exactly 32 bytes
  3: bytes step_digest                            exactly 32 bytes
  4: int64 step_deadline_unix_nano
  5: repeated string consumer_service_ids         sorted unique common refs
 oneof operation
 10: BackupCaptureAuthority capture
 11: BackupPruneAuthority prune
 12: BackupRestoreAuthority restore

CheckpointFence
  1: bytes authority_digest                       exactly 32 bytes
  2: uint64 dedupe_key_mod_revision

BackupTaskResume
  1: string assignment_id
  2: uint64 assignment_generation
  3: repeated BackupStepResume steps              exact authority order

BackupStepResume
  1: string step_id
  2: bytes execution_id                           exactly 32 bytes
 oneof operation
 10: BackupCaptureResume capture
 11: BackupPruneResume prune
 12: BackupRestoreResume restore
```

Capture and restore phase enums are exact:

```text
BackupCapturePhase: 0 UNSPECIFIED; 1 CAPTURING; 2 ARTIFACT_PREPARED;
  3 UPLOADING; 4 HEAD_VERIFICATION; 5 POINT_COMMIT; 6 SOURCE_CLEANUP;
  7 SERVICE_RECOVERY; 8 TERMINAL

BackupRestorePhase: 0 UNSPECIFIED; 1 ARTIFACT_VALIDATION;
  2 TARGET_PREPARATION; 3 TARGET_MUTATION; 4 PUBLICATION; 5 CLEANUP;
  6 SERVICE_RECOVERY; 7 TERMINAL
```

```text
BackupCaptureResume
  1: uint64 checkpoint_sequence
  2: CheckpointFence preceding_checkpoint
  3: BackupCapturePhase phase
  4: BackupCaptureCursor cursor
 oneof immediately_preceding_payload
 10: BackupArtifactPrepared artifact_prepared
 11: BackupUploadVerified upload_verified
 12: BackupSourceCleanupCompleted source_cleanup_completed
 13: BackupPostgresContainerObserved postgres_container_observed
 14: BackupPostgresDumpStart postgres_dump_start
 15: BackupVolumeProgress volume_progress
 16: BackupConfigProgress config_progress
 17: BackupUploadCompleted upload_completed

BackupPruneResume
  1: uint64 checkpoint_sequence
  2: CheckpointFence preceding_checkpoint
  3: uint64 next_object_ordinal
 oneof immediately_preceding_payload
 10: BackupPruneObjectDeleted object_deleted

BackupRestoreResume
  1: uint64 checkpoint_sequence
  2: CheckpointFence preceding_checkpoint
  3: BackupRestorePhase phase
  4: BackupRestoreCursor cursor
 oneof immediately_preceding_payload
 10: BackupRestoreArtifactValidated artifact_validated
 11: BackupPostgresContainerObserved postgres_container_observed
 12: BackupPostgresRestoreApplyStartCheckpoint postgres_restore_apply_start
 13: BackupConfigProgress config_progress
 14: BackupVolumeProgress volume_progress
 15: BackupPostgresRestoreVerified postgres_restore_verified
 16: BackupPostgresServiceProgress postgres_service_progress

BackupCaptureCursor
  1: uint64 object_attempt
  2: uint64 config_record_sequence
  3: uint64 config_value_ordinal
  4: uint64 service_cursor
  5: bytes cumulative_chain_sha256                 exactly 32 bytes

BackupRestoreCursor
  1: uint64 object_attempt
  2: uint64 config_record_sequence
  3: uint64 config_value_ordinal
  4: uint64 volume_cursor
  5: uint64 service_cursor
  6: bytes cumulative_chain_sha256                 exactly 32 bytes
```

## Checkpoint request and acknowledgement

The final request clean-replaces old tags 2..6. Old names `sequence`, `kind`,
and `control_payload_sha256` are reserved by name and are never decoded.

```text
BackupCheckpointRequest
  1: string task_id
  2: string step_id
  3: bytes execution_id                           exactly 32 bytes
  4: uint64 checkpoint_sequence
  5: CheckpointFence preceding_checkpoint
  6: bytes authority_digest                       exactly 32 bytes
  7: string assignment_id
 oneof payload
 20: BackupArtifactPrepared artifact_prepared
 21: BackupUploadVerified upload_verified
 22: BackupSourceCleanupCompleted source_cleanup_completed
 23: BackupPostgresContainerObserved postgres_container_observed
 24: BackupPostgresDumpStart postgres_dump_start
 25: BackupPostgresRestoreApplyStartCheckpoint postgres_restore_apply_start
 26: BackupRestoreArtifactValidated restore_artifact_validated
 27: BackupConfigCheckpoint config
 28: BackupVolumeCheckpoint volume
 29: BackupPruneObjectDeleted prune_object_deleted
 30: BackupPostgresRestoreVerified postgres_restore_verified
 31: BackupPostgresServiceProgress postgres_service_progress
 32: BackupUploadCompleted upload_completed

BackupCheckpointAck
  1: string task_id
  2: string step_id
  3: bytes execution_id                           exactly 32 bytes
  4: uint64 checkpoint_sequence
  5: CheckpointFence committed
  6: string assignment_id
```

## Common sealed authority

```text
BackupResourceKind: 0 UNSPECIFIED; 1 POSTGRES; 2 CONFIG; 3 VOLUME
BackupEncryptionKind: 0 UNSPECIFIED; 1 NONE; 2 AGE_X25519
BackupRuntimeIntentKind: 0 UNSPECIFIED; 1 STOPPED; 2 RUNNING

RevisionDigest
  1: uint64 mod_revision                          1..MaxInt64
  2: bytes sha256                                 exactly 32 bytes

BackupResourceIdentity
  1: BackupResourceKind kind
  2: string resource_id
  3: RevisionDigest resource

BackupEncryptionAuthority
  1: BackupEncryptionKind kind
  2: string secret_slot_id                        absent none; spt_ for age
  3: bytes recipient_sha256                       absent none; 32 bytes age
  4: RevisionDigest secret_slot                   absent none

BackupPriorRuntimeIntent
  1: BackupRuntimeIntentKind kind
  2: RevisionDigest intent

BackupServiceFact
  1: string service_id
  2: string current_name
  3: RevisionDigest service
  4: RevisionDigest compose
  5: BackupPriorRuntimeIntent prior_runtime_intent
  6: uint32 required_label_count                  0..64
  7: bytes required_labels_sha256                 exactly 32 bytes
  8: bytes repository_digest                      exactly 32 bytes

BackupArtifactEvidence
  1: uint64 source_size_bytes
  2: bytes source_sha256                          exactly 32 bytes
  3: uint64 stored_size_bytes
  4: bytes stored_sha256                          exactly 32 bytes

BackupConnectorAuthority
  1: string connector_id
  2: RevisionDigest connector
  3: bytes canonical_endpoint_url                 1..2048
  4: bytes region                                 1..64, each 0x21..0x7e
  5: bool path_style
  6: bytes prefix                                 0..1024
  7: string access_key_slot_id
  8: string secret_key_slot_id
  9: RevisionDigest access_key_slot
 10: RevisionDigest secret_key_slot

BackupS3VersionId  1: bytes value                 present, 0..1024
BackupS3ETag       1: bytes value                 present, 1..1024

BackupObjectTarget
  1: BackupConnectorAuthority connector
  2: string bucket
  3: bytes object_key

BackupObjectIdentity
  1: BackupConnectorAuthority connector
  2: string bucket
  3: bytes object_key
 oneof immutable_discriminator
  4: BackupS3VersionId version_id
  5: BackupS3ETag etag

BackupCaptureAuthority
  1: string point_id
  2: BackupResourceIdentity resource
  3: BackupObjectTarget target
  4: BackupEncryptionAuthority encryption
  reserved 5, "services"
 oneof format
 10: BackupPostgresCaptureAuthority postgres
 11: BackupConfigCaptureAuthority config
 12: BackupVolumeCaptureAuthority volume

BackupPruneAuthority
  1: RevisionDigest retention_policy
  2: repeated BackupPruneObject objects            1..11

BackupRestoreAuthority
  1: string point_id
  2: BackupResourceIdentity destination
  3: BackupObjectIdentity source_object
  4: BackupArtifactEvidence expected_evidence
  5: BackupEncryptionAuthority encryption
  reserved 6, "services"
 oneof format
 10: BackupPostgresRestoreAuthority postgres
 11: BackupConfigRestoreAuthority config
 12: BackupVolumeRestoreAuthority volume
 oneof identity_source
 20: BackupOriginalIdentity original
 21: BackupAdoptedIdentity adopted
```

## Staging recovery

```text
BackupRecoveredFileRole: 0 UNSPECIFIED; 1 SOURCE_PLAINTEXT; 2 STORED_OBJECT
BackupRemainingGrowth: 0 UNSPECIFIED; 1 NO_GROWTH; 2 BOUNDED;
  3 EXCLUSIVE_UNKNOWN

BackupStagingInventory
  1: repeated BackupRecoveredStage entries        0..32, recovery-key sorted

BackupRecoveredStage
  1: bytes recovery_key_sha256                    exactly 32 bytes
  2: repeated BackupRecoveredFile files           1..2, role order, unique

BackupRecoveredFile
  1: BackupRecoveredFileRole role
  2: uint64 size_bytes                            0..MaxInt64
  3: bytes sha256                                 exactly 32 bytes

BackupStagingRecoveryPlan
  1: bytes inventory_sha256                       exactly 32 bytes
  2: repeated BackupStagingDisposition dispositions

BackupStagingDisposition
  1: bytes recovery_key_sha256                    exactly 32 bytes
 oneof decision
  2: BackupResumePrepared resume_prepared
  3: BackupDiscardRecovered discard_recovered

BackupResumePrepared
  1: bytes assignment_resume_sha256               exactly 32 bytes
  2: BackupRemainingGrowth remaining_growth
  3: uint64 required_growth_bytes                 1..MaxInt64 iff BOUNDED
  4: repeated BackupRecoveredFile expected_files  1..2

BackupDiscardRecovered                            empty

BackupStagingRecoveryAck
  1: bytes inventory_sha256                       exactly 32 bytes
  2: bytes applied_plan_sha256                    exactly 32 bytes
  3: uint32 applied_disposition_count
```

Every inventory key occurs exactly once in the sorted Plan. Expected files are
byte-equal to inventory. Growth is zero/absent except in BOUNDED.

## Artifact and object checkpoints

```text
BackupStagingFinals
  1: bytes source_relative_name                   exact "source.final"
  2: bytes stored_relative_name                   source.final or stored.final
  3: bool same_inode

BackupArtifactPrepared
  1: string point_id
  2: BackupArtifactEvidence evidence
  3: BackupStagingFinals finals
 oneof format_evidence
 10: BackupPostgresArchiveEvidence postgres
 11: BackupConfigArchiveEvidence config
 12: BackupVolumeArchiveEvidence volume

BackupUploadCompleted
  1: string point_id
  2: BackupArtifactEvidence evidence
  3: BackupObjectTarget target
 oneof put_outcome
  4: BackupObjectIdentity returned_object
  5: BackupPutOutcomeUnknown unknown               empty
  6: uint32 metadata_count                        exactly 7
  7: bytes metadata_sha256                        exactly 32 bytes

BackupUploadVerified
  1: string point_id
  2: BackupArtifactEvidence evidence
  3: BackupObjectIdentity object
  4: uint32 metadata_count                        exactly 7
  5: bytes metadata_sha256                        exactly 32 bytes

BackupSourceCleanupCompleted
  1: string point_id
  2: BackupArtifactEvidence evidence

BackupRestoreArtifactValidated
  1: string point_id
  2: BackupObjectIdentity object
  3: BackupArtifactEvidence evidence
  4: BackupStagingFinals finals
 oneof format
 10: BackupPostgresArchiveEvidence postgres
 11: BackupConfigArchiveEvidence config
 12: BackupVolumeArchiveEvidence volume
```

## Config schema and transfer

Config metadata types are exact:

```text
BackupConfigEntry
  1: uint32 ordinal; 2: string entry_id; 3: RevisionDigest entry;
  4: optional bool secret
 oneof placement: 10 BackupConfigEnvironment; 11 BackupConfigFile
 oneof exposure: 20 BackupConfigExposureAll; 21 BackupConfigExposureServices
 oneof desired_source: 30 BackupConfigLiteral; 31 BackupConfigSecretRef;
                       32 BackupConfigFactRef
 40: uint64 selected_value_size_bytes; 41: bytes selected_value_sha256

BackupConfigEnvironment  1: bytes name
BackupConfigFile         1: bytes path; 2: uint32 mode; 3: uint32 uid; 4: uint32 gid
BackupConfigExposureAll  empty
BackupConfigExposureServices 1: repeated string service_ids
BackupConfigLiteral      empty
BackupConfigSecretRef    1: string secret_id; 2: bytes authored_key;
                         3: RevisionDigest secret
BackupConfigFactRef      1: string attach_id; 2: bytes fact;
                         3: optional string grant_attach_id;
                         4: RevisionDigest attach; 5: optional grant_attach

BackupConfigRestoreEntry
  1: string entry_id; 2: BackupConfigEntryMetadata metadata;
  3: BackupConfigEntryExposure exposure;
  4: BackupConfigArtifactDesiredSource source;
  5: optional bool secret; 6: BackupConfigSelectedValue selected_value

BackupConfigEntryMetadata oneof: 1 environment; 2 file
BackupConfigEntryExposure oneof: 1 all; 2 services
BackupConfigArtifactDesiredSource oneof:
  1 BackupConfigLiteralSource; 2 BackupConfigArtifactSecretRefSource;
  3 BackupConfigArtifactFactSource
BackupConfigLiteralSource empty
BackupConfigArtifactSecretRefSource 1: string authored_key
BackupConfigArtifactFactSource 1: string attach_id;
                               2: optional string grant_attach_id;
                               3: string fact
BackupConfigSelectedValue 1: uint64 size_bytes; 2: bytes sha256

BackupConfigContentAuthority
  1: bytes manifest_sha256; 2: uint32 entry_count;
  3: uint64 total_selected_value_bytes; 4: uint64 manifest_size_bytes;
  5: uint64 source_size_bytes; 6: bytes metadata_snapshot_sha256

BackupConfigArchiveEvidence
  1: BackupConfigContentAuthority content
  2: bytes capture_transcript_sha256

BackupConfigCaptureAuthority
  1: string environment_id; reserved 2, "environment";
  3: BackupConfigContentAuthority content;
  4: uint64 metadata_snapshot_revision;
  reserved 5, "metadata_snapshot_sha256";
  6: uint32 metadata_entry_count; 7: uint64 metadata_proto_bytes

BackupConfigRestoreAuthority
  1: string destination_environment_id; reserved 2, "destination";
  3: BackupConfigContentAuthority expected_content

BackupConfigMetadataRow
  1: bytes authority_digest; 2: bytes metadata_snapshot_sha256;
  3: uint32 ordinal
  oneof metadata: 4: BackupConfigEntry capture_entry;
                  5: BackupConfigRestoreEntry restore_entry
  6: bytes preceding_transfer_chain_sha256;
  7: bytes resulting_transfer_chain_sha256
```

All listed digests are exactly 32 bytes. `secret` presence is mandatory.
Transfer direction enum is `0 UNSPECIFIED; 1 CAPTURE; 2 RESTORE`.

```text
BackupConfigTransfer
  1: string task_id; 2: string step_id; 3: bytes execution_id;
  4: bytes transfer_id; 5: uint64 record_sequence; 6: string assignment_id
 oneof record: 10 start; 11 entry_header; 12 value_chunk;
               13 entry_end; 14 end; 15 resume

BackupConfigTransferStart
  1: BackupConfigTransferDirection direction;
  2: BackupConfigContentAuthority content

BackupConfigEntryHeader
 oneof metadata: 1: BackupConfigEntry capture_entry;
                 2: BackupConfigRestoreEntry restore_entry
  3: uint32 chunk_count

BackupConfigValueChunk
  1: uint32 ordinal; 2: uint64 offset; 3: bytes content

BackupConfigEntryEnd
  1: uint32 ordinal; 2: uint64 value_size_bytes; 3: bytes value_sha256

BackupConfigTransferEnd
  1: BackupConfigTransferDirection direction;
  2: BackupConfigContentAuthority content; 3: bytes transcript_sha256

BackupConfigTransferResume
  reserved tags 1,2,4,5,7,8,9 and names direction, next_record_sequence,
    metadata_accepted, metadata_chain_sha256, prior_ack_sequence,
    prior_committed_record_sequence, prior_ack_sha256
  3: uint32 next_ordinal; 6: bytes value_chain_sha256

BackupConfigCredit
  1: string task_id; 2: string step_id; 3: bytes execution_id;
  4: bytes transfer_id; 5: BackupConfigTransferDirection direction;
  6: uint64 credit_sequence; 7: string assignment_id
 oneof grant: 10: BackupConfigMetadataAccepted metadata_accepted;
               11: BackupConfigValueCredit value_credit;
               16: BackupConfigMetadataCredit metadata_credit
 12: uint64 committed_record_sequence; 13: uint32 next_ordinal;
 14: bytes cumulative_chain_sha256; 15: bytes ack_sha256

BackupConfigMetadataAccepted
  1 metadata_transcript_sha256;
  2 uint64 initial_value_credit_bytes             exactly 262144
  3 uint32 initial_value_record_credit            exactly 8

BackupConfigValueCredit
  1 uint64 value_credit_bytes                     0..262144
  2 uint32 record_credit                          0..8

BackupConfigMetadataCredit
  1 uint32 record_credit                          exactly 46
  2 uint64 byte_credit                            exactly 393216
```

Config progress checkpoints are:

```text
BackupConfigCheckpoint oneof:
  1 BackupConfigValueProgress value_progress
  2 BackupConfigTransferCompleted transfer_completed
  3 BackupConfigMaterializationVerified materialization_verified

BackupConfigProgress
  1: bool metadata_accepted; 2: bytes metadata_transcript_sha256;
  3: uint64 next_value_ordinal; 4: bytes value_chain_sha256
 oneof pending_claim: 10: BackupConfigTransferCompleted transfer;
                      11: BackupConfigMaterializationVerified materialization

BackupConfigValueProgress
  1: uint64 next_ordinal; 2: bytes chain_sha256

BackupConfigTransferCompleted
  1: string restore_generation_id; 2: BackupConfigContentAuthority content;
  3: uint64 committed_record_count; 4: bytes value_chain_sha256;
  5: bytes transfer_transcript_sha256; 6: uint64 render_generation

BackupConfigMaterializationVerified
  1: string restore_generation_id; 2: uint64 materialized_entry_count;
  3: bytes materialization_sha256
```

## PostgreSQL schema

```text
BackupPostgresCaptureAuthority
  1 uint32 adapter_contract_version               exactly 1
  2 string database_service_id
  reserved 3,4 and "consumers", "managed_postgres_repository_digest"
  5 bytes database_name; 6 bytes role_name; 7 uint64 max_plaintext_bytes

BackupPostgresRestoreAuthority
  1 uint32 adapter_contract_version               exactly 1
  2 string database_service_id
  reserved 3,4 and "consumers", "managed_postgres_repository_digest"
  5 bytes database_name; 6 bytes role_name

BackupPostgresContainerObserved
  1: string service_id; 2: string container_id; 3: bytes repository_digest;
  4: uint32 observed_label_count; 5: bytes observed_labels_sha256;
  6: bytes observation_sha256

BackupPostgresDumpStart
  1: string point_id; 2: bytes execution_nonce; 3: string container_id;
  4: string exec_id; 5: bytes repository_digest;
  6: bytes expected_labels_sha256; 7: uint32 adapter_contract_version;
  8: bytes database_name; 9: bytes role_name; 10: uint64 max_plaintext_bytes

BackupPostgresRestoreApplyStartCheckpoint
  1: string point_id; 2: bytes execution_nonce; 3: string container_id;
  4: string exec_id; 5: bytes repository_digest;
  6: bytes expected_labels_sha256; 7: uint64 source_size_bytes;
  8: bytes source_sha256

BackupPostgresRestoreVerified
  1: string point_id; 2: string container_id;
  3: BackupArtifactEvidence evidence; 4: bytes verification_sha256

BackupPostgresServiceProgress
  1: uint64 service_cursor; 2: string service_id;
  3: BackupVolumeServicePhase phase; 4: bytes observation_sha256

BackupPostgresArchiveEvidence
  1 uint32 pg_dump_major                         exactly 16
  2 uint32 adapter_contract_version              exactly 1
```

Nonces, repository/label/source/verification digests are 32 bytes; container
and exec ids are 64 lowercase hex. Capture plaintext maximum is exactly
5,497,558,138,880 for none or 5,496,216,289,016 for age.

## Volume schema and transfer

```text
BackupVolumeEntryKind: 0 UNSPECIFIED; 1 DIRECTORY; 2 REGULAR

BackupVolumeManifestEntry
  1: uint64 ordinal; 2: bytes relative_path; 3: BackupVolumeEntryKind kind;
  4: uint32 mode; 5: uint32 uid; 6: uint32 gid;
  7: uint64 size_bytes; 8: bytes content_sha256

BackupVolumeArchiveEvidence
  1: uint64 entry_count; 2: bytes content_manifest_sha256;
  3: bytes full_tree_sha256; 4: uint64 source_size_bytes

BackupVolumeProjectionAuthority
  1: string artifact_id; 2: bytes artifact_sha256;
  3: uint64 artifact_revision; 4: uint64 projection_root;
  5: uint64 render_generation; 6: string compose_volume_key;
  7: string docker_volume_name; 8: string authorized_volume_dir

BackupVolumeCaptureAuthority
  1: string volume_id; 2: RevisionDigest volume;
  3: uint64 source_size_upper_bound; reserved 4, "consumers";
  5: BackupVolumeProjectionAuthority projection

BackupVolumeRestoreAuthority
  1: string volume_id; 2: RevisionDigest volume;
  3: BackupVolumeArchiveEvidence archive; reserved 4, "consumers";
  5: BackupVolumeProjectionAuthority projection
```

Manifest direction/role are exact pairs:

```text
direction: 0 UNSPECIFIED; 1 CAPTURE_AGENT_TO_CONTROLLER;
           2 RESTORE_NEW_CONTROLLER_TO_AGENT;
           3 RESTORE_OLD_AGENT_TO_CONTROLLER
role:      0 UNSPECIFIED; 1 CAPTURED; 2 RESTORE_NEW;
           3 STOPPED_LIVE_OLD
```

```text
BackupVolumeManifestTransfer
  1: string task_id; 2: string assignment_id; 3: string step_id;
  4: bytes authority_digest; 5: bytes transfer_id;
  6: BackupVolumeManifestDirection direction; 7: uint64 record_sequence
 oneof record: 10 start; 11 batch; 12 end; 13 resume

BackupVolumeManifestStart
  1: string point_id; 2: string restore_generation_id;
  3: BackupVolumeManifestRole role; 4: uint32 entry_count;
  5: bytes content_manifest_sha256; 6: bytes full_tree_sha256
 oneof source_archive_evidence:
  7 BackupVolumeSourceArchiveEvidence source_archive
  8 BackupVolumeNoSourceArchiveEvidence no_source_archive

BackupVolumeSourceArchiveEvidence
  1 uint64 source_size_bytes; 2 bytes source_sha256
BackupVolumeNoSourceArchiveEvidence empty

BackupVolumeManifestBatch
  1: uint64 first_ordinal;
  2: repeated BackupVolumeManifestEntry entries;
  3: bytes preceding_transfer_chain_sha256;
  4: bytes resulting_transfer_chain_sha256

BackupVolumeManifestEnd
  1: uint32 entry_count; 2: bytes content_manifest_sha256;
  3: bytes full_tree_sha256; 4: bytes final_transfer_chain_sha256

BackupVolumeManifestResume
  1: uint64 next_record_sequence; 2: uint32 next_ordinal;
  3: bytes preceding_transfer_chain_sha256; 4: uint64 prior_ack_sequence;
  5: uint64 prior_committed_record_sequence

BackupVolumeManifestAckCredit
  1: string task_id; 2: string assignment_id; 3: string step_id;
  4: bytes authority_digest; 5: bytes transfer_id;
  6: uint64 ack_sequence; 7: uint64 committed_record_sequence;
  8: uint32 next_ordinal; 9: bytes transfer_chain_sha256;
 10: uint32 record_credit; 11: uint64 byte_credit
```

Volume checkpoint shape is exact:

```text
BackupVolumeCheckpoint oneof:
  1 consumers_stopped; 2 construction_intent; 3 construction_completed;
  4 finalization_intent; 5 finalization_completed; 6 exchange_intent;
  7 tree_exchanged; 8 replaced_path_delete_intent;
  9 volume_replaced_path_cleaned; 10 service_progress

BackupVolumeProgress
  1 uint64 construction_cursor; 2 construction_chain_sha256;
  3 optional pending_construction; 4 uint64 finalization_cursor;
  5 finalization_chain_sha256; 6 optional pending_finalization;
  7 old_full_tree_sha256; 8 new_full_tree_sha256;
  9 optional pending_exchange; 10 uint64 deletion_cursor;
 11 optional pending_delete; 12 uint64 service_cursor; 13 service_phase

BackupVolumeConsumersStopped
  1 uint64 stopped_count; 2 service_progress_sha256

BackupVolumeConstructionIntent
  1 point_id; 2 restore_generation_id; 3 new_tree_content_manifest_sha256;
  4 uint64 construction_cursor_before; 5 uint64 archive_ordinal;
  6 logical_path; 7 kind; 8 mode; 9 uid; 10 gid;
 11 uint64 size_bytes; 12 content_sha256
BackupVolumeConstructionCompleted
  1 BackupVolumeConstructionIntent intent; 2 uint64 construction_cursor_after

BackupVolumeFinalizationIntent
  1 point_id; 2 restore_generation_id; 3 new_tree_content_manifest_sha256;
  4 uint64 finalization_cursor_before; 5 uint64 finalization_ordinal;
  6 logical_path; 7 final_uid; 8 final_gid; 9 final_mode
BackupVolumeFinalizationCompleted
  1 BackupVolumeFinalizationIntent intent; 2 uint64 finalization_cursor_after

BackupVolumeExchangeIntent and BackupVolumeTreeExchanged
  1 point_id; 2 restore_generation_id;
  3 old_full_tree_sha256; 4 new_full_tree_sha256

BackupVolumeDeleteIntent and BackupVolumeReplacedPathCleaned
  1 point_id; 2 restore_generation_id;
  3 old_full_tree_sha256; 4 new_full_tree_sha256;
  5 uint64 deletion_cursor_before; 6 uint64 deletion_ordinal;
  7 logical_path; 8 kind; 9 parent_logical_path

BackupVolumeServiceProgress
  1 uint64 service_cursor; 2 service_id; 3 phase; 4 observation_sha256

BackupVolumeServicePhase:
  0 UNSPECIFIED; 1 NOT_RUNNING; 2 STOP_INTENT; 3 STOPPED;
  4 RESTART_INTENT; 5 RESTARTED; 6 HEALTH_WAIT; 7 HEALTHY
```

Intent and completion shapes repeat byte-identical facts except for the
completion's after cursor or enclosing checkpoint tag.

## Restore adoption and prune

```text
BackupOriginalIdentity
  1 bytes original_execution_id                    exactly 32 bytes
  2 RevisionDigest destination
  3 string original_assignment_id

BackupAdoptedIdentity
  1 bytes adoption_id                              exactly 32 bytes
  2 bytes predecessor_execution_id                 exactly 32 bytes
  3 CheckpointFence predecessor_fence
  4 uint64 adoption_mod_revision
  5 string predecessor_assignment_id
  6 string adopted_assignment_id

BackupPruneObject
  1 uint64 ordinal; 2 point_id; 3 RevisionDigest point;
  4 BackupArtifactEvidence evidence; 5 BackupObjectIdentity object;
  6 uint32 metadata_count                          exactly 7
  7 bytes metadata_sha256                         exactly 32 bytes

BackupPruneObjectDeleted
  1 uint64 ordinal; 2 point_id; 3 BackupObjectIdentity object
```

## Clean replacement rules

Implementation replaces the old checkpoint kind enum and its individual
`...Checkpoint` payloads with the final typed request above. It removes the old
Backup source/upload DTOs when their authority roles move into the final sealed
messages. It does not keep both meanings, translate old data, or negotiate.

The current tag collision is the sole newly observed unresolved wire issue from
this documentation migration. It is not permission to renumber either side
without an owner decision and a synchronized protobuf, persistence, Controller,
Agent, client, documentation, and delivery change.
