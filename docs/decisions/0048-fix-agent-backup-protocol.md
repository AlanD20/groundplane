# ADR 0048: Fix the Agent Backup protocol

- Status: Accepted
- Date: 2026-08-30
- Accepted: 2026-08-30
- Owners: Controller and Agent
- Related: ADR 0010, ADR 0020, ADR 0022, ADR 0024, ADR 0029, ADR 0035, ADR 0045, ADR 0046, ADR 0047

## Context

The checked-in Agent protocol is an incomplete pre-implementation contract. It
does not make Backup execution replay-safe, does not bound every stream and
durable record, and does not carry enough authority to recover retained staging
files without guessing. Backup also needs exact Config, Volume, PostgreSQL,
object-store, retention, restore-adoption, and deadline semantics before its
protobuf can be implemented.

This ADR fixes that contract in place. There is one final MVP protocol schema,
schema 1. There is no second schema, compatibility decoder, translation layer,
dual writer, store marker, or store conversion. Nothing in this ADR claims that
the protobuf, generated code, Controller, Agent, tests, or live S3 proof already
implements the decision.

## Decision

### 1. One local authenticated schema-1 channel

The Controller serves the Agent bidirectional gRPC channel only on the local
Unix-domain socket selected by machine bootstrap. The channel does not listen
on TCP. It uses the bootstrap token on every new stream and does not use TLS;
filesystem ownership and mode protect the socket, while the token authenticates
the Agent process.

`Authenticate.execution_plan_schema` is required and MUST equal `1`.
Protobuf's omitted/default value `0`, every unknown value, and every value other
than `1` are rejected before recovery or ordinary work is admitted. Every
`ExecutionPlan.schema` MUST equal `1`; the Agent rejects an assignment before
acknowledgement when it does not.

Schema 1 is the sole final MVP meaning of these messages. The incomplete,
unreleased meanings are cleanly replaced. Implementations MUST NOT retain an
alternate decoder, fallback, translation, or negotiation path.

Authenticate tag 5 is the sole PostgreSQL Backup executable capability. It
carries exactly the 32 decoded bytes of ADR 0047's
`managed_release_sha256`. Controller and Agent compare those bytes before
ordinary Backup admission. The field is not a mutable image reference, a
whole-ledger digest, or a generic adapter registry.

An actual ADR 0047 managed PostgreSQL asset and its registry-readback release
record are prerequisites to implementing this protocol. An implementation
MUST NOT make tag 5 optional, send an empty or zero digest, or fabricate a
placeholder digest while that release authority is unavailable.

PostgreSQL authority exists only when the authenticated value equals the
Controller's embedded ADR 0047 managed release digest. The same release record
controls the plan, Compose image, observed image, helper, recovery protocol,
and exit table. No second digest, fallback, or compatibility decoder exists.

The Agent samples `process_generation` once with the OS CSPRNG before its first
Authenticate in a process, reuses those exact 16 bytes for every reconnect in
that process, and samples a different generation after process restart. Zero or
changed-within-process generation is invalid.

Three generation identities are distinct. Durable `LocalAgent.Generation`
remains the Controller-owned positive `uint64` credential/configuration fence
and is named `AgentGeneration` in durable Go records and
`agent_record_generation` where a persistence protobuf must disambiguate it.
Wire `process_generation` is the exact 16-byte process/session identity above;
it is never persisted as the durable Agent generation. `assignment_generation`
is the Controller-owned positive `uint64` assignment fence defined in section
3. No one may substitute one generation for another.

The legacy readiness identifiers `backupAgentSchema2Available` and
`requireBackupAgentSchema2` are replaced everywhere by schema-1 readiness and
schema-1 assignment-gate names. Those legacy identifiers MUST NOT survive in
protobuf, Controller, Agent, Console store/types, fixtures, tests, or operator
text.

The incomplete `AgentConfig` field
`map<string,string> labels = 3` is clean-replaced, at the same tag and
length-delimited embedded-message wire class, by
`repeated AgentLabel labels = 3`. `AgentLabel` is
`message AgentLabel { string key = 1; string value = 2; }`. Entries sort by key
then value, key is unique, and the bounds below apply. No raw-map decoder,
fallback, or translation exists.

This is the sole schema-1 outer-message and assignment tag table. Every audited
checked-in field and reservation keeps its number. New Backup and terminal
fields attach only after the live Log, Script, and ManagedConfig fields; no
second table or compatibility numbering exists:

```proto
message Authenticate {
  string agent_id = 1;                  // exact canonical agt_ id, 30 bytes
  bytes token = 2;                      // exactly 32 bytes
  uint32 execution_plan_schema = 3;     // required, exactly 1
  bytes process_generation = 4;         // exactly 16 random bytes per process start
  bytes postgres16_managed_release_sha256 = 5; // exactly 32 decoded ADR 0047 bytes
}
message AgentConfig {
  int32 pull_interval_seconds = 1;
  int32 max_concurrent_tasks = 2;
  repeated AgentLabel labels = 3;       // clean replacement; sorted and unique
}
message Ready {
  int32 capacity = 1;                   // 0..32 free worker slots
  string version = 2;                   // 1..128 UTF-8 bytes, no NUL
  string operating_system = 3;
  string architecture = 4;
  string architecture_variant = 5;
  optional bool terminal_delivery_clean = 6; // presence required
}
message AgentMessage {
  oneof payload {
    Authenticate authenticate = 1;
    Ready ready = 2;
    TaskEvent task_event = 3;
    ObservedState observed_state = 4;
    TaskAck task_ack = 5;
    BackupCheckpointRequest backup_checkpoint_request = 6;
    LogEvent log_event = 7;
    LogEnd log_end = 8;
    LogReady log_ready = 9;
    ScriptCheckpointRequest script_checkpoint_request = 10;
    BackupConfigTransfer backup_config_transfer = 11;
    BackupConfigCredit backup_config_credit = 12;
    BackupStagingInventory backup_staging_inventory = 13;
    BackupStagingRecoveryAck backup_staging_recovery_ack = 14;
    BackupVolumeManifestTransfer backup_volume_manifest_transfer = 15;
    BackupVolumeManifestAckCredit backup_volume_manifest_ack_credit = 16;
    TaskTerminalReceiptApplied task_terminal_receipt_applied = 17;
    TaskTerminalAssignmentRetired task_terminal_assignment_retired = 18;
    ExecutionStepResultRequest execution_step_result_request = 19;
  }
}
message ControllerMessage {
  oneof payload {
    TaskAssignment task_assignment = 1;
    TaskAbort task_abort = 2;
    ConfigUpdate config_update = 3;
    Shutdown shutdown = 4;
    MaterializationTransfer materialization_transfer = 5;
    BackupCheckpointAck backup_checkpoint_ack = 6;
    BackupSecretSlotTransfer backup_secret_slot_transfer = 7;
    LogSubscribe log_subscribe = 8;
    LogCancel log_cancel = 9;
    ManagedConfigTransfer managed_config_transfer = 10;
    ScriptCheckpointAck script_checkpoint_ack = 11;
    BackupConfigTransfer backup_config_transfer = 12;
    BackupConfigCredit backup_config_credit = 13;
    BackupStagingRecoveryPlan backup_staging_recovery_plan = 14;
    BackupVolumeManifestTransfer backup_volume_manifest_transfer = 15;
    BackupVolumeManifestAckCredit backup_volume_manifest_ack_credit = 16;
    TaskTerminalReceiptAck task_terminal_receipt_ack = 17;
    TaskTerminalReceiptAppliedAck task_terminal_receipt_applied_ack = 18;
    BackupStagingRecoveryAckReceipt backup_staging_recovery_ack_receipt = 19;
    TaskTerminalAssignmentRetiredAck task_terminal_assignment_retired_ack = 20;
    ExecutionStepResultAck execution_step_result_ack = 21;
    TaskEventAck task_event_ack = 22;
  }
}
message TaskAssignment {
  string task_id = 1;
  string operation_id = 2;
  string retry_of = 3;
  ExecutionPlan plan = 4;
  reserved 5;
  reserved "timeout_seconds";
  string assignment_id = 6;
  google.protobuf.Timestamp forward_deadline = 7;
  ScriptAssignmentArtifacts script_artifacts = 8;
  repeated ScriptExecutionCheckpoint script_checkpoints = 9;
  bool automatic_reconcile = 10;
  BackupTaskAuthority backup_authority = 11;
  BackupTaskResume backup_resume = 12;
  bytes backup_authority_sha256 = 13;   // exactly 32 bytes
  uint64 assignment_generation = 14;   // 1..MaxInt64
  repeated ExecutionStepResult acknowledged_step_results = 15;
  uint32 execution_epoch = 16;          // positive; distinct from assignment_generation
  TaskExecutionMode execution_mode = 17;
  google.protobuf.Timestamp recovery_deadline = 18;
  ReleaseRestorationAuthority restoration_authority = 19; // ADR 0064 authority
  bytes release_recovery_record_sha256 = 20;
  ReleaseRecoveryDirective release_recovery_directive = 21;
  google.protobuf.Timestamp execution_deadline = 22;
}
message TaskAck {
  string task_id = 1;
  bytes plan_hash = 2;
  TaskTerminal terminal = 3;
  int32 exit_code = 4;
  oneof result {
    ComposeTaskResult compose_result = 5;
    EnvironmentDirectoryTaskResult environment_directory_result = 6;
    BackupTaskResult backup_result = 10;
  }
  string assignment_id = 7;
  uint32 execution_epoch = 8;
  bytes release_recovery_record_sha256 = 9;
  uint64 assignment_generation = 11;   // 1..MaxInt64
}
message TaskAbort {
  string task_id = 1;
  string reason = 2;                     // valid UTF-8, at most 256 bytes
  string assignment_id = 3;
  uint64 assignment_generation = 4;     // 1..MaxInt64
  bytes plan_hash = 5;                  // exactly 32 bytes
  TaskTerminal terminal = 6;            // exact non-UNSPECIFIED disposition
  bytes terminal_receipt_sha256 = 7;    // exactly 32 bytes
  uint64 durable_task_mod_revision = 8; // 1..MaxInt64
  optional bytes rejected_task_ack_sha256 = 9; // absent or exactly 32 bytes
  // The encoded TaskAbort envelope is at most 512 bytes.
}
enum TaskTerminal {
  TASK_TERMINAL_UNSPECIFIED = 0;
  TASK_TERMINAL_COMPLETED = 1;
  TASK_TERMINAL_FAILED = 2;
  TASK_TERMINAL_TIMED_OUT = 3;
  TASK_TERMINAL_ABORTED = 4;
}
message TaskTerminalReceiptAck {
  bytes process_generation = 1;         // exact authenticated 16 bytes
  string task_id = 2;                   // exact canonical task_ id
  string assignment_id = 3;             // exact canonical asgn_ id
  uint64 assignment_generation = 4;     // 1..MaxInt64
  bytes plan_hash = 5;                  // exactly 32 bytes
  TaskTerminal terminal = 6;
  bytes task_ack_sha256 = 7;            // exactly 32 bytes
  bytes terminal_receipt_sha256 = 8;    // exactly 32 bytes
  uint64 durable_task_mod_revision = 9; // 1..MaxInt64
}
message TaskTerminalReceiptApplied {
  bytes process_generation = 1;         // exact authenticated 16 bytes
  string task_id = 2;                   // exact canonical task_ id
  string assignment_id = 3;             // exact canonical asgn_ id
  uint64 assignment_generation = 4;     // 1..MaxInt64
  bytes plan_hash = 5;                  // exactly 32 bytes
  TaskTerminal terminal = 6;
  bytes task_ack_sha256 = 7;            // exactly 32 bytes
  bytes terminal_receipt_sha256 = 8;    // exactly 32 bytes
  uint64 durable_task_mod_revision = 9; // 1..MaxInt64
}
message TaskTerminalReceiptAppliedAck {
  bytes process_generation = 1;         // exact authenticated 16 bytes
  string task_id = 2;                   // exact canonical task_ id
  string assignment_id = 3;             // exact canonical asgn_ id
  uint64 assignment_generation = 4;     // 1..MaxInt64
  bytes plan_hash = 5;                  // exactly 32 bytes
  TaskTerminal terminal = 6;
  bytes task_ack_sha256 = 7;            // exactly 32 bytes
  bytes terminal_receipt_sha256 = 8;    // exactly 32 bytes
  uint64 durable_task_mod_revision = 9; // 1..MaxInt64
}
message BackupStagingRecoveryAckReceipt {
  bytes process_generation = 1;         // exact authenticated 16 bytes
  bytes inventory_sha256 = 2;           // exactly 32 bytes
  bytes applied_plan_sha256 = 3;        // exactly 32 bytes
  bytes recovery_ack_sha256 = 4;        // exactly 32 bytes
}
message TaskTerminalAssignmentRetired {
  bytes process_generation = 1;         // exact authenticated 16 bytes
  string task_id = 2;
  string assignment_id = 3;
  uint64 assignment_generation = 4;     // 1..MaxInt64
  bytes plan_hash = 5;                  // exactly 32 bytes
  TaskTerminal terminal = 6;
}
message TaskTerminalAssignmentRetiredAck {
  bytes process_generation = 1;         // exact authenticated 16 bytes
  string task_id = 2;
  string assignment_id = 3;
  uint64 assignment_generation = 4;     // 1..MaxInt64
  bytes plan_hash = 5;                  // exactly 32 bytes
  TaskTerminal terminal = 6;
}
```

For a Backup assignment, fields 11, 12, and 13 are required and
`backup_authority_sha256` is the sole authority digest. For a non-Backup
assignment those three are absent. Field 14 is required for every assignment.
`assignment_id` is exact canonical `asgn_`
identity and is included in authority, every stream transfer, checkpoint,
terminal acknowledgement, and adoption fence. `TaskAck.backup_result` is valid
only with a non-`UNSPECIFIED` `TaskTerminal` outcome and the exact assignment id
and generation fence. `assignment_generation` is the Controller-owned assignment
identity fence; it is distinct from, and MUST NOT be substituted for or derived
from, the existing `execution_epoch` field. `TaskAck.execution_epoch` at tag 8
and `TaskAck.release_recovery_record_sha256` at tag 9 retain their current
meanings and validators. Tag 3 preserves the existing `TaskTerminal` enum and
MUST never be decoded or represented as a bool.

The current protobuf occupies `AgentMessage` tag 19, `ControllerMessage` tags
21 and 22, `TaskAssignment` tags 15 through 22, and `TaskAck` tags 8 and 9.
Those names, types, and numbers are retained exactly. The accepted Backup outer
additions use only free `AgentMessage` tags 11 through 18 and
`ControllerMessage` tags 12 through 20; the accepted `TaskAssignment` additions
use only free tags 11 through 14; and the accepted `TaskAck` additions use only
free tags 10 and 11. Current `TaskAbort` ends at tag 3, so its final additions at
tags 4 through 9 remain free. These are clean schema-1 additions, not alternate
wire shapes.

### 2. Wire envelopes, sizes, and stream scheduling

Every limit in this section applies to `proto.Size` of the complete outer
protobuf envelope and excludes only the five-byte gRPC frame header. Senders
check the variant and global caps before enqueueing. Receivers check both after
decode and before dispatch. gRPC compression is disabled.

| Direction and variant | Maximum serialized bytes |
| --- | ---: |
| any `AgentMessage` | 16,777,216 |
| `Authenticate` | 1,609 |
| `Ready` | 256 |
| `TaskEvent` | 65,536 |
| `ObservedState` | 16,777,216 |
| `TaskAck` | 16,777,216 |
| `BackupCheckpointRequest` | 16,384 |
| `BackupConfigTransfer` | 262,144 |
| `BackupConfigCredit` | 512 |
| `BackupStagingInventory` | 4,259 |
| `BackupStagingRecoveryAck` | 72 |
| `BackupVolumeManifestTransfer` | 262,144 |
| `BackupVolumeManifestAckCredit` | 512 |
| `TaskTerminalReceiptApplied` | 512 |
| `TaskTerminalAssignmentRetired` | 512 |
| any `ControllerMessage` | 5,242,880 |
| `TaskAssignment` | 5,242,880 |
| `TaskAbort` | 512 |
| `ConfigUpdate` | 65,536 |
| `Shutdown` | 16 |
| `MaterializationTransfer` | 65,536 |
| `BackupCheckpointAck` | 512 |
| `BackupSecretSlotTransfer` | 65,536 |
| `BackupConfigTransfer` | 262,144 |
| `BackupConfigCredit` | 512 |
| `BackupStagingRecoveryPlan` | 5,861 |
| `BackupVolumeManifestTransfer` | 262,144 |
| `BackupVolumeManifestAckCredit` | 512 |
| `TaskTerminalReceiptAck` | 512 |
| `TaskTerminalReceiptAppliedAck` | 512 |
| `BackupStagingRecoveryAckReceipt` | 512 |
| `TaskTerminalAssignmentRetiredAck` | 512 |

The Controller gRPC server sets `MaxRecvMsgSize=16,777,216` and
`MaxSendMsgSize=5,242,880`. The Agent call sets
`MaxCallRecvMsgSize=5,242,880` and `MaxCallSendMsgSize=16,777,216`.
Authenticate fields 1..4 cost 86 bytes. Tag 5 adds 34 bytes, so Authenticate is
exactly 120 bytes at its maximum. Its complete AgentMessage variant is 122
bytes. The 32-record connection-control queue therefore reserves exactly 3,904
bytes for 32 maximum Authenticate envelopes within its unchanged 262,144-byte
cap.

Moving the accepted `TaskAck.backup_result` and `assignment_generation` to
free tags 10 and 11 does not change their protobuf key widths. Retaining the
occupied `uint32 execution_epoch` at tag 8 adds at most six bytes and retaining
the occupied 32-byte recovery-record digest at tag 9 adds at most 34 bytes
relative to the incomplete Backup draft. The nested `TaskAck` payload therefore
grows by at most 40 bytes. Its enclosing `AgentMessage` field-5 length prefix
can grow by one byte at a varint boundary, so the complete outer envelope grows
by at most 41 bytes. Both remain subject to the unchanged 16,777,216-byte
variant and global `AgentMessage` caps and the complete-envelope serialized-size
check.

The deterministic serialization of an `ExecutionPlan`, with its 32-byte
`plan_hash` field populated, is at most 4,194,304 bytes. A `TaskAssignment`,
including plan, authority, and resume, still fits 5,242,880 bytes. At most 32
Tasks may be assigned and non-terminal on one Agent connection. Each Task has at
most four active credit-controlled bulk-transfer queues, plus the separately
bounded Materialization and secret-slot inline queues below.

`BackupSecretSlotTransfer` keeps the checked-in schema-1 tags: task 1,
assignment 2, step 3, purpose 4, and Header/Chunk/End records 5/6/7. With each
identifier exactly 31 bytes and purpose/sequence/chunk-count values fitting one
varint byte, the exact deterministic `proto.Size` ceilings are:

| Secret-slot message or complete outer variant | Exact ceiling (bytes) |
| --- | ---: |
| credential `BackupSecretSlotHeader` (`total_bytes<=262144`, `chunk_count<=8`) | 6 |
| age-identity `BackupSecretSlotHeader` (`total_bytes<=4096`, `chunk_count=1`) | 5 |
| `BackupSecretSlotChunk` (`content<=32768`) | 32,774 |
| `BackupSecretSlotEnd` | 2 |
| credential `BackupSecretSlotTransfer.header` | 109 |
| age-identity `BackupSecretSlotTransfer.header` | 108 |
| `BackupSecretSlotTransfer.chunk` | 32,879 |
| `BackupSecretSlotTransfer.end` | 105 |
| complete credential `ControllerMessage.header` | 111 |
| complete age-identity `ControllerMessage.header` | 110 |
| complete `ControllerMessage.chunk` | 32,883 |
| complete `ControllerMessage.end` | 107 |

The chunk calculation is `2 + 1 + 3 + 32768 = 32774` for sequence plus the
length-delimited content; its transfer wrapper is
`101 + 1 + 3 + 32774 = 32879`, and Controller field 7 adds `1+3`. Header and
End use one-byte nested lengths. Both S3 credential purposes require 1..8
canonical chunks totaling 1..262,144 bytes; each age-identity purpose requires
exactly one 1..4,096-byte chunk. Implementations reject a record above its exact
variant ceiling even though the connection's conservative outer class cap is
65,536 bytes.

The following nested bounds are part of schema 1, not implementation defaults:

| Value | Bound |
| --- | --- |
| canonical `task_`, `asgn_`, `plan_`, `step_` id | exactly the kind prefix plus 26-character ULID, 31 bytes |
| canonical three-letter-kind stable id | exactly 30 bytes |
| canonical two-letter-kind stable id | exactly 29 bytes |
| SHA-256 or authority/transfer digest | exactly 32 bytes |
| Docker container id | exactly 64 lowercase hexadecimal bytes |
| Ready capacity | 0..32 |
| Ready release version | 1..128 UTF-8 bytes, no NUL |
| AgentConfig labels | 0..64 sorted unique pairs; key 1..128 UTF-8 bytes, value 0..256 UTF-8 bytes, no NUL |
| plan artifacts | 0..16; canonical YAML <=1,048,576 bytes each |
| Blueprint input files | 0..64, each <=262,144 bytes, aggregate <=786,432 bytes, path <=240 bytes |
| resolved YAML graph | <=100,000 nodes, <=1,000 aliases, alias depth <=16 |
| resolved Blueprint resources | <=512 aggregate |
| Services, networks, Volumes, common Service facts | sorted unique, each list <=512; Service id exactly 30 bytes |
| one Config Entry exposure Service-ID list | sorted unique, <=128 exact 30-byte Service ids |
| Compose service name | 1..255 bytes |
| Backup source steps | 1..12 |
| prune steps | 1..11 |
| adapter key | 1..64 bytes |
| PostgreSQL database, role, adapter identity | 1..63 accepted ASCII bytes |
| adapter secret input | 1..256 bytes |
| materialization destination | 1..240 bytes |
| materialization content | <=1,048,576 bytes; <=32 chunks of <=32,768 bytes |
| Connector endpoint | canonical URL, 1..2,048 UTF-8 bytes |
| Connector region | 1..64 bytes, each exactly in ASCII range `0x21..0x7e` |
| S3 bucket | 3..63 accepted bucket bytes |
| S3 prefix | 0..1,024 bytes |
| complete S3 object key | 1..1,024 bytes |
| S3 credential slots | exactly two; each value 1..262,144 bytes in <=8 canonical 32-KiB chunks |
| age identity slot | 1..4,096 bytes in one chunk |
| present VersionId | 0..1,024 bytes; empty-present differs from absent |
| ETag | 1..1,024 bytes |
| stored object | 0..5,497,558,138,880 bytes (5 TiB) |
| plaintext with encryption none | 0..5,497,558,138,880 bytes |
| plaintext with age X25519 | 0..5,496,216,289,016 bytes |
| execution id and bulk transfer id | exactly 32 bytes |
| Config Entry id | exactly 29 bytes |
| Config generation or metadata snapshot id | exactly 30 bytes |
| Config Entry environment key | 1..255 bytes in the accepted grammar |
| Config Entry file destination | 1..240 bytes |
| Config selected value | 0..262,144 bytes, 0..8 canonical chunks |
| Config Entries/value bytes | 0..4,096 Entries; aggregate values <=1,073,741,824 bytes |
| Volume manifest | 1..2,048 Entries including root; path <=4,095 bytes; encoded Entry <=8,192 bytes |
| recovery inventory | 0..32 stages, each exactly 1..2 closed-role finals |

The shared `BackupTaskAuthority` is at most 522,752 deterministic bytes and
`BackupTaskResume` is at most 65,536. Every `RevisionDigest.mod_revision`,
assignment generation, durable Task revision, and etcd-derived revision is a
positive integer at most `MaxInt64`; its protobuf varint is therefore at most
nine bytes. `RevisionDigest` is exactly at most 44 bytes.

The authority proof accounts for every repeated wrapper. `BackupServiceFact`
is at most 502 bytes: 32 for the 30-byte Service id, 258 for the 255-byte current
name, two 46-byte wrapped RevisionDigests, 50 for prior intent, two for label
count, and 34 each for label and repository digests. Task-authority field 13 adds
one tag and a two-byte length, so each repeated fact costs 505 and 512 cost
258,560. Common nonrepeated authority fields are capped by actual deterministic
encoding at 8,192 bytes. Each `BackupStepAuthority` is at most 65,536 bytes and
field 14 adds one tag plus a three-byte length; the sum of all 1..12 encoded step
members including those four-byte wrappers is capped at 256,000. Thus
`8192+258560+256000=522752`, below 524,288. Step consumer ids are only 32-byte
repeated references to the sole common facts and count inside the step and
aggregate caps; no Service fact is duplicated per source.

The complete assignment proof accounts for the occupied fields as well as the
Backup additions. The maximum populated plan including its wrapper is
4,194,309 bytes, authority plus field-11 wrapper is 522,756, resume plus
field-12 wrapper is 65,540, and the authority digest is 34. Those four fields
consume at most 4,782,639 bytes. The five-byte `ControllerMessage` field-1
wrapper leaves 460,236 bytes under the 5,242,880-byte complete-envelope cap for
all other `TaskAssignment` fields and wrappers, including fields 1..3, 6..10,
and 14..22. Their message-specific bounds and this complete-envelope bound both
apply; one bound does not relax the other. Absence of Config Entries and Volume
manifests from the assignment is required. Every sender still checks actual
`proto.Size` of the final outer envelope.

A checkpoint contains no manifest batch. The largest checkpoint payload is a Volume delete intent:
8,351 bytes for two 4,095-byte paths plus IDs, digests, cursors, and kind. With
checkpoint wrappers and maximum common fields the complete Agent outer is at
most 8,586 bytes. A maximum acknowledgement outer is 194 bytes. Thus every
value admitted by the nested bounds fits its required envelope; the outer
`proto.Size` check remains defense in depth.

The new Connector endpoint/region limits and label limits are required MVP
mirror changes. ADR 0005 and ADR 0045 must be synchronized before paired
ADR 0047/0048 acceptance. No general non-Backup plan step-count rule is added.

Inbound decode does not discard unknown fields. The receiver recursively rejects
unknown fields on the outer envelope, selected oneof, and nested protocol
messages. It also rejects an unset oneof, invalid enum, non-canonical ordering,
and every length or count violation before durable or external mutation.

There is one goroutine calling `Recv` and one calling `Send` per stream. `Recv`
validates and attempts nonblocking admission to a separate bounded target queue;
it never waits on a Task worker and never drops, coalesces, or overwrites a
record. A full inbound queue terminates the stream with `RESOURCE_EXHAUSTED`. A
full outbound Task queue fails that Task; a full connection-control queue closes
the stream. Both are fail-closed and recover only through the specified durable
protocol.

| Exact variants in queue | Records | Serialized bytes | Additional payload-content cap |
| --- | ---: | ---: | ---: |
| `Authenticate`, `Ready`, `TaskAbort`, `ConfigUpdate`, `Shutdown`, `BackupConfigCredit`, `BackupVolumeManifestAckCredit`, `BackupStagingInventory`, `BackupStagingRecoveryPlan`, `BackupStagingRecoveryAck` | 32 | 262,144 | none |
| `TaskAssignment` | 1 | 5,242,880 | none |
| `BackupCheckpointRequest`, `BackupCheckpointAck` | 64 | 1,048,576 | none |
| `TaskAck`, `TaskTerminalReceiptApplied`, `TaskTerminalAssignmentRetired` | 32 | 16,777,216 | none |
| `TaskTerminalReceiptAck`, `TaskTerminalReceiptAppliedAck`, `BackupStagingRecoveryAckReceipt`, `TaskTerminalAssignmentRetiredAck` | 32 | 16,777,216 | none |
| `ObservedState` | 1 | 16,777,216 | none |
| `TaskEvent` | 128 | 1,048,576 | none |
| each `BackupConfigTransfer` or `BackupVolumeManifestTransfer` instance | 8 | 1,048,576 | 262,144 |
| one per-assignment `MaterializationTransfer` inline queue | 8 | 262,144 | 262,144 |
| one per-assignment `BackupSecretSlotTransfer` inline queue | 8 | 262,144 | 262,144 |

These are distinct queues in both directions where a variant exists; variants
never borrow another row's records or bytes. The credit-controlled bulk row is
per active Config or Volume transfer and the Task-wide maximum remains four
such active queues. Serialized bytes use complete outer `proto.Size`; content
bytes count only chunk/content payload and must satisfy both caps.

Materialization and secret slots use separately bounded inline queues, not
credit-controlled bulk queues. One assignment admits at most one active
Materialization transfer. It admits at most four active secret-slot transfers,
exactly one for each of the two credential and two closed age-identity purposes;
all four multiplex through the one per-assignment secret queue. Their producers
enqueue incrementally through these queues and the sole writer round-robins them.
They are explicitly exempt from bulk record/byte-credit eligibility, but not
from their queue record, serialized-byte, content-byte, envelope, concurrency,
or whole-assignment transfer bounds. Enqueue is nonblocking; a full outbound
inline or bulk queue fails that Task, never blocks `Recv`, and no producer calls
`Send` directly.

A maximum Materialization transfer is exactly 34 records (Header, 32 chunks,
End) and 1,054,256 complete serialized bytes: Header at most 836, each maximum
chunk 32,915, and End 140. Thus the exact per-assignment Materialization
concurrency/transfer totals are one, 34 records, 1,054,256 serialized bytes,
and 1,048,576 content bytes. It is produced incrementally and never all queued.
Each credential secret slot is at most 10 records and 263,282 serialized bytes;
each age identity is at most three records and 33,100 bytes. Across the two
credential and two closed identity purposes, one assignment admits at most 26
records, 592,764 serialized bytes, and 532,480 content bytes. Thus its exact
per-assignment secret concurrency/transfer totals are four, 26, 592,764, and
532,480 respectively. They are also produced incrementally. The exact
per-record secret-slot ceilings above remain mandatory.

A bulk record additionally has at most 262,144 content bytes; its complete outer
envelope must still fit its variant cap. Recovery inventory and plan consume at
most 8 KiB each in the control queue; recovery acknowledgement consumes at most
512 bytes.

The sole writer uses a work-conserving cyclic scan in this order: control,
assignment, checkpoint, terminal, observed, event, materialization-inline,
secret-inline, bulk. The next scan begins after the class that last emitted,
at most one record emits from a class per scan, and each of
materialization-inline, secret-inline, and bulk rotates independently by Task
then active transfer. A bulk queue is eligible only if it is a Config
metadata/value or Volume queue and both record credit and serialized-byte credit
admit its next complete outer record. Config and Volume
Start and Resume retain their explicit first-record exceptions; Config EntryHeader,
ValueChunk, EntryEnd, and TransferEnd and Volume Batch and End consume both
credits. A materialization-inline or secret-inline queue is eligible whenever
nonempty because its admission caps replace bulk credit; neither class receives
or consumes bulk credit. An ineligible queue cannot block another queue.
Producers never call `Send` directly.

One observed snapshot may use multiple envelopes. Projects sort by stable ID and
are never split. Every envelope has the same non-empty 16-byte `snapshot_id`, a
zero-based `uint64 batch_ordinal`, and `final_batch` only on the last envelope.
A project that cannot fit alone fails the observation. The Controller publishes
only a contiguous sequence from zero through the final batch and discards an
unpublished partial sequence on disconnect.

```proto
message ObservedState {
  repeated ObservedProject projects = 1;
  bytes snapshot_id = 2;       // exactly 16 bytes
  uint64 batch_ordinal = 3;
  bool final_batch = 4;
}
```

### 3. Assignment authority, deadlines, and checkpoint fences

Every Agent has one durable assignment sequence at
`/v1/runtime/agent-assignment-sequences/{agent_id}`. Absence means zero. A
successful claim transaction compares the current sequence and writes its exact
successor both to that key and to every copy of the new
`TaskAssignmentRecord.assignment_generation`; allocation and assignment happen
atomically, so a failed claim consumes no value. The first assignment is 1.
Every later new assignment, including retry or a new Task, consumes the next
value. Redispatch, reconnect, and process restart replay the same assignment id
and generation. The sequence continues across `process_generation` and durable
`AgentGeneration` changes. A future same-assignment durable replacement is one
CAS that increments the sequence and replaces every assignment copy; schema 1
currently has no such path. Exhaustion at `MaxInt64` fails closed. An etcd
`ModRevision` is never an assignment-generation value.

The sequence key is deleted only by final Agent removal after a fixed-revision
proof of no assignments, assignment indexes, timeout indexes, or non-clean
terminal deliveries. Retained clean delivery/receipt pairs do not block final
identity-sequence deletion. Credential rotation, disconnect, Task prune, and
ordinary assignment cleanup never reset it.

The plan hash covers deterministic schema-1 `ExecutionPlan` bytes with
`plan_hash` cleared. Resume position is outside the plan hash. A reconnect can
therefore advance execution without changing desired work. The Controller
assigns only authority and resume read together at their exact revisions.

Every checkpoint-sequence, resume-sequence, dedupe-sequence, transfer-record
sequence, attempt, and fencing revision is protobuf `uint64`; no component of
one of those sequences uses a narrower type. Durable byte offsets and mutation
cursors are `uint64`. A bounded collection count or ordinal may be `uint32`
exactly where its declared maximum fits: Config Entry ordinal/count <=4,096,
Volume Entry ordinal/count <=2,048 on transfer messages, and corresponding next
ordinals. Each operation defines its valid start; those bounded ordinals are
one-based while completed cursors may be zero. Schema and prose use the same
width. Increment or conversion overflow is a permanent protocol error.

Checkpoint requests start at sequence 1 and are contiguous/nonzero. A fresh
assignment resume may carry sequence 0 with no predecessor fence; after the
first completion it carries the exact last nonzero sequence and fence.

Canonical Backup Task authority is one deterministic protobuf with no maps.
There is exactly one project, Environment, deadline, assignment fence, and
sorted common Service fact set, plus 1..12 ordered unique step authorities keyed
by `step_id`. Step order exactly matches the Backup steps in the sealed plan.
Every Task resume contains one matching ordered step summary and no history.
Repeated fields use their specified order. Strings are valid UTF-8 without
normalization, IDs use canonical lowercase product form, and SHA-256 values are
32 raw bytes.

```proto
message BackupTaskAuthority {
  uint32 authority_schema = 1; // exactly 1
  string task_id = 2;
  string operation_id = 3;
  uint64 task_attempt = 4;
  bytes plan_hash = 5;                  // exactly 32 bytes
  string project_id = 6;
  RevisionDigest project = 7;
  string environment_id = 8;
  RevisionDigest environment = 9;
  int64 task_deadline_unix_nano = 10;
  string assignment_id = 11;            // exact canonical asgn_ id, 31 bytes
  uint64 assignment_generation = 12;    // 1..MaxInt64
  repeated BackupServiceFact services = 13; // <=512, sorted service_id
  repeated BackupStepAuthority steps = 14;  // 1..12, sorted unique step_id
}
message BackupStepAuthority {
  string step_id = 1;
  bytes execution_id = 2;               // exactly 32 bytes
  bytes step_digest = 3;                // exactly 32 bytes
  int64 step_deadline_unix_nano = 4;
  repeated string consumer_service_ids = 5; // sorted unique refs into task services
  oneof operation {
    BackupCaptureAuthority capture = 10;
    BackupPruneAuthority prune = 11;
    BackupRestoreAuthority restore = 12;
  }
}
message CheckpointFence {
  bytes authority_digest = 1;
  uint64 dedupe_key_mod_revision = 2;
}
message BackupTaskResume {
  string assignment_id = 1;             // exact canonical asgn_ id
  uint64 assignment_generation = 2;     // 1..MaxInt64
  repeated BackupStepResume steps = 3;  // 1..12, exact authority order
}
message BackupStepResume {
  string step_id = 1;
  bytes execution_id = 2;               // exactly 32 bytes
  oneof operation {
    BackupCaptureResume capture = 10;
    BackupPruneResume prune = 11;
    BackupRestoreResume restore = 12;
  }
}
message BackupCaptureResume {
  uint64 checkpoint_sequence = 1;
  CheckpointFence preceding_checkpoint = 2;
  BackupCapturePhase phase = 3;
  BackupCaptureCursor cursor = 4;
  oneof immediately_preceding_payload {
    BackupArtifactPrepared artifact_prepared = 10;
    BackupUploadVerified upload_verified = 11;
    BackupSourceCleanupCompleted source_cleanup_completed = 12;
    BackupPostgresContainerObserved postgres_container_observed = 13;
    BackupPostgresDumpStart postgres_dump_start = 14;
    BackupVolumeProgress volume_progress = 15;
    BackupConfigProgress config_progress = 16;
    BackupUploadCompleted upload_completed = 17;
  }
}
message BackupPruneResume {
  uint64 checkpoint_sequence = 1;
  CheckpointFence preceding_checkpoint = 2;
  uint64 next_object_ordinal = 3;
  oneof immediately_preceding_payload {
    BackupPruneObjectDeleted object_deleted = 10;
  }
}
message BackupRestoreResume {
  uint64 checkpoint_sequence = 1;
  CheckpointFence preceding_checkpoint = 2;
  BackupRestorePhase phase = 3;
  BackupRestoreCursor cursor = 4;
  oneof immediately_preceding_payload {
    BackupRestoreArtifactValidated artifact_validated = 10;
    BackupPostgresContainerObserved postgres_container_observed = 11;
    BackupPostgresRestoreApplyStartCheckpoint postgres_restore_apply_start = 12;
    BackupConfigProgress config_progress = 13;
    BackupVolumeProgress volume_progress = 14;
    BackupPostgresRestoreVerified postgres_restore_verified = 15;
    BackupPostgresServiceProgress postgres_service_progress = 16;
  }
}
enum BackupCapturePhase {
  BACKUP_CAPTURE_PHASE_UNSPECIFIED = 0;
  BACKUP_CAPTURE_PHASE_CAPTURING = 1;
  BACKUP_CAPTURE_PHASE_ARTIFACT_PREPARED = 2;
  BACKUP_CAPTURE_PHASE_UPLOADING = 3;
  BACKUP_CAPTURE_PHASE_HEAD_VERIFICATION = 4;
  BACKUP_CAPTURE_PHASE_POINT_COMMIT = 5;
  BACKUP_CAPTURE_PHASE_SOURCE_CLEANUP = 6;
  BACKUP_CAPTURE_PHASE_SERVICE_RECOVERY = 7;
  BACKUP_CAPTURE_PHASE_TERMINAL = 8;
}
enum BackupRestorePhase {
  BACKUP_RESTORE_PHASE_UNSPECIFIED = 0;
  BACKUP_RESTORE_PHASE_ARTIFACT_VALIDATION = 1;
  BACKUP_RESTORE_PHASE_TARGET_PREPARATION = 2;
  BACKUP_RESTORE_PHASE_TARGET_MUTATION = 3;
  BACKUP_RESTORE_PHASE_PUBLICATION = 4;
  BACKUP_RESTORE_PHASE_CLEANUP = 5;
  BACKUP_RESTORE_PHASE_SERVICE_RECOVERY = 6;
  BACKUP_RESTORE_PHASE_TERMINAL = 7;
}
message BackupCaptureCursor {
  uint64 object_attempt = 1;
  uint64 config_record_sequence = 2;
  uint64 config_value_ordinal = 3;
  uint64 service_cursor = 4;
  bytes cumulative_chain_sha256 = 5;   // exactly 32 bytes
}
message BackupRestoreCursor {
  uint64 object_attempt = 1;
  uint64 config_record_sequence = 2;
  uint64 config_value_ordinal = 3;
  uint64 volume_cursor = 4;
  uint64 service_cursor = 5;
  bytes cumulative_chain_sha256 = 6;   // exactly 32 bytes
}
```

`authority_digest` is the `groundplane.agent.v1.backup-authority` digest defined
below and is byte-equal to `TaskAssignment.backup_authority_sha256`; no other
field is an alternate authority digest.
Resume position, checkpoint payload, checkpoint sequence, pending claim, and the
resulting acknowledgement fence are not hashed into authority. A resume is valid
only when execution ID, digest, exact completed-dedupe-key `ModRevision`, phase,
cursor, and immediately preceding payload match one Controller read transaction.
Historical rows remain dedupe/audit evidence and are not copied into the claim.

Cumulative resume state is bounded to current phase, fixed full-tree or archive
digests, cursors, at most one pending destructive intent, Service progress, and
the immediately preceding checkpoint/fence. It does not grow with history.

#### 3.1 Canonical digest framing

Every digest named by this ADR uses SHA-256 and the same primitive framing.
Every compare-plus-mutation count is the Groundplane application's combined
transaction-operation budget, not etcd's server max-txn-ops setting.
`D(domain, body)` means
`SHA-256(ASCII(domain) || 0x00 || body)`. `B(x)` is
`u32be(len(x)) || x`. `E(m)` is the raw deterministic protobuf encoding of `m`
after recursive unknown-field rejection and with the message's resulting digest
field cleared; `P(m)` is shorthand for `u32be(len(E(m))) || E(m)` only where a
non-Config/Volume row below uses that shorthand. Every Config or Volume digest
protobuf member is written explicitly as `u32be(len(E)) || E` exactly once.
`P(m)` MUST NOT receive another length prefix and MUST NOT be used as though it
were raw `E(m)`.
Integers are unsigned big-endian at their declared 32/64-bit width, booleans are
one byte 0/1, and enums are u32 BE. No map, native integer, JSON number, protobuf
wrapper, or textual length enters a digest unless listed below.

| Digest field | Exact preimage after domain and NUL |
| --- | --- |
| plan step digest, domain `groundplane.agent.v1.execution-step` | `plan_hash || P(ExecutionStep)` |
| Agent terminal acknowledgement, `groundplane.agent.v1.task-terminal-result` | `P(TaskAck)` over the sole final schema-1 shape: fields 1..4, the selected result at 5, 6, or 10, `assignment_id` at 7, `execution_epoch` at 8, `release_recovery_record_sha256` at 9, and required `assignment_generation` at 11 |
| canonical terminal Task, `groundplane.task.terminal-record.v1` | `B(the exact canonical terminal Task primary value bytes)` |
| durable terminal receipt, `groundplane.agent.v1.task-terminal-receipt` | `P(TaskTerminalReceiptRecord)`; the record has no digest field |
| Backup authority, `groundplane.agent.v1.backup-authority` | `P(BackupTaskAuthority)` |
| Config metadata content, `groundplane.backup.config-metadata-content.v1` | `u32be(entry_count)`, then for each canonical ordinal the direction-independent artifact projection bytes `E_i` as `u32be(len(E_i)) || E_i`, where `E_i=E(BackupConfigRestoreEntry_i)` |
| Config metadata transfer seed, `groundplane.agent.v1.config-metadata-transfer-chain` | `authority_digest || transfer_id || u32be(direction) || u32be(len(E_a)) || E_a`, where `E_a=E(BackupConfigContentAuthority)` |
| Config metadata member | previous 32-byte chain, then `u32be(ordinal) || u32be(len(E_i)) || E_i`, where direction selects exactly `E_i=E(BackupConfigEntry_i)` for capture or `E_i=E(BackupConfigRestoreEntry_i)` for restore |
| Config value seed, `groundplane.agent.v1.config-value-chain` | final metadata chain |
| Config value member | previous chain, `u32be(ordinal) || u32be(chunk_count) || u64be(value_size) || value bytes` |
| Volume content manifest, `groundplane.backup.volume-manifest-content.v1` | `u32be(entry_count)`, then for each canonical ordinal `u32be(len(E_i)) || E_i`, where `E_i=E(BackupVolumeManifestEntry_i)` |
| Volume transfer seed, `groundplane.agent.v1.volume-manifest-transfer-chain` | `authority_digest || transfer_id || u32be(direction) || u32be(role) || content_manifest_sha256 || full_tree_sha256 || u32be(entry_count) || source-evidence`, where source-evidence is `u32be(1) || u64be(size) || source_sha256` when present or `u32be(2)` when role-required absent |
| Volume transfer member | previous chain then `u64be(record_sequence) || u32be(first_ordinal) || u32be(batch_count)`, then for each Batch Entry `u32be(len(E_i)) || E_i`, where `E_i=E(BackupVolumeManifestEntry_i)` |
| Volume construction seed, `groundplane.agent.v1.volume-construction-chain` | `authority_digest || new_tree_content_manifest_sha256` |
| Volume construction member | previous chain then `u32be(len(E_i)) || E_i || u32be(len(E_c)) || E_c`, where `E_i=E(BackupVolumeConstructionIntent)` and `E_c=E(BackupVolumeConstructionCompleted)` |
| Volume finalization seed, `groundplane.agent.v1.volume-finalization-chain` | `authority_digest || new_tree_content_manifest_sha256 || final_construction_chain` |
| Volume finalization member | previous chain then `u32be(len(E_i)) || E_i || u32be(len(E_c)) || E_c`, where `E_i=E(BackupVolumeFinalizationIntent)` and `E_c=E(BackupVolumeFinalizationCompleted)` |
| Service progress seed, `groundplane.agent.v1.service-progress-chain` | `authority_digest || u32be(operation_kind) || u32be(service_count)` |
| Service progress member | previous chain then `u32be(len(E_s)) || E_s`, where `E_s=E(BackupVolumeServiceProgress)` or `E_s=E(BackupPostgresServiceProgress)` as selected by operation kind |
| Config materialization, `groundplane.agent.v1.config-materialization` | `B(restore_generation_id) || u64be(render_generation)`, then for each stable-Entry-ID ordered proof `B(entry_id) || B(service_id) || B(destination) || value_sha256 || observation_sha256` |
| container labels, `groundplane.agent.v1.container-labels` | `u32be(count)`, then sorted unique pairs `B(key) || B(value)` |
| PostgreSQL verification, `groundplane.agent.v1.postgres-restore-verification` | `B(point_id) || B(container_id) || P(BackupArtifactEvidence) || B(database) || B(role) || source_sha256 || observation_sha256` |
| runtime observation, `groundplane.agent.v1.runtime-observation` | `B(service_id) || B(container_id) || repository_digest || labels_sha256 || u32be(runtime_phase)` |
| Config credit acknowledgement, `groundplane.agent.v1.config-credit-ack` | `u32be(len(E_c)) || E_c`, where `E_c=E(BackupConfigCredit)` |
| fixed S3 metadata, `groundplane.agent.v1.s3-object-metadata` | `u32be(count)`, then sorted unique `B(key) || B(value)` |

The Ack-independent durable receipt has exactly this unknown-field-free
deterministic protobuf record:

```proto
message TaskTerminalReceiptRecord {
  uint32 schema = 1;                   // value required to equal 1
  string task_id = 2;                 // exact canonical task_ id
  string agent_id = 3;                // exact canonical agt_ id
  uint64 agent_generation = 4;        // durable LocalAgent generation, 1..MaxInt64
  string assignment_id = 5;           // exact canonical asgn_ id
  uint64 assignment_generation = 6;   // 1..MaxInt64
  bytes plan_hash = 7;                 // exactly 32 bytes
  TaskTerminal terminal = 8;
  bytes terminal_task_sha256 = 9;     // exactly 32 bytes
}
```

`terminal_task_sha256` is exactly
`D("groundplane.task.terminal-record.v1", B(the exact canonical terminal Task
primary value bytes))`. `terminal_receipt_sha256` is exactly
`D("groundplane.agent.v1.task-terminal-receipt",
P(TaskTerminalReceiptRecord))`. The receipt has no TaskAck digest, Task
revision, delivery state, process generation, or receipt-digest field. Ack
digest and the Task/receipt modification revision travel only in the separate
pending/applied delivery handshake.

Canonical `P(TaskAck)` is formed only after validating every field of that
final shape. It binds the current `execution_epoch` and
`release_recovery_record_sha256` as well as the new `assignment_generation`;
no field may be cleared, defaulted, aliased, or omitted to reproduce the
incomplete draft. A decoder or hash path for the old shape is forbidden.

The chain seed is `D(domain, listed seed)` and each member is
`D(domain, listed previous-chain/member bytes)`. Empty chains are their seed.
The Config metadata content row is likewise transfer-independent and
direction-independent. Capture projects each revision-bearing
`BackupConfigEntry` to the exact artifact-derived `BackupConfigRestoreEntry`;
restore parses that same projection. Its digest therefore round-trips in the
Recovery Point and is sealed in content authority before a transfer chain is
derived. Authority digest, transfer id/direction, record sequence, credit, ack,
and replay never enter it and no digest cycle exists. The separate Config
metadata transfer chain binds the direction-selected wire metadata to task
authority and session.

The Volume content manifest row is a direct digest of only the canonical ordered
entries and is therefore byte-identical across capture and restore. The Volume
transfer seed/member rows form a separate chain that binds transport authority,
direction, role, source-evidence presence, batching, and replay; Start and End
carry the content digest while Batch, Resume, Ack/Credit, and End carry transfer
chain state. Neither digest may be substituted for the other.
`metadata_snapshot_sha256`, `metadata_transcript_sha256`, value/manifest/
construction/finalization/service chains, `materialization_sha256`,
`expected_labels_sha256`, `verification_sha256`, `observation_sha256`,
object `metadata_sha256`, and Config `ack_sha256` MUST use the corresponding
row above. The Config transfer transcript in section 7 remains a separate exact
archive-byte transcript and is not an authority alternative.

Every Backup capture, restore, and prune Task has one persisted absolute deadline
exactly 21,600 seconds after Task creation. Reconnect, adoption, process restart,
and generic retry do not extend it. Every step deadline is persisted, delivered,
digest-bound, and no later than the Task deadline. Every prune step is capped at
exactly 1,800 seconds.

For an API accepting integral seconds, remaining timeout is
`floor((deadline_unix_nano-now_unix_nano)/1_000_000_000)`, clamped at zero. Zero
means do not start. A monotonic timer correlated to the absolute deadline still
terminates work at that deadline, so rounding can shorten but never lengthen it.
`TaskAssignment.forward_deadline` is the exact persisted absolute Task
deadline. It is
never recomputed from the remaining whole-second value, extended on assignment
or reconnect, or treated as a fresh relative deadline. The superseded field
name `timeout_seconds` and its tag 5 are both reserved.

```proto
message BackupCheckpointRequest {
  reserved "sequence", "kind", "control_payload_sha256";
  string task_id = 1;
  string step_id = 2;
  bytes execution_id = 3;              // exactly 32 bytes
  uint64 checkpoint_sequence = 4;
  CheckpointFence preceding_checkpoint = 5;
  bytes authority_digest = 6;
  string assignment_id = 7;            // exact canonical asgn_ id
  oneof payload {
    BackupArtifactPrepared artifact_prepared = 20;
    BackupUploadVerified upload_verified = 21;
    BackupSourceCleanupCompleted source_cleanup_completed = 22;
    BackupPostgresContainerObserved postgres_container_observed = 23;
    BackupPostgresDumpStart postgres_dump_start = 24;
    BackupPostgresRestoreApplyStartCheckpoint postgres_restore_apply_start = 25;
    BackupRestoreArtifactValidated restore_artifact_validated = 26;
    BackupConfigCheckpoint config = 27;
    BackupVolumeCheckpoint volume = 28;
    BackupPruneObjectDeleted prune_object_deleted = 29;
    BackupPostgresRestoreVerified postgres_restore_verified = 30;
    BackupPostgresServiceProgress postgres_service_progress = 31;
    BackupUploadCompleted upload_completed = 32;
  }
}
message BackupCheckpointAck {
  string task_id = 1;
  string step_id = 2;
  bytes execution_id = 3;              // exactly 32 bytes
  uint64 checkpoint_sequence = 4;
  CheckpointFence committed = 5;
  string assignment_id = 6;            // exact canonical asgn_ id
}
```

This `BackupCheckpointRequest` intentionally clean-replaces the checked-in
incomplete schema-1 meanings of tags 2..6: old `assignment_id`, `step_id`,
uint32 `sequence`, `kind`, and `control_payload_sha256` are not decoded. The
new meanings above are `step_id`, `execution_id`, uint64
`checkpoint_sequence`, `preceding_checkpoint`, and `authority_digest`, while
`assignment_id` moves to tag 7. The superseded field names `sequence`, `kind`,
and `control_payload_sha256` are reserved by name in the replacement protobuf.
There is no compatibility decoder.

For every request, `authority_digest` is the one shared Task-authority digest;
`step_id` and `execution_id` MUST select exactly one ordered StepAuthority and
its matching StepResume. There is no per-step authority digest alternative.
One Controller transaction compares authority, locks,
dependency holds, current resume, predecessor fence, and operation cursor. It
atomically performs the authoritative transition, creates the immutable
completed-dedupe key, advances resume, and performs required publication. The
ack revision is the completed-dedupe key's exact `ModRevision`, not another
key's or a guessed revision. Only after commit does the Controller construct the
fence from authority digest and that `ModRevision`.

There is one explicit exception: Config primary roll-forward and
materialization cleanup are bounded multi-transaction publications. The initial
checkpoint transaction compares the same authority/fence and atomically creates
a pending claim with fixed membership, hashes, holds, preallocated IDs, cursor,
and prior resume; it does not create completed dedupe or acknowledge completion.
Each bounded transaction compares and advances only that claim. The final
transaction performs the last publication/cleanup, creates completed dedupe and
ack fence, advances resume, and clears the claim atomically. Exact replay while
pending resumes the claim; it never invokes the generic completed path.

An exact replay with the same sequence, authority, and payload returns the stored
result/fence without transition. Lost-ack dedupe is checked before stale-fence
rejection. Changed duplicate, sequence reuse, gap, missing predecessor, or fence
mismatch rejects before work. A pending claim is not completed dedupe.
Config publication, materialization, point publication, prune, terminalization,
and restore adoption use the same revision-and-digest fence rule.

Completed checkpoint-dedupe rows have a bounded lifecycle. Every terminalized
assigned Agent-executor Task has one Ack-independent generic receipt at
`/v1/runtime/task-terminal-receipts/{task_id}`. It cleanly replaces every
Backup-only receipt/key/read. Native Controller Tasks and pending abort without
a terminal assignment have no receipt.

The terminal transaction always reserves one of the exact 96 operations for
that receipt, leaving at most 95 comparisons plus mutations for the terminal
Task and domain effects. On the Agent-Ack path, `AcknowledgeAgentTask`
atomically publishes the canonical terminal Task and exact receipt at one
ModRevision, then separately CAS-ensures `TaskTerminalDeliveryRecord` state
`pending` with the exact TaskAck digest. On Controller timeout or abort of an
assigned Task, the Controller atomically publishes the same canonical terminal
Task and exact receipt at one ModRevision, then separately CAS-ensures delivery
state `awaiting_ack` with the Ack digest absent. An exact late TaskAck changes
only delivery state `awaiting_ack -> pending` and adds its digest; it never
rewrites the terminal Task or receipt. A changed or outcome-inconsistent late
TaskAck returns a typed protocol/state conflict, leaves `awaiting_ack`, the
terminal Task, and the receipt byte-identical, never enters the pending receipt
handshake, and cannot block or reset the authoritative receipt-only retirement
timer or transition.

Active delivery state is exactly `awaiting_ack | pending | applied`, carries
stable Controller receipt evidence and historical assignment
`AgentGeneration`, has no process generation, and is bounded <=32 rows. Clean
is a separate immutable
`/v1/runtime/task-terminal-delivery-clean/{task_id}` archive. `awaiting_ack`
uses only the TaskAbort/retirement path and never enters the receipt handshake;
only `pending` and `applied` participate in ReceiptAck/Applied exchanges.
Mandatory fixed-revision reconciliation repairs receipt-without-row before
session/Ready, pruning, or final Agent removal. An unknown terminal-transaction
outcome is accepted only by one fixed-revision read that finds the canonical
Task and receipt at the same ModRevision and validates both exact digests.
Anything missing, torn, mismatched, or reconstructed is corruption. Assignment
evidence remains until delivery ensure; receipt removal without active or clean
state is forbidden. Awaiting/pending/applied block final Agent removal; clean
archive does not.

One `Client.Run` owns the WorkerPool, registry, terminal store, Retired markers,
retirement-pending markers, and recovery/staging state across reconnects. Stable
local records never contain process generation. Receipt/Applied messages are
reconstructed with the current process generation; exact stable duplicates
attach and never rerun, while conflict closes the stream.

For each awaiting row Controller sends/replays only `TaskAbort` with exact task,
assignment, assignment generation, plan and terminal disposition. Agent
serializes access to the running registry, stops and joins the exact worker, and
prevents any worker from mutating the inventory. If TaskAck exists or wins the
completion race, Agent sends or replays it. Only Controller's exact acceptance
enters the pending receipt handshake. A typed conflict leaves the Ack journal
byte-identical and waits for a replayed TaskAbort carrying the matching rejected
Ack digest; under unchanged receipt, assignment, plan, terminal, and durable-
revision authority, Agent then atomically enters conflict-bound
`retirement_pending`. With no TaskAck, Agent durably temp-write/fsync/rename/
parent-fsync records the ordinary bounded `retirement_pending` marker containing
only the stable TaskAbort tuple and authorized recovery membership. Terminal,
staging, inventory, recovery-plan, other assignment recovery state, an
unclassified Ack journal, or an accepted/pending-handshake Ack journal blocks
inventory completion and `Retired`; one byte-identical local Ack whose canonical
digest is bound by the matching conflict-bearing `retirement_pending` marker
under identical receipt, assignment, plan, terminal, and durable-revision
authority is inert conflict evidence, does not enter the receipt handshake, and
may be retained through `Retired` until `RetiredAck` cleanup. At most 32 process-independent retirement-pending markers exist,
and any one blocks true Ready.

Agent first completes all pending/applied receipt handshakes and exact terminal
cleanup. It then scans a quiescent staging root and sends Inventory. Controller's
unique RecoveryPlan remains kind- and state-specific. Only a reversibly
disposable stage for which schema 1 already grants exact discard authority may
receive `DiscardRecovered`. Irreversible, possibly applied, protected restore,
publication, or helper state receives its exact `ResumePrepared`, adoption, or
other recovery disposition, or remains `recovery_required` with no destructive
Plan. Terminal, timeout, and abort never manufacture discard authority; silence
is never discard. Agent journals/applies only the authorized Plan, fsyncs,
durably sends stable recovery Ack until AckReceipt, and proves exact
stage/recovery-journal absence. `retirement_pending` and false Ready persist
until protected recovery reaches its exact safe terminal or adopted state and
stage absence is durably proven. Only then may it atomically transform the
matching retirement-pending marker into a durable Retired marker and send
`TaskTerminalAssignmentRetired`.

Retired and RetiredAck contain only current wire `process_generation` plus stable
task id, assignment id/generation, plan hash and terminal disposition. Local
Retired marker contains only that stable tuple and never process generation.
For the no-Ack Retired transition, Controller never resolves or validates a
TaskAck. It fixed-revision-validates the canonical terminal Task and immutable
receipt at the same ModRevision plus the exact Task/Agent/assignment identities,
durable Agent generation, assignment generation, plan hash, terminal result,
terminal Task digest, and receipt digest from the awaiting row and receipt. On
exact Retired it also validates absent clean archive, compares active
ModRevision, compares clean Version=0, deletes active and puts immutable clean:
four Groundplane operations. Replay validates clean and resends RetiredAck with
the current process generation. Agent validates and fsync-removes marker. A
late TaskAck after Retired proof returns the same typed protocol/state conflict;
it cannot change or invalidate the receipt-only retirement result.

`terminal_delivery_clean=true` means terminal, Retired and retirement-pending
stores are empty. Only after recovery AckReceipt and all Retired handshakes does
Agent send true Ready with capacity 0. Controller fixed-revision-lists <=32 active
rows across historical `AgentGeneration`. Awaiting, pending, malformed or
conflicting rejects. Applied rows move active->clean after strict fixed-revision
value validation. Each uses active-ModRevision compare, clean-Version=0 compare,
delete and put: four operations. Max32 requires two transactions: max24/96 then
max8/32. Controller then proves active prefix empty. Only an otherwise eligible
later/current Ready admits positive capacity, Config or dispatch. False Ready
requires zero capacity and updates liveness only.

Startup validates/replays <=32 terminal/Applied/Retired/retirement-pending entries
in Task-ID order. Corrupt/unknown/33rd/oversize fails closed. No reset, TTL, age,
reconnect, inferred absence, listing or automatic recovery evicts. Clean archives
are not enumerated by Ready and do not block final Agent removal. Retention
fixed-revision-validates clean+receipt and compares only their ModRevisions before
two deletes: four operations/pair, max24/96. Awaiting/pending/applied retain
receipt indefinitely.

Checkpoint cleanup begins after terminal commit, deletes <=47 rows plus cursor,
and retains membership. No second receipt/replay authority. Retired messages are
exactly 130 bytes inner: process generation 18, task and assignment ids 33 each,
assignment generation 10, plan hash 34 and terminal 2. Agent oneof tag14 adds a
one-byte key and two-byte length, exactly 133 outer. Controller tag16 adds a
two-byte key and two-byte length, exactly 134 outer. Both are below 512.

### 4. Sealed Backup authorities and evidence

The Controller seals each authority from one consistent read. Every project,
Environment, Service, Volume, Config Entry, secret, fact, grant, Connector,
Compose input, retention policy, dependency hold, and object identity carries
its exact Controller `ModRevision` and content digest. The Agent never resolves
a mutable name during sealed work.

```proto
enum BackupResourceKind {
  BACKUP_RESOURCE_KIND_UNSPECIFIED = 0;
  BACKUP_RESOURCE_KIND_POSTGRES = 1;
  BACKUP_RESOURCE_KIND_CONFIG = 2;
  BACKUP_RESOURCE_KIND_VOLUME = 3;
}
message BackupResourceIdentity {
  BackupResourceKind kind = 1;
  string resource_id = 2;              // exact kind-specific stable id
  RevisionDigest resource = 3;
}
enum BackupEncryptionKind {
  BACKUP_ENCRYPTION_KIND_UNSPECIFIED = 0;
  BACKUP_ENCRYPTION_KIND_NONE = 1;
  BACKUP_ENCRYPTION_KIND_AGE_X25519 = 2;
}
message BackupEncryptionAuthority {
  BackupEncryptionKind kind = 1;
  string secret_slot_id = 2;           // absent for none; exact spt_ id for age
  bytes recipient_sha256 = 3;          // absent for none; exactly 32 for age
  RevisionDigest secret_slot = 4;      // absent for none
}
message RevisionDigest {
  uint64 mod_revision = 1;              // 1..MaxInt64
  bytes sha256 = 2;
}
message BackupServiceFact {
  string service_id = 1;
  string current_name = 2;
  RevisionDigest service = 3;
  RevisionDigest compose = 4;
  BackupPriorRuntimeIntent prior_runtime_intent = 5;
  uint32 required_label_count = 6;      // 0..64
  bytes required_labels_sha256 = 7;     // exactly 32 bytes
  bytes repository_digest = 8;          // exactly 32 bytes
}
enum BackupRuntimeIntentKind {
  BACKUP_RUNTIME_INTENT_KIND_UNSPECIFIED = 0;
  BACKUP_RUNTIME_INTENT_KIND_STOPPED = 1;
  BACKUP_RUNTIME_INTENT_KIND_RUNNING = 2;
}
message BackupPriorRuntimeIntent {
  BackupRuntimeIntentKind kind = 1;
  RevisionDigest intent = 2;
}
message BackupArtifactEvidence {
  uint64 source_size_bytes = 1;
  bytes source_sha256 = 2;
  uint64 stored_size_bytes = 3;
  bytes stored_sha256 = 4;
}
message BackupObjectIdentity {
  BackupConnectorAuthority connector = 1;
  string bucket = 2;
  bytes object_key = 3;
  oneof immutable_discriminator {
    BackupS3VersionId version_id = 4;
    BackupS3ETag etag = 5;
  }
}
message BackupS3VersionId { bytes value = 1; } // present value 0..1024 bytes
message BackupS3ETag { bytes value = 1; }      // present value 1..1024 bytes
message BackupObjectTarget {
  BackupConnectorAuthority connector = 1;
  string bucket = 2;
  bytes object_key = 3;
}
message BackupConnectorAuthority {
  string connector_id = 1;
  RevisionDigest connector = 2;
  bytes canonical_endpoint_url = 3;    // 1..2048 bytes
  bytes region = 4;                    // 1..64 bytes, each 0x21..0x7e
  bool path_style = 5;
  bytes prefix = 6;                    // 0..1024 bytes
  string access_key_slot_id = 7;       // exact canonical spt_ id
  string secret_key_slot_id = 8;       // exact canonical spt_ id
  RevisionDigest access_key_slot = 9;
  RevisionDigest secret_key_slot = 10;
}
message BackupCaptureAuthority {
  string point_id = 1;
  BackupResourceIdentity resource = 2;
  BackupObjectTarget target = 3;
  BackupEncryptionAuthority encryption = 4;
  reserved 5;
  reserved "services";
  oneof format {
    BackupPostgresCaptureAuthority postgres = 10;
    BackupConfigCaptureAuthority config = 11;
    BackupVolumeCaptureAuthority volume = 12;
  }
}
message BackupPruneAuthority {
  RevisionDigest retention_policy = 1;
  repeated BackupPruneObject objects = 2; // 1..11
}
message BackupRestoreAuthority {
  string point_id = 1;
  BackupResourceIdentity destination = 2;
  BackupObjectIdentity source_object = 3;
  BackupArtifactEvidence expected_evidence = 4;
  BackupEncryptionAuthority encryption = 5;
  reserved 6;
  reserved "services";
  oneof format {
    BackupPostgresRestoreAuthority postgres = 10;
    BackupConfigRestoreAuthority config = 11;
    BackupVolumeRestoreAuthority volume = 12;
  }
  oneof identity_source {
    BackupOriginalIdentity original = 20;
    BackupAdoptedIdentity adopted = 21;
  }
}
```

Metadata sorts by unsigned key bytes with unique keys and is authority-bound.
Object keys are opaque bytes. A successful S3 result uses the `version_id`
wrapper whenever the VersionId member/header is present; an empty-present value
is valid and differs from absence. Only when VersionId is absent does it use the
non-empty exact returned ETag wrapper. Each present value is at most 1,024
bytes. Discriminator kind/value cannot change.

The object metadata set has exactly seven lowercase ASCII keys:
`groundplane-encryption`, `groundplane-format`, `groundplane-point-id`,
`groundplane-source-sha256`, `groundplane-source-size`,
`groundplane-stored-sha256`, and `groundplane-stored-size`. Values are,
respectively, the closed enum token, closed format token, canonical raw point ID,
64 lowercase digest hex, canonical unsigned decimal length, 64 lowercase digest
hex, and canonical unsigned decimal length. Agent derives these pairs from the
sealed authority. Every `metadata_count` is exactly 7 and every metadata digest
uses section 3.1; raw metadata is never embedded in an assignment.

The four-part evidence is plaintext source length and SHA-256 plus stored-object
length and SHA-256. `BackupTaskAuthority.services` is the sole stable sorted
Service-ID fact map. It carries current name, Service/Compose revisions, prior
runtime intent, exact label count/digest, and repository digest once per Service.
Every step's sorted unique `consumer_service_ids`, PostgreSQL
`database_service_id`, and Config exposure Service id MUST resolve to exactly one
row in that map. Capture, restore, PostgreSQL, and Volume format authorities
contain no duplicate consumer map, labels, repository digest, or conflicting
Service revision. The common facts govern stop/start, health, Config exposure,
PostgreSQL consumers, and adoption.

The stored-object ceiling is exactly
`L=5*2^40=5,497,558,138,880` bytes. With encryption none, producer and staging
reject before source length exceeds `L`. With age X25519 and
`T(S)=S+184+16*max(1,ceil(S/65536))`, the exact largest accepted plaintext is
`S=5,496,216,289,016`: `ceil(S/65536)=83,865,605` and `T(S)=L`; `S+1` produces
`L+1` and rejects. Config remains bounded by its smaller exact source/stored
limits. Volume preflight uses the applicable plaintext cap. PostgreSQL
`ExclusiveUnknown` checks before every exact-range allocation/write and stops
without `artifact_prepared` before either source or growing age output could
exceed its cap. All accepted four-part evidence therefore has stored size at
most 5 TiB.

### 5. Staging allocation and startup recovery

The Agent stages under a bootstrap-owned directory unavailable to project
containers. One directory belongs to one recovery key. Closed final roles are
`SOURCE_PLAINTEXT` at `source.final` and `STORED_OBJECT` at `stored.final`. With
encryption `none`, only `source.final` exists and its evidence is both source and
stored evidence. With `age`, source alone or both finals may exist as allowed by
the durable checkpoint. Restore likewise permits only its checkpoint-allowed
subset. The fixed basenames are 12 bytes.

Managed partials `.source.final.partial` and `.stored.final.partial` are 21 bytes
and are removed locally before inventory. A namespace with no final is removed.
Unknown names, unsafe metadata, duplicate roles, more than two finals, or a 33rd
recovered point fail startup closed and are never omitted or truncated.

Config and Volume reserve their bounds before source/stored bytes. PostgreSQL
uses `ExclusiveUnknown`: no other staging writer is admitted, every exact append
range is physically allocated before write, and exclusivity lasts until final or
failure.

Agent outer oneof field 13 is `backup_staging_inventory`, field 14 is
`backup_staging_recovery_ack`; Controller outer oneof field 14 is
`backup_staging_recovery_plan`.

```proto
message BackupStagingInventory {
  repeated BackupRecoveredStage entries = 1; // 0..32, sorted recovery_key_sha256
}
message BackupRecoveredStage {
  bytes recovery_key_sha256 = 1; // exactly 32 bytes
  repeated BackupRecoveredFile files = 2; // exactly 1..2, role order, unique
}
enum BackupRecoveredFileRole {
  BACKUP_RECOVERED_FILE_ROLE_UNSPECIFIED = 0;
  BACKUP_RECOVERED_FILE_ROLE_SOURCE_PLAINTEXT = 1;
  BACKUP_RECOVERED_FILE_ROLE_STORED_OBJECT = 2;
}
message BackupRecoveredFile {
  BackupRecoveredFileRole role = 1;
  uint64 size_bytes = 2; // 0..MaxInt64
  bytes sha256 = 3;      // exactly 32 bytes
}
message BackupStagingRecoveryPlan {
  bytes inventory_sha256 = 1;
  repeated BackupStagingDisposition dispositions = 2;
}
message BackupStagingDisposition {
  bytes recovery_key_sha256 = 1;
  oneof decision {
    BackupResumePrepared resume_prepared = 2;
    BackupDiscardRecovered discard_recovered = 3;
  }
}
message BackupResumePrepared {
  bytes assignment_resume_sha256 = 1;
  BackupRemainingGrowth remaining_growth = 2;
  uint64 required_growth_bytes = 3;
  repeated BackupRecoveredFile expected_files = 4;
}
enum BackupRemainingGrowth {
  BACKUP_REMAINING_GROWTH_UNSPECIFIED = 0;
  BACKUP_REMAINING_GROWTH_NO_GROWTH = 1;
  BACKUP_REMAINING_GROWTH_BOUNDED = 2;
  BACKUP_REMAINING_GROWTH_EXCLUSIVE_UNKNOWN = 3;
}
message BackupDiscardRecovered {}
message BackupStagingRecoveryAck {
  bytes inventory_sha256 = 1;
  bytes applied_plan_sha256 = 2;
  uint32 applied_disposition_count = 3;
}
```

`BackupRecoveredFile` is at most 46 bytes, a recovered row 130,
`BackupStagingInventory` inner 4,256 and outer 4,259,
`BackupResumePrepared` 142, a disposition 179,
`BackupStagingRecoveryPlan` inner 5,858 and outer 5,861, and
`BackupStagingRecoveryAck` inner 70 and outer 72 bytes.

`required_growth_bytes` is 1..MaxInt64 only for `BOUNDED`; it is absent/zero for
other modes. Expected files number 1..2 and are byte-equal to inventory.
Every inventory key occurs exactly once in the sorted plan; extras, duplicates,
or omissions fail closed.

The recovery key is:

```text
SHA-256("groundplane.agent.v1.backup-staging-recovery-key\x00" ||
       u32be(len(task_id)) || task_id ||
       u32be(len(step_id)) || step_id ||
       u32be(len(point_id)) || point_id)
```

No raw IDs or filenames cross recovery messages. These digests use section
3.1's sole framing: `D(domain,P(message))`, with
`D(domain,body)=SHA-256(ASCII(domain)||0x00||body)` and
`P(message)=u32be(len(E(message)))||E(message)`. E is raw deterministic protobuf
after recursive unknown-field rejection, with only a message's own self-digest
cleared/absent. These messages have no self-digest, so E includes every field.
Complete `BackupStagingInventory` uses domain
`groundplane.agent.v1.backup-staging-inventory`. Complete
`BackupStagingRecoveryPlan`, including inventory digest/dispositions, uses
`groundplane.agent.v1.backup-staging-recovery-plan`. Complete
`BackupStagingRecoveryAck`, including inventory digest, applied-plan digest and
count, uses `groundplane.agent.v1.backup-staging-recovery-ack`. No u64 framing
or second P exists. Assignment resume uses
`D("groundplane.agent.v1.backup-staging-assignment",
P(the exact future TaskAssignment))`. Hashes are 32 bytes and one inventory
digest receives one persisted plan digest.

Startup order is exact:

1. Agent authenticates and sends presence-checked false Ready with capacity 0.
2. Controller reconciles missing delivery rows, then sends exact terminal
   `TaskAbort` for every awaiting row.
3. Agent serializes registry/terminal/recovery state, stops and joins each
   matching worker, and prevents further inventory mutation. TaskAck winners
   enter pending; all others durably enter retirement-pending.
4. Agent completes every pending/applied Receipt/Applied handshake and terminal
   cleanup. Retirement-pending remains durable and blocks true Ready.
5. Agent scans the now-quiescent staging root and sends Inventory. Controller
   persists the one kind/state-specific Plan: reversible rows may receive only
   already-authorized DiscardRecovered; protected or possibly-applied rows
   receive exact ResumePrepared/adoption/recovery or remain recovery_required.
6. Agent journals/applies an authorized Plan, durably replays stable Ack until
   AckReceipt, and proves exact staging/recovery-journal absence. Protected
   recovery keeps retirement-pending and false Ready until exact safe terminal
   or adopted state.
7. Only then Agent transforms eligible retirement-pending markers to Retired and completes
   Retired/RetiredAck, and fsync-removes their markers.
8. Agent sends true Ready with capacity 0. Controller archives applied rows in
   max24/96 then max8/32 operations and proves active prefix empty.
9. Only then Agent sends ordinary true Ready with real capacity; Config and
   dispatch remain gated until this later readiness.

Crash, lost Ack/receipt, process restart, TaskAck-versus-retirement race, worker
join, quiescent Inventory, recovery Discard, pending-to-Retired transform,
Retired replay, true-zero archive batches, and every boundary above require
actual-session race tests. No running worker may mutate Inventory or staging
during steps 5-7, and no external effect replays after a durable applied journal.

A decision timeout never implies discard. Controller may transactionally persist
an explicit all-Discard plan only when every inventory row independently meets
the reversible Discard conditions below. If any irreversible or uncertain row
cannot receive exact ResumePrepared, Controller persists recovery-required
state, sends no destructive plan, and blocks Ready for operator resolution.
Silence is never discard. Post-send timeout replays the same plan. Reconnect in
one boot resends immutable inventory and cached Ack; process restart rescans.
Different plans for one digest are fatal. Resume finals remain protected;
Discard unlinks and fsyncs parents. Malformed/unmatched data remains for operator
inspection unless covered by explicit Discard.

Before any recovery-plan effect Agent fsyncs a bootstrap-owned journal with
exact inventory/plan digests, deterministic Ack bytes/digest, dispositions, and
`applying`. At most 32 records exist. Effects run canonically and are
crash-idempotent only while applying. After all effects/parent fsyncs, Agent
atomically writes `applied` and fsyncs before sending stable
`BackupStagingRecoveryAck`. Reconnect/restart replays the byte-identical Ack;
applied never reapplies effects. Controller durably accepts or exact-deduplicates
Ack before sending `BackupStagingRecoveryAckReceipt` with current authenticated
`process_generation` and stable inventory/plan/Ack digests. Agent validates and
fsync-removes journal. Mismatch closes without removal. Receipt is 120 bytes
inner and tag 19 makes 123 outer. Journal shares the 32-entry outstanding ceiling,
adds at most 32 6,400-byte records (204,800 disk bytes), and one 6,400-byte
replay buffer.

Controller may choose Discard only for terminal, cancelled, superseded,
unknown, or explicitly abandoned authority. A stage belonging to active
irreversible mutation must receive exact ResumePrepared or remain
`recovery_required`; timeout or local inference never converts it to Discard.

The 32-row proof is an implementation invariant: a clean root has zero; the
Agent admits at most one unresolved stage per active Backup assignment and at
most 32 active assignments; cleanup removes it and crash preserves a subset;
bootstrap creates none and resolves all before Prepare reopens. Live admission
and startup scan both reject duplicate assignment staging or entry 33.

### 6. Artifact preparation, encryption, upload, and point commit

```proto
message BackupArtifactPrepared {
  string point_id = 1;
  BackupArtifactEvidence evidence = 2;
  BackupStagingFinals finals = 3;
  oneof format_evidence {
    BackupPostgresArchiveEvidence postgres = 10;
    BackupConfigArchiveEvidence config = 11;
    BackupVolumeArchiveEvidence volume = 12;
  }
}
message BackupStagingFinals {
  bytes source_relative_name = 1; // "source.final"
  bytes stored_relative_name = 2; // "source.final" or "stored.final"
  bool same_inode = 3;
}
message BackupUploadVerified {
  string point_id = 1;
  BackupArtifactEvidence evidence = 2;
  BackupObjectIdentity object = 3;
  uint32 metadata_count = 4;           // exactly 7
  bytes metadata_sha256 = 5;
}
message BackupUploadCompleted {
  string point_id = 1;
  BackupArtifactEvidence evidence = 2;
  BackupObjectTarget target = 3;
  oneof put_outcome {
    BackupObjectIdentity returned_object = 4;
    BackupPutOutcomeUnknown unknown = 5;
  }
  uint32 metadata_count = 6;           // exactly 7
  bytes metadata_sha256 = 7;
}
message BackupPutOutcomeUnknown {}
message BackupSourceCleanupCompleted {
  string point_id = 1;
  BackupArtifactEvidence evidence = 2;
}
```

`artifact_prepared` occurs only after EOF, producer success, exact bounded
length, each final `fsync`, open-descriptor `fstat`, atomic final installation,
and parent `fsync`. No active-generation checkpoint exists. Pre-checkpoint
failure removes partials and creates no evidence checkpoint.
Its acknowledgement durably advances capture from `CAPTURING` to `UPLOADING`;
no Put is allowed before that acknowledgement.

Encryption `none` reuses source. `age` writes stored once from retained source.
For exact source length `S`, the stored upper bound is exactly:

```text
T(S) = S + 184 + 16 * max(1, ceil(S / 65536))
```

Arithmetic is checked. Bounded capture reserves source plus `T(S)` before
credit/write. After `artifact_prepared`, reconnect reuses verified finals and
never regenerates or re-encrypts.

Upload uses sealed Connector revision, endpoint, region, bucket, key, fixed
metadata digest, evidence, and encryption. The Agent derives the exact fixed
metadata pairs from sealed evidence and verifies their count/digest. After Put,
including an uncertain response, it sends `upload_completed` and MUST NOT Head
until that checkpoint is durably acknowledged. A normal response binds the
returned object discriminator; an uncertain response uses the typed empty
`unknown` outcome and binds no invented discriminator. The acknowledgement
advances the Controller phase from upload to head verification.

In head-verification phase the Agent performs Head and adopts the object only
when bucket/key, every exact metadata pair, and all four evidence values match.
For a normal Put, Head's VersionId-present-or-ETag discriminator must also match
the returned discriminator. For an uncertain Put, the exact Head discriminator
becomes the attempt's immutable identity only at `upload_verified`. Any mismatch retains local finals and object authority for
operator inspection and is never automatically overwritten or deleted. It then
sends `upload_verified`. Its acknowledgement advances only from head
verification to point commit.

Point commit is a separate Controller-owned atomic transaction, or the same
Controller reconciler performing that transaction later. It creates the
immutable Recovery Point and exact object/evidence authority before terminal
success. There is deliberately no `point_committed` Agent checkpoint. Crash or
uncertainty after `upload_verified` acknowledgement retains orphan
`phase=point_commit`, staging finals, and object authority until Controller
reconciliation commits the point. Thus Head evidence and point commit are never
conflated.

Generic source cleanup applies only to PostgreSQL and Config after the
Controller has durably committed the point. Volume completes after all stopped consumers restart and pass
health. Stored finals remain recoverable until terminal transaction/ack; a send
or stream failure never eagerly deletes them.

### 7. Config archive and bidirectional transfer

#### 7.1 Canonical Entry and archive

```proto
message BackupConfigEntry {
  uint32 ordinal = 1;
  string entry_id = 2;
  RevisionDigest entry = 3;
  optional bool secret = 4;            // presence required
  oneof placement {
    BackupConfigEnvironment environment = 10;
    BackupConfigFile file = 11;
  }
  oneof exposure {
    BackupConfigExposureAll all = 20;
    BackupConfigExposureServices services = 21;
  }
  oneof desired_source {
    BackupConfigLiteral literal = 30;
    BackupConfigSecretRef secret_ref = 31;
    BackupConfigFactRef fact = 32;
  }
  uint64 selected_value_size_bytes = 40;
  bytes selected_value_sha256 = 41;
}
message BackupConfigEnvironment { bytes name = 1; }
message BackupConfigFile { bytes path = 1; uint32 mode = 2; uint32 uid = 3; uint32 gid = 4; }
message BackupConfigExposureAll {}
message BackupConfigExposureServices { repeated string service_ids = 1; } // 1..128
message BackupConfigLiteral {}
message BackupConfigSecretRef {
  string secret_id = 1;
  bytes authored_key = 2;
  RevisionDigest secret = 3;
}
message BackupConfigFactRef {
  string attach_id = 1;
  bytes fact = 2;
  optional string grant_attach_id = 3;
  RevisionDigest attach = 4;
  optional RevisionDigest grant_attach = 5;
}
message BackupConfigRestoreEntry {
  string entry_id = 1;                         // exact authenticated 29-byte id
  BackupConfigEntryMetadata metadata = 2;
  BackupConfigEntryExposure exposure = 3;
  BackupConfigArtifactDesiredSource source = 4;
  optional bool secret = 5;                    // presence required
  BackupConfigSelectedValue selected_value = 6;
}
message BackupConfigEntryMetadata {
  oneof placement {
    BackupConfigEnvironment environment = 1;
    BackupConfigFile file = 2;
  }
}
message BackupConfigEntryExposure {
  oneof exposure {
    BackupConfigExposureAll all = 1;
    BackupConfigExposureServices services = 2;
  }
}
message BackupConfigArtifactDesiredSource {
  oneof source {
    BackupConfigLiteralSource literal = 1;
    BackupConfigArtifactSecretRefSource secret_ref = 2;
    BackupConfigArtifactFactSource fact = 3;
  }
}
message BackupConfigLiteralSource {}
message BackupConfigArtifactSecretRefSource { string authored_key = 1; }
message BackupConfigArtifactFactSource {
  string attach_id = 1;
  optional string grant_attach_id = 2;
  string fact = 3;
}
message BackupConfigSelectedValue {
  uint64 size_bytes = 1;
  bytes sha256 = 2;                    // exactly 32 bytes
}
message BackupConfigContentAuthority {
  bytes manifest_sha256 = 1;
  uint32 entry_count = 2;
  uint64 total_selected_value_bytes = 3;
  uint64 manifest_size_bytes = 4;
  uint64 source_size_bytes = 5;
  bytes metadata_snapshot_sha256 = 6;  // exactly 32; transfer-independent
}
message BackupConfigArchiveEvidence {
  BackupConfigContentAuthority content = 1;
  bytes capture_transcript_sha256 = 2;
}
message BackupConfigCaptureAuthority {
  string environment_id = 1;           // exact ref to common Environment fact
  reserved 2;
  reserved "environment";
  BackupConfigContentAuthority content = 3;
  uint64 metadata_snapshot_revision = 4; // 1..MaxInt64
  reserved 5;
  reserved "metadata_snapshot_sha256";
  uint32 metadata_entry_count = 6;
  uint64 metadata_proto_bytes = 7;
}
message BackupConfigRestoreAuthority {
  string destination_environment_id = 1; // exact ref to common Environment fact
  reserved 2;
  reserved "destination";
  BackupConfigContentAuthority expected_content = 3;
}
message BackupConfigMetadataRow {
  bytes authority_digest = 1;          // exactly 32 bytes
  bytes metadata_snapshot_sha256 = 2; // exactly 32 bytes
  uint32 ordinal = 3;                 // 1..4096
  oneof metadata {
    BackupConfigEntry capture_entry = 4;
    BackupConfigRestoreEntry restore_entry = 5;
  }
  bytes preceding_transfer_chain_sha256 = 6; // exactly 32 bytes
  bytes resulting_transfer_chain_sha256 = 7; // exactly 32 bytes
}
```

Entries sort by stable Entry ID and have contiguous ordinals from one.
Environment and file placement are complete. Exposure is either all Services or
sorted unique stable Service IDs. Desired source preserves literal, secret ref
with authored key, or exact fact name under stable Attach plus optional grant
Attach and their exact revisions. Selected value bytes are separate from
metadata. `secret` is an explicit required boolean and is never inferred from
mode or desired-source kind.

Capture authority contains no Entry and its Environment id is only an exact
reference to the shared Environment fact. It seals content/count/size/digest
facts and the revision, count, and aggregate bytes of
a Controller-owned immutable subordinate metadata snapshot. The content
authority carries the transfer-independent `metadata_snapshot_sha256`. The
Controller streams all `N` Headers from that snapshot during the metadata phase.
This keeps every valid assignment below 5 MiB; the Agent accepts no Header that
does not reproduce the sealed snapshot chain.

`metadata_snapshot_sha256` is computed before and independently of transfer.
Its exact preimage is the section 3.1 Config metadata-content domain and NUL,
then `u32be(N)` and, for each canonical ordinal, exactly one
`u32be(P) || deterministic BackupConfigRestoreEntry[P]`. Capture obtains that
message by the canonical artifact projection of its revision-bearing Entry;
restore obtains the byte-identical message from the authenticated manifest. There is
exactly one length prefix per metadata message. Authority digest, transfer id,
direction, record sequence, credit, ack, and transfer chain are excluded. Capture
seals it from the subordinate fixed-revision snapshot; restore pass one seals it
from the authenticated parsed artifact. Transfer Start repeats it through
`BackupConfigContentAuthority`, and the separate authority-bound metadata
transfer chain starts only afterward. This ordering makes a digest cycle
impossible.

Capture alone uses
`BackupConfigEntry`, including its live Entry and desired-source revisions.

Restore alone uses `BackupConfigRestoreEntry`. The Agent preserves the exact
authenticated archive Entry id and derives its explicit secret presence/value,
placement, exposure,
revision-free archived-source descriptor, and selected-value size/hash solely
from the already authenticated canonical Config artifact. The source descriptor
is exactly literal, secret-ref authored key, or fact Attach id/fact/optional
grant Attach id. It carries no Entry, Secret, Attach, grant, or Environment
revision, generation, resolved value, or lookup result. The Controller MUST NOT
translate it into `BackupConfigEntry` before hashing or replace selected bytes
from current state. It may resolve secret-ref only to validate authored-key
existence and kind, and fact references only to validate dependency, scope, and
classification; neither result may replace the authenticated selected bytes.
The published primary preserves the artifact descriptor and stable Entry id,
selects the restored immutable generation, and may participate in later ordinary
render resolution only after restore publication.

`entry_count` is `N` and equals the manifest Entry count. `N` is 0..4,096;
empty Config is valid. Each selected value `V_i` is 0..262,144 bytes and total
selected value bytes `T` is at most 1,073,741,824. Each deterministic direction-selected Entry metadata encoding is at most 7,936 bytes and its complete outer EntryHeader envelope is
at most 8,118 bytes. The sum of deterministic Entry encodings is at most
32,505,856 bytes and exact canonical manifest length `M` is at most 67,108,864
bytes. Environment
values from every desired source are valid UTF-8 without NUL. File values may
be arbitrary bytes, but an authored literal itself remains valid UTF-8.
Environment keys are 1..255 bytes and file destinations 1..240 bytes within
those marshal ceilings.
Each services exposure has at most 128 sorted unique stable Service ids and each
must reference the sole common task Service facts. The sender marshals the
complete direction-selected metadata and rejects unless actual deterministic
`proto.Size<=7936`; the list cap is not a substitute for that encoder check.

`BackupConfigMetadataRow` uses authority/content digests tags 1/2, uint32
ordinal tag 3, capture/restore metadata oneof tags 4/5, and
preceding/resulting transfer-chain digests tags 6/7. Its exact maximum is
`34+34+3+(1+2+7936)+34+34=8078`. The 87-byte metadata key produces an exact
8,170-byte PutRequest. `BackupConfigEntryHeader` uses capture tag 1 or restore
tag 2 and chunk-count tag 3; because exactly one metadata message is present,
the complete outer Header maximum remains 8,118 bytes.

ADR 0047's archive writer owns the exact canonical JCS manifest and USTAR header
grammar. The Agent accepts only plain canonical USTAR with no PAX, GNU, sparse,
extension, link, device, or duplicate member. This protocol binds the resulting
exact manifest bytes and archive length; it does not define a second manifest
encoding.

With manifest length `M`, count `N`, and value lengths `V_i`, checked arithmetic
computes:

```text
round512(x) = x + ((512 - (x % 512)) % 512)
S = 1024 + 512 * (N + 1) + round512(M) + sum(round512(V_i))
```

`S` is exact source tar length and is at most 1,142,949,376. Its exact maximum
is `1073741824 + 67108864 + 512*(4096+1) + 1024`. At that maximum
`ceil(S/65536)=17441`, so age stored maximum is 1,143,228,616 and simultaneous
source-plus-stored peak is 2,286,177,992. Controller seals
the complete content authority and rejects bounds, overflow, or PAX need.
Transfer Start repeats it before credit, sink, or write:

```proto
enum BackupConfigTransferDirection {
  BACKUP_CONFIG_TRANSFER_DIRECTION_UNSPECIFIED = 0;
  BACKUP_CONFIG_TRANSFER_DIRECTION_CAPTURE = 1;
  BACKUP_CONFIG_TRANSFER_DIRECTION_RESTORE = 2;
}
message BackupConfigTransferStart {
  BackupConfigTransferDirection direction = 1;
  BackupConfigContentAuthority content = 2;
}
```

Every field equals authority. Agent preflights `S` for none or `S+T(S)` for age
and creates one source tar sink; no hidden second plaintext spool. Finalization
requires exact `M`, digest, count, hashes, recomputed `S`, transcript, EOF blocks,
fsync and fstat.

The transfer transcript is SHA-256 over one exact byte string. Its prefix is the
ASCII domain `groundplane.backup.config-transfer.v1`, one NUL, `u32be(direction)`,
the 32-byte manifest digest, `u64be(M)`, `u64be(S)`, `u32be(N)`, and `u64be(T)`.
All metadata frames follow, each as
`u32be(ordinal) || u32be(len(E_i)) || E_i`. For capture,
`E_i=E(BackupConfigEntry_i)`; for restore,
`E_i=E(BackupConfigRestoreEntry_i)`.
The direction selects exactly one type and its distinct schema is part of the
transcript grammar.
All value frames then follow by ordinal as
`u32be(ordinal) || u32be(chunk_count) || u64be(V_i) || value_i`. The two uint64
lengths occur directly after manifest digest in manifest-size/source-size order.
No native integer, JSON number, textual decimal, or protobuf chunk boundary
enters the transcript. The empty transfer test vectors are
`c5595baebe3953162a534d981e7ed5f241d2a6358accbd7e3f9a1ad6449c1ee5`
for capture and
`a1652dfd0833e8ed6bd8d18657b4d014e1cac11edad358a2f58c320d2bdf33b0`
for restore.

#### 7.2 Capture and restore transfers

```proto
message BackupConfigTransfer {
  string task_id = 1;
  string step_id = 2;
  bytes execution_id = 3;              // exactly 32 bytes
  bytes transfer_id = 4;               // exactly 32 bytes, derived below
  uint64 record_sequence = 5;
  string assignment_id = 6;            // exact canonical asgn_ id
  oneof record {
    BackupConfigTransferStart start = 10;
    BackupConfigEntryHeader entry_header = 11;
    BackupConfigValueChunk value_chunk = 12;
    BackupConfigEntryEnd entry_end = 13;
    BackupConfigTransferEnd end = 14;
    BackupConfigTransferResume resume = 15;
  }
}
message BackupConfigCredit {
  string task_id = 1;
  string step_id = 2;
  bytes execution_id = 3;              // exactly 32 bytes
  bytes transfer_id = 4;               // exactly 32 bytes
  BackupConfigTransferDirection direction = 5;
  uint64 credit_sequence = 6;
  string assignment_id = 7;            // exact canonical asgn_ id
  oneof grant {
    BackupConfigMetadataAccepted metadata_accepted = 10;
    BackupConfigValueCredit value_credit = 11;
    BackupConfigMetadataCredit metadata_credit = 16;
  }
  uint64 committed_record_sequence = 12;
  uint32 next_ordinal = 13;
  bytes cumulative_chain_sha256 = 14;  // exactly 32 bytes
  bytes ack_sha256 = 15;               // exactly 32 bytes
}
message BackupConfigEntryHeader {
  oneof metadata {
    BackupConfigEntry capture_entry = 1;
    BackupConfigRestoreEntry restore_entry = 2;
  }
  uint32 chunk_count = 3;
}
message BackupConfigValueChunk { uint32 ordinal = 1; uint64 offset = 2; bytes content = 3; }
message BackupConfigEntryEnd {
  uint32 ordinal = 1; uint64 value_size_bytes = 2; bytes value_sha256 = 3;
}
message BackupConfigTransferEnd {
  BackupConfigTransferDirection direction = 1;
  BackupConfigContentAuthority content = 2;
  bytes transcript_sha256 = 3;
}
message BackupConfigTransferResume {
  reserved 1, 2, 4, 5, 7, 8, 9;
  reserved "direction", "next_record_sequence", "metadata_accepted",
           "metadata_chain_sha256", "prior_ack_sequence",
           "prior_committed_record_sequence", "prior_ack_sha256";
  uint32 next_ordinal = 3;
  bytes value_chain_sha256 = 6;        // exactly 32 bytes
}
message BackupConfigMetadataAccepted {
  bytes metadata_transcript_sha256 = 1;
  uint64 initial_value_credit_bytes = 2; // exactly 262144
  uint32 initial_value_record_credit = 3; // exactly 8
}
message BackupConfigValueCredit {
  uint64 value_credit_bytes = 1;         // 0..262144
  uint32 record_credit = 2;              // 0..8
}
message BackupConfigMetadataCredit {
  uint32 record_credit = 1;             // exactly 46 on every grant
  uint64 byte_credit = 2;               // exactly 393216 serialized bytes
}
```

Capture is Controller-to-Agent and restore is Agent-to-Controller; therefore
the receiver returns Credit on the opposite outer direction in each case.
Direction is fixed by authority, Start, Credit, and End and cannot change. Start
is record 1 and is the only fresh record exempt from credit. For `N>0`, after
accepting Start its receiver sends the initial `metadata_credit` with exactly
`record_credit=46` and `byte_credit=393216` complete-outer serialized bytes;
no Header may precede it. Headers are admitted in the same canonical batches as
the durability transaction: every non-final batch has exactly 46 Headers and
the final batch has 1..46. The receiver sends no Credit per Header. Only after
one whole batch commits may it send one cumulative replacement Credit: if more
Headers remain, that single replenishment is again exactly 46 records and
393,216 serialized bytes; after the final batch it sends `MetadataAccepted`
instead of another metadata grant. `46*8118=373428`, leaving 19,788 bytes in
each metadata serialized-byte window. For `N=0`, the receiver durably crosses
the metadata barrier and sends `MetadataAccepted` directly.

Exactly `N` EntryHeader records follow in ordinal order. Header ordinal is
derived as `record_sequence-1`; capture `BackupConfigEntry.ordinal` MUST equal
it, while restore metadata intentionally has no duplicate ordinal field. The
sole durable row stores that ordinal as uint32. The receiver validates all
metadata, dependencies, aggregate encodings, bounds, and metadata transcript
before durably persisting the acceptance barrier and sending phase-tagged
`MetadataAccepted` in the direction-appropriate Credit envelope. It carries the
metadata transcript SHA-256 and the initial value window of exactly eight
records and 262,144 complete-outer-serialized bytes. The durable barrier retires
all metadata credit; metadata and value credits are never fungible. `N=0` still
requires this barrier before End.

Config metadata grants are cumulative replacement windows, not additive
counters. A sender charges one record and the complete outer `proto.Size` before
the sole writer emits EntryHeader, ValueChunk, EntryEnd, or TransferEnd. A
non-final metadata batch exhausts its 46 record credits, and no Header from the
next batch is eligible before the post-commit replacement grant. Accepting that
grant restores the window to exactly 46 records and 393,216 serialized bytes;
it neither adds unused byte slack to the new window nor erases accounting for a
newer record. Queued but unemitted records consume queue capacity, not wire
credit, so the sender still produces through the eight-record bulk queue.

Value grants likewise replace the available bounded value window. At most one
Config value record per transfer may be emitted without its cumulative Credit
acknowledgement. A grant is applied at most once in its authenticated stream
session, so an exact duplicate cannot replenish either balance twice. This
makes every EntryEnd and TransferEnd credit-controlled and eligible without
inventing zero-content byte credit.

`credit_sequence` has two phase-scoped namespaces. A nonempty fresh metadata
phase is initialized exactly at 1. Each newly durable non-final 46-Header batch
advances it by exactly one and emits its one replenishment, while the final
batch transitions to `MetadataAccepted` without another metadata sequence.
Thus a nonempty metadata phase has at most
`ceil(N/46)<=ceil(4096/46)=90` metadata Credit sequences; an empty phase has
none. `MetadataAccepted` starts the value namespace at sequence 1. Within that
stream session, each newly admitted ValueChunk or EntryEnd advances it exactly
once and replenishes both value record and serialized-byte credit; durable
TransferEnd advances once more and returns terminal
`ValueCredit{value_credit_bytes=0,record_credit=0}`.

Within one authenticated stream session and phase, Credit is strictly
monotonic. The receiver retains the current and immediately preceding Credit
envelopes and `ack_sha256` values. The current sequence and immediately
preceding sequence are accepted only as byte-identical duplicates and never
reapply a grant; an exact duplicate transfer record may replay the corresponding
byte-identical current Credit. The only new sequence is current plus one and
must be the Credit produced by the required batch commit or value-record
admission. A changed duplicate, older sequence, gap, or regression terminates
the stream. Current/preceding replay is only a within-session lost-message rule;
it is never reconnect authority.

Disconnect invalidates every outstanding record/byte balance, every retained
current/preceding Credit, and the sender's applied-grant set for that stream.
All grants and acknowledgements from the old authenticated stream session are
invalid. Before `MetadataAccepted`, reconnect discards the partial metadata
transfer and an accepted fresh Start establishes a new metadata session at
credit sequence 1. After durable `MetadataAccepted`, Resume is the sole first
record exempt from credit. Only its exact durable Entry-boundary match
establishes the new value session, resets value `credit_sequence` to 1, and
causes a fresh bounded ValueCredit to be issued. A Credit not issued for the
currently accepted Start/Resume session fails closed.

The value phase is ordinal order. Each Entry has 0..8 canonical chunks: every
non-final chunk is exactly 32,768 bytes, the final non-empty chunk is 1..32,768,
and an empty value has zero chunks. Exactly one EntryEnd follows. Total chunks
are at most 32,768. Exactly one Entry value may be in flight, so the receiver
retains at most one incomplete value of at most 262,144 bytes plus O(1) hash and
cursor state. Offsets are contiguous without hole/overlap, EntryEnd repeats the
sealed length/hash, and the next Entry cannot begin before the receiver durably
commits the current EntryEnd.

If the stream disconnects during an Entry, the receiver zeroizes every retained
partial-value byte before releasing its buffer, discards or truncates any
uncommitted sink bytes, and rolls the ephemeral value offset, chunk hash,
transcript state, transfer record sequence, and record/byte-credit state back to
the last durable EntryEnd boundary. No chunk offset, chunk hash, partial byte
count, partial transcript state, or old-session Credit is durable or replay
authority. After Resume, the incomplete Entry is retransmitted from offset zero,
including all its chunks and EntryEnd; completed Entries are not retransmitted.

Pre-End record count is exactly `1+2N+C` and at most 40,961; TransferEnd sequence
is that count plus one and at most 40,962. Any derived-maximum excess, sequence
gap, wrap, changed duplicate, direction mismatch, or transcript mismatch fails
before publication.
The empty transfer is Start sequence 1 and End sequence 2 with pre-End count 1.

`transfer_id` is fixed across reconnect and equals
`SHA-256("groundplane.agent.v1.backup-config-transfer-id\x00" ||
authority_digest || u32be(direction))`. A normal Start is legal only for a fresh
transfer. Before durable MetadataAccepted, either direction discards partial
state and begins a fresh transfer at Start. After durable MetadataAccepted, the
first record on every reconnected stream is Resume. Resume carries only the
durable `next_ordinal` and completed-Entry `value_chain_sha256`; direction is
fixed by authority and the derived transfer ID. Its outer `record_sequence` is
the durable Entry-boundary sequence mechanically reconstructed from the sealed
chunk counts for ordinals below `next_ordinal`; it is an assertion, not separate
replay authority, and consumes no semantic record. The first following value
record is the reconstructed boundary plus one. Receiver compares assignment,
derived transfer ID, authority/direction, next ordinal, value chain, and the
reconstructed boundary before starting the new credit session. A mismatch fails
closed. Committed Headers and completed Entries are never replayed; the whole
incomplete Entry is replayed from offset zero.

Value credit is for the exact execution/transfer/direction. Its initial window
at value credit sequence 1 is exactly eight records and 262,144 serialized
bytes, both after MetadataAccepted on the original stream and after Resume on a
new stream. Every nonterminal replenishment is positive in both dimensions, at
most those values, and accounts all admitted records so outstanding credit
remains within both limits. Let `C<=32768` be the exact chunk count. The whole
value phase has exactly `C+N+1<=36,865` credit-controlled records including
TransferEnd. A single session can advance credit at most once for each of those
records; including its initial sequence 1, its maximum `credit_sequence` is
`1+(C+N+1)=C+N+2<=36,866`. Reconnect count never accumulates into that bound
because an accepted Resume creates a new session at sequence 1, and values above
the per-session derived bound reject. The bulk queue remains
bounded independently to eight records, 1,048,576 serialized bytes, and 262,144
content bytes; a transfer record whose complete envelope exceeds 262,144 rejects.
Adding the two-byte initial record-credit member while reducing maximum
`credit_sequence` from a five-byte to a three-byte varint leaves the recomputed
maximum complete `BackupConfigCredit` outer size at 293 bytes, below its
512-byte cap.

Disconnect before `MetadataAccepted` removes the partial sink, resets ephemeral
record/credit state to zero, and restarts record one with the same `M`/`S`.
After the barrier and before `artifact_prepared`, the receiver preserves only
completed-Entry sink boundaries and retransmits the incomplete Entry from offset
zero under Resume as specified above. After artifact preparation it reuses
evidence and never transfers, tars, or encrypts again.

Config restore first makes a complete local pass over the immutable archive,
validates it, and durably checkpoints `BackupConfigContentAuthority` in
`restore_artifact_validated`. It rewinds the same retained file for the distinct
Agent-to-Controller transfer. The wire transfer then has the metadata and value
phases above; capture uses the same phases in the opposite direction.
Metadata sends complete Entries without selected value bytes.
Controller persists exactly the sole direction-selected
`BackupConfigMetadataRow` defined in section 7.1: tags 1/2 are authority/content
digests, tag 3 is ordinal, tags 4/5 select capture/restore metadata, and tags 6/7
are preceding/resulting transfer chains. There is no transfer-id-bearing legacy
row or capture-only row. The transfer id and durable cursor belong only to the
bounded progress value. At least 67,108,864 bytes of etcd quota headroom is
required before each transaction. Progress holds transfer id, direction, next
ordinal, the completed-Entry boundary sequence, cumulative metadata chain, and
transcript state in deterministic protobuf of at most 512 bytes, giving O(1)
replay state. Session Credit, acknowledgements, chunk offsets, and partial-value
state are never persisted in that progress value.

The metadata row key remains exactly
`/v1/backup/config-metadata/<snapshot-id>/<entry-id>`: 27 prefix bytes,
30 snapshot-ID bytes, one separator, and 29 Entry-ID bytes, totaling 87. The
sole row maximum is `34+34+3+(1+2+7936)+34+34=8078` bytes. Its encoded etcd
`PutRequest` is `(1+1+87)+(1+2+8078)=8170`. A `RequestOp.request_put` wrapper is
`1+2+8170=8173`, and the enclosing `TxnRequest.success` member is
`1+2+8173=8176` bytes.

One metadata durability transaction admits `q=1..46` next contiguous rows; each
non-final transaction has exactly `q=46`, the final transaction has `q=1..46`,
and the empty-Config acceptance transaction uses `q=0`. It compares exactly the
one
66-byte progress key at `Version=0` for the first batch or exact positive
`ModRevision` later, plus each of the `q` Entry keys at `Version=0`. Its success
branch mutates exactly the `q` immutable Entry puts plus one progress put. Its
failure branch is empty, and it contains no Range, DeleteRange, or nested Txn.
Thus it has exactly `q+1` compares and `q+1` mutations, at most 94 local
operations, strictly below the Groundplane 96-operation application budget.

`/v1/backup/config-metadata-progress/<snapshot-id>` = 36-byte prefix + exact
30-byte snapshot id = 66 bytes.

An Entry absence `Compare` is 91 bytes: the 87-byte key costs 89 and the
oneof-selected zero Version costs two; its `TxnRequest.compare` wrapper makes
93. The progress compare is at most 80 bytes before and 82 bytes after its
wrapper: its key costs `1+1+66=68`, target `MOD` costs two, and the positive
`ModRevision` costs at most ten including its tag. The progress `PutRequest` is
at most `(1+1+66)+(1+2+512)=583`; its `RequestOp` and success wrappers make 586
and 589 bytes. Therefore the complete deterministic protobuf `TxnRequest`,
including every Compare, RequestOp, and repeated-field wrapper, is at most
`46*(93+8176)+82+589=381045` bytes. Construction rejects above either 94
operations or 393,216 encoded bytes, strictly below the Groundplane 96-operation application budget and the
1,048,576-byte encoded request limit. At most `ceil(4096/46)=90` nonempty metadata
commits are required. The initial grant admits the first such batch; after each
non-final commit, exactly one next-sequence replacement grant restores exactly
46 record slots and the 393,216-byte window. The final commit emits
`MetadataAccepted`, never a metadata replenishment. Receiver rejects unknown
fields and size violations before constructing this request.

The affected memory proof is also bounded. A metadata receiver retains at most
46 complete Header envelopes, `46*8118=373428` serialized bytes. Even if it
retains all of them while constructing the maximum 381,045-byte `TxnRequest`
and retaining the current and preceding maximum 293-byte Credit envelopes, one
active metadata receiver uses at most
`373428+381045+2*293=755059` protocol bytes. Across the Task-wide maximum of
`32*4=128` active bulk transfers this is 96,647,552 bytes. A value receiver's
one incomplete Entry plus those two Credit envelopes is at most 262,730 bytes,
or 33,629,440 bytes across 128 transfers; metadata and value accumulation are
phase-exclusive per transfer. The unchanged per-transfer 1,048,576-byte sender
queue cap contributes at most 134,217,728 bytes across 128 queues. Therefore
the affected Config bulk sender-queue plus receiver-accumulation ceiling is
230,865,280 protocol bytes, below 221 MiB, before fixed implementation object
overhead; implementations MUST cap that overhead separately and fail closed.

Only after all metadata is validated/durable does Controller send
`MetadataAccepted` and grant values.
Exactly one Entry is in flight. Controller validates it, writes that Entry's
hidden immutable `cfg_` generation, atomically commits ordinal/hash, then grants
next. Reconnect resumes
after committed ordinal; uncommitted Entry restarts offset zero. Controller does
not spool tar or retain completed value bytes in stream memory.

Restore disconnect before durable `MetadataAccepted` has created no target
generation: Controller discards receiver metadata state and the transfer
restarts at record 1 with zero credit. Once `MetadataAccepted` is durably
acknowledged, Controller retains the bounded metadata rows, dependency holds,
and preallocated generation IDs. Each completed EntryEnd atomically protects
and writes exactly one hidden generation and advances the durable ordinal and
hash-chain cursor. Reconnect zeroizes and discards the incomplete current value,
rolls every ephemeral record/byte-credit and record-sequence value back to that
Entry boundary, sends only the durable next ordinal and value chain in Resume,
and retransmits the first uncommitted Entry from offset zero under a new
session's sequence-1 credit. It never retransmits committed metadata headers or
completed values and never re-encrypts. After `config_transfer_completed`, no
transfer record is replayed.

`config_transfer_completed` has a pending checkpoint claim separate from
completed dedupe. Before staging, Controller pins one retry-stable restore
generation, every preallocated `cfg_` generation ID, the exact next render
generation, dependency revisions/holds, and bounded roll-forward cursors. Its
acceptance transactions verify cursors/hashes and roll canonical
`/v1/records/entries/<entry-id>` primaries and owner indexes forward in stable-ID
order. Omitted Entries are deleted and present Entries upserted. After the first
primary batch, recovery only rolls forward and never reallocates IDs. The final
transaction publishes the preallocated render generation, removes read hiding,
creates acknowledgement/fence and completed dedupe, clears pending claim, and
advances resume. There is no Environment-wide active Config generation or
current-pointer model. It owns primary Entry publication, not materialization.

`materialization_verified` has a separate pending claim spanning first external
write, verification, and obsolete-generation cleanup. Its final transaction
checks exact cursor, creates completed dedupe/fence, clears claim/holds, and
advances resume. Crash during cleanup resumes the pending claim.

```proto
message BackupConfigCheckpoint {
  oneof checkpoint {
    BackupConfigValueProgress value_progress = 1;
    BackupConfigTransferCompleted transfer_completed = 2;
    BackupConfigMaterializationVerified materialization_verified = 3;
  }
}
message BackupConfigProgress {
  bool metadata_accepted = 1;
  bytes metadata_transcript_sha256 = 2;
  uint64 next_value_ordinal = 3;
  bytes value_chain_sha256 = 4;
  oneof pending_claim {
    BackupConfigTransferCompleted transfer = 10;
    BackupConfigMaterializationVerified materialization = 11;
  }
}
message BackupConfigValueProgress { uint64 next_ordinal = 1; bytes chain_sha256 = 2; }
message BackupConfigTransferCompleted {
  string restore_generation_id = 1;
  BackupConfigContentAuthority content = 2;
  uint64 committed_record_count = 3;
  bytes value_chain_sha256 = 4;
  bytes transfer_transcript_sha256 = 5;
  uint64 render_generation = 6;
}
message BackupConfigMaterializationVerified {
  string restore_generation_id = 1;
  uint64 materialized_entry_count = 2;
  bytes materialization_sha256 = 3;
}
```

### 8. PostgreSQL capture and restore

MVP adapter contract version is exactly `1`, PostgreSQL 16 only, using one
first-party Groundplane-managed OCI index with exactly one native runnable
child for each supported `linux/amd64` and `linux/arm64` platform. Docker
selects only the child matching the local host; emulation is outside the MVP.
Each child embeds the helper and is runtime-pinned by its managed repository
and platform-specific release-record digests. Each platform's upstream runnable
child, config, and ordered layer descriptors, complete Config.Env, and helper and
client-gate measurements are authenticated and retained in ADR 0047's level-one
platform entry; its selected process profile is retained in the matching
level-one `process.platform_profiles` entry. The level-two release record
separately retains the post-push managed child, config, and added-layer digests;
source documentation does not predict those post-push managed digests.
The derived image preserves inherited Entrypoint, Cmd, and Env and does not set
Dockerfile `USER` or `WORKDIR`. ADR 0047 exclusively defines the complete
managed-image addition inventory.
This ADR incorporates that inventory through direct equality with ADR 0047's
managed-image release digest. It neither repeats nor extends ADR 0047's
directory, helper, private-gate, state-directory, label, metadata, byte, or
digest authority.
The state directory is not a mount, tmpfs, or Docker `VOLUME`; no host,
named, anonymous, socket, or secret mount is added for it.

Environment, Blueprint, Project, and operator cannot override repository, tag,
digest, binary, parent, helper/state path, platform, or contract version. The old
`postgres:16-alpine` selection is cleanly replaced without fallback.

Authority contains database Service and every cross-Environment consumer in
Compose coordination. Consumers sort by `(environment_id,service_id)` and carry
stable Service ID/current name, exact Service and Compose revisions, required
label count/canonical digest derived from sealed Compose YAML, repository digest,
prior runtime intent, and adapter contract version. Raw labels are not embedded
in assignment authority. Runtime
container ID is observed in a checkpoint, never sealed plan.

```proto
message BackupPostgresCaptureAuthority {
  uint32 adapter_contract_version = 1; // exactly 1
  string database_service_id = 2;
  reserved 3, 4;
  reserved "consumers", "managed_postgres_repository_digest";
  bytes database_name = 5;
  bytes role_name = 6;
  uint64 max_plaintext_bytes = 7;
}
message BackupPostgresRestoreAuthority {
  uint32 adapter_contract_version = 1; // exactly 1
  string database_service_id = 2;
  reserved 3, 4;
  reserved "consumers", "managed_postgres_repository_digest";
  bytes database_name = 5;
  bytes role_name = 6;
}
// Capture max_plaintext_bytes is exactly 5497558138880 for none or
// 5496216289016 for age; no estimate or other value is valid.
message BackupPostgresContainerObserved {
  string service_id = 1;
  string container_id = 2;             // exactly 64 lowercase hex bytes
  bytes repository_digest = 3;         // exactly 32 bytes
  uint32 observed_label_count = 4;     // 0..64
  bytes observed_labels_sha256 = 5;    // exactly 32 bytes
  bytes observation_sha256 = 6;        // exactly 32 bytes
}
message BackupPostgresDumpStart {
  string point_id = 1;
  bytes execution_nonce = 2;           // exactly 32 bytes
  string container_id = 3;
  string exec_id = 4;                  // exactly 64 lowercase hex bytes
  bytes repository_digest = 5;         // exactly 32 bytes
  bytes expected_labels_sha256 = 6;    // exactly 32 bytes
  uint32 adapter_contract_version = 7; // exactly 1
  bytes database_name = 8;
  bytes role_name = 9;
  uint64 max_plaintext_bytes = 10;
}
message BackupPostgresRestoreApplyStartCheckpoint {
  string point_id = 1;
  bytes execution_nonce = 2;           // exactly 32 bytes
  string container_id = 3;
  string exec_id = 4;                  // exactly 64 lowercase hex bytes
  bytes repository_digest = 5;         // exactly 32 bytes
  bytes expected_labels_sha256 = 6;    // exactly 32 bytes
  uint64 source_size_bytes = 7;
  bytes source_sha256 = 8;             // exactly 32 bytes
}
message BackupPostgresRestoreVerified {
  string point_id = 1;
  string container_id = 2;
  BackupArtifactEvidence evidence = 3;
  bytes verification_sha256 = 4;       // exactly 32 bytes
}
message BackupPostgresServiceProgress {
  uint64 service_cursor = 1;
  string service_id = 2;
  BackupVolumeServicePhase phase = 3;
  bytes observation_sha256 = 4;        // exactly 32 bytes
}
message BackupPostgresArchiveEvidence {
  uint32 pg_dump_major = 1;            // exactly 16
  uint32 adapter_contract_version = 2; // exactly 1
}
```

Container selection uses only sealed Compose authority, labels, Service identity,
and repository digest. `postgres_container_observed` durably assigns observed
container ID. Managed PostgreSQL Service validation at compile/authority construction and
ContainerInspect at runtime canonicalize mount destinations and require the
observed Mounts set to equal the exact sealed Mounts set. They reject a mount
destination inside, equal to, or a path-component ancestor of any protected root
or protected path: `/bin`, `/sbin`, `/lib`, `/lib64`, `/usr`, `/etc`,
`/run`, `/var/run`,
`/usr/local/libexec/groundplane-postgres16-helper`, and
`/usr/local/libexec/groundplane-postgres16-client-gate`, and
`/run/groundplane-postgres16`. Path-component comparison is required; string
prefix is insufficient. The canonical managed data mount at
`/var/lib/postgresql/data` is the sole exception and is accepted only when its
complete source, destination, mode/type, propagation and read/write facts are
sealed in Compose authority and observed byte-for-byte equal. No extra target
may shadow helper, `env`, PostgreSQL clients, dynamic runtime, identities, or
the PostgreSQL socket.

The managed container's HostConfig security projection is exact:

```text
Privileged: false
CapDrop: [ALL]
CapAdd: [CHOWN,DAC_OVERRIDE,FOWNER,KILL,SETGID,SETPCAP,SETUID,SYS_PTRACE]
SecurityOpt: [no-new-privileges:true]
```

The ordered capability allowlist is the complete root startup/helper envelope:
`CHOWN`, `DAC_OVERRIDE`, and `FOWNER` permit the inherited entrypoint's bounded
data-directory preparation; `SETGID` and `SETUID` permit its numeric `70:70`
drop; `KILL` permits pidfd signaling of the uid-70 client; `SYS_PTRACE` permits
cross-uid `/proc` identity attestation by the root parent; and `SETPCAP` permits
only the root client's gate to empty its bounding set. Compose authority seals these exact fields. ContainerInspect requires exact
equality and rejects privileged mode, an additional capability/security option,
or a missing value.

After the inherited entrypoint execs the live postmaster, Agent authenticates
the container init/postmaster lifetime from ContainerInspect pid plus host
`/proc` start ticks and boot id. `/proc/<pid>/status` must report all four uid
and gid values as 70, empty supplementary groups, `CapInh`, `CapPrm`, `CapEff`
and `CapAmb` zero, `CapBnd` equal exactly the eight-name HostConfig allowlist, and
`NoNewPrivs: 1`; executable and closed server argv must match the release-pinned
postmaster. Release rootfs proof rejects every uid-70-reachable setuid/setgid or
file-capability executable and every uid-70-writable executable path component.
With saved ids 70, zero active capabilities and no-new-privileges, the
postmaster cannot reacquire a startup capability. Failure blocks Ready and all
ordinary PostgreSQL Backup work.

Exact Docker Exec argv prefix is:

```text
[0] /usr/bin/env
[1] -i
[2] PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
[3] HOME=/nonexistent
[4] LC_ALL=C
[5] TZ=UTC
[6] /usr/local/libexec/groundplane-postgres16-helper
```

The exact suffix begins
`run,1,<64-lowercase-hex-nonce>,<hard-deadline-unix-nano>` followed by exactly
one of these closed operation suffixes:

```text
probe-pg-dump,16
probe-pg-restore,16
probe-psql,16
server-major,16,<database>
dump,<database>,<role>
restore-list,<source-size>,<source-sha256>
terminate-db-connections,<database>
assert-zero-db-connections,<database>
restore-apply,<source-size>,<source-sha256>,<database>,<role>
post-restore-verify,<database>
```

The fixed helper binary is embedded in the approved Groundplane-managed
PostgreSQL image and runs inside the already managed database container. It is
not a helper image or helper container. Packaging, Controller authority, and
Agent execution share closed constants and validators in the cycle-free leaf
package `internal/common/postgres16protocol`; no package copies argv, paths,
platform, contract version, or stream policy.

ADR 0047 exclusively owns the immutable two-level PostgreSQL release authority.
Its exact build contract binds upstream index, runnable manifest/config digests,
platform, helper and private-gate bytes/mode, closed client paths/environment/
security/fd/gate profiles, state metadata, inherited fields and schema; its
exact release record binds that build-contract digest to actual managed
repository and post-push digest. ADR 0048 references that build contract, release
record and their digests and defines no competing schema. ADR 0048 alone owns
the exact Moby ExecCreate, sole `ContainerExecAttach` request, raw-stream
demultiplexing, and runtime mount-attestation mechanics. Controller seals the
referenced published digest and Agent reattests it. Managed repository/post-push
digest are never guessed, tag-derived, or defaults. The only second command is
`stop,1,<nonce>,<cleanup-deadline>`; no collect,
shell, generic command, generic SQL, filename, password, or ambient environment
mode exists. Every Moby ExecCreate has exact numeric `User="0:0"`,
`Privileged=false`, `Tty=false`, `WorkingDir=/`, empty `Env`,
`DetachKeys=""`, zero `ConsoleSize`, `AttachStdout=true` and
`AttachStderr=true`. `AttachStdin=true` only for restore-list/apply; no
`OpenStdin` exists. Execution uses one attached start, Moby
`ContainerExecAttach` / `POST /exec/{id}/start`, with `Detach=false`,
`Tty=false` and zero `ConsoleSize`; never separate sole attached-start request. TTY false requires
`stdcopy.StdCopy` raw-stream demultiplex before independent stdout/stderr policy.
Capture has no stdin. The root helper gets exactly PATH, HOME, LC_ALL, TZ. It
owns ADR 0047's journal/recovery/state and launches only the private release-
pinned gate and resulting database client through `internal/postgres16helper`'s
existing narrow raw `os/exec`/Linux-process-control exception. The generic
shared Runner remains unchanged and is not used for this privileged boundary.
The closed helper-side builder fixes numeric real/effective/saved/filesystem
uid/gid `70:70`, empty supplementary groups, zero capability sets,
`PR_SET_NO_NEW_PRIVS=1`, the replacement four-entry environment, fixed absolute
client executable, closed argv including `--no-password`, and exact three-fd
profile. There is no shell, PATH lookup, ambient environment, credential,
secret, caller-selected process field, or second process-launch path. The root
helper retains ownership of the absolute deadline, process group, pidfd,
pidfd-only leader TERM/KILL/STOP/CONT, fd3/fd4 gate handshake, bounded single
reap, descriptors, and nonce state.

ADR 0047's `internal/common/postgres16protocol` table is the sole numeric helper
exit authority; ADR 0048 defines no duplicate mapping. Each probe stdout and
stderr stream has ADR 0047's independent exact 32,768-byte helper limit;
ADR 0048 adds no alternate or pooled Moby-side probe allowance.

The pinned Engine API represents ExecInspect `ExitCode` as nullable, while the
pinned high-level Moby Go client result flattens a null value to integer zero.
That high-level result therefore MUST NOT classify PostgreSQL helper terminal
state. `internal/infra/docker` owns one narrow, presence-preserving decoder for
the raw Engine `GET /exec/{id}/json` response. It retains the nullable exit-code
presence together with `ID`, `ContainerID`, and `Running`, returns only the
closed typed PostgreSQL-exec inspection needed by this runtime, and exposes no
generic raw Engine request, response, transport, or JSON surface to Agent or
other packages. Null, missing, malformed, or out-of-range exit state is
`recovery_required`; it is never converted to zero.

The Agent interprets a present helper exit only after that decoder returns the
expected exec id and container id with `Running=false`. Exit zero alone is
never terminal or success proof. For `run`, the Agent also requires the
acknowledged attached-start path, all operation-selected streams and EOF checks,
the helper's exact child-zero/original-parent-reap/state-removal proof, and this
ADR's same-container not-running ExecInspect result. A successful `stop`
instead requires either original-parent normal stop or already-reaped cleanup
with exact terminal child status, sole Wait4 when applicable, durable
`phase=reaped`, and state removal; or non-parent/boot-change recovery with
durable `recovery_retired`, exact `unavailable_not_parent` or
`unavailable_boot_changed`, and state removal. Neither branch requires child
exit zero or claims workload success; the non-parent branch never claims Wait4
or `reaped`.

A known `1..12` exit is consumed only with its ADR-0047 meaning and the current
operation's retry/destructive-boundary rules. Exit `3`, any normal value outside
`0..12`, `126`, `127`, any `128..255` signal-style value, an ExecInspect error,
`Running=true`, null or otherwise unproven exit state, or an
exec/container identity mismatch is `recovery_required`, never a helper
success or a guessed child result. Moby/API failures have no invented helper
exit code. The Agent preserves the observed integer only as bounded internal
evidence and returns the typed Backup outcome required by this ADR; it does not
publish a raw helper diagnostic through the operator REST/CLI/Console surface.

Docker Exec root is bounded inside the already managed PostgreSQL container and
does not weaken `Privileged=false`, protected-mount rejection, exact managed
image/helper digest attestation, or the no-copy/no-helper-container rule. The
Agent's Docker authority, Docker daemon and host root, and container root are
explicit trust-boundary actors. PostgreSQL server uid 70 is outside the trusted
supervisor-state boundary and cannot traverse or mutate the root-owned state
tree; protection from host root or container root is not claimed.

Before this release can send true Ready, advertise positive capacity, or accept
ordinary Backup, the Agent applies ADR 0047's zero-old-state activation barrier
to every managed PostgreSQL container. Any legacy uid-70, nonempty, malformed,
or unknown-version state blocks those actions; it is never chowned, deleted,
migrated, or adopted. Authentication, false Ready with zero capacity, inventory,
Controller dispositions, stop, and recovery remain available. The clean
pre-release replacement keeps no old helper or capability. Discovery of old
state stays false Ready and recovery-required.

Capture source length is unknown. Exactly one `pg_dump` runs: no estimate,
sealed bound, probe, or two-pass dump. Before sole attached-start request, Agent obtains
`postgres_dump_start` acknowledgement with container, fresh 32-byte nonce, and
deadline. Once acknowledged the attempt is spent. Crash before
`artifact_prepared` uses nonce recovery to clean and fails. It cannot dump again
under that assignment; only Controller generic retry with new point/execution
and barrier may.

`max_plaintext_bytes` is a sealed safety ceiling, not an estimated or expected
source length and not a reservation. Source uses `ExclusiveUnknown`, physically
preallocating every exact next write range. Age stored output grows under the
same exclusive mode; none reuses source.
Only EOF, helper exit zero, fsync, fstat, hashing, and final installation create
actual evidence. Failure creates no artifact checkpoint/upload. Lost prepared
ack verifies retained files and never dumps again.

Restore uses two independent readers of the retained decoded host source, both
opened at offset zero. It never uses Docker cp or an in-container staged file.
For each restore-list/apply exec, AttachStdin is true; no nonexistent Moby
`OpenStdin` field is assumed. Agent forwards canonical chunks of at most 32 KiB,
calls CloseWrite only at exact EOF, and helper hashes/counts all bytes, requires
the expected size/SHA and one extra EOF read, then closes child stdin. Early,
extra, short, or mismatched input fails.

Restore-list is safe validation and may rerun. Its stdout is fully drained and
discarded with O(1) retained memory and a checked uint64 byte count, without a
size rejection; stderr alone is capped at 32 KiB. Complete input and child exit
zero are required. Apply stdout and stderr each have an independent hard 32-KiB
drain/discard cap. Diagnostics are not logged or persisted.

After list success, Controller durably holds owning Attach, surviving grants,
all consuming and fact-exposed Services across Environments, exact revisions,
and prior runtime intents; it locks affected Environments in ID order and
orders effects in Service-ID order. The Agent alone stops/inspects host Services
and returns checkpoint proof for Controller acknowledgement. Agent runs the
fixed terminate and zero-connection operations. It then reattests, ExecCreates restore-apply in the
unstarted state, and sends `postgres_restore_apply_start` binding exact nonce,
container/exec, repository/labels, source evidence, and the request's sealed
database, role, target, dependency, lock, and hold authority. No apply helper or
stdin byte may exist before Controller acknowledgement.

After acknowledgement Agent reattests the same container and exec, opens a
fresh second reader, and starts-and-attaches once through sole
`ContainerExecAttach`. This acknowledgement is the
destructive boundary. Crash before acknowledgement is safe redo. After
acknowledgement Agent never blindly starts or reapplies. Exact helper input
size/hash/EOF, child exit zero/reap, and same-container ExecInspect proving
not-running/exit-zero are positive terminal proof; recovery then runs only fixed
post-restore verification. Once apply-start is acknowledged or helper
`gate_released` is durable, the attempt is spent. Zero forwarded bytes,
pre-exec failure, boot change, unknown release consumption, or any result short
of the complete successful terminal proof is `recovery_required` and cannot
authorize another apply intent; artifact, authority, locks, holds,
and stopped consumers remain for explicit recovery. Live nonce state exists
only during a run for authenticated stop and removes itself after child reap;
terminal ExecInspect is the retained proof. Fixed post-restore verification is
then checkpointed as `postgres_restore_verified`. Previously running Services
restart in ID order through the same acknowledged restart/health phases used by
Volume and checkpoint `postgres_service_progress`. Restart failure resumes only
Service recovery and never reapplies.

### 9. Volume capture and restore

```proto
enum BackupVolumeEntryKind {
  BACKUP_VOLUME_ENTRY_KIND_UNSPECIFIED = 0;
  BACKUP_VOLUME_ENTRY_KIND_DIRECTORY = 1;
  BACKUP_VOLUME_ENTRY_KIND_REGULAR = 2;
}
message BackupVolumeManifestEntry {
  uint64 ordinal = 1;
  bytes relative_path = 2;
  BackupVolumeEntryKind kind = 3;
  uint32 mode = 4;
  uint32 uid = 5;
  uint32 gid = 6;
  uint64 size_bytes = 7;
  bytes content_sha256 = 8;
}
message BackupVolumeArchiveEvidence {
  uint64 entry_count = 1;
  bytes content_manifest_sha256 = 2;
  bytes full_tree_sha256 = 3;
  uint64 source_size_bytes = 4;
}
message BackupVolumeProjectionAuthority {
  string artifact_id = 1;
  bytes artifact_sha256 = 2;
  uint64 artifact_revision = 3;
  uint64 projection_root = 4;
  uint64 render_generation = 5;
  string compose_volume_key = 6;
  string docker_volume_name = 7;
  string authorized_volume_dir = 8;
}
message BackupVolumeCaptureAuthority {
  string volume_id = 1;
  RevisionDigest volume = 2;
  uint64 source_size_upper_bound = 3;
  reserved 4;
  reserved "consumers";
  BackupVolumeProjectionAuthority projection = 5;
}
message BackupVolumeRestoreAuthority {
  string volume_id = 1;
  RevisionDigest volume = 2;
  BackupVolumeArchiveEvidence archive = 3;
  reserved 4;
  reserved "consumers";
  BackupVolumeProjectionAuthority projection = 5;
}
```

`projection` is required and is copied byte-for-byte from the immutable
Controller projection fixed at assignment. `projection_root` is exactly the
positive etcd ModRevision of the `EnvironmentBlueprintSeal` stored at
`/v1/records/environment-blueprint-revisions/<environment-id>/<desired-revision-id>/root`.
The seal selects the historical `EnvironmentComposeProjection`; its
`ComposeArtifact` field supplies `artifact_id` and `artifact_sha256`, and
`artifact_revision` MUST equal `projection_root`. The projection's stored
`RenderGeneration` MUST equal both this message's `render_generation` and the
seal's `RenderGeneration`. Any missing record, revision inequality, digest
mismatch, or render-generation inequality fails before assignment.

For capture, `authorized_volume_dir` is the captured Environment's durable
`volume_dir`; together with `compose_volume_key` it selects the existing live
source directory. For Restore, the same field is the destination Environment's
durable `volume_dir`; together with the destination projection's key it selects
the live replacement target and its hidden sibling parent. Restore never copies
this directory authority from Recovery Point metadata or the source
Environment. The enclosing capture-versus-Restore authority changes this
directional meaning without changing the byte-identical, digest-bound
`BackupVolumeProjectionAuthority` shape.

The enclosing authority supplies the stable Volume identity and revision; the
sole Task authority supplies the stable Environment identity and revision; and
the step's consumer references resolve only into the common Service facts that
pin every mounting Service and Compose revision and exact prior runtime intent.
Tag 4 remains reserved for the removed `consumers` field. Neither Controller nor
Agent may infer, resolve, or repair any projection fact from mutable runtime
state.

Manifest has at most 2,048 entries including root. Relative path is at most
4,095 bytes. Root is the sole entry whose complete path is `.`. Every descendant
has no NUL, empty component, `.`, `..`, leading slash, `./` prefix, or trailing
slash and stays beneath opened root with component-safe no-symlink resolution. Entries are
unique and unsigned-byte sorted, root first. Links, devices, FIFOs, sockets,
xattrs, ACLs, sparse extensions, and mount crossing reject.

The Controller orders and acknowledges every host effect; the Agent alone
executes Docker/filesystem stop, inspect, restart, health, and Volume mutation.
Capture observes sealed consumers in Service-ID byte order. For an observed
running Service it checkpoints `STOP_INTENT`, waits for acknowledgement, stops,
verifies, then checkpoints `STOPPED`. An observed non-running Service checkpoints
`NOT_RUNNING` and is never started by Backup. It captures one fixed tree and
prepares the artifact. Previously running consumers then advance in canonical
order through acknowledged `RESTART_INTENT`, verified `RESTARTED`,
`HEALTH_WAIT`, and `HEALTHY`. Volume capture succeeds only when all previously
running consumers are healthy. Upload may run after restart, but does not erase
restart/health resume. Each observation must equal the assignment's exact
prior-runtime-intent kind and revision. Mismatch fails before stop. "Previously
running" means sealed prior intent `RUNNING`, never a volatile container
observation; adoption and reconnect carry the same intent revision.

Restore validates artifact/manifest before stopping. It durably: persists the
validated new manifest; stops and verifies consumers in Service-ID order;
persists the stopped-live old manifest; constructs a hidden replacement in
archive order; finalizes files then directories in descending unsigned path
order with descendants before ancestors and root last; computes the new digest
and atomically exchanges roots; deletes the old tree in descending descendant
path order with root last; restarts and health-checks consumers.

Validated new and stopped-live old manifests are separate immutable Controller
sets. Every canonical wire Batch is exactly one manifest transaction: 1..31
Entry puts plus exactly one root/progress put, at most 32 operations and 262,144
key/value bytes before framing because each encoded KV is at most 8,192 bytes.
The Controller never combines or splits a wire Batch, commits it before the
cumulative ack, and needs at most `ceil(2048/31)=67` manifest-entry commits.
Root/progress pins role, point/restore IDs, count, content-manifest digest,
full-tree digest, transfer chain, next ordinal, completion, and previous root
revision. Transactions compare that revision and entry-key absence; completed
rows never rewrite. Resume carries O(1) cursors/hash chains rather than
accumulated entries.

Before capture, exact tar bytes plus 67,108,864 bytes must fit Agent staging.
Before restore construction, total regular-file bytes plus 67,108,864 bytes and
enough inodes must fit the target filesystem. Overflow, quota, allocation,
inode, or query failure rejects before output/mutation.

Every construction mutation has its own acknowledged intent and completion.
The intent binds point/restore generation, new content-manifest digest, cursor before,
archive ordinal, path, kind, mode, uid, gid, size, and content digest. Completion
clears that intent and advances only after required file and parent fsyncs.
A pending regular file may be absent, temporary `0:0/0600` with exact expected
prefix, final uid/gid with `0600` and complete content, or exact final state; a
pending directory is absent or exact temporary `0:0/0700` without later
descendants. Recovery removes/replays temporary state or completes remaining
metadata/fsync. Any other state is ambiguity.

Every directory finalization similarly has an acknowledged intent binding
point/restore generation, new content-manifest digest, cursor/ordinal, path, and final
uid/gid/mode. Completion follows final metadata and directory fsync. Pending
state may be temporary `0:0/0700`, final uid/gid with `0700`, or exact final
metadata; any other state is ambiguity.

After finalization Agent fsyncs the authorized parent and verifies new full-tree
digest. An acknowledged exchange intent fixes constant pre-exchange old and
post-exchange new full-tree digests. Agent proves one filesystem, calls
`renameat2(RENAME_EXCHANGE)` exactly once, fsyncs parent, and verifies live-new.
Resume accepts only live-old/hidden-new, which exchanges once, or
live-new/hidden-old, which replays completion without exchange. No fallback,
copy, move-aside, or delete-then-rename exists.

Before unlink/rmdir, Agent sends intent with exact path bytes, kind, parent,
deletion cursor before, one-based deletion ordinal, and both digests and waits
for Controller acknowledgement.
After unlink/rmdir plus parent fsync it sends completion named exactly
`volume_replaced_path_cleaned`, repeating fields. Cursor and one pending intent
define survivors. Every pending delete repeats the same fixed two digests.
Changing subtree hashes are forbidden.

Without a pending deletion exactly the cursor suffix survives; with one pending,
only its exact path may be present or absent. Present is verified and removed;
absent permits replay of only that completion. A gap, completed survivor,
changed kind/parent, non-empty directory, changed live tree, or extra path is
ambiguity. Root completion records authorized-parent fsync and moves directly
to restart/health. Durable deletion cursor equal to entry count is forbidden.
There is no aggregate tree-cleaned checkpoint. Restore completes only with
healthy Services.

Volume manifests use a dedicated bidirectional bulk protocol and never a
`BackupCheckpointRequest` payload:

```proto
enum BackupVolumeManifestDirection {
  BACKUP_VOLUME_MANIFEST_DIRECTION_UNSPECIFIED = 0;
  BACKUP_VOLUME_MANIFEST_DIRECTION_CAPTURE_AGENT_TO_CONTROLLER = 1;
  BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_NEW_CONTROLLER_TO_AGENT = 2;
  BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_OLD_AGENT_TO_CONTROLLER = 3;
}
enum BackupVolumeManifestRole {
  BACKUP_VOLUME_MANIFEST_ROLE_UNSPECIFIED = 0;
  BACKUP_VOLUME_MANIFEST_ROLE_CAPTURED = 1;
  BACKUP_VOLUME_MANIFEST_ROLE_RESTORE_NEW = 2;
  BACKUP_VOLUME_MANIFEST_ROLE_STOPPED_LIVE_OLD = 3;
}
message BackupVolumeManifestTransfer {
  string task_id = 1;                  // exact 31 bytes
  string assignment_id = 2;            // exact 31 bytes
  string step_id = 3;                  // exact 31 bytes
  bytes authority_digest = 4;          // exactly 32 bytes
  bytes transfer_id = 5;               // exactly 32 bytes
  BackupVolumeManifestDirection direction = 6;
  uint64 record_sequence = 7;          // 1..69
  oneof record {
    BackupVolumeManifestStart start = 10;
    BackupVolumeManifestBatch batch = 11;
    BackupVolumeManifestEnd end = 12;
    BackupVolumeManifestResume resume = 13;
  }
}
message BackupVolumeManifestStart {
  string point_id = 1;                 // exact 29 bytes
  string restore_generation_id = 2;    // empty capture; exact 30 restore
  BackupVolumeManifestRole role = 3;
  uint32 entry_count = 4;              // 1..2048, root included
  bytes content_manifest_sha256 = 5;   // exactly 32 bytes
  bytes full_tree_sha256 = 6;          // exactly 32 bytes
  oneof source_archive_evidence {
    BackupVolumeSourceArchiveEvidence source_archive = 7;
    BackupVolumeNoSourceArchiveEvidence no_source_archive = 8;
  }
}
message BackupVolumeSourceArchiveEvidence {
  uint64 source_size_bytes = 1;
  bytes source_sha256 = 2;             // exactly 32 bytes
}
message BackupVolumeNoSourceArchiveEvidence {}
message BackupVolumeManifestBatch {
  uint64 first_ordinal = 1;
  repeated BackupVolumeManifestEntry entries = 2; // non-final exactly 31; final 1..31
  bytes preceding_transfer_chain_sha256 = 3; // exactly 32 bytes
  bytes resulting_transfer_chain_sha256 = 4; // exactly 32 bytes
}
message BackupVolumeManifestEnd {
  uint32 entry_count = 1;
  bytes content_manifest_sha256 = 2;
  bytes full_tree_sha256 = 3;
  bytes final_transfer_chain_sha256 = 4;
}
message BackupVolumeManifestResume {
  uint64 next_record_sequence = 1;
  uint32 next_ordinal = 2;
  bytes preceding_transfer_chain_sha256 = 3;
  uint64 prior_ack_sequence = 4;
  uint64 prior_committed_record_sequence = 5;
}
message BackupVolumeManifestAckCredit {
  string task_id = 1;
  string assignment_id = 2;
  string step_id = 3;
  bytes authority_digest = 4;
  bytes transfer_id = 5;
  uint64 ack_sequence = 6;
  uint64 committed_record_sequence = 7;
  uint32 next_ordinal = 8;
  bytes transfer_chain_sha256 = 9;
  uint32 record_credit = 10;
  uint64 byte_credit = 11;
}
```

Direction and role are the matching 1, 2, or 3 pair. Capture and restore-new
roles MUST set `source_archive` with a source SHA-256 and its exact possibly-zero
archive size. The stopped-live-old role MUST instead set the explicit empty
`no_source_archive`; it is a filesystem scan, creates no source archive, and has
no source size or source SHA-256. The opposite presence choice is invalid.

`content_manifest_sha256` is the role-independent digest of only the canonical
ordered Entries defined in section 3.1 and ADR 0047. Start seals it before any
Batch. Each Batch carries the exact preceding and resulting role-bound transfer
chain, and End repeats the Start count, content digest, and full-tree digest plus
the final transfer chain. The receiver independently recomputes both digests;
End is invalid unless every repeated value and the durable cursor match.

Every Batch has the next contiguous record sequence, `first_ordinal` equal to
the durable next ordinal, exactly 31 contiguous canonical Entries unless it is the final 1..31-entry Batch, the exact durable
preceding transfer chain, and the resulting chain computed with the section 3.1
batch framing. The receiver rejects a gap, overlap, changed duplicate, reordered
Entry, content-digest mismatch, full-tree mismatch, or chain mismatch before
acknowledgement. End is the next record after the last Batch and is acknowledged
only after recomputation from the complete immutable Entry set.

`transfer_id` is
`SHA-256("groundplane.agent.v1.backup-volume-manifest-transfer-id\x00" ||
authority_digest || u32be(direction) || u32be(role))`. Start is fresh record 1.
For `N` entries, `b=ceil(N/31)`: every non-final Batch has exactly 31 entries,
the final Batch has 1..31, Batches have exact sequences `2..b+1`, and End has
sequence `b+2`. Thus `1<=b<=67` and End is sequence 3..69; wording that permits
a short non-final Batch or 32 entries is false. Resume is first after reconnect, repeats the prior committed sequence as a
non-chain assertion, and is accepted only with exact authority, direction,
transfer ID, ordinal, chain, and prior ack. It does not consume a semantic
manifest record; no acknowledged Entry replays.

An encoded `BackupVolumeManifestEntry` is at most 8,192 bytes. One repeated
field costs at most 8,195 bytes. Complete fixed outer/transfer/Batch framing at
maximum IDs, digests, and sequence is 250 bytes, so 31 entries are at most
`250+31*8195=254295`, below 262,144. Thirty-two are at least 262,490 and are
forbidden. Ack/Credit is at most 217 bytes, below 512. Credits grant 0..8 records
and 0..262,144 encoded bytes; at least one is positive for a grant, and
outstanding totals never exceed both queue limits. Both outer variants use the
per-transfer bulk scheduler.

Each admitted wire Batch is one Controller durable transaction with exactly its
1..31 immutable Entry puts and one progress put. The progress value includes
content digest, transfer sequence, prior ack, cursor, and transfer chain, so no
generic checkpoint dedupe/resume operation is added. The transaction is at most
32 operations and 262,144 key/value bytes before framing; 2,048 entries require
at most 67 such commits. The cumulative ack is sent only after that commit.

```proto
message BackupVolumeCheckpoint {
  oneof checkpoint {
    BackupVolumeConsumersStopped consumers_stopped = 1;
    BackupVolumeConstructionIntent construction_intent = 2;
    BackupVolumeConstructionCompleted construction_completed = 3;
    BackupVolumeFinalizationIntent finalization_intent = 4;
    BackupVolumeFinalizationCompleted finalization_completed = 5;
    BackupVolumeExchangeIntent exchange_intent = 6;
    BackupVolumeTreeExchanged tree_exchanged = 7;
    BackupVolumeDeleteIntent replaced_path_delete_intent = 8;
    BackupVolumeReplacedPathCleaned volume_replaced_path_cleaned = 9;
    BackupVolumeServiceProgress service_progress = 10;
  }
}
message BackupVolumeProgress {
  uint64 construction_cursor = 1;
  bytes construction_chain_sha256 = 2;
  optional BackupVolumeConstructionIntent pending_construction = 3;
  uint64 finalization_cursor = 4;
  bytes finalization_chain_sha256 = 5;
  optional BackupVolumeFinalizationIntent pending_finalization = 6;
  bytes old_full_tree_sha256 = 7;
  bytes new_full_tree_sha256 = 8;
  optional BackupVolumeExchangeIntent pending_exchange = 9;
  uint64 deletion_cursor = 10;
  optional BackupVolumeDeleteIntent pending_delete = 11;
  uint64 service_cursor = 12;
  BackupVolumeServicePhase service_phase = 13;
}
message BackupVolumeConsumersStopped { uint64 stopped_count = 1; bytes service_progress_sha256 = 2; }
message BackupVolumeConstructionIntent {
  string point_id = 1;
  string restore_generation_id = 2;
  bytes new_tree_content_manifest_sha256 = 3;
  uint64 construction_cursor_before = 4;
  uint64 archive_ordinal = 5;
  bytes logical_path = 6;
  BackupVolumeEntryKind kind = 7;
  uint32 mode = 8;
  uint32 uid = 9;
  uint32 gid = 10;
  uint64 size_bytes = 11;
  bytes content_sha256 = 12;
}
message BackupVolumeConstructionCompleted {
  BackupVolumeConstructionIntent intent = 1;
  uint64 construction_cursor_after = 2;
}
message BackupVolumeFinalizationIntent {
  string point_id = 1;
  string restore_generation_id = 2;
  bytes new_tree_content_manifest_sha256 = 3;
  uint64 finalization_cursor_before = 4;
  uint64 finalization_ordinal = 5;
  bytes logical_path = 6;
  uint32 final_uid = 7;
  uint32 final_gid = 8;
  uint32 final_mode = 9;
}
message BackupVolumeFinalizationCompleted {
  BackupVolumeFinalizationIntent intent = 1;
  uint64 finalization_cursor_after = 2;
}
message BackupVolumeExchangeIntent {
  string point_id = 1;
  string restore_generation_id = 2;
  bytes old_full_tree_sha256 = 3;
  bytes new_full_tree_sha256 = 4;
}
message BackupVolumeTreeExchanged {
  string point_id = 1;
  string restore_generation_id = 2;
  bytes old_full_tree_sha256 = 3;
  bytes new_full_tree_sha256 = 4;
}
message BackupVolumeDeleteIntent {
  string point_id = 1;
  string restore_generation_id = 2;
  bytes old_full_tree_sha256 = 3;
  bytes new_full_tree_sha256 = 4;
  uint64 deletion_cursor_before = 5;
  uint64 deletion_ordinal = 6;
  bytes logical_path = 7;
  BackupVolumeEntryKind kind = 8;
  bytes parent_logical_path = 9;
}
message BackupVolumeReplacedPathCleaned {
  string point_id = 1;
  string restore_generation_id = 2;
  bytes old_full_tree_sha256 = 3;
  bytes new_full_tree_sha256 = 4;
  uint64 deletion_cursor_before = 5;
  uint64 deletion_ordinal = 6;
  bytes logical_path = 7;
  BackupVolumeEntryKind kind = 8;
  bytes parent_logical_path = 9;
}
message BackupVolumeServiceProgress {
  uint64 service_cursor = 1;
  string service_id = 2;
  BackupVolumeServicePhase phase = 3;
  bytes observation_sha256 = 4;
}
enum BackupVolumeServicePhase {
  BACKUP_VOLUME_SERVICE_PHASE_UNSPECIFIED = 0;
  BACKUP_VOLUME_SERVICE_PHASE_NOT_RUNNING = 1;
  BACKUP_VOLUME_SERVICE_PHASE_STOP_INTENT = 2;
  BACKUP_VOLUME_SERVICE_PHASE_STOPPED = 3;
  BACKUP_VOLUME_SERVICE_PHASE_RESTART_INTENT = 4;
  BACKUP_VOLUME_SERVICE_PHASE_RESTARTED = 5;
  BACKUP_VOLUME_SERVICE_PHASE_HEALTH_WAIT = 6;
  BACKUP_VOLUME_SERVICE_PHASE_HEALTHY = 7;
}
```

Delete intent and completion are byte-equal except enclosing tag. Replay of acknowledged
intent inspects exact path under exchanged old root. Already absent succeeds only
when retained cursor/parent/kind/digests prove same pending operation. Mismatch
stops for operator inspection.

### 10. Restore artifact validation and format evidence

Restore verifies Head identity/metadata, downloads stored temp, validates stored
length/hash, decrypts if needed, validates source length/hash and format, fsyncs
and finalizes retained files, then checkpoints:

```proto
message BackupRestoreArtifactValidated {
  string point_id = 1;
  BackupObjectIdentity object = 2;
  BackupArtifactEvidence evidence = 3;
  BackupStagingFinals finals = 4;
  oneof format {
    BackupPostgresArchiveEvidence postgres = 10;
    BackupConfigArchiveEvidence config = 11;
    BackupVolumeArchiveEvidence volume = 12;
  }
}
```

Format is required. Config additionally carries exact shared archive evidence;
it is not inferred from tar or extension. No destination mutation precedes its
ack. PostgreSQL uses its start/mutation barriers, Config uses two-pass
metadata/values, and Volume uses construction/finalization/exchange/path cleanup.
Capture payloads never serve as restore evidence.

### 11. Restore identity, protected continuation, and adoption

```proto
message BackupOriginalIdentity {
  bytes original_execution_id = 1;     // exactly 32 bytes
  RevisionDigest destination = 2;
  string original_assignment_id = 3;
}
message BackupAdoptedIdentity {
  bytes adoption_id = 1;               // exactly 32 bytes
  bytes predecessor_execution_id = 2;  // exactly 32 bytes
  CheckpointFence predecessor_fence = 3;
  uint64 adoption_mod_revision = 4;
  string predecessor_assignment_id = 5;
  string adopted_assignment_id = 6;
}
```

The sealed identity-source oneof cannot change in assignment. Before first
destination mutation, an operator-old execution may release locks/holds and fail.
After first mutation it is protected: authority, locks, holds, finals, helper
nonce, pending claims/intents, cursors, and resume fence cannot enter generic
retry.

A new operator execution can continue only through one Controller transaction
that compares exact old authority/fence, verifies retained files/holds, creates
adoption identity, transfers identical locks/holds/cursors/pending/resume, and
terminalizes old identity as adopted. New assignment carries exact adoption
revision/digest and resumes; it does not rerun dump, redownload validated data,
republish Config, reconstruct exchanged Volume, or clear pending destruction.
Uncertain adoption fails closed.

Old identity is otherwise volatile and grants no retry. Post-mutation terminal
failure is recovery-required. Non-authoritative result remains:

```proto
message BackupTaskResult {
  string failed_step_id = 1;
  bool recovery_required = 2;
}
```

Authority remains in Controller checkpoint/resume/hold/object/adoption rows.

### 12. Controller-only retention and deterministic prune

Only Controller evaluates retention. Agent never bucket-lists to choose points.
At one revision Controller excludes held, restoring, pending, unverified, or
already-deleting points; orders exact raw canonical Recovery Point IDs by
unsigned bytes descending; and retains the newest `keep` IDs exactly as accepted
ADR 0024 requires. ULID time inside the raw `rp_` ID is the sole retention time;
no captured-at field or wall-clock tie breaker exists. Deletions order point ID
ascending, then object key ascending, discriminator kind (`version_id` before
`etag`), and discriminator value ascending.

One executable prune authority seals the first 1..11 candidates, with point revision,
evidence, metadata, Connector, bucket, key and discriminator. No later candidate
is substituted. Before Delete, Head must reproduce exact object metadata digest,
four-part evidence, and discriminator. Mismatch retains authority for operator
inspection and is never auto-deleted. Each deletion step is at most 1,800 seconds. Its transaction
advances cursor and exact point state. NotFound succeeds only for proved replay
of the same sealed deletion.

```proto
message BackupPruneObject {
  uint64 ordinal = 1;
  string point_id = 2;
  RevisionDigest point = 3;
  BackupArtifactEvidence evidence = 4;
  BackupObjectIdentity object = 5;
  uint32 metadata_count = 6;           // exactly 7
  bytes metadata_sha256 = 7;
}
message BackupPruneObjectDeleted {
  uint64 ordinal = 1;
  string point_id = 2;
  BackupObjectIdentity object = 3;
}
```

### 13. Transition ownership

| Operation | Ordered authoritative transitions |
| --- | --- |
| PostgreSQL capture | container observed; dump start acknowledged; artifact prepared; upload completed; Head/upload verified; Controller point commit; source cleanup completed; terminal |
| Config capture | bounded transfer; artifact prepared; upload completed; Head/upload verified; Controller point commit; source cleanup completed; terminal |
| Volume capture | consumers stopped/progress; artifact prepared; restart/health progress; upload completed; Head/upload verified; Controller point commit; terminal |
| PostgreSQL restore | artifact validated; container observed; list validation; consumers stopped/zero connections; apply start acknowledged; exact terminal proof; fixed verification; restart/health; terminal |
| Config restore | artifact validated; metadata progress/completed; value progress; pending transfer; publication; pending materialization; materialization verified; terminal |
| Volume restore | artifact validated; stopped; construction; finalization; exchange; repeated delete intent/path cleaned; restart/health; terminal |
| Prune | repeated sealed object deletion; terminal |

`artifact_prepared` is direct capture-to-upload evidence. `upload_completed`
separates Put from Head, and `upload_verified` separates Head from the
Controller-owned point-commit transaction. There is no Agent point-commit
checkpoint. There is no active-generation checkpoint.
Config pending claims differ from completed dedupe. Volume has no aggregate
cleanup checkpoint. PostgreSQL sole attached-start request follows an acknowledged barrier.

`TaskEvent` is diagnostic only: it cannot checkpoint, grant credit, move cursor,
publish, release a hold, or terminalize. `BackupTaskResult` cannot replace the
Controller transaction log.

#### Volume manifest role, credit, and replay

The sole `BackupVolumeManifestRole` definition is in section 9. Its canonical
symbols are `UNSPECIFIED=0`, `CAPTURED=1`, `RESTORE_NEW=2`, and
`STOPPED_LIVE_OLD=3`; `BackupVolumeManifestStart.role` is tag 3. The role MUST
correspond exactly to transfer direction 1, 2, or 3 respectively. There is no
fourth role and `UNSPECIFIED` is invalid on the wire.

A fresh transfer sends `Start` as record sequence 1 without prior credit. The
receiver validates and durably creates the transfer session, then returns
`BackupVolumeManifestAckCredit` with `ack_sequence=1`,
`committed_record_sequence=1`, `next_ordinal=1`, the transfer-chain seed, and a grant of at
most eight records and 262,144 encoded bytes. Each subsequent `Batch` or `End`
consumes one record and its complete outer `proto.Size`; outstanding credit may
never exceed both bounds. The receiver durably commits each accepted wire Batch
as exactly its 1..31 Entry puts plus one progress put, at most 32 operations and
262,144 key/value bytes, before returning the next cumulative ack and replacement
credit. An ack's `ack_sequence` equals its
`committed_record_sequence`; a duplicate record receives the byte-identical
stored ack and does not consume credit twice.

After reconnect, `Resume` is the sole first-record exception to credit. Its outer
`record_sequence` repeats `prior_committed_record_sequence` as a nonsemantic
assertion; its `next_record_sequence`, `next_ordinal`, preceding chain, and prior
ack sequence MUST equal the receiver's durable session. The receiver replays that
ack with fresh bounded credit. The next data record has exactly
`next_record_sequence`; committed entries are never retransmitted. A mismatch in
authority, transfer ID, cursor, transfer chain, or prior ack fails closed. `End` is complete
only after its cumulative ack is durable.

#### PostgreSQL helper recovery retirement

The embedded root helper's durable `root:root` state uses the closed `phase` vocabulary
`phase_created`, `gate_durable`, `gate_released`, `profile_applied`,
`child_durable`, `io_complete`, `terminal`, `reaped`, and
`recovery_retired`. A normal live supervisor remains the child parent: after a
terminal pidfd/proc observation it performs exactly one `Wait4`, durably records
the exact wait status and `reaped`, fsyncs the state, unlinks it, and fsyncs the
parent directory.

Recovery by a different helper invocation has a separate closed branch. If the
stored supervisor lifetime is proven absent or terminal, the recovery invocation
is not the child parent and MUST NOT claim a normal exit status or `reaped`:

- ADR 0047's pidfd-first rule applies to every recovered supervisor, gate,
  post-exec client, and scan candidate. `/proc` enumeration supplies only a
  numeric candidate: recovery must call `pidfd_open(candidate_pid, 0)` first,
  bracket the complete stored-identity `/proc` snapshot with zero-timeout pidfd
  `ppoll(events=POLLIN)` calls that each return zero with no `revents`, and
  revalidate every applicable stored pid,
  parent, pgid, start/boot, credential/group/capability/profile, executable,
  argv/environment, and fd fact before `pidfd_send_signal`. Failure, terminal
  `POLLIN`, disappearance, or mismatch is never signal authority and enters
  only the bounded phase-specific absence/reuse proof. No recovered PID is ever
  authenticated from `/proc` before its pidfd is open. Non-parent recovery never
  calls waitid or Wait4; those remain exclusive to the original parent;
- `phase_created` never retires merely because no child identity was durably
  recorded. Recovery proves the supervisor absent and performs ADR 0047's two
  stable complete process scans requiring gate root credentials, actual
  ppid/parent start/boot identity, exact gate device/inode/digest/mode and closed
  nonce-bound argv. A uid-70 argv spoof is not a gate lifetime;
- `gate_durable`, `gate_released`, and `profile_applied` first open the stored
  positive pid by pidfd and then require positive authentication or absence of
  the exact stored gate/client lifetime. Unknown release consumption or exec
  is not retirement evidence;
- for `child_durable` or a later nonterminal phase, first open each stored
  positive pid by pidfd, then authenticate the stored boot identity, original
  supervisor uid/gid `0:0`, and child uid/gid `70:70`, empty
  supplementary groups, start time, absolute executable, closed argv, and
  process group before signaling;
- signal only the authenticated positive leader with `pidfd_send_signal`; use
  the operation's finite absolute deadline and a finite post-`SIGKILL` deadline,
  and wait only by bounded pidfd `ppoll` for `POLLIN`; never use recovery waitid,
  Wait4, or a blocking wait;
- after the exact child lifetime is gone, durably replace the state with
  `phase=recovery_retired` and
  `terminal_status=unavailable_not_parent`, fsync it, then unlink it and fsync the
  parent directory. The stop operation may then report cleanup success; and
- any identity ambiguity or deadline expiry retains the state and returns
  `recovery_required`.

`unavailable_not_parent` and `unavailable_boot_changed` are valid only with
`recovery_retired`; neither is an exit code, signal status, or synonym for
`reaped`. This retirement branch applies
to every closed helper operation when its original supervisor is absent:
`probe-pg-dump`, `probe-pg-restore`, `probe-psql`, `server-major`, `dump`,
`restore-list`, `terminate-db-connections`, `assert-zero-db-connections`,
`restore-apply`, and `post-restore-verify`, including any of their
`child_durable` or later stored states. It does not weaken the exact
parent-owned `Wait4` path while the original supervisor exists.

Boot-id mismatch positively retires only the old process lifetime with
`unavailable_boot_changed`; it never supplies a child wait status or proves a
restore apply. Any apply whose start was acknowledged or gate release was
durable remains `recovery_required` unless the exact successful terminal proof
was already durable.

All normal and recovery state replacement, fsync, rename, unlink, and parent
directory fsync run in the root helper parent. The uid-70 child never opens or
inherits a descriptor for `/run/groundplane-postgres16`. A hostile PostgreSQL
server, extension, or client at the same uid as the child therefore cannot
rename or unlink helper state; file mode `0600` alone would not have prevented
that attack in the cleanly replaced same-UID directory design.

ADR 0047 solely defines the version-1 helper state encoding, launch intent,
gate protocol, fd profile, client security profile and phase-transition
semantics, including pidfd signaling, non-reaping original-parent observation,
and the sole original-parent Wait4. This ADR binds ADR 0047's immutable
managed-image release digest directly into authentication, recovery admission,
and Ready. It defines no second process builder, image inventory, or durable
helper format.
Inventory/adoption/retirement treats any unresolved
helper phase as protected state and never converts gate uncertainty into
discard or retry authority.

### Conflicting late Ack binding and retirement recovery

The executable `TaskAbort` record includes, in addition to its existing task and assignment authority, these fields:

- mandatory `terminal_receipt_sha256`, exactly 32 bytes;
- mandatory `durable_task_mod_revision`, a positive etcd `ModRevision`;
- optional `rejected_task_ack_sha256`, exactly 32 bytes when present; and
- `reason`, at most 256 UTF-8 bytes.

The complete encoded `TaskAbort` envelope MUST remain at most 512 bytes. This is the only `TaskAbort` shape; there is no compatibility record or inference from an omitted mandatory field.

When Controller first publishes an `awaiting_ack` abort, `rejected_task_ack_sha256` is absent. A canonical late `TaskAck` whose outcome and authority exactly match the immutable terminal Task and receipt CASes `awaiting_ack` to `pending`, stores its accepted Ack digest, and enters the receipt handshake. For a changed or outcome-inconsistent canonical late `TaskAck`, Controller fixed-revision-validates the canonical terminal Task, immutable receipt, assignment identity, durable Agent generation, assignment generation, plan hash, terminal outcome, their exact digests, and the same positive `durable_task_mod_revision`. It then CASes the first canonical conflicting Ack digest into `rejected_task_ack_sha256` on that same `awaiting_ack` row without changing its authoritative retirement deadline, terminal Task, receipt, or other authority. Exact replay of that first digest succeeds idempotently. A different second digest returns the typed protocol/state conflict and changes nothing. Controller replays `TaskAbort` with the bound rejected digest and archives that digest through `Retired` and normal pruning.

Agent preserves its existing Ack journal throughout this flow. Its durable `retirement_pending` marker has an optional `rejected_task_ack_sha256`. A replayed `TaskAbort` may bind that field only when its receipt digest, assignment identity and generations, plan hash, terminal outcome and Task digest, and positive durable Task revision exactly match local immutable authority and its rejected digest exactly matches the canonical local Ack digest. Agent writes the binding and marker atomically and fsyncs it; no other abort, digest, TTL, overwrite, or inferred value may bind or replace it.

Closed outcomes classify local Ack evidence before retirement:

- no local Ack enters ordinary `retirement_pending`;
- an exact accepted Ack remains in the `pending` receipt handshake; and
- a conflicting Ack enters conflict-bound `retirement_pending` only after the matching replayed `TaskAbort` is durably bound as above.

Any unclassified Ack blocks `Retired`. An exactly conflict-bound Ack is inert recovery evidence: Agent proceeds through protected recovery while retaining both marker and Ack journal. Agent transforms the marker to `Retired` while retaining the Ack. Only after `RetiredAck` is durably fsynced does Agent remove the Ack journal and then the marker, in that order. A crash at any point replays `Retired`; it never recreates a receipt handshake or reclassifies the Ack. There are no overwrites, TTL-based decisions, or digest inference in this lifecycle.

## Consequences

- One protocol meaning removes negotiation and split-brain execution.
- Bounded resume plus immutable dedupe/subordinate rows preserves audit evidence.
- Lost acknowledgements do not repeat publication or destruction.
- Config preserves desired authorship separately from selected bytes and restores
  without Controller tar spooling.
- Volume resumes at construction, finalization, exchange, every delete, restart,
  and health boundary.
- PostgreSQL cannot speculatively or repeatedly dump in one attempt.
- Object uncertainty retains evidence rather than deleting mismatches.
- Stream, terminal-cache memory, terminal-cache disk, and durable records have
  fail-closed ceilings.
- Receipt application and true Ready prove cleanup without guessing from
  disconnect, age, or missing rows.
- Durable rows require fixed 64 MiB etcd quota headroom.

## Rejected alternatives

- Negotiating multiple plan meanings.
- Hashing resume position into the plan.
- Treating events or terminal summaries as checkpoints.
- Replaying checkpoint history in every assignment.
- Eager startup deletion of staging finals.
- One unbounded Config tar or Controller tar spool.
- Repeating pg_dump after acknowledged dump start.
- Volume deletion without acknowledged per-path intent.
- Agent-side retention selection.
- Object adoption by key alone.
- One unbounded send queue or direct worker `Send`.
- Volatile terminal caches, TTL eviction, and reconnect-time worker-pool reset.
- Reusing the Backup-only receipt key or JSON record for generic Agent Tasks.
- Deriving assignment generation from etcd `ModRevision`.

## Relationship to existing decisions and delivery

This ADR narrows ADR 0010's channel and ADR 0020's plan boundary. It uses ADR
0022's Controller/Console contract boundary and preserves ADR 0029's bootstrap
boundary. Accepted together on 2026-08-30, ADRs 0047 and 0048 supersede ADR
0035's Backup-only terminal-receipt key/record/replay/pruning clauses with the
sole generic contract here, and supersede only ADR 0024's incomplete artifact
framing/canonical formats, upload/Head/point-commit checkpoint detail, Config
active-generation publication, Agent staging/recovery mechanics, and Backup-
only terminal receipt. ADR 0024's source kinds, scheduling, immutable run/retry
identity, raw-Recovery-Point-ID retention, operator actions, ownership and
deadlines remain in force. PostgreSQL unknown-length staging becomes
`ExclusiveUnknown`. This supplies the ADR 0045/0046 Backup protocol. The two
accepted decisions form one paired state-ownership and checkpoint contract
with no compatibility path.

The ordered mirror set for ADR 0047 and this ADR is identical:

1. `docs/mvp.md`
2. `docs/api-cli.md`
3. `docs/blueprint.md`
4. `docs/architecture.md`
5. `docs/capabilities.md`
6. `docs/status.md`
7. `console/src/lib/store.tsx`
8. `console/src/lib/types.ts`
9. `console/src/lib/mock-data.ts`
10. `console/src/components/common/service-form-body.tsx`
11. ADR 0005
12. ADR 0010
13. ADR 0020
14. ADR 0022
15. ADR 0024
16. ADR 0029
17. ADR 0035
18. ADR 0045
19. ADR 0047
20. ADR 0048

Protobuf/generated artifacts, persistence, runtime, tests, managed-image
release evidence, and live S3 proof are separate C16 implementation proof, not
prerequisites for decision acceptance. ADR 0048 MUST NOT be described as
implemented before those proof gates land.

Delivery cleanly replaces protobuf first, regenerates, then updates persistence,
Controller, and Agent together before schema-1 readiness/assignment gates. It
requires a clean pre-release store and clean Agent bootstrap state: there is no
old receipt decoder, dual key, terminal-cache converter, online migration, or
mixed binary mode. No mixed binary pair accepts Backup. The PostgreSQL workload
also passes the exact schema/release-identity gate; tag match is insufficient.
Legacy gates and `postgres:16-alpine` are removed in the same delivery.

Implementation proof includes actual Unix-socket gRPC sessions and focused race
tests for lost TaskAck, ReceiptAck, Applied, AppliedAck, Retired, RetiredAck, recovery Ack,
and recovery AckReceipt; worker-stop/join and TaskAck-versus-retirement races;
retirement-pending crashes before/after Inventory, reversible Discard,
protected ResumePrepared/adoption/recovery-required, safe-terminal proof,
AckReceipt and Retired transform; proof that terminal/timeout/abort creates no
discard authority; duplicate assignment
while running and terminal; EOF, Canceled, and Unavailable reconnect; process
restart; durable Agent generation rotation; Controller-terminalized assigned
Tasks; Task prune before replay; every conflicting identity/digest/revision;
entry 33 and byte ceilings; false/true Ready ordering; 24-row/96-operation plus 8-row/32-operation active-to-clean archive;
24-pair/96-operation ModRevision-only retention prune; 24-row/96-operation and
8-row/32-operation active-to-clean archive batches; Retired one-row archive;
ordinary 95-plus-receipt plans; startup delivery reconciliation; cross-record-
generation adoption; causal true-zero Ready; and exact
Backup/Backup-prune 96-operation terminal plans. Spawned goroutines report by
channels and never call `t.Fatal`. Packaging proof inspects OCI parent/derived
config, platform, inherited fields, helper/state metadata, no-volume rule,
Docker Exec fields/streams, shared protocol identity, and published digest. It
also proves exact Exec `User="0:0"`; helper real/effective/saved/filesystem
uid/gid `0:0`; root-only mode-`0500` gate bytes/digest, freestanding static
identity, exactly one task, closed internal argv and `pgid==pid`; exact fd3
release byte and fd4 READY/PROFILE_APPLIED/FATAL frames; and gated client
real/effective/saved/filesystem uid/gid `70:70`, empty supplementary groups,
zero inheritable/permitted/effective/bounding/ambient capabilities,
`NoNewPrivs: 1`, and release-pinned fork/vfork/clone/clone3-denying seccomp.
It proves replacement rather than inherited child environment; release-pinned
client path without setuid/setgid/file capabilities; fixed argv; READY `/proc`
fd set exactly 0..4 with CLOEXEC on 3/4; exact per-mode fd 0/1/2 policy;
`CLOSE_RANGE_UNSHARE|CLOEXEC`; no gate descriptor surviving exec; root-owned
`0700`/`0600` state metadata; no state descriptor inherited by the child;
uid-70 failures for traverse, read, write, link, unlink, rename, replace, chmod,
chown, gate execution and argv-spoof authentication; root-helper success for
durable create, replace, recovery retirement and removal; and rejection of every
legacy helper-as-postgres or uid-70 state contract/digest.

Golden/corrupt/fuzz proof covers every canonical state/intent primitive, field
order, enum, presence row, bound, digest chain, status-frame sequence, phase
edge and forbidden same-phase mutation. Crash/race proof injects death before
and after every ADR 0047 spawn, gate, release, status-frame, exec-EOF,
alive-after-EOF or already-terminal-before-classification branch,
durable-state, I/O, signal, terminal, sole Wait4 and removal boundary. It proves
the sealed gate's no-normal-exit-without-exec-or-FATAL invariant, fast normal
client-exit classification, and recovery-required handling for signal/core,
missing or inconsistent FATAL, or any gate-invariant failure. It also proves
pidfd-only positive-leader signaling, no group/negative-pid
signal, non-reaping ordinary observations, exactly one successful parent Wait4,
phase-created root/file/ppid/start/boot authentication, boot-change retirement
without apply-success inference, and recovery-required for every acknowledged
apply lacking complete success, including zero bytes.

Container proof covers exact CapDrop/CapAdd/SecurityOpt/Privileged fields, live
postmaster four uid/gid values, empty groups, all capability sets and exact
bounding policy, no-new-privileges, and absence of reachable setuid/setgid/
file-cap escalation. Cutover proof keeps authentication, false Ready, inventory,
dispositions and old stop/recovery available while blocking true Ready,
capacity and ordinary Backup until the old helper drains state. It verifies the
narrow standards exception does not escape `internal/postgres16helper`, the
generic Runner is unchanged, and only the root parent journals or recovers state.
These are delivery requirements, not claims of current implementation.

## Remaining decisions

No protocol or product choice remains in this ADR. The managed PostgreSQL
repository and post-push digest are release outputs recorded by release
authority, not undecided configuration. Paired owner acceptance is recorded on
2026-08-30; schema, runtime, managed-image, test, and live-provider proof remain
separate C16 delivery work.
