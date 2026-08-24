package etcd

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"filippo.io/age"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	testBackupEnvironmentID        = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testBackupBackingEnvironmentID = "env_01BRZ3NDEKTSV4RRFFQ69G5FAV"
	testBackupTaskID               = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testBackupRetryTaskID          = "task_01BRZ3NDEKTSV4RRFFQ69G5FAV"
	testBackupOperationID          = "op_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testBackupSourceID             = "spt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testBackupPointID              = "rp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testBackupPointIDTwo           = "rp_01BRZ3NDEKTSV4RRFFQ69G5FAV"
	testBackupConnectorID          = "con_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testBackupAttachID             = "att_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testBackupVolumeID             = "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testBackupProjectID            = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testBackupServiceID            = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testBackupEntryID              = "ev_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testBackupDigest               = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
)

// Rationale: restart recovery requires every C16 runtime category to preserve its exact typed state through v1 JSON.
func TestBackupRuntimeRecordCodecsRoundTrip(t *testing.T) {
	createdAt := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	updatedAt := createdAt.Add(time.Minute)
	recipient := newTestBackupRecipient(t)
	point := testBackupVolumePoint(createdAt, recipient)

	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{"schedule cursor", func(t *testing.T) {
			record := BackupScheduleCursorRecord{
				EnvironmentID: testBackupEnvironmentID, PolicyRevision: 7, Frequency: "*-*-* 03:00:00",
				EnabledAt: createdAt, LastEvaluatedAt: updatedAt, NextDueAt: updatedAt.Add(time.Hour),
				UpdatedAt: updatedAt,
			}
			assertBackupCodecRoundTrip(t, record, encodeBackupScheduleCursorRecord, decodeBackupScheduleCursorRecord)
		}},
		{"due outcome", func(t *testing.T) {
			record := BackupDueOutcomeRecord{
				EnvironmentID: testBackupEnvironmentID, PolicyRevision: 7, ScheduledAt: createdAt,
				Outcome: BackupDueDispatched, TaskID: testBackupTaskID, CreatedAt: updatedAt,
				RetainUntil: updatedAt.Add(24 * time.Hour),
			}
			assertBackupCodecRoundTrip(t, record, encodeBackupDueOutcomeRecord, decodeBackupDueOutcomeRecord)
		}},
		{"mutation epoch", func(t *testing.T) {
			record := EnvironmentMutationEpochRecord{EnvironmentID: testBackupEnvironmentID}
			assertBackupCodecRoundTrip(
				t,
				record,
				encodeEnvironmentMutationEpochRecord,
				decodeEnvironmentMutationEpochRecord,
			)
		}},
		{"operation lock", func(t *testing.T) {
			record := BackupOperationLockRecord{
				EnvironmentID: testBackupEnvironmentID, OperationID: testBackupOperationID,
				TaskID: testBackupTaskID, Kind: BackupOperationDeletion, CreatedAt: createdAt, UpdatedAt: updatedAt,
			}
			assertBackupCodecRoundTrip(t, record, encodeBackupOperationLockRecord, decodeBackupOperationLockRecord)
		}},
		{"source-target exclusion", func(t *testing.T) {
			record := BackupSourceTargetExclusionRecord{
				EnvironmentID: testBackupEnvironmentID, OperationID: testBackupOperationID,
				TaskID: testBackupTaskID, OperationKind: BackupOperationBackup,
				TargetKind: BackupSourceTargetBackingService, TargetID: testBackupServiceID,
				CreatedAt: createdAt, UpdatedAt: updatedAt,
			}
			assertBackupCodecRoundTrip(
				t, record, encodeBackupSourceTargetExclusionRecord, decodeBackupSourceTargetExclusionRecord,
			)
		}},
		{"backup run", func(t *testing.T) {
			record := testBackupRun(createdAt, updatedAt, recipient)
			assertBackupCodecRoundTrip(t, record, encodeBackupRunRecord, decodeBackupRunRecord)
		}},
		{"backup run Volume service", func(t *testing.T) {
			record := BackupRunVolumeServiceRecord{
				TaskID: testBackupTaskID, EnvironmentID: testBackupEnvironmentID,
				SourceID: testBackupSourceID, SourceOrdinal: 2, ServiceOrdinal: 3,
				ServiceID: testBackupServiceID, ServiceRevision: 12, PriorIntent: BackupServiceIntentRunning,
			}
			assertBackupCodecRoundTrip(
				t, record, encodeBackupRunVolumeServiceRecord, decodeBackupRunVolumeServiceRecord,
			)
		}},
		{"recovery point", func(t *testing.T) {
			record := BackupRecoveryPointRecord{BackupRecoveryPointSnapshot: point, VerifiedAt: updatedAt}
			assertBackupCodecRoundTrip(t, record, encodeBackupRecoveryPointRecord, decodeBackupRecoveryPointRecord)
		}},
		{"orphan", func(t *testing.T) {
			record := BackupOrphanRecord{
				Point: point, TaskID: testBackupTaskID, State: BackupOrphanInspect,
				CreatedAt: createdAt, UpdatedAt: updatedAt,
			}
			assertBackupCodecRoundTrip(t, record, encodeBackupOrphanRecord, decodeBackupOrphanRecord)
		}},
		{"retention", func(t *testing.T) {
			record := BackupRetentionSweepRecord{
				SourceID: testBackupSourceID, TriggerRecoveryPointID: testBackupPointID, Keep: 7, Revision: 42,
				Cursor: testBackupPointIDTwo, State: BackupRetentionScanning,
				CreatedAt: createdAt, UpdatedAt: updatedAt,
			}
			assertBackupCodecRoundTrip(t, record, encodeBackupRetentionSweepRecord, decodeBackupRetentionSweepRecord)
		}},
		{"prune", func(t *testing.T) {
			record := BackupRecoveryPointPruneRecord{
				Point: point, State: BackupPruneAssigned, TaskID: testBackupTaskID,
				CreatedAt: createdAt, UpdatedAt: updatedAt,
			}
			assertBackupCodecRoundTrip(
				t, record, encodeBackupRecoveryPointPruneRecord, decodeBackupRecoveryPointPruneRecord,
			)
		}},
		{"prune dispatch", func(t *testing.T) {
			record := BackupRecoveryPointPruneDispatchRecord{
				TaskID: testBackupTaskID, ConnectorID: testBackupConnectorID,
				RecoveryPointIDs: []string{testBackupPointID, testBackupPointIDTwo}, CreatedAt: createdAt,
			}
			assertBackupCodecRoundTrip(
				t, record, encodeBackupRecoveryPointPruneDispatchRecord,
				decodeBackupRecoveryPointPruneDispatchRecord,
			)
		}},
		{"restore", func(t *testing.T) {
			record := testBackupRestore(point, createdAt, updatedAt)
			assertBackupCodecRoundTrip(t, record, encodeBackupRestoreRecord, decodeBackupRestoreRecord)
		}},
		{"restore service", func(t *testing.T) {
			record := BackupRestoreServiceRecord{
				TaskID: testBackupTaskID, Ordinal: 1, ServiceID: testBackupServiceID,
				ServiceRevision: 12, PriorIntent: BackupServiceIntentRunning,
			}
			assertBackupCodecRoundTrip(
				t, record, encodeBackupRestoreServiceRecord, decodeBackupRestoreServiceRecord,
			)
		}},
		{"key rotation", func(t *testing.T) {
			record := BackupKeyRotationRecord{
				TaskID: testBackupTaskID, OperationID: testBackupOperationID,
				EnvironmentID: testBackupEnvironmentID, ExpectedCurrentRecordRevision: 10,
				ExpectedCurrentValueRevision: 11, CurrentKeyEra: 2, NextKeyEra: 3,
				NextRecipient: recipient, NextEncryptedIdentity: []byte{1, 2, 3},
				State: BackupKeyRotationPrepared, CreatedAt: createdAt, UpdatedAt: updatedAt,
			}
			assertBackupCodecRoundTrip(t, record, encodeBackupKeyRotationRecord, decodeBackupKeyRotationRecord)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, test.run)
	}
}

// Rationale: closed state and union validation must reject records that cannot be resumed without inference.
func TestBackupRuntimeRecordsRejectAmbiguousState(t *testing.T) {
	createdAt := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	updatedAt := createdAt.Add(time.Minute)
	recipient := newTestBackupRecipient(t)

	due := BackupDueOutcomeRecord{
		EnvironmentID: testBackupEnvironmentID, PolicyRevision: 1, ScheduledAt: createdAt,
		Outcome: BackupDueSkippedOverlap, TaskID: testBackupTaskID, CreatedAt: updatedAt,
		RetainUntil: updatedAt.Add(time.Hour),
	}
	if _, err := encodeBackupDueOutcomeRecord(due); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("encodeBackupDueOutcomeRecord() error = %v, want validation failure", err)
	}

	run := testBackupRun(createdAt, updatedAt, recipient)
	run.Sources[0].Snapshot.Volume = &BackupVolumeSourceSnapshot{
		EnvironmentID: testBackupEnvironmentID, VolumeID: testBackupVolumeID, VolumeRevision: 1,
	}
	if _, err := encodeBackupRunRecord(run); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("encodeBackupRunRecord() error = %v, want validation failure", err)
	}

	restore := testBackupRestore(testBackupVolumePoint(createdAt, recipient), createdAt, updatedAt)
	restore.UsesOldIdentity = true
	if _, err := encodeBackupRestoreRecord(restore); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("encodeBackupRestoreRecord() error = %v, want validation failure", err)
	}
}

// Rationale: Environment config artifacts are scoped to their owning Environment
// and must never be restorable into a target captured from another scope.
func TestBackupRuntimeRecordsRejectConfigTargetOutsideEnvironment(t *testing.T) {
	createdAt := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	updatedAt := createdAt.Add(time.Minute)
	recipient := newTestBackupRecipient(t)

	run := testBackupRun(createdAt, updatedAt, recipient)
	run.Sources[0].Kind = BackupRuntimeSourceConfig
	run.Sources[0].TargetID = testBackupBackingEnvironmentID
	run.Sources[0].Snapshot = BackupRunSourceSnapshot{Config: &BackupConfigSourceSnapshot{
		ConfigSnapshotTaskID: testBackupTaskID,
	}}
	run.Sources[0].Format = BackupRuntimeFormatConfig
	if _, err := encodeBackupRunRecord(run); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("encodeBackupRunRecord() error = %v, want validation failure", err)
	}

	point := testBackupVolumePoint(createdAt, recipient)
	point.SourceKind = BackupRuntimeSourceConfig
	point.TargetID = testBackupBackingEnvironmentID
	point.SourceFormat = BackupRuntimeFormatConfig
	if err := validateBackupRecoveryPointSnapshot(point); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("validateBackupRecoveryPointSnapshot() error = %v, want validation failure", err)
	}
}

// Rationale: durable data is untrusted input and unknown fields must fail internally rather than silently default.
func TestBackupRuntimeDecoderRejectsUnknownFields(t *testing.T) {
	value := []byte(`{"schema":1,"kind":"backup-operation-lock","data":{` +
		`"environment_id":"env_01ARZ3NDEKTSV4RRFFQ69G5FAV",` +
		`"operation_id":"op_01ARZ3NDEKTSV4RRFFQ69G5FAV",` +
		`"task_id":"task_01ARZ3NDEKTSV4RRFFQ69G5FAV",` +
		`"kind":"backup","created_at":"2026-08-24T10:00:00Z",` +
		`"updated_at":"2026-08-24T10:00:00Z","unexpected":true}}`)
	if _, err := decodeBackupOperationLockRecord(value); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("decodeBackupOperationLockRecord() error = %v, want internal", err)
	}
}

// Rationale: point visibility requires verified private evidence and encryption metadata to remain self-consistent.
func TestBackupRecoveryPointRejectsEncryptionMismatch(t *testing.T) {
	createdAt := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	point := testBackupVolumePoint(createdAt, newTestBackupRecipient(t))
	point.Encryption = BackupRuntimeEncryptionNone
	if err := validateBackupRecoveryPointSnapshot(point); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("validateBackupRecoveryPointSnapshot() error = %v, want validation failure", err)
	}
}

// Rationale: pruning transactions are bounded to twelve unique points so dispatch records cannot exceed the MVP plan.
func TestBackupPruneDispatchRejectsDuplicateAndOversizedBatches(t *testing.T) {
	createdAt := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	record := BackupRecoveryPointPruneDispatchRecord{
		TaskID: testBackupTaskID, ConnectorID: testBackupConnectorID,
		RecoveryPointIDs: []string{testBackupPointID, testBackupPointID}, CreatedAt: createdAt,
	}
	if err := validateBackupRecoveryPointPruneDispatchRecord(record); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("duplicate dispatch error = %v, want validation failure", err)
	}
	record.RecoveryPointIDs = make([]string, maximumBackupPruneDispatchPoints+1)
	for index := range record.RecoveryPointIDs {
		record.RecoveryPointIDs[index] = ids.NewAt(ids.KindRecoveryPoint, createdAt, int64(index+1))
	}
	if err := validateBackupRecoveryPointPruneDispatchRecord(record); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("oversized dispatch error = %v, want validation failure", err)
	}
}

// Rationale: one visible run is fail-fast, so a terminal failure must preserve
// one exact failed checkpoint and mark every later source unstarted.
func TestBackupRunValidationEnforcesFailFastCheckpointTable(t *testing.T) {
	createdAt := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	updatedAt := createdAt.Add(time.Minute)
	run := testBackupRun(createdAt, updatedAt, newTestBackupRecipient(t))
	run.State = BackupRunFailed
	run.Sources[0].State = BackupSourceAttemptFailed
	run.Sources[0].SizeBytes = 0
	run.Sources[0].SHA256 = ""
	run.Sources[0].FailureCode = BackupFailureCapture
	run.Sources = append(run.Sources, testBackupLaterSource(createdAt, 1, BackupSourceAttemptUnstarted))
	if _, err := encodeBackupRunRecord(run); err != nil {
		t.Fatalf("encodeBackupRunRecord(valid failed checkpoint) error = %v", err)
	}

	run.Sources[1] = testBackupLaterSource(createdAt, 1, BackupSourceAttemptSucceeded)
	if _, err := encodeBackupRunRecord(run); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("later successful source error = %v, want validation", err)
	}

	run.Sources[1] = testBackupLaterSource(createdAt, 1, BackupSourceAttemptUnstarted)
	run.Sources[0].FailureCode = BackupFailureCode("provider said access key rejected")
	if _, err := encodeBackupRunRecord(run); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("free-form failure error = %v, want validation", err)
	}

	run.Sources[0].State = BackupSourceAttemptPointCommitted
	run.Sources[0].SizeBytes = 123
	run.Sources[0].SHA256 = testBackupDigest
	run.Sources[0].FailureCode = BackupFailureRetention
	if _, err := encodeBackupRunRecord(run); err != nil {
		t.Fatalf("encodeBackupRunRecord(retention checkpoint) error = %v", err)
	}
}

// Rationale: source snapshots and exclusion records must identify the exact
// consumer Environment and every backing resource they protect.
func TestBackupRuntimeRecordsRejectCrossEnvironmentSourceOwnership(t *testing.T) {
	createdAt := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	run := testBackupRun(createdAt, createdAt.Add(time.Minute), newTestBackupRecipient(t))
	run.Sources[0].Snapshot.Postgres.ConsumerEnvironmentID = testBackupBackingEnvironmentID
	if _, err := encodeBackupRunRecord(run); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("cross-Environment Attach snapshot error = %v, want validation", err)
	}

	exclusion := BackupSourceTargetExclusionRecord{
		EnvironmentID: testBackupEnvironmentID, OperationID: testBackupOperationID,
		TaskID: testBackupTaskID, OperationKind: BackupOperationRotation,
		TargetKind: BackupSourceTargetBackingProject, TargetID: testBackupProjectID,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	if _, err := encodeBackupSourceTargetExclusionRecord(exclusion); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("rotation source-target exclusion error = %v, want validation", err)
	}
}

// Rationale: Volume overwrite is not durable until the exact staged tree
// digest exists, and mutation starts only at the atomic exchange checkpoint.
func TestBackupVolumeRestoreRequiresManifestAndExactMutationBoundary(t *testing.T) {
	createdAt := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	restore := testBackupRestore(
		testBackupVolumePoint(createdAt, newTestBackupRecipient(t)),
		createdAt,
		createdAt.Add(time.Minute),
	)
	restore.StagedTreeManifestSHA256 = ""
	if _, err := encodeBackupRestoreRecord(restore); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("completed restore without manifest error = %v, want validation", err)
	}

	restore.StagedTreeManifestSHA256 = testBackupDigest
	restore.State = BackupRestoreExchangeReady
	restore.Verification = BackupVerificationPending
	if _, err := encodeBackupRestoreRecord(restore); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("pre-exchange mutation error = %v, want validation", err)
	}
	restore.MutationStarted = false
	if _, err := encodeBackupRestoreRecord(restore); err != nil {
		t.Fatalf("encodeBackupRestoreRecord(exchange ready) error = %v", err)
	}
}

// Rationale: config restore cursors must pin the next render generation and
// cannot authorize the canonical switch before every staged Entry is upserted.
func TestBackupConfigRestoreProgressEnforcesPhaseAndMaterialization(t *testing.T) {
	progress := BackupRestoreConfigProgress{
		CurrentEntryOrdinal: 2, FinalizedEntryCount: 2,
		DescriptorChunkCount: 2, ValueChunkCount: 2, PlainValueBytes: 10,
		DescriptorChainSHA256: testBackupDigest, StoredValueChainSHA256: testBackupDigest,
		StoredManifestSHA256: testBackupDigest, UpsertEntryOrdinal: 2,
		MaterializationGeneration: 9,
	}
	if err := validateBackupRestoreConfigProgress(BackupRestoreCompleted, progress); err != nil {
		t.Fatalf("validateBackupRestoreConfigProgress(valid) error = %v", err)
	}
	progress.UpsertEntryOrdinal = 1
	if err := validateBackupRestoreConfigProgress(BackupRestoreCompleted, progress); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("incomplete upsert error = %v, want validation", err)
	}
	progress.UpsertEntryOrdinal = 2
	progress.MaterializationGeneration = 0
	if err := validateBackupRestoreConfigProgress(BackupRestoreCompleted, progress); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("missing materialization generation error = %v, want validation", err)
	}
}

// Rationale: durable receiving cursors cannot resume beyond chunks already incorporated into aggregate evidence.
func TestBackupConfigRestoreDecoderRejectsChunkCursorBeyondAggregateCount(t *testing.T) {
	createdAt := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	point := testBackupVolumePoint(createdAt, newTestBackupRecipient(t))
	point.SourceKind = BackupRuntimeSourceConfig
	point.TargetID = testBackupEnvironmentID
	point.SourceFormat = BackupRuntimeFormatConfig
	restore := testBackupRestore(point, createdAt, createdAt.Add(time.Minute))
	restore.CurrentTarget = BackupRestoreTargetSnapshot{Config: &BackupRestoreConfigTarget{
		EnvironmentID: testBackupEnvironmentID, EnvironmentRevision: 22,
	}}
	restore.StagedTreeManifestSHA256 = ""
	restore.State = BackupRestoreReceiving
	restore.MutationStarted = false
	restore.ServiceCount = 0
	restore.ConfigProgress = &BackupRestoreConfigProgress{MaterializationGeneration: 9}
	restore.Verification = BackupVerificationPending
	value, err := encodeBackupRestoreRecord(restore)
	if err != nil {
		t.Fatalf("encodeBackupRestoreRecord() error = %v", err)
	}

	tests := []struct {
		name      string
		cursor    []byte
		maxUint32 []byte
	}{
		{
			name:      "descriptor MaxUint32",
			cursor:    []byte(`"next_descriptor_chunk_ordinal":0`),
			maxUint32: []byte(`"next_descriptor_chunk_ordinal":4294967295`),
		},
		{
			name:      "value MaxUint32",
			cursor:    []byte(`"next_value_chunk_ordinal":0`),
			maxUint32: []byte(`"next_value_chunk_ordinal":4294967295`),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			malformed := bytes.Replace(value, test.cursor, test.maxUint32, 1)
			if bytes.Equal(malformed, value) {
				t.Fatalf("encoded restore does not contain %q", test.cursor)
			}
			if _, err := decodeBackupRestoreRecord(malformed); !errors.Is(
				err,
				errs.New(errs.KindInternal, ""),
			) {
				t.Fatalf("decodeBackupRestoreRecord() error = %v, want internal", err)
			}
		})
	}
}

// Rationale: once rotation commits the new current key, its transient wrapped
// identity must be removed from the durable operation checkpoint.
func TestBackupKeyRotationAppliedRecordRejectsRetainedIdentity(t *testing.T) {
	createdAt := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	record := BackupKeyRotationRecord{
		TaskID: testBackupTaskID, OperationID: testBackupOperationID,
		EnvironmentID: testBackupEnvironmentID, ExpectedCurrentRecordRevision: 10,
		ExpectedCurrentValueRevision: 11, CurrentKeyEra: 2, NextKeyEra: 3,
		NextRecipient: newTestBackupRecipient(t), State: BackupKeyRotationApplied,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	if _, err := encodeBackupKeyRotationRecord(record); err != nil {
		t.Fatalf("encodeBackupKeyRotationRecord(applied) error = %v", err)
	}
	record.NextEncryptedIdentity = []byte("wrapped")
	if _, err := encodeBackupKeyRotationRecord(record); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("applied rotation retained identity error = %v, want validation", err)
	}
}

func assertBackupCodecRoundTrip[T any](
	t *testing.T,
	record T,
	encode func(T) ([]byte, error),
	decode func([]byte) (T, error),
) {
	t.Helper()
	value, err := encode(record)
	if err != nil {
		t.Fatalf("encode() error = %v", err)
	}
	decoded, err := decode(value)
	if err != nil {
		t.Fatalf("decode() error = %v", err)
	}
	reencoded, err := encode(decoded)
	if err != nil {
		t.Fatalf("re-encode() error = %v", err)
	}
	if !bytes.Equal(reencoded, value) {
		t.Fatalf("re-encoded bytes = %q, want %q", reencoded, value)
	}
}

func testBackupRun(createdAt time.Time, updatedAt time.Time, recipient string) BackupRunRecord {
	return BackupRunRecord{
		TaskID: testBackupTaskID, OperationID: testBackupOperationID,
		EnvironmentID: testBackupEnvironmentID, PolicyRevision: 7,
		Initiator: BackupRunInitiatorOperator, ConnectorID: testBackupConnectorID,
		ConnectorRevision: 8, ConnectorCredentialsRevision: 9,
		Encryption: BackupRuntimeEncryptionAge, BackupKeyRecordRevision: 10,
		BackupKeyValueRevision: 11, KeyEra: 2, Recipient: recipient,
		State: BackupRunCompleted,
		Sources: []BackupRunSourceAttemptRecord{{
			Ordinal: 0, SourceID: testBackupSourceID, Kind: BackupRuntimeSourceAttach,
			TargetID: testBackupAttachID, SourceRevision: 12, TargetRevision: 13,
			Snapshot: BackupRunSourceSnapshot{Postgres: &BackupPostgresSourceSnapshot{
				ConsumerEnvironmentID: testBackupEnvironmentID,
				AttachID:              testBackupAttachID, AttachRevision: 13,
				BackingProjectID: testBackupProjectID, BackingProjectRevision: 14,
				BackingEnvironmentID: testBackupBackingEnvironmentID, BackingEnvironmentRevision: 15,
				BackingServiceID: testBackupServiceID, BackingServiceRevision: 16, AttachFactsRevision: 17,
			}},
			Format: BackupRuntimeFormatPostgres, RecoveryPointID: testBackupPointID,
			ObjectKey: testBackupEnvironmentID + "/" + testBackupSourceID + "/" +
				testBackupPointID + "/artifact.bin",
			State: BackupSourceAttemptSucceeded, SizeBytes: 123, SHA256: testBackupDigest,
		}},
		CreatedAt: createdAt, UpdatedAt: updatedAt,
	}
}

func testBackupLaterSource(
	createdAt time.Time,
	ordinal uint32,
	state BackupSourceAttemptState,
) BackupRunSourceAttemptRecord {
	sourceID := ids.NewAt(ids.KindBackupSource, createdAt, int64(100+ordinal))
	attachID := ids.NewAt(ids.KindAttach, createdAt, int64(200+ordinal))
	pointID := ids.NewAt(ids.KindRecoveryPoint, createdAt, int64(300+ordinal))
	record := BackupRunSourceAttemptRecord{
		Ordinal: ordinal, SourceID: sourceID, Kind: BackupRuntimeSourceAttach,
		TargetID: attachID, SourceRevision: 12, TargetRevision: 13,
		Snapshot: BackupRunSourceSnapshot{Postgres: &BackupPostgresSourceSnapshot{
			ConsumerEnvironmentID: testBackupEnvironmentID, AttachID: attachID, AttachRevision: 13,
			BackingProjectID: testBackupProjectID, BackingProjectRevision: 14,
			BackingEnvironmentID: testBackupBackingEnvironmentID, BackingEnvironmentRevision: 15,
			BackingServiceID: testBackupServiceID, BackingServiceRevision: 16, AttachFactsRevision: 17,
		}},
		Format: BackupRuntimeFormatPostgres, RecoveryPointID: pointID,
		ObjectKey: testBackupEnvironmentID + "/" + sourceID + "/" + pointID + "/artifact.bin",
		State:     state,
	}
	if state == BackupSourceAttemptSucceeded {
		record.SizeBytes = 123
		record.SHA256 = testBackupDigest
	}
	return record
}

func testBackupVolumePoint(createdAt time.Time, recipient string) BackupRecoveryPointSnapshot {
	return BackupRecoveryPointSnapshot{
		ID: testBackupPointID, EnvironmentID: testBackupEnvironmentID,
		SourceID: testBackupSourceID, SourceKind: BackupRuntimeSourceVolume,
		TargetID: testBackupVolumeID, ConnectorID: testBackupConnectorID,
		ObjectKey: testBackupEnvironmentID + "/" + testBackupSourceID + "/" +
			testBackupPointID + "/artifact.bin",
		SourceFormat: BackupRuntimeFormatVolume, Encryption: BackupRuntimeEncryptionAge,
		KeyEra: 2, Recipient: recipient, SizeBytes: 123, SHA256: testBackupDigest,
		CreatedAt: createdAt,
	}
}

func newTestBackupRecipient(t *testing.T) string {
	t.Helper()
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("age.GenerateX25519Identity() error = %v", err)
	}
	return identity.Recipient().String()
}

func testBackupRestore(
	point BackupRecoveryPointSnapshot,
	createdAt time.Time,
	updatedAt time.Time,
) BackupRestoreRecord {
	return BackupRestoreRecord{
		TaskID: testBackupTaskID, OperationID: testBackupOperationID,
		EnvironmentID: testBackupEnvironmentID, RecoveryPointRevision: 20,
		Point: point, SourceRevision: 21,
		CurrentTarget: BackupRestoreTargetSnapshot{Volume: &BackupVolumeSourceSnapshot{
			EnvironmentID: testBackupEnvironmentID, VolumeID: testBackupVolumeID, VolumeRevision: 22,
		}},
		ConnectorRevision: 23, ConnectorCredentialsRevision: 24,
		ExpectedKeyRecordRevision: 25, ExpectedKeyValueRevision: 26,
		Artifact:                 &BackupArtifactEvidence{SizeBytes: point.SizeBytes, SHA256: point.SHA256},
		StagedTreeManifestSHA256: testBackupDigest,
		State:                    BackupRestoreCompleted, MutationStarted: true, ServiceCount: 1,
		Verification: BackupVerificationPassed, CreatedAt: createdAt, UpdatedAt: updatedAt,
	}
}
