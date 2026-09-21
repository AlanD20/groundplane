package backupruntime

import (
	bytes "bytes"
	age "filippo.io/age"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	testing "testing"
	time "time"
)

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
	pointID := ids.NewAt(ids.KindRecoveryPoint, createdAt, 300)
	return BackupRunRecord{
		TaskID: testBackupTaskID, OperationID: testBackupOperationID,
		EnvironmentID: testBackupEnvironmentID, PolicyRevision: 7, RetentionKeep: 3,
		Initiator: BackupRunInitiatorOperator, ConnectorID: testBackupConnectorID,
		ConnectorPrefix:   "production/",
		ConnectorRevision: 8, ConnectorHasDirectCredentials: true, ConnectorCredentialsRevision: 9,
		Encryption: BackupRuntimeEncryptionAge, BackupKeyRecordRevision: 10,
		BackupKeyValueRevision: 11, KeyEra: 2, Recipient: recipient,
		State: BackupRunCompleted,
		Sources: []BackupRunSourceAttemptRecord{{
			Ordinal: 0, SourceID: testBackupSourceID, Kind: BackupRuntimeSourceAttach,
			TargetID: testBackupAttachID, SourceRevision: 12, TargetRevision: 13,
			Snapshot: BackupRunSourceSnapshot{Postgres: &BackupPostgresSourceSnapshot{
				ConsumerEnvironmentID:      testBackupEnvironmentID,
				AttachID:                   testBackupAttachID,
				AttachRevision:             13,
				BackingProjectID:           testBackupProjectID,
				BackingProjectRevision:     14,
				BackingEnvironmentID:       testBackupBackingEnvironmentID,
				BackingEnvironmentRevision: 15,
				BackingServiceID:           testBackupServiceID,
				BackingServiceRevision:     16,
				AttachFactsRevision:        17,
				Database:                   "app_db",
				Role:                       "app_role",
			}},
			Format: BackupRuntimeFormatPostgres, RecoveryPointID: pointID,
			RecoveryPointCreatedAt: createdAt,
			ObjectKey: "production/" + testBackupEnvironmentID + "/" + testBackupSourceID + "/" +
				pointID + "/artifact.bin",
			State: BackupSourceAttemptSucceeded, Phase: BackupSourcePhaseCleanup,
			SizeBytes: 123, SHA256: testBackupDigest,
		}},
		CreatedAt: createdAt, UpdatedAt: updatedAt,
	}
}

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

func testBackupLaterSource(
	createdAt time.Time,
	ordinal uint32,
	state BackupSourceAttemptState,
) BackupRunSourceAttemptRecord {
	phase := BackupSourcePhaseCapture
	if state == BackupSourceAttemptSucceeded {
		phase = BackupSourcePhaseCleanup
	}
	sourceID := ids.NewAt(ids.KindBackupSource, createdAt, int64(100+ordinal))
	attachID := ids.NewAt(ids.KindAttach, createdAt, int64(200+ordinal))
	pointID := ids.NewAt(ids.KindRecoveryPoint, createdAt, int64(300+ordinal))
	record := BackupRunSourceAttemptRecord{
		Ordinal:        ordinal,
		SourceID:       sourceID,
		Kind:           BackupRuntimeSourceAttach,
		TargetID:       attachID,
		SourceRevision: 12,
		TargetRevision: 13,
		Snapshot: BackupRunSourceSnapshot{Postgres: &BackupPostgresSourceSnapshot{
			ConsumerEnvironmentID:      testBackupEnvironmentID,
			AttachID:                   attachID,
			AttachRevision:             13,
			BackingProjectID:           testBackupProjectID,
			BackingProjectRevision:     14,
			BackingEnvironmentID:       testBackupBackingEnvironmentID,
			BackingEnvironmentRevision: 15,
			BackingServiceID:           testBackupServiceID,
			BackingServiceRevision:     16,
			AttachFactsRevision:        17,
			Database:                   "app_db",
			Role:                       "app_role",
		}},
		Format:                 BackupRuntimeFormatPostgres,
		RecoveryPointID:        pointID,
		RecoveryPointCreatedAt: createdAt,
		ObjectKey:              "production/" + testBackupEnvironmentID + "/" + sourceID + "/" + pointID + "/artifact.bin",
		State:                  state,
		Phase:                  phase,
	}
	if state == BackupSourceAttemptSucceeded {
		record.SizeBytes = 123
		record.SHA256 = testBackupDigest
	}
	return record
}

func testBackupVolumePoint(createdAt time.Time, recipient string) BackupRecoveryPointSnapshot {
	pointID := ids.NewAt(ids.KindRecoveryPoint, createdAt, 301)
	return BackupRecoveryPointSnapshot{
		ID: pointID, EnvironmentID: testBackupEnvironmentID,
		SourceID: testBackupSourceID, SourceKind: BackupRuntimeSourceVolume,
		TargetID: testBackupVolumeID, ConnectorID: testBackupConnectorID,
		ConnectorPrefix: "production/",
		ObjectKey: "production/" + testBackupEnvironmentID + "/" + testBackupSourceID + "/" +
			pointID + "/artifact.bin",
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
		TaskID:                        testBackupTaskID,
		OperationID:                   testBackupOperationID,
		EnvironmentID:                 testBackupEnvironmentID,
		RecoveryPointRevision:         20,
		Point:                         point,
		SourceRevision:                21,
		CurrentTarget:                 BackupRestoreTargetSnapshot{Volume: ptrTestBackupVolumeSourceSnapshot(22)},
		ConnectorRevision:             23,
		ConnectorHasDirectCredentials: true,
		ConnectorCredentialsRevision:  24,
		ExpectedKeyRecordRevision:     25,
		ExpectedKeyValueRevision:      26,
		Artifact: &BackupArtifactEvidence{
			SizeBytes: point.SizeBytes,
			SHA256:    point.SHA256,
		},
		StagedTreeManifestSHA256: testBackupDigest,
		State:                    BackupRestoreCompleted,
		MutationStarted:          true,
		ServiceCount:             1,
		Verification:             BackupVerificationPassed,
		CreatedAt:                createdAt,
		UpdatedAt:                updatedAt,
	}
}

func ptrTestBackupVolumeSourceSnapshot(revision int64) *BackupVolumeSourceSnapshot {
	snapshot := testBackupVolumeSourceSnapshot(revision)
	return &snapshot
}
func testBackupVolumeSourceSnapshot(revision int64) BackupVolumeSourceSnapshot {
	return BackupVolumeSourceSnapshot{
		EnvironmentID: testBackupEnvironmentID, EnvironmentRevision: revision,
		VolumeID: testBackupVolumeID, DesiredRevisionID: testBackupTaskID,
		ProjectionRoot: revision + 1, DependencyDigest: testBackupDigest, RenderGeneration: 1,
		ComposeVolumeKey: "data", DockerVolumeName: "gp_vol_" + testBackupVolumeID,
		AuthorizedVolumeDir: "/var/lib/groundplane/vol/test",
		Services: []BackupVolumeServiceSnapshot{{
			ServiceID: testBackupServiceID, ServiceRevision: revision + 2,
			ComposeKey: "database", MountPaths: []string{"/data"}, PriorIntent: BackupServiceIntentRunning,
		}},
	}
}
