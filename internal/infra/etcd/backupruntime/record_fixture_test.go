package backupruntime

import (
	bytes "bytes"
	age "filippo.io/age"
	"fmt"
	"github.com/AlanD20/groundplane/internal/common/backupobject"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"strings"
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
	return BackupRunRecord{
		TaskID: testBackupTaskID, OperationID: testBackupOperationID,
		EnvironmentID: testBackupEnvironmentID, PolicyRevision: 7, PolicySHA256: testBackupDigest, RetentionKeep: 3,
		Initiator: BackupRunInitiatorOperator, ConnectorID: testBackupConnectorID,
		ConnectorPrefix:   "production/",
		ConnectorEndpoint: "https://objects.example.test", ConnectorBucket: "backups", ConnectorRegion: "auto",
		ConnectorRevision: 8, ConnectorHasDirectCredentials: true, ConnectorCredentialsRevision: 9,
		Encryption: BackupRuntimeEncryptionAge, BackupKeyRecordRevision: 10,
		BackupKeyValueRevision: 11, KeyEra: 2, Recipient: recipient,
		State:     BackupRunCompleted,
		Sources:   []BackupRunSourceAttemptRecord{testBackupLaterSource(createdAt, 0, BackupSourceAttemptSucceeded)},
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
	if ordinal == 0 {
		sourceID, attachID = testBackupSourceID, testBackupAttachID
	}
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
			ConsumerServiceID:          testBackupServiceID,
			ManagedReleaseIndex:        testBackupManagedRelease(),
			AttachID:                   attachID,
			AttachRevision:             13,
			BackingProjectID:           testBackupProjectID,
			BackingProjectRevision:     14,
			BackingEnvironmentID:       testBackupBackingEnvironmentID,
			BackingEnvironmentRevision: 15,
			BackingServiceID:           testBackupServiceID,
			BackingServiceRevision:     16,
			AttachFactsRevision:        17,
			Database:                   "app-web_000001",
			Role:                       "app-web_000001",
		}},
		Format:                 BackupRuntimeFormatPostgres,
		RecoveryPointID:        pointID,
		RecoveryPointCreatedAt: createdAt,
		ObjectKey:              "production/" + testBackupEnvironmentID + "/" + sourceID + "/" + pointID + "/artifact.bin",
		State:                  state,
		Phase:                  phase,
	}
	if state == BackupSourceAttemptSucceeded {
		record.Evidence = testBackupArtifact()
		record.Object = testBackupObject(record.ObjectKey)
		record.Upload = BackupUploadOutcome{Kind: BackupUploadReturned,
			Target: record.Object.Target, ReturnedObject: record.Object}
	}
	return record
}

func testBackupVolumePoint(createdAt time.Time, recipient string) BackupRecoveryPointSnapshot {
	pointID := ids.NewAt(ids.KindRecoveryPoint, createdAt, 301)
	point := BackupRecoveryPointSnapshot{BackupRecoveryPointTargetSnapshot: BackupRecoveryPointTargetSnapshot{
		ID: pointID, EnvironmentID: testBackupEnvironmentID,
		SourceID: testBackupSourceID, SourceKind: BackupRuntimeSourceVolume,
		TargetID: testBackupVolumeID, ConnectorID: testBackupConnectorID,
		ConnectorPrefix:   "production/",
		ConnectorEndpoint: "https://objects.example.test", ConnectorBucket: "backups", ConnectorRegion: "auto",
		ObjectKey: "production/" + testBackupEnvironmentID + "/" + testBackupSourceID + "/" +
			pointID + "/artifact.bin",
		SourceFormat: BackupRuntimeFormatVolume, Encryption: BackupRuntimeEncryptionAge,
		KeyEra: 2, Recipient: recipient,
		CreatedAt: createdAt,
	}, Evidence: testBackupArtifact(), VolumeArchive: BackupVolumeArchiveEvidence{
		EntryCount: 1, ContentManifestSHA256: testBackupDigest, FullTreeSHA256: testBackupDigest,
		SourceSizeBytes: 4096,
		Manifest: BackupVolumeManifestReference{
			TaskID: testBackupTaskID, AssignmentID: ids.NewAt(ids.KindAssignment, createdAt, 302),
			StepID: ids.NewAt(ids.KindStep, createdAt, 303), TransferID: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
			AuthoritySHA256: testBackupDigest, AgentID: ids.NewAt(ids.KindAgent, createdAt, 304),
			AgentGeneration: 1, AssignmentGeneration: 1, CursorRevision: 40,
		}},
	}
	point.Object = testBackupObject(point.ObjectKey)
	return point
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
		RestoreGenerationID:           "01ARZ3NDEKTSV4RRFFQ69G5FAV",
		RecoveryPointRevision:         20,
		Point:                         point,
		SourceRevision:                21,
		CurrentTarget:                 BackupRestoreTargetSnapshot{Volume: ptrTestBackupVolumeSourceSnapshot(22)},
		ConnectorRevision:             23,
		ConnectorHasDirectCredentials: true,
		ConnectorCredentialsRevision:  24,
		ExpectedKeyRecordRevision:     25,
		ExpectedKeyValueRevision:      26,
		Artifact:                      &point.Evidence,
		VolumeProgress: &BackupRestoreVolumeProgress{
			OldEntryCount: 1, OldContentManifestSHA256: testBackupDigest, OldFullTreeSHA256: testBackupDigest,
			ConstructionCursor: 1, FinalizationCursor: 1, DeletionCursor: 1,
			ExchangeIntent: true, Exchanged: true, OldRemoved: true,
			ServiceMutationStarted: true, ConsumersStopped: true, ServiceCursor: 1, ServicesRecovered: true,
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
		HeadRevision: revision, HeadSHA256: testBackupDigest,
		ProjectionRoot: revision + 1, DependencyDigest: testBackupDigest, RenderGeneration: 1,
		ComposeVolumeKey: "data", DockerVolumeName: "gp_vol_" + strings.ToLower(testBackupVolumeID),
		AuthorizedVolumeDir: "/var/lib/groundplane/vol/test",
		Services: []BackupVolumeServiceSnapshot{{
			ServiceID: testBackupServiceID, ServiceRevision: revision + 2,
			ComposeKey: "database", MountPaths: []string{"/data"}, PriorIntent: BackupServiceIntentRunning,
		}},
	}
}

func testBackupArtifact() BackupArtifactEvidence {
	// One 4096-byte source encrypted with the fixed X25519 AGE framing.
	return BackupArtifactEvidence{SourceSizeBytes: 4096, SourceSHA256: testBackupDigest,
		StoredSizeBytes: 4296, StoredSHA256: testBackupDigest}
}

func testBackupObject(key string) BackupObjectIdentity {
	return BackupObjectIdentity{
		Target: BackupObjectTarget{ConnectorID: testBackupConnectorID, ConnectorPrefix: "production/",
			ConnectorEndpoint: "https://objects.example.test", ConnectorBucket: "backups", ConnectorRegion: "auto", ObjectKey: key},
		Discriminator: BackupObjectDiscriminator{Kind: backupobject.DiscriminatorVersionID, Value: "version-1"},
	}
}

func testBackupConfigRestore(createdAt time.Time, recipient string) BackupRestoreRecord {
	point := testBackupVolumePoint(createdAt, recipient)
	point.SourceKind, point.TargetID, point.SourceFormat = BackupRuntimeSourceConfig, testBackupEnvironmentID, BackupRuntimeFormatConfig
	point.VolumeArchive = BackupVolumeArchiveEvidence{}
	point.ConfigArchive = BackupConfigArchiveEvidence{
		ManifestSHA256: testBackupDigest, MetadataSnapshotSHA256: testBackupDigest,
		CaptureTranscriptSHA256: testBackupDigest, EntryCount: 2, ManifestSizeBytes: 128, SourceSizeBytes: 4096,
	}
	restore := testBackupRestore(point, createdAt, createdAt.Add(time.Minute))
	restore.RestoreGenerationID = ids.NewAt(ids.KindConfig, createdAt, 27)
	restore.CurrentTarget = BackupRestoreTargetSnapshot{Config: &BackupRestoreConfigTarget{
		EnvironmentID: testBackupEnvironmentID, EnvironmentRevision: 22,
		BaselineRevisionID: testBackupTaskID, BaselineHeadRevision: 22,
		RenderGeneration: 9, FileContextSHA256: testBackupDigest,
	}}
	restore.StagedTreeManifestSHA256 = ""
	restore.State = BackupRestoreReceiving
	restore.MutationStarted = false
	restore.ServiceCount = 0
	restore.VolumeProgress = nil
	restore.ConfigProgress = &BackupRestoreConfigProgress{}
	restore.Verification = BackupVerificationPending
	return restore
}

func testBackupManagedRelease() string {
	return fmt.Sprintf(
		`{"schema":1,"image":"registry.example.test/postgres@sha256:%[1]s","images":[{"repository_digest":"registry.example.test/postgres@sha256:%[1]s","image_id":"sha256:%[1]s","manifest":{"schema":1,"os":"linux","architecture":"amd64","postgresql_major":16,"helper_sha256":"%[1]s","gate_sha256":"%[1]s","pg_dump_sha256":"%[1]s","pg_restore_sha256":"%[1]s","psql_sha256":"%[1]s","launch_profile_sha256":"%[2]s","gate_seccomp_sha256":"%[1]s"}}]}`,
		testBackupDigest,
		postgres16protocol.ManagedLaunchProfileSHA256().String(),
	)
}
