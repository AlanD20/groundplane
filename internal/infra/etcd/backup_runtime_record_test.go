package etcd

import (
	fmt "fmt"
	testing "testing"
	time "time"

	age "filippo.io/age"
	backupobject "github.com/AlanD20/groundplane/internal/common/backupobject"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	postgres16protocol "github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
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

func testBackupRun(createdAt time.Time, updatedAt time.Time, recipient string) testbackupruntime.BackupRunRecord {
	pointID := ids.NewAt(ids.KindRecoveryPoint, createdAt, 300)
	run := testbackupruntime.BackupRunRecord{
		TaskID: testBackupTaskID, OperationID: testBackupOperationID,
		EnvironmentID: testBackupEnvironmentID, PolicyRevision: 7, PolicySHA256: testBackupDigest, RetentionKeep: 3,
		Initiator: testbackupruntime.BackupRunInitiatorOperator, ConnectorID: testBackupConnectorID,
		ConnectorPrefix:   "production/",
		ConnectorEndpoint: "https://objects.example.test", ConnectorBucket: "backups", ConnectorRegion: "auto",
		ConnectorRevision: 8, ConnectorHasDirectCredentials: true, ConnectorCredentialsRevision: 9,
		Encryption: testbackupruntime.BackupRuntimeEncryptionAge, BackupKeyRecordRevision: 10,
		BackupKeyValueRevision: 11, KeyEra: 2, Recipient: recipient,
		State: testbackupruntime.BackupRunCompleted,
		Sources: []testbackupruntime.BackupRunSourceAttemptRecord{{
			Ordinal: 0, SourceID: testBackupSourceID, Kind: testbackupruntime.BackupRuntimeSourceAttach,
			TargetID: testBackupAttachID, SourceRevision: 12, TargetRevision: 13,
			Snapshot: testbackupruntime.BackupRunSourceSnapshot{
				Postgres: &testbackupruntime.BackupPostgresSourceSnapshot{
					ConsumerEnvironmentID:      testBackupEnvironmentID,
					AttachID:                   testBackupAttachID,
					AttachRevision:             13,
					BackingProjectID:           testBackupProjectID,
					BackingProjectRevision:     14,
					BackingEnvironmentID:       testBackupBackingEnvironmentID,
					BackingEnvironmentRevision: 15,
					BackingServiceID:           testBackupServiceID,
					BackingServiceRevision:     16,
					ConsumerServiceID:          testBackupServiceID,
					AttachFactsRevision:        17,
					Database:                   "app_000001",
					Role:                       "app_000001",
					ManagedReleaseIndex:        testBackupManagedRelease(),
				},
			},
			Format: testbackupruntime.BackupRuntimeFormatPostgres, RecoveryPointID: pointID,
			RecoveryPointCreatedAt: createdAt,
			ObjectKey: "production/" + testBackupEnvironmentID + "/" + testBackupSourceID + "/" +
				pointID + "/artifact.bin",
			State: testbackupruntime.BackupSourceAttemptSucceeded, Phase: testbackupruntime.BackupSourcePhaseCleanup,
			Evidence: testBackupArtifact(),
		}},
		CreatedAt: createdAt, UpdatedAt: updatedAt,
	}
	run.Sources[0].Object = testBackupObject(run.Sources[0].ObjectKey)
	run.Sources[0].PostgresArchive = testbackupruntime.BackupPostgresArchiveEvidence{
		PGDumpMajor: 16, AdapterContractVersion: 1, SourceServerVersion: "16.9", BackupToolVersion: "16.9",
	}
	run.Sources[0].Upload = testbackupruntime.BackupUploadOutcome{
		Kind:   testbackupruntime.BackupUploadReturned,
		Target: run.Sources[0].Object.Target, ReturnedObject: run.Sources[0].Object,
	}
	return run
}

func testBackupLaterSource(
	createdAt time.Time,
	ordinal uint32,
	state testbackupruntime.BackupSourceAttemptState,
) testbackupruntime.BackupRunSourceAttemptRecord {
	phase := testbackupruntime.BackupSourcePhaseCapture
	if state == testbackupruntime.BackupSourceAttemptSucceeded {
		phase = testbackupruntime.BackupSourcePhaseCleanup
	}
	sourceID := ids.NewAt(ids.KindBackupSource, createdAt, int64(100+ordinal))
	attachID := ids.NewAt(ids.KindAttach, createdAt, int64(200+ordinal))
	pointID := ids.NewAt(ids.KindRecoveryPoint, createdAt, int64(300+ordinal))
	record := testbackupruntime.BackupRunSourceAttemptRecord{
		Ordinal:        ordinal,
		SourceID:       sourceID,
		Kind:           testbackupruntime.BackupRuntimeSourceAttach,
		TargetID:       attachID,
		SourceRevision: 12,
		TargetRevision: 13,
		Snapshot: testbackupruntime.BackupRunSourceSnapshot{Postgres: &testbackupruntime.BackupPostgresSourceSnapshot{
			ConsumerEnvironmentID:      testBackupEnvironmentID,
			AttachID:                   attachID,
			AttachRevision:             13,
			BackingProjectID:           testBackupProjectID,
			BackingProjectRevision:     14,
			BackingEnvironmentID:       testBackupBackingEnvironmentID,
			BackingEnvironmentRevision: 15,
			BackingServiceID:           testBackupServiceID,
			BackingServiceRevision:     16,
			ConsumerServiceID:          testBackupServiceID,
			AttachFactsRevision:        17,
			Database:                   "app_000001",
			Role:                       "app_000001",
			ManagedReleaseIndex:        testBackupManagedRelease(),
		}},
		Format:                 testbackupruntime.BackupRuntimeFormatPostgres,
		RecoveryPointID:        pointID,
		RecoveryPointCreatedAt: createdAt,
		ObjectKey:              "production/" + testBackupEnvironmentID + "/" + sourceID + "/" + pointID + "/artifact.bin",
		State:                  state,
		Phase:                  phase,
	}
	if state == testbackupruntime.BackupSourceAttemptSucceeded {
		record.PostgresArchive = testbackupruntime.BackupPostgresArchiveEvidence{
			PGDumpMajor: 16, AdapterContractVersion: 1, SourceServerVersion: "16.9", BackupToolVersion: "16.9",
		}
		record.Evidence = testBackupArtifact()
		record.Object = testBackupObject(record.ObjectKey)
		record.Upload = testbackupruntime.BackupUploadOutcome{
			Kind: testbackupruntime.BackupUploadReturned, Target: record.Object.Target, ReturnedObject: record.Object,
		}
	}
	return record
}

func newTestBackupRecipient(t *testing.T) string {
	t.Helper()
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("age.GenerateX25519Identity() error = %v", err)
	}
	return identity.Recipient().String()
}

func testBackupArtifact() testbackupruntime.BackupArtifactEvidence {
	return testbackupruntime.BackupArtifactEvidence{
		SourceSizeBytes: 4096, SourceSHA256: testBackupDigest,
		StoredSizeBytes: 4296, StoredSHA256: testBackupDigest,
	}
}

func testBackupObject(key string) testbackupruntime.BackupObjectIdentity {
	return testbackupruntime.BackupObjectIdentity{
		Target: testbackupruntime.BackupObjectTarget{
			ConnectorID: testBackupConnectorID, ConnectorPrefix: "production/",
			ConnectorEndpoint: "https://objects.example.test", ConnectorBucket: "backups",
			ConnectorRegion: "auto", ObjectKey: key,
		},
		Discriminator: testbackupruntime.BackupObjectDiscriminator{
			Kind: backupobject.DiscriminatorVersionID, Value: "version-1",
		},
	}
}

func testBackupManagedRelease() string {
	return fmt.Sprintf(
		`{"schema":1,"image":"registry.example.test/postgres-tools@sha256:%[1]s","database_image":"postgres:16-alpine@sha256:%[1]s","images":[{"repository_digest":"registry.example.test/postgres-tools@sha256:%[1]s","image_id":"sha256:%[1]s","manifest":{"schema":1,"os":"linux","architecture":"amd64","postgresql_major":16,"helper_sha256":"%[1]s","gate_sha256":"%[1]s","pg_dump_sha256":"%[1]s","pg_restore_sha256":"%[1]s","psql_sha256":"%[1]s","launch_profile_sha256":"%[2]s","gate_seccomp_sha256":"%[1]s"}}]}`,
		testBackupDigest,
		postgres16protocol.ManagedLaunchProfileSHA256().String(),
	)
}
