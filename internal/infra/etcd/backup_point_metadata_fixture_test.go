package etcd

import (
	context "context"
	testing "testing"
	time "time"

	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func seedBackupRuntimePointAuthority(
	t *testing.T,
	store *memoryHierarchyStore,
	point testbackupruntime.BackupRecoveryPointRecord,
) int64 {
	t.Helper()
	value, err := testbackupruntime.EncodeBackupRecoveryPointRecord(point)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value)
	environmentIndex, err := testbackupruntime.BackupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	sourceIndex, err := testbackupruntime.BackupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	connectorIndex, err := testbackupruntime.BackupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Transact(context.Background(), []testkeyvalue.Condition{
		{Key: testbackupruntime.BackupRecoveryPointKey(point.ID)}, {Key: environmentIndex},
		{Key: sourceIndex}, {Key: connectorIndex},
	}, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testbackupruntime.BackupRecoveryPointKey(point.ID), Value: value},
		{Type: testkeyvalue.MutationPut, Key: environmentIndex, Value: []byte(point.ID)},
		{Type: testkeyvalue.MutationPut, Key: sourceIndex, Value: []byte(point.ID)},
		{Type: testkeyvalue.MutationPut, Key: connectorIndex, Value: []byte(point.ID)},
	})
	if err != nil || !result.Succeeded {
		t.Fatalf("seed Recovery Point authority = %#v, %v", result, err)
	}
	return result.Revision
}

func backupRuntimeTestPoint(
	run testbackupruntime.BackupRunRecord,
	source testbackupruntime.BackupRunSourceAttemptRecord,
	verifiedAt time.Time,
) testbackupruntime.BackupRecoveryPointRecord {
	point := testbackupruntime.BackupRecoveryPointRecord{
		BackupRecoveryPointSnapshot: testbackupruntime.BackupRecoveryPointSnapshot{
			BackupRecoveryPointTargetSnapshot: testbackupruntime.BackupRecoveryPointTargetSnapshot{
				ID: source.RecoveryPointID, EnvironmentID: run.EnvironmentID,
				SourceID: source.SourceID, SourceKind: source.Kind, TargetID: source.TargetID,
				ConnectorID: run.ConnectorID, ConnectorPrefix: run.ConnectorPrefix,
				ConnectorEndpoint: run.ConnectorEndpoint, ConnectorBucket: run.ConnectorBucket,
				ConnectorRegion: run.ConnectorRegion, ConnectorPathStyle: run.ConnectorPathStyle,
				ObjectKey: source.ObjectKey, SourceFormat: source.Format,
				Encryption: run.Encryption, KeyEra: run.KeyEra, Recipient: run.Recipient,
				CreatedAt: source.RecoveryPointCreatedAt,
			},
			Evidence: source.Evidence, Object: source.Object,
			ConfigArchive: source.ConfigArchive, VolumeArchive: source.VolumeArchive,
			PostgresArchive: source.PostgresArchive, MySQLArchive: source.MySQLArchive,
		},
		VerifiedAt: verifiedAt,
	}
	if source.Snapshot.Postgres != nil {
		point.Postgres = testbackupruntime.BackupPostgresPointIdentity{
			Database: source.Snapshot.Postgres.Database, Role: source.Snapshot.Postgres.Role,
			BackingEnvironmentID: source.Snapshot.Postgres.BackingEnvironmentID,
			BackingServiceID:     source.Snapshot.Postgres.BackingServiceID,
			ConsumerServiceID:    source.Snapshot.Postgres.ConsumerServiceID,
		}
	}
	return point
}

func backupRuntimeCompleteSourceArtifact(
	run testbackupruntime.BackupRunRecord,
	source *testbackupruntime.BackupRunSourceAttemptRecord,
) {
	source.Evidence = testBackupArtifact()
	if source.Format == testbackupruntime.BackupRuntimeFormatPostgres {
		source.PostgresArchive = testbackupruntime.BackupPostgresArchiveEvidence{
			PGDumpMajor: 16, AdapterContractVersion: 1, SourceServerVersion: "16.9", BackupToolVersion: "16.9",
		}
	}
	source.Object = testbackupruntime.BackupObjectIdentity{
		Target: testbackupruntime.BackupObjectTarget{
			ConnectorID: run.ConnectorID, ConnectorPrefix: run.ConnectorPrefix,
			ConnectorEndpoint: run.ConnectorEndpoint, ConnectorBucket: run.ConnectorBucket,
			ConnectorRegion: run.ConnectorRegion, ConnectorPathStyle: run.ConnectorPathStyle,
			ObjectKey: source.ObjectKey,
		},
		Discriminator: testBackupObject(source.ObjectKey).Discriminator,
	}
	source.Upload = testbackupruntime.BackupUploadOutcome{
		Kind:   testbackupruntime.BackupUploadReturned,
		Target: source.Object.Target, ReturnedObject: source.Object,
	}
}
