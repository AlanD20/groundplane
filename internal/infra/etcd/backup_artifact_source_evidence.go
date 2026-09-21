package etcd

import (
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func backupPointMatchesRunSource(
	point backupruntime.BackupRecoveryPointSnapshot,
	run backupruntime.BackupRunRecord,
	ordinal uint32,
) bool {
	if int(ordinal) >= len(run.Sources) || backupruntime.ValidateBackupRecoveryPointSnapshot(point) != nil {
		return false
	}
	source := run.Sources[ordinal]
	return point.ID == source.RecoveryPointID && point.EnvironmentID == run.EnvironmentID &&
		point.CreatedAt.Equal(source.RecoveryPointCreatedAt) &&
		point.SourceID == source.SourceID && point.SourceKind == source.Kind &&
		point.TargetID == source.TargetID && point.ConnectorID == run.ConnectorID &&
		point.ConnectorPrefix == run.ConnectorPrefix && point.ObjectKey == source.ObjectKey &&
		point.SourceFormat == source.Format &&
		point.Encryption == run.Encryption && point.KeyEra == run.KeyEra && point.Recipient == run.Recipient &&
		point.SizeBytes == source.SizeBytes && point.SHA256 == source.SHA256
}

func runContainsOrphanedPoint(run backupruntime.BackupRunRecord, orphan backupruntime.BackupOrphanRecord) bool {
	for ordinal, source := range run.Sources {
		if source.State == backupruntime.BackupSourceAttemptOrphaned &&
			backupOrphanMatchesRunSource(orphan, run, uint32(ordinal)) {
			return true
		}
	}
	return false
}

func backupOrphanMatchesRunSource(
	orphan backupruntime.BackupOrphanRecord,
	run backupruntime.BackupRunRecord,
	ordinal uint32,
) bool {
	return orphan.TaskID == run.TaskID &&
		orphan.Reconciliation == (backupruntime.BackupOrphanReconciliationAuthority{
			OperationID:    run.OperationID,
			PolicyRevision: run.PolicyRevision,
			RetentionKeep:  run.RetentionKeep,
		}) &&
		backupPointMatchesRunSource(orphan.Point, run, ordinal)
}

func validateBackupOrphanCompanionEvidence(values []*etcdstore.KeyValue, expected backupruntime.BackupOrphanRecord) error {
	expectedVersion := int64(1)
	if expected.State == backupruntime.BackupOrphanDelete {
		expectedVersion = 2
	}
	if len(values) != 3 || values[0] == nil || values[1] == nil || values[2] == nil ||
		values[0].Version != expectedVersion || values[1].Version != expectedVersion ||
		values[2].Version != expectedVersion ||
		values[0].ModRevision != values[1].ModRevision ||
		values[0].ModRevision != values[2].ModRevision ||
		string(
			values[1].Value,
		) != expected.Point.ID || string(values[2].Value) != expected.Point.ID {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	stored, err := backupruntime.DecodeBackupOrphanRecord(values[0].Value)
	if err != nil || stored != expected {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	return nil
}

func validateBackupConnectorSnapshotEvidence(values []*etcdstore.KeyValue, run backupruntime.BackupRunRecord) error {
	if len(values) != 2 || values[0] == nil || values[0].ModRevision != run.ConnectorRevision {
		return errs.New(errs.KindStateConflict, "backup connector snapshot changed")
	}
	connector, err := connectorrecord.DecodeRecord(values[0].Value)
	if err != nil || connector.Connector.ID != run.ConnectorID {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	if connector.Connector.EnvironmentID != run.EnvironmentID {
		return errs.New(errs.KindStateConflict, "backup connector belongs to another environment")
	}
	if connector.Connector.Prefix != run.ConnectorPrefix {
		return errs.New(errs.KindStateConflict, "backup connector prefix changed")
	}
	hasDirectCredentials := connectorrecord.HasDirectCredentials(connector)
	if hasDirectCredentials != run.ConnectorHasDirectCredentials {
		return errs.New(errs.KindStateConflict, "backup connector credential mode changed")
	}
	if !run.ConnectorHasDirectCredentials {
		if run.ConnectorCredentialsRevision != 0 || values[1] != nil {
			return errs.New(errs.KindStateConflict, "backup connector credential snapshot changed")
		}
		return nil
	}
	if values[1] == nil || values[1].ModRevision != run.ConnectorCredentialsRevision {
		return errs.New(errs.KindStateConflict, "backup connector credential snapshot changed")
	}
	credentials, err := connectorrecord.DecodeEncryptedCredentials(values[1].Value)
	defer clear(credentials.Ciphertext)
	if err != nil || credentials.ConnectorID != run.ConnectorID {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	return nil
}
