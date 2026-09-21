package backupruntime

import (
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func BackupPointMatchesRunSource(
	point BackupRecoveryPointSnapshot,
	run BackupRunRecord,
	ordinal uint32,
) bool {
	if int(ordinal) >= len(run.Sources) || ValidateBackupRecoveryPointSnapshot(point) != nil {
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

func RunContainsOrphanedPoint(run BackupRunRecord, orphan BackupOrphanRecord) bool {
	for ordinal, source := range run.Sources {
		if source.State == BackupSourceAttemptOrphaned &&
			BackupOrphanMatchesRunSource(orphan, run, uint32(ordinal)) {
			return true
		}
	}
	return false
}

func BackupOrphanMatchesRunSource(
	orphan BackupOrphanRecord,
	run BackupRunRecord,
	ordinal uint32,
) bool {
	return orphan.TaskID == run.TaskID &&
		orphan.Reconciliation == (BackupOrphanReconciliationAuthority{
			OperationID:    run.OperationID,
			PolicyRevision: run.PolicyRevision,
			RetentionKeep:  run.RetentionKeep,
		}) &&
		BackupPointMatchesRunSource(orphan.Point, run, ordinal)
}

func ValidateBackupOrphanCompanionEvidence(values []*etcdstore.KeyValue, expected BackupOrphanRecord) error {
	expectedVersion := int64(1)
	if expected.State == BackupOrphanDelete {
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
		return CorruptBackupRuntimeRecord()
	}
	stored, err := DecodeBackupOrphanRecord(values[0].Value)
	if err != nil || stored != expected {
		return CorruptBackupRuntimeRecord()
	}
	return nil
}

func ValidateBackupConnectorSnapshotEvidence(values []*etcdstore.KeyValue, run BackupRunRecord) error {
	if len(values) != 2 || values[0] == nil || values[0].ModRevision != run.ConnectorRevision {
		return errs.New(errs.KindStateConflict, "backup connector snapshot changed")
	}
	connector, err := connectorrecord.DecodeRecord(values[0].Value)
	if err != nil || connector.Connector.ID != run.ConnectorID {
		return CorruptBackupRuntimeRecord()
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
		return CorruptBackupRuntimeRecord()
	}
	return nil
}
