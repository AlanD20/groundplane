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
	return point.BackupRecoveryPointTargetSnapshot == backupSourceTarget(run, source) &&
		point.Evidence == source.Evidence && point.ConfigArchive == source.ConfigArchive &&
		point.VolumeArchive == source.VolumeArchive && point.Object == source.Object
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

// Adoption accepts only a fully verified selected identity for the retained
// target, and never replaces an identity returned by the original Put.
func BackupOrphanMatchesRecoveryPoint(orphan BackupOrphanRecord, point BackupRecoveryPointSnapshot) bool {
	if validateBackupOrphanRecord(orphan) != nil || ValidateBackupRecoveryPointSnapshot(point) != nil ||
		orphan.Target != point.BackupRecoveryPointTargetSnapshot || orphan.Evidence != point.Evidence ||
		orphan.ConfigArchive != point.ConfigArchive || orphan.VolumeArchive != point.VolumeArchive {
		return false
	}
	if orphan.Postgres != point.Postgres {
		return false
	}
	if orphan.Object != (BackupObjectIdentity{}) {
		return orphan.Object == point.Object
	}
	if orphan.Upload.Kind == BackupUploadReturned {
		return orphan.Upload.ReturnedObject == point.Object
	}
	return false
}

func BackupOrphanMatchesRunSource(
	orphan BackupOrphanRecord,
	run BackupRunRecord,
	ordinal uint32,
) bool {
	if int(ordinal) >= len(run.Sources) || validateBackupOrphanRecord(orphan) != nil {
		return false
	}
	source := run.Sources[ordinal]
	return orphan.TaskID == run.TaskID &&
		orphan.Reconciliation == (BackupOrphanReconciliationAuthority{
			OperationID:    run.OperationID,
			PolicyRevision: run.PolicyRevision,
			PolicySHA256:   run.PolicySHA256,
			RetentionKeep:  run.RetentionKeep,
		}) &&
		orphan.Target == backupSourceTarget(run, source) && orphan.Evidence == source.Evidence &&
		orphan.ConfigArchive == source.ConfigArchive && orphan.VolumeArchive == source.VolumeArchive &&
		orphan.Upload == source.Upload && orphan.Object == source.Object && orphan.Phase == source.Phase &&
		backupOrphanPostgresMatchesSource(orphan.Postgres, source.Snapshot.Postgres)
}

func backupOrphanPostgresMatchesSource(point BackupPostgresPointIdentity, source *BackupPostgresSourceSnapshot) bool {
	if source == nil {
		return point == (BackupPostgresPointIdentity{})
	}
	return point == (BackupPostgresPointIdentity{Database: source.Database, Role: source.Role,
		BackingEnvironmentID: source.BackingEnvironmentID, BackingServiceID: source.BackingServiceID,
		ConsumerServiceID: source.ConsumerServiceID})
}

func ValidateBackupOrphanCompanionEvidence(values []*etcdstore.KeyValue, expected BackupOrphanRecord) error {
	expectedVersion := backupOrphanRecordVersion(expected)
	if len(values) != 3 || values[0] == nil || values[1] == nil || values[2] == nil ||
		values[0].Version != expectedVersion || values[1].Version != expectedVersion ||
		values[2].Version != expectedVersion ||
		values[0].ModRevision != values[1].ModRevision ||
		values[0].ModRevision != values[2].ModRevision ||
		string(
			values[1].Value,
		) != expected.Target.ID || string(values[2].Value) != expected.Target.ID {
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
