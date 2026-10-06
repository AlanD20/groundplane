package backupruntime

import (
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func BackupPruneAuthorityKeys(point BackupRecoveryPointSnapshot) ([]string, error) {
	environmentIndex, err := BackupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
	if err != nil {
		return nil, err
	}
	sourceIndex, err := BackupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
	if err != nil {
		return nil, err
	}
	connectorIndex, err := BackupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		return nil, err
	}
	return []string{
		BackupRecoveryPointPruneKey(point.ID),
		BackupRecoveryPointKey(point.ID),
		environmentIndex,
		sourceIndex,
		connectorIndex,
	}, nil
}

func ValidatePendingBackupPruneAuthority(
	values []*etcdstore.KeyValue,
	version etcdstore.Versioned[BackupRecoveryPointPruneRecord],
) error {
	if len(values) != 5 {
		return CorruptBackupRuntimeRecord()
	}
	// A zero revision is an initial selection: its tombstone must be absent
	// until the same transaction publishes the assigned deletion Task.
	if version.Revision == 0 {
		if values[0] != nil || version.Record.State != BackupPrunePending ||
			version.Record.TaskID != "" || version.Record.DispatchAttempts != 0 ||
			validateBackupRecoveryPointPruneRecord(version.Record) != nil {
			return errs.New(errs.KindStateConflict, "recovery point deletion is already selected")
		}
	} else {
		if values[0] == nil || values[0].ModRevision != version.Revision {
			return errs.New(errs.KindStateConflict, "backup prune authority changed")
		}
		storedPrune, err := DecodeBackupRecoveryPointPruneRecord(values[0].Value)
		if err != nil || storedPrune != version.Record {
			return CorruptBackupRuntimeRecord()
		}
	}
	if values[1] == nil || values[2] == nil || values[3] == nil || values[4] == nil ||
		values[1].Version != 1 || values[2].Version != 1 || values[3].Version != 1 ||
		values[4].Version != 1 || values[1].ModRevision != version.Record.PointRevision ||
		values[2].ModRevision != version.Record.PointRevision ||
		values[1].ModRevision != values[3].ModRevision ||
		values[1].ModRevision != values[4].ModRevision ||
		string(values[2].Value) != version.Record.Point.ID ||
		string(values[3].Value) != version.Record.Point.ID ||
		string(values[4].Value) != version.Record.Point.ID {
		return CorruptBackupRuntimeRecord()
	}
	point, err := DecodeBackupRecoveryPointRecord(values[1].Value)
	if err != nil || point.BackupRecoveryPointSnapshot != version.Record.Point {
		return CorruptBackupRuntimeRecord()
	}
	environmentIndex, err := BackupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
	if err != nil {
		return CorruptBackupRuntimeRecord()
	}
	sourceIndex, err := BackupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
	if err != nil {
		return CorruptBackupRuntimeRecord()
	}
	connectorIndex, err := BackupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil || values[1].Key != BackupRecoveryPointKey(point.ID) ||
		values[2].Key != environmentIndex || values[3].Key != sourceIndex ||
		values[4].Key != connectorIndex {
		return CorruptBackupRuntimeRecord()
	}
	return nil
}

func ValidateCompletedBackupRetentionSweep(
	value *etcdstore.KeyValue,
	point BackupRecoveryPointSnapshot,
	pointRevision int64,
) error {
	if value == nil || value.Version < 2 || value.ModRevision <= pointRevision {
		return CorruptBackupRuntimeRecord()
	}
	sweep, err := DecodeBackupRetentionSweepRecord(value.Value)
	if err != nil || sweep.SourceID != point.SourceID || sweep.TriggerRecoveryPointID != point.ID ||
		sweep.State != BackupRetentionCompleted {
		return CorruptBackupRuntimeRecord()
	}
	return nil
}

func ValidateExactBackupPruneDispatchValue(
	value *etcdstore.KeyValue,
	expected etcdstore.Versioned[BackupRecoveryPointPruneDispatchRecord],
) error {
	if value == nil || value.ModRevision != expected.Revision {
		return errs.New(errs.KindStateConflict, "backup prune dispatch changed")
	}
	stored, err := DecodeBackupRecoveryPointPruneDispatchRecord(value.Value)
	if err != nil || !BackupPruneDispatchRecordsEqual(stored, expected.Record) {
		return CorruptBackupRuntimeRecord()
	}
	return nil
}
