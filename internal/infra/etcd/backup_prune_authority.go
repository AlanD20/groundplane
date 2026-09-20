package etcd

import (
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func backupPruneAuthorityKeys(point backupruntime.BackupRecoveryPointSnapshot) ([]string, error) {
	environmentIndex, err := backupruntime.BackupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
	if err != nil {
		return nil, err
	}
	sourceIndex, err := backupruntime.BackupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
	if err != nil {
		return nil, err
	}
	connectorIndex, err := backupruntime.BackupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		return nil, err
	}
	return []string{
		backupruntime.BackupRecoveryPointPruneKey(point.ID),
		backupruntime.BackupRecoveryPointKey(point.ID),
		environmentIndex,
		sourceIndex,
		connectorIndex,
	}, nil
}

func validatePendingBackupPruneAuthority(
	values []*etcdstore.KeyValue,
	version etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord],
) error {
	if len(values) != 5 {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	if values[0] == nil || values[0].ModRevision != version.Revision {
		return errs.New(errs.KindStateConflict, "backup prune authority changed")
	}
	storedPrune, err := backupruntime.DecodeBackupRecoveryPointPruneRecord(values[0].Value)
	if err != nil || storedPrune != version.Record {
		return backupruntime.CorruptBackupRuntimeRecord()
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
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	point, err := backupruntime.DecodeBackupRecoveryPointRecord(values[1].Value)
	if err != nil || point.BackupRecoveryPointSnapshot != version.Record.Point {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	environmentIndex, err := backupruntime.BackupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
	if err != nil {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	sourceIndex, err := backupruntime.BackupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
	if err != nil {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	connectorIndex, err := backupruntime.BackupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil || values[1].Key != backupruntime.BackupRecoveryPointKey(point.ID) ||
		values[2].Key != environmentIndex || values[3].Key != sourceIndex ||
		values[4].Key != connectorIndex {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	return nil
}

func validateCompletedBackupRetentionSweep(
	value *etcdstore.KeyValue,
	point backupruntime.BackupRecoveryPointSnapshot,
	pointRevision int64,
) error {
	if value == nil || value.Version < 2 || value.ModRevision <= pointRevision {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	sweep, err := backupruntime.DecodeBackupRetentionSweepRecord(value.Value)
	if err != nil || sweep.SourceID != point.SourceID || sweep.TriggerRecoveryPointID != point.ID ||
		sweep.State != backupruntime.BackupRetentionCompleted {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	return nil
}

func validateExactBackupPruneDispatchValue(
	value *etcdstore.KeyValue,
	expected etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneDispatchRecord],
) error {
	if value == nil || value.ModRevision != expected.Revision {
		return errs.New(errs.KindStateConflict, "backup prune dispatch changed")
	}
	stored, err := backupruntime.DecodeBackupRecoveryPointPruneDispatchRecord(value.Value)
	if err != nil || !backupPruneDispatchRecordsEqual(stored, expected.Record) {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	return nil
}

func backupPruneDispatchRecordsEqual(
	left backupruntime.BackupRecoveryPointPruneDispatchRecord,
	right backupruntime.BackupRecoveryPointPruneDispatchRecord,
) bool {
	if left.TaskID != right.TaskID || left.OperationID != right.OperationID ||
		left.EnvironmentID != right.EnvironmentID ||
		left.CreatedAt != right.CreatedAt || len(left.RecoveryPointIDs) != len(right.RecoveryPointIDs) {
		return false
	}
	for index := range left.RecoveryPointIDs {
		if left.RecoveryPointIDs[index] != right.RecoveryPointIDs[index] {
			return false
		}
	}
	return true
}

func backupPruneDispatchContains(
	dispatch backupruntime.BackupRecoveryPointPruneDispatchRecord,
	recoveryPointID string,
) bool {
	for _, candidate := range dispatch.RecoveryPointIDs {
		if candidate == recoveryPointID {
			return true
		}
	}
	return false
}

func backupPruneDispatchPointOrdinal(
	dispatch backupruntime.BackupRecoveryPointPruneDispatchRecord,
	pointID string,
) (uint32, bool) {
	for index, candidate := range dispatch.RecoveryPointIDs {
		if candidate == pointID {
			return uint32(index), true
		}
	}
	return 0, false
}
