package backupwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (repository *InputRepository) captureFailureOrphan(ctx context.Context,
	current, next backupruntime.BackupRunRecord, revision int64,
) (workqueue.Change, error) {
	create := backupruntime.BackupRunNeedsTerminalOrphan(current, next)
	retain := backupruntime.BackupRunRetainsTerminalOrphan(current, next)
	if !create && !retain {
		return workqueue.Change{}, nil
	}
	ordinal, changed := backupruntime.ChangedBackupSourceOrdinal(current, next)
	if !changed {
		return workqueue.Change{}, conflict("Backup failed upload has no unique source")
	}
	orphan := backupruntime.BackupOrphanRecordFromRun(next, ordinal)
	connector, err := backupruntime.BackupOrphanConnectorIndexKey(orphan.Target.ConnectorID, orphan.Target.ID)
	if err != nil {
		return workqueue.Change{}, err
	}
	environment, err := backupruntime.BackupOrphanEnvironmentIndexKey(orphan.Target.EnvironmentID, orphan.Target.ID)
	if err != nil {
		return workqueue.Change{}, err
	}
	keys := []string{backupruntime.BackupOrphanKey(orphan.Target.ID), connector, environment}
	var change workqueue.Change
	if create {
		raw, err := backupruntime.EncodeBackupOrphanRecord(orphan)
		if err != nil {
			return workqueue.Change{}, err
		}
		for _, key := range keys {
			change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key})
		}
		change.Mutations = []keyvalue.Mutation{
			{Type: keyvalue.MutationPut, Key: keys[0], Value: raw},
			{Type: keyvalue.MutationPut, Key: connector, Value: []byte(orphan.Target.ID)},
			{Type: keyvalue.MutationPut, Key: environment, Value: []byte(orphan.Target.ID)},
		}
		return change, nil
	}
	return repository.retainedCaptureOrphan(ctx, current, ordinal, revision)
}

func (repository *InputRepository) retainedCaptureOrphan(ctx context.Context, current backupruntime.BackupRunRecord,
	ordinal uint32, revision int64,
) (workqueue.Change, error) {
	if uint64(ordinal) >= uint64(len(current.Sources)) {
		return workqueue.Change{}, conflict("Backup orphan source is unavailable")
	}
	point := current.Sources[ordinal].RecoveryPointID
	connector, err := backupruntime.BackupOrphanConnectorIndexKey(current.ConnectorID, point)
	if err != nil {
		return workqueue.Change{}, err
	}
	environment, err := backupruntime.BackupOrphanEnvironmentIndexKey(current.EnvironmentID, point)
	if err != nil {
		return workqueue.Change{}, err
	}
	keys := []string{backupruntime.BackupOrphanKey(point), connector, environment}
	read, err := repository.readSettlementKeys(ctx, keys, revision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return workqueue.Change{}, conflict("Backup failed upload lost its orphan record")
	}
	saved, err := backupruntime.DecodeBackupOrphanRecord(read.Values[0].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	if saved.TaskID != current.TaskID || saved.State != backupruntime.BackupOrphanInspect ||
		!backupruntime.BackupOrphanMatchesRunSource(saved, current, ordinal) {
		return workqueue.Change{}, conflict("Backup failed upload orphan ownership changed")
	}
	if err := backupruntime.ValidateBackupOrphanCompanionEvidence(read.Values, saved); err != nil {
		return workqueue.Change{}, err
	}
	var change workqueue.Change
	for index, value := range read.Values {
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: keys[index], ModRevision: value.ModRevision})
	}
	return change, nil
}
