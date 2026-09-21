package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

// AdvanceBackupRetentionSweep scans one bounded newest-first source page,
// retains exactly the first Keep visible points, and atomically tombstones
// every older visible point in the page under the owning Backup lock.
func (repository *BackupRuntimeRepository) AdvanceBackupRetentionSweep(
	ctx context.Context,
	run etcdstore.Versioned[backupruntime.BackupRunRecord],
	current etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord],
	advancedAt time.Time,
) (etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord], []etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord], error) {
	if current.Revision <= 0 ||
		(current.Record.State != backupruntime.BackupRetentionPending && current.Record.State != backupruntime.BackupRetentionScanning) ||
		!backupruntime.ValidBackupRuntimeInstant(advancedAt) || !advancedAt.After(current.Record.UpdatedAt) ||
		backupruntime.ValidateBackupRunRecord(run.Record) != nil || run.Revision <= 0 ||
		run.Record.State != backupruntime.BackupRunRunning ||
		!backupRetentionSweepMatchesRun(run.Record, current.Record) {
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, errs.New(
			errs.KindValidationFailed,
			"backup retention transition is invalid",
		)
	}
	sweepKey := backupruntime.BackupRetentionKey(current.Record.SourceID, current.Record.TriggerRecoveryPointID)
	anchor, err := repository.ReadCurrentKeys(
		ctx,
		[]string{backupruntime.BackupRunKey(run.Record.TaskID), sweepKey},
	)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, err
	}
	defer etcdstore.ClearValues(anchor.Values)
	if anchor.Values[0] == nil || anchor.Values[0].ModRevision != run.Revision ||
		anchor.Values[1] == nil || anchor.Values[1].ModRevision != current.Revision {
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, errs.New(
			errs.KindStateConflict,
			"backup retention authority changed",
		)
	}
	storedRun, runErr := backupruntime.DecodeBackupRunRecord(anchor.Values[0].Value)
	storedSweep, sweepErr := backupruntime.DecodeBackupRetentionSweepRecord(anchor.Values[1].Value)
	if runErr != nil || sweepErr != nil || !backupruntime.BackupRunRecordsEqual(storedRun, run.Record) ||
		storedSweep != current.Record || !backupRetentionSweepMatchesRun(storedRun, storedSweep) {
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, backupruntime.CorruptBackupRuntimeRecord()
	}
	prefix := backupruntime.BackupRecoveryPointSourcePrefix + current.Record.SourceID + "/"
	startExclusive := ""
	if current.Record.Cursor != "" {
		startExclusive, err = backupruntime.BackupRecoveryPointSourceIndexKey(
			current.Record.SourceID,
			current.Record.Cursor,
		)
		if err != nil {
			return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, err
		}
	}
	selectionRevision := current.Record.SelectionRevision
	if selectionRevision == 0 {
		selectionRevision = anchor.ReadRevision
	}
	if selectionRevision <= 0 || selectionRevision > anchor.ReadRevision {
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, backupruntime.CorruptBackupRuntimeRecord()
	}
	index, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: prefix, StartExclusive: startExclusive, Limit: backupruntime.MaximumBackupPruneBatch,
		Revision: selectionRevision,
	})
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, err
	}
	if index == nil || index.ReadRevision != selectionRevision {
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, errs.New(
			errs.KindInternal,
			"backup retention point page is incomplete",
		)
	}
	defer etcdstore.ClearRangeValues(index.Values)
	authorityKeys := make([]string, 0, len(index.Values)*2)
	pointIDs := make([]string, len(index.Values))
	for position, item := range index.Values {
		pointID := string(item.Value)
		expected, keyErr := backupruntime.BackupRecoveryPointSourceIndexKey(current.Record.SourceID, pointID)
		if keyErr != nil || expected != item.Key {
			return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, backupruntime.CorruptBackupRuntimeRecord()
		}
		pointIDs[position] = pointID
		authorityKeys = append(
			authorityKeys,
			backupruntime.BackupRecoveryPointKey(pointID),
			backupruntime.BackupRecoveryPointPruneKey(pointID),
		)
	}
	authority, err := repository.ReadRecoveryPointPageChunks(
		ctx,
		authorityKeys,
		selectionRevision,
	)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, err
	}
	if authority == nil || authority.ReadRevision != selectionRevision ||
		len(authority.Values) != len(authorityKeys) {
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, errs.New(
			errs.KindInternal,
			"backup retention point authority is incomplete",
		)
	}
	defer etcdstore.ClearValues(authority.Values)
	points := make([]backupruntime.BackupRecoveryPointRecord, len(pointIDs))
	companionKeys := make([]string, 0, len(pointIDs)*3)
	for position, pointID := range pointIDs {
		pointValue := authority.Values[position*2]
		if pointValue == nil || pointValue.Version != 1 || index.Values[position].Version != 1 ||
			index.Values[position].ModRevision != pointValue.ModRevision {
			return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, backupruntime.CorruptBackupRuntimeRecord()
		}
		point, decodeErr := backupruntime.DecodeBackupRecoveryPointRecord(pointValue.Value)
		if decodeErr != nil || point.ID != pointID ||
			point.EnvironmentID != run.Record.EnvironmentID ||
			point.SourceID != current.Record.SourceID {
			return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, backupruntime.CorruptBackupRuntimeRecord()
		}
		environmentIndex, keyErr := backupruntime.BackupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
		if keyErr != nil {
			return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, backupruntime.CorruptBackupRuntimeRecord()
		}
		sourceIndex, keyErr := backupruntime.BackupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
		if keyErr != nil || sourceIndex != index.Values[position].Key {
			return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, backupruntime.CorruptBackupRuntimeRecord()
		}
		connectorIndex, keyErr := backupruntime.BackupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
		if keyErr != nil {
			return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, backupruntime.CorruptBackupRuntimeRecord()
		}
		points[position] = point
		companionKeys = append(companionKeys, environmentIndex, sourceIndex, connectorIndex)
	}
	companions, err := repository.ReadRecoveryPointPageChunks(
		ctx,
		companionKeys,
		selectionRevision,
	)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, err
	}
	defer etcdstore.ClearValues(companions.Values)
	if companions.ReadRevision != selectionRevision || len(companions.Values) != len(companionKeys) {
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, backupruntime.CorruptBackupRuntimeRecord()
	}
	for position, point := range points {
		pointRevision := authority.Values[position*2].ModRevision
		for offset := range 3 {
			companion := companions.Values[position*3+offset]
			if companion == nil || companion.Version != 1 || companion.ModRevision != pointRevision ||
				companion.Key != companionKeys[position*3+offset] || string(companion.Value) != point.ID {
				return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, backupruntime.CorruptBackupRuntimeRecord()
			}
		}
	}
	next := current.Record
	next.State = backupruntime.BackupRetentionScanning
	next.SelectionRevision = selectionRevision
	next.UpdatedAt = advancedAt
	if next.PruneOperationID == "" {
		next.PruneOperationID = ids.New(ids.KindOperation)
	}
	conditions := []etcdstore.Condition{
		{Key: backupruntime.BackupRunKey(run.Record.TaskID), ModRevision: run.Revision},
		{Key: sweepKey, ModRevision: current.Revision},
	}
	created := make([]backupruntime.BackupRecoveryPointPruneRecord, 0, len(index.Values))
	mutations := make([]etcdstore.Mutation, 0, len(index.Values)+2)
	for position, pointID := range pointIDs {
		pointValue := authority.Values[position*2]
		pruneValue := authority.Values[position*2+1]
		if pointValue == nil {
			return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, backupruntime.CorruptBackupRuntimeRecord()
		}
		point := points[position]
		conditions = append(
			conditions,
			etcdstore.Condition{Key: companionKeys[position*3], ModRevision: pointValue.ModRevision},
			etcdstore.Condition{Key: companionKeys[position*3+1], ModRevision: pointValue.ModRevision},
			etcdstore.Condition{Key: companionKeys[position*3+2], ModRevision: pointValue.ModRevision},
			etcdstore.Condition{Key: backupruntime.BackupRecoveryPointKey(pointID), ModRevision: pointValue.ModRevision},
		)
		if pruneValue != nil {
			prune, pruneErr := backupruntime.DecodeBackupRecoveryPointPruneRecord(pruneValue.Value)
			if pruneErr != nil || prune.Point != point.BackupRecoveryPointSnapshot ||
				prune.State == backupruntime.BackupPruneVerifiedAbsent {
				return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, backupruntime.CorruptBackupRuntimeRecord()
			}
			conditions = append(conditions, etcdstore.Condition{
				Key: backupruntime.BackupRecoveryPointPruneKey(pointID), ModRevision: pruneValue.ModRevision,
			})
			next.Cursor = pointID
			continue
		}
		conditions = append(conditions, etcdstore.Condition{Key: backupruntime.BackupRecoveryPointPruneKey(pointID)})
		if next.RetainedCount < next.Keep {
			next.RetainedCount++
			next.Cursor = pointID
			continue
		}
		prune := backupruntime.BackupRecoveryPointPruneRecord{
			Point: point.BackupRecoveryPointSnapshot, PointRevision: pointValue.ModRevision,
			OperationID: next.PruneOperationID,
			State:       backupruntime.BackupPrunePending, CreatedAt: advancedAt, UpdatedAt: advancedAt,
		}
		encoded, encodeErr := backupruntime.EncodeBackupRecoveryPointPruneRecord(prune)
		if encodeErr != nil {
			etcdstore.ClearMutationValues(mutations)
			return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, encodeErr
		}
		mutations = append(mutations, etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: backupruntime.BackupRecoveryPointPruneKey(pointID), Value: encoded,
		})
		created = append(created, prune)
		next.Cursor = pointID
	}
	if !index.More {
		next.State = backupruntime.BackupRetentionCompleted
	}
	nextValue, err := backupruntime.EncodeBackupRetentionSweepRecord(next)
	if err != nil {
		etcdstore.ClearMutationValues(mutations)
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, err
	}
	mutations = append(
		[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: sweepKey, Value: nextValue}},
		mutations...)
	evidence, err := repository.loadOwnedEvidence(ctx, run.Record, anchor.ReadRevision)
	if err != nil {
		etcdstore.ClearMutationValues(mutations)
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, err
	}
	conditions = append(conditions, evidence.fence.TransactionConditions()...)
	epoch, err := evidence.fence.EpochRewriteMutation()
	if err != nil {
		etcdstore.ClearMutationValues(mutations)
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, err
	}
	mutations = append(mutations, epoch)
	result, err := repository.TransactRuntime(ctx, conditions, mutations)
	etcdstore.ClearMutationValues(mutations)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, err
	}
	if !result.Succeeded {
		etcdstore.ClearValues(result.FailureReads)
		return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{}, nil, errs.New(
			errs.KindStateConflict,
			"backup retention authority changed",
		)
	}
	prunes := make([]etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord], len(created))
	for index, prune := range created {
		prunes[index] = etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{
			Record: prune, Revision: result.Revision, ReadRevision: result.Revision,
		}
	}
	return etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord]{
		Record: next, Revision: result.Revision, ReadRevision: result.Revision,
	}, prunes, nil
}

func backupRetentionSweepMatchesRun(
	run backupruntime.BackupRunRecord,
	sweep backupruntime.BackupRetentionSweepRecord,
) bool {
	if sweep.Revision != run.PolicyRevision || sweep.Keep != run.RetentionKeep {
		return false
	}
	for index := range run.Sources {
		source := run.Sources[index]
		if source.SourceID == sweep.SourceID {
			return source.RecoveryPointID == sweep.TriggerRecoveryPointID
		}
	}
	return false
}
