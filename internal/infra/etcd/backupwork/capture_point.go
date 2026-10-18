package backupwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// prepareCapturedPoint publishes only from the verified run source after its
// cleanup receipt. Its mutations must join run progress and the native receipt;
// no diagnostic Task history is needed to read or retain the resulting Point.
func (repository *InputRepository) prepareCapturedPoint(ctx context.Context, run backupruntime.BackupRunRecord,
	ordinal uint32, revision int64, at time.Time,
) (workqueue.Change, error) {
	point, sweep, err := backupruntime.PrepareBackupPointPublication(run, ordinal, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	point.Capture = backupruntime.BackupRecoveryPointCapture{TaskID: run.TaskID, CreatedAt: run.CreatedAt, SourceCount: len(run.Sources)}
	environmentIndex, err := backupruntime.BackupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
	if err != nil {
		return workqueue.Change{}, err
	}
	sourceIndex, err := backupruntime.BackupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
	if err != nil {
		return workqueue.Change{}, err
	}
	connectorIndex, err := backupruntime.BackupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		return workqueue.Change{}, err
	}
	keys := []string{connectors.RecordKey(point.ConnectorID), connectors.CredentialValueKey(point.ConnectorID),
		backupruntime.BackupRecoveryPointKey(point.ID), backupruntime.BackupRecoveryPointCaptureKey(point.ID),
		environmentIndex, sourceIndex, connectorIndex, backupruntime.BackupRetentionKey(point.SourceID, point.ID),
		backupruntime.BackupOrphanKey(point.ID), backupruntime.BackupRecoveryPointPruneKey(point.ID)}
	read, err := repository.readSettlementKeys(ctx, keys, revision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if err := backupruntime.ValidateBackupConnectorSnapshotEvidence(read.Values[:2], run); err != nil {
		return workqueue.Change{}, err
	}
	change := workqueue.Change{}
	for index, key := range keys {
		if index >= 2 && read.Values[index] != nil {
			return workqueue.Change{}, conflict("Backup Point publication already has resource ownership")
		}
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[index])})
	}
	pointValue, err := backupruntime.EncodeBackupRecoveryPointRecord(point)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(pointValue)
	captureValue, err := backupruntime.EncodeRecoveryPointCapture(point.Capture)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(captureValue)
	sweepValue, err := backupruntime.EncodeBackupRetentionSweepRecord(sweep)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(sweepValue)
	fence, err := environmentfence.LoadOwned(ctx, repository.store, run.EnvironmentID, revision,
		environmentfence.Owner{Kind: backupruntime.BackupOperationBackup, OperationID: run.OperationID, TaskID: run.TaskID})
	if err != nil {
		return workqueue.Change{}, err
	}
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, fence.TransactionConditions()...)
	change.Mutations = []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: keys[2], Value: append([]byte(nil), pointValue...)},
		{Type: keyvalue.MutationPut, Key: keys[3], Value: append([]byte(nil), captureValue...)},
		{Type: keyvalue.MutationPut, Key: environmentIndex, Value: []byte(point.ID)},
		{Type: keyvalue.MutationPut, Key: sourceIndex, Value: []byte(point.ID)},
		{Type: keyvalue.MutationPut, Key: connectorIndex, Value: []byte(point.ID)},
		{Type: keyvalue.MutationPut, Key: keys[7], Value: append([]byte(nil), sweepValue...)},
		epoch,
	}
	return change, nil
}
