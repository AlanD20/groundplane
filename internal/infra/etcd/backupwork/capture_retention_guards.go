package backupwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// An orphan outlives execution history only after it owns an acknowledged
// cleanup proof. Final collection must repeat these guards in its transaction;
// a successful read alone never releases the original execution evidence.
func (repository *InputRepository) captureRetentionGuards(ctx context.Context, view workqueue.View,
	input *agentpb.BackupWorkInput,
) (bool, backupruntime.BackupRunRecord, []keyvalue.Condition, error) {
	var zero backupruntime.BackupRunRecord
	if view.Item.Item.Operation != workexecution.Backup || view.Task.Task.CollectionStartedAt == nil ||
		view.Item.Item.Phase != workexecution.Settled || view.Item.Item.ActiveAttemptID != "" ||
		(view.Item.Item.Cleanup != workexecution.CleanupNone && view.Item.Item.Cleanup != workexecution.CleanupComplete) {
		return false, zero, nil, conflict("Backup source retention requires fully settled execution")
	}
	run, runGuard, err := repository.LoadStagingCapture(ctx, StagingSource{View: view, Input: input})
	if err != nil {
		return false, zero, nil, err
	}
	if !backupruntime.TerminalBackupRunState(run.State) {
		return false, zero, nil, conflict("Backup execution settled without a terminal resource run")
	}
	environments, err := backupruntime.DatabaseBackingEnvironmentIDs(run)
	if err != nil {
		return false, zero, nil, err
	}
	ready, backingGuards, err := repository.retentionBackingGuards(
		ctx,
		view,
		environments,
		run.OperationID,
		backupruntime.BackupOperationBackup,
	)
	if err != nil || !ready {
		return false, zero, nil, err
	}
	keys := make([]string, len(run.Sources))
	for ordinal, source := range run.Sources {
		keys[ordinal] = backupruntime.BackupOrphanKey(source.RecoveryPointID)
	}
	read, err := repository.readSettlementKeys(ctx, keys, view.ReadRevision)
	if err != nil {
		return false, zero, nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	conditions := append(backingGuards, runGuard)
	for ordinal, value := range read.Values {
		guard := keyvalue.Condition{Key: keys[ordinal]}
		if value != nil {
			orphan, err := backupruntime.DecodeBackupOrphanRecord(value.Value)
			if err != nil {
				return false, zero, nil, err
			}
			source := run.Sources[ordinal]
			if orphan.TaskID != run.TaskID || orphan.Target.ID != source.RecoveryPointID ||
				orphan.Target.EnvironmentID != run.EnvironmentID || orphan.Target.SourceID != source.SourceID {
				return false, zero, nil, conflict("Backup orphan retention belongs to another source")
			}
			if orphan.CleanupProof == (backupruntime.BackupOrphanCleanupProof{}) {
				return false, run, nil, nil
			}
			guard.ModRevision = value.ModRevision
		}
		conditions = append(conditions, guard)
	}
	return true, run, conditions, nil
}
