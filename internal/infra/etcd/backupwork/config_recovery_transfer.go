package backupwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// LoadConfigRecoveryTransfer authorizes the current recovery attempt to use
// the original transfer. The failed forward identity alone grants no access.
func (repository *InputRepository) LoadConfigRecoveryTransfer(ctx context.Context, queue *workqueue.Repository,
	binding backupconfigtransfer.Binding, recovery workexecution.Attempt, revision int64,
) (*agentpb.BackupRecoveryAssignment, []keyvalue.Condition, error) {
	if queue == nil || binding.Validate() != nil || revision <= 0 || recovery.Direction != workexecution.Recovery ||
		binding.Direction != agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE {
		return nil, nil, conflict("Config recovery transfer requires its original source and recovery attempt")
	}
	view, err := queue.ReadResumeAt(ctx, recovery.TaskID, recovery.WorkItemID, recovery.ID, recovery.Executor, revision)
	if err != nil {
		return nil, nil, err
	}
	live := view.Attempt.Attempt
	now := time.Now().UTC()
	if !workexecution.SameAssignment(live, recovery) || !view.CurrentExecutor || !view.ActiveAttempt ||
		live.StartedAt == nil || live.FinishedAt != nil || !now.Before(live.Deadline) {
		return nil, nil, conflict("Config recovery transfer no longer owns its running attempt")
	}
	assignment, change, err := repository.LoadRecoveryAssignment(ctx, view)
	if err != nil {
		return nil, nil, err
	}
	expected, err := backupconfigtransfer.BindRecoverySource(assignment, recovery.Executor, binding.SourceOrdinal, binding.Direction)
	if err != nil {
		return nil, nil, err
	}
	if expected != binding || assignment.RestoreMode != agentpb.BackupRestoreRecoveryMode_BACKUP_RESTORE_RECOVERY_MODE_LIVE_MUTATION {
		return nil, nil, conflict("Config recovery transfer differs from the captured mutation")
	}
	conditions := append(change.Conditions,
		keyvalue.Condition{Key: workqueue.TaskKey(recovery.TaskID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(recovery.WorkItemID), ModRevision: view.Work.Item.Revision},
		keyvalue.Condition{Key: workqueue.AttemptKey(recovery.ID), ModRevision: view.Attempt.Revision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(recovery.Executor), ModRevision: view.ExecutorRevision})
	return assignment, conditions, nil
}
