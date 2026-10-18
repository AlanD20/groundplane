package backupwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func FailureKey(attemptID string) string { return "/v1/runtime/backup-work-failures/" + attemptID }

// PrepareStoppedFailure joins the original failure with its resource state. Only
// the authenticated running executor may report this boundary; disconnected
// executions require interruption inspection rather than this stopped path.
func (repository *InputRepository) PrepareStoppedFailure(ctx context.Context, view workqueue.ResumeView,
	reason executionplan.BackupFailureReason, at time.Time,
) (workqueue.Change, workexecution.Completion, error) {
	attempt, item, task := view.Attempt.Attempt, view.Work.Item.Item, view.Work.Task.Task
	if !view.CanSettleResult() || !view.ActiveAttempt || view.ExecutorRevision <= 0 ||
		attempt.Direction != workexecution.Forward || attempt.StartedAt == nil || attempt.FinishedAt != nil || !reason.Valid() {
		return workqueue.Change{}, workexecution.Completion{}, conflict("Backup failure no longer owns the active forward execution")
	}
	var change workqueue.Change
	var err error
	settled := false
	outcome := workexecution.Failed
	if task.Stop != nil {
		outcome = task.Stop.Outcome
	} else if item.ForwardRevoked {
		outcome = workexecution.Superseded
	}
	switch item.Operation {
	case workexecution.BackupPrune:
		change, err = repository.PreparePruneFailure(ctx, view, at)
		settled = true
	case workexecution.Restore:
		change, err = repository.PrepareRestoreFailure(ctx, view, at)
	case workexecution.Backup:
		change, err = repository.PrepareCaptureFailure(ctx, view, outcome, reason, at)
	default:
		return workqueue.Change{}, workexecution.Completion{}, conflict("Backup failure operation is unsupported")
	}
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	raw, evidence, err := executionplan.EncodeBackupWorkFailure(attempt, executionplan.BackupWorkFailure{
		AttemptID: attempt.ID, Input: attempt.Input, Reason: reason, Checkpoint: view.Checkpoint.Record, RecordedAt: at,
	})
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	change.Conditions = append(change.Conditions,
		keyvalue.Condition{Key: FailureKey(attempt.ID)},
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: view.Work.Item.Revision},
		keyvalue.Condition{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
		keyvalue.Condition{Key: workqueue.CheckpointKey(attempt.ID), ModRevision: view.Checkpoint.Revision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: FailureKey(attempt.ID), Value: raw})
	result := workexecution.Completion{Outcome: outcome, Evidence: evidence, EffectsSettled: settled}
	if !settled {
		if err := retainFailedExecution(&change, item, attempt, result, raw, at); err != nil {
			keyvalue.ClearMutationValues(change.Mutations)
			return workqueue.Change{}, workexecution.Completion{}, err
		}
	}
	return change, result, nil
}

func (repository *InputRepository) LoadFailure(ctx context.Context, view workqueue.ResumeView) ([]byte, error) {
	attempt := view.Attempt.Attempt
	operation := view.Work.Item.Item.Operation
	if (operation != workexecution.BackupPrune && operation != workexecution.Restore && operation != workexecution.Backup) || attempt.FinishedAt == nil || attempt.Result == nil {
		return nil, conflict("Backup failure has no retained result")
	}
	read, err := repository.readSettlementKeys(ctx, []string{FailureKey(attempt.ID)}, view.Work.ReadRevision)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return nil, conflict("Backup failure evidence is unavailable")
	}
	if _, err := executionplan.DecodeBackupWorkFailure(attempt, read.Values[0].Value, *attempt.Result); err != nil {
		return nil, err
	}
	return append([]byte(nil), read.Values[0].Value...), nil
}
