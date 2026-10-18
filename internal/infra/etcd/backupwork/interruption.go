package backupwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareInterrupted releases only assignments without a start grant. Started work
// retains claims, selections and its Environment lock until inspection proves
// that the former executor cannot continue.
func (repository *InputRepository) PrepareInterrupted(ctx context.Context, view workqueue.ResumeView,
	at time.Time,
) (workqueue.Change, workexecution.Completion, error) {
	attempt, item, task := view.Attempt.Attempt, view.Work.Item.Item, view.Work.Task.Task
	if view.CurrentExecutor || !view.ActiveAttempt || view.ExecutorRevision <= 0 ||
		(item.Operation != workexecution.BackupPrune && item.Operation != workexecution.Backup && item.Operation != workexecution.Restore) ||
		attempt.Direction != workexecution.Forward ||
		attempt.FinishedAt != nil {
		return workqueue.Change{}, workexecution.Completion{}, conflict("Backup interruption requires a replaced attempt")
	}
	var change workqueue.Change
	var err error
	outcome := workexecution.Failed
	if task.Stop != nil {
		outcome = task.Stop.Outcome
	} else if item.ForwardRevoked {
		outcome = workexecution.Superseded
	}
	settled := attempt.StartedAt == nil
	if settled {
		if view.Checkpoint.Record != nil || view.Checkpoint.Revision != 0 {
			return workqueue.Change{}, workexecution.Completion{}, conflict("Unstarted Backup prune has unexpected execution evidence")
		}
		change, err = repository.prepareUnexecutedResources(ctx, view.Work, outcome, executionplan.BackupExecutionInterrupted, at)
	} else {
		_, change, err = repository.PrepareAdmission(ctx, view.Work)
	}
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	raw, evidence, err := executionplan.EncodeBackupWorkFailure(attempt, executionplan.BackupWorkFailure{
		AttemptID: attempt.ID, Input: attempt.Input, Reason: executionplan.BackupExecutionInterrupted,
		Checkpoint: view.Checkpoint.Record, RecordedAt: at,
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
