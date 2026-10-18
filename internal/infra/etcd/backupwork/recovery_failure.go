package backupwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareRecoveryFailure preserves the original forward failure and every
// unresolved resource claim. A failed inspection grants neither release nor an
// automatic retry, including when the recovery Agent itself is replaced.
func (repository *InputRepository) PrepareRecoveryFailure(ctx context.Context, view workqueue.ResumeView,
	reason executionplan.BackupFailureReason, at time.Time, interrupted bool,
) (workqueue.Change, workexecution.Completion, error) {
	attempt, item, task := view.Attempt.Attempt, view.Work.Item.Item, view.Work.Task.Task
	if !view.ActiveAttempt || view.CurrentExecutor == interrupted || view.ExecutorRevision <= 0 ||
		(item.Operation != workexecution.BackupPrune && item.Operation != workexecution.Backup && item.Operation != workexecution.Restore) ||
		attempt.Direction != workexecution.Recovery || attempt.FinishedAt != nil ||
		!reason.Valid() || interrupted && reason != executionplan.BackupExecutionInterrupted ||
		!interrupted && attempt.StartedAt == nil {
		return workqueue.Change{}, workexecution.Completion{}, conflict("Backup recovery failure no longer owns the active inspection")
	}
	if item.Operation == workexecution.BackupPrune && (view.Checkpoint.Record != nil || view.Checkpoint.Revision != 0) {
		return workqueue.Change{}, workexecution.Completion{}, conflict("Backup prune recovery has unexpected checkpoint evidence")
	}
	_, change, err := repository.PrepareAdmission(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	source, err := repository.LoadRecovery(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	if err := source.Source.ValidateFor(attempt); err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	raw, evidence, err := executionplan.EncodeBackupWorkFailure(attempt, executionplan.BackupWorkFailure{
		AttemptID: attempt.ID, Input: attempt.Input, Reason: reason, Checkpoint: view.Checkpoint.Record, RecordedAt: at,
	})
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	change.Conditions = append(change.Conditions,
		keyvalue.Condition{Key: RecoveryKey(item.ID), ModRevision: source.Revision},
		keyvalue.Condition{Key: FailureKey(attempt.ID)},
		keyvalue.Condition{Key: RecoveryReportKey(attempt.ID)},
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: view.Work.Item.Revision},
		keyvalue.Condition{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
		keyvalue.Condition{Key: workqueue.CheckpointKey(attempt.ID), ModRevision: view.Checkpoint.Revision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: FailureKey(attempt.ID), Value: raw})
	return change, workexecution.Completion{Outcome: workexecution.Failed, Evidence: evidence, EffectsSettled: false}, nil
}
