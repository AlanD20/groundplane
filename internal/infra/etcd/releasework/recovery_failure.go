package releasework

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareRecoveryFailure accounts for a stopped cleanup executor. Even failure
// before its first checkpoint retains claims: the original effects still exist.
func PrepareRecoveryFailure(ctx context.Context, store Reader, view workqueue.ResumeView, input releaseexecution.RecoveryInput,
	reason releaseexecution.FailureReason, at time.Time,
) (workqueue.Change, workexecution.Completion, error) {
	if !view.CanSettleResult() {
		return workqueue.Change{}, workexecution.Completion{}, invalid()
	}
	return prepareRecoveryFailure(ctx, store, view, input, reason, at)
}

// PrepareInterruptedRecoveryFailure freezes a replaced attempt, retaining all
// resource claims. It grants no cleanup and asserts no helper termination.
func PrepareInterruptedRecoveryFailure(ctx context.Context, store Reader, view workqueue.ResumeView, input releaseexecution.RecoveryInput, at time.Time) (workqueue.Change, workexecution.Completion, error) {
	if view.CanSettleResult() || view.ExecutorRevision <= 0 {
		return workqueue.Change{}, workexecution.Completion{}, invalid()
	}
	return prepareRecoveryFailure(ctx, store, view, input, releaseexecution.ExecutionInterrupted, at)
}

func prepareRecoveryFailure(ctx context.Context, store Reader, view workqueue.ResumeView, input releaseexecution.RecoveryInput,
	reason releaseexecution.FailureReason, at time.Time,
) (workqueue.Change, workexecution.Completion, error) {
	if ctx == nil || store == nil || input.Validate() != nil || !view.ActiveAttempt ||
		!workexecution.SameAssignment(view.Attempt.Attempt, input.Input.Attempt) ||
		(input.Group == nil && view.Work.Item.Item.Operation != workexecution.Release ||
			input.Group != nil && view.Work.Item.Item.Operation != workexecution.ReleaseRestoration) {
		return workqueue.Change{}, workexecution.Completion{}, invalid()
	}
	progress, err := LoadRecovery(ctx, store, input, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	if progress.Record != nil {
		if progress.Record.Report.Progress.Phase == releaseexecution.RecoveryComplete || view.Checkpoint.Record == nil ||
			progress.Revision != view.Checkpoint.Revision {
			return workqueue.Change{}, workexecution.Completion{}, invalid()
		}
		checkpoint, err := progress.Record.Report.Checkpoint(input)
		if err != nil || !releaseexecution.EqualCheckpoint(checkpoint, *view.Checkpoint.Record) {
			return workqueue.Change{}, workexecution.Completion{}, invalid()
		}
	} else if view.Checkpoint.Record != nil || view.Checkpoint.Revision != 0 {
		return workqueue.Change{}, workexecution.Completion{}, invalid()
	}
	attempt := input.Input.Attempt
	failure := releaseexecution.Failure{AttemptID: attempt.ID, Input: attempt.Input, Reason: reason,
		Checkpoint: view.Checkpoint.Record, RecordedAt: at}
	raw, reference, err := releaseexecution.EncodeFailure(attempt, failure)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	change := workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: FailureKey(reference.ID)},
		{Key: workqueue.TaskKey(attempt.TaskID), ModRevision: view.Work.Task.Revision},
		{Key: workqueue.ItemKey(attempt.WorkItemID), ModRevision: view.Work.Item.Revision},
		{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
		{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision},
		{Key: workqueue.CheckpointKey(attempt.ID), ModRevision: view.Checkpoint.Revision},
		{Key: RecoveryKey(attempt.ID), ModRevision: progress.Revision},
	}, Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: FailureKey(reference.ID), Value: raw}}}
	return change, workexecution.Completion{Outcome: workexecution.Failed, Evidence: reference, EffectsSettled: false}, nil
}
