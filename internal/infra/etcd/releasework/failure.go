package releasework

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func FailureKey(evidenceID string) string { return "/v1/execution/release-failures/" + evidenceID }

// PrepareFailure records a stopped local executor, not proof that Docker effects
// were undone. Any granted effect retains conflict claims for operation recovery;
// the shared completion transaction can still release the worker slot.
func PrepareFailure(ctx context.Context, store Reader, view workqueue.ResumeView, input releaseexecution.Input, reason releaseexecution.FailureReason, at time.Time) (workqueue.Change, workexecution.Completion, error) {
	if ctx == nil || store == nil || !view.ActiveAttempt || !view.CanSettleResult() ||
		!workexecution.SameAssignment(view.Attempt.Attempt, input.Attempt) || input.Attempt.Direction != workexecution.Forward ||
		input.Attempt.FinishedAt != nil || view.Work.Item.Item.Operation != workexecution.Release {
		return workqueue.Change{}, workexecution.Completion{}, invalid()
	}
	progress, err := Load(ctx, store, input, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	if progress.Record != nil && progress.Record.Progress.Phase == releaseexecution.Complete {
		return workqueue.Change{}, workexecution.Completion{}, invalid()
	}
	checkpoint := view.Checkpoint.Record
	if progress.Record != nil && checkpoint == nil {
		return workqueue.Change{}, workexecution.Completion{}, invalid()
	}
	failure := releaseexecution.Failure{AttemptID: input.Attempt.ID, Input: input.Attempt.Input, Reason: reason,
		Checkpoint: checkpoint, RecordedAt: at}
	raw, reference, err := releaseexecution.EncodeFailure(input.Attempt, failure)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	outcome := workexecution.Failed
	if view.Work.Task.Task.Stop != nil {
		outcome = view.Work.Task.Task.Stop.Outcome
	} else if view.Work.Item.Item.ForwardRevoked {
		outcome = workexecution.Superseded
	}
	change := workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: FailureKey(reference.ID)},
		{Key: workqueue.TaskKey(input.Attempt.TaskID), ModRevision: view.Work.Task.Revision},
		{Key: workqueue.ItemKey(input.Attempt.WorkItemID), ModRevision: view.Work.Item.Revision},
		{Key: workqueue.AttemptKey(input.Attempt.ID), ModRevision: view.Attempt.Revision},
		{Key: workqueue.ExecutorKey(input.Attempt.Executor), ModRevision: view.ExecutorRevision},
		{Key: workqueue.CheckpointKey(input.Attempt.ID), ModRevision: view.Checkpoint.Revision},
		{Key: ProgressKey(input.Attempt.ID), ModRevision: progress.Revision},
	}, Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: FailureKey(reference.ID), Value: raw}}}
	return change, workexecution.Completion{Outcome: outcome, Evidence: reference, EffectsSettled: checkpoint == nil}, nil
}
