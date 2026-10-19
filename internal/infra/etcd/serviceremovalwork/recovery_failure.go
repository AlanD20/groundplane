package serviceremovalwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/removalexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func RecoveryFailureKey(attemptID string) string {
	return "/v1/execution/service-removal/recovery-failures/" + attemptID
}

// PrepareRecoveryFailure never releases the deletion fence. Unknown restoration
// effects remain protected while queue completion releases the worker slot.
func (repository *Repository) PrepareRecoveryFailure(
	ctx context.Context,
	view workqueue.ResumeView,
	delivery *agentpb.ServiceLifecycleWorkInput,
	reason lifecycleexecution.FailureReason,
	at time.Time,
	interrupted bool,
) (workqueue.Change, workexecution.Completion, error) {
	task, item, attempt := view.Work.Task.Task, view.Work.Item.Item, view.Attempt.Attempt
	if !view.ActiveAttempt || view.CanSettleResult() == interrupted || view.ExecutorRevision <= 0 ||
		attempt.FinishedAt != nil || attempt.Direction != workexecution.Recovery ||
		item.Operation != workexecution.ServiceRemoval || item.Cleanup != workexecution.CleanupRunning ||
		!task.PublicationComplete || task.CollectionStartedAt != nil ||
		interrupted && reason != lifecycleexecution.ExecutionInterrupted {
		return workqueue.Change{}, workexecution.Completion{}, conflict(
			"Service removal recovery failure does not own active cleanup",
		)
	}
	input, captureGuard, err := repository.Load(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	saved, err := repository.LoadRecovery(ctx, view.Work, delivery)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	fence, err := repository.DeletionGuard(ctx, input, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	failure := removalexecution.Failure{AttemptID: attempt.ID, OriginalFailure: saved.Capture.Source.Evidence,
		Reason: reason, RecordedAt: at}
	conditions := []keyvalue.Condition{captureGuard, fence,
		{Key: RecoveryKey(item.ID), ModRevision: saved.Revision},
		{Key: RecoveryFailureKey(attempt.ID)},
		{Key: workqueue.TaskKey(task.ID), ModRevision: view.Work.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: view.Work.Item.Revision},
		{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
		{Key: workqueue.CheckpointKey(attempt.ID), ModRevision: view.Checkpoint.Revision},
		{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision},
	}
	if checkpoint := view.Checkpoint.Record; checkpoint != nil {
		key := RecoveryReportKey(attempt.ID, checkpoint.Sequence)
		read, err := repository.read(ctx, []string{key}, view.Work.ReadRevision)
		if err != nil {
			return workqueue.Change{}, workexecution.Completion{}, err
		}
		defer keyvalue.ClearValues(read.Values)
		if read.Values[0] == nil {
			return workqueue.Change{}, workexecution.Completion{}, conflict(
				"Recovery failure has no saved checkpoint evidence",
			)
		}
		progress, err := removalexecution.DecodeProgress(attempt, delivery, saved.Capture, read.Values[0].Value)
		if err != nil {
			return workqueue.Change{}, workexecution.Completion{}, err
		}
		expected, err := progress.Checkpoint(attempt, delivery, saved.Capture)
		if err != nil || expected != *checkpoint {
			return workqueue.Change{}, workexecution.Completion{}, conflict(
				"Recovery failure differs from its saved checkpoint",
			)
		}
		failure.Progress = &progress
		conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision})
	}
	raw, evidence, err := removalexecution.EncodeFailure(attempt, delivery, saved.Capture, failure)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	return workqueue.Change{Conditions: conditions, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: RecoveryFailureKey(attempt.ID), Value: raw},
	}}, workexecution.Completion{Outcome: workexecution.Failed, Evidence: evidence, EffectsSettled: false}, nil
}
