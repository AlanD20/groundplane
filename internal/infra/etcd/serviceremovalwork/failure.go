package serviceremovalwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/removalexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/servicelifecyclework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func RecoveryKey(workID string) string { return "/v1/execution/service-removal/recovery/" + workID }

// PrepareFailure records the original failure and the captured boundary in the
// same transaction as attempt completion. Any removed member or in-flight effect
// keeps the deletion fence and claims until operation-owned recovery settles it.
func (repository *Repository) PrepareFailure(ctx context.Context, view workqueue.ResumeView,
	delivery *agentpb.ServiceLifecycleWorkInput, reason lifecycleexecution.FailureReason, at time.Time, interrupted bool,
) (workqueue.Change, workexecution.Completion, error) {
	task, item, attempt := view.Work.Task.Task, view.Work.Item.Item, view.Attempt.Attempt
	if !view.ActiveAttempt || view.CanSettleResult() == interrupted || view.ExecutorRevision <= 0 ||
		attempt.FinishedAt != nil || attempt.Direction != workexecution.Forward ||
		item.Operation != workexecution.ServiceRemoval || !task.PublicationComplete || task.CollectionStartedAt != nil ||
		interrupted && reason != lifecycleexecution.ExecutionInterrupted {
		return workqueue.Change{}, workexecution.Completion{}, conflict("Service removal failure does not own active execution")
	}
	input, capture, err := repository.Load(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	if !matchesDelivery(input, delivery) {
		return workqueue.Change{}, workexecution.Completion{}, conflict("Service removal failure differs from its captured runtime")
	}
	continuation, continuationGuard, err := repository.LoadContinuation(ctx, view.Work, attempt, delivery)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	inherited := 0
	if continuation != nil {
		completed, err := continuation.Completed(delivery)
		if err != nil {
			return workqueue.Change{}, workexecution.Completion{}, err
		}
		inherited = len(completed)
	}
	progress, progressGuard, err := repository.LoadProgress(ctx, view, delivery)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	if progress != nil && progress.BeganAt == nil && len(progress.Completed) == len(delivery.Members) && task.Stop == nil && !item.ForwardRevoked {
		return workqueue.Change{}, workexecution.Completion{}, conflict("completed service removal must settle its saved result")
	}
	// No stage can execute without its durable before-effect checkpoint grant.
	settled := inherited == 0 && (progress == nil || progress.BeganAt == nil && len(progress.Completed) == 0)
	failure := lifecycleexecution.Failure{AttemptID: attempt.ID, Input: attempt.Input, Reason: reason,
		Checkpoint: view.Checkpoint.Record, RecordedAt: at}
	raw, evidence, err := lifecycleexecution.EncodeFailure(attempt, delivery, failure)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	outcome := workexecution.Failed
	if task.Stop != nil {
		outcome = task.Stop.Outcome
	} else if item.ForwardRevoked {
		outcome = workexecution.Superseded
	}
	completion := workexecution.Completion{Outcome: outcome, Evidence: evidence, EffectsSettled: settled}
	change := workqueue.Change{Conditions: []keyvalue.Condition{capture, continuationGuard,
		{Key: servicelifecyclework.FailureKey(attempt.ID)},
		{Key: workqueue.TaskKey(task.ID), ModRevision: view.Work.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: view.Work.Item.Revision},
		{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
		{Key: workqueue.CheckpointKey(attempt.ID), ModRevision: view.Checkpoint.Revision},
		{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision},
	}, Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: servicelifecyclework.FailureKey(attempt.ID), Value: raw}}}
	if progress != nil {
		change.Conditions = append(change.Conditions, progressGuard)
	}
	if settled {
		fence, err := repository.DeletionGuard(ctx, input, view.Work.ReadRevision)
		if err != nil {
			keyvalue.ClearMutationValues(change.Mutations)
			return workqueue.Change{}, workexecution.Completion{}, err
		}
		change.Conditions = append(change.Conditions, fence)
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: fence.Key})
		return change, completion, nil
	}
	_, finished, err := workexecution.Finish(item, attempt, completion, at)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	source := lifecycleexecution.Recovery{Forward: finished, Failure: failure, Evidence: evidence, Progress: progress}
	encoded, err := EncodeRecoveryCapture(delivery, removalexecution.RecoveryCapture{Source: source, Continuation: continuation})
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	key := RecoveryKey(item.ID)
	previous, err := repository.read(ctx, []string{key}, view.Work.ReadRevision)
	if err != nil {
		clear(encoded)
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	defer keyvalue.ClearValues(previous.Values)
	guard := keyvalue.Condition{Key: key}
	if previous.Values[0] != nil {
		guard.ModRevision = previous.Values[0].ModRevision
	}
	change.Conditions = append(change.Conditions, guard)
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: encoded})
	return change, completion, nil
}

func (repository *Repository) LoadProgress(ctx context.Context, view workqueue.ResumeView,
	delivery *agentpb.ServiceLifecycleWorkInput,
) (*lifecycleexecution.Progress, keyvalue.Condition, error) {
	checkpoint := view.Checkpoint.Record
	if checkpoint == nil {
		return nil, keyvalue.Condition{}, nil
	}
	key := servicelifecyclework.ReportKey(view.Attempt.Attempt.ID, checkpoint.Sequence)
	read, err := repository.read(ctx, []string{key}, view.Work.ReadRevision)
	if err != nil {
		return nil, keyvalue.Condition{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return nil, keyvalue.Condition{}, conflict("Service removal failure has no saved checkpoint report")
	}
	progress, err := lifecycleexecution.DecodeProgress(view.Attempt.Attempt, delivery, read.Values[0].Value)
	if err != nil {
		return nil, keyvalue.Condition{}, err
	}
	expected, err := progress.Checkpoint(view.Attempt.Attempt, delivery)
	if err != nil || expected != *checkpoint {
		return nil, keyvalue.Condition{}, conflict("service removal report differs from its acknowledged checkpoint")
	}
	return &progress, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision}, nil
}
