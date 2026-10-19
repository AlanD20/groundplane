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
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (repository *Repository) PrepareRestoredRetry(
	ctx context.Context,
	view workqueue.View,
	previous workqueue.AttemptSnapshot,
	delivery *agentpb.ServiceLifecycleWorkInput,
	at, deadline time.Time,
) (workqueue.Change, error) {
	task, item, attempt := view.Task.Task, view.Item.Item, previous.Attempt
	if task.Stop != nil || item.ForwardRevoked || task.CollectionStartedAt != nil ||
		item.Phase != workexecution.Settled ||
		item.Outcome != workexecution.Failed ||
		item.Cleanup != workexecution.CleanupComplete ||
		item.CleanupResult == nil ||
		attempt.Direction != workexecution.Recovery ||
		attempt.Outcome != workexecution.Succeeded ||
		attempt.FinishedAt == nil ||
		attempt.Result == nil ||
		*attempt.Result != *item.CleanupResult ||
		attempt.Number != item.AttemptNumber ||
		attempt.Number == ^uint64(0) {
		return workqueue.Change{}, errs.New(
			errs.KindTaskNotRetryable,
			"service removal Retry requires verified completed restoration",
		)
	}
	input, capture, err := repository.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	saved, err := repository.LoadRecovery(ctx, view, delivery)
	if err != nil {
		return workqueue.Change{}, err
	}
	if err := saved.Capture.ValidateFor(attempt, delivery); err != nil {
		return workqueue.Change{}, err
	}
	// ReadResume is deliberately not used: Retry uses the same fixed storage
	// revision as its Task, item and terminal attempt.
	read, err := repository.read(ctx, []string{workqueue.CheckpointKey(attempt.ID)}, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return workqueue.Change{}, conflict("service removal restoration checkpoint is unavailable")
	}
	checkpoint, err := workqueue.DecodeCheckpoint(read.Values[0].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	resume := workqueue.ResumeView{
		Work:       view,
		Attempt:    previous,
		Checkpoint: workqueue.CheckpointSnapshot{Record: &checkpoint, Revision: read.Values[0].ModRevision},
	}
	progress, progressGuard, err := repository.LoadRecoveryProgress(ctx, resume, delivery, saved.Capture)
	if err != nil {
		return workqueue.Change{}, err
	}
	if progress.Phase != removalexecution.Complete || progress.InFlight || checkpoint.Evidence != *attempt.Result ||
		progress.UpdatedAt.After(*attempt.FinishedAt) {
		return workqueue.Change{}, conflict("service removal restoration did not verify the captured predecessor")
	}
	encoded, err := lifecycleexecution.EncodeContinuation(
		delivery,
		lifecycleexecution.Continuation{Source: saved.Capture.Source, Restored: &attempt},
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	fence, err := repository.prepareRetryFence(ctx, view, input, at)
	if err != nil {
		clear(encoded)
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareRetry(view.Task, view.Item, previous, workexecution.RetryDecision{
		Direction: workexecution.Forward, Result: *attempt.Result, NotBefore: at, Deadline: deadline}, at)
	if err != nil {
		clear(encoded)
		keyvalue.ClearMutationValues(fence.Mutations)
		return workqueue.Change{}, err
	}
	key := servicelifecyclework.ContinuationKey(item.ID, attempt.Number+1)
	change.Conditions = append(change.Conditions, capture, progressGuard,
		keyvalue.Condition{Key: workqueue.CheckpointKey(attempt.ID), ModRevision: read.Values[0].ModRevision},
		keyvalue.Condition{Key: RecoveryKey(item.ID), ModRevision: saved.Revision}, keyvalue.Condition{Key: key})
	change.Conditions = append(change.Conditions, fence.Conditions...)
	change.Mutations = append(change.Mutations, fence.Mutations...)
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: encoded})
	return change, nil
}
