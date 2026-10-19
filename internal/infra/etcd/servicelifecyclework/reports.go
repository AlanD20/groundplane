package servicelifecyclework

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (repository *Repository) LoadLifecycleInput(ctx context.Context, view workqueue.View) (Input, error) {
	input, _, err := repository.Load(ctx, view)
	return input, err
}

func (repository *Repository) PrepareLifecycleCheckpoint(
	ctx context.Context,
	view workqueue.ResumeView,
	supplied Input,
	delivery *agentpb.ServiceLifecycleWorkInput,
	progress lifecycleexecution.Progress,
	at time.Time,
) (workqueue.Change, error) {
	captured, guard, err := repository.Load(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !sameCapture(captured, supplied, delivery) {
		return workqueue.Change{}, conflict("Service lifecycle report differs from its captured runtime")
	}
	continuation, continuationGuard, err := repository.LoadContinuation(ctx, view.Work, view.Attempt.Attempt, delivery)
	if err != nil {
		return workqueue.Change{}, err
	}
	if err := progress.ValidateContinuation(view.Attempt.Attempt, delivery, continuation); err != nil {
		return workqueue.Change{}, err
	}
	change, err := PrepareCheckpoint(ctx, repository.store, view, delivery, progress, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, guard, continuationGuard)
	if progress.BeganAt != nil &&
		(view.Checkpoint.Record == nil || progress.Sequence > view.Checkpoint.Record.Sequence) {
		conditions, _, err := services.PrepareLifecycleRuntime(
			ctx,
			repository.store,
			captured.ServiceID,
			captured.Scope.EnvironmentID,
			captured.TaskID,
			captured.Action,
			view.Work.ReadRevision,
			false,
		)
		if err != nil {
			keyvalue.ZeroMutationBytes(change.Mutations)
			return workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, conditions...)
	}
	return change, nil
}

func (repository *Repository) PrepareLifecycleCompletion(
	ctx context.Context,
	view workqueue.ResumeView,
	supplied Input,
	delivery *agentpb.ServiceLifecycleWorkInput,
	at time.Time,
) (workqueue.Change, error) {
	captured, guard, err := repository.Load(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !sameCapture(captured, supplied, delivery) {
		return workqueue.Change{}, conflict("Service lifecycle completion differs from its captured runtime")
	}
	change, err := PrepareCompletionProof(ctx, repository.store, view, delivery, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, guard)
	// A stopped/replaced intent can acknowledge completed effects, but must not
	// confirm or overwrite a newer Service request.
	if view.Work.Task.Task.Stop == nil && !view.Work.Item.Item.ForwardRevoked {
		conditions, mutations, err := services.PrepareLifecycleRuntime(
			ctx,
			repository.store,
			captured.ServiceID,
			captured.Scope.EnvironmentID,
			captured.TaskID,
			captured.Action,
			view.Work.ReadRevision,
			true,
		)
		if err != nil {
			keyvalue.ZeroMutationBytes(change.Mutations)
			return workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, conditions...)
		change.Mutations = append(change.Mutations, mutations...)
	}
	return change, nil
}

func sameCapture(captured, supplied Input, delivery *agentpb.ServiceLifecycleWorkInput) bool {
	want, err := captured.Reference()
	if err != nil {
		return false
	}
	got, err := supplied.Reference()
	return err == nil && want == got && delivery != nil && delivery.CapturedSource != nil &&
		delivery.CapturedSource.InputId == want.ID && hex.EncodeToString(delivery.CapturedSource.Sha256) == want.SHA256 &&
		delivery.ServiceId == captured.ServiceID && delivery.EnvironmentId == captured.Scope.EnvironmentID
}
