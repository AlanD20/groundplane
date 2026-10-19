package serviceremovalwork

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/servicelifecyclework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (repository *Repository) LoadContinuation(ctx context.Context, view workqueue.View, attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
) (*lifecycleexecution.Continuation, keyvalue.Condition, error) {
	return servicelifecyclework.LoadContinuation(ctx, repository.store, view, attempt, input)
}

// PrepareCheckpoint acknowledges exact per-member evidence. Before granting a
// new effect it rechecks deletion ownership; completed observations may still be
// recorded after revocation so recovery knows what actually happened.
func (repository *Repository) PrepareCheckpoint(ctx context.Context, view workqueue.ResumeView,
	delivery *agentpb.ServiceLifecycleWorkInput, progress lifecycleexecution.Progress, at time.Time,
) (workqueue.Change, error) {
	input, guard, err := repository.Load(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !matchesDelivery(input, delivery) {
		return workqueue.Change{}, conflict("Service removal report differs from its captured runtime")
	}
	continuation, continuationGuard, err := repository.LoadContinuation(ctx, view.Work, view.Attempt.Attempt, delivery)
	if err != nil {
		return workqueue.Change{}, err
	}
	if err := progress.ValidateContinuation(view.Attempt.Attempt, delivery, continuation); err != nil {
		return workqueue.Change{}, err
	}
	change, err := servicelifecyclework.PrepareCheckpoint(ctx, repository.store, view, delivery, progress, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, guard, continuationGuard)
	if progress.BeganAt != nil &&
		(view.Checkpoint.Record == nil || progress.Sequence > view.Checkpoint.Record.Sequence) {
		boundary, err := repository.PrepareForwardBoundary(ctx, view.Work, input)
		if err != nil {
			keyvalue.ClearMutationValues(change.Mutations)
			return workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, boundary.Conditions...)
	}
	return change, nil
}

// PrepareHostProof proves all captured members absent. The caller MUST join
// definition finalization or the stopped-operation settlement before completing
// work. A helper exit or this proof alone cannot remove resource records.
func (repository *Repository) PrepareHostProof(ctx context.Context, view workqueue.ResumeView,
	delivery *agentpb.ServiceLifecycleWorkInput, at time.Time,
) (Input, workqueue.Change, error) {
	input, guard, err := repository.Load(ctx, view.Work)
	if err != nil {
		return Input{}, workqueue.Change{}, err
	}
	if !matchesDelivery(input, delivery) {
		return Input{}, workqueue.Change{}, conflict("Service removal completion differs from its captured runtime")
	}
	change, err := servicelifecyclework.PrepareCompletionProof(ctx, repository.store, view, delivery, at)
	if err != nil {
		return Input{}, workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, guard)
	return input, change, nil
}

func matchesDelivery(input Input, delivery *agentpb.ServiceLifecycleWorkInput) bool {
	ref, err := input.Reference()
	return err == nil && input.Runtime != nil && delivery != nil && delivery.CapturedSource != nil &&
		delivery.CapturedSource.InputId == ref.ID && hex.EncodeToString(delivery.CapturedSource.Sha256) == ref.SHA256 &&
		delivery.ServiceId == input.ServiceID && delivery.EnvironmentId == input.Scope.EnvironmentID &&
		delivery.Action == agentpb.ServiceLifecycleAction_SERVICE_LIFECYCLE_ACTION_DESTROY && delivery.Hook == nil
}
