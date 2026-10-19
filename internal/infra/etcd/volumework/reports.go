package volumework

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (repository *InputRepository) LoadVolumeWorkInput(
	ctx context.Context,
	view workqueue.View,
) (*agentpb.VolumeWorkInput, error) {
	input, _, err := repository.Load(ctx, view)
	return input, err
}

func (repository *InputRepository) PrepareVolumeWorkCheckpoint(ctx context.Context, view workqueue.ResumeView,
	input *agentpb.VolumeWorkInput, progress volumeexecution.Progress, at time.Time,
) (workqueue.Change, error) {
	captured, guards, err := repository.Load(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !proto.Equal(input, captured) {
		return workqueue.Change{}, conflict("Volume report differs from its published input")
	}
	// Replays and confirmations account for already-authorized effects. Only a
	// new stage needs fresh desired-state authority; never reject its receipt
	// merely because a later action changed the Environment.
	if progress.InFlight && (view.Checkpoint.Record == nil || progress.Sequence > view.Checkpoint.Record.Sequence) {
		claims, err := repository.transactions.ReadRunningClaimGuards(ctx, view)
		if err != nil {
			return workqueue.Change{}, err
		}
		guards = append(guards, claims...)
		current, desired, err := repository.ReadDesiredMatch(ctx, view.Work)
		if err != nil {
			return workqueue.Change{}, err
		}
		if !current {
			return workqueue.Change{}, conflict(
				"Environment configuration changed; this Volume action cannot start another stage",
			)
		}
		guards = append(guards, desired...)
	}
	change, err := PrepareCheckpoint(ctx, repository.store, view, captured, progress, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, guards...)
	return change, nil
}

// PrepareCapturedCompletion supplies only input and host-effect proof. The
// Volume resource owner must add applied-state writes before queue settlement.
func (repository *InputRepository) PrepareCapturedCompletion(ctx context.Context, view workqueue.ResumeView,
	input *agentpb.VolumeWorkInput, at time.Time,
) (workqueue.Change, error) {
	captured, guards, err := repository.Load(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !proto.Equal(input, captured) {
		return workqueue.Change{}, conflict("Volume completion differs from its published input")
	}
	change, err := PrepareCompletionProof(ctx, repository.store, view, captured, at)
	if err != nil {
		keyvalue.ZeroMutationBytes(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, guards...)
	return change, nil
}
