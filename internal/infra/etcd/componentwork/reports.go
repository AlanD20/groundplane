package componentwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (repository *InputRepository) PrepareComponentWorkCheckpoint(ctx context.Context, view workqueue.ResumeView,
	input *agentpb.ComponentWorkInput, progress componentexecution.Progress, at time.Time,
) (workqueue.Change, error) {
	captured, _, guards, err := repository.Load(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !proto.Equal(input, captured) {
		return workqueue.Change{}, conflict("Component report differs from its published input")
	}
	if progress.InFlight && (view.Checkpoint.Record == nil || progress.Sequence > view.Checkpoint.Record.Sequence) {
		current, desired, err := repository.ReadDesiredMatch(ctx, view.Work)
		if err != nil {
			return workqueue.Change{}, err
		}
		if !current {
			return workqueue.Change{}, conflict("A newer Blueprint replaced this Component action; another stage cannot start")
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

// Completion proves host effects. The Component owner still joins resource
// settlement and queue completion; a final report alone is not applied state.
func (repository *InputRepository) PrepareCapturedCompletion(ctx context.Context, view workqueue.ResumeView,
	input *agentpb.ComponentWorkInput, at time.Time,
) (Capture, workqueue.Change, error) {
	captured, capture, guards, err := repository.Load(ctx, view.Work)
	if err != nil {
		return Capture{}, workqueue.Change{}, err
	}
	if !proto.Equal(input, captured) {
		return Capture{}, workqueue.Change{}, conflict("Component completion differs from its published input")
	}
	change, err := PrepareCompletionProof(ctx, repository.store, view, captured, at)
	if err != nil {
		return Capture{}, workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, guards...)
	return capture, change, nil
}

// ReadDesiredMatch fences new effects, not receipts of already authorized work.
func (repository *InputRepository) ReadDesiredMatch(ctx context.Context, view workqueue.View) (bool, []keyvalue.Condition, error) {
	key := blueprints.EnvironmentBlueprintHeadKey(view.Task.Task.Scope.EnvironmentID)
	read, err := repository.read(ctx, []string{key}, view.ReadRevision)
	if err != nil {
		return false, nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	guards := []keyvalue.Condition{{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[0])}}
	if read.Values[0] == nil {
		return false, guards, nil
	}
	current, err := idempotency.DecodeTaskReference(read.Values[0].Value)
	if err != nil {
		return false, nil, err
	}
	return current == view.Task.Task.ID, guards, nil
}
