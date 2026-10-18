package servicelifecyclework

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// LoadRecoverySource reads the failed forward attempt's immutable evidence at
// the caller's snapshot. The returned comparisons must join recovery publication.
func (repository *Repository) LoadRecoverySource(ctx context.Context, view workqueue.View, previous workqueue.AttemptSnapshot,
	input *agentpb.ServiceLifecycleWorkInput,
) (lifecycleexecution.Recovery, []keyvalue.Condition, error) {
	item, attempt := view.Item.Item, previous.Attempt
	if item.Operation != workexecution.ServiceLifecycle || previous.Revision <= 0 || item.Input == nil || item.Result == nil ||
		attempt.TaskID != view.Task.Task.ID || attempt.WorkItemID != item.ID || attempt.Direction != workexecution.Forward ||
		attempt.FinishedAt == nil || attempt.Result == nil || *attempt.Result != *item.Result || attempt.Input != *item.Input ||
		attempt.Executor.HostID != item.Placement.HostID || attempt.Number != item.AttemptNumber {
		return lifecycleexecution.Recovery{}, nil, conflict("Service lifecycle recovery lacks its failed forward execution")
	}
	captured, guard, err := repository.Load(ctx, view)
	if err != nil {
		return lifecycleexecution.Recovery{}, nil, err
	}
	if !sameCapture(captured, captured, input) {
		return lifecycleexecution.Recovery{}, nil, conflict("Service lifecycle recovery differs from its captured input")
	}
	failureKey := FailureKey(attempt.ID)
	read, err := repository.read(ctx, []string{failureKey}, view.ReadRevision)
	if err != nil {
		return lifecycleexecution.Recovery{}, nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return lifecycleexecution.Recovery{}, nil, conflict("Service lifecycle recovery failure evidence is missing")
	}
	failure, err := lifecycleexecution.DecodeFailure(attempt, input, read.Values[0].Value, *attempt.Result)
	if err != nil {
		return lifecycleexecution.Recovery{}, nil, err
	}
	source := lifecycleexecution.Recovery{Forward: attempt, Failure: failure, Evidence: *attempt.Result}
	guards := []keyvalue.Condition{guard,
		{Key: workqueue.AttemptKey(attempt.ID), ModRevision: previous.Revision},
		{Key: failureKey, ModRevision: read.Values[0].ModRevision},
	}
	if failure.Checkpoint != nil {
		key := ReportKey(attempt.ID, failure.Checkpoint.Sequence)
		progressRead, err := repository.read(ctx, []string{key}, view.ReadRevision)
		if err != nil {
			return lifecycleexecution.Recovery{}, nil, err
		}
		defer keyvalue.ClearValues(progressRead.Values)
		if progressRead.Values[0] == nil {
			return lifecycleexecution.Recovery{}, nil, conflict("Service lifecycle recovery checkpoint report is missing")
		}
		progress, err := lifecycleexecution.DecodeProgress(attempt, input, progressRead.Values[0].Value)
		if err != nil {
			return lifecycleexecution.Recovery{}, nil, err
		}
		source.Progress = &progress
		guards = append(guards, keyvalue.Condition{Key: key, ModRevision: progressRead.Values[0].ModRevision})
	}
	if err := source.Validate(input); err != nil {
		return lifecycleexecution.Recovery{}, nil, err
	}
	return source, guards, nil
}
