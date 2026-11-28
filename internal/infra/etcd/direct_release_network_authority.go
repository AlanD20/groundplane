package etcd

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// DirectReleaseNetworkConditions guards a new effect on a remote Deploy
// prerequisite. Replays and recovery of admitted effects use the exact native
// Network input and do not acquire new Deploy authority.
func (ledger *ReleaseLedger) DirectReleaseNetworkConditions(ctx context.Context, work workqueue.View,
	executor workexecution.Identity,
) ([]keyvalue.Condition, error) {
	task, item := work.Task.Task, work.Item.Item
	if task.Action != workexecution.ActionDeploy || ids.Validate(ids.KindService, task.TargetID) != nil {
		return nil, nil
	}
	request, preparation, err := ledger.ReadDirectReleaseTaskPreparation(ctx, work.Task, work.ReadRevision)
	if err != nil || request.Remote == nil {
		return nil, err
	}
	if item.Operation != workexecution.Network || item.TaskID != task.ID || item.Input == nil ||
		item.Placement != request.Configuration.Placement || executor != request.Remote.Executor ||
		!slices.ContainsFunc(request.Networks, func(network DirectReleaseNetwork) bool {
			return network.WorkItemID == item.ID && network.NetworkID == item.TargetID && network.Input == *item.Input
		}) {
		return nil, errs.New(
			errs.KindStateConflict,
			"Network work differs from its captured remote Deploy prerequisite",
		)
	}
	desired, err := ledger.directReleaseSourceConditions(
		ctx,
		task.Scope,
		request.Source,
		task.TargetID,
		work.ReadRevision,
	)
	if err != nil {
		return nil, err
	}
	remote, err := ledger.DirectReleaseRemoteConditions(ctx, request.Remote, work.ReadRevision)
	if err != nil {
		return nil, err
	}
	return append(append(desired, remote...), preparation), nil
}

// remoteReleaseBridgeConditions ties the workload to successful observations of
// its own sealed bridge inputs on the selected Agent generation.
func (ledger *ReleaseLedger) remoteReleaseBridgeConditions(ctx context.Context, request DirectReleasePreparation,
	revision int64,
) ([]keyvalue.Condition, error) {
	if request.Remote == nil {
		return nil, nil
	}
	repository, err := networkwork.NewInputRepository(ledger.store)
	if err != nil {
		return nil, err
	}
	var conditions []keyvalue.Condition
	for _, network := range request.Networks {
		observation, guards, err := repository.ReadRemoteApplied(
			ctx, request.Remote.Executor, request.Intent.EnvironmentID, network.NetworkID, revision,
		)
		if err != nil {
			return nil, err
		}
		if observation.Revision <= 0 || observation.Record.Input != network.Input ||
			observation.Record.TaskID != request.Configuration.TaskID || observation.Record.WorkItemID != network.WorkItemID {
			return nil, errs.New(errs.KindStateConflict, "Remote Deploy bridge has no matching ready observation")
		}
		conditions = append(conditions, guards...)
	}
	return conditions, nil
}
