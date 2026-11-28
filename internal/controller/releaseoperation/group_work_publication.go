package releaseoperation

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/workexecution"

	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasegroup"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// prepareGroupWork compiles the complete native graph after prerequisites
// succeed. Promotion retains the original Task and keeps dispatch closed while
// the publication owner installs every typed source and member descriptor.
func (service *Service) prepareGroupWork(ctx context.Context, scope releasequeries.ReleasePlanningScope,
	group releasegroup.Versioned, candidates []releaseCandidateInput, accepted workqueue.TaskSnapshot,
) (taskdependencies.Prepared, error) {
	if err := service.sealCandidates(ctx, candidates); err != nil {
		return taskdependencies.Prepared{}, err
	}
	projection, err := service.captureDesiredProjection(ctx, scope, false)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	proxies := make([]core.Service, len(candidates))
	for index, candidate := range candidates {
		proxies[index] = candidate.planning.Service.Record.Desired
		proxies[index].Expose = proxies[index].ProxyExposures()
	}
	addresses, err := taskplanning.PrepareServiceProxyAddresses(ctx, service.ledger.Planner, projection, proxies)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	defer addresses.Clear()
	capture, err := service.captureGroupWork(
		ctx,
		scope,
		group,
		projection,
		candidates,
		addresses,
		accepted.Task,
	)
	defer capture.Clear()
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	task, err := capture.Root.Task()
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	authority, err := service.ledger.PrepareGroupReleaseCapture(ctx, accepted, capture, addresses)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	authority.Mutations = keyvalue.CloneMutations(authority.Mutations)
	task.AcceptedIntent = accepted.Task.AcceptedIntent
	task.After = accepted.Task.After
	firstID := capture.Root.Policy.Members[0].ForwardID
	var initial []workexecution.Item
	for _, item := range capture.Root.Graph {
		if item.ID == firstID {
			initial = []workexecution.Item{item}
			break
		}
	}
	return taskdependencies.Prepared{
		Task:      task,
		Graph:     append([]workexecution.Item(nil), capture.Root.Graph...),
		Initial:   initial,
		Authority: authority,
		Staged:    true,
	}, nil
}
