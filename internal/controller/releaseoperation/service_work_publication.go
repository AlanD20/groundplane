package releaseoperation

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
)

func (service *Service) prepareServiceWork(ctx context.Context, scope releasequeries.ReleasePlanningScope,
	candidate releaseCandidateInput, accepted workexecution.Task,
) (taskdependencies.Prepared, error) {
	candidates := []releaseCandidateInput{candidate}
	if err := service.sealCandidates(ctx, candidates); err != nil {
		return taskdependencies.Prepared{}, err
	}
	candidate = candidates[0]
	projection, err := service.captureDesiredProjection(ctx, scope)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	proxy := candidate.planning.Service.Record.Desired
	proxy.Expose = proxy.ProxyExposures()
	addresses, err := taskplanning.PrepareServiceProxyAddresses(
		ctx,
		service.ledger.Planner,
		projection,
		[]core.Service{proxy},
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	defer addresses.Clear()
	task, item, request, rollout, err := service.captureServiceWork(
		ctx,
		scope,
		projection,
		candidate,
		addresses,
		accepted,
	)
	defer clear(rollout.CandidateArtifact)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	authority, err := service.ledger.PrepareDirectReleaseCapture(ctx, task, item, request, rollout, addresses)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	authority.Mutations = keyvalue.CloneMutations(authority.Mutations)
	graph, err := request.WorkGraph(task, item)
	if err != nil {
		keyvalue.ClearMutationValues(authority.Mutations)
		return taskdependencies.Prepared{}, err
	}
	return taskdependencies.Prepared{
		Task:      task,
		Graph:     graph,
		Initial:   []workexecution.Item{item},
		Authority: authority,
		Staged:    len(graph) > 1,
	}, nil
}
