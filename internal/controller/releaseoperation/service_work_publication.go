package releaseoperation

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/hostplacement"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) prepareServiceWork(ctx context.Context, scope releasequeries.ReleasePlanningScope,
	candidate releaseCandidateInput, accepted workexecution.Task,
) (taskdependencies.Prepared, error) {
	return service.prepareServiceWorkForHost(ctx, scope, candidate, accepted, nil)
}

type remoteServiceSelection struct {
	host         hostplacement.Snapshot
	localImageID string
}

func (service *Service) prepareServiceWorkForHost(ctx context.Context, scope releasequeries.ReleasePlanningScope,
	candidate releaseCandidateInput, accepted workexecution.Task, remote *remoteServiceSelection,
) (taskdependencies.Prepared, error) {
	candidates := []releaseCandidateInput{candidate}
	var selected *hostplacement.Snapshot
	if remote != nil {
		selected = &remote.host
	}
	if err := service.sealCandidatesOnHost(ctx, candidates, selected); err != nil {
		return taskdependencies.Prepared{}, err
	}
	candidate = candidates[0]
	if remote != nil && candidate.workload.LocalImageID != remote.localImageID {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindStateConflict,
			"selected host image differs from its successful Fetch receipt",
		)
	}
	projection, err := service.captureDesiredProjection(ctx, scope, remote != nil)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	var placement workexecution.Placement
	if remote == nil {
		placement, err = service.ledger.ResolveReleaseHost(ctx, scope.ReadRevision)
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
	} else {
		placement = remote.host.Placement
		if err := validateRemoteDeploymentProjection(scope, projection, candidate); err != nil {
			return taskdependencies.Prepared{}, err
		}
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
		placement,
	)
	defer clear(rollout.CandidateArtifact)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	var authority workqueue.Change
	if remote == nil {
		authority, err = service.ledger.PrepareDirectReleaseCapture(ctx, task, item, request, rollout, addresses)
	} else {
		authority, err = service.ledger.PrepareRemoteDirectReleaseCapture(ctx, task, item, request, rollout, addresses, remote.host.Identity)
	}
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if candidate.promotionSource != "" {
		key := releases.ReleasePromotionSourceKey(request.Intent.ID)
		authority.Conditions = append(authority.Conditions, candidate.promotionGuards...)
		authority.Conditions = append(authority.Conditions, keyvalue.Condition{Key: key})
		authority.Mutations = append(authority.Mutations, keyvalue.Mutation{
			Type: keyvalue.MutationPut, Key: key, Value: []byte(candidate.promotionSource),
		})
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
