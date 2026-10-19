package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Both placements retain the same Service rollout inputs before the proxy
// selection becomes executable or is settled as disabled metadata.
func (service *Service) prepareComponentProxyRollouts(
	ctx context.Context,
	work workqueue.View,
	services []workqueue.View,
	snapshot blueprintunits.Snapshot,
	source authoredRuntimeProjectionSource,
	candidate componentrecord.Record,
	authority componentplanning.FrozenProjectionAuthority,
) (*componentplanning.ProxyServicePreflight, workqueue.Change, error) {
	parent := work.Task.Task
	projection, err := service.compileAuthoredProxyCandidateInput(ctx, parent, source, snapshot,
		authoredPendingProxy{record: candidate})
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	var stored keyvalue.Versioned[componentplanning.ProxyCandidateInputReference]
	if work.Item.Item.Placement.Executor == workexecution.Controller {
		stored, err = service.repository.StageBlueprintDisabledProxyCandidateInput(ctx, work,
			candidate, projection, source.desired.Revision)
	} else {
		stored, err = service.repository.StageBlueprintProxyCandidateInput(ctx, work,
			projection, source.desired.Revision)
	}
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	preflight, found := authority.ProxyServiceCapture()
	if !found {
		return nil, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"proxy change lost its captured Service membership",
		)
	}
	var selected []workqueue.View
	for _, id := range authority.ProxyReplacementServices() {
		found := false
		for _, candidate := range services {
			if candidate.Item.Item.TargetID == id {
				selected = append(selected, candidate)
				found = true
				break
			}
		}
		if !found {
			return nil, workqueue.Change{}, errs.New(
				errs.KindStateConflict,
				"Proxy replacement is absent from captured Service work",
			)
		}
	}
	rollouts, err := service.prepareAuthoredProxyServiceRollouts(ctx, work, selected,
		projection, stored.Record, preflight)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	bound, change, err := componentplanning.BindProxyWorkRollouts(work, preflight, stored, rollouts)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	held, sources, err := service.blueprintReleases.PrepareProxySourceHold(ctx, work, rollouts, *bound)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, sources.Conditions...)
	change.Mutations = append(change.Mutations, sources.Mutations...)
	return held, change, nil
}
