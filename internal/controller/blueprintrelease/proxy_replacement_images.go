package blueprintrelease

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	"github.com/AlanD20/groundplane/internal/controller/workloadseal"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareProxyReplacementImages resolves all affected workloads before a
// router can change. Later children reverify these local IDs; they never resolve
// the authored tags again to select different content for the same switch.
func (service *Service) PrepareProxyReplacementImages(ctx context.Context,
	desired []core.Service, kind core.ComponentKind, serviceIDs []string,
) ([]componentplanning.ProxyReplacementImage, error) {
	if ctx == nil || service == nil || !kind.IsEnvironmentProxy() {
		return nil, errs.New(errs.KindValidationFailed, "proxy replacement image request is invalid")
	}
	if len(serviceIDs) == 0 {
		return nil, nil
	}
	requested := make(map[string]core.Service, len(desired))
	for _, item := range desired {
		requested[item.ID] = item
	}
	selections := make([]workloadseal.Selection, len(serviceIDs))
	result := make([]componentplanning.ProxyReplacementImage, len(serviceIDs))
	for index, serviceID := range serviceIDs {
		item, found := requested[serviceID]
		if !found || item.Adapter != "" || item.Replicas < 1 || uint64(item.Replicas) > uint64(^uint32(0)) ||
			index > 0 && serviceIDs[index-1] >= serviceID {
			return nil, errs.New(errs.KindValidationFailed, "proxy replacement Service selection is invalid")
		}
		ports, err := domain.ProxyPorts(item.ProxyExposures())
		if err != nil {
			return nil, err
		}
		strategy, err := domain.DeploymentStrategy("", string(item.Strategy))
		if err != nil {
			return nil, err
		}
		render := releaserender.ReleaseRenderInput{ServiceID: serviceID, ServiceName: item.Name,
			Strategy: strategy, ProxyPorts: ports, ProxyImplementation: serviceproxy.Implementation(kind)}
		if err := service.plans.PrepareReleaseProxyImage(&render, nil); err != nil {
			return nil, err
		}
		if render.ProxyImage == nil {
			return nil, errs.New(errs.KindStateConflict, "proxy replacement has no compiled proxy image")
		}
		result[index] = componentplanning.ProxyReplacementImage{ServiceID: serviceID, Proxy: *render.ProxyImage}
		selections[index] = workloadseal.Selection{Requested: &workloadseal.Requested{
			Reference: item.Image, Replicas: uint32(item.Replicas)}}
	}
	agent, err := service.agents.GetSingleton(ctx)
	if err != nil {
		return nil, err
	}
	seals, err := workloadseal.Resolve(ctx, service.images, agent.Record.ID, selections)
	if err != nil {
		return nil, err
	}
	for index, seal := range seals {
		result[index].Workload = seal
	}
	return result, nil
}
