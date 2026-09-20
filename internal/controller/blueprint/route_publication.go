package blueprint

import (
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func (service *Service) prepareRoutePublication(componentEnvironment core.Environment, pinnedComponents []etcd.ComponentRecord, componentPreparation etcd.ComponentTaskPreparation, generation uint64, routeChanges []etcd.EnvironmentBlueprintRouteChange) (etcd.ComponentTaskPreparation, []etcd.EnvironmentBlueprintRouteChange, error) {
	routeProvider, routeProjection, err := controller.ResolveComponentTaskRouteProvider(
		service.componentCatalog,
		componentEnvironment,
		pinnedComponents,
		componentPreparation.Intent.Candidates,
		int64(generation),
		generation,
	)
	if err != nil {
		return etcd.ComponentTaskPreparation{}, nil, err
	}
	if routeProjection {
		if routeProvider != nil {
			provider := etcd.RouteProviderObservation{
				ComponentID:      routeProvider.ComponentID,
				DefinitionDigest: routeProvider.DefinitionDigest,
				CatalogDigest:    routeProvider.CatalogDigest,
				InputRevision:    routeProvider.InputRevision,
				InputGeneration:  routeProvider.InputGeneration,
			}
			for index, change := range routeChanges {
				change.Record, err = etcd.SetRouteObservation(change.Record, etcd.RouteObservation{
					Status:            etcd.RouteObservedPending,
					DesiredGeneration: change.Record.DesiredGeneration,
					Provider:          provider,
				})
				if err != nil {
					return etcd.ComponentTaskPreparation{}, nil, err
				}
				routeChanges[index] = change
			}
		}
		routeRecords := make([]etcd.RouteRecord, len(routeChanges))
		for index, change := range routeChanges {
			routeRecords[index] = change.Record
		}
		componentPreparation, err = etcd.WithComponentTaskRouteProjection(
			componentPreparation, routeRecords, routeProvider,
		)
		if err != nil {
			return etcd.ComponentTaskPreparation{}, nil, err
		}
	}
	return componentPreparation, routeChanges, nil
}
