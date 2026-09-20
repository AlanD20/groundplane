package blueprint

import (
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
)

func (service *Service) prepareRoutePublication(componentEnvironment core.Environment, pinnedComponents []componentrecord.Record, componentPreparation etcd.ComponentTaskPreparation, generation uint64, routeChanges []etcd.EnvironmentBlueprintRouteChange) (etcd.ComponentTaskPreparation, []etcd.EnvironmentBlueprintRouteChange, error) {
	routeProvider, routeProjection, err := taskplanning.ResolveComponentTaskRouteProvider(
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
			provider := routerecord.ProviderObservation{
				ComponentID:      routeProvider.ComponentID,
				DefinitionDigest: routeProvider.DefinitionDigest,
				CatalogDigest:    routeProvider.CatalogDigest,
				InputRevision:    routeProvider.InputRevision,
				InputGeneration:  routeProvider.InputGeneration,
			}
			for index, change := range routeChanges {
				change.Record, err = routerecord.SetObservation(change.Record, routerecord.Observation{
					Status:            routerecord.ObservedPending,
					DesiredGeneration: change.Record.DesiredGeneration,
					Provider:          provider,
				})
				if err != nil {
					return etcd.ComponentTaskPreparation{}, nil, err
				}
				routeChanges[index] = change
			}
		}
		routeRecords := make([]routerecord.Record, len(routeChanges))
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
