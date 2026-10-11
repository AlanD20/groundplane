package app

import (
	"github.com/AlanD20/groundplane-registered-components/cloudflaretunnel"
	"github.com/AlanD20/groundplane/internal/app/componentregistration"
	"github.com/AlanD20/groundplane/internal/controller/edgechecks"
	"github.com/AlanD20/groundplane/internal/controller/imagedelivery"
	checkrecord "github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
)

func newControllerEdgeCheckService(dependencies controllerHTTPDependencies) (*edgechecks.Service, error) {
	catalog, err := componentregistration.NewCatalog()
	if err != nil {
		return nil, err
	}
	records, err := checkrecord.NewRepository(dependencies.store)
	if err != nil {
		return nil, err
	}
	return edgechecks.NewService(dependencies.dataServices.routeRecords, records, dependencies.authority.idempotency,
		dependencies.authority.intentCoordinator,
		imagedelivery.SelectionResolver{Images: dependencies.agentRuntime.Registry, Store: dependencies.store},
		dependencies.agentRuntime.Registry, catalog, cloudflaretunnel.ObserveConnectionAction)
}
