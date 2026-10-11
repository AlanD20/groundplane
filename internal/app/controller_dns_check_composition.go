package app

import (
	"github.com/AlanD20/groundplane/internal/app/componentregistration"
	"github.com/AlanD20/groundplane/internal/controller/dnsresolver"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnschecks"
)

func newControllerDNSCheckService(
	dependencies controllerHTTPDependencies,
) (*dnsresolver.ConnectionCheckService, error) {
	catalog, err := componentregistration.NewCatalog()
	if err != nil {
		return nil, err
	}
	planner, err := componentregistration.NewDNSConnectionCheckPlanner(
		dependencies.dataServices.componentRecords,
		catalog,
	)
	if err != nil {
		return nil, err
	}
	records, err := dnschecks.NewRepository(dependencies.store)
	if err != nil {
		return nil, err
	}
	return dnsresolver.NewConnectionCheckService(planner, dependencies.authority.tasks, records,
		dependencies.authority.idempotency, dependencies.authority.intentCoordinator)
}
