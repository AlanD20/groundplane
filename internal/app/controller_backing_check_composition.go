package app

import (
	"github.com/AlanD20/groundplane/internal/controller/backingchecks"
	"github.com/AlanD20/groundplane/internal/controller/imagedelivery"
	checkrecord "github.com/AlanD20/groundplane/internal/infra/etcd/backingchecks"
)

func newControllerBackingCheckService(dependencies controllerHTTPDependencies) (*backingchecks.Service, error) {
	records, err := checkrecord.NewRepository(dependencies.store)
	if err != nil {
		return nil, err
	}
	return backingchecks.NewService(dependencies.execution.attachRecords, dependencies.execution.attachFactValues,
		records, dependencies.authority.idempotency, dependencies.authority.intentCoordinator,
		dependencies.authority.intentProtector,
		imagedelivery.SelectionResolver{Images: dependencies.agentRuntime.Registry, Store: dependencies.store},
		dependencies.agentRuntime.Registry)
}
