package app

import (
	servicelogs "github.com/AlanD20/groundplane/internal/controller/servicelogs"
	agentregistration "github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	"github.com/AlanD20/groundplane/internal/controller/environment"
	"github.com/AlanD20/groundplane/internal/controller/serviceobservation"
	"github.com/AlanD20/groundplane/internal/controller/serviceread"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	environmentetcd "github.com/AlanD20/groundplane/internal/infra/etcd/environment"
)

type serviceReadResources struct {
	environments *environment.Reader
	services     *serviceread.Reader
	observations *serviceobservation.Reader
	logs         *servicelogs.Service
}

func newServiceReadResources(
	hierarchy *etcd.HierarchyRepository, services *etcd.ServiceRepository, zones *etcd.ZoneRepository,
	releases *etcd.ReleaseLedger, agents *agentregistration.Repository, channel *agentchannel.Registry,
) (serviceReadResources, error) {
	reads, err := serviceread.New(hierarchy, services)
	if err != nil {
		return serviceReadResources{}, err
	}
	observer, err := serviceobservation.New(services, releases, channel, time.Now)
	if err != nil {
		return serviceReadResources{}, err
	}
	observations, err := serviceobservation.NewReader(agents, observer)
	if err != nil {
		return serviceReadResources{}, err
	}
	environments := environment.NewEtcdReader(environmentetcd.NewRepository(hierarchy, zones))
	return serviceReadResources{
		environments: environments, services: reads, observations: observations,
		logs: servicelogs.New(environments, reads, releases, channel),
	}, nil
}
