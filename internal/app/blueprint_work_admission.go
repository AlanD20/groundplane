package app

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/blueprintcoordinator"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
)

func newBlueprintWorkAdmission(
	hosts hostbindings.Reader,
	removals blueprintcoordinator.ServiceRemovalReader,
) (*blueprintcoordinator.Admission, error) {
	services, err := blueprintcoordinator.NewServicePlanner(hosts, removals)
	if err != nil {
		return nil, err
	}
	resources, err := blueprintcoordinator.NewHostResourcePlanner(hosts)
	if err != nil {
		return nil, err
	}
	volumes, err := blueprintcoordinator.NewVolumePlanner(hosts)
	if err != nil {
		return nil, err
	}
	components, err := blueprintcoordinator.NewComponentPlanner(hosts)
	if err != nil {
		return nil, err
	}
	return blueprintcoordinator.NewAdmission([]blueprintcoordinator.ResourceRegistration{
		{Kind: ids.KindService, Planner: services},
		{Kind: ids.KindEnvEntry, Planner: resources},
		{Kind: ids.KindNetwork, Planner: resources},
		{Kind: ids.KindVolume, Planner: volumes},
		{Kind: ids.KindAttach, Planner: resources},
		{Kind: ids.KindComponent, Planner: components},
	})
}
