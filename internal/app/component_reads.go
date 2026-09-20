package app

import (
	"github.com/AlanD20/groundplane/internal/componentregistration"
	componentrender "github.com/AlanD20/groundplane/internal/controller/componentrender"

	componentcapability "github.com/AlanD20/groundplane/internal/controller/component"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	networketcd "github.com/AlanD20/groundplane/internal/infra/etcd/network"
)

func newComponentReadService(
	components *etcd.ComponentRepository,
	network *networketcd.Repository,
	catalog []componentrender.EnvironmentComponentRegistration,
) (*componentcapability.ReadService, error) {
	platform, err := componentregistration.NewDNSManagedConfigProjector(components, components)
	if err != nil {
		return nil, err
	}
	var previews []componentcapability.ManagedConfigRegistration
	for _, registration := range catalog {
		if managed := registration.ManagedConfiguration; managed != nil {
			previews = append(previews, componentcapability.ManagedConfigRegistration{
				Kind: registration.Kind, SourcePath: managed.SourcePath, Plan: registration.Plan,
			})
		}
	}
	projector, err := componentcapability.NewConfigProjector(network, platform, previews)
	if err != nil {
		return nil, err
	}
	return componentcapability.NewReadService(components, projector)
}
