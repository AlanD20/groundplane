package taskplanning

import (
	"github.com/AlanD20/groundplane/internal/controller/composeidentity"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/pkg/errs"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// Route rendering needs all target definitions, but its sealed host artifact
// owns only the selected router. Do not acknowledge authored application rows
// as executed Component runtime.
func routeProviderCompose(provider environmentchanges.RouteProviderPin) environmentComposeTransform {
	return func(project *composetypes.Project, projection projectionrecord.EnvironmentComposeProjection) ([]composeidentity.Resource, error) {
		if project == nil {
			return nil, errs.New(errs.KindStateConflict, "Route router projection is unavailable")
		}
		owned := false
		for _, component := range projection.Components {
			if component.Desired.ID == provider.ComponentID && component.Desired.Enabled &&
				len(
					component.Runtime.GeneratedServices,
				) == 1 && component.Runtime.GeneratedServices[0] == provider.ServiceID {
				owned = true
			}
		}
		if !owned {
			return nil, errs.New(errs.KindStateConflict, "Route router ownership changed")
		}
		identities, err := composerender.ComposeIdentitySnapshotFromProjection(projection)
		if err != nil {
			return nil, err
		}
		name := ""
		for _, identity := range identities.Services {
			if identity.ID == provider.ServiceID && identity.ComponentID == provider.ComponentID {
				name = identity.Name
				break
			}
		}
		selected, found := project.Services[name]
		if !found || name == "" {
			return nil, errs.New(errs.KindStateConflict, "Route router workload is unavailable")
		}
		project.Services = composetypes.Services{name: selected}
		project.DisabledServices = nil
		project.Volumes = nil
		return managedAttachExternalNetworks(project)
	}
}
