package taskplanning

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	componentrender "github.com/AlanD20/groundplane/internal/controller/componentrender"
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/core"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"

	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/pkg/errs"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// projectPinnedEnvironmentComponents reconstructs the exact Environment input
// for Component renderers from one immutable Blueprint and its durable render
// projection. It never allocates an identity or consults mutable Components.
func projectPinnedEnvironmentComponents(
	project *composetypes.Project,
	serviceExtensions map[string]core.ServiceExtensionSpec,
	identity pinnedEnvironmentIdentity,
	projection projectionrecord.EnvironmentComposeProjection,
	_ []core.RouteSpec,
	componentSpecs map[string]core.ComponentSpec,
	entries []core.EnvEntry,
	catalog []componentrender.EnvironmentComponentRegistration,
) (composerender.EnvironmentComponentComposeProjection, error) {
	generated := make(map[string]struct{})
	for _, component := range projection.Components {
		for _, serviceID := range component.Runtime.GeneratedServices {
			generated[serviceID] = struct{}{}
		}
	}
	// Generated identities are not members of the desired Service collection.
	// Their count therefore cannot be subtracted from its allocation bound.
	authored := make([]composeidentity.Resource, 0, len(projection.DesiredServices))
	for _, service := range projection.DesiredServices {
		if _, owned := generated[service.Desired.ID]; owned {
			continue
		}
		authored = append(authored, composeidentity.Resource{ID: service.Desired.ID, Name: service.Desired.Name})
	}
	identities := composeidentity.Snapshot{
		Services: authored,
		Networks: composerender.DesiredZoneResourceIdentities(projection.DesiredZones),
		Volumes:  composerender.ComposeVolumeResourceIdentities(projection.Volumes),
	}
	zones, err := ProjectZoneProjection(
		project,
		identities,
		core.ZoneOwnerEnvironment,
		identity.EnvironmentID,
	)
	if err != nil {
		return composerender.EnvironmentComponentComposeProjection{}, err
	}
	services, err := ProjectServiceProjection(project, identities, serviceExtensions)
	if err != nil {
		return composerender.EnvironmentComponentComposeProjection{}, err
	}
	effectiveRoutes := make([]core.Route, len(projection.DesiredRoutes))
	for index, route := range projection.DesiredRoutes {
		effectiveRoutes[index] = route.Desired
	}
	componentRecords := make([]core.Component, len(projection.Components))
	for index, record := range projection.Components {
		componentRecords[index], err = componentrecord.ProjectRecord(record)
		if err != nil {
			return composerender.EnvironmentComponentComposeProjection{}, err
		}
		// Health is mutable runtime state, not part of the immutable Blueprint
		// projection. Replaying a pinned Task must not schedule a second repair.
		if componentRecords[index].Enabled {
			componentRecords[index].Healthy = true
		}
	}
	componentAllocated := false
	components, err := ReconcileBlueprintComponents(componentSpecs, componentRecords, func(_ ids.Kind) string {
		componentAllocated = true
		return ""
	})
	if err != nil || componentAllocated || len(components.Candidates) != 0 {
		return composerender.EnvironmentComponentComposeProjection{}, errs.New(
			errs.KindInternal,
			"Blueprint Task Component projection cannot be reproduced exactly",
		)
	}
	environment := core.Environment{
		ID: identity.EnvironmentID, ProjectID: identity.ProjectID, Name: identity.EnvironmentName,
		VolumeDir: identity.AuthorizedVolumeDir,
		Zones:     make(map[string]core.Zone, len(zones)),
		Services:  make(map[string]core.Service, len(services)),
		Routes:    effectiveRoutes, Components: components.Effective,
		Entries: append([]core.EnvEntry(nil), entries...),
	}
	for _, zone := range zones {
		environment.Zones[zone.Name] = zone
	}
	for _, service := range services {
		environment.Services[service.Name] = service
	}
	return composerender.ProjectEnvironmentComponents(project, environment, catalog)
}
