package controller

import (
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/components"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// projectPinnedEnvironmentComponents reconstructs the exact Environment input
// for Component renderers from one immutable Blueprint and its durable render
// projection. It never allocates an identity or consults mutable Components.
func projectPinnedEnvironmentComponents(
	project *composetypes.Project,
	identity pinnedEnvironmentIdentity,
	projection etcd.EnvironmentComposeProjection,
	routeSpecs []core.RouteSpec,
	componentSpecs map[string]core.ComponentSpec,
	entries []core.EnvEntry,
	catalog []components.Registration,
) (EnvironmentComponentComposeProjection, error) {
	authored, generated, err := splitPinnedServiceIdentities(project, projection.Services)
	if err != nil {
		return EnvironmentComponentComposeProjection{}, err
	}
	identities := ComposeIdentitySnapshot{
		Services: authored,
		Networks: composeResourceIdentities(projection.Networks),
		Volumes:  composeVolumeResourceIdentities(projection.Volumes),
	}
	zones, err := ProjectZoneProjection(
		project,
		identities,
		core.ZoneOwnerEnvironment,
		identity.EnvironmentID,
	)
	if err != nil {
		return EnvironmentComponentComposeProjection{}, err
	}
	services, err := ProjectServiceProjection(project, identities)
	if err != nil {
		return EnvironmentComponentComposeProjection{}, err
	}
	pinnedRoutes := append([]etcd.EnvironmentRouteIdentity(nil), projection.Routes...)
	pinnedRoutes = append(pinnedRoutes, projection.SuppressedRoutes...)
	sort.Slice(pinnedRoutes, func(left int, right int) bool {
		leftMatch := pinnedRoutes[left].Host + "\x00" + pinnedRoutes[left].Path
		rightMatch := pinnedRoutes[right].Host + "\x00" + pinnedRoutes[right].Path
		return leftMatch < rightMatch
	})
	previousRoutes := make([]RouteIdentity, len(pinnedRoutes))
	for index, route := range pinnedRoutes {
		previousRoutes[index] = RouteIdentity{ID: route.ID, Host: route.Host, Path: route.Path}
	}
	allocated := false
	routes, err := ReconcileBlueprintRoutes(routeSpecs, services, previousRoutes, func(_ ids.Kind) string {
		allocated = true
		return ""
	})
	if err != nil || allocated || len(routes.RemovedRouteIDs) != 0 || len(routes.Current) != len(previousRoutes) {
		return EnvironmentComponentComposeProjection{}, errs.New(
			errs.KindInternal,
			"Blueprint Task Route projection cannot be reproduced exactly",
		)
	}
	suppressed := make(map[string]struct{}, len(projection.SuppressedRoutes))
	for _, route := range projection.SuppressedRoutes {
		suppressed[route.ID] = struct{}{}
	}
	effectiveRoutes := make([]core.Route, 0, len(projection.Routes))
	for _, route := range routes.Current {
		if _, omitted := suppressed[route.ID]; !omitted {
			effectiveRoutes = append(effectiveRoutes, route)
		}
	}
	if !samePinnedRouteIdentities(effectiveRoutes, projection.Routes) {
		return EnvironmentComponentComposeProjection{}, errs.New(
			errs.KindInternal,
			"Blueprint Task effective Route projection cannot be reproduced exactly",
		)
	}
	componentRecords := make([]core.Component, len(projection.Components))
	for index, record := range projection.Components {
		componentRecords[index], err = etcd.ProjectComponentRecord(record)
		if err != nil {
			return EnvironmentComponentComposeProjection{}, err
		}
	}
	componentAllocated := false
	components, err := ReconcileBlueprintComponents(componentSpecs, componentRecords, func(_ ids.Kind) string {
		componentAllocated = true
		return ""
	})
	if err != nil || componentAllocated || len(components.Candidates) != 0 {
		return EnvironmentComponentComposeProjection{}, errs.New(
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
	result, err := ProjectEnvironmentComponents(project, environment, catalog)
	if err != nil {
		return EnvironmentComponentComposeProjection{}, err
	}
	if !sameComposeResourceIdentities(result.Services, generated) {
		return EnvironmentComponentComposeProjection{}, errs.New(
			errs.KindInternal,
			"Blueprint Task generated Service identities cannot be reproduced exactly",
		)
	}
	return result, nil
}

func samePinnedRouteIdentities(routes []core.Route, identities []etcd.EnvironmentRouteIdentity) bool {
	if len(routes) != len(identities) {
		return false
	}
	byID := make(map[string]etcd.EnvironmentRouteIdentity, len(identities))
	for _, identity := range identities {
		byID[identity.ID] = identity
	}
	for _, route := range routes {
		identity, found := byID[route.ID]
		if !found || identity.Host != route.Host || identity.Path != route.Path {
			return false
		}
	}
	return true
}

func splitPinnedServiceIdentities(
	project *composetypes.Project,
	pinned []etcd.EnvironmentComposeIdentity,
) ([]ComposeResourceIdentity, []ComposeResourceIdentity, error) {
	names, err := ownedServiceNames(project)
	if err != nil {
		return nil, nil, err
	}
	authoredNames := make(map[string]struct{}, len(names))
	for _, name := range names {
		authoredNames[name] = struct{}{}
	}
	authored := make([]ComposeResourceIdentity, 0, len(names))
	generated := make([]ComposeResourceIdentity, 0, len(pinned)-len(names))
	for _, identity := range pinned {
		value := ComposeResourceIdentity{ID: identity.ID, Name: identity.Name}
		if _, exists := authoredNames[identity.Name]; exists {
			authored = append(authored, value)
			delete(authoredNames, identity.Name)
		} else {
			generated = append(generated, value)
		}
	}
	if len(authoredNames) != 0 {
		return nil, nil, errs.New(errs.KindInternal, "Blueprint Task authored Service identity is missing")
	}
	return authored, generated, nil
}

func composeResourceIdentities(values []etcd.EnvironmentComposeIdentity) []ComposeResourceIdentity {
	result := make([]ComposeResourceIdentity, len(values))
	for index, value := range values {
		result[index] = ComposeResourceIdentity{ID: value.ID, Name: value.Name}
	}
	return result
}

func composeVolumeResourceIdentities(values []etcd.EnvironmentVolumeIdentity) []ComposeResourceIdentity {
	result := make([]ComposeResourceIdentity, len(values))
	for index, value := range values {
		result[index] = ComposeResourceIdentity{ID: value.ID, Name: value.Key}
	}
	return result
}

func sameComposeResourceIdentities(left []ComposeResourceIdentity, right []ComposeResourceIdentity) bool {
	if len(left) != len(right) {
		return false
	}
	left = append([]ComposeResourceIdentity(nil), left...)
	right = append([]ComposeResourceIdentity(nil), right...)
	sort.Slice(left, func(i, j int) bool { return left[i].Name < left[j].Name })
	sort.Slice(right, func(i, j int) bool { return right[i].Name < right[j].Name })
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
