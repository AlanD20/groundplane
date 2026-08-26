package controller

import (
	"sort"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func BuildEnvironmentComposeProjection(
	environmentID string,
	revisionID string,
	generation uint64,
	snapshot ComposeIdentitySnapshot,
	routes []core.Route,
	components []etcd.ComponentRecord,
	entries []etcd.EntryRecord,
	dependencyPlans core.ServiceDependencyPlans,
) (etcd.EnvironmentComposeProjection, error) {
	convert := func(values []ComposeResourceIdentity) []etcd.EnvironmentComposeIdentity {
		result := make([]etcd.EnvironmentComposeIdentity, len(values))
		for index, value := range values {
			result[index] = etcd.EnvironmentComposeIdentity{ID: value.ID, Name: value.Name}
		}
		return result
	}
	routeIdentities := make([]etcd.EnvironmentRouteIdentity, len(routes))
	for index, route := range routes {
		routeIdentities[index] = etcd.EnvironmentRouteIdentity{ID: route.ID, Host: route.Host, Path: route.Path}
	}
	sort.Slice(routeIdentities, func(left int, right int) bool {
		leftMatch := routeIdentities[left].Host + "\x00" + routeIdentities[left].Path
		rightMatch := routeIdentities[right].Host + "\x00" + routeIdentities[right].Path
		return leftMatch < rightMatch
	})
	return etcd.EnvironmentComposeProjection{
		EnvironmentID: environmentID, BlueprintRevisionID: revisionID, RenderGeneration: generation,
		Services: convert(snapshot.Services), Networks: convert(snapshot.Networks), Volumes: convert(snapshot.Volumes),
		Routes: routeIdentities, Components: components, Entries: entries,
		ServiceDependencyPlans: dependencyPlans.Clone(),
	}, nil
}
