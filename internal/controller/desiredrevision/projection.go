package desiredrevision

import (
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func ComposeProjection(
	environmentID string,
	revisionID string,
	generation uint64,
	snapshot controller.ComposeIdentitySnapshot,
	volumeSlugs map[string]string,
	volumeMounts []etcd.EnvironmentServiceVolumeMount,
	artifact []byte,
	routes []core.Route,
	components []etcd.ComponentRecord,
	entries []etcd.EntryRecord,
) etcd.EnvironmentComposeProjection {
	convert := func(values []controller.ComposeResourceIdentity) []etcd.EnvironmentComposeIdentity {
		result := make([]etcd.EnvironmentComposeIdentity, len(values))
		for index, value := range values {
			result[index] = etcd.EnvironmentComposeIdentity{ID: value.ID, Name: value.Name}
		}
		return result
	}
	convertVolumes := func(values []controller.ComposeResourceIdentity) []etcd.EnvironmentVolumeIdentity {
		result := make([]etcd.EnvironmentVolumeIdentity, len(values))
		for index, value := range values {
			result[index] = etcd.EnvironmentVolumeIdentity{
				ID: value.ID, Slug: volumeSlugs[value.Name], Key: value.Name,
			}
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
		EnvironmentID: environmentID, RevisionID: revisionID, RenderGeneration: generation,
		ComposeArtifact: append([]byte(nil), artifact...),
		Services:        convert(snapshot.Services), Networks: convert(snapshot.Networks), Volumes: convertVolumes(snapshot.Volumes),
		VolumeMounts: append([]etcd.EnvironmentServiceVolumeMount(nil), volumeMounts...),
		Routes:       routeIdentities, Components: components, Entries: entries,
	}
}

func BlueprintRevision(
	environmentID string,
	revisionID string,
	createdAt time.Time,
	bundle core.BlueprintBundle,
) etcd.EnvironmentBlueprintRevision {
	files := make([]etcd.EnvironmentBlueprintFile, len(bundle.Files))
	for index, file := range bundle.Files {
		files[index] = etcd.EnvironmentBlueprintFile{Path: file.Path, Content: file.Content}
	}
	return etcd.EnvironmentBlueprintRevision{
		EnvironmentID: environmentID, RevisionID: revisionID, RootPath: bundle.RootPath,
		ComposeSources: append([]string(nil), bundle.ComposeSources...),
		Interpolation:  cloneBlueprintInterpolation(bundle.Interpolation),
		Files:          files, CreatedAt: createdAt,
	}
}

func cloneBlueprintInterpolation(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}
