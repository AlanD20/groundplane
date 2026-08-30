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
	normalizedCompose []byte,
	runtimeFiles []core.BlueprintFile,
	serviceExtensions map[string]core.ServiceExtensionSpec,
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
	files := make([]core.BlueprintFile, len(runtimeFiles))
	for index, file := range runtimeFiles {
		files[index] = core.BlueprintFile{Path: file.Path, Content: append([]byte(nil), file.Content...)}
	}
	return etcd.EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: revisionID, RenderGeneration: generation,
		ComposeArtifact:   append([]byte(nil), artifact...),
		NormalizedCompose: append([]byte(nil), normalizedCompose...),
		RuntimeFiles:      files,
		ServiceExtensions: cloneServiceExtensions(serviceExtensions),
		Services:          convert(snapshot.Services), Networks: convert(snapshot.Networks), Volumes: convertVolumes(snapshot.Volumes),
		VolumeMounts: append([]etcd.EnvironmentServiceVolumeMount(nil), volumeMounts...),
		Routes:       routeIdentities, Components: components, Entries: entries,
	}
}

func cloneServiceExtensions(source map[string]core.ServiceExtensionSpec) map[string]core.ServiceExtensionSpec {
	if source == nil {
		return nil
	}
	result := make(map[string]core.ServiceExtensionSpec, len(source))
	for name, extension := range source {
		clone := extension
		if extension.Release != nil {
			release := *extension.Release
			clone.Release = &release
		}
		if extension.DependsOn != nil {
			clone.DependsOn = make(map[string]core.ServiceDependency, len(extension.DependsOn))
			for dependency, decision := range extension.DependsOn {
				decision.Phases = append([]core.ServiceDependencyPhase(nil), decision.Phases...)
				clone.DependsOn[dependency] = decision
			}
		}
		result[name] = clone
	}
	return result
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
