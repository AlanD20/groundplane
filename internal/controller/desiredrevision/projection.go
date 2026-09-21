package desiredrevision

import (
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func ComposeProjection(
	environmentID string,
	revisionID string,
	generation uint64,
	snapshot composeidentity.Snapshot,
	volumeSlugs map[string]string,
	volumeMounts []etcd.EnvironmentServiceVolumeMount,
	artifact []byte,
	normalizedCompose []byte,
	runtimeFiles []core.BlueprintFile,
	serviceExtensions map[string]core.ServiceExtensionSpec,
	_ []core.Route,
	components []componentrecord.Record,
	entries []entryrecord.Record,
) etcd.EnvironmentComposeProjection {
	convertVolumes := func(values []composeidentity.Resource) []etcd.EnvironmentVolumeIdentity {
		result := make([]etcd.EnvironmentVolumeIdentity, len(values))
		for index, value := range values {
			result[index] = etcd.EnvironmentVolumeIdentity{
				ID: value.ID, Slug: volumeSlugs[value.Name], Key: value.Name,
			}
		}
		return result
	}
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
		Volumes:           convertVolumes(snapshot.Volumes),
		VolumeMounts:      append([]etcd.EnvironmentServiceVolumeMount(nil), volumeMounts...),
		Components:        components, Entries: entries,
	}
}

func WithDesiredTopology(
	projection etcd.EnvironmentComposeProjection,
	zones []etcd.EnvironmentZoneProjection,
	services []servicerecord.EnvironmentServiceProjection,
	routes []etcd.EnvironmentRouteProjection,
) etcd.EnvironmentComposeProjection {
	projection.DesiredZones = append([]etcd.EnvironmentZoneProjection(nil), zones...)
	sort.Slice(projection.DesiredZones, func(left int, right int) bool {
		return projection.DesiredZones[left].Desired.Name < projection.DesiredZones[right].Desired.Name
	})
	projection.DesiredServices = append([]servicerecord.EnvironmentServiceProjection(nil), services...)
	sort.Slice(projection.DesiredServices, func(left int, right int) bool {
		return projection.DesiredServices[left].Desired.Name < projection.DesiredServices[right].Desired.Name
	})
	projection.DesiredRoutes = append([]etcd.EnvironmentRouteProjection(nil), routes...)
	sort.Slice(projection.DesiredRoutes, func(left int, right int) bool {
		leftRoute := projection.DesiredRoutes[left].Desired
		rightRoute := projection.DesiredRoutes[right].Desired
		return leftRoute.Host+"\x00"+leftRoute.Path < rightRoute.Host+"\x00"+rightRoute.Path
	})
	return projection
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
