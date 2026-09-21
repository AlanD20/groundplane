package etcd

import (
	"github.com/AlanD20/groundplane/internal/core"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
)

func cloneEnvironmentComposeProjection(source EnvironmentComposeProjection) EnvironmentComposeProjection {
	clone := source
	clone.ServiceDependencyPlans = source.ServiceDependencyPlans.Clone()
	clone.BlueprintRequirements = source.BlueprintRequirements.Clone()
	clone.ComposeArtifact = append([]byte(nil), source.ComposeArtifact...)
	clone.NormalizedCompose = append([]byte(nil), source.NormalizedCompose...)
	if source.RuntimeFiles != nil {
		clone.RuntimeFiles = make([]core.BlueprintFile, len(source.RuntimeFiles))
		for index, file := range source.RuntimeFiles {
			clone.RuntimeFiles[index] = core.BlueprintFile{
				Path:    file.Path,
				Content: append([]byte(nil), file.Content...),
			}
		}
	}
	clone.ServiceExtensions = cloneEnvironmentServiceExtensions(source.ServiceExtensions)
	clone.DesiredZones = append([]EnvironmentZoneProjection(nil), source.DesiredZones...)
	clone.DesiredServices = append([]servicerecord.EnvironmentServiceProjection(nil), source.DesiredServices...)
	clone.DesiredRoutes = append([]EnvironmentRouteProjection(nil), source.DesiredRoutes...)
	clone.Volumes = append([]EnvironmentVolumeIdentity(nil), source.Volumes...)
	clone.VolumeMounts = append([]EnvironmentServiceVolumeMount(nil), source.VolumeMounts...)
	clone.Backup = CloneEnvironmentBlueprintBackupPolicy(source.Backup)
	if source.Components != nil {
		clone.Components = make([]componentrecord.Record, len(source.Components))
		for index, component := range source.Components {
			clone.Components[index] = cloneComponentTaskRecord(component)
		}
	}
	clone.ManagedComponentRuntimeSources = append(
		[]ManagedComponentRuntimeSource(nil), source.ManagedComponentRuntimeSources...,
	)
	if source.Entries != nil {
		clone.Entries = make([]entryrecord.Record, len(source.Entries))
		for index, entry := range source.Entries {
			clone.Entries[index] = entryrecord.CloneRecord(entry)
		}
	}
	return clone
}

func cloneEnvironmentServiceExtensions(
	source map[string]core.ServiceExtensionSpec,
) map[string]core.ServiceExtensionSpec {
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
