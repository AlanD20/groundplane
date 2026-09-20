package desiredrevision

import (
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/pkg/errs"
	"math"
)

func NextGeneration(
	environmentID string,
	head etcd.Versioned[etcd.EnvironmentBlueprintHead],
	hasHead bool,
	projection etcd.Versioned[etcd.EnvironmentComposeProjection],
	hasProjection bool,
) (int64, uint64, error) {
	if hasHead != hasProjection {
		return 0, 0, errs.New(errs.KindInternal, "Environment desired-state pointers are inconsistent")
	}
	if !hasHead {
		return 0, 1, nil
	}
	if head.Record.EnvironmentID != environmentID || projection.Record.EnvironmentID != environmentID ||
		head.Record.RevisionID != projection.Record.RevisionID || head.Revision <= 0 ||
		projection.Revision != head.Revision || projection.Record.RenderGeneration == math.MaxUint64 {
		return 0, 0, errs.New(errs.KindInternal, "Environment desired-state pointers are corrupt")
	}
	return head.Revision, projection.Record.RenderGeneration + 1, nil
}

func CloneProjection(current etcd.EnvironmentComposeProjection) etcd.EnvironmentComposeProjection {
	var runtimeFiles []core.BlueprintFile
	if current.RuntimeFiles != nil {
		runtimeFiles = make([]core.BlueprintFile, len(current.RuntimeFiles))
		for index, file := range current.RuntimeFiles {
			runtimeFiles[index] = core.BlueprintFile{Path: file.Path, Content: append([]byte(nil), file.Content...)}
		}
	}
	result := etcd.EnvironmentComposeProjection{
		EnvironmentID: current.EnvironmentID, RevisionID: current.RevisionID,
		RenderGeneration:       current.RenderGeneration,
		ComposeArtifact:        append([]byte(nil), current.ComposeArtifact...),
		NormalizedCompose:      append([]byte(nil), current.NormalizedCompose...),
		RuntimeFiles:           runtimeFiles,
		ServiceExtensions:      CloneServiceExtensions(current.ServiceExtensions),
		DesiredZones:           append([]etcd.EnvironmentZoneProjection(nil), current.DesiredZones...),
		DesiredServices:        append([]etcd.EnvironmentServiceProjection(nil), current.DesiredServices...),
		DesiredRoutes:          append([]etcd.EnvironmentRouteProjection(nil), current.DesiredRoutes...),
		Volumes:                append([]etcd.EnvironmentVolumeIdentity(nil), current.Volumes...),
		VolumeMounts:           append([]etcd.EnvironmentServiceVolumeMount(nil), current.VolumeMounts...),
		Components:             append([]etcd.ComponentRecord(nil), current.Components...),
		Entries:                append([]entryrecord.Record(nil), current.Entries...),
		ServiceDependencyPlans: current.ServiceDependencyPlans.Clone(),
	}
	result.ManagedComponentRuntimeSources = append(
		[]etcd.ManagedComponentRuntimeSource(nil), current.ManagedComponentRuntimeSources...,
	)
	return result
}

func CloneServiceExtensions(source map[string]core.ServiceExtensionSpec) map[string]core.ServiceExtensionSpec {
	result := make(map[string]core.ServiceExtensionSpec, len(source)+1)
	for name, extension := range source {
		result[name] = core.CloneServiceExtension(extension)
	}
	return result
}
