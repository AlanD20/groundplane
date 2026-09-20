package etcd

import (
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func validateBackingServiceAdapterCreationShape(service core.Service, custom bool) error {
	if !custom {
		return nil
	}
	health := service.Healthcheck
	if service.Authentication != "" || service.FactsPrefix != "" || len(service.Mounts) != 0 ||
		len(service.Command) != 0 || len(service.Expose) != 0 ||
		health.HTTP != "" || health.TCP != "" || health.Pgrep != "" {
		return errs.New(errs.KindValidationFailed, "custom backing-service creation must be network-only")
	}
	return nil
}

func validateBackingServiceComponents(components []ComponentRecord) error {
	if len(components) != 0 {
		return errs.New(errs.KindValidationFailed, "Backing-service creation requires zero Environment Components")
	}
	return nil
}

func validateBackingServiceProjection(creation BackingServiceCreation) error {
	projection := creation.Projection
	volumeCount := len(projection.Volumes)
	expectedVolumeCount := 1
	if creation.Service.Desired.Adapter == "custom" {
		expectedVolumeCount = 0
	}
	if creation.Revision.EnvironmentID != creation.Environment.ID || creation.Revision.RevisionID != creation.Task.ID ||
		creation.Claim.EnvironmentID != creation.Environment.ID || creation.Claim.RevisionID != creation.Task.ID ||
		creation.Claim.TaskID != creation.Task.ID || creation.Claim.BaselineHeadRevision != 0 ||
		creation.Claim.SourceKind != EnvironmentBlueprintSourceApply || creation.Claim.RenderGeneration != 1 ||
		projection.EnvironmentID != creation.Environment.ID || projection.RevisionID != creation.Task.ID ||
		projection.RenderGeneration != 1 || len(projection.ComposeArtifact) == 0 ||
		volumeCount != expectedVolumeCount ||
		len(creation.Service.Desired.Mounts) != volumeCount ||
		len(projection.DesiredZones) != 1 || len(projection.DesiredServices) != 1 ||
		len(projection.VolumeMounts) != volumeCount || len(projection.Entries) != len(creation.Entries) ||
		projection.DesiredZones[0].EnvironmentID != creation.Environment.ID ||
		projection.DesiredZones[0].Desired != creation.Zone.Desired ||
		projection.DesiredServices[0].EnvironmentID != creation.Environment.ID ||
		projection.DesiredServices[0].BackingNetworkID != creation.Service.BackingNetworkID ||
		!sameServiceRemovalDesired(projection.DesiredServices[0].Desired, creation.Service.Desired) {
		return errs.New(errs.KindValidationFailed, "Backing-service desired projection is invalid")
	}
	if volumeCount == 1 && (projection.VolumeMounts[0].ServiceID != creation.Service.Desired.ID ||
		projection.VolumeMounts[0].VolumeID != projection.Volumes[0].ID ||
		creation.Service.Desired.Mounts[0].Volume != projection.Volumes[0].ID ||
		creation.Service.Desired.Mounts[0].Mount != projection.VolumeMounts[0].Target) {
		return errs.New(errs.KindValidationFailed, "Backing-service desired Volume projection is invalid")
	}
	for index, entry := range creation.Entries {
		if projection.Entries[index].EnvironmentID != creation.Environment.ID ||
			projection.Entries[index].Entry.ID != entry.Entry.ID ||
			projection.Entries[index].CurrentValueGenerationID != entry.CurrentValueGenerationID {
			return errs.New(errs.KindValidationFailed, "Backing-service desired Entry projection is invalid")
		}
	}
	return nil
}
