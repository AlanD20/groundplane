package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type environmentArtifactKind uint8

const (
	environmentArtifactDesired environmentArtifactKind = iota
	environmentArtifactCapturedRuntime
)

// Captured Attach runtime can contain a single serving slot at the current
// desired generation: native Deploy does not advance that generation. Desired
// publication still requires a complete fresh topology. Both paths validate the
// same metadata, resource ownership and artifact integrity.
func validateEnvironmentProjection(projection EnvironmentComposeProjection, kind environmentArtifactKind) error {
	if validateStableID(ids.KindEnvironment, projection.EnvironmentID) != nil ||
		validateStableID(ids.KindTask, projection.RevisionID) != nil || projection.RenderGeneration == 0 {
		return errs.New(errs.KindValidationFailed, "Environment Compose projection identity is invalid")
	}
	if err := validateEnvironmentServiceProjections(projection.EnvironmentID, projection.DesiredServices); err != nil {
		return err
	}
	if err := validateEnvironmentNormalizedCompose(projection.NormalizedCompose); err != nil {
		return err
	}
	if err := core.ValidateNormalizedBlueprintFiles(projection.RuntimeFiles); err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	names := make([]string, len(projection.DesiredServices))
	for index, service := range projection.DesiredServices {
		names[index] = service.Desired.Name
	}
	if err := projection.ServiceDependencyPlans.Validate(names); err != nil {
		return err
	}
	if err := projection.BlueprintRequirements.Validate(); err != nil {
		return err
	}
	if err := validateEnvironmentServiceExtensions(names, projection.ServiceExtensions); err != nil {
		return err
	}
	if err := validateEnvironmentBlueprintBackupPolicy(projection.EnvironmentID, projection.Backup); err != nil {
		return err
	}
	if err := validateEnvironmentZoneProjections(projection.EnvironmentID, projection.DesiredZones); err != nil {
		return err
	}
	if err := validateEnvironmentVolumeIdentities(projection.Volumes); err != nil {
		return err
	}
	if err := validateEnvironmentServiceVolumeMounts(projection); err != nil {
		return err
	}
	if err := validateEnvironmentComponentProjection(projection.EnvironmentID, projection.Components); err != nil {
		return err
	}
	if err := validateManagedComponentRuntimeSources(projection); err != nil {
		return err
	}
	if err := validateEnvironmentProjectionArtifact(projection, kind); err != nil {
		return err
	}
	if err := validateEnvironmentRouteProjections(projection.EnvironmentID, projection.DesiredRoutes); err != nil {
		return err
	}
	return validateEnvironmentEntryProjection(projection.EnvironmentID, projection.Entries)
}
