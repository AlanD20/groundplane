package executionplan

import (
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const maximumVolumeTraversalCursorBytes = 16 * 1024

// AuthorizeVolumeDirectories applies daemon-owned host policy after generic
// plan validation. The trusted root never enters the sealed plan or its hash.
func AuthorizeVolumeDirectories(plan *agentpb.ExecutionPlan, volumeRoot string) error {
	if plan == nil {
		return errs.New(errs.KindValidationFailed, "execution plan is required")
	}
	if err := environmentpath.ValidateRoot(volumeRoot); err != nil {
		return err
	}
	for _, artifact := range plan.Artifacts {
		if artifact == nil || artifact.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT {
			continue
		}
		scope, err := environmentpath.Parse(volumeRoot, artifact.AuthorizedVolumeDir)
		if err != nil || scope.EnvironmentID != artifact.OwnerId {
			return errs.New(
				errs.KindValidationFailed,
				"authorized environment volume directory is outside its trusted owner policy",
			)
		}
	}
	for _, step := range plan.Steps {
		create := step.GetEnvironmentDirectoryCreate()
		if create != nil {
			scope, err := environmentpath.Parse(volumeRoot, create.ExpectedVolumeDir)
			if err != nil || scope.EnvironmentID != create.EnvironmentId || create.EnvironmentId != plan.TargetId {
				return errs.New(
					errs.KindValidationFailed,
					"environment directory is outside its trusted owner policy",
				)
			}
		}
		remove := step.GetEnvironmentDirectoryRemove()
		if remove != nil {
			scope, err := environmentpath.Parse(volumeRoot, remove.ExpectedVolumeDir)
			if err != nil || scope.EnvironmentID != remove.EnvironmentId || remove.EnvironmentId != plan.TargetId {
				return errs.New(
					errs.KindValidationFailed,
					"environment directory removal is outside its trusted owner policy",
				)
			}
		}
	}
	return nil
}

func validateVolumes(plan *agentpb.ExecutionPlan, artifact *agentpb.ComposeArtifact) error {
	previous := ""
	composeNames := make(map[string]struct{}, len(artifact.Volumes))
	dockerNames := make(map[string]struct{}, len(artifact.Volumes))
	for _, volume := range artifact.Volumes {
		if volume == nil || validateID(ids.KindVolume, volume.VolumeId) != nil {
			return errs.New(errs.KindValidationFailed, "Compose artifact volume id is invalid")
		}
		if previous >= volume.VolumeId {
			return errs.New(errs.KindValidationFailed, "Compose artifact volumes must be uniquely sorted by id")
		}
		previous = volume.VolumeId
		if err := validateComposeName(volume.ComposeName); err != nil {
			return err
		}
		if err := validateComposeName(volume.DockerName); err != nil {
			return err
		}
		if _, exists := composeNames[volume.ComposeName]; exists {
			return errs.New(errs.KindValidationFailed, "Compose artifact volume Compose keys must be unique")
		}
		if _, exists := dockerNames[volume.DockerName]; exists {
			return errs.New(errs.KindValidationFailed, "Compose artifact Docker volume names must be unique")
		}
		composeNames[volume.ComposeName] = struct{}{}
		dockerNames[volume.DockerName] = struct{}{}
		if err := validateLabels(plan, artifact, "volume", volume.VolumeId, volume.ExpectedLabels); err != nil {
			return err
		}
	}
	return nil
}

func validManagedVolumeComposeKey(value string) bool {
	if value == "" || value == "." || value == ".." || len(value) > 255 || !utf8.ValidString(value) {
		return false
	}
	for index := range len(value) {
		character := value[index]
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || character == '.' || character == '_' || character == '-' {
			continue
		}
		return false
	}
	return true
}

func operationCreatesManagedVolumes(operation agentpb.PlanOperation) bool {
	switch operation {
	case agentpb.PlanOperation_PLAN_OPERATION_RECONCILE,
		agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
		agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE,
		agentpb.PlanOperation_PLAN_OPERATION_DEPLOY,
		agentpb.PlanOperation_PLAN_OPERATION_ROLLBACK,
		agentpb.PlanOperation_PLAN_OPERATION_START:
		return true
	default:
		return false
	}
}
