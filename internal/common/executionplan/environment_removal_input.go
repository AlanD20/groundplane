package executionplan

import (
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ValidateEnvironmentRemovalInput validates captured delivery content. It does
// not authorize a host path or prove runtime absence; the native executor must
// enforce its configured root and compare observed ownership before removal.
func ValidateEnvironmentRemovalInput(input *agentpb.EnvironmentRemovalInput) error {
	if input == nil || ids.Validate(ids.KindEnvironment, input.EnvironmentId) != nil || input.CapturedSource == nil ||
		ids.Validate(
			ids.KindPlan,
			input.CapturedSource.InputId,
		) != nil || len(input.CapturedSource.Sha256) != sha256.Size {
		return errs.New(errs.KindValidationFailed, "Environment removal input identity is invalid")
	}
	if err := RejectUnknown(input); err != nil {
		return err
	}
	if err := validateVolumeDirectory(input.ExpectedVolumeDir, input.EnvironmentId); err != nil {
		return err
	}
	if input.Compose == nil {
		return nil
	}
	if input.Compose.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		input.Compose.OwnerId != input.EnvironmentId ||
		input.Compose.AuthorizedVolumeDir != input.ExpectedVolumeDir {
		return errs.New(errs.KindValidationFailed, "Environment removal artifact belongs to another owner or directory")
	}
	return validateArtifactContent(input.Compose)
}
