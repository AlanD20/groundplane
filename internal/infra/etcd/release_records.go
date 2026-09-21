package etcd

import (
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func validateReleaseOperationHead(
	value releases.ReleaseOperationHead,
	manifest releases.ReleaseStagedManifest,
	task TaskRecord,
) error {
	if value.OperationID != manifest.OperationID || value.PublicationID != manifest.PublicationID ||
		ids.Validate(ids.KindEnvironment, value.EnvironmentID) != nil || value.State != domain.StatePending ||
		len(value.Attempts) != 1 || value.Attempts[0].TaskID != task.ID || value.LatestTaskID != task.ID ||
		len(value.Members) != len(manifest.Members) || value.CreatedAt.IsZero() || value.UpdatedAt != value.CreatedAt ||
		value.ConfiguredTimeoutSeconds <= 0 || value.ComputedBudgetSeconds <= 0 ||
		value.ConfiguredTimeoutSeconds < value.ComputedBudgetSeconds {
		return errs.New(errs.KindValidationFailed, "release operation head is invalid")
	}
	if value.RecoveryOutcome != "" || value.FailedMemberOrdinal != 0 {
		return errs.New(errs.KindValidationFailed, "new release operation carries recovery state")
	}
	if value.ReleaseGroupID != "" && ids.Validate(ids.KindReleaseGroup, value.ReleaseGroupID) != nil {
		return errs.New(errs.KindValidationFailed, "release operation group id is invalid")
	}
	if value.ReleaseGroupID == "" && value.Progress != nil || value.ReleaseGroupID != "" && value.Progress == nil {
		return errs.New(errs.KindValidationFailed, "release operation group progress presence is invalid")
	}
	if value.Progress != nil &&
		(value.Progress.OperationID != value.OperationID || value.Progress.AttemptID != task.ID ||
			value.Progress.NextMemberOrdinal != 1 || len(value.Progress.Results) != 0 || value.Progress.Compensating) {
		return errs.New(errs.KindValidationFailed, "release operation initial group progress is invalid")
	}
	if value.FailurePolicy != domain.OnFailureSwitchBack && value.FailurePolicy != domain.OnFailureLeaveActive {
		return errs.New(errs.KindValidationFailed, "release operation failure policy is invalid")
	}
	for index, member := range value.Members {
		if member.Ordinal != uint32(index+1) || member.ReleaseID != manifest.Members[index].ReleaseID ||
			member.ServiceID != manifest.Members[index].ServiceID {
			return errs.New(errs.KindValidationFailed, "release operation members do not match staging")
		}
	}
	return nil
}
