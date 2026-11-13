package runners

import (
	buildversion "github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type CapturedRuntime struct {
	Software          buildversion.ContainerIdentity `json:"software"`
	RunnerID          string                         `json:"runner_id"`
	RunnerRevision    int64                          `json:"runner_revision"`
	LifecycleRevision int64                          `json:"lifecycle_revision"`
	Desired           RunnerDesiredRecord            `json:"desired"`
	Ownership         RunnerRuntimeOwnershipRecord   `json:"ownership"`
	ContainerID       string                         `json:"container_id"`
}

func (source CapturedRuntime) Validate() error {
	if ValidateRunnerDesired(source.Desired) != nil ||
		ValidateRunnerRuntimeOwnership(source.Ownership) != nil ||
		source.Software.Validate() != nil ||
		source.Software.Image != source.Desired.ImageRef ||
		source.RunnerID != source.Desired.ID ||
		source.Ownership.RunnerID != source.RunnerID ||
		source.RunnerRevision <= 0 ||
		source.LifecycleRevision <= 0 ||
		!ValidLowerHex(source.ContainerID, RunnerContainerIDEncodedLength) {
		return errs.New(errs.KindValidationFailed, "Runner diagnostic source is invalid")
	}
	return nil
}
