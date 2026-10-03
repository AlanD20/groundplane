package taskjournal

import (
	"github.com/AlanD20/groundplane/internal/common/backingruntimefact"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func validateBackingObservations(result TaskResultRecord) error {
	if len(result.BackingObservations) > 64 ||
		len(result.BackingObservations) != 0 && result.Kind != TaskResultCompose {
		return errs.New(errs.KindValidationFailed, "task Backing observations exceed their execution boundary")
	}
	previous := ""
	for _, observation := range result.BackingObservations {
		identity := observation.ProjectName + "\x00" + observation.ContainerID
		if identity <= previous || backingruntimefact.ValidateObservation(observation) != nil {
			return errs.New(errs.KindValidationFailed, "task Backing observations are invalid or unsorted")
		}
		previous = identity
	}
	return nil
}
