package taskjournal

import (
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func validateRollingResultEvidence(result TaskResultRecord) error {
	if len(result.RollingEvidence) > 32 || len(result.RollingEvidence) != 0 && result.Kind != TaskResultCompose {
		return errs.New(errs.KindValidationFailed, "rolling completion evidence is not valid for this Task result")
	}
	previous := ""
	for _, evidence := range result.RollingEvidence {
		if err := rollingrelease.ValidateCompletion(evidence); err != nil {
			return err
		}
		if evidence.ServiceID <= previous {
			return errs.New(errs.KindValidationFailed, "rolling completion evidence must be uniquely sorted")
		}
		previous = evidence.ServiceID
		for _, proxy := range result.ProxyEvidence {
			if proxy.ServiceID == evidence.ServiceID {
				return errs.New(
					errs.KindValidationFailed,
					"rolling Service cannot also carry single-target proxy evidence",
				)
			}
		}
		for _, recreate := range result.RecreateEvidence {
			if recreate.ServiceID == evidence.ServiceID {
				return errs.New(errs.KindValidationFailed, "rolling Service cannot also carry recreate evidence")
			}
		}
	}
	return nil
}
