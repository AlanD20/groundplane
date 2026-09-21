package etcd

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const backupTaskTimeoutSeconds = 6 * 60 * 60

type backupTaskPlanValidator func(*agentpb.ExecutionPlan) error

// Rationale: the public Task journal identifies one exact sealed private
// procedure without copying source controls, object locators, or secrets into
// generic Params or materialization fields.
func validateBackupTaskSealedPlan(
	authority backupTaskPublicationAuthority,
	record TaskRecord,
	sealed *agentpb.ExecutionPlan,
) error {
	validated, err := executionplan.Validate(sealed)
	if err != nil {
		return err
	}
	if authority.validatePlan == nil || record.PlanID != validated.PlanId ||
		record.PlanHash != hex.EncodeToString(validated.PlanHash) ||
		record.RenderGeneration != 0 || record.TimeoutSeconds != backupTaskTimeoutSeconds ||
		len(record.Params) != 0 || len(record.Materializations) != 0 ||
		len(record.Steps) != len(validated.Steps) {
		return errs.New(errs.KindValidationFailed, "backup Task sealed plan identity is invalid")
	}
	for index, step := range validated.Steps {
		if record.Steps[index].ID != step.StepId {
			return errs.New(errs.KindValidationFailed, "backup Task step order is invalid")
		}
	}
	return authority.validatePlan(validated)
}
