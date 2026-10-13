package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// A native Attach child provisions one captured credential owner. Consumer
// deployment is a separate unit; no Compose or arbitrary execution belongs here.
func validateBlueprintAttachProvisionPlan(plan *agentpb.ExecutionPlan) error {
	if validateID(ids.KindEnvironment, plan.TargetId) != nil || len(plan.Steps) == 0 ||
		len(plan.ScriptRunnerSnapshots) != 0 || len(plan.ScriptRunnerProjections) != 0 ||
		len(plan.ScriptBodyArtifacts) != 0 ||
		plan.ComponentLifecycleMode != agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_UNSPECIFIED ||
		plan.GetCandidateReleaseProcedure() != nil || plan.GetManagedComponentProcedure() != nil ||
		plan.GetComponentRecoveryProcedure() != nil || plan.GetServiceAbsenceRecoveryProcedure() != nil {
		return errs.New(
			errs.KindValidationFailed,
			"Blueprint Attach provisioning plan shape is invalid",
		)
	}
	owner := plan.Steps[0].GetAdapterProcedure()
	if owner == nil ||
		owner.Phase != agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_PROVISION {
		return errs.New(
			errs.KindValidationFailed,
			"Blueprint Attach child must begin with credential provisioning",
		)
	}
	stepIDs := make(map[string]bool, len(plan.Steps))
	grants := make(map[string]bool, len(plan.Steps)-1)
	for index, step := range plan.Steps {
		if err := validateStep(plan, step, nil, nil); err != nil {
			return err
		}
		procedure := step.GetAdapterProcedure()
		if procedure == nil || procedure.AttachId != owner.AttachId ||
			procedure.BackingServiceId != owner.BackingServiceId || procedure.AdapterKey != owner.AdapterKey ||
			procedure.Authentication != owner.Authentication || procedure.Role != owner.Role {
			return errs.New(
				errs.KindValidationFailed,
				"Blueprint Attach procedures must share one captured credential owner",
			)
		}
		if index > 0 {
			if procedure.Phase != agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_GRANT ||
				grants[procedure.GrantOn] {
				return errs.New(
					errs.KindValidationFailed,
					"Blueprint Attach child may only grant each database once after provisioning",
				)
			}
			grants[procedure.GrantOn] = true
		}
		if stepIDs[step.StepId] {
			return errs.New(errs.KindValidationFailed, "execution plan step ids must be unique")
		}
		stepIDs[step.StepId] = true
	}
	return nil
}
