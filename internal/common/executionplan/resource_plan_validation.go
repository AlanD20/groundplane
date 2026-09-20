package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func validateArtifactFreeAdapterPlan(plan *agentpb.ExecutionPlan) error {
	if validateID(ids.KindAttach, plan.TargetId) != nil || len(plan.Steps) == 0 {
		return errs.New(errs.KindValidationFailed, "artifact-free adapter plan shape is invalid")
	}
	stepIDs := make(map[string]struct{}, len(plan.Steps))
	for _, step := range plan.Steps {
		if err := validateStep(plan, step, nil, nil); err != nil {
			return err
		}
		procedure := step.GetAdapterProcedure()
		if procedure == nil || procedure.AttachId != plan.TargetId {
			return errs.New(errs.KindValidationFailed, "artifact-free adapter plan target is invalid")
		}
		if _, duplicate := stepIDs[step.StepId]; duplicate {
			return errs.New(errs.KindValidationFailed, "execution plan step ids must be unique")
		}
		stepIDs[step.StepId] = struct{}{}
	}
	return nil
}

func validateArtifactFreeEnvironmentRemovePlan(plan *agentpb.ExecutionPlan) error {
	if plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_REMOVE ||
		validateID(ids.KindEnvironment, plan.TargetId) != nil || len(plan.Steps) != 1 {
		return errs.New(errs.KindValidationFailed, "artifact-free Environment remove plan shape is invalid")
	}
	step := plan.Steps[0]
	if err := validateStep(plan, step, nil, nil); err != nil {
		return err
	}
	remove := step.GetEnvironmentDirectoryRemove()
	if remove == nil || remove.EnvironmentId != plan.TargetId {
		return errs.New(errs.KindValidationFailed, "artifact-free Environment remove plan target is invalid")
	}
	return nil
}

func validateArtifactFreeManagedNetworkRemovePlan(plan *agentpb.ExecutionPlan) error {
	if plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_REMOVE ||
		validateID(ids.KindNetwork, plan.TargetId) != nil || len(plan.Steps) != 1 {
		return errs.New(errs.KindValidationFailed, "artifact-free managed network remove plan shape is invalid")
	}
	step := plan.Steps[0]
	if err := validateStep(plan, step, nil, nil); err != nil {
		return err
	}
	remove := step.GetManagedNetworkRemove()
	if remove == nil || remove.NetworkId != plan.TargetId {
		return errs.New(errs.KindValidationFailed, "managed network removal does not identify the plan target")
	}
	return nil
}

func validateEnvironmentCreatePlan(plan *agentpb.ExecutionPlan) error {
	if validateID(ids.KindEnvironment, plan.TargetId) != nil {
		return errs.New(errs.KindValidationFailed, "environment create target id is invalid")
	}
	if len(plan.Artifacts) != 0 || len(plan.Steps) != 1 {
		return errs.New(errs.KindValidationFailed, "environment create plan shape is invalid")
	}
	step := plan.Steps[0]
	if err := validateStep(plan, step, nil, nil); err != nil {
		return err
	}
	return validateEnvironmentDirectoryCreateTarget(plan, step)
}

func validateEnvironmentDirectoryCreateTarget(plan *agentpb.ExecutionPlan, step *agentpb.ExecutionStep) error {
	create := step.GetEnvironmentDirectoryCreate()
	if create.GetEnvironmentId() != plan.TargetId {
		return errs.New(errs.KindValidationFailed, "environment directory does not identify the plan target")
	}
	return validateVolumeDirectory(create.GetExpectedVolumeDir(), create.GetEnvironmentId())
}
