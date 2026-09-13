package executionplan

import (
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Compose application has one selection contract, including the closed
// validation-only Attach case for workloads that must remain stopped.
func validateComposeApply(
	plan *agentpb.ExecutionPlan,
	step *agentpb.ExecutionStep,
	artifacts map[string]*agentpb.ComposeArtifact,
) error {
	apply, operation := step.GetComposeApply(), plan.GetOperation()
	if apply == nil {
		return errs.New(errs.KindValidationFailed, "Compose apply payload is empty")
	}
	selected := apply.ServiceIds
	attachNoop := (operation == agentpb.PlanOperation_PLAN_OPERATION_ATTACH ||
		operation == agentpb.PlanOperation_PLAN_OPERATION_DETACH) &&
		!apply.FullReconcile && len(selected) == 0 &&
		apply.NoDependencies && !apply.ForceRecreate
	if !attachNoop && apply.FullReconcile == (len(selected) != 0) {
		return errs.New(errs.KindValidationFailed, "Compose apply selection is inconsistent")
	}
	if apply.FullReconcile &&
		(apply.ForceRecreate || apply.NoDependencies) ||
		apply.NoDependencies && !apply.ForceRecreate &&
			!dependencyFreeServiceSelection(plan, step, artifacts) {
		return errs.New(errs.KindValidationFailed, "Compose apply replacement options are inconsistent")
	}
	if attachNoop {
		if artifacts[apply.ArtifactId] == nil {
			return errs.New(errs.KindValidationFailed, "Attach no-op artifact is absent")
		}
		return nil
	}
	return validateSelection(apply.ArtifactId, selected, artifacts, false)
}
