package executionplan

import (
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func dependencyFreeServiceSelection(
	plan *agentpb.ExecutionPlan, step *agentpb.ExecutionStep, artifacts map[string]*agentpb.ComposeArtifact,
) bool {
	if blueprintManagedServiceSelection(plan.Operation, step.GetComposeApply(), artifacts) {
		return true
	}
	if _, selected, err := AttachMutationServices(plan, step.GetStepId()); selected {
		return err == nil
	}
	_, selected, err := VolumeRemovalServices(plan, step.StepId)
	return selected && err == nil
}

// VolumeRemovalServices selects runtime names from the sealed baseline mounts,
// not every proxy/slot sharing a logical Service id. The boolean distinguishes
// unrelated operations from a removal with no deployed consumers.
func VolumeRemovalServices(plan *agentpb.ExecutionPlan, stepID string) ([]string, bool, error) {
	if plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_REMOVE ||
		ids.Validate(ids.KindVolume, plan.GetTargetId()) != nil {
		return nil, false, nil
	}
	invalid := func() ([]string, bool, error) {
		return nil, true, errs.New(errs.KindValidationFailed, "Volume removal consumer selection is invalid")
	}
	if len(plan.Steps) != 3 || len(plan.Artifacts) != 2 || plan.Steps[0].GetStepId() != stepID {
		return invalid()
	}
	apply := plan.Steps[0].GetComposeApply()
	remove := plan.Steps[1].GetManagedVolumeRemove()
	directory := plan.Steps[2].GetManagedVolumeDirectoryRemove()
	if apply == nil || apply.FullReconcile || apply.ForceRecreate || !apply.NoDependencies ||
		remove.GetVolumeId() != plan.TargetId || directory.GetVolumeId() != plan.TargetId {
		return invalid()
	}
	var baseline, candidate *agentpb.ComposeArtifact
	for _, artifact := range plan.Artifacts {
		if artifact.GetArtifactId() == directory.ArtifactId {
			baseline = artifact
		}
		if artifact.GetArtifactId() == apply.ArtifactId {
			candidate = artifact
		}
	}
	if remove.DockerName != "gp_vol_"+strings.ToLower(plan.TargetId) {
		return invalid()
	}
	names, err := (volumeexecution.Removal{
		VolumeID: plan.TargetId, Key: directory.ComposeKey,
		Baseline: baseline, Candidate: candidate, ConsumerIDs: apply.ServiceIds,
	}).Consumers()
	return names, true, err
}
