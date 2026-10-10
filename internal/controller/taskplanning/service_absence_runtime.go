package taskplanning

import (
	"slices"
	"strconv"

	"github.com/AlanD20/groundplane/internal/controller/taskplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func buildAuthoredServiceAbsencePlan(
	volumeRoot string,
	task etcd.TaskRecord,
	intent environmentchanges.ServiceRemovalIntent,
	artifact *agentpb.ComposeArtifact,
) (*agentpb.ExecutionPlan, error) {
	selected := make(map[string]bool)
	for _, member := range artifact.GetServices() {
		if member.GetServiceId() == intent.ServiceID {
			selected[member.GetComposeName()] = true
		}
	}
	if len(selected) == 0 {
		return nil, errs.New(errs.KindStateConflict, "authored Service absence artifact has no physical identity")
	}
	artifact, err := pruneServiceLifecycleArtifact(artifact, selected)
	if err != nil {
		return nil, err
	}
	source, err := serviceAbsenceRuntimeSource(artifact, task.Steps[0].ID)
	if err != nil {
		return nil, err
	}
	forwardID, probeID, compensateID := task.Steps[0].ID, task.Steps[1].ID, task.Steps[2].ID
	remove := func(stepID string, policy agentpb.ExecutionStepPolicy, prerequisite string) *agentpb.ExecutionStep {
		return &agentpb.ExecutionStep{
			StepId: stepID, TimeoutSeconds: uint32(task.TimeoutSeconds), Policy: policy,
			PrerequisiteStepId: prerequisite,
			Payload: &agentpb.ExecutionStep_ComposeRemove{ComposeRemove: &agentpb.ComposeRemove{
				ArtifactId: artifact.GetArtifactId(), ServiceIds: []string{intent.ServiceID},
			}},
		}
	}
	return taskplan.Build(taskplan.BuildInput{
		VolumeRoot: volumeRoot, PlanID: task.PlanID, RenderGeneration: uint64(task.RenderGeneration),
		Operation: agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY, TargetID: intent.ServiceID,
		Artifacts: []*agentpb.ComposeArtifact{artifact},
		Steps: []*agentpb.ExecutionStep{
			remove(forwardID, agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD, ""),
			{
				StepId: probeID, TimeoutSeconds: uint32(task.TimeoutSeconds),
				Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
				Payload: &agentpb.ExecutionStep_ServiceAbsenceProbe{ServiceAbsenceProbe: &agentpb.ServiceAbsenceProbe{
					ArtifactId: artifact.GetArtifactId(), ServiceId: intent.ServiceID,
				}},
			},
			remove(compensateID, agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE, forwardID),
		},
		ServiceLifecycleProcedure: &agentpb.ServiceLifecycleProcedure{
			Sources: []*agentpb.ServiceLifecycleSource{source},
		},
		ServiceAbsenceRecoveryProcedure: &agentpb.ServiceAbsenceRecoveryProcedure{
			ServiceId: intent.ServiceID, ArtifactId: artifact.GetArtifactId(), ForwardStepIds: []string{forwardID},
			ProbeStepId: probeID, CompensateStepId: compensateID,
		},
	})
}

func serviceAbsenceRuntimeSource(
	artifact *agentpb.ComposeArtifact,
	stepID string,
) (*agentpb.ServiceLifecycleSource, error) {
	if artifact == nil || len(artifact.GetServices()) == 0 {
		return nil, errs.New(errs.KindInternal, "Service absence artifact is empty")
	}
	source := &agentpb.ServiceLifecycleSource{
		ArtifactId: artifact.GetArtifactId(), ServiceId: artifact.GetServices()[0].GetServiceId(), StepId: stepID,
	}
	for _, member := range artifact.GetServices() {
		if member.GetServiceId() != source.GetServiceId() {
			return nil, errs.New(errs.KindStateConflict, "Service absence artifact contains a foreign member")
		}
		source.ComposeNames = append(source.ComposeNames, member.GetComposeName())
		planID, generation := "", ""
		for _, label := range member.GetExpectedLabels() {
			switch label.GetKey() {
			case "com.groundplane.plan-id":
				planID = label.GetValue()
			case "com.groundplane.render-generation":
				generation = label.GetValue()
			}
		}
		parsed, parseErr := strconv.ParseUint(generation, 10, 64)
		if parseErr != nil || parsed == 0 ||
			source.GetSourcePlanId() != "" && source.GetSourcePlanId() != planID ||
			source.GetSourceRenderGeneration() != 0 && source.GetSourceRenderGeneration() != parsed {
			return nil, errs.New(errs.KindStateConflict, "Service absence physical ownership is inconsistent")
		}
		source.SourcePlanId, source.SourceRenderGeneration = planID, parsed
	}
	slices.Sort(source.ComposeNames)
	return source, nil
}
