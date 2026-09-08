package controller

import (
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// BlueprintManagedServiceSteps starts only Component-owned Services. Native
// workload decisions remain exclusively in the candidate Release procedure.
// Identities derive from the immutable plan and artifact for exact replay.
func BlueprintManagedServiceSteps(
	task etcd.TaskRecord,
	artifact *agentpb.ComposeArtifact,
	prerequisite string,
	releaseForward bool,
) ([]*agentpb.ExecutionStep, error) {
	serviceIDs := []string{}
	for _, service := range artifact.GetServices() {
		if service.GetOwnerComponentId() == "" {
			continue
		}
		if service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_UNSPECIFIED ||
			ids.Validate(
				ids.KindComponent,
				service.GetOwnerComponentId(),
			) != nil || ids.Validate(ids.KindService, service.GetServiceId()) != nil {
			return nil, errs.New(errs.KindInternal, "Blueprint managed Service ownership is invalid")
		}
		serviceIDs = append(serviceIDs, service.GetServiceId())
	}
	if len(serviceIDs) == 0 {
		return nil, nil
	}
	sort.Strings(serviceIDs)
	planTime, err := ids.Timestamp(ids.KindPlan, task.PlanID)
	if err != nil {
		return nil, err
	}
	steps := make([]*agentpb.ExecutionStep, 0, len(serviceIDs)*2)
	for _, serviceID := range serviceIDs {
		applyID := ids.DeriveAt(ids.KindStep, planTime, task.PlanID, "blueprint-managed-apply:"+serviceID)
		// Ordinary targeted up reconciles the sealed Component configuration;
		// forced candidate replacement belongs only to native Release procedures.
		apply := &agentpb.ExecutionStep{
			StepId:             applyID,
			PrerequisiteStepId: prerequisite,
			TimeoutSeconds:     uint32(task.TimeoutSeconds),
			Payload: &agentpb.ExecutionStep_ComposeApply{
				ComposeApply: &agentpb.ComposeApply{
					ArtifactId:     artifact.GetArtifactId(),
					ServiceIds:     []string{serviceID},
					NoDependencies: true,
				},
			},
		}
		if releaseForward {
			apply.Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD
		}
		steps = append(steps, apply)
		prerequisite = applyID
	}
	// Health observes the whole sealed project. First reconcile every managed
	// Service so a still-prior Component cannot collide with current ownership.
	for _, serviceID := range serviceIDs {
		healthID := ids.DeriveAt(ids.KindStep, planTime, task.PlanID, "blueprint-managed-health:"+serviceID)
		health := &agentpb.ExecutionStep{
			StepId:             healthID,
			PrerequisiteStepId: prerequisite,
			TimeoutSeconds:     uint32(task.TimeoutSeconds),
			Payload: &agentpb.ExecutionStep_WaitHealthy{
				WaitHealthy: &agentpb.WaitHealthy{
					ArtifactId: artifact.GetArtifactId(),
					ServiceIds: []string{serviceID},
				},
			},
		}
		if releaseForward {
			health.Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD
		}
		steps = append(steps, health)
		prerequisite = healthID
	}
	return steps, nil
}

func blueprintManagedStepsMatch(task etcd.TaskRecord, start int, steps []*agentpb.ExecutionStep) error {
	if start < 0 || start > len(task.Steps) || len(steps) > len(task.Steps)-start {
		return errs.New(errs.KindInternal, "durable Blueprint managed Service steps are incomplete")
	}
	for index, step := range steps {
		if task.Steps[start+index].ID != step.StepId || task.Steps[start+index].Kind != etcd.TaskStepOperation {
			return errs.New(errs.KindInternal, "durable Blueprint managed Service steps changed")
		}
	}
	return nil
}
