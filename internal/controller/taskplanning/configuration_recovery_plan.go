package taskplanning

import (
	"context"
	taskmaterialization "github.com/AlanD20/groundplane/internal/controller/taskmaterialization"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Project the feature-owned source selection into the existing file wire shape.
func (resolver *TaskPlanResolver) configurationRecoverySteps(ctx context.Context, task etcd.TaskRecord,
	artifactID string) (*agentpb.ConfigurationRestoration, []*agentpb.ExecutionStep, error) {
	prepared, err := resolver.configurationRecovery.Prepare(ctx, task)
	if err != nil {
		return nil, nil, err
	}
	steps := make([]*agentpb.ExecutionStep, 0, len(prepared.Files)*2)
	for index, file := range prepared.Files {
		probe, err := taskmaterialization.BuildTaskMaterializationStep(file.Probe, artifactID, uint32(task.TimeoutSeconds))
		if err != nil {
			return nil, nil, err
		}
		compensate, err := taskmaterialization.BuildTaskMaterializationStep(file.Compensate, artifactID, uint32(task.TimeoutSeconds))
		if err != nil {
			return nil, nil, err
		}
		probe.Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE
		compensate.Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE
		compensate.PrerequisiteStepId = prepared.Procedure.Files[index].ForwardStepId
		steps = append(steps, probe, compensate)
	}
	return prepared.Procedure, steps, nil
}

func (resolver *TaskPlanResolver) configurationRecoverySuffix(
	ctx context.Context,
	task etcd.TaskRecord,
	start int,
) error {
	prepared, err := resolver.configurationRecovery.Prepare(ctx, task)
	if err != nil {
		return err
	}
	if start < 0 || start > len(task.Steps) || len(task.Steps)-start != len(prepared.Files)*2 {
		return errs.New(errs.KindInternal, "durable Blueprint configuration recovery suffix is invalid")
	}
	for _, file := range prepared.Files {
		for _, id := range []string{file.Probe.StepID, file.Compensate.StepID} {
			if task.Steps[start].ID != id || task.Steps[start].Kind != etcd.TaskStepOperation {
				return errs.New(errs.KindInternal, "durable Blueprint configuration recovery order is invalid")
			}
			start++
		}
	}
	return nil
}
