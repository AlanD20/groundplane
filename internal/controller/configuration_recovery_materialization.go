package controller

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (resolver *TaskMaterializationResolver) materializationReference(
	ctx context.Context,
	task etcd.TaskRecord,
	plan *agentpb.ExecutionPlan,
	step *agentpb.ExecutionStep,
) (etcd.TaskMaterializationRecord, etcd.TaskMaterializationRecord, error) {
	switch step.GetPolicy() {
	case agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
		agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE:
		return resolver.configurationRecovery.Resolve(ctx, task, plan, step.GetStepId())
	default:
		reference, err := taskMaterializationReference(task, step.GetStepId())
		return reference, reference, err
	}
}
