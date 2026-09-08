package controller

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ResolveExecutionPlan reconstructs the immutable plan and verifies its durable
// Component mutation projection before an assignment can be dispatched.
func (resolver *TaskPlanResolver) ResolveExecutionPlan(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	plan, err := resolver.resolveExecutionPlan(ctx, task)
	if err != nil || plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY {
		return plan, err
	}
	stepIDs, err := executionplan.ComponentActionStepIDs(plan)
	if err != nil {
		return nil, err
	}
	if !slices.Equal(stepIDs, task.ComponentActionStepIDs) {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Component action authority differs from persisted Task")
	}
	return plan, nil
}
