package taskplanning

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type blueprintComponentExecutionReader interface {
	GetBlueprintComponentExecution(context.Context, string) (keyvalue.Versioned[componentplanning.ChildExecution], error)
}

func (resolver *TaskPlanResolver) resolveBlueprintComponentUnitPlan(ctx context.Context, task etcd.TaskRecord) (*agentpb.ExecutionPlan, error) {
	reader, ok := resolver.blueprints.(blueprintComponentExecutionReader)
	if !ok {
		return nil, errs.New(errs.KindInternal, "Blueprint Component execution reader is not configured")
	}
	stored, err := reader.GetBlueprintComponentExecution(ctx, task.ID)
	if err != nil {
		return nil, err
	}
	record := stored.Record
	if err := record.ValidateTaskInputs(task.Configuration, task.Materializations); err != nil {
		return nil, err
	}
	plan, err := record.ExecutionPlan()
	if err != nil {
		return nil, err
	}
	if record.ParentTaskID != task.Params[taskjournal.TaskBlueprintParentParam] ||
		record.EnvironmentID != task.Owner.EnvironmentID || record.Component.Desired.ID != task.Params[taskjournal.TaskBlueprintComponentUnitParam] ||
		plan.GetPlanId() != task.PlanID || plan.GetTargetId() != task.Target ||
		plan.GetRenderGeneration() != uint64(task.RenderGeneration) || hex.EncodeToString(plan.GetPlanHash()) != task.PlanHash ||
		len(plan.GetSteps()) != len(task.Steps) {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Component execution differs from the sealed Task")
	}
	for index, step := range plan.GetSteps() {
		if task.Steps[index].Kind != taskjournal.TaskStepOperation || task.Steps[index].ID != step.GetStepId() ||
			int64(step.GetTimeoutSeconds()) > task.TimeoutSeconds {
			return nil, errs.New(errs.KindStateConflict, "Blueprint Component execution steps changed")
		}
	}
	if err := executionplan.AuthorizeVolumeDirectories(plan, resolver.volumeRoot); err != nil {
		return nil, err
	}
	return plan, nil
}
