package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// preparePendingBlueprintChildAbort withdraws a unit that never acquired an
// Agent assignment. Its ledger claim must disappear in the same transaction
// as the pending Task's queue and marker transition.
func (repository *TaskRepository) preparePendingBlueprintChildAbort(
	ctx context.Context, task TaskRecord,
) (blueprintunits.MutationPlan, error) {
	if !taskjournal.IsBlueprintChild(task.Params) {
		return blueprintunits.MutationPlan{}, nil
	}
	if task.Status != taskjournal.TaskStatusPending {
		return blueprintunits.MutationPlan{}, errs.New(errs.KindStateConflict, "Blueprint child was already assigned")
	}
	ledger, err := blueprintunits.NewRepository(repository.store)
	if err != nil {
		return blueprintunits.MutationPlan{}, err
	}
	snapshot, err := ledger.Load(ctx, task.Owner.EnvironmentID)
	if err != nil {
		return blueprintunits.MutationPlan{}, err
	}
	for _, execution := range snapshot.Executions {
		if execution.Record.PlanID != task.PlanID {
			continue
		}
		if execution.Record.TaskID != task.ID ||
			execution.Record.ParentTaskID != task.Params[taskjournal.TaskBlueprintParentParam] ||
			execution.Record.State != blueprintunits.Pending {
			return blueprintunits.MutationPlan{}, errs.New(errs.KindStateConflict, "Blueprint child claim changed")
		}
		return blueprintunits.PrepareMutation(snapshot, nil, []blueprintunits.ExecutionChange{{PlanID: task.PlanID}})
	}
	return blueprintunits.MutationPlan{}, errs.New(errs.KindStateConflict, "Blueprint child claim is missing")
}
