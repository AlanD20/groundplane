package etcd

import (
	"context"
	"math"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// prepareBlueprintChildAssignmentEpoch joins assignment replacement to its
// resource claim. Recovery keeps the original claim even after supersession;
// a forward reconnect still requires the parent's current desired head.
func (repository *TaskRepository) prepareBlueprintChildAssignmentEpoch(
	ctx context.Context, task TaskRecord, epoch uint32, recovery bool,
) (blueprintunits.MutationPlan, error) {
	if !taskjournal.IsBlueprintChild(task.Params) {
		return blueprintunits.MutationPlan{}, nil
	}
	if epoch == 0 || epoch == math.MaxUint32 {
		return blueprintunits.MutationPlan{}, errs.New(
			errs.KindStateConflict,
			"Blueprint child assignment epoch is exhausted",
		)
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
		current := execution.Record
		if current.PlanID != task.PlanID {
			continue
		}
		if current.TaskID != task.ID || current.ParentTaskID != task.Params[taskjournal.TaskBlueprintParentParam] ||
			current.Epoch != int64(
				epoch,
			) || current.State != blueprintunits.Running && current.State != blueprintunits.Draining ||
			!recovery && (current.State != blueprintunits.Running || snapshot.HeadTaskID != current.ParentTaskID) {
			return blueprintunits.MutationPlan{}, errs.New(
				errs.KindStateConflict,
				"Blueprint child assignment no longer matches its resource claim",
			)
		}
		current.Epoch++
		if recovery {
			current.State = blueprintunits.Draining
		}
		return blueprintunits.PrepareMutation(
			snapshot,
			nil,
			[]blueprintunits.ExecutionChange{{PlanID: task.PlanID, Next: &current}},
		)
	}
	return blueprintunits.MutationPlan{}, errs.New(
		errs.KindStateConflict,
		"Blueprint child resource claim is unavailable",
	)
}
