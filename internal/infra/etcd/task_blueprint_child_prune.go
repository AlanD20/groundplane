package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// A terminal child can still own an uncertain resource claim. Its journal and
// plan must remain available until the coordinator has settled that claim.
func (repository *TaskRepository) blueprintChildPruneHeld(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) (bool, error) {
	if !taskjournal.IsBlueprintChild(task.Params) {
		return false, nil
	}
	key := blueprintunits.ExecutionKey(task.Owner.EnvironmentID, task.PlanID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{key}, Revision: revision,
	})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return false, errs.New(errs.KindInternal, "Blueprint child prune claim read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return false, nil
	}
	claim, err := blueprintunits.DecodeExecution(read.Values[0].Value)
	if err != nil || claim.EnvironmentID != task.Owner.EnvironmentID ||
		claim.PlanID != task.PlanID || claim.TaskID != task.ID ||
		claim.ParentTaskID != task.Params[taskjournal.TaskBlueprintParentParam] {
		return false, errs.New(errs.KindInternal, "Blueprint child prune claim changed")
	}
	return true, nil
}
