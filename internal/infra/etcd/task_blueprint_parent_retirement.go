package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RetireSupersededBlueprintParent terminalizes a running parent only after a
// newer head has replaced it and every child claim it owned is gone. Ledger
// execution records remain until stop and exact effect accounting are proven.
func (repository *TaskRepository) RetireSupersededBlueprintParent(
	ctx context.Context, environmentID, taskID string, at time.Time,
) (keyvalue.Versioned[TaskRecord], error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || ids.Validate(ids.KindTask, taskID) != nil {
		return keyvalue.Versioned[TaskRecord]{}, errs.New(errs.KindValidationFailed, "Blueprint parent identity is invalid")
	}
	if err := recordcodec.ValidateTimestamp("Blueprint parent retirement", at); err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	ledger, err := blueprintunits.NewRepository(repository.store)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	snapshot, err := ledger.Load(ctx, environmentID)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	if snapshot.HeadTaskID == taskID {
		return keyvalue.Versioned[TaskRecord]{}, errs.New(errs.KindStateConflict, "Blueprint parent is still current")
	}
	for _, execution := range snapshot.Executions {
		if execution.Record.ParentTaskID == taskID {
			return keyvalue.Versioned[TaskRecord]{}, errs.New(errs.KindResourceInUse, "Blueprint parent has unsettled child effects")
		}
	}
	for _, applied := range snapshot.Applied {
		if applied.Record.ParentTaskID == taskID && applied.Record.State == blueprintunits.Uncertain {
			return keyvalue.Versioned[TaskRecord]{}, errs.New(errs.KindResourceInUse, "Blueprint parent has unknown child effects")
		}
	}
	return repository.terminalizeBlueprintParent(ctx, snapshot, taskID, taskjournal.TaskStatusAborted, at)
}
