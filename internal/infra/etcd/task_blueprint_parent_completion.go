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

// CompleteBlueprintParent requires a sealed current plan whose every unit is
// backed by an acknowledged applied receipt. A child Task's terminal status
// alone cannot satisfy a unit or release the parent claim.
func (repository *TaskRepository) CompleteBlueprintParent(
	ctx context.Context, environmentID, taskID string, at time.Time,
) (keyvalue.Versioned[TaskRecord], error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || ids.Validate(ids.KindTask, taskID) != nil {
		return keyvalue.Versioned[TaskRecord]{}, errs.New(errs.KindValidationFailed, "Blueprint parent identity is invalid")
	}
	if err := recordcodec.ValidateTimestamp("Blueprint parent completion", at); err != nil {
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
	if snapshot.HeadTaskID != taskID || snapshot.Desired == nil ||
		snapshot.Desired.Record.ParentTaskID != taskID || !snapshot.Desired.Record.Complete {
		return keyvalue.Versioned[TaskRecord]{}, errs.New(errs.KindStateConflict, "Blueprint parent has no sealed current plan")
	}
	if len(snapshot.Executions) != 0 {
		return keyvalue.Versioned[TaskRecord]{}, errs.New(errs.KindResourceInUse, "Blueprint parent has unsettled unit effects")
	}
	for _, applied := range snapshot.Applied {
		if applied.Record.State == blueprintunits.Uncertain || applied.Record.State == blueprintunits.Diverged {
			return keyvalue.Versioned[TaskRecord]{}, errs.New(errs.KindResourceInUse, "Blueprint has unresolved unit effects")
		}
	}
	selection, err := blueprintunits.Select(snapshot)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	if len(selection.Satisfied) != len(snapshot.Desired.Record.Units) ||
		len(selection.Ready)+len(selection.Waiting)+len(selection.ResolveEffects)+
			len(selection.CancelPending)+len(selection.CancelRunning)+
			len(selection.ContinuePending)+len(selection.ContinueRunning) != 0 {
		return keyvalue.Versioned[TaskRecord]{}, errs.New(errs.KindStateConflict, "Blueprint parent has unfinished desired units")
	}
	return repository.terminalizeBlueprintParent(ctx, snapshot, taskID, taskjournal.TaskStatusCompleted, at)
}
