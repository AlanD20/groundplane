package attachremoval

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareDetachRetention preserves authority independently of diagnostic
// lifetime. Unknown effects never reach this collector: the item must be fully
// settled and cleanup complete before either captured inputs or reports expire.
func (repository *InputRepository) PrepareDetachRetention(ctx context.Context, view workqueue.View, at time.Time, final bool) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Validate() != nil || item.Validate() != nil || view.Task.Revision <= 0 || view.Item.Revision <= 0 ||
		task.CollectionStartedAt == nil || task.FinishedAt == nil || task.RetainUntil == nil ||
		at.IsZero() || at.Location() != time.UTC || at.Before(*task.CollectionStartedAt) || at.Before(*task.RetainUntil) ||
		item.Operation != workexecution.Detach || item.TaskID != task.ID || item.Input == nil || item.Result == nil ||
		item.Phase != workexecution.Settled || item.ActiveAttemptID != "" ||
		item.Cleanup != workexecution.CleanupNone && item.Cleanup != workexecution.CleanupComplete {
		return false, workqueue.Change{}, errs.New(errs.KindStateConflict, "Detach history requires expired, fully settled execution")
	}
	capture, err := repository.LoadDetachCapture(ctx, view)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	defer capture.Clear()
	if capture.Revision <= 0 {
		return false, workqueue.Change{}, errs.New(errs.KindStateConflict, "executed Detach has lost its captured inputs")
	}
	change, err := repository.prepareRetiredDetach(ctx, view, capture.Input, at)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, capture.Conditions...)
	change.Conditions = append(change.Conditions,
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision})
	if !final {
		return true, change, nil
	}
	retired, pins, err := repository.retireDetachPins(ctx, view, capture, change.Conditions)
	if err != nil || !retired {
		keyvalue.ClearMutationValues(change.Mutations)
		return false, workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, pins.Conditions...)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: workqueue.AttemptIndexPrefix(item.ID), Prefix: true})
	change.Mutations = append(change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: DetachInputPrefix(item.ID), Prefix: true},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: WorkCaptureKey(item.ID)},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: WorkRecoveryKey(item.ID)})
	return true, change, nil
}

func (repository *InputRepository) retireDetachPins(ctx context.Context, view workqueue.View, capture DetachCapture, guards []keyvalue.Condition) (bool, workqueue.Change, error) {
	if capture.Capture.Pins != nil {
		return RetireCapturePins(ctx, repository.store, view.ReadRevision, capture.Capture, guards)
	}
	operationID := DetachOperation(view)
	// Capture and pin activation joined initial publication. A capture without
	// Secret sources must not have either a root or an unpublished preparation.
	return true, workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: tasksecretpins.RootKey(operationID)}, {Key: tasksecretpins.PreparationKey(operationID)},
		{Key: tasksecretpins.ReversePrefix(operationID), Prefix: true},
	}}, nil
}

func PrepareDetachAttemptRetirement(view workqueue.View, snapshot workqueue.AttemptSnapshot) (workqueue.Change, error) {
	item, attempt := view.Item.Item, snapshot.Attempt
	if item.Operation != workexecution.Detach || item.Input == nil || attempt.Input != *item.Input ||
		attempt.Executor.Executor != workexecution.Agent || attempt.Executor.HostID != item.Placement.HostID {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Detach attempt history differs from captured input or host")
	}
	prepared, err := workqueue.PrepareAttemptCollection(view.Task, view.Item, snapshot)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: prepared.Conditions, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationDelete, Key: WorkReportPrefix(attempt.ID), Prefix: true},
		{Type: keyvalue.MutationDelete, Key: WorkFailureKey(attempt.ID)},
		{Type: keyvalue.MutationDelete, Key: WorkInspectionKey(attempt.ID)},
	}}, nil
}
