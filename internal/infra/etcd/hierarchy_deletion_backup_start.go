package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionbackupcleanup"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyDeletionRepository) PrepareHierarchyBackupAdmission(ctx context.Context, view workqueue.View, direction workexecution.Direction) (workqueue.Change, error) {
	item, task := view.Item.Item, view.Task.Task
	if task.Action != workexecution.ActionRemove || !task.PublicationComplete || task.FinishedAt != nil ||
		(direction != workexecution.Forward && direction != workexecution.Recovery) ||
		(direction == workexecution.Forward && (task.Stop != nil || item.ForwardRevoked)) ||
		item.Operation != workexecution.RemovalBackupObject || item.Placement.Executor != workexecution.Controller ||
		item.TaskID != task.ID {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "backup object removal is no longer executable")
	}
	captured, err := repository.LoadHierarchyWork(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	if direction == workexecution.Recovery {
		if item.Result == nil || (item.Cleanup != workexecution.CleanupRequired && item.Cleanup != workexecution.CleanupStopped && item.Cleanup != workexecution.CleanupRunning) {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "backup cleanup has no retained failed removal")
		}
		outcome, err := hierarchydeletionbackupcleanup.NewRepository(repository.store).ReadOutcome(ctx, captured.Operation, captured.Action, view.ReadRevision)
		if err != nil {
			return workqueue.Change{}, err
		}
		if !outcome.DeleteGranted {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "backup cleanup has no previously authorized delete")
		}
		captured.Conditions = append(captured.Conditions, outcome.Conditions...)
	}
	conditions, err := hierarchydeletionbackupcleanup.NewRepository(repository.store).
		PrepareAdmission(ctx, captured.Operation, captured.Action)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: append(captured.Conditions, conditions...)}, nil
}

// PrepareHierarchyBackupStart is joined with shared Start, which compares the
// Controller generation, active attempt, claims, Task stop and deadline.
func (repository *HierarchyDeletionRepository) PrepareHierarchyBackupStart(ctx context.Context, view workqueue.ResumeView) (workqueue.Change, error) {
	item, attempt := view.Work.Item.Item, view.Attempt.Attempt
	if !view.CurrentExecutor || !view.ActiveAttempt || attempt.StartedAt != nil ||
		workexecution.ValidateDelivery(item, attempt) != nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "backup object removal no longer owns Start authority")
	}
	return repository.PrepareHierarchyBackupAdmission(ctx, view.Work, attempt.Direction)
}

func (repository *HierarchyDeletionRepository) LoadHierarchyBackupExecution(ctx context.Context, view workqueue.ResumeView) (hierarchydeletionbackupcleanup.Execution, error) {
	captured, err := repository.LoadHierarchyWork(ctx, view.Work)
	if err != nil {
		return hierarchydeletionbackupcleanup.Execution{}, err
	}
	return hierarchydeletionbackupcleanup.NewExecution(view, captured.Operation, captured.Action)
}

func (repository *HierarchyDeletionRepository) LoadHierarchyBackupObservation(ctx context.Context, view workqueue.ResumeView) (hierarchydeletionbackupcleanup.Execution, error) {
	captured, err := repository.LoadHierarchyWork(ctx, view.Work)
	if err != nil {
		return hierarchydeletionbackupcleanup.Execution{}, err
	}
	return hierarchydeletionbackupcleanup.NewObservation(view, captured.Operation, captured.Action)
}
