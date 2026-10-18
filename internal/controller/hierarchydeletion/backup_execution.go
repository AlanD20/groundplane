package hierarchydeletion

import (
	"context"

	"github.com/AlanD20/groundplane/internal/controller/backup"

	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (owner *BackupWork) AuthorizeStart(ctx context.Context, delivery workqueue.Delivery) (workqueue.Change, error) {
	attempt := delivery.Attempt.Attempt
	view, err := owner.queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil {
		return workqueue.Change{}, err
	}
	return owner.records.PrepareHierarchyBackupStart(ctx, view)
}

func (owner *BackupWork) Execute(ctx context.Context, delivery workqueue.Delivery) error {
	attempt := delivery.Attempt.Attempt
	view, err := owner.queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil {
		return err
	}
	execution, err := owner.records.LoadHierarchyBackupExecution(ctx, view)
	if err != nil {
		return err
	}
	reporter, err := backup.NewRemovalReporter(owner.diagnostics, view.Attempt.Attempt)
	if err != nil {
		return err
	}
	return owner.cleanup.Execute(ctx, execution, reporter)
}

func (owner *BackupWork) Reconcile(ctx context.Context, delivery workqueue.Delivery) error {
	attempt := delivery.Attempt.Attempt
	view, err := owner.queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil {
		return err
	}
	execution, err := owner.records.LoadHierarchyBackupObservation(ctx, view)
	if err != nil {
		return err
	}
	return owner.cleanup.Reconcile(ctx, execution)
}
