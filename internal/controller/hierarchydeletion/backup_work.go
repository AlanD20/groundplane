package hierarchydeletion

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/backup"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BackupWork admits remote object deletion as real Controller execution. Unlike
// local metadata finalizers, it consumes an attempt and retains external evidence.
type BackupWork struct {
	records     *etcd.HierarchyDeletionRepository
	queue       *workqueue.Repository
	cleanup     *backup.DeletionCleanup
	diagnostics *executiondiagnostics.Repository
}

func NewBackupWork(
	records *etcd.HierarchyDeletionRepository,
	queue *workqueue.Repository,
	cleanup *backup.DeletionCleanup,
	diagnostics *executiondiagnostics.Repository,
) (*BackupWork, error) {
	if records == nil || queue == nil || cleanup == nil || diagnostics == nil {
		return nil, errs.New(errs.KindInternal, "backup removal requires captured storage")
	}
	return &BackupWork{records: records, queue: queue, cleanup: cleanup, diagnostics: diagnostics}, nil
}

func (owner *BackupWork) Admit(
	ctx context.Context,
	request workqueue.Assignment,
	at time.Time,
) (workdispatch.Decision, error) {
	item, task := request.Item.Item, request.Task.Task
	if request.DecisionOnly ||
		(request.Direction != workexecution.Forward && request.Direction != workexecution.Recovery) ||
		item.Operation != workexecution.RemovalBackupObject ||
		item.Placement.Executor != workexecution.Controller ||
		item.Phase != workexecution.Ready ||
		item.ActiveAttemptID != "" ||
		at.IsZero() ||
		at.Before(item.UpdatedAt) {
		return workdispatch.Decision{}, errs.New(
			errs.KindStateConflict,
			"backup object removal cannot obtain this assignment",
		)
	}
	if task.Stop != nil && request.Direction == workexecution.Forward {
		change, err := owner.Stop(ctx, request.Task, request.Item, at)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	renewal, err := owner.records.PrepareHierarchyDeadline(ctx, request, at)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	if len(renewal.Mutations) != 0 {
		return workdispatch.Decision{Disposition: workdispatch.Advance, Change: renewal}, nil
	}
	if !at.Before(item.Deadline) {
		if request.Direction == workexecution.Recovery {
			change, err := workqueue.PrepareCleanupRetryExpiry(request.Task, request.Item, at)
			return workdispatch.Decision{Disposition: workdispatch.NeedsAttention, Change: change}, err
		}
		if item.AttemptNumber != 0 {
			_, previous, err := owner.queue.ReadAttempt(ctx, task.ID, item.ID, item.AttemptNumber, request.ReadRevision)
			if err != nil {
				return workdispatch.Decision{}, err
			}
			if previous.Attempt.FinishedAt == nil || previous.Attempt.Result == nil {
				return workdispatch.Decision{}, errs.New(
					errs.KindStateConflict,
					"expired backup Retry has no saved preceding result",
				)
			}
			change, err := workqueue.PrepareQueuedRetrySettlement(request.Task, request.Item, workexecution.Completion{
				Outcome: workexecution.Failed, Evidence: *previous.Attempt.Result, EffectsSettled: true,
			}, at)
			if err != nil {
				return workdispatch.Decision{}, err
			}
			change.Conditions = append(change.Conditions, keyvalue.Condition{
				Key: workqueue.AttemptKey(previous.Attempt.ID), ModRevision: previous.Revision})
			return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, nil
		}
		change, err := owner.records.PrepareHierarchyUnstartedStop(
			ctx,
			request.Task,
			request.Item,
			workexecution.Failed,
			at,
		)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	change, err := owner.records.PrepareHierarchyBackupAdmission(
		ctx,
		workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision},
		request.Direction,
	)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	deadline := at.Add(30 * time.Second)
	if item.Deadline.Before(deadline) {
		deadline = item.Deadline
	}
	if task.Deadline.Before(deadline) {
		deadline = task.Deadline
	}
	return workdispatch.Decision{Disposition: workdispatch.Dispatch, Deadline: deadline, Change: change}, nil
}

func (owner *BackupWork) Stop(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	item workqueue.ItemSnapshot,
	at time.Time,
) (workqueue.Change, error) {
	if task.Task.Stop == nil || item.Item.Operation != workexecution.RemovalBackupObject ||
		item.Item.Placement.Executor != workexecution.Controller || item.Item.TaskID != task.Task.ID {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"backup object removal has no matching stop request",
		)
	}
	if item.Item.ActiveAttemptID == "" && item.Item.Cleanup == workexecution.CleanupNone {
		return owner.records.PrepareHierarchyUnstartedStop(ctx, task, item, task.Task.Stop.Outcome, at)
	}
	return workqueue.PrepareRevocation(task, item, task.Task.Stop.Outcome, at)
}

func (owner *BackupWork) PrepareWorkRetry(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot,
	previous workqueue.AttemptSnapshot, revision int64, at time.Time,
) (workqueue.Change, error) {
	return owner.records.PrepareHierarchyBackupRetry(
		ctx,
		workqueue.View{Task: task, Item: item, ReadRevision: revision},
		previous,
		at,
	)
}
