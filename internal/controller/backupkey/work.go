package backupkey

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Work owns the queued atomic key decision. It has no external effect to replay
// and never needs a fabricated executor attempt or a generic Task runner.
type Work struct {
	repository Repository
	queue      *workqueue.Repository
}

func NewWork(service *Service, queue *workqueue.Repository) (*Work, error) {
	if service == nil || queue == nil {
		return nil, errs.New(errs.KindInternal, "backup key work requires its service and execution storage")
	}
	return &Work{repository: service.repository, queue: queue}, nil
}

func (owner *Work) Admit(ctx context.Context, request workqueue.Assignment, at time.Time) (workdispatch.Decision, error) {
	if !request.DecisionOnly || request.Direction != workexecution.Forward {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "backup key rotation requires an atomic Controller decision")
	}
	task, item := request.Task.Task, request.Item.Item
	outcome := workexecution.Succeeded
	if task.Stop != nil {
		outcome = task.Stop.Outcome
	} else if item.ForwardRevoked {
		outcome = workexecution.Superseded
	} else if !at.Before(item.Deadline) || !at.Before(task.Deadline) {
		outcome = workexecution.Failed
	}
	change, err := owner.repository.PrepareBackupKeyRotationSettlement(ctx,
		workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision}, outcome, at)
	return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
}

func (owner *Work) Stop(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot, at time.Time) (workqueue.Change, error) {
	if task.Task.Stop == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "backup key rotation has no stop request")
	}
	view, err := owner.queue.ReadWork(ctx, task.Task.ID, item.Item.ID, 0)
	if err != nil {
		return workqueue.Change{}, err
	}
	if view.Task.Revision != task.Revision || view.Item.Revision != item.Revision {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "backup key rotation changed before Stop")
	}
	return owner.repository.PrepareBackupKeyRotationSettlement(ctx, view, task.Task.Stop.Outcome, at)
}

func (owner *Work) PrepareFinish(ctx context.Context, finished workqueue.FinishedWork, _ time.Time) (workqueue.Change, error) {
	return owner.repository.PrepareBackupKeyRotationFinish(ctx, finished)
}

func (owner *Work) RetireWork(ctx context.Context, view workqueue.View, at time.Time, final bool) (bool, workqueue.Change, error) {
	return owner.repository.PrepareBackupKeyRotationRetention(ctx, view, at, final)
}

func (owner *Work) PrepareAttemptRetirement(_ workqueue.View, _ workqueue.AttemptSnapshot) (workqueue.Change, error) {
	return workqueue.Change{}, errs.New(errs.KindStateConflict, "atomic key rotation cannot own execution attempts")
}
