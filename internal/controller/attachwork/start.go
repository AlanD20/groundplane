package attachwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (owner *Inputs) AuthorizeAgentStart(ctx context.Context, view workqueue.ResumeView) (workqueue.Change, error) {
	task, item, attempt := view.Work.Task.Task, view.Work.Item.Item, view.Attempt.Attempt
	if attempt.Direction == workexecution.Recovery {
		if !view.CurrentExecutor || !view.ActiveAttempt || attempt.StartedAt != nil || task.CollectionStartedAt != nil ||
			workexecution.ValidateDelivery(item, attempt) != nil || !task.PublicationComplete {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Attach inspection no longer owns a new start grant")
		}
		_, guards, err := owner.Recovery(ctx, view.Work, attempt)
		return workqueue.Change{Conditions: guards}, err
	}
	if !view.CurrentExecutor || !view.ActiveAttempt || attempt.StartedAt != nil ||
		attempt.Direction != workexecution.Forward || item.Operation != workexecution.Attach ||
		workexecution.ValidateDelivery(item, attempt) != nil || task.FinishedAt != nil ||
		task.CollectionStartedAt != nil || !task.PublicationComplete || task.Stop != nil || item.ForwardRevoked {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Attach provisioning no longer owns a new start grant")
	}
	current, conditions, err := owner.repository.ReadDesiredMatch(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !current {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Attach provisioning was superseded before execution started")
	}
	bindings, err := owner.repository.PendingBindingConditions(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: append(conditions, bindings...)}, nil
}
