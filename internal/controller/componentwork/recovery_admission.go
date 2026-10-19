package componentwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/componentwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (owner *AgentWork) admitRecovery(
	ctx context.Context,
	request workqueue.Assignment,
	at time.Time,
) (workdispatch.Decision, error) {
	item := request.Item.Item
	if request.DecisionOnly || item.Operation != workexecution.Component ||
		item.Placement.Executor != workexecution.Agent ||
		item.Phase != workexecution.Ready ||
		item.ActiveAttemptID != "" ||
		at.IsZero() ||
		at.Before(item.UpdatedAt) ||
		item.Cleanup != workexecution.CleanupRequired && item.Cleanup != workexecution.CleanupStopped {
		return workdispatch.Decision{}, errs.New(
			errs.KindStateConflict,
			"Component recovery cannot obtain this assignment",
		)
	}
	if !at.Before(item.Deadline) {
		change, err := workqueue.PrepareCleanupRetryExpiry(request.Task, request.Item, at)
		return workdispatch.Decision{Disposition: workdispatch.NeedsAttention, Change: change}, err
	}
	if request.Executor.Record.Identity.HostID != item.Placement.HostID {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Component recovery belongs to another host")
	}
	view := workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision}
	guards, err := owner.inputs.ExecutionSecretConditions(ctx, view)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	source, err := owner.inputs.LoadRecovery(ctx, view)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	guards = append(guards, keyvalue.Condition{Key: componentrecord.RecoveryKey(item.ID), ModRevision: source.Revision})
	return workdispatch.Decision{
		Disposition: workdispatch.Dispatch,
		Deadline:    item.Deadline,
		Change:      workqueue.Change{Conditions: guards},
	}, nil
}
