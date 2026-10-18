package network

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (owner *AgentWork) PreparePublication(ctx context.Context, task workqueue.TaskSnapshot, revision int64,
	at time.Time,
) (workdispatch.PublicationBatch, error) {
	if task.Task.PublicationComplete || at.IsZero() || at.Before(task.Task.UpdatedAt) {
		return workdispatch.PublicationBatch{}, errs.New(errs.KindStateConflict, "Zone publication timestamp predates its accepted Task")
	}
	graph, authority, err := owner.inputs.LoadCascadeGraph(ctx, task, revision)
	if err != nil {
		return workdispatch.PublicationBatch{}, err
	}
	_, _, rejection, rejected, err := owner.inputs.LoadCascadeRejection(ctx, task, revision)
	if err != nil {
		return workdispatch.PublicationBatch{}, err
	}
	if rejected {
		advanced, settled, cleanup, err := owner.inputs.ContinueCascadeRejection(ctx, task, graph, authority, revision, at)
		if err != nil {
			return workdispatch.PublicationBatch{}, err
		}
		if advanced {
			return workdispatch.PublicationBatch{Planning: true}, nil
		}
		return workdispatch.PublicationBatch{Rejected: true, Graph: settled, Authority: cleanup}, nil
	}
	authority.Conditions = append(authority.Conditions, rejection.Conditions...)
	if task.Task.Stop != nil || !at.Before(task.Task.Deadline) {
		if err := owner.inputs.RejectStoppedCascade(ctx, task, authority, at); err != nil {
			return workdispatch.PublicationBatch{}, err
		}
		return workdispatch.PublicationBatch{Planning: true}, nil
	}
	advanced, err := owner.inputs.PublishNextCascadeConsumer(ctx, task, graph, authority, revision, at)
	if err != nil {
		return workdispatch.PublicationBatch{}, err
	}
	if advanced {
		return workdispatch.PublicationBatch{Planning: true}, nil
	}
	return workdispatch.PublicationBatch{Graph: graph, Authority: authority}, nil
}
