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
	return workdispatch.PublicationBatch{Graph: graph, Authority: authority}, nil
}
