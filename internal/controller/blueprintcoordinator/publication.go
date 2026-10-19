package blueprintcoordinator

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Publication struct{ records *blueprintunits.Repository }

func NewPublication(records *blueprintunits.Repository) (*Publication, error) {
	if records == nil {
		return nil, errs.New(errs.KindInternal, "Blueprint publication requires captured work storage")
	}
	return &Publication{records: records}, nil
}

func (publication *Publication) PreparePublication(ctx context.Context, task workqueue.TaskSnapshot,
	revision int64, at time.Time,
) (workdispatch.PublicationBatch, error) {
	if task.Revision <= 0 || task.Revision > revision || at.Before(task.Task.UpdatedAt) {
		return workdispatch.PublicationBatch{}, errs.New(
			errs.KindStateConflict,
			"Blueprint publication snapshot is invalid",
		)
	}
	graph, err := publication.records.LoadCapturedGraph(ctx, task.Task, revision)
	if err != nil {
		return workdispatch.PublicationBatch{}, err
	}
	// Even a superseded Task finishes publishing its captured membership so the
	// stop walker can settle it. Dispatch independently checks the stop boundary.
	return workdispatch.PublicationBatch{
		Graph: graph.Record.Items,
		Authority: workqueue.Change{Conditions: []keyvalue.Condition{{
			Key:         blueprintunits.CapturedGraphKey(graph.Record.EnvironmentID, graph.Record.TaskID),
			ModRevision: graph.Revision,
		}}},
	}, nil
}
