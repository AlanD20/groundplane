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

type ReleaseGroupPublication interface {
	PrepareReleaseGroupPublication(context.Context, workqueue.TaskSnapshot, int64) (workqueue.Change, bool, error)
}

type Publication struct {
	records *blueprintunits.Repository
	groups  ReleaseGroupPublication
}

func NewPublication(records *blueprintunits.Repository, groups ReleaseGroupPublication) (*Publication, error) {
	if records == nil || groups == nil {
		return nil, errs.New(errs.KindInternal, "Blueprint publication requires captured work storage")
	}
	return &Publication{records: records, groups: groups}, nil
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
	metadata, pending, err := publication.groups.PrepareReleaseGroupPublication(ctx, task, revision)
	if err != nil {
		return workdispatch.PublicationBatch{}, err
	}
	if pending {
		return workdispatch.PublicationBatch{Planning: true, Authority: metadata}, nil
	}
	// Even a superseded Task finishes publishing its captured membership so the
	// stop walker can settle it. Dispatch independently checks the stop boundary.
	metadata.Conditions = append(metadata.Conditions, keyvalue.Condition{
		Key: blueprintunits.CapturedGraphKey(
			graph.Record.EnvironmentID,
			graph.Record.TaskID,
		), ModRevision: graph.Revision,
	})
	return workdispatch.PublicationBatch{
		Graph:     graph.Record.Items,
		Authority: metadata,
	}, nil
}
