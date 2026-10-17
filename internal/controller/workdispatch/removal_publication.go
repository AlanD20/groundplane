package workdispatch

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RemovalPublication struct {
	hierarchy PublicationOwner
	zones     PublicationOwner
}

func NewRemovalPublication(hierarchy, zones PublicationOwner) (*RemovalPublication, error) {
	if hierarchy == nil || zones == nil {
		return nil, errs.New(errs.KindInternal, "removal publication requires hierarchy and Zone owners")
	}
	return &RemovalPublication{hierarchy: hierarchy, zones: zones}, nil
}

func (owner *RemovalPublication) PreparePublication(ctx context.Context, task workqueue.TaskSnapshot, revision int64,
	at time.Time,
) (PublicationBatch, error) {
	if task.Task.Action != workexecution.ActionRemove {
		return PublicationBatch{}, errs.New(errs.KindStateConflict, "removal publication received another action")
	}
	if ids.Validate(ids.KindNetwork, task.Task.TargetID) == nil {
		return owner.zones.PreparePublication(ctx, task, revision, at)
	}
	if ids.Validate(ids.KindTenant, task.Task.TargetID) == nil ||
		ids.Validate(ids.KindProject, task.Task.TargetID) == nil || ids.Validate(ids.KindEnvironment, task.Task.TargetID) == nil {
		return owner.hierarchy.PreparePublication(ctx, task, revision, at)
	}
	return PublicationBatch{}, errs.New(errs.KindStateConflict, "resource removal has no graph publication owner")
}
