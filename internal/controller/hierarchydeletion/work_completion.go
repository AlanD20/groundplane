package hierarchydeletion

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RemovalCompletion struct {
	records  *etcd.HierarchyDeletionRepository
	volumes  workdispatch.CompletionOwner
	networks workdispatch.CompletionOwner
}

func NewRemovalCompletion(records *etcd.HierarchyDeletionRepository, volumes, networks workdispatch.CompletionOwner) (*RemovalCompletion, error) {
	if records == nil || volumes == nil || networks == nil {
		return nil, errs.New(errs.KindInternal, "removal completion requires captured storage")
	}
	return &RemovalCompletion{records: records, volumes: volumes, networks: networks}, nil
}

func (owner *RemovalCompletion) PrepareFinish(ctx context.Context, finished workqueue.FinishedWork, at time.Time) (workqueue.Change, error) {
	if finished.Task.Task.Action != workexecution.ActionRemove || len(finished.Items) == 0 {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "removal completion requires settled removal work")
	}
	if ids.Validate(ids.KindNetwork, finished.Task.Task.TargetID) == nil {
		return owner.networks.PrepareFinish(ctx, finished, at)
	}
	switch finished.Items[0].Item.Operation {
	case workexecution.Volume:
		return owner.volumes.PrepareFinish(ctx, finished, at)
	case workexecution.Image:
		return (workdispatch.CheckImageCompletion{}).PrepareFinish(ctx, finished, at)
	case workexecution.Hierarchy, workexecution.RemovalBackupObject:
		return owner.records.PrepareHierarchyWorkFinish(ctx, finished, at)
	default:
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "removal operation has no aggregate completion owner")
	}
}
