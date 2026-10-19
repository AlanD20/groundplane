package releaseoperation

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (owner *GraphPublication) PrepareTaskRetirement(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	revision int64,
	at time.Time,
) (workqueue.Change, error) {
	if ids.Validate(ids.KindReleaseGroup, task.Task.TargetID) != nil {
		// Standalone preparation is retired with its sole Release member.
		return workqueue.Change{
			Conditions: []keyvalue.Condition{{Key: etcd.DirectReleasePreparationKey(task.Task.ID)}},
		}, nil
	}
	root, err := owner.groups.Load(ctx, task, revision)
	if err != nil {
		return workqueue.Change{}, err
	}
	return owner.ledger.PrepareGroupReleaseCaptureRetirement(ctx, task, root, at)
}

func (owner *GraphPublication) CollectDrafts(ctx context.Context, after string, at time.Time) (string, error) {
	return owner.ledger.CollectAbandonedGroupReleaseDraftPage(ctx, after, at)
}
