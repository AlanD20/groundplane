package imagedelivery

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// A direct Release captures workload, proxy and hook images before it has
// executable input. Image removal must preserve that entire accepted selection,
// including while private-DNS prerequisites are still running.
func (service *Service) retainDirectReleaseWorkImages(ctx context.Context, retention *imageRetention,
	value keyvalue.KeyValue, revision int64,
) error {
	item, err := workqueue.DecodeItem(value.Value)
	if err != nil {
		return err
	}
	if value.Key != workqueue.ItemKey(item.ID) {
		return errs.New(errs.KindStateConflict, "Release image retention work identity is inconsistent")
	}
	if item.Operation != workexecution.Release || item.Input != nil || item.Phase == workexecution.Settled {
		return nil
	}
	queue, err := workqueue.NewRepository(service.store)
	if err != nil {
		return err
	}
	view, err := queue.ReadWork(ctx, item.TaskID, item.ID, revision)
	if err != nil {
		return err
	}
	if view.Task.Task.Action != workexecution.ActionDeploy && view.Task.Task.Action != workexecution.ActionRollback {
		return nil
	}
	if ids.Validate(ids.KindReleaseGroup, view.Task.Task.TargetID) == nil {
		// The accepted Group root protects every member, including descriptors
		// that have not been published yet. Do not demand a promoted rollout.
		return nil
	}
	key := releaserender.ServiceRolloutKey(view.Task.Task.Scope.EnvironmentID, item.TaskID, item.ID)
	read, err := service.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return errs.New(errs.KindStateConflict, "Captured Release image read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	stored := read.Values[0]
	if stored == nil || stored.Key != key || stored.ModRevision <= 0 {
		return errs.New(errs.KindStateConflict, "Accepted Release image capture is unavailable")
	}
	capture, err := releaserender.DecodeServiceRolloutCapture(stored.Value)
	if err != nil {
		return err
	}
	defer clear(capture.CandidateArtifact)
	if err := capture.MatchesWork(view.Task.Task, item); err != nil {
		return err
	}
	return retainServiceRolloutImages(retention, capture, "Preparing Release in Task "+item.TaskID)
}
