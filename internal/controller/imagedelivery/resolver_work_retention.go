package imagedelivery

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/resolverwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func retainResolverAppliedImages(retention *imageRetention, value keyvalue.KeyValue) error {
	applied, err := resolverwork.DecodeApplied(value.Value)
	if err != nil {
		return err
	}
	if value.Key != resolverwork.AppliedKey(applied.HostID, applied.ComponentID) {
		return errs.New(errs.KindStateConflict, "CoreDNS applied image authority belongs to another host or Component")
	}
	if applied.Desired.Enabled {
		retention.composeArtifact(applied.Configuration.Runtime, "Applied CoreDNS resolver on host "+applied.HostID)
	}
	return nil
}

func (service *Service) retainResolverWorkImages(ctx context.Context, retention *imageRetention, value keyvalue.KeyValue, revision int64) error {
	item, err := workqueue.DecodeItem(value.Value)
	if err != nil {
		return err
	}
	if value.Key != workqueue.ItemKey(item.ID) {
		return errs.New(errs.KindStateConflict, "CoreDNS image work identity differs from its stored key")
	}
	if item.Operation != workexecution.Resolver || item.Phase == workexecution.Settled && item.Outcome != workexecution.Failed {
		return nil
	}
	queue, err := workqueue.NewRepository(service.store)
	if err != nil {
		return err
	}
	parent, _, err := queue.ReadTask(ctx, item.TaskID, revision)
	if err != nil {
		return err
	}
	if parent.Task.CollectionStartedAt != nil {
		return nil
	}
	view, err := queue.ReadWork(ctx, item.TaskID, item.ID, revision)
	if err != nil {
		return err
	}
	records, err := resolverwork.NewRepository(service.store)
	if err != nil {
		return err
	}
	stored, err := records.Load(ctx, view)
	if err != nil {
		return err
	}
	reason := "CoreDNS execution or Retry in Task " + item.TaskID
	if stored.Input.Candidate != nil {
		retention.composeArtifact(stored.Input.Candidate.Runtime, reason)
	}
	if stored.Input.PreviousEnabled {
		retention.composeArtifact(stored.Input.Previous.Runtime, reason)
	}
	return nil
}
