package imagedelivery

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingprovisionwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) retainBackingWorkImages(ctx context.Context, retention *imageRetention, value keyvalue.KeyValue, revision int64) error {
	item, err := workqueue.DecodeItem(value.Value)
	if err != nil {
		return err
	}
	if value.Key != workqueue.ItemKey(item.ID) {
		return errs.New(errs.KindStateConflict, "backing creation image protection has a different stored work identity")
	}
	if item.Operation != workexecution.BackingProvision || item.Phase == workexecution.Settled {
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
	inputs, err := backingprovisionwork.NewRepository(service.store)
	if err != nil {
		return err
	}
	captured, _, err := inputs.Load(ctx, view)
	if err != nil {
		return err
	}
	defer clear(captured.Wire)
	procedure, err := captured.Procedure()
	if err != nil {
		return err
	}
	retention.composeArtifact(procedure.Runtime, "Backing creation or cleanup in Task "+item.TaskID)
	return nil
}
