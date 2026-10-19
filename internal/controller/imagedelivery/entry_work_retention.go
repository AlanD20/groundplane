package imagedelivery

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entrywork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Entry edits can recreate captured running consumers, while recovery needs
// their predecessor images. Protect those images independently of Task logs.
func (service *Service) retainEntryWorkImages(
	ctx context.Context,
	retention *imageRetention,
	value keyvalue.KeyValue,
	revision int64,
) error {
	item, err := workqueue.DecodeItem(value.Value)
	if err != nil {
		return err
	}
	if value.Key != workqueue.ItemKey(item.ID) {
		return errs.New(errs.KindStateConflict, "image retention work identity differs from its storage key")
	}
	if item.Operation != workexecution.EntryConfiguration || item.Input == nil || item.Phase == workexecution.Settled {
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
	inputs, err := entrywork.NewInputRepository(service.store)
	if err != nil {
		return err
	}
	input, _, _, err := inputs.Load(ctx, view)
	if err != nil {
		return err
	}
	if len(input.RunningConsumerIds) == 0 {
		return nil
	}
	reason := "Entry configuration work in Task " + item.TaskID
	retention.composeArtifact(input.Candidate, reason)
	retention.composeArtifact(input.Baseline, reason)
	return nil
}
