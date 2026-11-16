package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entrywork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// EntryWorkCapture joins the original accepted Task promotion; it never publishes a second Task or replay record.
type EntryWorkCapture struct {
	Task        workexecution.Task
	Item        workexecution.Item
	Change      workqueue.Change
	Decision    *workexecution.Input
	store       hierarchyStore
	operationID string
}

// Finish distinguishes a committed native root from inert, unpublished chunks
// before releasing pending credential pins. Unknown outcomes never guess.
func (capture EntryWorkCapture) Finish(ctx context.Context, applied bool) error {
	if applied || capture.store == nil {
		return nil
	}
	keys := []string{
		workqueue.TaskKey(capture.Task.ID),
		workqueue.ItemKey(capture.Item.ID),
		entrywork.InputKey(capture.Item.ID),
	}
	read, err := capture.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return err
	}
	if read == nil || len(read.Values) != len(keys) {
		return errs.New(errs.KindInternal, "Entry preparation outcome is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return errs.New(errs.KindStateConflict, "Entry preparation lost its accepted Task")
	}
	task, err := workqueue.DecodeTask(read.Values[0].Value)
	if err != nil {
		return err
	}
	if read.Values[1] != nil {
		item, err := workqueue.DecodeItem(read.Values[1].Value)
		if err != nil {
			return err
		}
		if task.Intent != capture.Task.Intent || item.TaskID != task.ID || item.ID != capture.Item.ID ||
			(item.Input == nil) != (capture.Item.Input == nil) || item.Input != nil && (*item.Input != *capture.Item.Input || read.Values[2] == nil) || item.Input == nil && read.Values[2] != nil {
			return errs.New(errs.KindStateConflict, "Entry preparation belongs to different native work")
		}
		return nil
	}
	if task.AcceptedIntent == nil || task.Intent != (workexecution.Input{}) || task.ExpectedItems != 0 ||
		read.Values[2] != nil {
		return errs.New(errs.KindStateConflict, "Entry preparation publication is incomplete")
	}
	if capture.operationID != "" {
		pins, err := tasksecretpins.NewEtcdRepository(capture.store, task.Scope.ProjectID)
		if err != nil {
			return err
		}
		if err := pins.Abandon(ctx, capture.operationID); err != nil {
			return err
		}
	}
	result, err := capture.store.Transact(ctx, []keyvalue.Condition{
		{Key: keys[0], ModRevision: read.Values[0].ModRevision}, {Key: keys[1]}, {Key: keys[2]},
	}, []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: entrywork.InputPrefix(capture.Item.ID), Prefix: true}})
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Entry preparation changed during cleanup")
	}
	return nil
}

func (repository *HierarchyRepository) AbandonEntryWorkPreparation(ctx context.Context, task workexecution.Task,
	item workexecution.Item, operationID string,
) error {
	return (EntryWorkCapture{Task: task, Item: item, store: repository.store, operationID: operationID}).Finish(
		ctx,
		false,
	)
}
