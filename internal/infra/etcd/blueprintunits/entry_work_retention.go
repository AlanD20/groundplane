package blueprintunits

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareEntryWorkRetention retires disposable evidence only. Value generations
// and applied receipts belong to resources, never to Task history collection.
func (repository *Repository) PrepareEntryWorkRetention(
	ctx context.Context,
	view workqueue.View,
	at time.Time,
) (workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return workqueue.Change{}, err
	}
	if task.Validate() != nil || item.Validate() != nil || view.ReadRevision <= 0 ||
		view.Task.Revision <= 0 || view.Item.Revision <= 0 || task.CollectionStartedAt == nil ||
		task.RetainUntil == nil || at.Before(*task.RetainUntil) || task.Action != workexecution.ActionApply ||
		item.Operation != workexecution.EntrySettlement || item.TaskID != task.ID ||
		item.Phase != workexecution.Settled || item.Result == nil || item.AttemptNumber != 0 || item.ActiveAttemptID != "" {
		return workqueue.Change{}, invalidRecord()
	}
	key := EntryWorkResultKey(item.ID)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision},
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 1 {
		return workqueue.Change{}, corruptRecord()
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil || value.Key != key || value.ModRevision <= 0 || value.ModRevision > view.ReadRevision ||
		len(value.Value) > 4096 {
		return workqueue.Change{}, corruptRecord()
	}
	digest := sha256.Sum256(value.Value)
	if item.Result.ID != ids.DeriveAt(ids.KindPlan, task.CreatedAt, item.ID, "entry-result") ||
		item.Result.SHA256 != hex.EncodeToString(digest[:]) {
		return workqueue.Change{}, corruptRecord()
	}
	return workqueue.Change{
		Conditions: []keyvalue.Condition{
			{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
			{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision},
			{Key: key, ModRevision: value.ModRevision},
		},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: key}},
	}, nil
}
