package blueprintunits

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// Only execution evidence expires here. Component metadata, applied receipts
// and any retained proxy rollout inputs keep their resource-owned lifetimes.
func (repository *Repository) PrepareComponentDefinitionRetention(
	ctx context.Context,
	view workqueue.View,
	at time.Time,
) (workqueue.Change, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return workqueue.Change{}, err
	}
	task, item := view.Task.Task, view.Item.Item
	if task.Validate() != nil || item.Validate() != nil || task.Action != workexecution.ActionApply ||
		task.CollectionStartedAt == nil || task.RetainUntil == nil || at.Before(*task.RetainUntil) ||
		view.Task.Revision <= 0 || view.Item.Revision <= 0 || view.ReadRevision < view.Task.Revision || view.ReadRevision < view.Item.Revision ||
		item.TaskID != task.ID || item.Operation != workexecution.Component || item.Placement.Executor != workexecution.Controller ||
		item.Phase != workexecution.Settled || item.Result == nil || item.Input == nil || item.AttemptNumber != 0 || item.ActiveAttemptID != "" {
		return workqueue.Change{}, invalidRecord()
	}
	key := ComponentDefinitionResultKey(item.ID)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision},
	)
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return workqueue.Change{}, corruptRecord()
	}
	value := read.Values[0]
	if value.Key != key || value.ModRevision <= 0 || value.ModRevision > view.ReadRevision || len(value.Value) > 4096 {
		return workqueue.Change{}, corruptRecord()
	}
	digest := sha256.Sum256(value.Value)
	if item.Result.ID != ids.DeriveAt(ids.KindPlan, task.CreatedAt, item.ID, "component-definition-result") ||
		item.Result.SHA256 != hex.EncodeToString(digest[:]) {
		return workqueue.Change{}, corruptRecord()
	}
	result, err := recordcodec.Decode[componentDefinitionResult](value.Value, "blueprint-component-definition-result")
	if err != nil {
		return workqueue.Change{}, err
	}
	if result.TaskID != task.ID || result.WorkID != item.ID || result.ComponentID != item.TargetID ||
		result.Outcome != item.Outcome || result.Input != *item.Input || !result.At.Equal(item.UpdatedAt) {
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
