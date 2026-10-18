package imageoperations

import (
	"context"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefence"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PruneInput leaves applied tag state intact. Collection requires settled work
// and proves this operation no longer owns a removal/tag-write guard.
func (repository *Repository) PruneInput(ctx context.Context, taskID string, at time.Time) (bool, error) {
	if ids.Validate(ids.KindTask, taskID) != nil || !recordcodec.IsCanonicalUTC(at) {
		return false, invalid()
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{Key(taskID), workqueue.TaskKey(taskID)}})
	if err != nil {
		return false, err
	}
	if read == nil {
		return false, conflict()
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision <= 0 || len(read.Values) != 2 {
		return false, conflict()
	}
	if read.Values[0] == nil {
		return true, nil
	}
	if read.Values[1] == nil {
		return false, conflict()
	}
	record, err := Decode(read.Values[0].Value)
	if err != nil {
		return false, err
	}
	task, err := workqueue.DecodeTask(read.Values[1].Value)
	if err != nil {
		return false, err
	}
	input, err := record.Input.Reference()
	if err != nil {
		return false, err
	}
	if task.ID != taskID || record.Input.TaskID != taskID || task.Intent != input {
		return false, conflict()
	}
	if record.Phase != Finished || task.CollectionStartedAt == nil || task.FinishedAt == nil ||
		task.RetainUntil == nil || at.Before(*task.RetainUntil) {
		return false, nil
	}
	selectionKey, writeKey, err := tagKeys(record.Input)
	if err != nil {
		return false, err
	}
	keys := []string{workqueue.ItemKey(record.Input.WorkItemID), imagefence.Key}
	if selectionKey != "" {
		keys = append(keys, selectionKey, writeKey)
	}
	owned, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: read.ReadRevision})
	if err != nil {
		return false, err
	}
	if owned == nil {
		return false, conflict()
	}
	defer keyvalue.ClearValues(owned.Values)
	if owned.ReadRevision != read.ReadRevision || len(owned.Values) != len(keys) || owned.Values[0] == nil {
		return false, conflict()
	}
	item, err := workqueue.DecodeItem(owned.Values[0].Value)
	if err != nil {
		return false, err
	}
	if item.ID != record.Input.WorkItemID || item.TaskID != taskID || item.Operation != workexecution.Image ||
		item.Phase != workexecution.Settled || item.Input == nil || *item.Input != input {
		return false, conflict()
	}
	if owned.Values[1] != nil {
		var guard removalGuard
		if json.Unmarshal(owned.Values[1].Value, &guard) != nil {
			return false, conflict()
		}
		if guard.Operation == record.Input.OperationID {
			return false, nil
		}
	}
	conditions := []keyvalue.Condition{{Key: Key(taskID), ModRevision: read.Values[0].ModRevision},
		{Key: workqueue.TaskKey(taskID), ModRevision: read.Values[1].ModRevision}}
	for index, key := range keys {
		conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(owned.Values[index])})
	}
	mutations := []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: Key(taskID)}}
	if selectionKey != "" {
		if owned.Values[3] != nil {
			write, err := recordcodec.Decode[TagSelection](owned.Values[3].Value, "image_tag_write")
			if err != nil {
				return false, err
			}
			if write.TaskID == taskID {
				return false, nil
			}
		}
		if owned.Values[2] != nil {
			selection, err := recordcodec.Decode[TagSelection](owned.Values[2].Value, "image_tag_selection")
			if err != nil {
				return false, err
			}
			if selection.TaskID == taskID {
				if selection.Input != input || selection.WorkItemID != item.ID {
					return false, conflict()
				}
				mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: selectionKey})
			}
		}
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	keyvalue.ClearValues(result.FailureReads)
	return result.Succeeded, err
}
