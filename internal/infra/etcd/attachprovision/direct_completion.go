package attachprovision

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// DirectCompletionConditions joins native action completion to its immutable
// capture and, for Controller settlement, the exact saved binding result.
func (repository *InputRepository) DirectCompletionConditions(
	ctx context.Context,
	view workqueue.View,
) ([]keyvalue.Condition, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Action != workexecution.ActionAttach || task.ExpectedItems != 1 || item.TargetID != task.TargetID ||
		item.Phase != workexecution.Settled || item.ActiveAttemptID != "" || item.Result == nil ||
		(item.Cleanup != workexecution.CleanupNone && item.Cleanup != workexecution.CleanupComplete) {
		return nil, checkpointConflict("Attach completion requires one fully settled connection")
	}
	source, err := repository.LoadCaptured(ctx, view)
	if err != nil {
		return nil, err
	}
	defer source.Clear()
	guards := append(
		source.Conditions,
		keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision},
	)
	if item.Placement.Executor != workexecution.Controller || item.Outcome != workexecution.Succeeded {
		return guards, nil
	}
	if !source.Generation.Record.SettlesInController() || item.AttemptNumber != 0 || item.Input == nil {
		return nil, checkpointConflict("Attach Controller result has an incompatible execution owner")
	}
	key := DirectControllerResultKey(item.ID)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision},
	)
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != key || read.Values[0].ModRevision <= 0 || read.Values[0].ModRevision > view.ReadRevision {
		return nil, checkpointConflict("Attach Controller settlement result is unavailable")
	}
	value := read.Values[0]
	result, err := recordcodec.Decode[directControllerResult](value.Value, "attach-controller-result")
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(value.Value)
	expected := workexecution.Input{ID: ids.DeriveAt(ids.KindPlan, task.CreatedAt, item.ID, "attach-controller-result"),
		SHA256: hex.EncodeToString(digest[:])}
	if *item.Result != expected || result.TaskID != task.ID || result.WorkItemID != item.ID ||
		result.AttachID != item.TargetID || result.Input != *item.Input || !result.At.Equal(item.UpdatedAt) {
		return nil, checkpointConflict("Attach Controller settlement result differs from the completed work")
	}
	return append(guards, keyvalue.Condition{Key: key, ModRevision: value.ModRevision}), nil
}
