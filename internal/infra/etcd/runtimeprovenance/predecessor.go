// Package runtimeprovenance verifies runtime advances produced by a captured
// prerequisite. It never accepts an unrelated operator's newer runtime.
package runtimeprovenance

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Reader interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
}

func PredecessorConditions(ctx context.Context, store Reader, view workqueue.View,
	runtime keyvalue.Versioned[serviceruntimerecord.Record],
	graph []workexecution.Item,
) ([]keyvalue.Condition, bool, error) {
	source := runtime.Record.Source
	if runtime.Revision <= 0 || source.TaskID != view.Task.Task.ID {
		return nil, false, nil
	}
	ancestor, err := CapturedPrerequisite(view, source.WorkItemID, graph)
	if err != nil {
		return nil, false, err
	}
	if !ancestor {
		return nil, false, nil
	}
	if serviceruntimerecord.Validate(runtime.Record) != nil || runtime.ReadRevision != view.ReadRevision ||
		runtime.Record.EnvironmentID != view.Task.Task.Scope.EnvironmentID || source.WorkItemID == view.Item.Item.ID {
		return nil, false, errs.New(
			errs.KindStateConflict,
			"Runtime predecessor belongs to a different captured operation",
		)
	}
	keys := []string{workqueue.ItemKey(source.WorkItemID), workqueue.AttemptKey(source.AttemptID),
		workqueue.CheckpointKey(source.AttemptID)}
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: view.ReadRevision})
	if err != nil {
		return nil, false, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != len(keys) {
		return nil, false, errs.New(errs.KindInternal, "Runtime predecessor evidence is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	conditions := []keyvalue.Condition{
		{Key: serviceruntimerecord.Key(runtime.Record.Runtime.ServiceID), ModRevision: runtime.Revision},
	}
	for index, key := range keys {
		value := read.Values[index]
		if value == nil || value.Key != key || value.ModRevision <= 0 || value.ModRevision > view.ReadRevision {
			return nil, false, errs.New(errs.KindStateConflict, "Runtime predecessor evidence is unavailable")
		}
		conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: value.ModRevision})
	}
	item, itemErr := workqueue.DecodeItem(read.Values[0].Value)
	attempt, attemptErr := workqueue.DecodeAttempt(read.Values[1].Value)
	checkpoint, checkpointErr := workqueue.DecodeCheckpoint(read.Values[2].Value)
	if itemErr != nil || attemptErr != nil || checkpointErr != nil ||
		item.TaskID != source.TaskID || item.ID != source.WorkItemID || item.Phase != workexecution.Settled ||
		item.Outcome != workexecution.Succeeded || item.ActiveAttemptID != "" || item.Result == nil ||
		item.Input == nil || *item.Input != source.Input || item.AttemptNumber != attempt.Number ||
		attempt.ID != source.AttemptID || attempt.TaskID != source.TaskID || attempt.WorkItemID != item.ID ||
		attempt.Input != source.Input || attempt.Executor != source.Executor || attempt.Direction != workexecution.Forward ||
		attempt.StartedAt == nil || attempt.FinishedAt == nil || attempt.Outcome != workexecution.Succeeded || attempt.Result == nil ||
		*attempt.Result != *item.Result || source.AcknowledgedAt.After(*attempt.FinishedAt) ||
		source.AcknowledgedAt.Before(*attempt.StartedAt) ||
		checkpoint.AttemptID != attempt.ID || checkpoint.Sequence < source.Checkpoint.Sequence ||
		checkpoint.RecordedAt.Before(source.Checkpoint.RecordedAt) {
		return nil, false, errs.New(errs.KindStateConflict, "Runtime advance has no successful captured prerequisite")
	}
	return conditions, true, nil
}
