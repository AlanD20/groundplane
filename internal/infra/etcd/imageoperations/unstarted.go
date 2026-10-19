package imageoperations

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func PrepareUnstarted(snapshot keyvalue.Versioned[Record], task workqueue.TaskSnapshot,
	item workqueue.ItemSnapshot, outcome workexecution.Outcome, at time.Time,
) (workqueue.Change, error) {
	record := snapshot.Record
	if snapshot.Revision <= 0 || record.Validate() != nil || record.Phase != Ready ||
		task.Task.ID != record.Input.TaskID || item.Item.TaskID != task.Task.ID || item.Item.ID != record.Input.WorkItemID ||
		item.Item.Operation != workexecution.Image || item.Item.ActiveAttemptID != "" ||
		item.Item.Input == nil || item.Item.TargetID != record.Input.Target() || item.Item.Placement != record.Input.Placement {
		return workqueue.Change{}, conflict()
	}
	input, err := record.Input.Reference()
	if err != nil {
		return workqueue.Change{}, err
	}
	if input != *item.Item.Input || input != task.Task.Intent || at.Before(record.UpdatedAt) {
		return workqueue.Change{}, conflict()
	}
	if outcome == workexecution.Failed {
		if at.Before(item.Item.Deadline) {
			return workqueue.Change{}, conflict()
		}
	} else if task.Task.Stop == nil || task.Task.Stop.Outcome != outcome {
		return workqueue.Change{}, conflict()
	}
	record.Phase, record.Outcome, record.UpdatedAt, record.FinishedAt = Finished, outcome, at, &at
	value, err := Encode(record)
	if err != nil {
		return workqueue.Change{}, err
	}
	digest := sha256.Sum256(value)
	result := workexecution.Completion{Outcome: outcome, EffectsSettled: true,
		Evidence: workexecution.Input{
			ID:     ids.DeriveAt(ids.KindPlan, record.Input.CreatedAt, item.Item.ID, "image-unstarted-result"),
			SHA256: hex.EncodeToString(digest[:]),
		}}
	var change workqueue.Change
	if item.Item.AttemptNumber == 0 {
		change, err = workqueue.PrepareUnstartedSettlement(task, item, result, at)
	} else {
		change, err = workqueue.PrepareQueuedRetrySettlement(task, item, result, at)
	}
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(
		change.Conditions,
		keyvalue.Condition{Key: Key(task.Task.ID), ModRevision: snapshot.Revision},
	)
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: Key(task.Task.ID), Value: value},
	)
	return change, nil
}
