package releasework

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func UnstartedKey(workID string) string { return "/v1/execution/release-unstarted/" + workID }

// PrepareQueuedStop closes a stopped or expired Release before its next
// assignment. Captured sources and previous attempts remain retained.
func PrepareQueuedStop(task workqueue.TaskSnapshot, item workqueue.ItemSnapshot, at time.Time) (workqueue.Change, error) {
	if item.Item.Operation != workexecution.Release && item.Item.Operation != workexecution.ReleaseRestoration || item.Item.Placement.Executor != workexecution.Agent ||
		item.Item.TaskID != task.Task.ID || at.IsZero() || at.Location() != time.UTC {
		return workqueue.Change{}, invalid()
	}
	if item.Item.Input == nil {
		return workqueue.PrepareInputSettlement(workqueue.View{Task: task, Item: item}, at)
	}
	outcome := workexecution.Failed
	if task.Task.Stop != nil {
		outcome = task.Task.Stop.Outcome
		if item.Item.ActiveAttemptID != "" || item.Item.Cleanup != workexecution.CleanupNone {
			// Abort revokes forward permission, not the active slot or unknown
			// effects. Captured cleanup remains accountable to its own owner.
			return workqueue.PrepareRevocation(task, item, outcome, at)
		}
	} else if item.Item.ForwardRevoked {
		outcome = workexecution.Superseded
	} else if at.Before(item.Item.Deadline) && at.Before(task.Task.Deadline) {
		return workqueue.Change{}, invalid()
	}
	return prepareUnstartedOutcome(task, item, outcome, at)
}

// PrepareUnstartedSupersession requires a fresh owner-side comparison proving
// that the captured source was superseded. It cannot settle an assigned or
// started attempt; the first Start race is fenced by the native item revision.
func PrepareUnstartedSupersession(view workqueue.View, source []keyvalue.Condition, at time.Time) (workqueue.Change, error) {
	item := view.Item
	if len(source) == 0 || item.Item.Operation != workexecution.Release || item.Item.AttemptNumber != 0 ||
		item.Item.ActiveAttemptID != "" || item.Item.Phase != workexecution.Waiting && item.Item.Phase != workexecution.Ready {
		return workqueue.Change{}, invalid()
	}
	if item.Item.Input == nil {
		return workqueue.PrepareInputSupersession(view, source, at)
	}
	item.Item.ForwardRevoked = true
	change, err := prepareUnstartedOutcome(view.Task, item, workexecution.Superseded, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, source...)
	return change, nil
}

func prepareUnstartedOutcome(task workqueue.TaskSnapshot, item workqueue.ItemSnapshot, outcome workexecution.Outcome, at time.Time) (workqueue.Change, error) {
	value, err := recordcodec.Encode("release-unstarted", struct {
		TaskID  string                `json:"task_id"`
		WorkID  string                `json:"work_id"`
		Outcome workexecution.Outcome `json:"outcome"`
		At      time.Time             `json:"at"`
	}{task.Task.ID, item.Item.ID, outcome, at})
	if err != nil {
		return workqueue.Change{}, err
	}
	digest := sha256.Sum256(value)
	number := strconv.FormatUint(uint64(item.Item.AttemptNumber), 10)
	evidence := workexecution.Input{ID: ids.DeriveAt(ids.KindPlan, item.Item.CreatedAt, item.Item.ID, "release-unstarted/"+number), SHA256: hex.EncodeToString(digest[:])}
	result := workexecution.Completion{
		Outcome: outcome, Evidence: evidence, EffectsSettled: true,
	}
	var change workqueue.Change
	if item.Item.AttemptNumber == 0 {
		change, err = workqueue.PrepareUnstartedSettlement(task, item, result, at)
	} else {
		change, err = workqueue.PrepareQueuedRetrySettlement(task, item, result, at)
	}
	if err != nil {
		clear(value)
		return workqueue.Change{}, err
	}
	key := UnstartedKey(item.Item.ID) + "/" + number
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value})
	return change, nil
}
