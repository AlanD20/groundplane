package blueprintunits

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func PrepareDefinitionWorkStop(
	task workqueue.TaskSnapshot,
	item workqueue.ItemSnapshot,
	outcome workexecution.Outcome,
	at time.Time,
) (workqueue.Change, error) {
	stopped := task.Task.Stop != nil && outcome == task.Task.Stop.Outcome
	expired := outcome == workexecution.Failed && !at.Before(item.Item.Deadline)
	if task.Task.Action != workexecution.ActionApply || item.Item.Operation != workexecution.ResourceDefinition ||
		item.Item.TaskID != task.Task.ID || item.Item.Placement.Executor != workexecution.Controller ||
		(!stopped && !expired) {
		return workqueue.Change{}, invalidRecord()
	}
	value, err := recordcodec.Encode("blueprint-definition-unstarted", struct {
		TaskID  string                `json:"task_id"`
		WorkID  string                `json:"work_id"`
		Outcome workexecution.Outcome `json:"outcome"`
		At      time.Time             `json:"at"`
	}{task.Task.ID, item.Item.ID, outcome, at})
	if err != nil {
		return workqueue.Change{}, err
	}
	digest := sha256.Sum256(value)
	evidence := workexecution.Input{
		ID:     ids.DeriveAt(ids.KindPlan, task.Task.CreatedAt, item.Item.ID, "definition-result"),
		SHA256: hex.EncodeToString(digest[:]),
	}
	change, err := workqueue.PrepareUnstartedSettlement(task, item, workexecution.Completion{
		Outcome: outcome, Evidence: evidence, EffectsSettled: true,
	}, at)
	if err != nil {
		clear(value)
		return workqueue.Change{}, err
	}
	key := DefinitionWorkResultKey(item.Item.ID)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value})
	return change, nil
}
