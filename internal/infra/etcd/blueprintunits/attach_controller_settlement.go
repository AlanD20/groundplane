package blueprintunits

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func ControllerAttachResultKey(workID string) string {
	return recordPrefix + "attach-controller-results/" + workID
}

// PrepareAttachControllerSettlement records a decision, not a synthetic Agent
// attempt. The caller joins the exact encrypted input and binding publication.
func PrepareAttachControllerSettlement(snapshot Snapshot, view workqueue.View, unit Unit,
	operationID string, inputRevision int64, bindingSHA256 string, references []attachments.BindingReference, at time.Time,
) (workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if view.ReadRevision != snapshot.ReadRevision || task.Validate() != nil || item.Validate() != nil ||
		task.Action != workexecution.ActionApply || task.ID != snapshot.HeadTaskID ||
		task.Scope.EnvironmentID != snapshot.EnvironmentID || !task.PublicationComplete ||
		task.Stop != nil || task.FinishedAt != nil || task.CollectionStartedAt != nil ||
		item.TaskID != task.ID || item.TargetID != unit.Target.ID || item.Operation != workexecution.Attach ||
		item.Placement.Executor != workexecution.Controller || item.Input == nil || item.ForwardRevoked ||
		item.AttemptNumber != 0 || item.ActiveAttemptID != "" || item.Phase != workexecution.Ready ||
		at.IsZero() || at.Location() != time.UTC || at.Before(item.UpdatedAt) ||
		!at.Before(task.Deadline) || !at.Before(item.Deadline) {
		return workqueue.Change{}, invalidRecord()
	}
	receipt, err := PrepareControllerAttachBinding(snapshot, unit, operationID, inputRevision, bindingSHA256, references)
	if err != nil {
		return workqueue.Change{}, err
	}
	value, err := recordcodec.Encode("blueprint-attach-controller-result", struct {
		TaskID        string              `json:"task_id"`
		WorkID        string              `json:"work_id"`
		EnvironmentID string              `json:"environment_id"`
		AttachID      string              `json:"attach_id"`
		OperationID   string              `json:"operation_id"`
		InputRevision int64               `json:"input_revision"`
		Input         workexecution.Input `json:"input"`
		SettledAt     time.Time           `json:"settled_at"`
	}{task.ID, item.ID, snapshot.EnvironmentID, item.TargetID, operationID, inputRevision, *item.Input, at})
	if err != nil {
		receipt.Clear()
		return workqueue.Change{}, err
	}
	digest := sha256.Sum256(value)
	change, err := workqueue.PrepareUnstartedSettlement(view.Task, view.Item, workexecution.Completion{
		Outcome: workexecution.Succeeded, EffectsSettled: true,
		Evidence: workexecution.Input{ID: ids.DeriveAt(ids.KindPlan, task.CreatedAt, item.ID, "attach-controller-result"), SHA256: hex.EncodeToString(digest[:])},
	}, at)
	if err != nil {
		receipt.Clear()
		clear(value)
		return workqueue.Change{}, err
	}
	key := ControllerAttachResultKey(item.ID)
	change.Conditions = append(change.Conditions, receipt.Conditions()...)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key})
	change.Mutations = append(change.Mutations, receipt.Mutations()...)
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value})
	return change, nil
}
