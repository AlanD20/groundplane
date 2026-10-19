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

// ComponentDefinitionInput identifies metadata-only work. Runtime changes retain
// the distinct Agent Component input and cannot use this settlement path.
func ComponentDefinitionInput(task workexecution.Task, unit Unit) (workexecution.Input, error) {
	if task.Validate() != nil || task.Action != workexecution.ActionApply ||
		task.Scope.EnvironmentID == "" || !validUnit(unit) || unit.Target.Kind != ids.KindComponent {
		return workexecution.Input{}, invalidRecord()
	}
	value, err := recordcodec.Encode("blueprint-component-definition-input", struct {
		TaskID  string              `json:"task_id"`
		Desired workexecution.Input `json:"desired"`
		Unit    Unit                `json:"unit"`
	}{task.ID, task.Intent, unit})
	if err != nil {
		return workexecution.Input{}, err
	}
	defer clear(value)
	digest := sha256.Sum256(value)
	return workexecution.Input{ID: ids.DeriveAt(ids.KindPlan, task.CreatedAt, task.ID,
		"component-definition/"+unit.Target.ID), SHA256: hex.EncodeToString(digest[:])}, nil
}

type componentDefinitionResult struct {
	TaskID          string                `json:"task_id"`
	WorkID          string                `json:"work_id"`
	ComponentID     string                `json:"component_id"`
	Outcome         workexecution.Outcome `json:"outcome"`
	Input           workexecution.Input   `json:"input"`
	SourceRevision  int64                 `json:"source_revision,omitempty"`
	RuntimeRevision int64                 `json:"runtime_revision,omitempty"`
	Removed         bool                  `json:"removed,omitempty"`
	At              time.Time             `json:"at"`
}

func ComponentDefinitionResultKey(workID string) string {
	return recordPrefix + "component-definition-results/" + workID
}

// This result must be committed with the disabled metadata, its applied receipt
// and the scheduler's decision fence. It grants no host execution permission.
func PrepareComponentDefinitionSettlement(view workqueue.View, unit Unit,
	sourceRevision, runtimeRevision int64, at time.Time,
) (workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	expected, err := ComponentDefinitionInput(task, unit)
	if err != nil {
		return workqueue.Change{}, err
	}
	if item.Input == nil || *item.Input != expected || item.TargetID != unit.Target.ID ||
		task.Stop != nil || !at.Before(task.Deadline) || !at.Before(item.Deadline) ||
		sourceRevision <= 0 || sourceRevision > view.ReadRevision || runtimeRevision < 0 || runtimeRevision > view.ReadRevision {
		return workqueue.Change{}, invalidRecord()
	}
	return prepareComponentDefinitionResult(view, componentDefinitionResult{
		TaskID: task.ID, WorkID: item.ID, ComponentID: item.TargetID,
		Outcome: workexecution.Succeeded, Input: expected, SourceRevision: sourceRevision,
		RuntimeRevision: runtimeRevision, Removed: unit.Removal, At: at,
	})
}

func PrepareComponentDefinitionStop(
	view workqueue.View,
	outcome workexecution.Outcome,
	at time.Time,
) (workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	stopped := task.Stop != nil && outcome == task.Stop.Outcome
	expired := outcome == workexecution.Failed && (!at.Before(task.Deadline) || !at.Before(item.Deadline))
	if (!stopped && !expired) || item.Input == nil {
		return workqueue.Change{}, invalidRecord()
	}
	return prepareComponentDefinitionResult(view, componentDefinitionResult{
		TaskID: task.ID, WorkID: item.ID, ComponentID: item.TargetID,
		Outcome: outcome, Input: *item.Input, At: at,
	})
}

func prepareComponentDefinitionResult(view workqueue.View, result componentDefinitionResult) (workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Validate() != nil || item.Validate() != nil || task.Action != workexecution.ActionApply ||
		!task.PublicationComplete || task.FinishedAt != nil || task.CollectionStartedAt != nil ||
		view.Task.Revision <= 0 || view.Item.Revision <= 0 || view.ReadRevision < view.Task.Revision || view.ReadRevision < view.Item.Revision ||
		item.TaskID != task.ID || item.Operation != workexecution.Component || item.Placement.Executor != workexecution.Controller ||
		ids.Validate(ids.KindComponent, item.TargetID) != nil || item.Phase != workexecution.Ready ||
		item.AttemptNumber != 0 || item.ActiveAttemptID != "" || item.ForwardRevoked || item.Input == nil {
		return workqueue.Change{}, invalidRecord()
	}
	value, err := recordcodec.Encode("blueprint-component-definition-result", result)
	if err != nil {
		return workqueue.Change{}, err
	}
	digest := sha256.Sum256(value)
	evidence := workexecution.Input{ID: ids.DeriveAt(ids.KindPlan, task.CreatedAt, item.ID,
		"component-definition-result"), SHA256: hex.EncodeToString(digest[:])}
	change, err := workqueue.PrepareUnstartedSettlement(view.Task, view.Item, workexecution.Completion{
		Outcome: result.Outcome, Evidence: evidence, EffectsSettled: true,
	}, result.At)
	if err != nil {
		clear(value)
		return workqueue.Change{}, err
	}
	key := ComponentDefinitionResultKey(item.ID)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value})
	return change, nil
}
