package componentplanning

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ControllerProxyOperationID identifies metadata settlement of this exact work.
func ControllerProxyOperationID(task workexecution.Task, item workexecution.Item) (string, error) {
	if task.Validate() != nil || item.Validate() != nil || task.Action != workexecution.ActionApply ||
		item.TaskID != task.ID || item.Operation != workexecution.Component ||
		item.Placement.Executor != workexecution.Controller || ids.Validate(ids.KindComponent, item.TargetID) != nil {
		return "", errs.New(errs.KindValidationFailed, "Disabled proxy selection requires Controller Component work")
	}
	return ids.DeriveAt(ids.KindOperation, task.CreatedAt, item.ID, "disabled-proxy"), nil
}

// BindProxyWorkRollouts binds the exact dependent Service inputs to native
// Component work. Neither executor placement claims an applied runtime effect.
func BindProxyWorkRollouts(work workqueue.View, captured ProxyServicePreflight,
	input keyvalue.Versioned[ProxyCandidateInputReference], rollouts []ProxyServiceRolloutReference,
) (*ProxyServicePreflight, workqueue.Change, error) {
	task, item := work.Task.Task, work.Item.Item
	sourceID, sourceOwner := item.ID, ProxyInputAgentWork
	if item.Placement.Executor == workexecution.Controller {
		var err error
		sourceID, err = ControllerProxyOperationID(task, item)
		if err != nil {
			return nil, workqueue.Change{}, err
		}
		sourceOwner = ProxyInputControllerSettlement
	}
	if work.ReadRevision <= 0 || work.Task.Revision <= 0 || work.Item.Revision <= 0 ||
		task.Validate() != nil || item.Validate() != nil || task.Action != workexecution.ActionApply ||
		item.TaskID != task.ID || item.Operation != workexecution.Component ||
		(item.Placement.Executor != workexecution.Controller && item.Placement.Executor != workexecution.Agent) ||
		task.Stop != nil || task.FinishedAt != nil || task.CollectionStartedAt != nil ||
		item.AttemptNumber != 0 || item.ActiveAttemptID != "" || item.ForwardRevoked ||
		(item.Phase != workexecution.Waiting && item.Phase != workexecution.Ready) ||
		input.Record.Validate() != nil || input.Revision <= 0 || input.ReadRevision < input.Revision ||
		input.Record.SourceOwner != sourceOwner || input.Record.SourceID != sourceID ||
		input.Record.ParentTaskID != task.ID || input.Record.EnvironmentID != task.Scope.EnvironmentID ||
		input.Record.ComponentID != item.TargetID || captured.validate() != nil ||
		captured.ParentTaskID != task.ID || captured.EnvironmentID != task.Scope.EnvironmentID {
		return nil, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Proxy input differs from captured Component work",
		)
	}
	if captured.CandidateInput != nil && *captured.CandidateInput != input.Record {
		return nil, workqueue.Change{}, errs.New(errs.KindStateConflict, "Proxy work already captured different input")
	}
	preflight := cloneProxyServicePreflight(&captured)
	owned := input.Record
	preflight.CandidateInput = &owned
	preflight, conditions, err := BindProxyServiceRollouts(*preflight, rollouts)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	conditions = append(conditions,
		keyvalue.Condition{Key: ProxyCandidateInputKey(owned), ModRevision: input.Revision},
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: work.Item.Revision})
	return preflight, workqueue.Change{Conditions: conditions}, nil
}

// ValidateAgentProxyCapture checks the retained dependency capture without
// looking up a newer desired state. Publication separately fences its sources.
func ValidateAgentProxyCapture(work workqueue.View, captured ProxyServicePreflight) error {
	task, item := work.Task.Task, work.Item.Item
	if captured.validate() != nil || item.Operation != workexecution.Component ||
		item.Placement.Executor != workexecution.Agent || captured.ParentTaskID != task.ID ||
		captured.EnvironmentID != task.Scope.EnvironmentID || captured.CandidateInput == nil ||
		captured.ReadRevision > work.ReadRevision ||
		captured.CandidateInput.SourceOwner != ProxyInputAgentWork ||
		captured.CandidateInput.SourceID != item.ID || captured.CandidateInput.ComponentID != item.TargetID {
		return errs.New(errs.KindStateConflict, "Proxy dependency capture belongs to another Component operation")
	}
	for _, source := range captured.Services {
		if source.ReplacementRequired && source.Rollout == nil {
			return errs.New(errs.KindStateConflict, "Proxy dependency capture is missing a Service rollout")
		}
	}
	return nil
}
