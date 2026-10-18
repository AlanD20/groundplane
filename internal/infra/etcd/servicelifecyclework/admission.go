package servicelifecyclework

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareAdmission joins native Task/work publication and runtime intent. The
// caller joins the prepared input/envelope, hierarchy fences and request receipt
// in the same transaction. No executable payload is stored in the visible Task.
func PrepareAdmission(input Input, current keyvalue.Versioned[services.ServiceRecord], deadline time.Time, marker idempotency.IdempotencyMarker) (workexecution.Task, workqueue.Change, error) {
	ref, err := input.Reference()
	if err != nil {
		return workexecution.Task{}, workqueue.Change{}, err
	}
	if current.Revision != input.ServiceRevision || current.ReadRevision < current.Revision ||
		services.ValidateServiceRecord(current.Record) != nil || current.Record.Desired.ID != input.ServiceID ||
		current.Record.EnvironmentID != input.Scope.EnvironmentID || !deadline.After(input.CreatedAt) {
		return workexecution.Task{}, workqueue.Change{}, conflict("Service lifecycle capture differs from its Service")
	}
	intent := core.ServiceRuntimeIntentRunning
	switch input.Action {
	case workexecution.ActionStop:
		intent = core.ServiceRuntimeIntentStopped
	case workexecution.ActionDestroy:
		intent = core.ServiceRuntimeIntentAbsent
	}
	if runtime := input.Runtime; runtime != nil {
		var hooks *backinghook.Configuration
		if configured := current.Record.Desired.Hooks; configured != nil &&
			(input.Action == workexecution.ActionStart && configured.AfterStart != nil ||
				input.Action != workexecution.ActionStart && configured.BeforeStop != nil) {
			hooks = configured
		}
		if !backinghook.EqualConfiguration(runtime.HookConfiguration, hooks) ||
			hooks != nil && runtime.AdapterKey != current.Record.Desired.Adapter {
			return workexecution.Task{}, workqueue.Change{}, conflict("Service lifecycle hook configuration changed before admission")
		}
	}
	task := workexecution.Task{ID: input.TaskID, Actor: workexecution.OperatorActor,
		Scope: input.Scope, Action: input.Action, TargetID: input.ServiceID, TargetName: current.Record.Desired.Name,
		Intent: ref, ExpectedItems: 1, CreatedAt: input.CreatedAt, UpdatedAt: input.CreatedAt, Deadline: deadline}
	placement := workexecution.Placement{Executor: workexecution.Controller}
	if input.Runtime != nil {
		placement = workexecution.Placement{Executor: workexecution.Agent, HostID: input.HostID}
	}
	item := workexecution.Item{ID: input.WorkItemID, TaskID: input.TaskID, Operation: workexecution.ServiceLifecycle,
		TargetID: input.ServiceID, Placement: placement, Claims: []workexecution.Claim{{ScopeID: input.ServiceID}},
		Input: &ref, Phase: workexecution.Ready, Cleanup: workexecution.CleanupNone,
		CreatedAt: input.CreatedAt, UpdatedAt: input.CreatedAt, Deadline: deadline}
	change, err := workqueue.PrepareSingleTask(task, item, marker)
	if err != nil {
		return workexecution.Task{}, workqueue.Change{}, err
	}
	replacement := current.Record
	replacement.Runtime.RuntimeIntent = intent
	replacement.Runtime.UnconfirmedLifecycleTaskID = task.ID
	value, err := services.EncodeServiceRuntimeRecord(services.NewServiceRuntimeRecord(replacement))
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workexecution.Task{}, workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, services.ServiceDesiredCondition(current), services.ServiceRuntimeCondition(current))
	change.Conditions = append(change.Conditions, runtimeSourceConditions(input)...)
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
		Key: services.ServiceRuntimeKey(input.ServiceID), Value: value})
	return task, change, nil
}
