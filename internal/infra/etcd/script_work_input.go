package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ScriptWorkInput joins the immutable lookup and execution to the scheduler's
// fixed read revision. It returns comparisons, not execution permission.
type ScriptWorkInput struct {
	Execution  scriptexecutions.ScriptExecutionRecord
	Conditions []keyvalue.Condition
}

func (repository *ScriptRepository) LoadScriptWork(ctx context.Context, view workqueue.View) (ScriptWorkInput, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return ScriptWorkInput{}, err
	}
	task, item := view.Task.Task, view.Item.Item
	if repository == nil || repository.store == nil || task.Validate() != nil || item.Validate() != nil ||
		view.ReadRevision <= 0 || view.Task.Revision <= 0 || view.Item.Revision <= 0 ||
		view.Task.Revision > view.ReadRevision || view.Item.Revision > view.ReadRevision ||
		item.Operation != workexecution.Script || item.TaskID != task.ID ||
		item.Placement.Executor != workexecution.Agent || item.Input == nil {
		return ScriptWorkInput{}, errs.New(errs.KindValidationFailed, "Script work read identity is invalid")
	}
	var authority []keyvalue.Condition
	if task.Action == workexecution.ActionScript {
		if task.TargetID != item.TargetID || *item.Input != task.Intent {
			return ScriptWorkInput{}, errs.New(errs.KindStateConflict, "Standalone Script differs from its accepted Task input")
		}
	} else if (task.Action == workexecution.ActionDeploy || task.Action == workexecution.ActionRollback) &&
		ids.Validate(ids.KindReleaseGroup, task.TargetID) == nil {
		groups, err := releasegroups.NewExecutionRepository(repository.store)
		if err != nil {
			return ScriptWorkInput{}, err
		}
		authority, err = groups.PreflightAuthority(ctx, view)
		if err != nil {
			return ScriptWorkInput{}, err
		}
	} else {
		return ScriptWorkInput{}, errs.New(errs.KindStateConflict, "Task action does not authorize this Script work")
	}
	key := scriptexecutions.WorkKey(item.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision})
	if err != nil {
		return ScriptWorkInput{}, err
	}
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != key || read.Values[0].ModRevision <= 0 || read.Values[0].ModRevision > view.ReadRevision {
		return ScriptWorkInput{}, errs.New(errs.KindStateConflict, "Script captured work binding is unavailable")
	}
	binding, err := recordcodec.Decode[scriptexecutions.WorkBinding](read.Values[0].Value, "script-work")
	if err != nil || binding.Validate() != nil || binding.TaskID != task.ID || binding.WorkItemID != item.ID ||
		binding.Input != *item.Input {
		return ScriptWorkInput{}, errs.New(errs.KindInternal, "Script work binding disagrees with captured input")
	}
	input, err := repository.loadScriptExecutionForWork(ctx, view, binding.ExecutionID, binding.Input)
	if err != nil {
		return ScriptWorkInput{}, err
	}
	if input.Execution.ScriptID != item.TargetID {
		input.Execution.Clear()
		return ScriptWorkInput{}, errs.New(errs.KindInternal, "Script work target differs from captured execution")
	}
	input.Conditions = append(input.Conditions, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision})
	input.Conditions = append(input.Conditions, authority...)
	return input, nil
}

func (repository *ScriptRepository) loadScriptExecutionForWork(ctx context.Context, view workqueue.View,
	executionID string, expected workexecution.Input,
) (ScriptWorkInput, error) {
	task, item := view.Task.Task, view.Item.Item
	executionKey := scriptexecutions.ScriptExecutionKey(executionID)
	executionRead, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{executionKey}, Revision: view.ReadRevision,
	})
	if err != nil {
		return ScriptWorkInput{}, err
	}
	if executionRead != nil {
		defer keyvalue.ClearValues(executionRead.Values)
	}
	if executionRead == nil || executionRead.ReadRevision != view.ReadRevision || len(executionRead.Values) != 1 ||
		executionRead.Values[0] == nil || executionRead.Values[0].Key != executionKey ||
		executionRead.Values[0].ModRevision <= 0 || executionRead.Values[0].ModRevision > view.ReadRevision {
		return ScriptWorkInput{}, errs.New(errs.KindStateConflict, "Script captured execution is unavailable")
	}
	execution, err := recordcodec.Decode[scriptexecutions.ScriptExecutionRecord](executionRead.Values[0].Value, "script-execution")
	if err != nil || scriptexecutions.ValidateScriptExecutionRecord(execution) != nil ||
		execution.ID != executionID || execution.CurrentTaskID != task.ID || execution.WorkItemID != item.ID ||
		execution.Input != expected ||
		execution.WorkInput == nil || *execution.WorkInput != *item.Input ||
		execution.EnvironmentID != task.Scope.EnvironmentID {
		execution.Clear()
		return ScriptWorkInput{}, errs.New(errs.KindInternal, "Script execution disagrees with captured work")
	}
	var snapshot agentpb.ResolvedRunnerSnapshot
	if proto.Unmarshal(execution.Snapshot, &snapshot) != nil || snapshot.TenantId != task.Scope.TenantID ||
		snapshot.ProjectId != task.Scope.ProjectID || snapshot.EnvironmentId != task.Scope.EnvironmentID {
		execution.Clear()
		return ScriptWorkInput{}, errs.New(errs.KindInternal, "Script snapshot belongs to another resource scope")
	}
	return ScriptWorkInput{Execution: execution, Conditions: []keyvalue.Condition{
		{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision},
		{Key: executionKey, ModRevision: executionRead.Values[0].ModRevision},
	}}, nil
}
