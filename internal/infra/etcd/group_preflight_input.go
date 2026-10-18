package etcd

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareGroupPreflightWork joins exact source activation, Script input and
// native readiness. The caller adds its current-intent comparisons and commits
// once. Staging a snapshot or reserving source membership grants no host effect.
func (repository *ScriptRepository) PrepareGroupPreflightWork(ctx context.Context, view workqueue.View, at time.Time) (workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if repository == nil || repository.store == nil || view.Task.Revision <= 0 || view.Item.Revision <= 0 ||
		!task.PublicationComplete || task.Stop != nil || task.FinishedAt != nil || task.CollectionStartedAt != nil ||
		item.Operation != workexecution.Script || item.Input != nil || item.Phase != workexecution.Waiting ||
		item.Wait == nil || item.Wait.Reason != workexecution.WaitInput || item.AttemptNumber != 0 || item.ForwardRevoked ||
		!at.Before(task.Deadline) || !at.Before(item.Deadline) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Group preflight is not waiting for its captured executable input")
	}
	groups, err := releasegroups.NewExecutionRepository(repository.store)
	if err != nil {
		return workqueue.Change{}, err
	}
	root, err := groups.Load(ctx, view.Task, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	index := slices.IndexFunc(root.Record.Preflight, func(hook releasegroups.PreflightHook) bool { return hook.WorkItemID == item.ID })
	if index < 0 {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Script is not captured in this Group's preflight")
	}
	hook := root.Record.Preflight[index]
	key := GroupReleasePreflightCaptureKey(task.ID, item.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision})
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Group preflight has no sealed source capture")
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	preparedHook, err := recordcodec.Decode[GroupReleasePreflightCapture](value.Value, "group-release-preflight")
	if err != nil {
		return workqueue.Change{}, err
	}
	defer preparedHook.Execution.Clear()
	execution, sources := preparedHook.Execution, preparedHook.Sources
	if value.Key != key || value.ModRevision <= 0 || value.ModRevision > view.ReadRevision ||
		execution.CurrentTaskID != task.ID || execution.WorkItemID != item.ID || execution.ScriptID != hook.ScriptID ||
		execution.ServiceID != hook.ServiceID || execution.Input != hook.Input || execution.EnvironmentID != task.Scope.EnvironmentID ||
		execution.ReleaseID != root.Record.Policy.Members[hook.Ordinal-1].Member.ReleaseID ||
		sources.Project.Record.ID != task.Scope.ProjectID || sources.Project.Record.TenantID != task.Scope.TenantID ||
		scriptexecutions.ValidateScriptExecutionRecord(execution) != nil || execution.State != scriptexecutions.ScriptExecutionNotStarted ||
		execution.AttemptID != "" || execution.Executor != nil || execution.StartAuthorized || execution.SourceMembershipCount != 0 {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Group preflight source differs from its accepted native member")
	}
	if err := validateScriptExecutionSources(sources, execution); err != nil {
		return workqueue.Change{}, err
	}
	queue, err := workqueue.NewRepository(repository.store)
	if err != nil {
		return workqueue.Change{}, err
	}
	for _, id := range item.Dependencies {
		dependency, err := queue.ReadWork(ctx, task.ID, id, view.ReadRevision)
		if err != nil {
			return workqueue.Change{}, err
		}
		if dependency.Item.Item.Phase != workexecution.Settled || dependency.Item.Item.Outcome != workexecution.Succeeded {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Group preflight prerequisites have not completed successfully")
		}
	}
	snapshotRevision, err := repository.prepareManualScriptSnapshot(ctx, execution)
	if err != nil {
		return workqueue.Change{}, err
	}
	members, err := repository.manualScriptSourceMembers(ctx, sources, execution, snapshotRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clearScriptSourcePreparationMembers(members)
	authority, err := scriptsourcepublication.NewAuthority(repository.store)
	if err != nil {
		return workqueue.Change{}, err
	}
	prepared, err := authority.PrepareOwned(ctx, execution.OperationID, members, []keyvalue.Condition{
		{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision},
		{Key: key, ModRevision: value.ModRevision},
	})
	if err != nil {
		return workqueue.Change{}, err
	}
	fragment, err := authority.FinalPublicationFragment(ctx, prepared)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer fragment.Clear()
	primary, err := preparedScriptPrimary(ctx, repository.store, sources)
	if err != nil {
		return workqueue.Change{}, err
	}
	sourceConditions, err := manualScriptSourceConditions(sources)
	if err != nil {
		return workqueue.Change{}, err
	}
	input := execution.Input
	execution.WorkInput = &input
	execution.SourceMembershipCount, execution.SourceMembershipSHA256 = prepared.MembershipCount(), prepared.MembershipSHA256()
	executionValue, err := recordcodec.Encode("script-execution", execution)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(executionValue)
	bindingValue, err := recordcodec.Encode("script-work", scriptexecutions.WorkBinding{
		TaskID: task.ID, WorkItemID: item.ID, ExecutionID: execution.ID, Input: input,
	})
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(bindingValue)
	change, err := workqueue.PrepareInput(view.Task, view.Item, input, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	conditions, err := resourcePublicationConditions(change.Conditions, fragment.Conditions(), sourceConditions,
		[]keyvalue.Condition{primary, {Key: key, ModRevision: value.ModRevision},
			{Key: releasegroups.ExecutionCaptureKey(task.ID), ModRevision: root.Revision},
			{Key: scriptexecutions.ScriptExecutionKey(execution.ID)}, {Key: scriptexecutions.WorkKey(item.ID)},
			{Key: scripts.ScriptSetBodyGenerationKey(execution.EnvironmentID, execution.ScriptSetGeneration, execution.ScriptID, execution.ScriptGeneration), ModRevision: sources.BodyGeneration.Revision},
			{Key: scripts.ScriptSetActiveKey(execution.EnvironmentID), ModRevision: sources.ScriptSet.Revision},
			{Key: releases.ReleaseIntentStagingKey("", execution.ReleaseID), ModRevision: sources.Release.IntentRevision},
			{Key: releases.ReleaseRenderInputStagingKey("", execution.ReleaseID), ModRevision: sources.RenderInput.Revision},
			{Key: scriptexecutions.ScriptRunnerSnapshotKey(execution.SnapshotID), ModRevision: snapshotRevision}})
	if err != nil {
		keyvalue.ZeroMutationBytes(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = conditions
	for _, mutation := range fragment.Mutations() {
		mutation.Value = slices.Clone(mutation.Value)
		change.Mutations = append(change.Mutations, mutation)
	}
	change.Mutations = append(change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: scriptexecutions.ScriptExecutionKey(execution.ID), Value: slices.Clone(executionValue)},
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: scriptexecutions.WorkKey(item.ID), Value: slices.Clone(bindingValue)},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key})
	return change, nil
}
