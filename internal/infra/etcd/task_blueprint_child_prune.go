package etcd

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// A terminal child can still own an uncertain resource claim. Its journal and
// plan must remain available until the coordinator has settled that claim.
func (repository *TaskRepository) blueprintChildPruneHeld(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) (bool, error) {
	if !taskjournal.IsBlueprintChild(task.Params) {
		return false, nil
	}
	key := blueprintunits.ExecutionKey(task.Owner.EnvironmentID, task.PlanID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{key}, Revision: revision,
	})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return false, errs.New(errs.KindInternal, "Blueprint child prune claim read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return false, nil
	}
	claim, err := blueprintunits.DecodeExecution(read.Values[0].Value)
	if err != nil || claim.EnvironmentID != task.Owner.EnvironmentID ||
		claim.PlanID != task.PlanID || claim.TaskID != task.ID ||
		claim.ParentTaskID != task.Params[taskjournal.TaskBlueprintParentParam] {
		return false, errs.New(errs.KindInternal, "Blueprint child prune claim changed")
	}
	return true, nil
}

// The acknowledged Component runtime owns its own artifact. Once no execution,
// assignment or recovery owner remains, its retired child's recipe can expire
// atomically with the Task; a terminal report alone does not release it.
func (repository *TaskRepository) prepareBlueprintComponentExecutionPrune(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	if task.Params[taskjournal.TaskBlueprintComponentUnitParam] == "" {
		return nil, nil, nil
	}
	stored, err := componentplanning.ReadChildExecution(ctx, repository.store, task.ID, revision)
	if err != nil {
		return nil, nil, err
	}
	plan, err := stored.Record.ExecutionPlan()
	if err != nil {
		return nil, nil, err
	}
	if !taskjournal.IsBlueprintChild(task.Params) ||
		stored.Record.ParentTaskID != task.Params[taskjournal.TaskBlueprintParentParam] ||
		stored.Record.EnvironmentID != task.Owner.EnvironmentID ||
		stored.Record.Component.Desired.ID != task.Params[taskjournal.TaskBlueprintComponentUnitParam] ||
		plan.GetPlanId() != task.PlanID ||
		hex.EncodeToString(plan.GetPlanHash()) != task.PlanHash ||
		plan.GetRenderGeneration() != uint64(task.RenderGeneration) {
		return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Component recipe prune ownership changed")
	}
	keys := []string{
		blueprintunits.ExecutionKey(task.Owner.EnvironmentID, task.PlanID),
		taskjournal.TaskActiveOperationKey(task.OperationID),
		taskjournal.TaskAssignmentIndexKey(task.ID),
		taskjournal.TaskRecoveryProofRequiredKey(task.ID),
		taskassignments.ComponentRecoveryKey(task.ID),
		taskjournal.TaskStorageKey(stored.Record.ParentTaskID),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return nil, nil, errs.New(errs.KindInternal, "Blueprint Component recipe prune read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	conditions := []keyvalue.Condition{
		{Key: componentplanning.ChildExecutionKey(task.ID), ModRevision: stored.Revision},
	}
	for index, key := range keys {
		if index == len(keys)-1 {
			if read.Values[index] != nil {
				parent, err := DecodeTaskRecord(read.Values[index].Value)
				if err != nil || parent.ID != stored.Record.ParentTaskID ||
					parent.Owner.EnvironmentID != task.Owner.EnvironmentID {
					return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Component recipe parent changed")
				}
				if !taskjournal.IsTerminalTaskStatus(parent.Status) {
					return nil, nil, errs.New(
						errs.KindStateConflict,
						"Blueprint Apply still owns this Component's captured Service replacements",
					)
				}
			}
			conditions = append(
				conditions,
				keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[index])},
			)
			continue
		}
		if read.Values[index] != nil {
			return nil, nil, errs.New(
				errs.KindStateConflict,
				"Blueprint Component recipe still has an execution or recovery owner",
			)
		}
		conditions = append(conditions, keyvalue.Condition{Key: key})
	}
	return conditions, []keyvalue.Mutation{
		{Type: keyvalue.MutationDelete, Key: componentplanning.ChildExecutionKey(task.ID)},
	}, nil
}
