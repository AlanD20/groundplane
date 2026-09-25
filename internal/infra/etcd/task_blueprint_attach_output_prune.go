package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	attachinputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	attachoutputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachoutputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// The encrypted hook result remains while its exact child is the applied
// Attach authority. An Entry already settled from that result holds its own
// immutable value generation, so replacing/removing the Attach releases the
// old result once no execution or recovery claim remains.
func (repository *TaskRepository) pruneChildOwnedBlueprintAttachOutput(
	ctx context.Context,
	task TaskRecord,
	taskRevision int64,
	retention etcdstore.KeyValue,
	readRevision int64,
) (bool, error) {
	parentID := task.Params[taskjournal.TaskBlueprintParentParam]
	attachID := task.Params[attachinputs.TaskAttachIDParam]
	if parentID == "" || attachID == "" {
		return false, nil
	}
	if !taskjournal.IsBlueprintChild(task.Params) || task.Executor != taskjournal.TaskExecutorAgent ||
		task.Type != taskjournal.TaskUpdate || task.Target != task.Owner.EnvironmentID {
		return true, errs.New(errs.KindStateConflict, "Blueprint Attach output prune Task shape changed")
	}
	outputKey := attachoutputs.Key(attachID, task.ID)
	appliedKey := blueprintunits.AppliedKey(
		task.Owner.EnvironmentID, blueprintunits.ResourceKey{Kind: ids.KindAttach, ID: attachID},
	)
	keys := []string{
		outputKey,
		taskjournal.TaskActiveOperationKey(task.OperationID),
		taskjournal.TaskAssignmentIndexKey(task.ID),
		taskjournal.TaskRecoveryProofRequiredKey(task.ID),
		appliedKey,
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: readRevision})
	if err != nil {
		return true, err
	}
	if read == nil || read.ReadRevision != readRevision || len(read.Values) != len(keys) {
		if read != nil {
			etcdstore.ClearValues(read.Values)
		}
		return true, errs.New(errs.KindInternal, "Blueprint Attach output prune read is incomplete")
	}
	defer etcdstore.ClearValues(read.Values)
	for index, value := range read.Values {
		if value != nil && value.Key != keys[index] {
			return true, errs.New(errs.KindInternal, "Blueprint Attach output prune read is corrupt")
		}
	}
	if read.Values[0] == nil {
		return false, nil
	}
	output, err := attachoutputs.Decode(read.Values[0].Value)
	if err != nil {
		return true, err
	}
	defer attachoutputs.Clear(&output)
	if output.EnvironmentID != task.Owner.EnvironmentID || output.ParentTaskID != parentID ||
		output.ChildTaskID != task.ID || output.AttachID != attachID ||
		output.OperationID != task.OperationID || output.PlanID != task.PlanID {
		return true, errs.New(errs.KindStateConflict, "Blueprint Attach output ownership changed")
	}
	if read.Values[1] != nil || read.Values[2] != nil || read.Values[3] != nil {
		return true, nil
	}
	if read.Values[4] != nil {
		applied, err := blueprintunits.DecodeApplied(read.Values[4].Value)
		if err != nil || applied.EnvironmentID != task.Owner.EnvironmentID ||
			applied.Target != (blueprintunits.ResourceKey{Kind: ids.KindAttach, ID: attachID}) {
			return true, errs.New(errs.KindStateConflict, "Blueprint Attach applied output owner changed")
		}
		if applied.SourceTaskID == task.ID {
			return true, nil
		}
	}
	conditions := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: taskRevision},
		{Key: retention.Key, ModRevision: retention.ModRevision},
		{Key: outputKey, ModRevision: read.Values[0].ModRevision},
		{Key: appliedKey, ModRevision: etcdstore.RevisionOf(read.Values[4])},
	}
	for _, absentKey := range keys[1:4] {
		conditions = append(conditions, etcdstore.Condition{Key: absentKey})
	}
	result, err := repository.store.Transact(ctx, conditions, []etcdstore.Mutation{{
		Type: etcdstore.MutationDelete, Key: outputKey,
	}})
	etcdstore.ClearValues(result.FailureReads)
	if err != nil {
		return true, err
	}
	if !result.Succeeded {
		return true, errs.New(errs.KindStateConflict, "Blueprint Attach output prune raced ownership")
	}
	return true, nil
}
