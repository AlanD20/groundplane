package etcd

import (
	"bytes"
	"context"
	backinghooks "github.com/AlanD20/groundplane/internal/infra/etcd/backinghooks"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// CheckpointBackingHook fences one execution boundary against the exact live
// assignment. The durable record contains only ciphertext for hook results.
func (repository *AttachRepository) CheckpointBackingHook(
	ctx context.Context,
	input backinghooks.CheckpointInput,
) (etcdstore.Versioned[backinghooks.CheckpointRecord], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[backinghooks.CheckpointRecord]{}, false, err
	}
	if repository == nil || repository.store == nil || backinghooks.ValidateCheckpointInput(input) != nil {
		return etcdstore.Versioned[backinghooks.CheckpointRecord]{}, false,
			errs.New(errs.KindValidationFailed, "Backing hook checkpoint input is invalid")
	}
	for attempt := 0; attempt < 2; attempt++ {
		anchor, err := repository.loadBackingHookCheckpointAnchor(ctx, input)
		if err != nil {
			return etcdstore.Versioned[backinghooks.CheckpointRecord]{}, false, err
		}
		if anchor.current != nil {
			if backinghooks.SameCheckpoint(*anchor.current, input) {
				return etcdstore.Versioned[backinghooks.CheckpointRecord]{
					Record: *anchor.current, Revision: anchor.checkpointRevision, ReadRevision: anchor.readRevision,
				}, true, nil
			}
			if input.State != backinghooks.CheckpointResult || anchor.current.State != backinghooks.CheckpointStarted {
				return etcdstore.Versioned[backinghooks.CheckpointRecord]{}, false,
					errs.New(errs.KindStateConflict, "Backing hook checkpoint already exists with different evidence")
			}
		}
		next, err := backinghooks.AdvanceCheckpoint(anchor.current, input)
		if err != nil {
			return etcdstore.Versioned[backinghooks.CheckpointRecord]{}, false, err
		}
		value, err := recordcodec.Encode("backing-hook-checkpoint", next)
		if err != nil {
			return etcdstore.Versioned[backinghooks.CheckpointRecord]{}, false, err
		}
		transaction, err := repository.store.Transact(ctx, anchor.conditions, []etcdstore.Mutation{{
			Type: etcdstore.MutationPut, Key: backinghooks.CheckpointKey(input.TaskID, input.StepID), Value: value,
		}})
		clear(value)
		etcdstore.ClearValues(transaction.FailureReads)
		if err != nil {
			return etcdstore.Versioned[backinghooks.CheckpointRecord]{}, false, err
		}
		if transaction.Succeeded {
			return etcdstore.Versioned[backinghooks.CheckpointRecord]{
				Record: next, Revision: transaction.Revision, ReadRevision: transaction.Revision,
			}, false, nil
		}
	}
	return etcdstore.Versioned[backinghooks.CheckpointRecord]{}, false,
		errs.New(errs.KindStateConflict, "Backing hook checkpoint changed concurrently")
}

type backingHookCheckpointAnchor struct {
	current            *backinghooks.CheckpointRecord
	checkpointRevision int64
	readRevision       int64
	conditions         []etcdstore.Condition
}

func (repository *AttachRepository) loadBackingHookCheckpointAnchor(
	ctx context.Context,
	input backinghooks.CheckpointInput,
) (backingHookCheckpointAnchor, error) {
	checkpointKey := backinghooks.CheckpointKey(input.TaskID, input.StepID)
	primary, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		taskjournal.TaskStorageKey(input.TaskID), taskjournal.TaskAssignmentIndexKey(input.TaskID), checkpointKey,
	}})
	if err != nil {
		return backingHookCheckpointAnchor{}, err
	}
	if primary == nil || len(primary.Values) != 3 || primary.Values[0] == nil || primary.Values[1] == nil {
		if primary != nil {
			etcdstore.ClearValues(primary.Values)
		}
		return backingHookCheckpointAnchor{}, errs.New(errs.KindStateConflict, "Backing hook assignment is unavailable")
	}
	defer etcdstore.ClearValues(primary.Values)
	task, taskErr := DecodeTaskRecord(primary.Values[0].Value)
	assignment, assignmentErr := taskassignments.DecodeTaskAssignment(primary.Values[1].Value)
	if taskErr != nil || assignmentErr != nil {
		return backingHookCheckpointAnchor{}, errs.New(errs.KindInternal, "Backing hook assignment is corrupt")
	}
	stepFound := false
	for _, step := range task.Steps {
		stepFound = stepFound || step.ID == input.StepID
	}
	if !stepFound || task.ID != input.TaskID || task.OperationID != input.OperationID ||
		task.Status != taskjournal.TaskStatusRunning || task.Executor != taskjournal.TaskExecutorAgent || task.PlanHash != input.PlanHash ||
		assignment.TaskID != input.TaskID || assignment.AssignmentID != input.AssignmentID ||
		assignment.AgentID != input.AgentID || assignment.AgentGeneration != input.AgentGeneration ||
		assignment.ExecutionEpoch != input.ExecutionEpoch || assignment.Executor != taskjournal.TaskExecutorAgent {
		return backingHookCheckpointAnchor{}, errs.New(
			errs.KindStateConflict,
			"Backing hook checkpoint does not own the running assignment",
		)
	}
	claimKey := taskjournal.TaskExecutionClaimKey(taskjournal.TaskExecutorAgent, input.AgentID, input.TaskID)
	claim, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{claimKey}, Revision: primary.ReadRevision},
	)
	if err != nil {
		return backingHookCheckpointAnchor{}, err
	}
	if claim == nil || len(claim.Values) != 1 || claim.Values[0] == nil {
		if claim != nil {
			etcdstore.ClearValues(claim.Values)
		}
		return backingHookCheckpointAnchor{}, errs.New(errs.KindStateConflict, "Backing hook execution claim changed")
	}
	defer etcdstore.ClearValues(claim.Values)
	if claim.Values[0].ModRevision != primary.Values[1].ModRevision ||
		!bytes.Equal(claim.Values[0].Value, primary.Values[1].Value) {
		return backingHookCheckpointAnchor{}, errs.New(errs.KindInternal, "Backing hook assignment copies diverged")
	}
	anchor := backingHookCheckpointAnchor{readRevision: primary.ReadRevision, conditions: []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(input.TaskID), ModRevision: primary.Values[0].ModRevision},
		{Key: taskjournal.TaskAssignmentIndexKey(input.TaskID), ModRevision: primary.Values[1].ModRevision},
		{Key: claimKey, ModRevision: claim.Values[0].ModRevision},
	}}
	if primary.Values[2] == nil {
		anchor.conditions = append(anchor.conditions, etcdstore.Condition{Key: checkpointKey, ModRevision: 0})
		return anchor, nil
	}
	record, err := recordcodec.Decode[backinghooks.CheckpointRecord](
		primary.Values[2].Value,
		"backing-hook-checkpoint",
	)
	if err != nil || backinghooks.ValidateCheckpointRecord(record) != nil {
		return backingHookCheckpointAnchor{}, errs.New(errs.KindInternal, "Backing hook checkpoint is corrupt")
	}
	anchor.current = &record
	anchor.checkpointRevision = primary.Values[2].ModRevision
	anchor.conditions = append(
		anchor.conditions,
		etcdstore.Condition{Key: checkpointKey, ModRevision: primary.Values[2].ModRevision},
	)
	return anchor, nil
}
