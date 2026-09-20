package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) applyBackingHookTerminal(
	ctx context.Context,
	task TaskRecord,
	assignment TaskAssignmentRecord,
	input AttachTaskRenderInput,
	revision int64,
	change *attachTaskChange,
) error {
	if input.HookConfiguration == nil || change == nil {
		return nil
	}
	event := backinghook.Attach
	definition := input.HookConfiguration.Attach
	if task.Type == TaskDetach {
		event = backinghook.Detach
		definition = input.HookConfiguration.Detach
	}
	if definition == nil {
		return nil
	}
	if len(task.Steps) < 2 {
		return errs.New(errs.KindInternal, "hooked Attach terminal Task is incomplete")
	}
	checkpoint, condition, err := repository.requireBackingHookResultCheckpoint(
		ctx, task, assignment, task.Steps[0].ID, task.Target, event, revision,
	)
	if err != nil {
		return err
	}
	if checkpoint.Facts != nil {
		defer clear(checkpoint.Facts.Ciphertext)
	}
	change.conditions = append(change.conditions, condition)
	if event != backinghook.Attach {
		if checkpoint.Facts != nil {
			return errs.New(errs.KindInternal, "Detach hook checkpoint contains facts")
		}
		return nil
	}
	if checkpoint.Facts == nil || checkpoint.Facts.AttachID != task.Target {
		return errs.New(errs.KindInternal, "Attach hook checkpoint facts are missing")
	}
	factKey := attachFactsKey(task.Target)
	current, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{factKey}, Revision: revision})
	if err != nil {
		return err
	}
	if current == nil || current.ReadRevision != revision || len(current.Values) != 1 || current.Values[0] == nil {
		if current != nil {
			clearKeyValues(current.Values)
		}
		return errs.New(errs.KindInternal, "Attach hook input bundle is missing")
	}
	defer clearKeyValues(current.Values)
	value, err := encodeAttachEncryptedFacts(*checkpoint.Facts)
	if err != nil {
		return err
	}
	change.conditions = append(change.conditions, Condition{Key: factKey, ModRevision: current.Values[0].ModRevision})
	change.mutations = append(change.mutations, Mutation{Type: MutationPut, Key: factKey, Value: value})
	change.values = append(change.values, value)
	change.mutates = true
	return nil
}

func (repository *TaskRepository) requireBackingHookResultCheckpoint(
	ctx context.Context,
	task TaskRecord,
	assignment TaskAssignmentRecord,
	stepID string,
	attachID string,
	event backinghook.Event,
	revision int64,
) (BackingHookCheckpointRecord, Condition, error) {
	key := backingHookCheckpointKey(task.ID, stepID)
	checkpointResult, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return BackingHookCheckpointRecord{}, Condition{}, err
	}
	if checkpointResult == nil || checkpointResult.ReadRevision != revision ||
		len(checkpointResult.Values) != 1 || checkpointResult.Values[0] == nil {
		if checkpointResult != nil {
			clearKeyValues(checkpointResult.Values)
		}
		return BackingHookCheckpointRecord{}, Condition{}, errs.New(
			errs.KindStateConflict,
			"Backing hook RESULT checkpoint is missing",
		)
	}
	defer clearKeyValues(checkpointResult.Values)
	checkpoint, err := decodeEnvelope[BackingHookCheckpointRecord](
		checkpointResult.Values[0].Value,
		"backing-hook-checkpoint",
	)
	if err != nil || validateBackingHookCheckpointRecord(checkpoint) != nil ||
		checkpoint.TaskID != task.ID || checkpoint.OperationID != task.OperationID ||
		checkpoint.AssignmentID != assignment.AssignmentID || checkpoint.ExecutionEpoch != assignment.ExecutionEpoch ||
		checkpoint.StepID != stepID || checkpoint.PlanHash != task.PlanHash ||
		checkpoint.AttachID != attachID || checkpoint.Event != string(event) ||
		checkpoint.State != BackingHookCheckpointResult {
		if checkpoint.Facts != nil {
			clear(checkpoint.Facts.Ciphertext)
		}
		return BackingHookCheckpointRecord{}, Condition{}, errs.New(
			errs.KindStateConflict,
			"Backing hook RESULT checkpoint does not match the terminal Task",
		)
	}
	return checkpoint, Condition{
		Key: key, ModRevision: checkpointResult.Values[0].ModRevision,
	}, nil
}
