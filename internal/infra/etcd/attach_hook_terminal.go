package etcd

import (
	"context"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	backinghooks "github.com/AlanD20/groundplane/internal/infra/etcd/backinghooks"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) applyBackingHookTerminal(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	input AttachTaskRenderInput,
	revision int64,
	change *attachTaskChange,
) error {
	if input.HookConfiguration == nil || change == nil {
		return nil
	}
	event := backinghook.Attach
	definition := input.HookConfiguration.Attach
	if task.Type == taskjournal.TaskDetach {
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
	factKey := attachrecord.AttachFactsKey(task.Target)
	current, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{factKey}, Revision: revision})
	if err != nil {
		return err
	}
	if current == nil || current.ReadRevision != revision || len(current.Values) != 1 || current.Values[0] == nil {
		if current != nil {
			etcdstore.ClearValues(current.Values)
		}
		return errs.New(errs.KindInternal, "Attach hook input bundle is missing")
	}
	defer etcdstore.ClearValues(current.Values)
	value, err := attachrecord.EncodeAttachEncryptedFacts(*checkpoint.Facts)
	if err != nil {
		return err
	}
	change.conditions = append(change.conditions, etcdstore.Condition{Key: factKey, ModRevision: current.Values[0].ModRevision})
	change.mutations = append(change.mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: factKey, Value: value})
	change.values = append(change.values, value)
	change.mutates = true
	return nil
}

func (repository *TaskRepository) requireBackingHookResultCheckpoint(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	stepID string,
	attachID string,
	event backinghook.Event,
	revision int64,
) (backinghooks.CheckpointRecord, etcdstore.Condition, error) {
	key := backinghooks.CheckpointKey(task.ID, stepID)
	checkpointResult, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return backinghooks.CheckpointRecord{}, etcdstore.Condition{}, err
	}
	if checkpointResult == nil || checkpointResult.ReadRevision != revision ||
		len(checkpointResult.Values) != 1 || checkpointResult.Values[0] == nil {
		if checkpointResult != nil {
			etcdstore.ClearValues(checkpointResult.Values)
		}
		return backinghooks.CheckpointRecord{}, etcdstore.Condition{}, errs.New(
			errs.KindStateConflict,
			"Backing hook RESULT checkpoint is missing",
		)
	}
	defer etcdstore.ClearValues(checkpointResult.Values)
	checkpoint, err := recordcodec.Decode[backinghooks.CheckpointRecord](
		checkpointResult.Values[0].Value,
		"backing-hook-checkpoint",
	)
	if err != nil || backinghooks.ValidateCheckpointRecord(checkpoint) != nil ||
		checkpoint.TaskID != task.ID || checkpoint.OperationID != task.OperationID ||
		checkpoint.AssignmentID != assignment.AssignmentID || checkpoint.ExecutionEpoch != assignment.ExecutionEpoch ||
		checkpoint.StepID != stepID || checkpoint.PlanHash != task.PlanHash ||
		checkpoint.AttachID != attachID || checkpoint.Event != string(event) ||
		checkpoint.State != backinghooks.CheckpointResult {
		if checkpoint.Facts != nil {
			clear(checkpoint.Facts.Ciphertext)
		}
		return backinghooks.CheckpointRecord{}, etcdstore.Condition{}, errs.New(
			errs.KindStateConflict,
			"Backing hook RESULT checkpoint does not match the terminal Task",
		)
	}
	return checkpoint, etcdstore.Condition{
		Key: key, ModRevision: checkpointResult.Values[0].ModRevision,
	}, nil
}
