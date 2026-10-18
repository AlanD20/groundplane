package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backinghooks"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/pkg/errs"
)

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
	checkpointResult, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision},
	)
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
