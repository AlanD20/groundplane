package etcd

import (
	"bytes"
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"google.golang.org/protobuf/proto"
	"time"
)

func (repository *BackupRuntimeRepository) loadBackupCheckpointPlan(
	ctx context.Context,
	input backupruntime.BackupCheckpointInput,
	revision int64,
	binding backupCheckpointBinding,
) (backupCheckpointPlan, error) {
	digest, err := backupruntime.BackupCheckpointDigest(input)
	if err != nil || backupruntime.ValidateBackupCheckpointInput(input) != nil ||
		validateBackupCheckpointBinding(binding) != nil || revision <= 0 {
		return backupCheckpointPlan{}, errs.New(
			errs.KindValidationFailed,
			"backup checkpoint is invalid",
		)
	}
	taskRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{taskjournal.TaskStorageKey(input.TaskID)}, Revision: revision,
	})
	if err != nil {
		return backupCheckpointPlan{}, err
	}
	if taskRead == nil || taskRead.ReadRevision != revision || len(taskRead.Values) != 1 ||
		taskRead.Values[0] == nil {
		return backupCheckpointPlan{}, errs.New(
			errs.KindStateConflict,
			"backup task assignment changed",
		)
	}
	defer etcdstore.ClearValues(taskRead.Values)
	task, err := DecodeTaskRecord(taskRead.Values[0].Value)
	if err != nil || task.ID != input.TaskID {
		return backupCheckpointPlan{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	if task.Status != taskjournal.TaskStatusRunning || !taskContainsStep(task, input.StepID) {
		return backupCheckpointPlan{}, errs.New(
			errs.KindStateConflict,
			"backup task assignment changed",
		)
	}
	if task.Type != binding.taskType || int(binding.ordinal) >= len(task.Steps) ||
		task.Steps[binding.ordinal].ID != input.StepID ||
		(backupruntime.BackupCheckpointPointID(input.Request) != "" &&
			backupruntime.BackupCheckpointPointID(input.Request) != binding.pointID) {
		return backupCheckpointPlan{}, errs.New(
			errs.KindValidationFailed,
			"backup checkpoint step binding is invalid",
		)
	}
	claimKey := taskjournal.TaskExecutionClaimKey(task.Executor, input.AgentID, input.TaskID)
	cursorKey := backupruntime.BackupCheckpointCursorKey(input)
	dedupKey := backupruntime.BackupCheckpointDedupKey(input)
	assignmentRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			claimKey,
			taskjournal.TaskAssignmentIndexKey(input.TaskID),
			cursorKey,
			dedupKey,
		},
		Revision: revision,
	})
	if err != nil {
		return backupCheckpointPlan{}, err
	}
	if assignmentRead == nil || assignmentRead.ReadRevision != revision ||
		len(assignmentRead.Values) != 4 || assignmentRead.Values[0] == nil ||
		assignmentRead.Values[1] == nil {
		return backupCheckpointPlan{}, errs.New(
			errs.KindStateConflict,
			"backup task assignment changed",
		)
	}
	defer etcdstore.ClearValues(assignmentRead.Values)
	claimValue := assignmentRead.Values[0]
	indexValue := assignmentRead.Values[1]
	if claimValue.ModRevision != indexValue.ModRevision ||
		!bytes.Equal(claimValue.Value, indexValue.Value) {
		return backupCheckpointPlan{}, errs.New(
			errs.KindInternal,
			"backup task assignment copies differ",
		)
	}
	assignment, err := taskassignments.DecodeTaskAssignment(claimValue.Value)
	if err != nil || assignment.AssignmentID != input.AssignmentID ||
		assignment.TaskID != input.TaskID ||
		assignment.Executor != task.Executor ||
		assignment.AgentID != input.AgentID ||
		assignment.AgentGeneration != input.AgentGeneration {
		return backupCheckpointPlan{}, errs.New(
			errs.KindStateConflict,
			"backup task assignment changed",
		)
	}
	if !assignment.BackupAuthorityFence.MatchesCheckpoint(input.AssignmentGeneration,
		input.StepID, input.ExecutionID, input.AuthoritySHA256) {
		return backupCheckpointPlan{}, errs.New(errs.KindStateConflict, "backup checkpoint authority changed")
	}
	timeoutRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{taskjournal.TaskTimeoutIndexKey(input.TaskID, assignment.Deadline)}, Revision: revision,
	})
	if err != nil {
		return backupCheckpointPlan{}, err
	}
	if timeoutRead == nil || timeoutRead.ReadRevision != revision || len(timeoutRead.Values) != 1 ||
		timeoutRead.Values[0] == nil || timeoutRead.Values[0].ModRevision != claimValue.ModRevision ||
		!bytes.Equal(timeoutRead.Values[0].Value, claimValue.Value) {
		return backupCheckpointPlan{}, errs.New(
			errs.KindStateConflict,
			"backup task assignment changed",
		)
	}
	defer etcdstore.ClearValues(timeoutRead.Values)
	nextSequence := uint64(1)
	if assignmentRead.Values[2] != nil {
		cursor, decodeErr := backupruntime.DecodeBackupCheckpointCursorRecord(assignmentRead.Values[2].Value)
		if decodeErr != nil || cursor.TaskID != input.TaskID ||
			cursor.AssignmentID != input.AssignmentID ||
			cursor.StepID != input.StepID || cursor.ExecutionID != input.ExecutionID ||
			cursor.AssignmentGeneration != input.AssignmentGeneration || cursor.AuthoritySHA256 != input.AuthoritySHA256 {
			return backupCheckpointPlan{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		nextSequence = cursor.NextSequence
	}
	if assignmentRead.Values[3] != nil {
		cursorValue := assignmentRead.Values[2]
		dedupValue := assignmentRead.Values[3]
		if cursorValue == nil || dedupValue.Version != 1 || cursorValue.ModRevision <= 0 ||
			dedupValue.ModRevision <= 0 || dedupValue.ModRevision > cursorValue.ModRevision {
			return backupCheckpointPlan{}, errs.New(
				errs.KindStateConflict,
				"backup checkpoint replay evidence changed",
			)
		}
		dedup, decodeErr := backupruntime.DecodeBackupCheckpointDedupRecord(assignmentRead.Values[3].Value)
		if decodeErr != nil || dedup.TaskID != input.TaskID ||
			dedup.AssignmentID != input.AssignmentID ||
			dedup.StepID != input.StepID ||
			dedup.Sequence != input.Sequence ||
			dedup.CheckpointTag != backupruntime.BackupCheckpointTag(
				input.Request,
			) || dedup.ExecutionID != input.ExecutionID ||
			dedup.AssignmentGeneration != input.AssignmentGeneration || dedup.AuthoritySHA256 != input.AuthoritySHA256 ||
			dedup.PrecedingCheckpointRevision != input.PrecedingCheckpointRevision {
			return backupCheckpointPlan{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		if dedup.PayloadSHA256 != digest {
			return backupCheckpointPlan{}, errs.New(
				errs.KindStateConflict,
				"backup checkpoint digest changed",
			)
		}
		if input.Sequence == ^uint64(0) || nextSequence < input.Sequence+1 {
			return backupCheckpointPlan{}, errs.New(
				errs.KindStateConflict,
				"backup checkpoint sequence changed",
			)
		}
		return backupCheckpointPlan{
			readRevision: revision, commitRevision: dedupValue.ModRevision,
			digest: digest, duplicate: true,
		}, nil
	}
	if !time.Now().UTC().Before(assignment.Deadline) {
		return backupCheckpointPlan{}, errs.New(errs.KindStateConflict, "backup assignment deadline has expired")
	}
	if input.Sequence != nextSequence {
		return backupCheckpointPlan{}, errs.New(
			errs.KindStateConflict,
			"backup checkpoint sequence changed",
		)
	}
	if input.Sequence == ^uint64(0) ||
		(input.Sequence == 1 && (input.PrecedingCheckpointRevision != 0 || assignmentRead.Values[2] != nil)) ||
		(input.Sequence > 1 && (assignmentRead.Values[2] == nil ||
			input.PrecedingCheckpointRevision != assignmentRead.Values[2].ModRevision)) {
		return backupCheckpointPlan{}, errs.New(errs.KindStateConflict, "backup checkpoint predecessor changed")
	}
	cursor := backupruntime.BackupCheckpointCursorRecord{
		TaskID: input.TaskID, AssignmentID: input.AssignmentID, StepID: input.StepID,
		ExecutionID: input.ExecutionID, AssignmentGeneration: input.AssignmentGeneration,
		AuthoritySHA256: input.AuthoritySHA256,
		NextSequence:    input.Sequence + 1,
	}
	dedup := backupruntime.BackupCheckpointDedupRecord{
		TaskID: input.TaskID, AssignmentID: input.AssignmentID, StepID: input.StepID,
		Sequence: input.Sequence, CheckpointTag: backupruntime.BackupCheckpointTag(input.Request), PayloadSHA256: digest,
		ExecutionID: input.ExecutionID, AssignmentGeneration: input.AssignmentGeneration,
		AuthoritySHA256: input.AuthoritySHA256, PrecedingCheckpointRevision: input.PrecedingCheckpointRevision,
	}
	dedup.Request, err = (proto.MarshalOptions{Deterministic: true}).Marshal(input.Request)
	if err != nil {
		return backupCheckpointPlan{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(dedup.Request)
	cursorValue, err := backupruntime.EncodeBackupCheckpointCursorRecord(cursor)
	if err != nil {
		return backupCheckpointPlan{}, err
	}
	dedupValue, err := backupruntime.EncodeBackupCheckpointDedupRecord(dedup)
	if err != nil {
		clear(cursorValue)
		return backupCheckpointPlan{}, err
	}
	conditions := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(input.TaskID), ModRevision: taskRead.Values[0].ModRevision},
		{Key: claimKey, ModRevision: claimValue.ModRevision},
		{Key: taskjournal.TaskAssignmentIndexKey(input.TaskID), ModRevision: indexValue.ModRevision},
		{
			Key:         taskjournal.TaskTimeoutIndexKey(input.TaskID, assignment.Deadline),
			ModRevision: timeoutRead.Values[0].ModRevision,
		},
		{Key: dedupKey},
	}
	if assignmentRead.Values[2] == nil {
		conditions = append(conditions, etcdstore.Condition{Key: cursorKey})
	} else {
		conditions = append(conditions, etcdstore.Condition{
			Key: cursorKey, ModRevision: assignmentRead.Values[2].ModRevision,
		})
	}
	return backupCheckpointPlan{
		conditions: conditions,
		mutations: []etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: cursorKey, Value: cursorValue},
			{Type: etcdstore.MutationPut, Key: dedupKey, Value: dedupValue},
		},
		readRevision: revision,
		digest:       digest,
	}, nil
}

func validateBackupCheckpointBinding(binding backupCheckpointBinding) error {
	if (binding.taskType != taskjournal.TaskBackup && binding.taskType != taskjournal.TaskBackupPrune && binding.taskType != taskjournal.TaskRestore) ||
		recordcodec.ValidateID(ids.KindRecoveryPoint, binding.pointID) != nil {
		return errs.New(errs.KindValidationFailed, "backup checkpoint binding is invalid")
	}
	return nil
}
