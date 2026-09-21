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
)

func (repository *BackupRuntimeRepository) loadBackupAssignmentFence(
	ctx context.Context,
	input backupruntime.BackupAssignmentInput,
	revision int64,
) ([]etcdstore.Condition, error) {
	if recordcodec.ValidateID(ids.KindTask, input.TaskID) != nil ||
		recordcodec.ValidateID(ids.KindAssignment, input.AssignmentID) != nil ||
		recordcodec.ValidateID(ids.KindStep, input.StepID) != nil || revision <= 0 ||
		(input.AgentID == "" && input.AgentGeneration != 0) ||
		(input.AgentID != "" &&
			(recordcodec.ValidateID(ids.KindAgent, input.AgentID) != nil || input.AgentGeneration == 0)) {
		return nil, errs.New(errs.KindValidationFailed, "backup assignment fence is invalid")
	}
	taskResult, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{taskjournal.TaskStorageKey(input.TaskID)}, Revision: revision,
	})
	if err != nil {
		return nil, err
	}
	if taskResult == nil || taskResult.ReadRevision != revision || len(taskResult.Values) != 1 ||
		taskResult.Values[0] == nil {
		return nil, errs.New(errs.KindStateConflict, "backup task assignment changed")
	}
	defer etcdstore.ClearValues(taskResult.Values)
	task, err := decodeTaskRecord(taskResult.Values[0].Value)
	if err != nil || task.ID != input.TaskID {
		return nil, backupruntime.CorruptBackupRuntimeRecord()
	}
	if task.Status != taskjournal.TaskStatusRunning || !taskContainsStep(task, input.StepID) {
		return nil, errs.New(errs.KindStateConflict, "backup task assignment changed")
	}
	claimKey := taskjournal.TaskExecutionClaimKey(task.Executor, input.AgentID, input.TaskID)
	assignmentResult, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{claimKey, taskjournal.TaskAssignmentIndexKey(input.TaskID)}, Revision: revision,
	})
	if err != nil {
		return nil, err
	}
	if assignmentResult == nil || assignmentResult.ReadRevision != revision ||
		len(assignmentResult.Values) != 2 || assignmentResult.Values[0] == nil ||
		assignmentResult.Values[1] == nil ||
		assignmentResult.Values[0].ModRevision != assignmentResult.Values[1].ModRevision ||
		!bytes.Equal(assignmentResult.Values[0].Value, assignmentResult.Values[1].Value) {
		return nil, errs.New(errs.KindStateConflict, "backup task assignment changed")
	}
	defer etcdstore.ClearValues(assignmentResult.Values)
	assignment, err := taskassignments.DecodeTaskAssignment(assignmentResult.Values[0].Value)
	if err != nil || assignment.AssignmentID != input.AssignmentID ||
		assignment.TaskID != input.TaskID || assignment.Executor != task.Executor ||
		assignment.AgentID != input.AgentID || assignment.AgentGeneration != input.AgentGeneration {
		return nil, errs.New(errs.KindStateConflict, "backup task assignment changed")
	}
	timeoutResult, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{taskjournal.TaskTimeoutIndexKey(input.TaskID, assignment.Deadline)}, Revision: revision,
	})
	if err != nil {
		return nil, err
	}
	if timeoutResult == nil || timeoutResult.ReadRevision != revision ||
		len(timeoutResult.Values) != 1 ||
		timeoutResult.Values[0] == nil ||
		timeoutResult.Values[0].ModRevision != assignmentResult.Values[0].ModRevision ||
		!bytes.Equal(timeoutResult.Values[0].Value, assignmentResult.Values[0].Value) {
		return nil, errs.New(errs.KindStateConflict, "backup task assignment changed")
	}
	defer etcdstore.ClearValues(timeoutResult.Values)
	return []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(input.TaskID), ModRevision: taskResult.Values[0].ModRevision},
		{Key: claimKey, ModRevision: assignmentResult.Values[0].ModRevision},
		{
			Key:         taskjournal.TaskAssignmentIndexKey(input.TaskID),
			ModRevision: assignmentResult.Values[1].ModRevision,
		},
		{
			Key:         taskjournal.TaskTimeoutIndexKey(input.TaskID, assignment.Deadline),
			ModRevision: timeoutResult.Values[0].ModRevision,
		},
	}, nil
}

func backupAssignmentFromCheckpoint(input backupruntime.BackupCheckpointInput) backupruntime.BackupAssignmentInput {
	return backupruntime.BackupAssignmentInput{
		TaskID: input.TaskID, AssignmentID: input.AssignmentID, AgentID: input.AgentID,
		AgentGeneration: input.AgentGeneration, StepID: input.StepID,
	}
}
