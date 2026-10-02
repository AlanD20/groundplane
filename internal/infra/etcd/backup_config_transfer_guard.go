package etcd

import (
	"bytes"
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (repository *BackupRuntimeRepository) configTransferGuard(
	ctx context.Context,
	owner backupconfiguration.ConfigTransferOwner,
	revision int64,
) ([]etcdstore.Condition, error) {
	if backupconfiguration.ValidateConfigTransferOwner(owner) != nil || revision <= 0 {
		return nil, configTransferAuthorityConflict()
	}
	binding := owner.Binding
	keys := []string{
		taskjournal.TaskStorageKey(binding.TaskID), backupruntime.BackupExecutionPlanKey(binding.TaskID),
		taskjournal.TaskExecutionClaimKey(taskjournal.TaskExecutorAgent, owner.AgentID, binding.TaskID),
		taskjournal.TaskAssignmentIndexKey(binding.TaskID),
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return nil, configTransferAuthorityConflict()
	}
	defer etcdstore.ClearValues(read.Values)
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] || value.ModRevision <= 0 {
			return nil, configTransferAuthorityConflict()
		}
	}
	task, taskErr := DecodeTaskRecord(read.Values[0].Value)
	plan, planErr := backupruntime.DecodeBackupExecutionPlan(read.Values[1].Value)
	assignment, assignmentErr := taskassignments.DecodeTaskAssignment(read.Values[2].Value)
	if taskErr != nil || planErr != nil || assignmentErr != nil || read.Values[1].Version != 1 ||
		task.ID != binding.TaskID || task.Status != taskjournal.TaskStatusRunning || task.Executor != taskjournal.TaskExecutorAgent ||
		task.PlanID != plan.PlanId || task.PlanHash != hex.EncodeToString(plan.PlanHash) || task.Target != plan.TargetId ||
		assignment.TaskID != task.ID || assignment.AssignmentID != binding.AssignmentID || assignment.AgentID != owner.AgentID ||
		assignment.AgentGeneration != owner.AgentGeneration || assignment.Executor != task.Executor || assignment.BackupAuthorityFence == nil ||
		!assignment.BackupAuthorityFence.MatchesCheckpoint(
			owner.AssignmentGeneration,
			binding.StepID,
			binding.ExecutionID,
			owner.AuthoritySHA256,
		) ||
		read.Values[2].ModRevision != read.Values[3].ModRevision || !bytes.Equal(read.Values[2].Value, read.Values[3].Value) ||
		!time.Now().UTC().Before(assignment.Deadline) {
		return nil, configTransferAuthorityConflict()
	}
	authority, digest, err := executionplan.BindBackupTaskAuthority(plan, executionplan.BackupAssignmentIdentity{
		TaskID: task.ID, OperationID: task.OperationID, AssignmentID: binding.AssignmentID,
		Generation: owner.AssignmentGeneration, DeadlineUnixNano: uint64(assignment.Deadline.UnixNano()),
	})
	if err != nil || hex.EncodeToString(digest) != assignment.BackupAuthorityFence.AuthoritySHA256 {
		return nil, configTransferAuthorityConflict()
	}
	matched := false
	for _, step := range authority.Steps {
		if step.StepId != binding.StepID || step.ExecutionId != binding.ExecutionID {
			continue
		}
		if step.StepDeadlineUnixNano <= uint64(time.Now().UnixNano()) ||
			hex.EncodeToString(step.StepDigest) != owner.AuthoritySHA256 {
			return nil, configTransferAuthorityConflict()
		}
		switch binding.Direction {
		case agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_CAPTURE:
			matched = task.Type == taskjournal.TaskBackup && step.GetCapture().GetConfig() != nil
		case agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE:
			matched = task.Type == taskjournal.TaskRestore && step.GetRestore().GetConfig() != nil
		}
	}
	if !matched {
		return nil, configTransferAuthorityConflict()
	}
	moreKeys := []string{
		taskjournal.TaskTimeoutIndexKey(task.ID, assignment.Deadline),
		hierarchy.EnvironmentOperationLockKey(plan.TargetId),
	}
	more, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: moreKeys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if more == nil || more.ReadRevision != revision || len(more.Values) != 2 || more.Values[0] == nil ||
		more.Values[1] == nil {
		return nil, configTransferAuthorityConflict()
	}
	defer etcdstore.ClearValues(more.Values)
	lock, err := backupruntime.DecodeBackupOperationLockRecord(more.Values[1].Value)
	expectedKind := backupruntime.BackupOperationBackup
	if binding.Direction == agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE {
		expectedKind = backupruntime.BackupOperationRestore
	}
	if err != nil || lock.EnvironmentID != plan.TargetId || lock.TaskID != task.ID ||
		lock.OperationID != task.OperationID ||
		lock.Kind != expectedKind ||
		more.Values[0].Key != moreKeys[0] ||
		more.Values[1].Key != moreKeys[1] ||
		more.Values[0].ModRevision != read.Values[2].ModRevision ||
		!bytes.Equal(more.Values[0].Value, read.Values[2].Value) ||
		more.Values[1].ModRevision <= 0 {
		return nil, configTransferAuthorityConflict()
	}
	conditions := make([]etcdstore.Condition, 0, 6)
	for index, value := range read.Values {
		conditions = append(conditions, etcdstore.Condition{Key: keys[index], ModRevision: value.ModRevision})
	}
	for index, value := range more.Values {
		conditions = append(conditions, etcdstore.Condition{Key: moreKeys[index], ModRevision: value.ModRevision})
	}
	return conditions, nil
}

func configTransferAuthorityConflict() error {
	return errs.New(errs.KindStateConflict, "Config transfer native Task or source authority changed")
}
