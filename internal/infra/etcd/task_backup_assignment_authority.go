package etcd

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type backupAssignmentClaim struct {
	assignmentID string
	fence        *taskassignments.BackupAuthorityFence
	conditions   []etcdstore.Condition
	mutations    []etcdstore.Mutation
}

// prepareBackupAssignmentFence binds exactly the immutable procedure at the
// claim's MVCC revision. Publication and claim each compare the procedure key;
// no later resource read can supply replacement execution authority.
func (repository *TaskRepository) prepareBackupAssignmentFence(ctx context.Context, task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord, revision int64,
) (backupAssignmentClaim, error) {
	if task.Type != taskjournal.TaskBackup && task.Type != taskjournal.TaskBackupPrune &&
		task.Type != taskjournal.TaskRestore {
		return backupAssignmentClaim{}, nil
	}
	key := backupruntime.BackupExecutionPlanKey(task.ID)
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return backupAssignmentClaim{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return backupAssignmentClaim{}, errs.New(errs.KindInternal, "backup claim has no immutable procedure")
	}
	defer etcdstore.ClearValues(read.Values)
	plan, err := backupruntime.DecodeBackupExecutionPlan(read.Values[0].Value)
	if err != nil {
		return backupAssignmentClaim{}, err
	}
	if plan.PlanId != task.PlanID || hex.EncodeToString(plan.PlanHash) != task.PlanHash ||
		plan.TargetId != task.Target ||
		plan.BackupScope.ProjectId != task.Owner.ProjectID ||
		plan.BackupScope.EnvironmentId != task.Owner.EnvironmentID {
		return backupAssignmentClaim{}, errs.New(errs.KindInternal, "backup claim procedure does not match its Task")
	}
	if task.Type == taskjournal.TaskRestore {
		if len(plan.Steps) != 1 || plan.Steps[0].GetBackupStep().GetRestore().GetOriginalIdentity() == nil {
			return backupAssignmentClaim{}, errs.New(
				errs.KindStateConflict,
				"Restore claim requires its sealed original identity",
			)
		}
		original := plan.Steps[0].GetBackupStep().GetRestore().GetOriginalIdentity()
		if original.OriginalExecutionId != plan.Steps[0].GetBackupStep().ExecutionId {
			return backupAssignmentClaim{}, errs.New(errs.KindStateConflict, "Restore execution identity changed")
		}
		assignment.AssignmentID = original.OriginalAssignmentId
	}
	authority, digest, err := executionplan.BindBackupTaskAuthority(plan, executionplan.BackupAssignmentIdentity{
		TaskID: task.ID, OperationID: task.OperationID, AssignmentID: assignment.AssignmentID,
		Generation: 1, DeadlineUnixNano: uint64(assignment.Deadline.UnixNano()),
	})
	if err != nil {
		return backupAssignmentClaim{}, err
	}
	fence := &taskassignments.BackupAuthorityFence{
		AssignmentGeneration: authority.AssignmentGeneration,
		AuthoritySHA256:      hex.EncodeToString(digest),
	}
	for _, step := range authority.Steps {
		fence.Steps = append(fence.Steps, taskassignments.BackupStepAuthorityFence{
			StepID: step.StepId, ExecutionID: step.ExecutionId, AuthoritySHA256: hex.EncodeToString(step.StepDigest),
		})
	}
	claim := backupAssignmentClaim{
		assignmentID: assignment.AssignmentID,
		fence:        fence,
		conditions:   []etcdstore.Condition{{Key: key, ModRevision: read.Values[0].ModRevision}},
	}
	for _, step := range authority.Steps {
		pointID := backupStagingStepPoint(step)
		if pointID == "" {
			continue
		}
		index := backupruntime.BackupStagingIndexRecord{
			Schema:          1,
			TaskID:          task.ID,
			StepID:          step.StepId,
			PointID:         pointID,
			AgentID:         assignment.AgentID,
			AgentGeneration: assignment.AgentGeneration,
			PlanSHA256:      task.PlanHash,
		}
		value, err := backupruntime.EncodeBackupStagingIndex(index)
		if err != nil {
			etcdstore.ClearMutationValues(claim.mutations)
			return backupAssignmentClaim{}, err
		}
		recoveryKey, err := index.RecoveryKey()
		if err != nil {
			clear(value)
			etcdstore.ClearMutationValues(claim.mutations)
			return backupAssignmentClaim{}, err
		}
		storageKey := backupruntime.BackupStagingIndexKey(recoveryKey)
		claim.conditions = append(claim.conditions, etcdstore.Condition{Key: storageKey})
		claim.mutations = append(
			claim.mutations,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: storageKey, Value: value},
		)
	}
	conditions, mutations, err := repository.prepareBackupRunClaim(ctx, task, assignment.AssignedAt, revision)
	if err != nil {
		etcdstore.ClearMutationValues(claim.mutations)
		return backupAssignmentClaim{}, err
	}
	claim.conditions = append(claim.conditions, conditions...)
	claim.mutations = append(claim.mutations, mutations...)
	conditions, mutations, err = repository.prepareConfigRestoreClaim(ctx, task, plan, assignment, revision)
	if err != nil {
		etcdstore.ClearMutationValues(claim.mutations)
		return backupAssignmentClaim{}, err
	}
	claim.conditions = append(claim.conditions, conditions...)
	claim.mutations = append(claim.mutations, mutations...)
	return claim, nil
}

func backupStagingStepPoint(step *agentpb.BackupStepAuthority) string {
	if capture := step.GetCapture(); capture != nil {
		return capture.PointId
	}
	if restore := step.GetRestore(); restore != nil {
		return restore.PointId
	}
	return ""
}
