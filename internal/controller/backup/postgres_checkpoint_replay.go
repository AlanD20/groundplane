package backup

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (service *BackupCheckpointService) validateDatabaseCheckpointAdvance(ctx context.Context,
	claim etcdstore.Versioned[taskassignments.TaskAssignmentRecord],
	plan *agentpb.ExecutionPlan, operationID string, readRevision int64,
	request *agentpb.BackupCheckpointRequest,
) error {
	assignment := claim.Record
	authority, digest, err := executionplan.BindBackupTaskAuthority(plan, executionplan.BackupAssignmentIdentity{
		TaskID: request.TaskId, OperationID: operationID, AssignmentID: request.AssignmentId,
		Generation:       assignment.BackupAuthorityFence.AssignmentGeneration,
		DeadlineUnixNano: uint64(assignment.Deadline.UnixNano()),
	})
	if err != nil {
		return err
	}
	if hex.EncodeToString(digest) != assignment.BackupAuthorityFence.AuthoritySHA256 || readRevision <= 0 {
		return errs.New(errs.KindStateConflict, "database checkpoint assignment changed")
	}
	var step *agentpb.BackupStepAuthority
	for _, candidate := range authority.Steps {
		if candidate.StepId == request.StepId && candidate.ExecutionId == request.ExecutionId {
			step = candidate
			break
		}
	}
	if step == nil || step.GetCapture().GetPostgres() == nil && step.GetCapture().GetMysql() == nil &&
		step.GetRestore().GetPostgres() == nil && step.GetRestore().GetMysql() == nil {
		return errs.New(errs.KindValidationFailed, "database checkpoint source binding is invalid")
	}
	replay, err := executionplan.NewBackupStepResumeReplay(authority, step, nil)
	if err != nil {
		return err
	}
	err = service.repository.VisitBackupStepCheckpoints(ctx, authority, step, readRevision,
		func(checkpoint backupruntime.BackupCommittedCheckpoint) error {
			return replay.Accept(
				executionplan.BackupCheckpointReplayEntry{Request: checkpoint.Request, Fence: checkpoint.Fence},
			)
		})
	if err != nil {
		return err
	}
	// This virtual fence checks only the proposed next transition. The atomic
	// Run/cursor CAS below supplies the real receipt; this value is never stored
	// or acknowledged and cannot authorize Agent execution.
	err = replay.Accept(executionplan.BackupCheckpointReplayEntry{Request: request,
		Fence: &agentpb.CheckpointFence{AuthorityDigest: step.StepDigest, DedupeKeyModRevision: readRevision + 1}})
	if err != nil {
		return err
	}
	_, err = replay.Resume()
	return err
}
