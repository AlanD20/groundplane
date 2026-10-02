package etcd

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupretention"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Capture has no consumer recovery mutations. Its successful worker report may
// settle only sources with native cleanup receipts and completed retention;
// Task and Run terminal publication still share their existing transaction.
func (repository *BackupRuntimeRepository) finalizeBackupCaptures(ctx context.Context, claim TaskAssignment,
	run etcdstore.Versioned[backupruntime.BackupRunRecord],
) (etcdstore.Versioned[backupruntime.BackupRunRecord], error) {
	assignment := claim.Assignment.Record
	if assignment.BackupAuthorityFence == nil {
		return run, errs.New(errs.KindStateConflict, "Backup completion has no assignment authority")
	}
	ctx, cancel := context.WithDeadline(ctx, assignment.Deadline)
	defer cancel()
	plan, err := repository.GetBackupExecutionPlan(ctx, run.Record.TaskID)
	if err != nil {
		return run, err
	}
	authority, digest, err := executionplan.BindBackupTaskAuthority(plan.Record, executionplan.BackupAssignmentIdentity{
		TaskID: claim.Task.Record.ID, OperationID: claim.Task.Record.OperationID, AssignmentID: assignment.AssignmentID,
		Generation: assignment.BackupAuthorityFence.AssignmentGeneration, DeadlineUnixNano: uint64(assignment.Deadline.UnixNano()),
	})
	if err != nil {
		return run, err
	}
	if hex.EncodeToString(digest) != assignment.BackupAuthorityFence.AuthoritySHA256 ||
		len(authority.Steps) != len(run.Record.Sources) {
		return run, errs.New(errs.KindStateConflict, "Backup completion differs from its sealed assignment")
	}
	for ordinal, source := range run.Record.Sources {
		if source.State == backupruntime.BackupSourceAttemptSucceeded {
			continue
		}
		if source.State != backupruntime.BackupSourceAttemptPointCommitted {
			// A run advances one source at a time. Completion of this prefix
			// must precede the cleanup ACK that permits the next Agent step.
			break
		}
		step := authority.Steps[ordinal]
		if step.GetCapture() == nil || step.GetCapture().PointId != source.RecoveryPointID ||
			source.State != backupruntime.BackupSourceAttemptPointCommitted || source.Phase != backupruntime.BackupSourcePhaseRetention {
			return run, errs.New(errs.KindStateConflict, "Backup capture has not committed source cleanup")
		}
		var cleanup backupruntime.BackupCommittedCheckpoint
		if err := repository.VisitBackupStepCheckpoints(ctx, authority, step, run.ReadRevision,
			func(committed backupruntime.BackupCommittedCheckpoint) error {
				cleanup = committed
				return nil
			}); err != nil {
			return run, err
		}
		if cleanup.Request.GetSourceCleanupCompleted() == nil ||
			cleanup.Request.GetSourceCleanupCompleted().PointId != source.RecoveryPointID ||
			!backupruntime.BackupArtifactEvidenceMatchesWire(
				source.Evidence,
				cleanup.Request.GetSourceCleanupCompleted().Evidence,
			) {
			return run, errs.New(errs.KindStateConflict, "Backup capture lacks its exact native cleanup receipt")
		}
		sweep, found, err := repository.GetBackupRetentionSweep(ctx, source.SourceID, source.RecoveryPointID)
		if err != nil {
			return run, err
		}
		if !found || !backupretention.BackupRetentionSweepMatchesRun(run.Record, sweep.Record) {
			return run, errs.New(errs.KindStateConflict, "Backup capture retention authority is unavailable")
		}
		for sweep.Record.State != backupruntime.BackupRetentionCompleted {
			previous := sweep.Record.Cursor
			sweep, _, err = repository.AdvanceBackupRetentionSweep(ctx, run, sweep,
				backupTerminalTimestamp(time.Now().UTC(), sweep.Record.UpdatedAt))
			if err != nil {
				return run, err
			}
			if sweep.Record.State != backupruntime.BackupRetentionCompleted && sweep.Record.Cursor == previous {
				return run, errs.New(errs.KindInternal, "Backup retention scan did not advance")
			}
		}
		run, err = repository.completeBackupCapture(
			ctx,
			claim,
			run,
			plan.Revision,
			uint32(ordinal),
			step,
			cleanup,
			sweep,
		)
		if err != nil {
			return run, err
		}
	}
	return run, nil
}

// FinalizeBackupCapture completes retention before acknowledging source
// cleanup. Replaying a lost cleanup ACK finishes this same prefix; it never
// uploads the source again or allows a later source to overtake retention.
func (repository *BackupRuntimeRepository) FinalizeBackupCapture(ctx context.Context,
	input backupruntime.BackupCheckpointInput,
) error {
	if input.Request.GetSourceCleanupCompleted() == nil {
		return errs.New(errs.KindValidationFailed, "Backup source completion requires its cleanup checkpoint")
	}
	if _, replay, err := repository.ReplayBackupCheckpoint(ctx, input); err != nil {
		return err
	} else if !replay {
		return errs.New(errs.KindStateConflict, "Backup source cleanup is not committed")
	}
	tasks, err := newTaskRepository(repository.store)
	if err != nil {
		return err
	}
	claim, assigned, err := tasks.loadBackupTaskAssignment(ctx,
		input.AgentID, input.AgentGeneration, input.TaskID, input.AssignmentID)
	if err != nil {
		return err
	}
	if !assigned || claim.Task.Record.Type != taskjournal.TaskBackup {
		return errs.New(errs.KindStateConflict, "Backup source completion assignment changed")
	}
	run, err := repository.GetBackupRun(ctx, input.TaskID)
	if err != nil {
		return err
	}
	if err := ValidateBackupRunTaskBinding(claim.Task.Record, run.Record); err != nil {
		return err
	}
	run, err = repository.finalizeBackupCaptures(ctx, claim, run)
	if err != nil {
		return err
	}
	for _, source := range run.Record.Sources {
		if source.RecoveryPointID == input.Request.GetSourceCleanupCompleted().PointId &&
			source.State == backupruntime.BackupSourceAttemptSucceeded {
			return nil
		}
	}
	return errs.New(errs.KindStateConflict, "Backup source retention is incomplete")
}

func (repository *BackupRuntimeRepository) completeBackupCapture(ctx context.Context, claim TaskAssignment,
	run etcdstore.Versioned[backupruntime.BackupRunRecord], planRevision int64, ordinal uint32,
	step *agentpb.BackupStepAuthority, cleanup backupruntime.BackupCommittedCheckpoint,
	sweep etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord],
) (etcdstore.Versioned[backupruntime.BackupRunRecord], error) {
	next := backupruntime.CloneBackupRunPublicationRecord(run.Record)
	next.Sources[ordinal].State, next.Sources[ordinal].Phase = backupruntime.BackupSourceAttemptSucceeded, backupruntime.BackupSourcePhaseCleanup
	next.UpdatedAt = backupTerminalTimestamp(time.Now().UTC(), run.Record.UpdatedAt)
	if err := backupruntime.ValidateBackupRunTransition(run.Record, next, backupruntime.BackupRunTransitionCaptureComplete); err != nil {
		return run, err
	}
	assignment := claim.Assignment.Record
	input := backupruntime.BackupCheckpointInput{TaskID: run.Record.TaskID, AssignmentID: assignment.AssignmentID,
		StepID: step.StepId, ExecutionID: step.ExecutionId, Sequence: cleanup.Request.CheckpointSequence}
	conditions := []etcdstore.Condition{
		{Key: backupruntime.BackupExecutionPlanKey(run.Record.TaskID), ModRevision: planRevision},
		{Key: backupruntime.BackupCheckpointDedupKey(input), ModRevision: cleanup.Fence.DedupeKeyModRevision},
		{Key: backupruntime.BackupCheckpointCursorKey(input), ModRevision: cleanup.Fence.DedupeKeyModRevision},
		{
			Key:         backupruntime.BackupRetentionKey(sweep.Record.SourceID, sweep.Record.TriggerRecoveryPointID),
			ModRevision: sweep.Revision,
		},
		{
			Key:         backupruntime.BackupRecoveryPointKey(next.Sources[ordinal].RecoveryPointID),
			ModRevision: cleanup.Fence.DedupeKeyModRevision,
		},
	}
	owner := backupruntime.BackupAssignmentInput{TaskID: run.Record.TaskID, AssignmentID: assignment.AssignmentID,
		AgentID: assignment.AgentID, AgentGeneration: assignment.AgentGeneration, StepID: step.StepId}
	return repository.replaceBackupRun(ctx, run, next, conditions, nil, nil, &owner, nil)
}
