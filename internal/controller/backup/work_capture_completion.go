package backup

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupretention"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Cleanup acknowledgement includes retention selection. Lost acknowledgements
// resume the same source; no later source may overtake this retained obligation.
func (owner *AgentWork) finishCaptureSource(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.BackupWorkCheckpointRequest,
) error {
	if request.GetSourceCleanupCompleted() == nil {
		return nil
	}
	attempt, err := workexecution.DecodeAgentAttempt(request.Attempt, authorization.Identity())
	if err != nil {
		return err
	}
	ctx, cancel := context.WithDeadline(ctx, attempt.Deadline)
	defer cancel()
	view, err := authorization.ReadResult(ctx, owner.queue, attempt.TaskID, attempt.WorkItemID, attempt.ID)
	if err != nil {
		return err
	}
	if view.Work.Item.Item.Operation != workexecution.Backup {
		return nil
	}
	assignment, _, err := owner.inputs.LoadAssignment(ctx, view)
	if err != nil {
		return err
	}
	ordinal := request.SourceOrdinal
	if uint64(ordinal) >= uint64(len(assignment.Sources)) {
		return errs.New(errs.KindStateConflict, "Backup cleanup source is unavailable")
	}
	run, guard, err := owner.inputs.LoadStagingCapture(ctx, backupwork.StagingSource{View: view.Work, Input: assignment.Input, Ordinal: ordinal})
	if err != nil {
		return err
	}
	if run.Sources[ordinal].State == backupruntime.BackupSourceAttemptSucceeded {
		return nil
	}
	source := run.Sources[ordinal]
	cleanup := assignment.Sources[ordinal].GetCapture().GetSourceCleanupCompleted()
	if cleanup == nil || cleanup.PointId != request.GetSourceCleanupCompleted().PointId ||
		source.State != backupruntime.BackupSourceAttemptPointCommitted || source.Phase != backupruntime.BackupSourcePhaseRetention {
		return errs.New(errs.KindStateConflict, "Backup retention requires acknowledged source cleanup")
	}
	sweep, found, err := owner.configRuntime.GetBackupRetentionSweep(ctx, source.SourceID, source.RecoveryPointID)
	if err != nil {
		return err
	}
	if !found || !backupretention.BackupRetentionSweepMatchesRun(run, sweep.Record) {
		return errs.New(errs.KindStateConflict, "Backup retention selection is unavailable")
	}
	for sweep.Record.State != backupruntime.BackupRetentionCompleted {
		previous := sweep.Record.Cursor
		at := time.Now().UTC()
		if !at.After(sweep.Record.UpdatedAt) {
			at = sweep.Record.UpdatedAt.Add(time.Nanosecond)
		}
		sweep, _, err = owner.configRuntime.AdvanceBackupRetentionSweep(ctx,
			keyvalue.Versioned[backupruntime.BackupRunRecord]{Record: run, Revision: guard.ModRevision, ReadRevision: view.Work.ReadRevision}, sweep, at)
		if err != nil {
			return err
		}
		if sweep.Record.State != backupruntime.BackupRetentionCompleted && sweep.Record.Cursor == previous {
			return errs.New(errs.KindInternal, "Backup retention selection did not advance")
		}
	}
	view, err = authorization.ReadResult(ctx, owner.queue, attempt.TaskID, attempt.WorkItemID, attempt.ID)
	if err != nil {
		return err
	}
	change, err := owner.inputs.PrepareCaptureSourceCompletion(ctx, view, ordinal, time.Now().UTC())
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	guards, err := authorization.Conditions()
	if err != nil {
		return err
	}
	result, err := owner.queue.Commit(ctx, workqueue.Change{Conditions: guards}, change)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Backup source changed during retention settlement; resend cleanup")
	}
	return nil
}
