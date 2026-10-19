package backupwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupretention"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Complete a source only after its exact cleanup receipt, published Point and
// completed retention selection agree at one snapshot. Retry does not reupload.
func (repository *InputRepository) PrepareCaptureSourceCompletion(ctx context.Context,
	view workqueue.ResumeView, ordinal uint32, at time.Time,
) (workqueue.Change, error) {
	assignment, change, err := repository.LoadAssignment(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	if assignment.Input.Operation != agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_CAPTURE ||
		uint64(ordinal) >= uint64(len(assignment.Sources)) || view.Attempt.Attempt.StartedAt == nil {
		return workqueue.Change{}, conflict("Backup source completion requires its started capture")
	}
	run, guard, err := repository.LoadStagingCapture(ctx, StagingSource{
		View: view.Work, Input: assignment.Input, Ordinal: ordinal,
	})
	if err != nil {
		return workqueue.Change{}, err
	}
	cleanup := assignment.Sources[ordinal].GetCapture().GetSourceCleanupCompleted()
	source := run.Sources[ordinal]
	if cleanup == nil || cleanup.PointId != source.RecoveryPointID ||
		!backupruntime.BackupArtifactEvidenceMatchesWire(source.Evidence, cleanup.Evidence) ||
		run.State != backupruntime.BackupRunRunning {
		return workqueue.Change{}, conflict("Backup source completion lacks acknowledged cleanup")
	}
	change.Conditions = append(change.Conditions, guard)
	if source.State == backupruntime.BackupSourceAttemptSucceeded {
		return change, nil
	}
	if source.State != backupruntime.BackupSourceAttemptPointCommitted ||
		source.Phase != backupruntime.BackupSourcePhaseRetention {
		return workqueue.Change{}, conflict("Backup source has not reached retention")
	}
	keys := []string{
		backupruntime.BackupRetentionKey(source.SourceID, source.RecoveryPointID),
		backupruntime.BackupRecoveryPointKey(source.RecoveryPointID),
	}
	read, err := repository.readSettlementKeys(ctx, keys, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil || read.Values[1] == nil {
		return workqueue.Change{}, conflict("Backup source retention or Point is missing")
	}
	sweep, err := backupruntime.DecodeBackupRetentionSweepRecord(read.Values[0].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	if sweep.State != backupruntime.BackupRetentionCompleted ||
		!backupretention.BackupRetentionSweepMatchesRun(run, sweep) {
		return workqueue.Change{}, conflict("Backup source retention is incomplete")
	}
	point, err := backupruntime.DecodeBackupRecoveryPointRecord(read.Values[1].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	if point.ID != source.RecoveryPointID || point.EnvironmentID != run.EnvironmentID ||
		point.SourceID != source.SourceID ||
		read.Values[1].ModRevision != assignment.Sources[ordinal].GetCapture().
			GetPrecedingCheckpoint().
			GetDedupeKeyModRevision() {
		return workqueue.Change{}, conflict("Backup source Point differs from its cleanup publication")
	}
	next := backupruntime.CloneBackupRunPublicationRecord(run)
	next.Sources[ordinal].State, next.Sources[ordinal].Phase = backupruntime.BackupSourceAttemptSucceeded, backupruntime.BackupSourcePhaseCleanup
	next.UpdatedAt = at
	if err := backupruntime.ValidateBackupRunTransition(run, next, backupruntime.BackupRunTransitionCaptureComplete); err != nil {
		return workqueue.Change{}, err
	}
	encoded, err := backupruntime.EncodeBackupRunRecord(next)
	if err != nil {
		return workqueue.Change{}, err
	}
	for index, key := range keys {
		change.Conditions = append(
			change.Conditions,
			keyvalue.Condition{Key: key, ModRevision: read.Values[index].ModRevision},
		)
	}
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: guard.Key, Value: encoded},
	)
	return change, nil
}
