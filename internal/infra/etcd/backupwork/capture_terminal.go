package backupwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/postgresbackingguard"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareCaptureCompletion joins resource terminal state and exclusion release
// to the caller's native Task completion. Inputs and receipts remain for replay.
func (repository *InputRepository) PrepareCaptureCompletion(ctx context.Context,
	view workqueue.ResumeView, at time.Time,
) (workqueue.Change, error) {
	assignment, change, err := repository.LoadAssignment(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	if assignment.Input.Operation != agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_CAPTURE ||
		view.Attempt.Attempt.StartedAt == nil || len(assignment.Sources) == 0 {
		return workqueue.Change{}, conflict("Backup completion requires a started capture")
	}
	run, guard, err := repository.LoadStagingCapture(ctx, StagingSource{View: view.Work, Input: assignment.Input})
	if err != nil {
		return workqueue.Change{}, err
	}
	if run.State != backupruntime.BackupRunRunning || !at.After(run.UpdatedAt) {
		return workqueue.Change{}, conflict("Backup completion requires its current running resource record")
	}
	for index, source := range run.Sources {
		cleanup := assignment.Sources[index].GetCapture().GetSourceCleanupCompleted()
		if source.State != backupruntime.BackupSourceAttemptSucceeded || cleanup == nil || cleanup.PointId != source.RecoveryPointID ||
			!backupruntime.BackupArtifactEvidenceMatchesWire(source.Evidence, cleanup.Evidence) {
			return workqueue.Change{}, conflict("Backup completion requires every source's cleanup and retention")
		}
	}
	next := backupruntime.CloneBackupRunPublicationRecord(run)
	next.State, next.UpdatedAt = backupruntime.BackupRunCompleted, at
	if !backupruntime.BackupRunReadyForSuccessfulTerminal(run, next) {
		return workqueue.Change{}, conflict("Backup sources are not settled")
	}
	if err := backupruntime.ValidateBackupRunTransition(run, next, backupruntime.BackupRunTransitionOrdinary); err != nil {
		return workqueue.Change{}, err
	}
	return repository.prepareCaptureRelease(ctx, view.Work, run, next, guard, change)
}

func (repository *InputRepository) prepareCaptureRelease(ctx context.Context, view workqueue.View,
	run, next backupruntime.BackupRunRecord, guard keyvalue.Condition, change workqueue.Change,
) (workqueue.Change, error) {
	exclusions, err := repository.captureExclusionGuards(ctx, run, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	fence, err := environmentfence.LoadBackupRunOwned(ctx, repository.store, run, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	environments, err := backupruntime.DatabaseBackingEnvironmentIDs(run)
	if err != nil {
		return workqueue.Change{}, err
	}
	backings, err := postgresbackingguard.PrepareOwnership(ctx, repository.store, environments, run.EnvironmentID,
		postgresbackingguard.Owner(backupruntime.BackupOperationBackup, run.OperationID, run.TaskID), view.ReadRevision, true)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer backings.Clear()
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return workqueue.Change{}, err
	}
	if !backupruntime.BackupRunRecordsEqual(run, next) {
		encoded, err := backupruntime.EncodeBackupRunRecord(next)
		if err != nil {
			return workqueue.Change{}, err
		}
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: guard.Key, Value: encoded})
	}
	change.Conditions = append(change.Conditions, guard)
	change.Conditions = append(change.Conditions, exclusions...)
	change.Conditions = append(change.Conditions, fence.TransactionConditions()...)
	change.Conditions = append(change.Conditions, backings.Conditions...)
	change.Mutations = append(change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: hierarchy.EnvironmentOperationLockKey(run.EnvironmentID)}, epoch)
	for _, exclusion := range exclusions {
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: exclusion.Key})
	}
	for index := range run.Sources {
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: credentialGrantKey(view.Item.Item.ID, uint32(index))})
	}
	for _, mutation := range backings.Mutations {
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: mutation.Type, Key: mutation.Key, Value: append([]byte(nil), mutation.Value...)})
	}
	return change, nil
}
