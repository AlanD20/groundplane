package backupwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/postgresbackingguard"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareCaptureFailure records fail-fast progress and uncertain uploads while
// retaining source, Environment and backing ownership until cleanup proves safe.
func (repository *InputRepository) PrepareCaptureFailure(ctx context.Context, view workqueue.ResumeView,
	outcome workexecution.Outcome, reason executionplan.BackupFailureReason, at time.Time,
) (workqueue.Change, error) {
	assignment, change, err := repository.LoadAssignment(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	if assignment.Input.Operation != agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_CAPTURE ||
		view.Attempt.Attempt.StartedAt == nil {
		return workqueue.Change{}, conflict("Backup failure requires a started capture")
	}
	run, guard, err := repository.LoadStagingCapture(ctx, StagingSource{View: view.Work, Input: assignment.Input})
	if err != nil {
		return workqueue.Change{}, err
	}
	if run.State != backupruntime.BackupRunRunning || !at.After(run.UpdatedAt) {
		return workqueue.Change{}, conflict("Backup failure requires its running resource record")
	}
	next := run
	allSucceeded := true
	for _, source := range run.Sources {
		allSucceeded = allSucceeded && source.State == backupruntime.BackupSourceAttemptSucceeded
	}
	if !allSucceeded {
		next, err = failedCaptureRun(run, outcome, reason, at)
		if err != nil {
			return workqueue.Change{}, err
		}
	}
	exclusions, err := repository.captureExclusionGuards(ctx, run, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	fence, err := environmentfence.LoadBackupRunOwned(ctx, repository.store, run, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	environments, err := backupruntime.DatabaseBackingEnvironmentIDs(run)
	if err != nil {
		return workqueue.Change{}, err
	}
	backings, err := postgresbackingguard.PrepareOwnership(
		ctx,
		repository.store,
		environments,
		run.EnvironmentID,
		postgresbackingguard.Owner(
			backupruntime.BackupOperationBackup,
			run.OperationID,
			run.TaskID,
		),
		view.Work.ReadRevision,
		false,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer backings.Clear()
	orphan, err := repository.captureFailureOrphan(ctx, run, next, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	encoded, err := backupruntime.EncodeBackupRunRecord(next)
	if err != nil {
		keyvalue.ClearMutationValues(orphan.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, guard)
	change.Conditions = append(change.Conditions, exclusions...)
	change.Conditions = append(change.Conditions, fence.TransactionConditions()...)
	change.Conditions = append(change.Conditions, backings.Conditions...)
	change.Conditions = append(change.Conditions, orphan.Conditions...)
	if !allSucceeded {
		change.Mutations = append(
			change.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: guard.Key, Value: encoded},
		)
	} else {
		// Preserve completed source receipts. Recovery will settle the resource
		// after helper exclusion; the separate Task failure is still recorded.
		clear(encoded)
	}
	change.Mutations = append(change.Mutations, orphan.Mutations...)
	for _, mutation := range backings.Mutations {
		change.Mutations = append(
			change.Mutations,
			keyvalue.Mutation{Type: mutation.Type, Key: mutation.Key, Value: append([]byte(nil), mutation.Value...)},
		)
	}
	return change, nil
}
