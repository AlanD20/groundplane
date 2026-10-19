package backupwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *InputRepository) PrepareRecoveryRetry(ctx context.Context, view workqueue.View,
	previous workqueue.AttemptSnapshot, at time.Time,
) (workqueue.Change, error) {
	item, attempt := view.Item.Item, previous.Attempt
	if view.Task.Task.CollectionStartedAt != nil ||
		(item.Operation != workexecution.BackupPrune && item.Operation != workexecution.Backup && item.Operation != workexecution.Restore) ||
		item.Phase != workexecution.Attention || item.ActiveAttemptID != "" || item.Input == nil ||
		(item.Cleanup != workexecution.CleanupRequired && item.Cleanup != workexecution.CleanupStopped) ||
		attempt.FinishedAt == nil || attempt.Result == nil || attempt.Input != *item.Input ||
		attempt.Number != item.AttemptNumber {
		return workqueue.Change{}, errs.New(
			errs.KindTaskNotRetryable,
			"Backup Retry is limited to captured unresolved cleanup",
		)
	}
	_, guards, err := repository.PrepareAdmission(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	source, err := repository.LoadRecovery(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	if attempt.Direction == workexecution.Forward {
		if !workexecution.SameAssignment(attempt, source.Source.Forward) || *attempt.Result != source.Source.Evidence {
			return workqueue.Change{}, conflict("Backup cleanup Retry differs from its original failure")
		}
	} else {
		if err := source.Source.ValidateFor(attempt); err != nil {
			return workqueue.Change{}, err
		}
		if item.CleanupResult == nil || *item.CleanupResult != *attempt.Result {
			return workqueue.Change{}, conflict("Backup cleanup Retry differs from its last recovery result")
		}
	}
	read, err := repository.readSettlementKeys(ctx, []string{FailureKey(attempt.ID)}, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return workqueue.Change{}, conflict("Backup cleanup Retry is missing retained failure evidence")
	}
	if _, err := executionplan.DecodeBackupWorkFailure(attempt, read.Values[0].Value, *attempt.Result); err != nil {
		return workqueue.Change{}, err
	}
	// Cleanup gets a fresh bounded window, not a new forward operation. Keep
	// the captured operation's time budget; changing input requires new work.
	budget := source.Source.Forward.Deadline.Sub(source.Source.Forward.AssignedAt)
	if item.Operation == workexecution.BackupPrune {
		budget = time.Duration(executionplan.MaximumBackupPruneStepTimeoutSeconds) * time.Second
	}
	change, err := workqueue.PrepareRetry(view.Task, view.Item, previous, workexecution.RetryDecision{
		Direction: workexecution.Recovery, Result: *attempt.Result, NotBefore: at,
		Deadline: at.Add(budget),
	}, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, guards.Conditions...)
	change.Conditions = append(change.Conditions,
		keyvalue.Condition{Key: RecoveryKey(item.ID), ModRevision: source.Revision},
		keyvalue.Condition{Key: FailureKey(attempt.ID), ModRevision: read.Values[0].ModRevision})
	return change, nil
}
