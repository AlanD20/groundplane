package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareBlueprintRecoveryRetry authorizes only restoration of the captured
// failed deployment. It does not grant another forward deployment or replay hooks.
// The caller joins this change with the operator request's replay record.
func (ledger *ReleaseLedger) PrepareBlueprintRecoveryRetry(ctx context.Context, view workqueue.View,
	previous workqueue.AttemptSnapshot, at time.Time,
) (workqueue.Change, error) {
	item := view.Item.Item
	if ledger == nil || ledger.store == nil || item.Operation != workexecution.Release ||
		previous.Attempt.Result == nil || previous.Attempt.FinishedAt == nil ||
		item.ActiveAttemptID != "" || item.Phase != workexecution.Attention ||
		(item.Cleanup != workexecution.CleanupRequired && item.Cleanup != workexecution.CleanupStopped) {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Release has no stopped captured cleanup to retry")
	}
	origin, err := releasework.LoadRecoverySource(ctx, ledger.store, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	sources, err := ledger.LoadBlueprintReleaseWorkInput(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(sources.Rollout.Record.CandidateArtifact)
	// Use the captured execution budget, not a deadline extended by a previous
	// retry or current platform settings. Every explicitly admitted cleanup is finite.
	budget := sources.Rollout.Record.Deadline.Sub(sources.Rollout.Record.CreatedAt)
	if budget <= 0 || budget > 24*time.Hour {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Release cleanup has no valid captured execution budget")
	}
	change, err := workqueue.PrepareRetry(view.Task, view.Item, previous, workexecution.RetryDecision{
		Direction: workexecution.Recovery, Result: *previous.Attempt.Result, NotBefore: at, Deadline: at.Add(budget),
	}, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, origin.Conditions...)
	change.Conditions = append(change.Conditions, sources.Conditions...)
	return change, nil
}
