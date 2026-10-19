package scheduler

import (
	"context"
	"errors"
	"github.com/AlanD20/groundplane/internal/common/storagemaintenance"
	"github.com/AlanD20/groundplane/pkg/errs"
	"log/slog"
	"time"
)

// Each repository resumes its own fenced pruning intent. The pass bounds both
// completed items and elapsed work; a crash never turns cleanup into deletion
// without the repository's idempotency, execution and recovery guards.
func (sch *Scheduler) runRetentionMaintenance(ctx context.Context, now time.Time) error {
	if !sch.nextPrune.IsZero() && now.Before(sch.nextPrune) {
		return nil
	}
	sch.nextPrune = now.Add(sch.maintenancePolicy.CleanupInterval)
	markerPass, cancelMarkers := context.WithTimeout(ctx, sch.maintenancePolicy.CleanupTimeBudget)
	markers, markerErr := pruneRetentionBatch(
		markerPass,
		now,
		sch.maintenancePolicy.CleanupBatchSize,
		sch.idempotency.PruneExpired,
	)
	cancelMarkers()
	if sch.logger != nil && (markers != 0 || markerErr != nil) {
		sch.logger.Info(
			"scheduler: submission replay cleanup",
			slog.Int("idempotency_records", markers),
			slog.Any("error", markerErr),
		)
	}
	resultErr := errors.Join(markerErr, ctx.Err())
	if sch.storageMaintenance != nil && ctx.Err() == nil {
		result := storagemaintenance.CleanupStatus{
			ObservedAt:         now,
			State:              "completed",
			IdempotencyRecords: markers,
		}
		if resultErr != nil {
			result.State, result.FailureCode = "failed", "retention_cleanup_failed"
		}
		resultErr = errors.Join(resultErr, sch.storageMaintenance.RecordCleanup(ctx, result))
	}
	return resultErr
}

func pruneRetentionBatch(
	ctx context.Context, now time.Time, limit int, prune func(context.Context, time.Time, int) (int, error),
) (int, error) {
	total := 0
	for calls := 0; calls < limit && total < limit && ctx.Err() == nil; calls++ {
		count, err := prune(ctx, now, limit-total)
		if count < 0 || count > limit-total {
			return total, errs.New(errs.KindInternal, "cleanup operation exceeded its remaining record budget")
		}
		total += count
		if err != nil {
			if ctx.Err() != nil {
				return total, nil // The resumable batch used its scheduled time budget.
			}
			return total, err
		}
	}
	return total, nil
}
