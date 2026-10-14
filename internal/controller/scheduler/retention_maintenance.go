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
	markerPass, cancelMarkers := context.WithTimeout(ctx, sch.maintenancePolicy.CleanupTimeBudget/3)
	markers, markerErr := pruneRetentionBatch(
		markerPass,
		now,
		sch.maintenancePolicy.CleanupBatchSize,
		sch.idempotency.PruneExpired,
	)
	cancelMarkers()
	// Task pruning independently checks marker ownership. A failed marker pass
	// cannot authorize its deletion, nor must it starve unrelated eligible Tasks.
	taskPass, cancelTasks := context.WithTimeout(ctx, sch.maintenancePolicy.CleanupTimeBudget/3)
	tasks, taskErr := pruneRetentionBatch(taskPass, now, sch.maintenancePolicy.CleanupBatchSize,
		func(ctx context.Context, now time.Time, _ int) (int, error) {
			return sch.tasks.PruneExpiredTasks(ctx, now)
		})
	cancelTasks()
	diagnosticPass, cancelDiagnostics := context.WithTimeout(ctx, sch.maintenancePolicy.CleanupTimeBudget/3)
	diagnostics, diagnosticErr := pruneRetentionBatch(diagnosticPass, now, sch.maintenancePolicy.CleanupBatchSize,
		func(ctx context.Context, now time.Time, remaining int) (int, error) {
			return sch.tasks.ExpireTaskDiagnostics(ctx, now, sch.maintenancePolicy.DiagnosticRetention, remaining)
		})
	cancelDiagnostics()
	if sch.logger != nil && (markers != 0 || tasks != 0 || markerErr != nil || taskErr != nil) {
		sch.logger.Info("scheduler: retained record cleanup",
			slog.Int("idempotency_records", markers), slog.Int("tasks", tasks),
			slog.Any("idempotency_error", markerErr), slog.Any("task_error", taskErr))
	}
	if sch.logger != nil && (diagnostics != 0 || diagnosticErr != nil) {
		sch.logger.Info(
			"scheduler: Task diagnostic cleanup",
			slog.Int("tasks", diagnostics),
			slog.Any("error", diagnosticErr),
		)
	}
	resultErr := errors.Join(markerErr, taskErr, diagnosticErr, ctx.Err())
	if sch.storageMaintenance != nil && ctx.Err() == nil {
		result := storagemaintenance.CleanupStatus{
			ObservedAt:         now,
			State:              "completed",
			IdempotencyRecords: markers,
			Tasks:              tasks,
			Diagnostics:        diagnostics,
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
