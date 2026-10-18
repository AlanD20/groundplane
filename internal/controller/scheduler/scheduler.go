package scheduler

import (
	"context"
	"errors"
	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/storagemaintenance"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type taskExpiration interface {
	PruneExpiredTasks(context.Context, time.Time) (int, error)
	ExpireTaskDiagnostics(context.Context, time.Time, time.Duration, int) (int, error)
	ResumeTaskSourceReleases(context.Context) (bool, error)
}

type idempotencyPruning interface {
	PruneExpired(context.Context, time.Time, int) (int, error)
}

type storageMaintenance interface {
	Policy() config.MaintenancePolicy
	Tick(context.Context, time.Time) error
	RecordCleanup(context.Context, storagemaintenance.CleanupStatus) error
}

type blueprintRevisionCleanup interface {
	CleanupEnvironmentBlueprintStaging(context.Context, time.Time, int) (int, error)
}

// BackupScheduleRunner evaluates all due policy occurrences in one bounded
// scheduler pass. It owns durable coordination, due outcomes, and publication;
// policies never create independent timers or goroutines.
type BackupScheduleRunner interface {
	RunBackupSchedules(context.Context, time.Time) error
}

type backupRetentionDispatcher interface {
	Tick(context.Context, string) (string, bool, error)
}

type backupOrphanReconciler interface {
	Tick(context.Context, string) (string, bool, error)
}

type mutationAdmission interface {
	CheckMutation(context.Context, string) error
}

// Scheduler runs ONE tick that evaluates every backup schedule from
// desired state and dispatches due actions to the Agent — N policies
// cost nothing extra, no per-policy timer/unit churn. See mvp.md,
// "systemd timers (backup, cleanup) -> the Controller scheduler".
type Scheduler struct {
	logger                 *slog.Logger
	admission              mutationAdmission
	Interval               time.Duration
	tasks                  taskExpiration
	idempotency            idempotencyPruning
	blueprintRevisions     blueprintRevisionCleanup
	backupSchedules        BackupScheduleRunner
	backupRetention        backupRetentionDispatcher
	retentionCursor        string
	backupOrphans          backupOrphanReconciler
	orphanCursor           string
	now                    func() time.Time
	nextPrune              time.Time
	maintenancePolicy      config.MaintenancePolicy
	storageMaintenance     storageMaintenance
	nextStorageMaintenance time.Time
}

func New(
	logger *slog.Logger,
	admission mutationAdmission,
	interval time.Duration,
	tasks *etcd.TaskRepository,
	idempotency idempotencyPruning,
	blueprintRevisions blueprintRevisionCleanup,
	backupSchedules BackupScheduleRunner,
	backupRetention backupRetentionDispatcher,
	backupOrphans backupOrphanReconciler,
) *Scheduler {
	return &Scheduler{
		logger: logger, admission: admission, Interval: interval, tasks: tasks, idempotency: idempotency,
		blueprintRevisions: blueprintRevisions,
		backupSchedules:    backupSchedules, backupRetention: backupRetention, backupOrphans: backupOrphans, now: time.Now,
		maintenancePolicy: config.DefaultMaintenancePolicy(),
	}
}

// ConfigureMaintenance receives the already-validated startup policy. Runtime
// changes follow the Controller's existing configuration/restart workflow.
func (sch *Scheduler) ConfigureMaintenance(storage storageMaintenance) {
	sch.maintenancePolicy = storage.Policy()
	sch.storageMaintenance = storage
}

// Run blocks, ticking at Interval until ctx is cancelled.
func (sch *Scheduler) Run(ctx context.Context) {
	t := time.NewTicker(sch.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := sch.tick(ctx); err != nil {
				sch.logger.Error("scheduler: tick failed", slog.Any("error", err))
			}
		}
	}
}

// tick handles overdue execution before dispatching scheduled captures or
// retained-object cleanup. Each retention pass advances a bounded cursor.
func (sch *Scheduler) tick(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if sch == nil || sch.tasks == nil || sch.idempotency == nil || sch.blueprintRevisions == nil ||
		sch.now == nil {
		return errs.New(errs.KindInternal, "scheduler task maintenance is not configured")
	}
	if sch.admission != nil {
		if err := sch.admission.CheckMutation(ctx, ""); err != nil {
			if kind, ok := errs.KindOf(err); ok && kind == errs.KindResourceInUse {
				return nil // An unfinished native operation deliberately holds this pass.
			}
			return err
		}
	}
	now := sch.now().UTC()
	maintenanceErr := sch.runRetentionMaintenance(ctx, now)
	if sch.storageMaintenance != nil && !now.Before(sch.nextStorageMaintenance) {
		sch.nextStorageMaintenance = now.Add(sch.maintenancePolicy.CleanupInterval)
		if err := sch.storageMaintenance.Tick(ctx, now); err != nil {
			maintenanceErr = errors.Join(maintenanceErr, err)
		}
	}
	if _, err := sch.tasks.ResumeTaskSourceReleases(ctx); err != nil {
		return err
	}
	if _, err := sch.blueprintRevisions.CleanupEnvironmentBlueprintStaging(ctx, now, 16); err != nil {
		return err
	}
	if sch.backupSchedules != nil {
		if err := sch.backupSchedules.RunBackupSchedules(ctx, now); err != nil {
			return err
		}
	}
	if sch.backupRetention != nil {
		next, _, err := sch.backupRetention.Tick(ctx, sch.retentionCursor)
		if err != nil {
			return err
		}
		sch.retentionCursor = next
	}
	if sch.backupOrphans != nil {
		next, _, err := sch.backupOrphans.Tick(ctx, sch.orphanCursor)
		sch.orphanCursor = next
		if err != nil {
			// A failing remote target must not indefinitely starve other
			// orphans or independent local retention maintenance.
			sch.logger.Error("scheduler: backup orphan reconciliation failed", slog.Any("error", err))
		}
	}
	return maintenanceErr
}
