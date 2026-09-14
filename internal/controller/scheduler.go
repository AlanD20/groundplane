package controller

import (
	"context"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type taskExpiration interface {
	ExpireTimedOutTasks(context.Context, time.Time) (int, error)
	PruneExpiredTasks(context.Context, time.Time) (int, error)
	ResumeTaskSourceReleases(context.Context) (bool, error)
}

type idempotencyPruning interface {
	PruneExpired(context.Context, time.Time) (int, error)
}

type staleAgentExpiration interface {
	ExpireStaleAgentTasks(context.Context, time.Time) (int, error)
}

// BackupScheduleRunner evaluates all due policy occurrences in one bounded
// scheduler pass. It owns durable coordination, due outcomes, and publication;
// policies never create independent timers or goroutines.
type BackupScheduleRunner interface {
	RunBackupSchedules(context.Context, time.Time) error
}

const dailyMaintenanceInterval = 24 * time.Hour

// Scheduler runs ONE tick that evaluates every backup schedule from
// desired state and dispatches due actions to the Agent — N policies
// cost nothing extra, no per-policy timer/unit churn. See mvp.md,
// "systemd timers (backup, cleanup) -> the Controller scheduler".
type Scheduler struct {
	Server          *Server
	Interval        time.Duration
	tasks           taskExpiration
	idempotency     idempotencyPruning
	agents          staleAgentExpiration
	backupSchedules BackupScheduleRunner
	now             func() time.Time
	nextPrune       time.Time
}

func NewScheduler(
	s *Server,
	interval time.Duration,
	tasks *etcd.TaskRepository,
	idempotency *etcd.IdempotencyRepository,
	agents staleAgentExpiration,
	backupSchedules ...BackupScheduleRunner,
) *Scheduler {
	var schedules BackupScheduleRunner
	if len(backupSchedules) > 0 {
		schedules = backupSchedules[0]
	}
	return &Scheduler{
		Server: s, Interval: interval, tasks: tasks, idempotency: idempotency, agents: agents,
		backupSchedules: schedules, now: time.Now,
	}
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
				sch.Server.Logger.Error("scheduler: tick failed", slog.Any("error", err))
			}
		}
	}
}

// tick first owns bounded runtime maintenance. BackupPolicy evaluation will
// join this same pass once desired-state storage is wired; it must not prevent
// overdue Task recovery from running today.
func (sch *Scheduler) tick(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if sch == nil || sch.tasks == nil || sch.idempotency == nil || sch.agents == nil || sch.now == nil {
		return errs.New(errs.KindInternal, "scheduler task maintenance is not configured")
	}
	if sch.Server != nil && sch.Server.mutationAdmission != nil {
		if err := sch.Server.mutationAdmission.CheckMutation(ctx, ""); err != nil {
			if kind, ok := errs.KindOf(err); ok && kind == errs.KindResourceInUse {
				return nil // An unfinished native operation deliberately holds this pass.
			}
			return err
		}
	}
	now := sch.now().UTC()
	if _, err := sch.tasks.ExpireTimedOutTasks(ctx, now); err != nil {
		return err
	}
	if _, err := sch.agents.ExpireStaleAgentTasks(ctx, now); err != nil {
		return err
	}
	if _, err := sch.tasks.ResumeTaskSourceReleases(ctx); err != nil {
		return err
	}
	if sch.backupSchedules != nil {
		if err := sch.backupSchedules.RunBackupSchedules(ctx, now); err != nil {
			return err
		}
	}
	if !sch.nextPrune.IsZero() && now.Before(sch.nextPrune) {
		return nil
	}
	if _, err := sch.idempotency.PruneExpired(ctx, now); err != nil {
		return err
	}
	if _, err := sch.tasks.PruneExpiredTasks(ctx, now); err != nil {
		return err
	}
	sch.nextPrune = now.Add(dailyMaintenanceInterval)
	return nil
}
