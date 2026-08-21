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
}

// Scheduler runs ONE tick that evaluates every backup schedule from
// desired state and dispatches due actions to the Agent — N policies
// cost nothing extra, no per-policy timer/unit churn. See mvp.md,
// "systemd timers (backup, cleanup) -> the Controller scheduler".
type Scheduler struct {
	Server   *Server
	Interval time.Duration
	tasks    taskExpiration
	now      func() time.Time
}

func NewScheduler(s *Server, interval time.Duration, tasks *etcd.TaskRepository) *Scheduler {
	return &Scheduler{Server: s, Interval: interval, tasks: tasks, now: time.Now}
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
	if sch == nil || sch.tasks == nil || sch.now == nil {
		return errs.New(errs.KindInternal, "scheduler task maintenance is not configured")
	}
	_, err := sch.tasks.ExpireTimedOutTasks(ctx, sch.now().UTC())
	return err
}
