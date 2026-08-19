package controller

import (
	"context"
	"log/slog"
	"time"
)

// Scheduler runs ONE tick that evaluates every backup schedule from
// desired state and dispatches due actions to the Agent — N policies
// cost nothing extra, no per-policy timer/unit churn. See mvp.md,
// "systemd timers (backup, cleanup) -> the Controller scheduler".
type Scheduler struct {
	Server   *Server
	Interval time.Duration
}

func NewScheduler(s *Server, interval time.Duration) *Scheduler {
	return &Scheduler{Server: s, Interval: interval}
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

// tick evaluates every environment's BackupPolicy.Frequency (a systemd
// calendar expression) and dispatches a backup-run task for anything
// due. TODO: read environments from etcd, compute next-run per policy,
// dispatch via the same task pipeline every other action uses (see
// server.go's acceptTask) — one well-tested path, not N ad-hoc timers.
func (sch *Scheduler) tick(ctx context.Context) error {
	return nil
}
