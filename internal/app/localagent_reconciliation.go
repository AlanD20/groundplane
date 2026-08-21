package app

import (
	"context"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type localAgentLifecycle interface {
	Reconcile(context.Context) error
}

// localAgentReconciliation is the native Controller's single reconciliation
// owner for its managed Agent container. It runs once at startup and then on
// the shared scheduler interval so Docker restarts and partial lifecycle
// operations converge from the durable record.
type localAgentReconciliation struct {
	lifecycle localAgentLifecycle
	interval  time.Duration
	logger    *slog.Logger
	after     func(time.Duration) <-chan time.Time
}

func newLocalAgentReconciliation(
	lifecycle localAgentLifecycle,
	interval time.Duration,
	logger *slog.Logger,
) (*localAgentReconciliation, error) {
	if lifecycle == nil {
		return nil, errs.New(errs.KindInternal, "local Agent lifecycle is required")
	}
	if interval <= 0 {
		return nil, errs.New(errs.KindInternal, "local Agent reconciliation interval must be positive")
	}
	if logger == nil {
		return nil, errs.New(errs.KindInternal, "local Agent reconciliation logger is required")
	}
	return &localAgentReconciliation{
		lifecycle: lifecycle,
		interval:  interval,
		logger:    logger,
		after:     time.After,
	}, nil
}

func (reconciliation *localAgentReconciliation) Run(ctx context.Context) {
	for {
		if err := reconciliation.lifecycle.Reconcile(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			reconciliation.logger.Error("local Agent reconciliation failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-reconciliation.after(reconciliation.interval):
		}
	}
}
