package dnsresolver

import (
	"context"
	"log/slog"
	"net/netip"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ListenerReconciler waits for prior Agent work before reconciling the private
// address selected at Controller startup. The repository suppresses failed-task
// resubmission; this loop retries only reads and publication races.
type ListenerReconciler struct {
	tasks    *etcd.TaskRepository
	listener string
	logger   *slog.Logger
}

func NewListenerReconciler(
	tasks *etcd.TaskRepository,
	address netip.Addr,
	logger *slog.Logger,
) (*ListenerReconciler, error) {
	if tasks == nil || logger == nil || !address.Is4() ||
		!(address.IsPrivate() || address == netip.MustParseAddr("127.0.0.1")) {
		return nil, errs.New(errs.KindInternal, "resolver listener reconciliation dependencies are invalid")
	}
	listener := ""
	if address.IsPrivate() {
		listener = address.String()
	}
	return &ListenerReconciler{tasks: tasks, listener: listener, logger: logger}, nil
}

func (reconciler *ListenerReconciler) Run(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		operation, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := reconciler.tasks.ReconcileResolverListener(operation, reconciler.listener)
		cancel()
		if err != nil && ctx.Err() == nil {
			reconciler.logger.Warn("resolver listener reconciliation pending", slog.Any("error", err))
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
