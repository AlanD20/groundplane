package dnsresolver

import (
	"context"
	"log/slog"
	"net/netip"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ConfigurationReconciler waits for prior Agent work before reconciling the
// private listener and Environment DNS bindings. The repository suppresses failed-task
// resubmission; this loop retries only reads and publication races.
type ConfigurationReconciler struct {
	projections PlatformProjectionReader
	zones       *ZoneListenerSource
	tasks       *etcd.TaskRepository
	listener    string
	logger      *slog.Logger
}

func NewConfigurationReconciler(
	tasks *etcd.TaskRepository,
	projections PlatformProjectionReader,
	zones *ZoneListenerSource,
	address netip.Addr,
	logger *slog.Logger,
) (*ConfigurationReconciler, error) {
	if tasks == nil || projections == nil || zones == nil || logger == nil || !address.Is4() ||
		!(address.IsPrivate() || address == netip.MustParseAddr("127.0.0.1")) {
		return nil, errs.New(errs.KindInternal, "resolver configuration reconciliation dependencies are invalid")
	}
	listener := ""
	if address.IsPrivate() {
		listener = address.String()
	}
	return &ConfigurationReconciler{
		tasks:       tasks,
		projections: projections,
		zones:       zones,
		listener:    listener,
		logger:      logger,
	}, nil
}

func (reconciler *ConfigurationReconciler) Run(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		operation, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := reconciler.reconcile(operation)
		cancel()
		if err != nil && ctx.Err() == nil {
			reconciler.logger.Warn("resolver configuration reconciliation pending", slog.Any("error", err))
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (reconciler *ConfigurationReconciler) reconcile(ctx context.Context) error {
	projection, found, err := reconciler.projections.GetHostResolutionProjection(ctx)
	if err != nil || !found {
		return err
	}
	selection, err := reconciler.zones.Resolve(ctx, projection.ReadRevision)
	if err != nil {
		return err
	}
	return reconciler.tasks.ReconcileResolverConfiguration(ctx, reconciler.listener, selection)
}
