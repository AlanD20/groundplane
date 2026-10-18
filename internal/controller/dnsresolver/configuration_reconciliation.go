package dnsresolver

import (
	"context"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ConfigurationReconciler publishes captured native work from durable resource
// intent. It never replays a failed target automatically or invents an Agent
// before the enrolled host has reported its platform.
type ConfigurationReconciler struct {
	tasks       *etcd.TaskRepository
	components  *etcd.ComponentRepository
	planner     *PlatformRenderPlanner
	coordinator *requestidempotency.Coordinator
	logger      *slog.Logger
}

func NewConfigurationReconciler(
	tasks *etcd.TaskRepository,
	components *etcd.ComponentRepository,
	planner *PlatformRenderPlanner,
	coordinator *requestidempotency.Coordinator,
	logger *slog.Logger,
) (*ConfigurationReconciler, error) {
	if tasks == nil || components == nil || planner == nil || coordinator == nil || logger == nil {
		return nil, errs.New(errs.KindInternal, "resolver configuration reconciliation dependencies are invalid")
	}
	return &ConfigurationReconciler{
		tasks: tasks, components: components, planner: planner, coordinator: coordinator, logger: logger,
	}, nil
}

func (reconciler *ConfigurationReconciler) Run(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		operation, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := reconciler.reconcile(operation)
		cleanupErr := reconciler.tasks.ReconcileResolverDNSMetadata(operation)
		if cleanupErr != nil && ctx.Err() == nil {
			reconciler.logger.Warn("resolver metadata cleanup pending", slog.Any("error", cleanupErr))
		}
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
	page, err := reconciler.components.ListPlatformComponents(ctx, keyvalue.PageRequest{Limit: keyvalue.MaximumPageLimit})
	if err != nil {
		return err
	}
	current, err := reconciler.planner.SelectResolver(ctx, page.Items)
	if err != nil {
		return err
	}
	if current.Revision <= 0 {
		return errs.New(errs.KindComponentNotFound, "platform CoreDNS is not initialized")
	}
	if !current.Record.Desired.Enabled {
		return nil
	}
	desired, err := componentrecord.ProjectRecord(current.Record)
	if err != nil {
		return err
	}
	prepared, err := reconciler.planner.PrepareHostWork(ctx, reconciler.tasks, current, desired,
		workexecution.SystemActor, workexecution.ActionUpdate, false, time.Now().UTC(), 8*time.Minute)
	if err != nil {
		return err
	}
	records, err := reconciler.tasks.ResolverWorkRepository()
	if err != nil {
		return err
	}
	needed, err := records.NeedsAutomaticWork(ctx, prepared.Input, prepared.Capture)
	if err != nil || !needed {
		return err
	}
	marker, err := prepareNativeResolverMarker(ctx, reconciler.coordinator, prepared)
	if err != nil {
		return err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	result, err := reconciler.tasks.PublishResolverWork(ctx, prepared.Input, prepared.Capture, marker)
	if err != nil {
		return err
	}
	outcome, replay, conflict, err := result.Classify()
	defer clear(replay.Intent.Ciphertext)
	defer clear(replay.Response.Body)
	if err != nil {
		return err
	}
	if conflict != nil {
		return conflict
	}
	if outcome != etcd.IdempotencyKnownApplied && outcome != etcd.IdempotencyKnownExisting {
		return errs.New(errs.KindStateConflict, "CoreDNS automatic work publication has no accepted result")
	}
	return nil
}
