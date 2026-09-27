package app

import (
	"context"
	"fmt"
	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/internal/app/componentregistration"
	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	controllerdns "github.com/AlanD20/groundplane/internal/controller/dnsresolver"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	resolutionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hostresolution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/resolverbaseline"
	"github.com/AlanD20/groundplane/internal/infra/hostresolution"
	"log/slog"
)

type controllerResolverComposition struct {
	renderer         componentdns.Renderer
	renderPlanner    *controllerdns.PlatformRenderPlanner
	executionPlanner *controllerdns.PlatformExecutionPlanner
	listener         *controllerdns.ListenerReconciler
}

func newControllerResolverComposition(
	ctx context.Context,
	cfg config.ControllerConfig,
	componentRecords *etcd.ComponentRepository,
	resolverBaselines *resolverbaseline.Repository,
	resolutionProjections *resolutionrecord.Repository,
	tasks *etcd.TaskRepository,
	intentCoordinator *requestidempotency.Coordinator,
	planResolver *taskplanning.TaskPlanResolver,
	logger *slog.Logger,
) (*controllerResolverComposition, error) {
	coreDNSRenderer, err := componentregistration.NewDNSRenderer()
	if err != nil {
		return nil, fmt.Errorf("controller: initialize CoreDNS renderer: %w", err)
	}
	actionCatalog, err := componentregistration.NewCatalog()
	if err != nil {
		return nil, fmt.Errorf("controller: initialize registered Component action catalog: %w", err)
	}
	platformRenderPlanner, err := controllerdns.NewPlatformRenderPlanner(
		resolutionProjections,
		resolverBaselines,
		componentRecords,
		controllerdns.BaselineCapture(hostresolution.CaptureBaseline),
		coreDNSRenderer,
		actionCatalog,
		actionCatalog,
		componentregistration.ManagedConfigActivateAction,
		imagefetch.RegistryAddress(cfg.Listen.HTTP),
	)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize platform Component render planner: %w", err)
	}
	if err := tasks.SetPlatformResolverTaskPreparer(controllerdns.NewPlatformResolverTaskPreparer(
		platformRenderPlanner, intentCoordinator,
	)); err != nil {
		return nil, fmt.Errorf("controller: configure platform resolver Task preparer: %w", err)
	}
	if err := tasks.SetPlatformResolverComponentSelector(platformRenderPlanner.SelectResolver); err != nil {
		return nil, fmt.Errorf("controller: configure platform resolver Component selector: %w", err)
	}
	if err := controllerdns.EnsurePlatformResolverTask(
		ctx, componentRecords, tasks, platformRenderPlanner, intentCoordinator,
	); err != nil {
		return nil, fmt.Errorf("controller: initialize platform resolver projection: %w", err)
	}
	platformComponentExecution, err := controllerdns.NewPlatformComponentExecutionPlanner(
		cfg.Storage.VolumeRoot, componentRecords, resolverBaselines, actionCatalog,
	)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Platform Component execution planner: %w", err)
	}
	if err := planResolver.EnableComponentPlans(platformComponentExecution); err != nil {
		return nil, fmt.Errorf("controller: initialize Platform Component plan resolver: %w", err)
	}
	listener, err := controllerdns.NewListenerReconciler(tasks, imagefetch.RegistryAddress(cfg.Listen.HTTP), logger)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize resolver listener reconciliation: %w", err)
	}
	return &controllerResolverComposition{
		renderer: coreDNSRenderer, renderPlanner: platformRenderPlanner,
		executionPlanner: platformComponentExecution,
		listener:         listener,
	}, nil
}
