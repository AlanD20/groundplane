package app

import (
	"fmt"
	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/internal/app/componentregistration"
	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	controllerdns "github.com/AlanD20/groundplane/internal/controller/dnsresolver"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	zonednsruntime "github.com/AlanD20/groundplane/internal/infra/docker/zonedns"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	resolutionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hostresolution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/resolverbaseline"
	"github.com/AlanD20/groundplane/internal/infra/hostresolution"
	"log/slog"
)

type controllerResolverComposition struct {
	renderer      componentdns.Renderer
	renderPlanner *controllerdns.PlatformRenderPlanner
	configuration *controllerdns.ConfigurationReconciler
}

func newControllerResolverComposition(
	cfg config.ControllerConfig,
	componentRecords *etcd.ComponentRepository,
	resolverBaselines *resolverbaseline.Repository,
	resolutionProjections *resolutionrecord.Repository,
	tasks *etcd.TaskRepository,
	intentCoordinator *requestidempotency.Coordinator,
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
	zoneListeners, err := controllerdns.NewZoneListenerSource(componentRecords, zonednsruntime.Inspector{})
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Zone DNS listeners: %w", err)
	}
	platformRenderPlanner, err := controllerdns.NewPlatformRenderPlanner(
		resolutionProjections,
		componentRecords,
		resolverBaselines,
		controllerdns.BaselineCapture(hostresolution.CaptureBaseline),
		coreDNSRenderer,
		actionCatalog,
		actionCatalog,
		componentregistration.ManagedConfigActivateAction,
		imagefetch.RegistryAddress(cfg.Listen.HTTP),
		zoneListeners,
	)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize platform Component render planner: %w", err)
	}
	configuration, err := controllerdns.NewConfigurationReconciler(
		tasks,
		componentRecords,
		platformRenderPlanner,
		intentCoordinator,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize resolver configuration reconciliation: %w", err)
	}
	return &controllerResolverComposition{
		renderer: coreDNSRenderer, renderPlanner: platformRenderPlanner,
		configuration: configuration,
	}, nil
}
