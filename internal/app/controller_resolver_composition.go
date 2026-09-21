package app

import (
	"context"
	"fmt"
	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/internal/app/componentregistration"
	controllerdns "github.com/AlanD20/groundplane/internal/controller/dnsresolver"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	resolutionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hostresolution"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	platformcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/resolverbaseline"
	"github.com/AlanD20/groundplane/internal/infra/hostresolution"
)

type controllerResolverComposition struct {
	renderer         componentdns.Renderer
	renderPlanner    *controllerdns.PlatformRenderPlanner
	executionPlanner *controllerdns.PlatformExecutionPlanner
}

func newControllerResolverComposition(
	ctx context.Context,
	volumeRoot string,
	componentRecords *etcd.ComponentRepository,
	resolverBaselines *resolverbaseline.Repository,
	resolutionProjections *resolutionrecord.Repository,
	tasks *etcd.TaskRepository,
	intentCoordinator *requestidempotency.Coordinator,
	planResolver *taskplanning.TaskPlanResolver,
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
	)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize platform Component render planner: %w", err)
	}
	if err := tasks.SetPlatformResolverTaskPreparer(func(
		ctx context.Context,
		current etcdstore.Versioned[componentrecord.Record],
		projection resolutionrecord.HostResolutionProjectionRecord,
		task etcd.TaskRecord,
		priorObservation *platformcomponents.ComponentObservationRecord,
	) (platformcomponents.PlatformComponentTaskRenderInput, error) {
		desired, err := componentrecord.ProjectRecord(current.Record)
		if err != nil {
			return platformcomponents.PlatformComponentTaskRenderInput{}, err
		}
		return platformRenderPlanner.PrepareConfigTaskAtProjection(
			ctx, current, desired, task, projection, priorObservation,
		)
	}); err != nil {
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
		volumeRoot, componentRecords, resolverBaselines, actionCatalog,
	)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Platform Component execution planner: %w", err)
	}
	if err := planResolver.EnableComponentPlans(platformComponentExecution); err != nil {
		return nil, fmt.Errorf("controller: initialize Platform Component plan resolver: %w", err)
	}
	return &controllerResolverComposition{
		renderer: coreDNSRenderer, renderPlanner: platformRenderPlanner,
		executionPlanner: platformComponentExecution,
	}, nil
}
