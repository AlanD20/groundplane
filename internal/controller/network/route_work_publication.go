package network

import (
	"context"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"slices"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentwork"
	desiredrevisionstore "github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/materializationcontent"
	"github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RouteWorkPublisher struct {
	repository    *etcd.HierarchyRepository
	desired       *desiredrevisionstore.Repository
	providers     *taskplanning.RouteProviderCapture
	configuration *taskplanning.ComponentConfigurationCapture
	snapshots     *runtimeconfiguration.Repository
	contents      *materializationcontent.Repository
}

func NewRouteWorkPublisher(
	repository *etcd.HierarchyRepository,
	desired *desiredrevisionstore.Repository,
	providers *taskplanning.RouteProviderCapture,
	configuration *taskplanning.ComponentConfigurationCapture,
	snapshots *runtimeconfiguration.Repository,
	contents *materializationcontent.Repository,
) (*RouteWorkPublisher, error) {
	if repository == nil || desired == nil || providers == nil || configuration == nil || snapshots == nil ||
		contents == nil {
		return nil, errs.New(errs.KindInternal, "Route native publication is not configured")
	}
	return &RouteWorkPublisher{
		repository:    repository,
		desired:       desired,
		providers:     providers,
		configuration: configuration,
		snapshots:     snapshots,
		contents:      contents,
	}, nil
}

type routeWorkRequest struct {
	Accepted      workqueue.TaskSnapshot
	At            time.Time
	Project       keyvalue.Versioned[hierarchy.ProjectRecord]
	Environment   keyvalue.Versioned[hierarchy.EnvironmentRecord]
	Target        keyvalue.Versioned[services.ServiceRecord]
	Current       *keyvalue.Versioned[routes.Record]
	Route         routes.Record
	Provider      *environmentchanges.RouteProviderPin
	Claim         blueprints.EnvironmentBlueprintStageClaim
	Candidate     environmentprojection.EnvironmentComposeProjection
	EpochRevision int64
	Action        workexecution.Action
}

func (publisher *RouteWorkPublisher) prepare(
	ctx context.Context,
	request routeWorkRequest,
	finish func(context.Context, bool) error) (taskdependencies.Prepared, error) {
	marker, err := routeWorkResponse(request.Claim, request.Route)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	if request.Provider == nil {
		return publisher.prepareDefinition(ctx, request, marker, finish)
	}
	placement, err := publisher.repository.ResolveResourceHost(ctx)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	task, item, err := taskplanning.NewRouteConfigurationWork(workexecution.Scope{
		TenantID: request.Project.Record.TenantID, ProjectID: request.Project.Record.ID,
		EnvironmentID: request.Environment.Record.ID,
	}, placement, request.Claim, request.Action, request.Route.Desired.ID,
		request.Route.Desired.Host+request.Route.Desired.Path, request.Provider.ComponentID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	task.Intent = *request.Accepted.Task.AcceptedIntent
	retained := request.Accepted.Task
	retained.Intent, retained.ExpectedItems, retained.UpdatedAt = task.Intent, 1, request.At
	task = retained
	item.CreatedAt, item.UpdatedAt = request.At, request.At
	item.Deadline = acceptedaction.ExecutionDeadline(
		task,
		request.At,
		time.Duration(desiredrevision.TaskTimeoutSeconds)*time.Second,
	)
	runtime, present, err := publisher.providers.CapturedRuntime(ctx, *request.Provider)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	var expected componentrecord.Record
	for _, component := range request.Candidate.Components {
		if component.Desired.ID == request.Provider.ComponentID {
			expected = component
			break
		}
	}
	component, err := publisher.repository.ReadRouteWorkComponent(ctx, expected, request.Provider.InputRevision)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	operationID := ids.DeriveAt(ids.KindOperation, task.CreatedAt, item.ID, "route-operation")
	configurationID := ids.DeriveAt(ids.KindConfig, task.CreatedAt, item.ID, "configuration")
	rendered, err := publisher.providers.RenderWork(ctx, task, item, request.Project.Record, request.Environment.Record,
		request.Candidate, *request.Provider)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	defer clear(rendered.Content)
	if err := publisher.contents.Stage(ctx, rendered.File, request.Candidate.RenderGeneration, rendered.Content); err != nil {
		return taskdependencies.Prepared{}, err
	}
	prepared, err := publisher.repository.PrepareMutationWorkConfiguration(ctx, task, item, request.Claim,
		request.Provider.InputRevision, configurationID, operationID, []taskmaterialization.Record{rendered.File})
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	var prior runtimeconfiguration.Snapshot
	if prepared.Capture.Prior != nil {
		prior, err = publisher.snapshots.LoadRetained(ctx, *prepared.Capture.Prior)
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
	}
	input, err := publisher.configuration.PrepareRouteWork(ctx, taskplanning.RouteWorkRequest{
		OperationID: operationID, RevisionID: task.ID, Provider: *request.Provider,
		Component: component.Record, Artifact: rendered.Artifact, File: rendered.File,
		Configuration: prepared.Capture, PriorConfiguration: prior,
	})
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	capture := componentwork.Capture{Current: component.Record, CurrentRevision: component.Revision,
		Candidate: component.Record, RuntimeRevision: runtime.Revision, Configuration: prepared.Capture,
		Route: &componentwork.RouteSettlement{
			Record: request.Route,
			Remove: request.Action == workexecution.ActionRemove,
			Provider: routes.ProviderObservation{ComponentID: request.Provider.ComponentID,
				DefinitionDigest: request.Provider.DefinitionDigest, CatalogDigest: request.Provider.CatalogDigest,
				InputRevision: request.Provider.InputRevision, InputGeneration: request.Provider.InputGeneration},
		},
	}
	if present {
		predecessor, err := componentwork.CaptureRuntimePredecessor(runtime.Record)
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
		capture.Predecessor = &predecessor
	}
	if !prepared.Secrets.IsZero() {
		binding, err := prepared.Secrets.Binding()
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
		capture.Secrets = &binding
	}
	for _, zone := range request.Candidate.DesiredZones {
		capture.Zones = append(
			capture.Zones,
			zonerecord.Record{EnvironmentID: zone.EnvironmentID, Desired: zone.Desired},
		)
	}
	slices.SortFunc(
		capture.Zones,
		func(left, right zonerecord.Record) int { return strings.Compare(left.Desired.ID, right.Desired.ID) },
	)
	captureResult, err := publisher.repository.PrepareRouteWorkCapture(ctx, etcd.RouteWorkPublication{
		Project: request.Project, Environment: request.Environment, Target: request.Target, Current: request.Current,
		Claim: request.Claim, Projection: request.Candidate, EpochRevision: request.EpochRevision,
		Task: task, Item: item, InputID: ids.DeriveAt(ids.KindPlan, task.CreatedAt, item.ID, "captured-input"),
		Input: input, Capture: capture, Secrets: prepared.Secrets, Marker: marker, PreparationConditions: prepared.Conditions,
	})
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	return taskdependencies.Prepared{
		Task:      task,
		Graph:     []workexecution.Item{captureResult.Item},
		Authority: captureResult.Change,
		Finalize:  finish,
	}, nil
}
