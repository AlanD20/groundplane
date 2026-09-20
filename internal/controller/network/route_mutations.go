package network

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	routeCreationRoute           = "/routes"
	routeEditRoute               = "/routes/{id}"
	maximumRouteMutationAttempts = 3
)

type routeMutationRepository interface {
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
	GetService(context.Context, string) (etcdstore.Versioned[etcd.ServiceRecord], error)
	GetRoute(context.Context, string) (etcdstore.Versioned[routerecord.Record], error)
	BeginRouteMutationWithTask(
		context.Context,
		etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
		etcdstore.Versioned[hierarchyrecord.ProjectRecord],
		etcdstore.Versioned[etcd.ServiceRecord],
		*etcdstore.Versioned[routerecord.Record],
		routerecord.Record,
		etcd.RouteMutationIntent,
		etcd.TaskRecord,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type routeMutationProjectionRepository interface {
	GetEnvironmentAppliedComposeProjection(
		context.Context, string,
	) (etcdstore.Versioned[etcd.EnvironmentComposeProjection], bool, error)
}

type routeMutationDesiredProjectionRepository interface {
	GetEnvironmentComposeProjection(
		context.Context, string,
	) (etcdstore.Versioned[etcd.EnvironmentComposeProjection], bool, error)
}

type routeMutationTaskPlanner interface {
	PrepareRouteMutationTask(
		context.Context,
		etcd.TaskRecord,
		etcd.RouteMutationIntent,
		etcd.RouteMutationProcedureIDs,
	) (etcd.RouteMutationTaskPreparation, error)
}

type routeMutationEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   idempotencyrecord.ProtectedIntentRecord
}

type routeMutationIntent struct {
	method        string
	route         string
	environmentID string
	path          []requestidempotency.PathBinding
	body          requestidempotency.Value
}

type routeMutationIdempotency interface {
	Prepare(context.Context, routeMutationIntent) (routeMutationEvidence, error)
	ResolveExisting(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		routeMutationEvidence,
	) (requestidempotency.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		routeMutationEvidence,
		etcd.IdempotencyTransactionResult,
	) (requestidempotency.Resolution, error)
	ResolveUnknown(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		routeMutationEvidence,
		error,
	) (requestidempotency.Resolution, error)
}

type durableRouteMutationIdempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  requestidempotency.EvidenceRepository
}

func newDurableRouteMutationIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository requestidempotency.EvidenceRepository,
) (*durableRouteMutationIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Route mutation idempotency is not configured")
	}
	return &durableRouteMutationIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableRouteMutationIdempotency) Prepare(
	ctx context.Context,
	intent routeMutationIntent,
) (routeMutationEvidence, error) {
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: intent.method,
		Route:  intent.route,
		Scope: requestidempotency.Scope{
			Kind: requestidempotency.ScopeEnvironment,
			ID:   intent.environmentID,
		},
		Path:  intent.path,
		Query: requestidempotency.Object(),
		Body:  requestidempotency.JSONBody(intent.body),
	})
	if err != nil {
		return routeMutationEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return routeMutationEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return routeMutationEvidence{}, err
	}
	return routeMutationEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableRouteMutationIdempotency) ResolveExisting(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence routeMutationEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableRouteMutationIdempotency) ResolveKnown(
	ctx context.Context,
	evidence routeMutationEvidence,
	result etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableRouteMutationIdempotency) ResolveUnknown(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence routeMutationEvidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type routeMutationService struct {
	repository  routeMutationRepository
	idempotency routeMutationIdempotency
	planner     routeMutationTaskPlanner
	deletions   *routeRemovalService
	now         func() time.Time
}

func newRouteMutationService(
	repository routeMutationRepository,
	idempotency routeMutationIdempotency,
) (*routeMutationService, error) {
	return newRouteMutationServiceWithPlanner(repository, idempotency, nil)
}

func newRouteMutationServiceWithPlanner(
	repository routeMutationRepository,
	idempotency routeMutationIdempotency,
	planner routeMutationTaskPlanner,
) (*routeMutationService, error) {
	if repository == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Route mutation service is not configured")
	}
	return &routeMutationService{
		repository: repository, idempotency: idempotency, planner: planner, now: time.Now,
	}, nil
}

func (service *routeMutationService) RemoveRoute(
	ctx context.Context,
	routeID string,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if service == nil || service.deletions == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Route deletion service is not configured")
	}
	return service.deletions.RemoveRoute(ctx, routeID, idempotencyKey)
}

func (service *routeMutationService) CreateRoute(
	ctx context.Context,
	input apiTypes.RouteCreate,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Route creation context is required")
	}
	if input.Path == "" {
		input.Path = "/"
	}
	if err := validateRouteCreationInput(input); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	intent := routeMutationIntent{
		method: http.MethodPost, route: routeCreationRoute, environmentID: input.EnvironmentID,
		body: requestidempotency.Object(
			requestidempotency.Field{Name: "environment_id", Value: requestidempotency.String(input.EnvironmentID)},
			requestidempotency.Field{Name: "exposure", Value: requestidempotency.String(input.Exposure)},
			requestidempotency.Field{Name: "host", Value: requestidempotency.String(input.Host)},
			requestidempotency.Field{Name: "path", Value: requestidempotency.String(input.Path)},
			requestidempotency.Field{
				Name:  "target_port",
				Value: requestidempotency.UnsignedInteger(uint64(input.TargetPort)),
			},
			requestidempotency.Field{Name: "target_service_id", Value: requestidempotency.String(input.TargetServiceID)},
		),
	}
	for attempt := 0; attempt < maximumRouteMutationAttempts; attempt++ {
		response, err := service.createRouteOnce(ctx, input, idempotencyKey, intent)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumRouteMutationAttempts-1 {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Route creation retry bound was not enforced")
}

func (service *routeMutationService) createRouteOnce(
	ctx context.Context,
	input apiTypes.RouteCreate,
	idempotencyKey string,
	intent routeMutationIntent,
) (idempotencyrecord.IdempotencyResponse, error) {
	evidence, err := service.idempotency.Prepare(ctx, intent)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := routeMutationLocator(intent, idempotencyKey)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if existing {
		return routeReplayResponse(resolution)
	}
	environment, project, target, err := service.routeHierarchy(ctx, input.EnvironmentID, input.TargetServiceID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if !core.ServiceExposesTCPPort(target.Record.Desired.Expose, input.TargetPort) {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Route target Service does not expose the requested TCP port",
		)
	}
	record, err := routerecord.NewRecord(input.EnvironmentID, core.Route{
		ID: ids.New(ids.KindRoute), Host: input.Host, Path: input.Path,
		Exposure: input.Exposure, TargetServiceID: input.TargetServiceID,
		TargetPort: input.TargetPort,
	})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	preparation, err := service.prepareRouteMutationTask(
		ctx, environment, project, record, nil, idempotencyKey,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	record = preparation.Intent.Route
	response, marker, err := service.routeResponseMarker(locator, evidence, record, preparation.Task.ID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	result, mutationErr := service.repository.BeginRouteMutationWithTask(
		ctx, environment, project, target, nil, record, preparation.Intent, preparation.Task, marker,
	)
	return service.resolveRouteMutation(ctx, locator, evidence, result, mutationErr, response)
}

func (service *routeMutationService) EditRoute(
	ctx context.Context,
	routeID string,
	input apiTypes.RouteEdit,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Route edit context is required")
	}
	if ids.Validate(ids.KindRoute, routeID) != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Route edit requires a stable Route id")
	}
	if input.Exposure != "public" && input.Exposure != "internal" {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Route exposure must be public or internal",
		)
	}
	for attempt := 0; attempt < maximumRouteMutationAttempts; attempt++ {
		response, err := service.editRouteOnce(ctx, routeID, input, idempotencyKey)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumRouteMutationAttempts-1 {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Route edit retry bound was not enforced")
}

func (service *routeMutationService) editRouteOnce(
	ctx context.Context,
	routeID string,
	input apiTypes.RouteEdit,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	current, err := service.repository.GetRoute(ctx, routeID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	intent := routeEditMutationIntent(routeID, current.Record.EnvironmentID, input)
	evidence, err := service.idempotency.Prepare(ctx, intent)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := routeMutationLocator(intent, idempotencyKey)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if existing {
		return routeReplayResponse(resolution)
	}
	environment, project, target, err := service.routeHierarchy(
		ctx, current.Record.EnvironmentID, current.Record.Desired.TargetServiceID,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	desired := current.Record.Desired
	desired.Exposure = input.Exposure
	replacement, err := routerecord.ReplaceDesired(current.Record, desired)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	preparation, err := service.prepareRouteMutationTask(
		ctx, environment, project, replacement, &current, idempotencyKey,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	replacement = preparation.Intent.Route
	response, marker, err := service.routeResponseMarker(locator, evidence, replacement, preparation.Task.ID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	result, mutationErr := service.repository.BeginRouteMutationWithTask(
		ctx, environment, project, target, &current, replacement, preparation.Intent, preparation.Task, marker,
	)
	return service.resolveRouteMutation(ctx, locator, evidence, result, mutationErr, response)
}

func (service *routeMutationService) prepareRouteMutationTask(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	record routerecord.Record,
	previous *etcdstore.Versioned[routerecord.Record],
	idempotencyKey string,
) (etcd.RouteMutationTaskPreparation, error) {
	var applied *etcdstore.Versioned[etcd.EnvironmentComposeProjection]
	{
		var projection etcdstore.Versioned[etcd.EnvironmentComposeProjection]
		var found bool
		var err error
		if projectionRepository, ok := service.repository.(routeMutationDesiredProjectionRepository); ok {
			projection, found, err = projectionRepository.GetEnvironmentComposeProjection(ctx, environment.Record.ID)
		} else if service.planner != nil {
			return etcd.RouteMutationTaskPreparation{}, errs.New(
				errs.KindInternal, "Route desired projection repository is not configured",
			)
		} else if projectionRepository, ok := service.repository.(routeMutationProjectionRepository); ok {
			projection, found, err = projectionRepository.GetEnvironmentAppliedComposeProjection(ctx, environment.Record.ID)
		} else {
			found = false
		}
		if err != nil {
			return etcd.RouteMutationTaskPreparation{}, err
		}
		if found {
			applied = &projection
		}
	}
	taskID := ids.New(ids.KindTask)
	operationID := ids.New(ids.KindOperation)
	owner, err := etcd.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		return etcd.RouteMutationTaskPreparation{}, err
	}
	now := service.now().UTC()
	task := etcd.TaskRecord{
		ID: taskID, OperationID: operationID, IdempotencyKey: idempotencyKey,
		Owner: owner, Actor: etcd.TaskActorOperator, PlanID: ids.New(ids.KindPlan),
		Type: etcd.TaskCreate, Target: record.Desired.ID,
		Status: etcd.TaskStatusPending, NextEventSequence: 1,
		CreatedAt: now, UpdatedAt: now,
	}
	if previous != nil {
		task.Type = etcd.TaskUpdate
	}
	intent, err := etcd.NewRouteMutationIntent(
		taskID, operationID, environment.Record.ID, record, previous, applied, now,
	)
	if err != nil {
		return etcd.RouteMutationTaskPreparation{}, err
	}
	if service.planner == nil && applied != nil {
		candidate, applyErr := etcd.ApplyEnvironmentRoute(applied.Record, intent.Route)
		if applyErr != nil {
			return etcd.RouteMutationTaskPreparation{}, applyErr
		}
		candidate.RevisionID = task.ID
		intent.CandidateProjection = &candidate
	}
	if service.planner == nil && applied == nil {
		intent.CurrentProjection = nil
		intent.CurrentProjectionRevision = 0
		task, err = prepareControllerRouteMutationTask(task, intent)
		if err != nil {
			return etcd.RouteMutationTaskPreparation{}, err
		}
		return etcd.RouteMutationTaskPreparation{Intent: intent, Task: task}, nil
	}
	procedures := etcd.RouteMutationProcedureIDs{
		ArtifactID: ids.New(ids.KindConfig), MaterializationID: ids.New(ids.KindConfig),
		MaterializeStepID: ids.New(ids.KindStep), ApplyStepID: ids.New(ids.KindStep),
		ActivateStepID: ids.New(ids.KindStep),
	}
	preparation, err := service.planner.PrepareRouteMutationTask(ctx, task, intent, procedures)
	if err != nil {
		return etcd.RouteMutationTaskPreparation{}, err
	}
	if preparation.Intent.TaskID == "" {
		preparation.Intent = intent
	}
	if preparation.Task.ID == "" {
		preparation.Task = task
	}
	if preparation.Intent.TaskID != taskID || preparation.Task.ID != taskID {
		return etcd.RouteMutationTaskPreparation{}, errs.New(
			errs.KindInternal, "Route mutation planner returned mismatched durable identities",
		)
	}
	return preparation, nil
}

func routeEditMutationIntent(
	routeID string,
	environmentID string,
	input apiTypes.RouteEdit,
) routeMutationIntent {
	return routeMutationIntent{
		method: http.MethodPatch, route: routeEditRoute, environmentID: environmentID,
		path: []requestidempotency.PathBinding{{Name: "id", Value: routeID}},
		body: requestidempotency.Object(
			requestidempotency.Field{Name: "exposure", Value: requestidempotency.String(input.Exposure)},
		),
	}
}

func (service *routeMutationService) routeHierarchy(
	ctx context.Context,
	environmentID string,
	targetServiceID string,
) (
	etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	etcdstore.Versioned[etcd.ServiceRecord],
	error,
) {
	environment, err := service.repository.GetEnvironment(ctx, environmentID)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, etcdstore.Versioned[hierarchyrecord.ProjectRecord]{},
			etcdstore.Versioned[etcd.ServiceRecord]{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, etcdstore.Versioned[hierarchyrecord.ProjectRecord]{},
			etcdstore.Versioned[etcd.ServiceRecord]{}, err
	}
	target, err := service.repository.GetService(ctx, targetServiceID)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, etcdstore.Versioned[hierarchyrecord.ProjectRecord]{},
			etcdstore.Versioned[etcd.ServiceRecord]{}, err
	}
	return environment, project, target, nil
}

func (service *routeMutationService) routeResponseMarker(
	locator idempotencyrecord.IdempotencyLocator,
	evidence routeMutationEvidence,
	record routerecord.Record,
	taskID string,
) (idempotencyrecord.IdempotencyResponse, idempotencyrecord.IdempotencyMarker, error) {
	body, err := json.Marshal(apiTypes.RouteTaskAccepted{Route: routeAPIResponse(record), TaskID: taskID})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, idempotencyrecord.IdempotencyMarker{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(body)
	response := idempotencyrecord.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: append([]byte(nil), body...),
	}
	marker, err := idempotencyrecord.NewCompletedDirectIdempotencyMarker(locator, evidence.durable, response, service.now().UTC())
	if err != nil {
		clear(response.Body)
		return idempotencyrecord.IdempotencyResponse{}, idempotencyrecord.IdempotencyMarker{}, err
	}
	return response, marker, nil
}

func (service *routeMutationService) resolveRouteMutation(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence routeMutationEvidence,
	result etcd.IdempotencyTransactionResult,
	mutationErr error,
	response idempotencyrecord.IdempotencyResponse,
) (idempotencyrecord.IdempotencyResponse, error) {
	var resolution requestidempotency.Resolution
	var err error
	if mutationErr != nil {
		if !isUnknownRouteMutationOutcome(mutationErr) {
			clear(response.Body)
			return idempotencyrecord.IdempotencyResponse{}, mutationErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, mutationErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		clear(response.Body)
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case requestidempotency.ResolutionApplied:
		return response, nil
	case requestidempotency.ResolutionReplay:
		clear(response.Body)
		return cloneIdempotencyResponse(resolution.Response), nil
	default:
		clear(response.Body)
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Route mutation resolution is invalid")
	}
}

func routeMutationLocator(intent routeMutationIntent, idempotencyKey string) idempotencyrecord.IdempotencyLocator {
	return idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment, ScopeID: intent.environmentID,
		Method: intent.method, Route: intent.route, Key: idempotencyKey,
	}
}

func routeReplayResponse(resolution requestidempotency.Resolution) (idempotencyrecord.IdempotencyResponse, error) {
	if resolution.Kind != requestidempotency.ResolutionReplay {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Route replay resolution is invalid")
	}
	return cloneIdempotencyResponse(resolution.Response), nil
}

func validateRouteCreationInput(input apiTypes.RouteCreate) error {
	if ids.Validate(ids.KindEnvironment, input.EnvironmentID) != nil {
		return errs.New(errs.KindValidationFailed, "Route creation requires a stable Environment id")
	}
	if ids.Validate(ids.KindService, input.TargetServiceID) != nil {
		return errs.New(errs.KindValidationFailed, "Route creation requires a stable target Service id")
	}
	if err := (core.Route{
		ID: ids.New(ids.KindRoute), Host: input.Host, Path: input.Path,
		Exposure: input.Exposure, TargetServiceID: input.TargetServiceID,
		TargetPort: input.TargetPort,
	}).Validate(); err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	return nil
}

func routeAPIResponse(record routerecord.Record) apiTypes.Route {
	return apiTypes.Route{
		ID: record.Desired.ID, EnvironmentID: record.EnvironmentID, Host: record.Desired.Host,
		Path: record.Desired.Path, Exposure: string(record.Desired.Exposure),
		TargetServiceID: record.Desired.TargetServiceID, TargetPort: record.Desired.TargetPort,
		Status: string(record.Observed.Status),
	}
}

func prepareControllerRouteMutationTask(
	task etcd.TaskRecord,
	intent etcd.RouteMutationIntent,
) (etcd.TaskRecord, error) {
	if intent.Provider != nil || intent.CurrentProjection != nil || intent.CandidateProjection != nil {
		return etcd.TaskRecord{}, errs.New(errs.KindInternal, "desired-only Route mutation has provider state")
	}
	task.Executor = etcd.TaskExecutorController
	task.TimeoutSeconds = 30
	task.RenderGeneration = int32(intent.Route.DesiredGeneration)
	task.Params = map[string]string{
		etcd.TaskResourceKindParam:     etcd.TaskResourceRoute,
		etcd.TaskRouteEnvironmentParam: intent.EnvironmentID,
	}
	task.Steps = []etcd.TaskStepRecord{{Kind: etcd.TaskStepOperation, ID: ids.New(ids.KindStep)}}
	value, err := json.Marshal(struct {
		Version    int    `json:"version"`
		TaskID     string `json:"task_id"`
		RouteID    string `json:"route_id"`
		Generation uint64 `json:"generation"`
	}{1, task.ID, intent.RouteID, intent.Route.DesiredGeneration})
	if err != nil {
		return etcd.TaskRecord{}, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(value)
	clear(value)
	task.PlanHash = hex.EncodeToString(digest[:])
	return task, nil
}

func isUnknownRouteMutationOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
