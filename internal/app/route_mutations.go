package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
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
	GetEnvironment(context.Context, string) (etcd.Versioned[etcd.EnvironmentRecord], error)
	GetProject(context.Context, string) (etcd.Versioned[etcd.ProjectRecord], error)
	GetService(context.Context, string) (etcd.Versioned[etcd.ServiceRecord], error)
	GetRoute(context.Context, string) (etcd.Versioned[etcd.RouteRecord], error)
	CreateRouteIdempotent(
		context.Context,
		etcd.Versioned[etcd.EnvironmentRecord],
		etcd.Versioned[etcd.ProjectRecord],
		etcd.Versioned[etcd.ServiceRecord],
		etcd.RouteRecord,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
	ReplaceDesiredIdempotent(
		context.Context,
		etcd.Versioned[etcd.EnvironmentRecord],
		etcd.Versioned[etcd.ProjectRecord],
		etcd.Versioned[etcd.ServiceRecord],
		etcd.Versioned[etcd.RouteRecord],
		core.Route,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type durableRouteMutationRepository struct {
	hierarchy *etcd.HierarchyRepository
	services  *etcd.ServiceRepository
	routes    *etcd.RouteRepository
}

func newDurableRouteMutationRepository(
	hierarchy *etcd.HierarchyRepository,
	services *etcd.ServiceRepository,
	routes *etcd.RouteRepository,
) (*durableRouteMutationRepository, error) {
	if hierarchy == nil || services == nil || routes == nil {
		return nil, errs.New(errs.KindInternal, "Route mutation repositories are not configured")
	}
	return &durableRouteMutationRepository{hierarchy: hierarchy, services: services, routes: routes}, nil
}

func (repository *durableRouteMutationRepository) GetEnvironment(
	ctx context.Context,
	id string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return repository.hierarchy.GetEnvironment(ctx, id)
}

func (repository *durableRouteMutationRepository) GetProject(
	ctx context.Context,
	id string,
) (etcd.Versioned[etcd.ProjectRecord], error) {
	return repository.hierarchy.GetProject(ctx, id)
}

func (repository *durableRouteMutationRepository) GetService(
	ctx context.Context,
	id string,
) (etcd.Versioned[etcd.ServiceRecord], error) {
	return repository.services.GetService(ctx, id)
}

func (repository *durableRouteMutationRepository) GetRoute(
	ctx context.Context,
	id string,
) (etcd.Versioned[etcd.RouteRecord], error) {
	return repository.routes.GetRoute(ctx, id)
}

func (repository *durableRouteMutationRepository) CreateRouteIdempotent(
	ctx context.Context,
	environment etcd.Versioned[etcd.EnvironmentRecord],
	project etcd.Versioned[etcd.ProjectRecord],
	target etcd.Versioned[etcd.ServiceRecord],
	record etcd.RouteRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.routes.CreateRouteIdempotent(ctx, environment, project, target, record, marker)
}

func (repository *durableRouteMutationRepository) ReplaceDesiredIdempotent(
	ctx context.Context,
	environment etcd.Versioned[etcd.EnvironmentRecord],
	project etcd.Versioned[etcd.ProjectRecord],
	target etcd.Versioned[etcd.ServiceRecord],
	current etcd.Versioned[etcd.RouteRecord],
	desired core.Route,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.routes.ReplaceDesiredIdempotent(ctx, environment, project, target, current, desired, marker)
}

type routeMutationEvidence struct {
	candidate idempotentintent.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type routeMutationIntent struct {
	method        string
	route         string
	environmentID string
	path          []idempotentintent.PathBinding
	body          idempotentintent.Value
}

type routeMutationIdempotency interface {
	Prepare(context.Context, routeMutationIntent) (routeMutationEvidence, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		routeMutationEvidence,
	) (idempotentintent.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		routeMutationEvidence,
		etcd.IdempotencyTransactionResult,
	) (idempotentintent.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		routeMutationEvidence,
		error,
	) (idempotentintent.Resolution, error)
}

type durableRouteMutationIdempotency struct {
	coordinator *idempotentintent.Coordinator
	repository  *etcd.IdempotencyRepository
}

func newDurableRouteMutationIdempotency(
	coordinator *idempotentintent.Coordinator,
	repository *etcd.IdempotencyRepository,
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
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: intent.method,
		Route:  intent.route,
		Scope: idempotentintent.Scope{
			Kind: idempotentintent.ScopeEnvironment,
			ID:   intent.environmentID,
		},
		Path:  intent.path,
		Query: idempotentintent.Object(),
		Body:  idempotentintent.JSONBody(intent.body),
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
	locator etcd.IdempotencyLocator,
	evidence routeMutationEvidence,
) (idempotentintent.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableRouteMutationIdempotency) ResolveKnown(
	ctx context.Context,
	evidence routeMutationEvidence,
	result etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableRouteMutationIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence routeMutationEvidence,
	original error,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type routeMutationService struct {
	repository  routeMutationRepository
	idempotency routeMutationIdempotency
	deletions   *routeDeletionService
	now         func() time.Time
}

func newRouteMutationService(
	repository routeMutationRepository,
	idempotency routeMutationIdempotency,
) (*routeMutationService, error) {
	if repository == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Route mutation service is not configured")
	}
	return &routeMutationService{repository: repository, idempotency: idempotency, now: time.Now}, nil
}

func (service *routeMutationService) RemoveRoute(
	ctx context.Context,
	routeID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if service == nil || service.deletions == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Route deletion service is not configured")
	}
	return service.deletions.RemoveRoute(ctx, routeID, idempotencyKey)
}

func (service *routeMutationService) CreateRoute(
	ctx context.Context,
	input apiTypes.RouteCreate,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Route creation context is required")
	}
	if input.Path == "" {
		input.Path = "/"
	}
	if err := validateRouteCreationInput(input); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	intent := routeMutationIntent{
		method: http.MethodPost, route: routeCreationRoute, environmentID: input.EnvironmentID,
		body: idempotentintent.Object(
			idempotentintent.Field{Name: "environment_id", Value: idempotentintent.String(input.EnvironmentID)},
			idempotentintent.Field{Name: "exposure", Value: idempotentintent.String(input.Exposure)},
			idempotentintent.Field{Name: "host", Value: idempotentintent.String(input.Host)},
			idempotentintent.Field{Name: "path", Value: idempotentintent.String(input.Path)},
			idempotentintent.Field{
				Name:  "target_port",
				Value: idempotentintent.UnsignedInteger(uint64(input.TargetPort)),
			},
			idempotentintent.Field{Name: "target_service_id", Value: idempotentintent.String(input.TargetServiceID)},
		),
	}
	for attempt := 0; attempt < maximumRouteMutationAttempts; attempt++ {
		response, err := service.createRouteOnce(ctx, input, idempotencyKey, intent)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumRouteMutationAttempts-1 {
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Route creation retry bound was not enforced")
}

func (service *routeMutationService) createRouteOnce(
	ctx context.Context,
	input apiTypes.RouteCreate,
	idempotencyKey string,
	intent routeMutationIntent,
) (etcd.IdempotencyResponse, error) {
	evidence, err := service.idempotency.Prepare(ctx, intent)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := routeMutationLocator(intent, idempotencyKey)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		return routeReplayResponse(resolution)
	}
	environment, project, target, err := service.routeHierarchy(ctx, input.EnvironmentID, input.TargetServiceID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	record, err := etcd.NewRouteRecord(input.EnvironmentID, core.Route{
		ID: ids.New(ids.KindRoute), Host: input.Host, Path: input.Path,
		Exposure: input.Exposure, TargetServiceID: input.TargetServiceID,
		TargetPort: input.TargetPort,
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	response, marker, err := service.routeResponseMarker(locator, evidence, record, http.StatusCreated)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	result, mutationErr := service.repository.CreateRouteIdempotent(
		ctx, environment, project, target, record, marker,
	)
	return service.resolveRouteMutation(ctx, locator, evidence, result, mutationErr, response)
}

func (service *routeMutationService) EditRoute(
	ctx context.Context,
	routeID string,
	input apiTypes.RouteEdit,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Route edit context is required")
	}
	if ids.Validate(ids.KindRoute, routeID) != nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Route edit requires a stable Route id")
	}
	if input.Exposure != "public" && input.Exposure != "internal" {
		return etcd.IdempotencyResponse{}, errs.New(
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
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Route edit retry bound was not enforced")
}

func (service *routeMutationService) editRouteOnce(
	ctx context.Context,
	routeID string,
	input apiTypes.RouteEdit,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	current, err := service.repository.GetRoute(ctx, routeID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	intent := routeEditMutationIntent(routeID, current.Record.EnvironmentID, input)
	evidence, err := service.idempotency.Prepare(ctx, intent)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := routeMutationLocator(intent, idempotencyKey)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		return routeReplayResponse(resolution)
	}
	environment, project, target, err := service.routeHierarchy(
		ctx, current.Record.EnvironmentID, current.Record.Desired.TargetServiceID,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	desired := current.Record.Desired
	desired.Exposure = input.Exposure
	replacement, err := etcd.ReplaceRouteDesired(current.Record, desired)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	response, marker, err := service.routeResponseMarker(locator, evidence, replacement, http.StatusOK)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	result, mutationErr := service.repository.ReplaceDesiredIdempotent(
		ctx, environment, project, target, current, desired, marker,
	)
	return service.resolveRouteMutation(ctx, locator, evidence, result, mutationErr, response)
}

func routeEditMutationIntent(
	routeID string,
	environmentID string,
	input apiTypes.RouteEdit,
) routeMutationIntent {
	return routeMutationIntent{
		method: http.MethodPatch, route: routeEditRoute, environmentID: environmentID,
		path: []idempotentintent.PathBinding{{Name: "id", Value: routeID}},
		body: idempotentintent.Object(
			idempotentintent.Field{Name: "exposure", Value: idempotentintent.String(input.Exposure)},
		),
	}
}

func (service *routeMutationService) routeHierarchy(
	ctx context.Context,
	environmentID string,
	targetServiceID string,
) (
	etcd.Versioned[etcd.EnvironmentRecord],
	etcd.Versioned[etcd.ProjectRecord],
	etcd.Versioned[etcd.ServiceRecord],
	error,
) {
	environment, err := service.repository.GetEnvironment(ctx, environmentID)
	if err != nil {
		return etcd.Versioned[etcd.EnvironmentRecord]{}, etcd.Versioned[etcd.ProjectRecord]{},
			etcd.Versioned[etcd.ServiceRecord]{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return etcd.Versioned[etcd.EnvironmentRecord]{}, etcd.Versioned[etcd.ProjectRecord]{},
			etcd.Versioned[etcd.ServiceRecord]{}, err
	}
	target, err := service.repository.GetService(ctx, targetServiceID)
	if err != nil {
		return etcd.Versioned[etcd.EnvironmentRecord]{}, etcd.Versioned[etcd.ProjectRecord]{},
			etcd.Versioned[etcd.ServiceRecord]{}, err
	}
	return environment, project, target, nil
}

func (service *routeMutationService) routeResponseMarker(
	locator etcd.IdempotencyLocator,
	evidence routeMutationEvidence,
	record etcd.RouteRecord,
	status int,
) (etcd.IdempotencyResponse, etcd.IdempotencyMarker, error) {
	body, err := json.Marshal(routeAPIResponse(record))
	if err != nil {
		return etcd.IdempotencyResponse{}, etcd.IdempotencyMarker{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(body)
	response := etcd.IdempotencyResponse{
		Status: status, ContentKind: "application/json", Body: append([]byte(nil), body...),
	}
	marker, err := etcd.NewCompletedDirectIdempotencyMarker(locator, evidence.durable, response, service.now().UTC())
	if err != nil {
		clear(response.Body)
		return etcd.IdempotencyResponse{}, etcd.IdempotencyMarker{}, err
	}
	return response, marker, nil
}

func (service *routeMutationService) resolveRouteMutation(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence routeMutationEvidence,
	result etcd.IdempotencyTransactionResult,
	mutationErr error,
	response etcd.IdempotencyResponse,
) (etcd.IdempotencyResponse, error) {
	var resolution idempotentintent.Resolution
	var err error
	if mutationErr != nil {
		if !isUnknownRouteMutationOutcome(mutationErr) {
			clear(response.Body)
			return etcd.IdempotencyResponse{}, mutationErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, mutationErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		clear(response.Body)
		return etcd.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case idempotentintent.ResolutionApplied:
		return response, nil
	case idempotentintent.ResolutionReplay:
		clear(response.Body)
		return cloneIdempotencyResponse(resolution.Response), nil
	default:
		clear(response.Body)
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Route mutation resolution is invalid")
	}
}

func routeMutationLocator(intent routeMutationIntent, idempotencyKey string) etcd.IdempotencyLocator {
	return etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: intent.environmentID,
		Method: intent.method, Route: intent.route, Key: idempotencyKey,
	}
}

func routeReplayResponse(resolution idempotentintent.Resolution) (etcd.IdempotencyResponse, error) {
	if resolution.Kind != idempotentintent.ResolutionReplay {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Route replay resolution is invalid")
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

func routeAPIResponse(record etcd.RouteRecord) apiTypes.Route {
	return apiTypes.Route{
		ID: record.Desired.ID, EnvironmentID: record.EnvironmentID, Host: record.Desired.Host,
		Path: record.Desired.Path, Exposure: string(record.Desired.Exposure),
		TargetServiceID: record.Desired.TargetServiceID, TargetPort: record.Desired.TargetPort,
	}
}

func isUnknownRouteMutationOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
