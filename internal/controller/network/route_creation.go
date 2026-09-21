package network

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
)

func (service *routeMutationService) CreateRoute(
	ctx context.Context,
	input apiTypes.RouteCreate,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Route creation context is required",
		)
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
			requestidempotency.Field{
				Name:  "target_service_id",
				Value: requestidempotency.String(input.TargetServiceID),
			},
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
	return idempotencyrecord.IdempotencyResponse{}, errs.New(
		errs.KindInternal,
		"Route creation retry bound was not enforced",
	)
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
