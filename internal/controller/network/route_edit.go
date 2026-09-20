package network

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
)

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
