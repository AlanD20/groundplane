package network

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
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
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Route edit requires a stable Route id",
		)
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
	return idempotencyrecord.IdempotencyResponse{}, errs.New(
		errs.KindInternal,
		"Route edit retry bound was not enforced",
	)
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
	environment, project, err := service.routeOwnership(ctx, current.Record.EnvironmentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	return service.submitRouteWork(
		ctx,
		environment,
		project,
		routeID,
		current.Record.Desired.Host+current.Record.Desired.Path,
		workexecution.ActionUpdate,
		locator,
		evidence,
		acceptedRouteMutation{Edit: &input, After: input.After},
	)

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
			requestidempotency.Field{Name: "after", Value: acceptedaction.AfterValue(input.After)},
			requestidempotency.Field{Name: "exposure", Value: requestidempotency.String(input.Exposure)},
		),
	}
}
