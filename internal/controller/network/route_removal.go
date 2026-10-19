package network

import (
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *routeMutationService) RemoveRoute(
	ctx context.Context,
	routeID, idempotencyKey string,
) (idempotency.IdempotencyResponse, error) {
	if ctx == nil || ids.Validate(ids.KindRoute, routeID) != nil {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Route removal requires a valid Route identity and request context",
		)
	}
	for attempt := 0; attempt < maximumRouteMutationAttempts; attempt++ {
		response, err := service.removeRouteOnce(ctx, routeID, idempotencyKey)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumRouteMutationAttempts-1 {
			return idempotency.IdempotencyResponse{}, err
		}
	}
	return idempotency.IdempotencyResponse{}, errs.New(errs.KindInternal, "Route removal retry bound was not enforced")
}

func (service *routeMutationService) removeRouteOnce(
	ctx context.Context,
	routeID, key string,
) (idempotency.IdempotencyResponse, error) {
	locator, indexed, err := service.idempotency.ResolveReplayLocator(ctx,
		idempotency.IdempotencyReplayTarget{Kind: idempotency.IdempotencyReplayTargetRoute, ID: routeID},
		http.MethodDelete, routeEditRoute, key)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if indexed {
		return service.replayRouteRemoval(ctx, routeID, locator)
	}
	current, err := service.repository.GetRoute(ctx, routeID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	intent := routeRemovalIntent(routeID, current.Record.EnvironmentID)
	evidence, err := service.idempotency.Prepare(ctx, intent)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator = routeMutationLocator(intent, key)
	resolution, found, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if found {
		return routeReplayResponse(resolution)
	}
	environment, project, target, err := service.routeHierarchy(
		ctx,
		current.Record.EnvironmentID,
		current.Record.Desired.TargetServiceID,
	)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	return service.acceptRouteWork(
		ctx,
		environment,
		project,
		target,
		&current,
		current.Record,
		workexecution.ActionRemove,
		locator,
		evidence,
	)
}

func routeRemovalIntent(routeID, environmentID string) routeMutationIntent {
	return routeMutationIntent{method: http.MethodDelete, route: routeEditRoute, environmentID: environmentID,
		path: []requestidempotency.PathBinding{{Name: "id", Value: routeID}}}
}

func (service *routeMutationService) replayRouteRemoval(
	ctx context.Context,
	routeID string,
	locator idempotency.IdempotencyLocator,
) (idempotency.IdempotencyResponse, error) {
	evidence, err := service.idempotency.Prepare(ctx, routeRemovalIntent(routeID, locator.ScopeID))
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, found, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if !found {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict,
			"Route removal replay record is unavailable",
		)
	}
	return routeReplayResponse(resolution)
}
