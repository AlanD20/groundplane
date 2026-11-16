package network

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
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
			requestidempotency.Field{Name: "after", Value: acceptedaction.AfterValue(input.After)},
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
	environment, project, err := service.routeOwnership(ctx, input.EnvironmentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}

	record, err := routerecord.NewRecord(input.EnvironmentID, core.Route{
		ID: ids.New(ids.KindRoute), Host: input.Host, Path: input.Path,
		Exposure: input.Exposure, TargetServiceID: input.TargetServiceID,
		TargetPort: input.TargetPort,
	})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	return service.submitRouteWork(
		ctx,
		environment,
		project,
		record.Desired.ID,
		record.Desired.Host+record.Desired.Path,
		workexecution.ActionCreate,
		locator,
		evidence,
		acceptedRouteMutation{Create: &input, After: input.After},
	)

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
