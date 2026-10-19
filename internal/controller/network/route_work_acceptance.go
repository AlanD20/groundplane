package network

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *routeMutationService) acceptRouteWork(
	ctx context.Context,
	environment keyvalue.Versioned[hierarchy.EnvironmentRecord],
	project keyvalue.Versioned[hierarchy.ProjectRecord],
	target keyvalue.Versioned[services.ServiceRecord],
	previous *keyvalue.Versioned[routes.Record],
	record routes.Record,
	action workexecution.Action,
	locator idempotency.IdempotencyLocator,
	evidence routeMutationEvidence,
) (idempotency.IdempotencyResponse, error) {
	current, found, err := service.repository.GetEnvironmentComposeProjection(ctx, environment.Record.ID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if !found {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict,
			"Route requires the Environment's authored Service definition",
		)
	}
	staged, err := service.publisher.stage(ctx, routeWorkStageRequest{
		Current: current, Route: record, Action: action, ReadRevision: current.ReadRevision,
		Locator: locator, Evidence: evidence, Idempotency: service.idempotency, CreatedAt: service.now().UTC(),
	})
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	marker, err := routeWorkResponse(staged.Claim, staged.Route)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	result, publicationErr := service.publisher.publish(ctx, routeWorkRequest{
		Project: project, Environment: environment, Target: target, Current: previous, Route: staged.Route,
		Provider: staged.Provider, Claim: staged.Claim, Candidate: staged.Candidate,
		EpochRevision: staged.EpochRevision, Action: action,
	})
	return service.resolveRouteMutation(ctx, locator, evidence, result, publicationErr, marker.Response)
}
