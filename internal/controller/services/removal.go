package services

import (
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type acceptedServiceRemoval struct {
	Locator idempotencyrecord.IdempotencyLocator
	Intent  idempotencyrecord.ProtectedIntentRecord
}

func (service *serviceMutationService) AcceptedPreparations() []taskdependencies.Registration {
	registrations := service.lifecycle.AcceptedPreparations()
	return append(registrations, acceptedaction.Registration(service.accepted, "service-remove",
		workexecution.ActionRemove, ids.KindService, service.prepareAcceptedRemoval))
}

func (service *serviceMutationService) RemoveService(ctx context.Context, serviceID string,
	input apiTypes.TaskRunAfterRequest, key string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil || ids.Validate(ids.KindService, serviceID) != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Service removal input is invalid",
		)
	}
	target := idempotencyrecord.IdempotencyReplayTarget{
		Kind: idempotencyrecord.IdempotencyReplayTargetService,
		ID:   serviceID,
	}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx,
		target,
		http.MethodDelete,
		serviceEditRoute,
		key,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if !indexed {
		current, err := service.repository.GetService(ctx, serviceID)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		locator = idempotencyrecord.IdempotencyLocator{ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment,
			ScopeID: current.Record.EnvironmentID, Method: http.MethodDelete, Route: serviceEditRoute, Key: key}
	}
	intent := serviceMutationIntent{
		method:        http.MethodDelete,
		route:         serviceEditRoute,
		environmentID: locator.ScopeID,
		serviceID:     serviceID,
		body: requestidempotency.Object(
			requestidempotency.Field{Name: "after", Value: acceptedaction.AfterValue(input.After)},
		),
	}
	evidence, err := service.idempotency.Prepare(ctx, intent)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if existing {
		return serviceReplayResponse(resolution)
	}
	if indexed {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Service removal replay target is inconsistent",
		)
	}
	current, err := service.repository.GetService(ctx, serviceID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if err := service.rejectComponentGeneratedServiceMutation(ctx, current.Record); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	environment, project, err := service.serviceHierarchy(ctx, current.Record.EnvironmentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	at := service.now().UTC()
	task := workexecution.Task{
		ID:     ids.New(ids.KindTask),
		Action: workexecution.ActionRemove,
		Actor:  workexecution.OperatorActor,
		Scope: workexecution.Scope{
			TenantID:      project.Record.TenantID,
			ProjectID:     project.Record.ID,
			EnvironmentID: environment.Record.ID,
		},
		TargetID:   serviceID,
		TargetName: current.Record.Desired.Name,
		After:      append([]string(nil), input.After...),
		CreatedAt:  at,
		UpdatedAt:  at,
		Deadline:   at.Add(acceptedaction.QueueTimeout),
	}
	return acceptedaction.Submit(ctx, service.accepted, acceptedaction.Submission{
		Task: task, Kind: "service-remove", ReplayTarget: &target, Locator: locator, Intent: evidence.durable, Evidence: evidence.candidate},
		acceptedServiceRemoval{Locator: locator, Intent: evidence.durable})
}
