package services

import (
	"context"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	serviceStartRoute                     = "/services/{id}/start"
	serviceStopRoute                      = "/services/{id}/stop"
	serviceDestroyRoute                   = "/services/{id}/destroy"
	serviceLifecycleAgentTimeoutSeconds   = int64(120)
	serviceLifecycleControlTimeoutSeconds = int64(30)
)

type serviceLifecycleService struct {
	repository  serviceLifecycleRepository
	idempotency serviceLifecycleIdempotency
	hookInputs  serviceLifecycleHookInputs
	accepted    *acceptedaction.Submitter
	now         func() time.Time
}

func NewLifecycleService(repository serviceLifecycleRepository, idempotency serviceLifecycleIdempotency,
	hookInputs serviceLifecycleHookInputs, accepted *acceptedaction.Submitter,
) (*serviceLifecycleService, error) {
	if repository == nil || idempotency == nil || hookInputs == nil || accepted == nil {
		return nil, errs.New(errs.KindInternal, "Service lifecycle service is not configured")
	}
	return &serviceLifecycleService{
		repository:  repository,
		idempotency: idempotency,
		hookInputs:  hookInputs,
		accepted:    accepted,
		now:         time.Now,
	}, nil
}

func (service *serviceMutationService) StartService(ctx context.Context, serviceID string,
	input apiTypes.TaskRunAfterRequest, key string,
) (idempotencyrecord.IdempotencyResponse, error) {
	return service.runServiceLifecycle(ctx, serviceID, input, key, workexecution.ActionStart)
}
func (service *serviceMutationService) StopService(ctx context.Context, serviceID string,
	input apiTypes.TaskRunAfterRequest, key string,
) (idempotencyrecord.IdempotencyResponse, error) {
	return service.runServiceLifecycle(ctx, serviceID, input, key, workexecution.ActionStop)
}
func (service *serviceMutationService) DestroyService(ctx context.Context, serviceID string,
	input apiTypes.TaskRunAfterRequest, key string,
) (idempotencyrecord.IdempotencyResponse, error) {
	return service.runServiceLifecycle(ctx, serviceID, input, key, workexecution.ActionDestroy)
}
func (service *serviceMutationService) runServiceLifecycle(ctx context.Context, serviceID string,
	input apiTypes.TaskRunAfterRequest, key string, action workexecution.Action,
) (idempotencyrecord.IdempotencyResponse, error) {
	if service == nil || service.lifecycle == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Service lifecycle is not configured",
		)
	}
	return service.lifecycle.Run(ctx, serviceID, input, key, action)
}

func (service *serviceLifecycleService) Run(ctx context.Context, serviceID string, input apiTypes.TaskRunAfterRequest,
	key string, action workexecution.Action,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil || ids.Validate(ids.KindService, serviceID) != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Service lifecycle requires a stable Service id and context",
		)
	}
	route, _, err := serviceLifecycleContract(action)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	target := idempotencyrecord.IdempotencyReplayTarget{
		Kind: idempotencyrecord.IdempotencyReplayTargetService,
		ID:   serviceID,
	}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(ctx, target, http.MethodPost, route, key)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if !indexed {
		current, err := service.repository.GetService(ctx, serviceID)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		locator = idempotencyrecord.IdempotencyLocator{ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment,
			ScopeID: current.Record.EnvironmentID, Method: http.MethodPost, Route: route, Key: key}
	}
	evidence, err := service.idempotency.Prepare(ctx, locator, serviceID, action, route, input.After)
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
			"Service lifecycle replay target is inconsistent",
		)
	}
	// Only identity and ancestry are needed before predecessors complete.
	current, err := service.repository.GetService(ctx, serviceID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	environment, err := service.repository.GetEnvironment(ctx, current.Record.EnvironmentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	now := service.now().UTC()
	task := workexecution.Task{ID: ids.New(ids.KindTask), Actor: workexecution.OperatorActor, Action: action,
		Scope: workexecution.Scope{
			TenantID:      project.Record.TenantID,
			ProjectID:     project.Record.ID,
			EnvironmentID: environment.Record.ID,
		},
		TargetID: serviceID, TargetName: current.Record.Desired.Name, After: append([]string(nil), input.After...),
		CreatedAt: now, UpdatedAt: now, Deadline: now.Add(acceptedaction.QueueTimeout)}
	return acceptedaction.Submit(ctx, service.accepted, acceptedaction.Submission{
		Task: task, Kind: "service-" + string(action), ReplayTarget: &target, Locator: locator, Intent: evidence.durable, Evidence: evidence.candidate}, input)
}
