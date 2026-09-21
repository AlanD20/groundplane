package services

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	taskconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
	"time"
)

const (
	serviceStartRoute                     = "/services/{id}/start"
	serviceStopRoute                      = "/services/{id}/stop"
	serviceDestroyRoute                   = "/services/{id}/destroy"
	serviceLifecycleAgentTimeoutSeconds   = int64(120)
	serviceLifecycleControlTimeoutSeconds = int64(30)
	maximumServiceLifecycleAttempts       = 3
)

type serviceLifecyclePlanResolver interface {
	PrepareServiceLifecycleHookTask(
		context.Context,
		etcd.TaskRecord,
		releaserender.ServiceLifecycleRenderInput,
		*taskconfiguration.BackingHookEncryptedInputs,
		[]string,
	) (etcd.TaskRecord, error)
	PrepareServiceRemovalTask(
		context.Context,
		etcd.TaskRecord,
		etcd.ServiceRemovalIntent,
		string,
		string,
	) (etcd.TaskRecord, error)
}

type serviceLifecycleService struct {
	repository  serviceLifecycleRepository
	plans       serviceLifecyclePlanResolver
	idempotency serviceLifecycleIdempotency
	hookInputs  serviceLifecycleHookInputs
	now         func() time.Time
}

func NewLifecycleService(
	repository serviceLifecycleRepository,
	plans serviceLifecyclePlanResolver,
	idempotency serviceLifecycleIdempotency,
	hookInputs serviceLifecycleHookInputs,
) (*serviceLifecycleService, error) {
	if repository == nil || plans == nil || idempotency == nil || hookInputs == nil {
		return nil, errs.New(errs.KindInternal, "Service lifecycle service is not configured")
	}
	return &serviceLifecycleService{
		repository: repository, plans: plans, idempotency: idempotency, hookInputs: hookInputs, now: time.Now,
	}, nil
}

func (service *serviceMutationService) StartService(
	ctx context.Context,
	serviceID string,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	return service.runServiceLifecycle(ctx, serviceID, idempotencyKey, taskjournal.TaskStart)
}

func (service *serviceMutationService) StopService(
	ctx context.Context,
	serviceID string,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	return service.runServiceLifecycle(ctx, serviceID, idempotencyKey, taskjournal.TaskStop)
}

func (service *serviceMutationService) DestroyService(
	ctx context.Context,
	serviceID string,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	return service.runServiceLifecycle(ctx, serviceID, idempotencyKey, taskjournal.TaskDestroy)
}

func (service *serviceMutationService) runServiceLifecycle(
	ctx context.Context,
	serviceID string,
	idempotencyKey string,
	taskType taskjournal.TaskType,
) (idempotencyrecord.IdempotencyResponse, error) {
	if service == nil || service.lifecycle == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Service lifecycle is not configured")
	}
	return service.lifecycle.Run(ctx, serviceID, idempotencyKey, taskType)
}

func (service *serviceLifecycleService) Run(
	ctx context.Context,
	serviceID string,
	idempotencyKey string,
	taskType taskjournal.TaskType,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Service lifecycle context is required")
	}
	if ids.Validate(ids.KindService, serviceID) != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Service lifecycle requires a stable Service id",
		)
	}
	for attempt := 0; attempt < maximumServiceLifecycleAttempts; attempt++ {
		response, err := service.runOnce(ctx, serviceID, idempotencyKey, taskType)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumServiceLifecycleAttempts-1 {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Service lifecycle retry bound was not enforced")
}

func (service *serviceLifecycleService) runOnce(
	ctx context.Context,
	serviceID string,
	idempotencyKey string,
	taskType taskjournal.TaskType,
) (idempotencyrecord.IdempotencyResponse, error) {
	route, intent, err := serviceLifecycleContract(taskType)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	target := idempotencyrecord.IdempotencyReplayTarget{Kind: idempotencyrecord.IdempotencyReplayTargetService, ID: serviceID}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx, target, http.MethodPost, route, idempotencyKey,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if indexed {
		evidence, prepareErr := service.idempotency.Prepare(ctx, locator, serviceID, taskType, route)
		if prepareErr != nil {
			return idempotencyrecord.IdempotencyResponse{}, prepareErr
		}
		defer clear(evidence.durable.Ciphertext)
		resolution, existing, resolveErr := service.idempotency.ResolveExisting(ctx, locator, evidence)
		if resolveErr != nil {
			return idempotencyrecord.IdempotencyResponse{}, resolveErr
		}
		if !existing || resolution.Kind != requestidempotency.ResolutionReplay {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Service lifecycle replay target is inconsistent",
			)
		}
		return requestidempotency.CloneResponse(resolution.Response), nil
	}

	current, err := service.repository.GetService(ctx, serviceID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	locator = idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment, ScopeID: current.Record.EnvironmentID,
		Method: http.MethodPost, Route: route, Key: idempotencyKey,
	}
	evidence, err := service.idempotency.Prepare(ctx, locator, serviceID, taskType, route)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != requestidempotency.ResolutionReplay {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Service lifecycle replay resolution is invalid",
			)
		}
		return requestidempotency.CloneResponse(resolution.Response), nil
	}

	environment, err := service.repository.GetEnvironment(ctx, current.Record.EnvironmentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	var tenant *etcdstore.Versioned[hierarchyrecord.TenantRecord]
	switch project.Record.Kind {
	case hierarchyrecord.ProjectKindTenant:
		if project.Record.TenantID == "" {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Service Project Tenant is missing")
		}
		currentTenant, getErr := service.repository.GetTenant(ctx, project.Record.TenantID)
		if getErr != nil {
			return idempotencyrecord.IdempotencyResponse{}, getErr
		}
		tenant = &currentTenant
	case hierarchyrecord.ProjectKindBacking:
		if project.Record.TenantID != "" {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Backing Project has a Tenant")
		}
	default:
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindStateConflict, "Service Project kind is invalid")
	}
	taskOwner, err := taskjournal.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	projection, hasProjection, err := service.repository.GetEnvironmentAppliedComposeProjection(
		ctx,
		environment.Record.ID,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	applied := hasProjection && serviceInComposeProjection(projection.Record, serviceID)
	var projectionInput *etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]
	replacement, err := servicerecord.SetServiceRuntimeIntent(current.Record, intent)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}

	now := service.now().UTC()
	task := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation), IdempotencyKey: idempotencyKey,
		Owner: taskOwner, Actor: taskjournal.TaskActorOperator,
		PlanID: ids.New(ids.KindPlan), Type: taskType, Target: serviceID,
		Status: taskjournal.TaskStatusPending, NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	var renderInput *releaserender.ServiceLifecycleRenderInput
	var sealedHookInputs *taskconfiguration.BackingHookEncryptedInputs
	defer func() {
		if sealedHookInputs != nil {
			clear(sealedHookInputs.Ciphertext)
		}
	}()
	if applied {
		projectionInput = &projection
		var input releaserender.ServiceLifecycleRenderInput
		task, input, sealedHookInputs, err = service.prepareAppliedServiceLifecycle(
			ctx, taskType, current, tenant, project, environment, projection, task,
		)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		renderInput = &input
	} else {
		task, err = prepareControllerServiceLifecycleTask(task, environment.Record.ID, current.Revision)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}

	responseBody, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := idempotencyrecord.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: append([]byte(nil), responseBody...),
	}
	marker := idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
		Locator: locator, ReplayTarget: &target, Intent: evidence.durable, Response: response,
		TaskID: task.ID, CreatedAt: now, UpdatedAt: now,
	}
	result, mutationErr := service.repository.BeginServiceLifecycleWithTaskHookInputs(
		ctx, tenant, project, environment, current, replacement, projectionInput, renderInput,
		sealedHookInputs, task, marker,
	)
	if mutationErr != nil {
		if !isUnknownServiceLifecycleMutationOutcome(mutationErr) {
			return idempotencyrecord.IdempotencyResponse{}, mutationErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, mutationErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case requestidempotency.ResolutionApplied:
		return requestidempotency.CloneResponse(response), nil
	case requestidempotency.ResolutionReplay:
		return requestidempotency.CloneResponse(resolution.Response), nil
	default:
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Service lifecycle resolution is invalid")
	}
}

func isUnknownServiceLifecycleMutationOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
