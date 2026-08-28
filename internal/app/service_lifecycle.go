package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	serviceStartRoute                     = "/services/{id}/start"
	serviceStopRoute                      = "/services/{id}/stop"
	serviceDestroyRoute                   = "/services/{id}/destroy"
	serviceLifecycleAgentTimeoutSeconds   = int64(120)
	serviceLifecycleControlTimeoutSeconds = int64(30)
	maximumServiceLifecycleAttempts       = 3
)

type serviceLifecycleRepository interface {
	GetTenant(context.Context, string) (etcd.Versioned[etcd.TenantRecord], error)
	GetProject(context.Context, string) (etcd.Versioned[etcd.ProjectRecord], error)
	GetEnvironment(context.Context, string) (etcd.Versioned[etcd.EnvironmentRecord], error)
	GetService(context.Context, string) (etcd.Versioned[etcd.ServiceRecord], error)
	GetEnvironmentComposeProjection(
		context.Context,
		string,
	) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error)
	BeginServiceLifecycleWithTask(
		context.Context,
		etcd.Versioned[etcd.TenantRecord],
		etcd.Versioned[etcd.ProjectRecord],
		etcd.Versioned[etcd.EnvironmentRecord],
		etcd.Versioned[etcd.ServiceRecord],
		etcd.ServiceRecord,
		*etcd.Versioned[etcd.EnvironmentComposeProjection],
		*etcd.ServiceLifecycleRenderInput,
		etcd.TaskRecord,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

func (repository *durableServiceMutationRepository) GetTenant(
	ctx context.Context,
	tenantID string,
) (etcd.Versioned[etcd.TenantRecord], error) {
	return repository.hierarchy.GetTenant(ctx, tenantID)
}

func (repository *durableServiceMutationRepository) GetEnvironmentComposeProjection(
	ctx context.Context,
	environmentID string,
) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
	return repository.hierarchy.GetEnvironmentComposeProjection(ctx, environmentID)
}

func (repository *durableServiceMutationRepository) BeginServiceLifecycleWithTask(
	ctx context.Context,
	tenant etcd.Versioned[etcd.TenantRecord],
	project etcd.Versioned[etcd.ProjectRecord],
	environment etcd.Versioned[etcd.EnvironmentRecord],
	current etcd.Versioned[etcd.ServiceRecord],
	replacement etcd.ServiceRecord,
	projection *etcd.Versioned[etcd.EnvironmentComposeProjection],
	renderInput *etcd.ServiceLifecycleRenderInput,
	task etcd.TaskRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.services.BeginServiceLifecycleWithTask(
		ctx, tenant, project, environment, current, replacement, projection, renderInput, task, marker,
	)
}

type serviceLifecycleEvidence struct {
	candidate idempotentintent.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type serviceLifecycleIdempotency interface {
	ResolveReplayLocator(
		context.Context,
		etcd.IdempotencyReplayTarget,
		string,
		string,
		string,
	) (etcd.IdempotencyLocator, bool, error)
	Prepare(
		context.Context,
		etcd.IdempotencyLocator,
		string,
		etcd.TaskType,
		string,
	) (serviceLifecycleEvidence, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		serviceLifecycleEvidence,
	) (idempotentintent.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		serviceLifecycleEvidence,
		etcd.IdempotencyTransactionResult,
	) (idempotentintent.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		serviceLifecycleEvidence,
		error,
	) (idempotentintent.Resolution, error)
}

type durableServiceLifecycleIdempotency struct {
	coordinator *idempotentintent.Coordinator
	repository  *etcd.IdempotencyRepository
}

func newDurableServiceLifecycleIdempotency(
	coordinator *idempotentintent.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableServiceLifecycleIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Service lifecycle idempotency is not configured")
	}
	return &durableServiceLifecycleIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableServiceLifecycleIdempotency) ResolveReplayLocator(
	ctx context.Context,
	target etcd.IdempotencyReplayTarget,
	method string,
	route string,
	key string,
) (etcd.IdempotencyLocator, bool, error) {
	return service.repository.ResolveReplayLocator(ctx, target, method, route, key)
}

func (service *durableServiceLifecycleIdempotency) Prepare(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	serviceID string,
	taskType etcd.TaskType,
	route string,
) (serviceLifecycleEvidence, error) {
	if locator.ScopeKind != etcd.IdempotencyScopeEnvironment ||
		ids.Validate(ids.KindEnvironment, locator.ScopeID) != nil {
		return serviceLifecycleEvidence{}, errs.New(errs.KindInternal, "Service lifecycle replay scope is invalid")
	}
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: http.MethodPost,
		Route:  route,
		Scope:  idempotentintent.Scope{Kind: idempotentintent.ScopeEnvironment, ID: locator.ScopeID},
		Path:   []idempotentintent.PathBinding{{Name: "id", Value: serviceID}},
		Query:  idempotentintent.Object(),
		Body:   idempotentintent.NoBody(),
	})
	if err != nil {
		return serviceLifecycleEvidence{}, err
	}
	defer digest.Destroy()
	if taskType != etcd.TaskStart && taskType != etcd.TaskStop && taskType != etcd.TaskDestroy {
		return serviceLifecycleEvidence{}, errs.New(errs.KindInternal, "Service lifecycle intent type is invalid")
	}
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return serviceLifecycleEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return serviceLifecycleEvidence{}, err
	}
	return serviceLifecycleEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableServiceLifecycleIdempotency) ResolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence serviceLifecycleEvidence,
) (idempotentintent.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableServiceLifecycleIdempotency) ResolveKnown(
	ctx context.Context,
	evidence serviceLifecycleEvidence,
	result etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableServiceLifecycleIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence serviceLifecycleEvidence,
	original error,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type serviceLifecyclePlanResolver interface {
	PrepareServiceLifecycleTask(
		context.Context,
		etcd.TaskRecord,
		etcd.ServiceLifecycleRenderInput,
		string,
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
	now         func() time.Time
}

func newServiceLifecycleService(
	repository serviceLifecycleRepository,
	plans serviceLifecyclePlanResolver,
	idempotency serviceLifecycleIdempotency,
) (*serviceLifecycleService, error) {
	if repository == nil || plans == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Service lifecycle service is not configured")
	}
	return &serviceLifecycleService{
		repository: repository, plans: plans, idempotency: idempotency, now: time.Now,
	}, nil
}

func (service *serviceMutationService) StartService(
	ctx context.Context,
	serviceID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	return service.runServiceLifecycle(ctx, serviceID, idempotencyKey, etcd.TaskStart)
}

func (service *serviceMutationService) StopService(
	ctx context.Context,
	serviceID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	return service.runServiceLifecycle(ctx, serviceID, idempotencyKey, etcd.TaskStop)
}

func (service *serviceMutationService) DestroyService(
	ctx context.Context,
	serviceID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	return service.runServiceLifecycle(ctx, serviceID, idempotencyKey, etcd.TaskDestroy)
}

func (service *serviceMutationService) runServiceLifecycle(
	ctx context.Context,
	serviceID string,
	idempotencyKey string,
	taskType etcd.TaskType,
) (etcd.IdempotencyResponse, error) {
	if service == nil || service.lifecycle == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Service lifecycle is not configured")
	}
	return service.lifecycle.Run(ctx, serviceID, idempotencyKey, taskType)
}

func (service *serviceLifecycleService) Run(
	ctx context.Context,
	serviceID string,
	idempotencyKey string,
	taskType etcd.TaskType,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Service lifecycle context is required")
	}
	if ids.Validate(ids.KindService, serviceID) != nil {
		return etcd.IdempotencyResponse{}, errs.New(
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
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Service lifecycle retry bound was not enforced")
}

func (service *serviceLifecycleService) runOnce(
	ctx context.Context,
	serviceID string,
	idempotencyKey string,
	taskType etcd.TaskType,
) (etcd.IdempotencyResponse, error) {
	route, intent, err := serviceLifecycleContract(taskType)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	target := etcd.IdempotencyReplayTarget{Kind: etcd.IdempotencyReplayTargetService, ID: serviceID}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx, target, http.MethodPost, route, idempotencyKey,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if indexed {
		evidence, prepareErr := service.idempotency.Prepare(ctx, locator, serviceID, taskType, route)
		if prepareErr != nil {
			return etcd.IdempotencyResponse{}, prepareErr
		}
		defer clear(evidence.durable.Ciphertext)
		resolution, existing, resolveErr := service.idempotency.ResolveExisting(ctx, locator, evidence)
		if resolveErr != nil {
			return etcd.IdempotencyResponse{}, resolveErr
		}
		if !existing || resolution.Kind != idempotentintent.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Service lifecycle replay target is inconsistent",
			)
		}
		return cloneIdempotencyResponse(resolution.Response), nil
	}

	current, err := service.repository.GetService(ctx, serviceID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	locator = etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: current.Record.EnvironmentID,
		Method: http.MethodPost, Route: route, Key: idempotencyKey,
	}
	evidence, err := service.idempotency.Prepare(ctx, locator, serviceID, taskType, route)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != idempotentintent.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Service lifecycle replay resolution is invalid",
			)
		}
		return cloneIdempotencyResponse(resolution.Response), nil
	}

	environment, err := service.repository.GetEnvironment(ctx, current.Record.EnvironmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if project.Record.Kind != etcd.ProjectKindTenant || project.Record.TenantID == "" {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict,
			"backing Service lifecycle requires the backing runtime slice",
		)
	}
	tenant, err := service.repository.GetTenant(ctx, project.Record.TenantID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	taskOwner, err := etcd.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	projection, hasProjection, err := service.repository.GetEnvironmentComposeProjection(ctx, environment.Record.ID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	var projectionInput *etcd.Versioned[etcd.EnvironmentComposeProjection]
	if hasProjection {
		projectionInput = &projection
	}
	replacement, err := etcd.SetServiceRuntimeIntent(current.Record, intent)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}

	now := service.now().UTC()
	task := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation), IdempotencyKey: idempotencyKey,
		Owner: taskOwner, Actor: etcd.TaskActorOperator,
		PlanID: ids.New(ids.KindPlan), Type: taskType, Target: serviceID,
		Status: etcd.TaskStatusPending, NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	var renderInput *etcd.ServiceLifecycleRenderInput
	if hasProjection && serviceInComposeProjection(projection.Record, serviceID) {
		input := etcd.ServiceLifecycleRenderInput{
			PlanID: task.PlanID, ServiceID: serviceID,
			TenantID: tenant.Record.ID, TenantSlug: tenant.Record.Slug,
			ProjectID: project.Record.ID, ProjectSlug: project.Record.Slug,
			EnvironmentID: environment.Record.ID, EnvironmentName: environment.Record.Name,
			AuthorizedVolumeDir: environment.Record.VolumeDir,
			ArtifactID:          ids.New(ids.KindConfig),
			Projection:          projection.Record,
		}
		task.Executor = etcd.TaskExecutorAgent
		task.TimeoutSeconds = serviceLifecycleAgentTimeoutSeconds
		task, err = service.plans.PrepareServiceLifecycleTask(
			ctx, task, input, ids.New(ids.KindStep),
		)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
		renderInput = &input
	} else {
		task, err = prepareControllerServiceLifecycleTask(task, environment.Record.ID, current.Revision)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
	}

	responseBody, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: append([]byte(nil), responseBody...),
	}
	marker := etcd.IdempotencyMarker{
		Kind: etcd.IdempotencyMarkerTask, State: etcd.IdempotencyMarkerPending,
		Locator: locator, ReplayTarget: &target, Intent: evidence.durable, Response: response,
		TaskID: task.ID, CreatedAt: now, UpdatedAt: now,
	}
	result, mutationErr := service.repository.BeginServiceLifecycleWithTask(
		ctx, tenant, project, environment, current, replacement, projectionInput, renderInput, task, marker,
	)
	if mutationErr != nil {
		if !isUnknownServiceLifecycleMutationOutcome(mutationErr) {
			return etcd.IdempotencyResponse{}, mutationErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, mutationErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case idempotentintent.ResolutionApplied:
		return cloneIdempotencyResponse(response), nil
	case idempotentintent.ResolutionReplay:
		return cloneIdempotencyResponse(resolution.Response), nil
	default:
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Service lifecycle resolution is invalid")
	}
}

func isUnknownServiceLifecycleMutationOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}

func serviceLifecycleContract(taskType etcd.TaskType) (string, core.ServiceRuntimeIntent, error) {
	switch taskType {
	case etcd.TaskStart:
		return serviceStartRoute, core.ServiceRuntimeIntentRunning, nil
	case etcd.TaskStop:
		return serviceStopRoute, core.ServiceRuntimeIntentStopped, nil
	case etcd.TaskDestroy:
		return serviceDestroyRoute, core.ServiceRuntimeIntentAbsent, nil
	default:
		return "", "", errs.New(errs.KindValidationFailed, "Service lifecycle action is invalid")
	}
}

func serviceInComposeProjection(projection etcd.EnvironmentComposeProjection, serviceID string) bool {
	for _, identity := range projection.Services {
		if identity.ID == serviceID {
			return true
		}
	}
	return false
}

func prepareControllerServiceLifecycleTask(
	task etcd.TaskRecord,
	environmentID string,
	serviceRevision int64,
) (etcd.TaskRecord, error) {
	if serviceRevision <= 0 || ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return etcd.TaskRecord{}, errs.New(errs.KindValidationFailed, "Controller Service lifecycle input is invalid")
	}
	task.Executor = etcd.TaskExecutorController
	task.RenderGeneration = 1
	task.Params = map[string]string{
		etcd.TaskResourceKindParam:       etcd.TaskResourceService,
		etcd.TaskServiceEnvironmentParam: environmentID,
	}
	task.Steps = []etcd.TaskStepRecord{{ID: ids.New(ids.KindStep)}}
	task.TimeoutSeconds = serviceLifecycleControlTimeoutSeconds
	value, err := json.Marshal(struct {
		Version         int           `json:"version"`
		PlanID          string        `json:"plan_id"`
		Type            etcd.TaskType `json:"type"`
		ServiceID       string        `json:"service_id"`
		EnvironmentID   string        `json:"environment_id"`
		ServiceRevision int64         `json:"service_revision"`
	}{
		Version: 1, PlanID: task.PlanID, Type: task.Type, ServiceID: task.Target,
		EnvironmentID: environmentID, ServiceRevision: serviceRevision,
	})
	if err != nil {
		return etcd.TaskRecord{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(value)
	digest := sha256.Sum256(value)
	task.PlanHash = hex.EncodeToString(digest[:])
	return task, nil
}
