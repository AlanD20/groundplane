package environment

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/netip"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	environmentCreationRoute           = "/environments"
	environmentCreationTimeoutSeconds  = int64(120)
	maximumEnvironmentCreationAttempts = 3
)

type environmentCreationRepository interface {
	GetProject(context.Context, string) (etcd.Versioned[etcd.ProjectRecord], error)
	GetEnvironmentPoolRegistry(context.Context) (etcd.Versioned[etcd.EnvironmentPoolRegistry], error)
	CreateEnvironmentWithTask(
		context.Context,
		string,
		etcd.Versioned[etcd.ProjectRecord],
		etcd.Versioned[etcd.EnvironmentPoolRegistry],
		etcd.EnvironmentRecord,
		[]etcd.ComponentRecord,
		etcd.TaskRecord,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type environmentCreationEvidence struct {
	candidate idempotentintent.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type environmentCreationIdempotency interface {
	Prepare(context.Context, CreateEnvironmentInput) (environmentCreationEvidence, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		environmentCreationEvidence,
	) (idempotentintent.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		environmentCreationEvidence,
		etcd.IdempotencyTransactionResult,
	) (idempotentintent.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		environmentCreationEvidence,
		error,
	) (idempotentintent.Resolution, error)
}

type durableEnvironmentCreationIdempotency struct {
	coordinator *idempotentintent.Coordinator
	repository  *etcd.IdempotencyRepository
}

func NewDurableCreationIdempotency(
	coordinator *idempotentintent.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableEnvironmentCreationIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Environment creation idempotency is not configured")
	}
	return &durableEnvironmentCreationIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableEnvironmentCreationIdempotency) Prepare(
	ctx context.Context,
	input CreateEnvironmentInput,
) (environmentCreationEvidence, error) {
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: http.MethodPost, Route: environmentCreationRoute,
		Scope: idempotentintent.Scope{Kind: idempotentintent.ScopeProject, ID: input.ProjectID},
		Query: idempotentintent.Object(),
		Body: idempotentintent.JSONBody(idempotentintent.Object(
			idempotentintent.Field{Name: "name", Value: idempotentintent.String(input.Name)},
			idempotentintent.Field{Name: "network_pool", Value: idempotentintent.String(input.NetworkPool)},
			idempotentintent.Field{Name: "project_id", Value: idempotentintent.String(input.ProjectID)},
		)),
	})
	if err != nil {
		return environmentCreationEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return environmentCreationEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return environmentCreationEvidence{}, err
	}
	return environmentCreationEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableEnvironmentCreationIdempotency) ResolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence environmentCreationEvidence,
) (idempotentintent.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableEnvironmentCreationIdempotency) ResolveKnown(
	ctx context.Context,
	evidence environmentCreationEvidence,
	result etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableEnvironmentCreationIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence environmentCreationEvidence,
	original error,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type environmentCreationService struct {
	volumeRoot      string
	environmentPool netip.Prefix
	repository      environmentCreationRepository
	idempotency     environmentCreationIdempotency
	now             func() time.Time
}

func NewCreationService(
	volumeRoot string,
	environmentPool string,
	repository environmentCreationRepository,
	idempotency environmentCreationIdempotency,
) (*environmentCreationService, error) {
	if repository == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Environment creation service is not configured")
	}
	if err := environmentpath.ValidateRoot(volumeRoot); err != nil {
		return nil, err
	}
	root, err := ipam.ParseIPv4Prefix(environmentPool)
	if err != nil {
		return nil, errs.New(errs.KindValidationFailed, "Controller environment pool is invalid")
	}
	return &environmentCreationService{
		volumeRoot: volumeRoot, environmentPool: root,
		repository: repository, idempotency: idempotency, now: time.Now,
	}, nil
}

func (service *environmentCreationService) CreateEnvironment(
	ctx context.Context,
	input CreateEnvironmentInput,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Environment creation context is required")
	}
	for attempt := 0; attempt < maximumEnvironmentCreationAttempts; attempt++ {
		response, err := service.createEnvironmentOnce(ctx, input, idempotencyKey)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumEnvironmentCreationAttempts-1 {
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Environment creation retry bound was not enforced")
}

func (service *environmentCreationService) createEnvironmentOnce(
	ctx context.Context,
	input CreateEnvironmentInput,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if err := ValidateEnvironmentCreateInput(input); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	evidence, err := service.idempotency.Prepare(ctx, input)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeProject, ScopeID: input.ProjectID,
		Method: http.MethodPost, Route: environmentCreationRoute, Key: idempotencyKey,
	}
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != idempotentintent.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Environment creation replay resolution is invalid",
			)
		}
		return cloneIdempotencyResponse(resolution.Response), nil
	}
	project, err := service.repository.GetProject(ctx, input.ProjectID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if project.Record.ID != input.ProjectID || project.Record.Kind != etcd.ProjectKindTenant {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindProjectNotFound, "project was not found")
	}
	poolRegistry, err := service.repository.GetEnvironmentPoolRegistry(ctx)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	now := service.now().UTC()
	taskID := ids.New(ids.KindTask)
	environmentID := ids.New(ids.KindEnvironment)
	nextPoolRegistry, networkPool, err := poolRegistry.Record.Reserve(
		service.environmentPool,
		environmentID,
		input.NetworkPool,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	poolRegistry.Record = nextPoolRegistry
	environment, err := etcd.NewProvisioningEnvironment(
		service.volumeRoot,
		project.Record,
		environmentID,
		input.Name,
		networkPool,
		taskID,
		now,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	components, err := newInitialEnvironmentComponents(environment.ID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	task, err := newEnvironmentCreationTask(
		project.Record,
		environment,
		taskID,
		idempotencyKey,
		now,
		service.volumeRoot,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	responseBody, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json",
		Body: append([]byte(nil), responseBody...),
	}
	marker := etcd.IdempotencyMarker{
		Kind: etcd.IdempotencyMarkerTask, State: etcd.IdempotencyMarkerPending,
		Locator: locator, Intent: evidence.durable, Response: response,
		TaskID: task.ID, CreatedAt: now, UpdatedAt: now,
	}
	result, createErr := service.repository.CreateEnvironmentWithTask(
		ctx,
		service.volumeRoot,
		project,
		poolRegistry,
		environment,
		components,
		task,
		marker,
	)
	if createErr != nil {
		if !isUnknownEnvironmentCreationOutcome(createErr) {
			return etcd.IdempotencyResponse{}, createErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, createErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if resolution.Kind == idempotentintent.ResolutionReplay {
		return cloneIdempotencyResponse(resolution.Response), nil
	}
	if resolution.Kind != idempotentintent.ResolutionApplied {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Environment creation resolution is invalid")
	}
	return cloneIdempotencyResponse(response), nil
}

func newInitialEnvironmentComponents(environmentID string) ([]etcd.ComponentRecord, error) {
	components := make([]etcd.ComponentRecord, 0, 2)
	for _, kind := range []core.ComponentKind{
		core.ComponentKindIngressCaddy,
		core.ComponentKindEdgeCloudflare,
	} {
		record, err := etcd.NewComponentRecord(core.Component{
			ID:      ids.New(ids.KindComponent),
			Owner:   core.ComponentOwnerEnvironment,
			OwnerID: environmentID,
			Kind:    kind,
			Enabled: false,
		})
		if err != nil {
			return nil, err
		}
		components = append(components, record)
	}
	return components, nil
}

func newEnvironmentCreationTask(
	project etcd.ProjectRecord,
	environment etcd.EnvironmentRecord,
	taskID string,
	idempotencyKey string,
	createdAt time.Time,
	volumeRoot string,
) (etcd.TaskRecord, error) {
	owner, err := etcd.EnvironmentTaskOwner(project, environment)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	planID := ids.New(ids.KindPlan)
	stepID := ids.New(ids.KindStep)
	plan, err := executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: 1, PlanId: planID, RenderGeneration: 1,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE,
		TargetId:  environment.ID,
		Steps: []*agentpb.ExecutionStep{{
			StepId: stepID, TimeoutSeconds: uint32(environmentCreationTimeoutSeconds),
			Payload: &agentpb.ExecutionStep_EnvironmentDirectoryCreate{
				EnvironmentDirectoryCreate: &agentpb.EnvironmentDirectoryCreate{
					EnvironmentId: environment.ID, ExpectedVolumeDir: environment.VolumeDir,
				},
			},
		}},
	})
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	if err := executionplan.AuthorizeVolumeDirectories(plan, volumeRoot); err != nil {
		return etcd.TaskRecord{}, err
	}
	return etcd.TaskRecord{
		ID: taskID, OperationID: ids.New(ids.KindOperation),
		Owner: owner, Actor: etcd.TaskActorOperator,
		IdempotencyKey: idempotencyKey, Executor: etcd.TaskExecutorAgent,
		PlanID: planID, PlanHash: hex.EncodeToString(plan.PlanHash), RenderGeneration: 1,
		Type: etcd.TaskCreate, Target: environment.ID,
		Params: map[string]string{
			taskcontract.EnvironmentCreateVolumeDirectoryParam: environment.VolumeDir,
		},
		Steps:          []etcd.TaskStepRecord{{Kind: etcd.TaskStepOperation, ID: stepID}},
		TimeoutSeconds: environmentCreationTimeoutSeconds,
		Status:         etcd.TaskStatusPending, NextEventSequence: 1, CreatedAt: createdAt, UpdatedAt: createdAt,
	}, nil
}

func isUnknownEnvironmentCreationOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
