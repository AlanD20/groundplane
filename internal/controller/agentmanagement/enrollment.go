package agentmanagement

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	agentEnrollmentRoute          = "/agents"
	agentEnrollmentTimeoutSeconds = int64(120)
)

type agentEnrollmentTaskRepository interface {
	CreateTask(
		context.Context,
		etcd.TaskRecord,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type agentEnrollmentEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type agentEnrollmentIdempotency interface {
	Prepare(context.Context) (agentEnrollmentEvidence, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		agentEnrollmentEvidence,
	) (requestidempotency.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		agentEnrollmentEvidence,
		etcd.IdempotencyTransactionResult,
	) (requestidempotency.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		agentEnrollmentEvidence,
		error,
	) (requestidempotency.Resolution, error)
}

type durableAgentEnrollmentIdempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  *etcd.IdempotencyRepository
}

func NewEnrollmentIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableAgentEnrollmentIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Agent enrollment idempotency is not configured")
	}
	return &durableAgentEnrollmentIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableAgentEnrollmentIdempotency) Prepare(
	ctx context.Context,
) (agentEnrollmentEvidence, error) {
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost,
		Route:  agentEnrollmentRoute,
		Scope:  requestidempotency.Scope{Kind: requestidempotency.ScopePlatform},
		Query:  requestidempotency.Object(),
		Body:   requestidempotency.NoBody(),
	})
	if err != nil {
		return agentEnrollmentEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return agentEnrollmentEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return agentEnrollmentEvidence{}, err
	}
	return agentEnrollmentEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableAgentEnrollmentIdempotency) ResolveKnown(
	ctx context.Context,
	evidence agentEnrollmentEvidence,
	result etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableAgentEnrollmentIdempotency) ResolveExisting(
	ctx context.Context, locator etcd.IdempotencyLocator, evidence agentEnrollmentEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableAgentEnrollmentIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence agentEnrollmentEvidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(
		ctx,
		service.repository,
		locator,
		evidence.candidate,
		original,
	)
}

type agentImageSource interface {
	DesiredAgentImage(context.Context) (string, error)
}

type agentEnrollmentService struct {
	images      agentImageSource
	config      localagent.Config
	tasks       agentEnrollmentTaskRepository
	idempotency agentEnrollmentIdempotency
	now         func() time.Time
}

func NewEnrollmentService(
	images agentImageSource,
	config localagent.Config,
	tasks agentEnrollmentTaskRepository,
	idempotency agentEnrollmentIdempotency,
) (*agentEnrollmentService, error) {
	if images == nil || tasks == nil || idempotency == nil || config.PullIntervalSeconds <= 0 ||
		config.MaxConcurrentTasks <= 0 {
		return nil, errs.New(errs.KindInternal, "Agent enrollment service dependencies are invalid")
	}
	return &agentEnrollmentService{
		images: images,
		config: localagent.Config{
			PullIntervalSeconds: config.PullIntervalSeconds,
			MaxConcurrentTasks:  config.MaxConcurrentTasks,
			Labels:              maps.Clone(config.Labels),
		},
		tasks: tasks, idempotency: idempotency, now: time.Now,
	}, nil
}

func (service *agentEnrollmentService) EnrollAgent(
	ctx context.Context,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Agent enrollment context is required")
	}
	evidence, err := service.idempotency.Prepare(ctx)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopePlatform, ScopeID: "-",
		Method: http.MethodPost, Route: agentEnrollmentRoute, Key: idempotencyKey,
	}
	existing, found, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if found {
		if existing.Kind != requestidempotency.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Agent enrollment replay resolution is invalid",
			)
		}
		return requestidempotency.CloneResponse(existing.Response), nil
	}
	image, err := service.images.DesiredAgentImage(ctx)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if !imageref.IsDigestPinned(image) {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Agent enrollment requires a selected digest-pinned image",
		)
	}
	now := service.now().UTC()
	task, err := service.newTask(now, idempotencyKey, image)
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
	result, createErr := service.tasks.CreateTask(ctx, task, etcd.IdempotencyMarker{
		Kind: etcd.IdempotencyMarkerTask, State: etcd.IdempotencyMarkerPending,
		Locator: locator, Intent: evidence.durable,
		Response: response, TaskID: task.ID, CreatedAt: now, UpdatedAt: now,
	})
	var resolution requestidempotency.Resolution
	if createErr != nil {
		if !isUnknownAgentEnrollmentOutcome(createErr) {
			return etcd.IdempotencyResponse{}, createErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, createErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if resolution.Kind == requestidempotency.ResolutionReplay {
		return requestidempotency.CloneResponse(resolution.Response), nil
	}
	if resolution.Kind != requestidempotency.ResolutionApplied {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Agent enrollment resolution is invalid")
	}
	return requestidempotency.CloneResponse(response), nil
}

func (service *agentEnrollmentService) newTask(
	createdAt time.Time,
	idempotencyKey string,
	image string,
) (etcd.TaskRecord, error) {
	agentID := ids.New(ids.KindAgent)
	taskID := ids.New(ids.KindTask)
	params := agentEnrollmentTaskParams(taskID, image, service.config)
	planHash, err := agentEnrollmentPlanHash(agentID, params)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	return etcd.TaskRecord{
		ID: taskID, OperationID: ids.New(ids.KindOperation),
		Owner: etcd.PlatformTaskOwner(), Actor: etcd.TaskActorOperator,
		IdempotencyKey: idempotencyKey, Executor: etcd.TaskExecutorController,
		PlanID: ids.New(ids.KindPlan), PlanHash: planHash, RenderGeneration: 1,
		Type: etcd.TaskCreate, Target: agentID, Params: params,
		TimeoutSeconds: agentEnrollmentTimeoutSeconds, Status: etcd.TaskStatusPending,
		NextEventSequence: 1, CreatedAt: createdAt, UpdatedAt: createdAt,
	}, nil
}

func agentEnrollmentPlanHash(agentID string, params map[string]string) (string, error) {
	value, err := json.Marshal(struct {
		Version int               `json:"version"`
		AgentID string            `json:"agent_id"`
		Params  map[string]string `json:"params"`
	}{Version: 1, AgentID: agentID, Params: params})
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	defer clear(value)
	digest := sha256.Sum256(value)
	defer clear(digest[:])
	return hex.EncodeToString(digest[:]), nil
}

func isUnknownAgentEnrollmentOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
