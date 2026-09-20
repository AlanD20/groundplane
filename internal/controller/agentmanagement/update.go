package agentmanagement

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
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
	agentUpdateRoute               = "/agents/{id}/update"
	agentUpdateTimeoutSeconds      = int64(300)
	agentTaskPreviousImageKey      = "previous_image"
	agentTaskStartingGenerationKey = "starting_generation"
)

type agentUpdateTargets interface {
	Health(context.Context, string) (localagent.Health, error)
}

type agentUpdateEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type agentUpdateIdempotency interface {
	Prepare(context.Context, string, string) (agentUpdateEvidence, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		agentUpdateEvidence,
	) (requestidempotency.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		agentUpdateEvidence,
		etcd.IdempotencyTransactionResult,
	) (requestidempotency.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		agentUpdateEvidence,
		error,
	) (requestidempotency.Resolution, error)
}

type durableAgentUpdateIdempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  *etcd.IdempotencyRepository
}

func NewUpdateIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableAgentUpdateIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Agent update idempotency is not configured")
	}
	return &durableAgentUpdateIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableAgentUpdateIdempotency) Prepare(
	ctx context.Context,
	agentID string,
	image string,
) (agentUpdateEvidence, error) {
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost,
		Route:  agentUpdateRoute,
		Scope:  requestidempotency.Scope{Kind: requestidempotency.ScopePlatform},
		Path:   []requestidempotency.PathBinding{{Name: "id", Value: agentID}},
		Query:  requestidempotency.Object(),
		Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "image", Value: requestidempotency.String(image)},
		)),
	})
	if err != nil {
		return agentUpdateEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return agentUpdateEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return agentUpdateEvidence{}, err
	}
	return agentUpdateEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableAgentUpdateIdempotency) ResolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence agentUpdateEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableAgentUpdateIdempotency) ResolveKnown(
	ctx context.Context,
	evidence agentUpdateEvidence,
	result etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableAgentUpdateIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence agentUpdateEvidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type agentUpdateService struct {
	targets     agentUpdateTargets
	tasks       agentEnrollmentTaskRepository
	idempotency agentUpdateIdempotency
	now         func() time.Time
}

func NewUpdateService(
	targets agentUpdateTargets,
	tasks agentEnrollmentTaskRepository,
	idempotency agentUpdateIdempotency,
) (*agentUpdateService, error) {
	if targets == nil || tasks == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Agent update service dependencies are invalid")
	}
	return &agentUpdateService{
		targets: targets, tasks: tasks, idempotency: idempotency, now: time.Now,
	}, nil
}

func (service *agentUpdateService) UpdateAgent(
	ctx context.Context,
	agentID string,
	image string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Agent update context is required")
	}
	if err := ids.Validate(ids.KindAgent, agentID); err != nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Agent id is invalid")
	}
	if !imageref.IsDigestPinned(image) {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Agent update requires a digest-pinned image",
		)
	}
	evidence, err := service.idempotency.Prepare(ctx, agentID, image)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopePlatform, ScopeID: "-",
		Method: http.MethodPost, Route: agentUpdateRoute, Key: idempotencyKey,
	}
	existing, found, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if found {
		if existing.Kind != requestidempotency.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Agent update replay resolution is invalid")
		}
		return requestidempotency.CloneResponse(existing.Response), nil
	}
	health, err := service.targets.Health(ctx, agentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if health.Agent.ID != agentID {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Agent update target lookup returned another Agent",
		)
	}
	if health.Agent.Phase != localagent.PhaseReady {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindResourceInUse, "Agent is not ready for update")
	}
	if health.Agent.Image == image {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindStateConflict, "Agent already runs the selected image")
	}

	now := service.now().UTC()
	task, err := newAgentUpdateTask(
		now,
		agentID,
		health.Agent.Image,
		image,
		health.Agent.Generation,
		idempotencyKey,
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
	result, createErr := service.tasks.CreateTask(ctx, task, etcd.IdempotencyMarker{
		Kind: etcd.IdempotencyMarkerTask, State: etcd.IdempotencyMarkerPending,
		Locator: locator, Intent: evidence.durable,
		Response: response, TaskID: task.ID, CreatedAt: now, UpdatedAt: now,
	})
	var resolution requestidempotency.Resolution
	if createErr != nil {
		if !isUnknownAgentUpdateOutcome(createErr) {
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
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Agent update resolution is invalid")
	}
	return requestidempotency.CloneResponse(response), nil
}

func newAgentUpdateTask(
	createdAt time.Time,
	agentID string,
	previousImage string,
	desiredImage string,
	startingGeneration uint64,
	idempotencyKey string,
) (etcd.TaskRecord, error) {
	if startingGeneration == 0 || startingGeneration > ^uint64(0)-2 {
		return etcd.TaskRecord{}, errs.New(
			errs.KindStateConflict,
			"agent generation has no replacement and recovery capacity",
		)
	}
	params := map[string]string{
		etcd.TaskResourceKindParam:     etcd.TaskResourceAgent,
		agentTaskPreviousImageKey:      previousImage,
		agentTaskImageKey:              desiredImage,
		agentTaskStartingGenerationKey: strconv.FormatUint(startingGeneration, 10),
	}
	planHash, err := agentUpdatePlanHash(agentID, params)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	return etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation),
		Owner: etcd.PlatformTaskOwner(), Actor: etcd.TaskActorOperator,
		IdempotencyKey: idempotencyKey, Executor: etcd.TaskExecutorController,
		PlanID: ids.New(ids.KindPlan), PlanHash: planHash, RenderGeneration: 1,
		Type: etcd.TaskUpdate, Target: agentID, Params: params,
		TimeoutSeconds: agentUpdateTimeoutSeconds, Status: etcd.TaskStatusPending,
		NextEventSequence: 1, CreatedAt: createdAt, UpdatedAt: createdAt,
	}, nil
}

func agentUpdatePlanHash(agentID string, params map[string]string) (string, error) {
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

func isUnknownAgentUpdateOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
