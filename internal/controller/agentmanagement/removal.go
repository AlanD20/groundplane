package agentmanagement

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	agentRemovalRoute          = "/agents/{id}"
	agentRemovalTimeoutSeconds = int64(120)
)

type agentRemovalTargets interface {
	Health(context.Context, string) (localagent.Health, error)
}

type agentRemovalEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   idempotencyrecord.ProtectedIntentRecord
}

type agentRemovalIdempotency interface {
	Prepare(context.Context, string) (agentRemovalEvidence, error)
	ResolveExisting(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		agentRemovalEvidence,
	) (requestidempotency.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		agentRemovalEvidence,
		etcd.IdempotencyTransactionResult,
	) (requestidempotency.Resolution, error)
	ResolveUnknown(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		agentRemovalEvidence,
		error,
	) (requestidempotency.Resolution, error)
}

type durableAgentRemovalIdempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  *etcd.IdempotencyRepository
}

func NewRemovalIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableAgentRemovalIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Agent removal idempotency is not configured")
	}
	return &durableAgentRemovalIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableAgentRemovalIdempotency) Prepare(
	ctx context.Context,
	agentID string,
) (agentRemovalEvidence, error) {
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodDelete,
		Route:  agentRemovalRoute,
		Scope:  requestidempotency.Scope{Kind: requestidempotency.ScopePlatform},
		Path:   []requestidempotency.PathBinding{{Name: "id", Value: agentID}},
		Query:  requestidempotency.Object(),
		Body:   requestidempotency.NoBody(),
	})
	if err != nil {
		return agentRemovalEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return agentRemovalEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return agentRemovalEvidence{}, err
	}
	return agentRemovalEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableAgentRemovalIdempotency) ResolveExisting(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence agentRemovalEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableAgentRemovalIdempotency) ResolveKnown(
	ctx context.Context,
	evidence agentRemovalEvidence,
	result etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableAgentRemovalIdempotency) ResolveUnknown(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence agentRemovalEvidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type agentRemovalService struct {
	targets     agentRemovalTargets
	tasks       agentEnrollmentTaskRepository
	idempotency agentRemovalIdempotency
	now         func() time.Time
}

func NewRemovalService(
	targets agentRemovalTargets,
	tasks agentEnrollmentTaskRepository,
	idempotency agentRemovalIdempotency,
) (*agentRemovalService, error) {
	if targets == nil || tasks == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Agent removal service dependencies are invalid")
	}
	return &agentRemovalService{targets: targets, tasks: tasks, idempotency: idempotency, now: time.Now}, nil
}

func (service *agentRemovalService) RemoveAgent(
	ctx context.Context,
	agentID string,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Agent removal context is required")
	}
	if err := ids.Validate(ids.KindAgent, agentID); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Agent id is invalid")
	}
	evidence, err := service.idempotency.Prepare(ctx, agentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopePlatform,
		ScopeID:   "-",
		Method:    http.MethodDelete,
		Route:     agentRemovalRoute,
		Key:       idempotencyKey,
	}
	existing, found, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if found {
		if existing.Kind != requestidempotency.ResolutionReplay {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Agent removal replay resolution is invalid",
			)
		}
		return requestidempotency.CloneResponse(existing.Response), nil
	}
	health, err := service.targets.Health(ctx, agentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if health.Agent.ID != agentID {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Agent removal target lookup returned another Agent",
		)
	}
	now := service.now().UTC()
	task, err := newAgentRemovalTask(now, agentID, idempotencyKey)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	responseBody, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := idempotencyrecord.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json",
		Body: append([]byte(nil), responseBody...),
	}
	result, createErr := service.tasks.CreateTask(ctx, task, idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
		Locator: locator, Intent: evidence.durable,
		Response: response, TaskID: task.ID, CreatedAt: now, UpdatedAt: now,
	})
	var resolution requestidempotency.Resolution
	if createErr != nil {
		if !isUnknownAgentRemovalOutcome(createErr) {
			return idempotencyrecord.IdempotencyResponse{}, createErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, createErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if resolution.Kind == requestidempotency.ResolutionReplay {
		return requestidempotency.CloneResponse(resolution.Response), nil
	}
	if resolution.Kind != requestidempotency.ResolutionApplied {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Agent removal resolution is invalid",
		)
	}
	return requestidempotency.CloneResponse(response), nil
}

func newAgentRemovalTask(
	createdAt time.Time,
	agentID string,
	idempotencyKey string,
) (etcd.TaskRecord, error) {
	planHash, err := agentRemovalPlanHash(agentID)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	return etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation),
		Owner: taskjournal.PlatformTaskOwner(), Actor: taskjournal.TaskActorOperator,
		IdempotencyKey: idempotencyKey, Executor: taskjournal.TaskExecutorController,
		PlanID: ids.New(ids.KindPlan), PlanHash: planHash, RenderGeneration: 1,
		Type: taskjournal.TaskRemove, Target: agentID,
		Params:         map[string]string{taskjournal.TaskResourceKindParam: taskjournal.TaskResourceAgent},
		TimeoutSeconds: agentRemovalTimeoutSeconds, Status: taskjournal.TaskStatusPending,
		NextEventSequence: 1, CreatedAt: createdAt, UpdatedAt: createdAt,
	}, nil
}

func agentRemovalPlanHash(agentID string) (string, error) {
	value, err := json.Marshal(struct {
		Version int    `json:"version"`
		Type    string `json:"type"`
		AgentID string `json:"agent_id"`
	}{Version: 1, Type: string(taskjournal.TaskRemove), AgentID: agentID})
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	defer clear(value)
	digest := sha256.Sum256(value)
	defer clear(digest[:])
	return hex.EncodeToString(digest[:]), nil
}

func isUnknownAgentRemovalOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
