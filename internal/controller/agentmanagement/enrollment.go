package agentmanagement

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentoperations"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
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
	agentEnrollmentRoute = "/agents"
)

type agentEnrollmentTaskRepository interface {
	CreateAgentWork(
		context.Context,
		agentoperations.Capture,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type agentEnrollmentEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   idempotencyrecord.ProtectedIntentRecord
}

type agentEnrollmentIdempotency interface {
	Prepare(context.Context) (agentEnrollmentEvidence, error)
	ResolveExisting(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		agentEnrollmentEvidence,
	) (requestidempotency.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		agentEnrollmentEvidence,
		etcd.IdempotencyTransactionResult,
	) (requestidempotency.Resolution, error)
	ResolveUnknown(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
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
	ctx context.Context, locator idempotencyrecord.IdempotencyLocator, evidence agentEnrollmentEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableAgentEnrollmentIdempotency) ResolveUnknown(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
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
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Agent enrollment context is required",
		)
	}
	evidence, err := service.idempotency.Prepare(ctx)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopePlatform, ScopeID: "-",
		Method: http.MethodPost, Route: agentEnrollmentRoute, Key: idempotencyKey,
	}
	existing, found, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if found {
		if existing.Kind != requestidempotency.ResolutionReplay {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Agent enrollment replay resolution is invalid",
			)
		}
		return requestidempotency.CloneResponse(existing.Response), nil
	}
	image, err := service.images.DesiredAgentImage(ctx)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if !imageref.IsDigestPinned(image) {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Agent enrollment requires a selected digest-pinned image",
		)
	}
	now := service.now().UTC()
	task, err := service.newCapture(now, image)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	responseBody, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.TaskID})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := idempotencyrecord.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json",
		Body: append([]byte(nil), responseBody...),
	}
	result, createErr := service.tasks.CreateAgentWork(ctx, task, idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
		Locator: locator, Intent: evidence.durable,
		Response: response, TaskID: task.TaskID, CreatedAt: now, UpdatedAt: now,
	})
	var resolution requestidempotency.Resolution
	if createErr != nil {
		if !isUnknownAgentEnrollmentOutcome(createErr) {
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
			"Agent enrollment resolution is invalid",
		)
	}
	return requestidempotency.CloneResponse(response), nil
}

func (service *agentEnrollmentService) newCapture(
	createdAt time.Time,
	image string,
) (agentoperations.Capture, error) {
	capture := agentoperations.Capture{TaskID: ids.New(ids.KindTask), WorkItemID: ids.New(ids.KindWorkItem),
		PlanID: ids.New(ids.KindPlan), AgentID: ids.New(ids.KindAgent), Action: workexecution.ActionCreate,
		Image: image, CreatedAt: createdAt, Enrollment: &agentoperations.Enrollment{
			PullIntervalSeconds: service.config.PullIntervalSeconds, MaxConcurrentTasks: service.config.MaxConcurrentTasks, Labels: maps.Clone(service.config.Labels)}}
	_, err := capture.Reference()
	return capture, err
}

func isUnknownAgentEnrollmentOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
