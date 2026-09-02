package network

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	routeDeletionRoute                   = "/routes/{id}"
	routeRemovalAgentTimeoutSeconds      = int64(120)
	routeRemovalControllerTimeoutSeconds = int64(30)
	maximumRouteDeletionAttempts         = 3
)

type routeRemovalRepository interface {
	GetEnvironment(context.Context, string) (etcd.Versioned[etcd.EnvironmentRecord], error)
	GetProject(context.Context, string) (etcd.Versioned[etcd.ProjectRecord], error)
	GetService(context.Context, string) (etcd.Versioned[etcd.ServiceRecord], error)
	GetRoute(context.Context, string) (etcd.Versioned[etcd.RouteRecord], error)
	GetEnvironmentComposeProjection(context.Context, string) (
		etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error,
	)
	BeginRouteDeletionWithTask(
		context.Context,
		etcd.Versioned[etcd.EnvironmentRecord],
		etcd.Versioned[etcd.ProjectRecord],
		etcd.Versioned[etcd.ServiceRecord],
		etcd.Versioned[etcd.RouteRecord],
		*etcd.Versioned[etcd.EnvironmentComposeProjection],
		etcd.DeletionTombstoneRecord,
		etcd.RouteRemovalIntent,
		etcd.TaskRecord,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type routeRemovalEvidence struct {
	candidate idempotentintent.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type routeRemovalIdempotency interface {
	ResolveReplayLocator(context.Context, etcd.IdempotencyReplayTarget, string, string, string) (
		etcd.IdempotencyLocator, bool, error,
	)
	Prepare(context.Context, etcd.IdempotencyLocator, string) (routeRemovalEvidence, error)
	ResolveExisting(context.Context, etcd.IdempotencyLocator, routeRemovalEvidence) (
		idempotentintent.Resolution, bool, error,
	)
	ResolveKnown(context.Context, routeRemovalEvidence, etcd.IdempotencyTransactionResult) (
		idempotentintent.Resolution, error,
	)
	ResolveUnknown(context.Context, etcd.IdempotencyLocator, routeRemovalEvidence, error) (
		idempotentintent.Resolution, error,
	)
}

type durableRouteRemovalIdempotency struct {
	coordinator *idempotentintent.Coordinator
	repository  idempotencyEvidenceRepository
}

func (service *durableRouteRemovalIdempotency) ResolveReplayLocator(
	ctx context.Context,
	target etcd.IdempotencyReplayTarget,
	method string,
	route string,
	key string,
) (etcd.IdempotencyLocator, bool, error) {
	return service.repository.ResolveReplayLocator(ctx, target, method, route, key)
}

func (service *durableRouteRemovalIdempotency) Prepare(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	routeID string,
) (routeRemovalEvidence, error) {
	if locator.ScopeKind != etcd.IdempotencyScopeEnvironment ||
		ids.Validate(ids.KindEnvironment, locator.ScopeID) != nil {
		return routeRemovalEvidence{}, errs.New(errs.KindInternal, "route removal replay scope is invalid")
	}
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: http.MethodDelete,
		Route:  routeDeletionRoute,
		Scope:  idempotentintent.Scope{Kind: idempotentintent.ScopeEnvironment, ID: locator.ScopeID},
		Path:   []idempotentintent.PathBinding{{Name: "id", Value: routeID}},
		Query:  idempotentintent.Object(),
		Body:   idempotentintent.NoBody(),
	})
	if err != nil {
		return routeRemovalEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return routeRemovalEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return routeRemovalEvidence{}, err
	}
	return routeRemovalEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableRouteRemovalIdempotency) ResolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence routeRemovalEvidence,
) (idempotentintent.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableRouteRemovalIdempotency) ResolveKnown(
	ctx context.Context,
	evidence routeRemovalEvidence,
	result etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableRouteRemovalIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence routeRemovalEvidence,
	original error,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type routeRemovalPlanResolver interface {
	PrepareRouteRemovalTask(
		context.Context,
		etcd.TaskRecord,
		etcd.RouteRemovalIntent,
		controller.RouteRemovalTaskProcedureIDs,
	) (etcd.RouteRemovalTaskPreparation, error)
}

// routeRemovalService owns the complete operator intent for removing a Route,
// from protected replay resolution through durable Task publication.
type routeRemovalService struct {
	repository  routeRemovalRepository
	plans       routeRemovalPlanResolver
	idempotency routeRemovalIdempotency
	now         func() time.Time
}

func newRouteRemovalService(
	repository routeRemovalRepository,
	plans routeRemovalPlanResolver,
	idempotency routeRemovalIdempotency,
) (*routeRemovalService, error) {
	if repository == nil || plans == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "route removal service is not configured")
	}
	return &routeRemovalService{repository: repository, plans: plans, idempotency: idempotency, now: time.Now}, nil
}

// RemoveRoute publishes one replay-safe Route removal Task.
func (service *routeRemovalService) RemoveRoute(
	ctx context.Context,
	routeID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "route removal context is required")
	}
	if ids.Validate(ids.KindRoute, routeID) != nil {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"route removal requires a stable Route id",
		)
	}
	for attempt := 0; attempt < maximumRouteDeletionAttempts; attempt++ {
		response, err := service.removeRouteOnce(ctx, routeID, idempotencyKey)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumRouteDeletionAttempts-1 {
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "route removal retry bound was not enforced")
}

func (service *routeRemovalService) removeRouteOnce(
	ctx context.Context,
	routeID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	target := etcd.IdempotencyReplayTarget{Kind: etcd.IdempotencyReplayTargetRoute, ID: routeID}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx, target, http.MethodDelete, routeDeletionRoute, idempotencyKey,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if indexed {
		return service.replayIndexedRemoval(ctx, routeID, locator)
	}

	current, err := service.repository.GetRoute(ctx, routeID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	locator = etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeEnvironment,
		ScopeID:   current.Record.EnvironmentID,
		Method:    http.MethodDelete,
		Route:     routeDeletionRoute,
		Key:       idempotencyKey,
	}
	evidence, err := service.idempotency.Prepare(ctx, locator, routeID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		return replayResponse(resolution)
	}

	environment, err := service.repository.GetEnvironment(ctx, current.Record.EnvironmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	taskOwner, err := etcd.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	targetService, err := service.repository.GetService(ctx, current.Record.Desired.TargetServiceID)
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

	now := service.now().UTC()
	task := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation), IdempotencyKey: idempotencyKey,
		Owner: taskOwner, Actor: etcd.TaskActorOperator, PlanID: ids.New(ids.KindPlan),
		Type: etcd.TaskRemove, Target: routeID, Status: etcd.TaskStatusPending,
		NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	intent, err := etcd.NewRouteRemovalIntent(
		task.ID, environment.Record.ID, routeID, current.Revision, projectionInput, now,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	task.TimeoutSeconds = routeRemovalAgentTimeoutSeconds
	preparation, err := service.plans.PrepareRouteRemovalTask(ctx, task, intent, controller.RouteRemovalTaskProcedureIDs{
		ArtifactID: ids.New(ids.KindConfig), MaterializationID: ids.New(ids.KindConfig),
		MaterializeStepID: ids.New(ids.KindStep), ComposeApplyStepID: ids.New(ids.KindStep),
		ActivateStepID: ids.New(ids.KindStep),
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	intent, task = preparation.Intent, preparation.Task
	if intent.Provider == nil {
		task, err = prepareControllerRouteRemovalTask(task, intent)
	}
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if intent.CurrentProjection == nil {
		projectionInput = nil
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
	phase := etcd.DeletionPhaseFinalizing
	if intent.Provider != nil {
		phase = etcd.DeletionPhaseHostEffects
	}
	tombstone := etcd.DeletionTombstoneRecord{
		TargetKind: etcd.DeletionTargetRoute, TargetID: routeID, TargetRevision: current.Revision,
		TaskID: task.ID, Phase: phase, CreatedAt: now, UpdatedAt: now,
	}
	result, mutationErr := service.repository.BeginRouteDeletionWithTask(
		ctx, environment, project, targetService, current, projectionInput, tombstone, intent, task, marker,
	)
	if mutationErr != nil {
		if !isUnknownRemovalOutcome(mutationErr) {
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
		return cloneResponse(response), nil
	case idempotentintent.ResolutionReplay:
		return cloneResponse(resolution.Response), nil
	default:
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "route removal resolution is invalid")
	}
}

func (service *routeRemovalService) replayIndexedRemoval(
	ctx context.Context,
	routeID string,
	locator etcd.IdempotencyLocator,
) (etcd.IdempotencyResponse, error) {
	evidence, err := service.idempotency.Prepare(ctx, locator, routeID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if !existing {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "route removal replay target is inconsistent")
	}
	return replayResponse(resolution)
}

func prepareControllerRouteRemovalTask(
	task etcd.TaskRecord,
	intent etcd.RouteRemovalIntent,
) (etcd.TaskRecord, error) {
	renderGeneration := uint64(1)
	if intent.CandidateProjection != nil {
		renderGeneration = intent.CandidateProjection.RenderGeneration
	}
	if renderGeneration == 0 || renderGeneration > math.MaxInt32 {
		return etcd.TaskRecord{}, errs.New(
			errs.KindStateConflict,
			"route render generation exceeds Controller Task limits",
		)
	}
	task.Executor = etcd.TaskExecutorController
	task.RenderGeneration = int32(renderGeneration)
	task.Params = map[string]string{
		etcd.TaskResourceKindParam:     etcd.TaskResourceRoute,
		etcd.TaskRouteEnvironmentParam: intent.EnvironmentID,
	}
	task.Steps = []etcd.TaskStepRecord{{Kind: etcd.TaskStepOperation, ID: ids.New(ids.KindStep)}}
	task.TimeoutSeconds = routeRemovalControllerTimeoutSeconds
	planHash, err := controllerRouteRemovalPlanHash(intent)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	task.PlanHash = planHash
	return task, nil
}

func controllerRouteRemovalPlanHash(intent etcd.RouteRemovalIntent) (string, error) {
	value, err := json.Marshal(struct {
		Version                   int    `json:"version"`
		Type                      string `json:"type"`
		RouteID                   string `json:"route_id"`
		EnvironmentID             string `json:"environment_id"`
		RouteRevision             int64  `json:"route_revision"`
		CurrentProjectionRevision int64  `json:"current_projection_revision"`
	}{
		Version: 1, Type: string(etcd.TaskRemove), RouteID: intent.RouteID,
		EnvironmentID: intent.EnvironmentID, RouteRevision: intent.RouteRevision,
		CurrentProjectionRevision: intent.CurrentProjectionRevision,
	})
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(value)
	clear(value)
	return hex.EncodeToString(digest[:]), nil
}

func replayResponse(resolution idempotentintent.Resolution) (etcd.IdempotencyResponse, error) {
	if resolution.Kind != idempotentintent.ResolutionReplay {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "route removal replay resolution is invalid")
	}
	return cloneResponse(resolution.Response), nil
}

func cloneResponse(response etcd.IdempotencyResponse) etcd.IdempotencyResponse {
	response.Body = append([]byte(nil), response.Body...)
	return response
}

func isUnknownRemovalOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
