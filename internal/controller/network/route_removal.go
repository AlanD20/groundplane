package network

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"math"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
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
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
	GetService(context.Context, string) (etcdstore.Versioned[servicerecord.ServiceRecord], error)
	GetRoute(context.Context, string) (etcdstore.Versioned[routerecord.Record], error)
	GetEnvironmentComposeProjection(context.Context, string) (
		etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error,
	)
	BeginRouteDeletionWithTask(
		context.Context,
		etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
		etcdstore.Versioned[hierarchyrecord.ProjectRecord],
		etcdstore.Versioned[servicerecord.ServiceRecord],
		etcdstore.Versioned[routerecord.Record],
		*etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
		deletionrecord.DeletionTombstoneRecord,
		etcd.RouteRemovalIntent,
		etcd.TaskRecord,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type routeRemovalEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   idempotencyrecord.ProtectedIntentRecord
}

type routeRemovalIdempotency interface {
	ResolveReplayLocator(context.Context, idempotencyrecord.IdempotencyReplayTarget, string, string, string) (
		idempotencyrecord.IdempotencyLocator, bool, error,
	)
	Prepare(context.Context, idempotencyrecord.IdempotencyLocator, string) (routeRemovalEvidence, error)
	ResolveExisting(context.Context, idempotencyrecord.IdempotencyLocator, routeRemovalEvidence) (
		requestidempotency.Resolution, bool, error,
	)
	ResolveKnown(context.Context, routeRemovalEvidence, etcd.IdempotencyTransactionResult) (
		requestidempotency.Resolution, error,
	)
	ResolveUnknown(context.Context, idempotencyrecord.IdempotencyLocator, routeRemovalEvidence, error) (
		requestidempotency.Resolution, error,
	)
}

type durableRouteRemovalIdempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  idempotencyEvidenceRepository
}

func (service *durableRouteRemovalIdempotency) ResolveReplayLocator(
	ctx context.Context,
	target idempotencyrecord.IdempotencyReplayTarget,
	method string,
	route string,
	key string,
) (idempotencyrecord.IdempotencyLocator, bool, error) {
	return service.repository.ResolveReplayLocator(ctx, target, method, route, key)
}

func (service *durableRouteRemovalIdempotency) Prepare(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	routeID string,
) (routeRemovalEvidence, error) {
	if locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		ids.Validate(ids.KindEnvironment, locator.ScopeID) != nil {
		return routeRemovalEvidence{}, errs.New(errs.KindInternal, "route removal replay scope is invalid")
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodDelete,
		Route:  routeDeletionRoute,
		Scope:  requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: locator.ScopeID},
		Path:   []requestidempotency.PathBinding{{Name: "id", Value: routeID}},
		Query:  requestidempotency.Object(),
		Body:   requestidempotency.NoBody(),
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
	locator idempotencyrecord.IdempotencyLocator,
	evidence routeRemovalEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableRouteRemovalIdempotency) ResolveKnown(
	ctx context.Context,
	evidence routeRemovalEvidence,
	result etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableRouteRemovalIdempotency) ResolveUnknown(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence routeRemovalEvidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type routeRemovalPlanResolver interface {
	PrepareRouteRemovalTask(
		context.Context,
		etcd.TaskRecord,
		etcd.RouteRemovalIntent,
		taskplanning.RouteRemovalTaskProcedureIDs,
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
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "route removal context is required")
	}
	if ids.Validate(ids.KindRoute, routeID) != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
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
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "route removal retry bound was not enforced")
}

func (service *routeRemovalService) removeRouteOnce(
	ctx context.Context,
	routeID string,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	target := idempotencyrecord.IdempotencyReplayTarget{Kind: idempotencyrecord.IdempotencyReplayTargetRoute, ID: routeID}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx, target, http.MethodDelete, routeDeletionRoute, idempotencyKey,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if indexed {
		return service.replayIndexedRemoval(ctx, routeID, locator)
	}

	current, err := service.repository.GetRoute(ctx, routeID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	locator = idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment,
		ScopeID:   current.Record.EnvironmentID,
		Method:    http.MethodDelete,
		Route:     routeDeletionRoute,
		Key:       idempotencyKey,
	}
	evidence, err := service.idempotency.Prepare(ctx, locator, routeID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if existing {
		return replayResponse(resolution)
	}

	environment, err := service.repository.GetEnvironment(ctx, current.Record.EnvironmentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	taskOwner, err := taskjournal.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	targetService, err := service.repository.GetService(ctx, current.Record.Desired.TargetServiceID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	projection, hasProjection, err := service.repository.GetEnvironmentComposeProjection(ctx, environment.Record.ID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	var projectionInput *etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]
	if hasProjection {
		projectionInput = &projection
	}

	now := service.now().UTC()
	task := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation), IdempotencyKey: idempotencyKey,
		Owner: taskOwner, Actor: taskjournal.TaskActorOperator, PlanID: ids.New(ids.KindPlan),
		Type: taskjournal.TaskRemove, Target: routeID, Status: taskjournal.TaskStatusPending,
		NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	intent, err := etcd.NewRouteRemovalIntent(
		task.ID, environment.Record.ID, routeID, current.Revision, projectionInput, now,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	task.TimeoutSeconds = routeRemovalAgentTimeoutSeconds
	preparation, err := service.plans.PrepareRouteRemovalTask(
		ctx,
		task,
		intent,
		taskplanning.RouteRemovalTaskProcedureIDs{
			ArtifactID: ids.New(ids.KindConfig), MaterializationID: ids.New(ids.KindConfig),
			MaterializeStepID: ids.New(ids.KindStep), ComposeApplyStepID: ids.New(ids.KindStep),
			ActivateStepID: ids.New(ids.KindStep),
		},
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	intent, task = preparation.Intent, preparation.Task
	if intent.Provider == nil {
		task, err = prepareControllerRouteRemovalTask(task, intent)
	}
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if intent.CurrentProjection == nil {
		projectionInput = nil
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
	phase := deletionrecord.DeletionPhaseFinalizing
	if intent.Provider != nil {
		phase = deletionrecord.DeletionPhaseHostEffects
	}
	tombstone := deletionrecord.DeletionTombstoneRecord{
		TargetKind: deletionrecord.DeletionTargetRoute, TargetID: routeID, TargetRevision: current.Revision,
		TaskID: task.ID, Phase: phase, CreatedAt: now, UpdatedAt: now,
	}
	result, mutationErr := service.repository.BeginRouteDeletionWithTask(
		ctx, environment, project, targetService, current, projectionInput, tombstone, intent, task, marker,
	)
	if mutationErr != nil {
		if !isUnknownRemovalOutcome(mutationErr) {
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
		return cloneResponse(response), nil
	case requestidempotency.ResolutionReplay:
		return cloneResponse(resolution.Response), nil
	default:
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "route removal resolution is invalid")
	}
}

func (service *routeRemovalService) replayIndexedRemoval(
	ctx context.Context,
	routeID string,
	locator idempotencyrecord.IdempotencyLocator,
) (idempotencyrecord.IdempotencyResponse, error) {
	evidence, err := service.idempotency.Prepare(ctx, locator, routeID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if !existing {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "route removal replay target is inconsistent")
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
	task.Executor = taskjournal.TaskExecutorController
	task.RenderGeneration = int32(renderGeneration)
	task.Params = map[string]string{
		taskjournal.TaskResourceKindParam:     taskjournal.TaskResourceRoute,
		taskjournal.TaskRouteEnvironmentParam: intent.EnvironmentID,
	}
	task.Steps = []taskjournal.TaskStepRecord{{Kind: taskjournal.TaskStepOperation, ID: ids.New(ids.KindStep)}}
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
		Version: 1, Type: string(taskjournal.TaskRemove), RouteID: intent.RouteID,
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

func replayResponse(resolution requestidempotency.Resolution) (idempotencyrecord.IdempotencyResponse, error) {
	if resolution.Kind != requestidempotency.ResolutionReplay {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "route removal replay resolution is invalid")
	}
	return cloneResponse(resolution.Response), nil
}

func cloneResponse(response idempotencyrecord.IdempotencyResponse) idempotencyrecord.IdempotencyResponse {
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
