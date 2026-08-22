package app

import (
	"context"
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
	environmentDeletionRoute           = "/environments/{id}"
	environmentDeletionTimeoutSeconds  = int64(120)
	maximumEnvironmentDeletionAttempts = 3
)

type environmentDeletionRepository interface {
	GetProject(context.Context, string) (etcd.Versioned[etcd.ProjectRecord], error)
	GetEnvironment(context.Context, string) (etcd.Versioned[etcd.EnvironmentRecord], error)
	GetEnvironmentComposeProjection(
		context.Context,
		string,
	) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error)
	BeginEnvironmentDeletionWithTask(
		context.Context,
		etcd.Versioned[etcd.ProjectRecord],
		etcd.Versioned[etcd.EnvironmentRecord],
		int64,
		etcd.DeletionTombstoneRecord,
		etcd.TaskRecord,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type environmentDeletionPlanResolver interface {
	ResolveExecutionPlan(context.Context, etcd.TaskRecord) (*controller.ExecutionPlan, error)
}

type environmentDeletionEvidence struct {
	candidate idempotentintent.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type environmentDeletionIdempotency interface {
	Prepare(context.Context, string) (environmentDeletionEvidence, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		environmentDeletionEvidence,
	) (idempotentintent.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		environmentDeletionEvidence,
		etcd.IdempotencyTransactionResult,
	) (idempotentintent.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		environmentDeletionEvidence,
		error,
	) (idempotentintent.Resolution, error)
}

type durableEnvironmentDeletionIdempotency struct {
	coordinator *idempotentintent.Coordinator
	repository  *etcd.IdempotencyRepository
}

func newDurableEnvironmentDeletionIdempotency(
	coordinator *idempotentintent.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableEnvironmentDeletionIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Environment deletion idempotency is not configured")
	}
	return &durableEnvironmentDeletionIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableEnvironmentDeletionIdempotency) Prepare(
	ctx context.Context,
	environmentID string,
) (environmentDeletionEvidence, error) {
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: http.MethodDelete, Route: environmentDeletionRoute,
		Scope: idempotentintent.Scope{Kind: idempotentintent.ScopeEnvironment, ID: environmentID},
		Path:  []idempotentintent.PathBinding{{Name: "id", Value: environmentID}},
		Query: idempotentintent.Object(), Body: idempotentintent.NoBody(),
	})
	if err != nil {
		return environmentDeletionEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return environmentDeletionEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return environmentDeletionEvidence{}, err
	}
	return environmentDeletionEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableEnvironmentDeletionIdempotency) ResolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence environmentDeletionEvidence,
) (idempotentintent.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableEnvironmentDeletionIdempotency) ResolveKnown(
	ctx context.Context,
	evidence environmentDeletionEvidence,
	result etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableEnvironmentDeletionIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence environmentDeletionEvidence,
	original error,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type environmentDeletionService struct {
	repository  environmentDeletionRepository
	plans       environmentDeletionPlanResolver
	idempotency environmentDeletionIdempotency
	now         func() time.Time
}

func newEnvironmentDeletionService(
	repository environmentDeletionRepository,
	plans environmentDeletionPlanResolver,
	idempotency environmentDeletionIdempotency,
) (*environmentDeletionService, error) {
	if repository == nil || plans == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Environment deletion service is not configured")
	}
	return &environmentDeletionService{
		repository: repository, plans: plans, idempotency: idempotency, now: time.Now,
	}, nil
}

func (service *environmentDeletionService) DeleteEnvironment(
	ctx context.Context,
	environmentID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Environment deletion context is required")
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Environment id is invalid")
	}
	for attempt := 0; attempt < maximumEnvironmentDeletionAttempts; attempt++ {
		response, err := service.deleteEnvironmentOnce(ctx, environmentID, idempotencyKey)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumEnvironmentDeletionAttempts-1 {
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Environment deletion retry bound was not enforced")
}

func (service *environmentDeletionService) deleteEnvironmentOnce(
	ctx context.Context,
	environmentID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	evidence, err := service.idempotency.Prepare(ctx, environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: environmentID,
		Method: http.MethodDelete, Route: environmentDeletionRoute, Key: idempotencyKey,
	}
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != idempotentintent.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Environment deletion replay resolution is invalid",
			)
		}
		return cloneIdempotencyResponse(resolution.Response), nil
	}
	environment, err := service.repository.GetEnvironment(ctx, environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if environment.Record.ProvisioningState == etcd.EnvironmentProvisioningProvisioning {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindResourceInUse, "Environment provisioning is in progress")
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	projection, hasProjection, err := service.repository.GetEnvironmentComposeProjection(ctx, environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	now := service.now().UTC()
	renderGeneration := int32(1)
	expectedBlueprintRevision := int64(0)
	params := map[string]string{
		controller.EnvironmentRemoveVolumeDirectoryParam: environment.Record.VolumeDir,
	}
	steps := []etcd.TaskStepRecord{{ID: ids.New(ids.KindStep)}}
	if hasProjection {
		if projection.Record.RenderGeneration > math.MaxInt32 {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindStateConflict,
				"Environment render generation exceeds Agent plan limits",
			)
		}
		renderGeneration = int32(projection.Record.RenderGeneration)
		expectedBlueprintRevision = projection.Revision
		params[etcd.EnvironmentBlueprintRevisionParam] = projection.Record.BlueprintRevisionID
		params[etcd.TaskMaterializationEnvironmentParam] = environment.Record.ID
		params[controller.EnvironmentBlueprintArtifactParam] = ids.New(ids.KindConfig)
		steps = []etcd.TaskStepRecord{{ID: ids.New(ids.KindStep)}, {ID: ids.New(ids.KindStep)}}
	}
	task := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation), IdempotencyKey: idempotencyKey,
		Executor: etcd.TaskExecutorAgent, PlanID: ids.New(ids.KindPlan), RenderGeneration: renderGeneration,
		Type: etcd.TaskRemove, Target: environmentID,
		Params:         params,
		Steps:          steps,
		TimeoutSeconds: environmentDeletionTimeoutSeconds,
		Status:         etcd.TaskStatusPending, NextEventSequence: 1, CreatedAt: now,
	}
	plan, err := service.plans.ResolveExecutionPlan(ctx, task)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	task.PlanHash = hex.EncodeToString(plan.PlanHash)
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
		Locator: locator, Intent: evidence.durable, Response: response,
		TaskID: task.ID, CreatedAt: now, UpdatedAt: now,
	}
	tombstone := etcd.DeletionTombstoneRecord{
		TargetKind: etcd.DeletionTargetEnvironment, TargetID: environmentID,
		TargetRevision: environment.Revision, TaskID: task.ID, Phase: etcd.DeletionPhaseHostEffects,
		CreatedAt: now, UpdatedAt: now,
	}
	result, deleteErr := service.repository.BeginEnvironmentDeletionWithTask(
		ctx, project, environment, expectedBlueprintRevision, tombstone, task, marker,
	)
	if deleteErr != nil {
		if !isUnknownEnvironmentDeletionOutcome(deleteErr) {
			return etcd.IdempotencyResponse{}, deleteErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, deleteErr)
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
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Environment deletion resolution is invalid")
	}
}

func isUnknownEnvironmentDeletionOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
