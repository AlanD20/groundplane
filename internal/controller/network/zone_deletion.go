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
	controllerrevision "github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	zoneDeletionRoute                = "/zones/{id}"
	zoneDeletionTimeoutSeconds       = int64(120)
	backingZoneCascadeTimeoutSeconds = int64(24 * 60 * 60)
	maximumZoneDeletionAttempts      = 3
)

type zoneDeletionRepository interface {
	GetZone(context.Context, string) (etcd.Versioned[etcd.ZoneRecord], error)
	GetEnvironment(context.Context, string) (etcd.Versioned[etcd.EnvironmentRecord], error)
	GetProject(context.Context, string) (etcd.Versioned[etcd.ProjectRecord], error)
	GetEnvironmentZoneRemovalAuthorities(context.Context, string) (etcd.EnvironmentZoneRemovalAuthorities, bool, error)
	ClaimEnvironmentBlueprintStage(
		context.Context,
		etcd.EnvironmentBlueprintStageClaimRequest,
	) (etcd.EnvironmentBlueprintStageClaim, error)
	StageEnvironmentBlueprintRevision(
		context.Context,
		etcd.EnvironmentBlueprintStageRequest,
	) (etcd.EnvironmentBlueprintSeal, error)
	BeginZoneDeletionWithTask(
		context.Context,
		etcd.Versioned[etcd.EnvironmentRecord],
		etcd.Versioned[etcd.ProjectRecord],
		etcd.Versioned[etcd.ZoneRecord],
		etcd.EnvironmentZoneRemovalAuthorities,
		etcd.DeletionTombstoneRecord,
		etcd.ZoneRemovalIntent,
		etcd.TaskRecord,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type zoneDeletionEvidence struct {
	candidate idempotentintent.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type zoneDeletionIdempotency interface {
	ResolveReplayLocator(
		context.Context,
		etcd.IdempotencyReplayTarget,
		string,
		string,
		string,
	) (etcd.IdempotencyLocator, bool, error)
	Prepare(context.Context, etcd.IdempotencyLocator, string, string) (zoneDeletionEvidence, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		zoneDeletionEvidence,
	) (idempotentintent.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		zoneDeletionEvidence,
		etcd.IdempotencyTransactionResult,
	) (idempotentintent.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		zoneDeletionEvidence,
		error,
	) (idempotentintent.Resolution, error)
	MatchesStaged(context.Context, zoneDeletionEvidence, etcd.ProtectedIntentRecord) (bool, error)
}

type durableZoneDeletionIdempotency struct {
	coordinator *idempotentintent.Coordinator
	repository  idempotencyEvidenceRepository
}

func newDurableZoneDeletionIdempotency(
	coordinator *idempotentintent.Coordinator,
	repository idempotencyEvidenceRepository,
) (*durableZoneDeletionIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Zone deletion idempotency is not configured")
	}
	return &durableZoneDeletionIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableZoneDeletionIdempotency) ResolveReplayLocator(
	ctx context.Context,
	target etcd.IdempotencyReplayTarget,
	method string,
	route string,
	key string,
) (etcd.IdempotencyLocator, bool, error) {
	return service.repository.ResolveReplayLocator(ctx, target, method, route, key)
}

func (service *durableZoneDeletionIdempotency) Prepare(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	zoneID string,
	impactToken string,
) (zoneDeletionEvidence, error) {
	if locator.ScopeKind != etcd.IdempotencyScopeEnvironment ||
		ids.Validate(ids.KindEnvironment, locator.ScopeID) != nil {
		return zoneDeletionEvidence{}, errs.New(errs.KindInternal, "Zone deletion replay scope is invalid")
	}
	query := idempotentintent.Object()
	if impactToken != "" {
		query = idempotentintent.Object(idempotentintent.Field{
			Name: "impact_token", Value: idempotentintent.String(impactToken),
		})
	}
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: http.MethodDelete, Route: zoneDeletionRoute,
		Scope: idempotentintent.Scope{Kind: idempotentintent.ScopeEnvironment, ID: locator.ScopeID},
		Path:  []idempotentintent.PathBinding{{Name: "id", Value: zoneID}},
		Query: query, Body: idempotentintent.NoBody(),
	})
	if err != nil {
		return zoneDeletionEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return zoneDeletionEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return zoneDeletionEvidence{}, err
	}
	return zoneDeletionEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableZoneDeletionIdempotency) ResolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence zoneDeletionEvidence,
) (idempotentintent.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableZoneDeletionIdempotency) ResolveKnown(
	ctx context.Context,
	evidence zoneDeletionEvidence,
	result etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableZoneDeletionIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence zoneDeletionEvidence,
	original error,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

func (service *durableZoneDeletionIdempotency) MatchesStaged(
	ctx context.Context,
	evidence zoneDeletionEvidence,
	existing etcd.ProtectedIntentRecord,
) (bool, error) {
	return service.coordinator.MatchesDurable(ctx, evidence.candidate, existing)
}

type zoneDeletionPlanResolver interface {
	ResolveExecutionPlan(context.Context, etcd.TaskRecord) (*controller.ExecutionPlan, error)
	PrepareZoneRemovalTask(
		context.Context,
		etcd.TaskRecord,
		etcd.ZoneRemovalIntent,
		controller.ZoneRemovalTaskProcedureIDs,
	) (etcd.TaskRecord, error)
}

type zoneDeletionService struct {
	repository  zoneDeletionRepository
	plans       zoneDeletionPlanResolver
	idempotency zoneDeletionIdempotency
	impacts     zoneDeletionImpactResolver
	now         func() time.Time
}

type zoneDeletionImpactResolver interface {
	GetZoneRemovalImpact(context.Context, string) (apiTypes.ZoneRemovalImpact, error)
}

func newZoneDeletionService(
	repository zoneDeletionRepository,
	plans zoneDeletionPlanResolver,
	idempotency zoneDeletionIdempotency,
) (*zoneDeletionService, error) {
	if repository == nil || plans == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Zone deletion service is not configured")
	}
	return &zoneDeletionService{repository: repository, plans: plans, idempotency: idempotency, now: time.Now}, nil
}

func (service *zoneCreationService) RemoveZone(
	ctx context.Context,
	zoneID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if service == nil || service.deletions == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Zone deletion service is not configured")
	}
	return service.RemoveZoneWithImpact(ctx, zoneID, idempotencyKey, "")
}

func (service *zoneCreationService) RemoveZoneWithImpact(
	ctx context.Context,
	zoneID string,
	idempotencyKey string,
	impactToken string,
) (etcd.IdempotencyResponse, error) {
	if service == nil || service.deletions == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Zone deletion service is not configured")
	}
	return service.deletions.RemoveZoneWithImpact(ctx, zoneID, idempotencyKey, impactToken)
}

func (service *zoneDeletionService) RemoveZone(
	ctx context.Context,
	zoneID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	return service.RemoveZoneWithImpact(ctx, zoneID, idempotencyKey, "")
}

func (service *zoneDeletionService) RemoveZoneWithImpact(
	ctx context.Context,
	zoneID string,
	idempotencyKey string,
	impactToken string,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Zone deletion context is required")
	}
	if ids.Validate(ids.KindNetwork, zoneID) != nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Zone id is invalid")
	}
	for attempt := 0; attempt < maximumZoneDeletionAttempts; attempt++ {
		response, err := service.removeZoneOnce(ctx, zoneID, idempotencyKey, impactToken)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumZoneDeletionAttempts-1 {
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Zone deletion retry bound was not enforced")
}

func (service *zoneDeletionService) removeZoneOnce(
	ctx context.Context,
	zoneID string,
	idempotencyKey string,
	impactToken string,
) (etcd.IdempotencyResponse, error) {
	target := etcd.IdempotencyReplayTarget{Kind: etcd.IdempotencyReplayTargetZone, ID: zoneID}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx, target, http.MethodDelete, zoneDeletionRoute, idempotencyKey,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if indexed {
		return service.replayZoneDeletion(ctx, locator, target, impactToken)
	}
	zone, err := service.repository.GetZone(ctx, zoneID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	backing := zone.Record.Desired.OwnerKind == core.ZoneOwnerBackingProject
	if backing {
		if service.impacts == nil {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Zone removal impact service is not configured",
			)
		}
		impact, impactErr := service.impacts.GetZoneRemovalImpact(ctx, zoneID)
		if impactErr != nil {
			return etcd.IdempotencyResponse{}, impactErr
		}
		if impactToken == "" || impact.ImpactToken != impactToken {
			return etcd.IdempotencyResponse{}, errs.New(errs.KindStateConflict, "backing Zone removal impact changed")
		}
	}
	environment, err := service.repository.GetEnvironment(ctx, zone.Record.EnvironmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if environment.Record.ProvisioningState != etcd.EnvironmentProvisioningReady {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindResourceInUse, "Zone Environment is not ready")
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	taskOwner, err := etcd.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	ordinaryOwnership := project.Record.Kind == etcd.ProjectKindTenant &&
		zone.Record.Desired.OwnerKind == core.ZoneOwnerEnvironment && zone.Record.Desired.OwnerID == environment.Record.ID
	backingOwnership := project.Record.Kind == etcd.ProjectKindBacking &&
		zone.Record.Desired.OwnerKind == core.ZoneOwnerBackingProject && zone.Record.Desired.OwnerID == project.Record.ID
	if (!backing && !ordinaryOwnership) || (backing && !backingOwnership) {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindStateConflict, "Zone ownership is inconsistent")
	}
	authorities, found, err := service.repository.GetEnvironmentZoneRemovalAuthorities(ctx, environment.Record.ID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if !found {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindStateConflict, "Zone desired revision is missing")
	}
	projection := authorities.Desired
	locator = etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: environment.Record.ID,
		Method: http.MethodDelete, Route: zoneDeletionRoute, Key: idempotencyKey,
	}
	evidence, err := service.idempotency.Prepare(ctx, locator, zoneID, impactToken)
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
			return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Zone deletion replay resolution is invalid")
		}
		return cloneIdempotencyResponse(resolution.Response), nil
	}

	now := service.now().UTC()
	candidateRevisionID := ids.New(ids.KindTask)
	candidate, affected, err := buildZoneRemovalProjection(
		projection.Record, zone.Record.Desired.ID, zone.Record.Desired.Name,
		candidateRevisionID, projection.Record.RenderGeneration+1,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	projectionEvidence, err := controllerrevision.PreflightProjection(candidate)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	claim, err := service.repository.ClaimEnvironmentBlueprintStage(ctx, etcd.EnvironmentBlueprintStageClaimRequest{
		EnvironmentID: environment.Record.ID, CandidateRevisionID: candidateRevisionID,
		CandidateTaskID: candidateRevisionID, Locator: locator, Intent: evidence.durable,
		BaselineHeadRevision: projection.Revision, SourceKind: etcd.EnvironmentBlueprintSourceMutation,
		RenderGeneration: candidate.RenderGeneration, ProjectionSchema: etcd.EnvironmentDesiredProjectionSchema,
		CreatedAt: now,
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if claim.Existing {
		matched, matchErr := service.idempotency.MatchesStaged(ctx, evidence, claim.Intent)
		if matchErr != nil {
			return etcd.IdempotencyResponse{}, matchErr
		}
		if !matched {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindIdempotencyMismatch,
				"idempotency key was used for another Zone removal",
			)
		}
		candidate, affected, err = buildZoneRemovalProjection(
			projection.Record, zone.Record.Desired.ID, zone.Record.Desired.Name,
			claim.RevisionID, claim.RenderGeneration,
		)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
		projectionEvidence, err = controllerrevision.PreflightProjection(candidate)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
		now = claim.CreatedAt
	}
	if claim.EnvironmentID != environment.Record.ID || claim.RevisionID != claim.TaskID ||
		claim.Locator != locator || claim.BaselineHeadRevision != projection.Revision ||
		claim.SourceKind != etcd.EnvironmentBlueprintSourceMutation ||
		claim.RenderGeneration != candidate.RenderGeneration ||
		claim.ProjectionSchema != etcd.EnvironmentDesiredProjectionSchema {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindStateConflict, "Zone staged baseline changed")
	}
	task := etcd.TaskRecord{
		ID: claim.RevisionID, OperationID: ids.New(ids.KindOperation), IdempotencyKey: idempotencyKey,
		Owner: taskOwner, Actor: etcd.TaskActorOperator,
		Executor: etcd.TaskExecutorAgent, PlanID: zoneStableIDFromRevision(ids.KindPlan, claim.RevisionID),
		Type: etcd.TaskRemove, Target: zoneID,
		TimeoutSeconds: zoneDeletionTimeoutSeconds,
		Status:         etcd.TaskStatusPending, NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	intent, err := etcd.NewZoneRemovalIntent(
		task.OperationID, task.ID, zone, authorities, claim, candidate, affected, now,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if backing {
		task.Executor = etcd.TaskExecutorController
		task.Params = map[string]string{
			etcd.TaskResourceKindParam:           etcd.TaskResourceBackingZone,
			etcd.TaskZoneEnvironmentParam:        environment.Record.ID,
			etcd.TaskZoneImpactTokenParam:        impactToken,
			etcd.TaskZoneRemovalOperationParam:   task.OperationID,
			etcd.EnvironmentDesiredRevisionParam: claim.RevisionID,
		}
		task.RenderGeneration = int32(candidate.RenderGeneration)
		task.Steps = []etcd.TaskStepRecord{{Kind: etcd.TaskStepOperation, ID: ids.New(ids.KindStep)}}
		task.TimeoutSeconds = backingZoneCascadeTimeoutSeconds
		task.PlanHash, err = backingZoneCascadePlanHash(intent, impactToken)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
	} else {
		serviceSteps := make([]string, len(affected))
		for index := range serviceSteps {
			serviceSteps[index] = ids.New(ids.KindStep)
		}
		task, err = service.plans.PrepareZoneRemovalTask(ctx, task, intent, controller.ZoneRemovalTaskProcedureIDs{
			ArtifactID:     zoneStableIDFromRevision(ids.KindConfig, claim.RevisionID),
			ServiceStepIDs: serviceSteps, NetworkStepID: ids.New(ids.KindStep),
		})
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
	}
	if candidate.RenderGeneration > math.MaxInt32 {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict,
			"Zone render generation exceeds Task limits",
		)
	}
	if _, err := service.repository.StageEnvironmentBlueprintRevision(ctx, etcd.EnvironmentBlueprintStageRequest{
		Claim: claim,
		Mutation: &etcd.EnvironmentDesiredMutationAudit{Zone: &etcd.EnvironmentZoneMutationAudit{
			Action: etcd.EnvironmentZoneMutationRemove, BaseRevisionID: projection.Record.RevisionID,
			ZoneID: zone.Record.Desired.ID,
		}},
		Projection: candidate, DependencyDigest: projectionEvidence.DependencyDigest,
	}); err != nil {
		return etcd.IdempotencyResponse{}, err
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
		Locator: locator, ReplayTarget: &target, Intent: claim.Intent, Response: response,
		TaskID: task.ID, CreatedAt: now, UpdatedAt: now,
	}
	tombstone := etcd.DeletionTombstoneRecord{
		TargetKind: etcd.DeletionTargetZone, TargetID: zoneID, TargetRevision: zone.Revision,
		TaskID: task.ID, Phase: etcd.DeletionPhaseHostEffects, CreatedAt: now, UpdatedAt: now,
	}
	result, mutationErr := service.repository.BeginZoneDeletionWithTask(
		ctx, environment, project, zone, authorities, tombstone, intent, task, marker,
	)
	if mutationErr != nil {
		if !isUnknownZoneDeletionOutcome(mutationErr) {
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
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Zone deletion resolution is invalid")
	}
}

func backingZoneCascadePlanHash(intent etcd.ZoneRemovalIntent, impactToken string) (string, error) {
	value, err := json.Marshal(struct {
		Version                               int `json:"version"`
		Type, ZoneID, ImpactToken, RevisionID string
		RenderGeneration                      uint64 `json:"render_generation"`
	}{Version: 2, Type: "backing_zone_cascade", ZoneID: intent.ZoneID, ImpactToken: impactToken,
		RevisionID: intent.Claim.RevisionID, RenderGeneration: intent.CandidateProjection.RenderGeneration})
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(value)
	clear(value)
	return hex.EncodeToString(digest[:]), nil
}

func (service *zoneDeletionService) replayZoneDeletion(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	target etcd.IdempotencyReplayTarget,
	impactToken string,
) (etcd.IdempotencyResponse, error) {
	evidence, err := service.idempotency.Prepare(ctx, locator, target.ID, impactToken)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if !existing || resolution.Kind != idempotentintent.ResolutionReplay {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Zone deletion replay target is inconsistent")
	}
	return cloneIdempotencyResponse(resolution.Response), nil
}

func isUnknownZoneDeletionOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
