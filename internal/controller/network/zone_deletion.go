package network

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"math"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	controllerrevision "github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	zoneDeletionRoute                = "/zones/{id}"
	zoneDeletionTimeoutSeconds       = int64(120)
	backingZoneCascadeTimeoutSeconds = int64(24 * 60 * 60)
	maximumZoneDeletionAttempts      = 3
)

type zoneDeletionRepository interface {
	GetZone(context.Context, string) (etcdstore.Versioned[zonerecord.Record], error)
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
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
		etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
		etcdstore.Versioned[hierarchyrecord.ProjectRecord],
		etcdstore.Versioned[zonerecord.Record],
		etcd.EnvironmentZoneRemovalAuthorities,
		deletionrecord.DeletionTombstoneRecord,
		etcd.ZoneRemovalIntent,
		etcd.TaskRecord,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type zoneDeletionEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   idempotencyrecord.ProtectedIntentRecord
}

type zoneDeletionIdempotency interface {
	ResolveReplayLocator(
		context.Context,
		idempotencyrecord.IdempotencyReplayTarget,
		string,
		string,
		string,
	) (idempotencyrecord.IdempotencyLocator, bool, error)
	Prepare(context.Context, idempotencyrecord.IdempotencyLocator, string, string) (zoneDeletionEvidence, error)
	ResolveExisting(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		zoneDeletionEvidence,
	) (requestidempotency.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		zoneDeletionEvidence,
		etcd.IdempotencyTransactionResult,
	) (requestidempotency.Resolution, error)
	ResolveUnknown(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		zoneDeletionEvidence,
		error,
	) (requestidempotency.Resolution, error)
	MatchesStaged(context.Context, zoneDeletionEvidence, idempotencyrecord.ProtectedIntentRecord) (bool, error)
}

type durableZoneDeletionIdempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  idempotencyEvidenceRepository
}

func newDurableZoneDeletionIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository idempotencyEvidenceRepository,
) (*durableZoneDeletionIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Zone deletion idempotency is not configured")
	}
	return &durableZoneDeletionIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableZoneDeletionIdempotency) ResolveReplayLocator(
	ctx context.Context,
	target idempotencyrecord.IdempotencyReplayTarget,
	method string,
	route string,
	key string,
) (idempotencyrecord.IdempotencyLocator, bool, error) {
	return service.repository.ResolveReplayLocator(ctx, target, method, route, key)
}

func (service *durableZoneDeletionIdempotency) Prepare(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	zoneID string,
	impactToken string,
) (zoneDeletionEvidence, error) {
	if locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		ids.Validate(ids.KindEnvironment, locator.ScopeID) != nil {
		return zoneDeletionEvidence{}, errs.New(errs.KindInternal, "Zone deletion replay scope is invalid")
	}
	query := requestidempotency.Object()
	if impactToken != "" {
		query = requestidempotency.Object(requestidempotency.Field{
			Name: "impact_token", Value: requestidempotency.String(impactToken),
		})
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodDelete, Route: zoneDeletionRoute,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: locator.ScopeID},
		Path:  []requestidempotency.PathBinding{{Name: "id", Value: zoneID}},
		Query: query, Body: requestidempotency.NoBody(),
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
	locator idempotencyrecord.IdempotencyLocator,
	evidence zoneDeletionEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableZoneDeletionIdempotency) ResolveKnown(
	ctx context.Context,
	evidence zoneDeletionEvidence,
	result etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableZoneDeletionIdempotency) ResolveUnknown(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence zoneDeletionEvidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

func (service *durableZoneDeletionIdempotency) MatchesStaged(
	ctx context.Context,
	evidence zoneDeletionEvidence,
	existing idempotencyrecord.ProtectedIntentRecord,
) (bool, error) {
	return service.coordinator.MatchesDurable(ctx, evidence.candidate, existing)
}

type zoneDeletionPlanResolver interface {
	ResolveExecutionPlan(context.Context, etcd.TaskRecord) (*agentpb.ExecutionPlan, error)
	PrepareZoneRemovalTask(
		context.Context,
		etcd.TaskRecord,
		etcd.ZoneRemovalIntent,
		taskplanning.ZoneRemovalTaskProcedureIDs,
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
) (idempotencyrecord.IdempotencyResponse, error) {
	if service == nil || service.deletions == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Zone deletion service is not configured")
	}
	return service.RemoveZoneWithImpact(ctx, zoneID, idempotencyKey, "")
}

func (service *zoneCreationService) RemoveZoneWithImpact(
	ctx context.Context,
	zoneID string,
	idempotencyKey string,
	impactToken string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if service == nil || service.deletions == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Zone deletion service is not configured")
	}
	return service.deletions.RemoveZoneWithImpact(ctx, zoneID, idempotencyKey, impactToken)
}

func (service *zoneDeletionService) RemoveZone(
	ctx context.Context,
	zoneID string,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	return service.RemoveZoneWithImpact(ctx, zoneID, idempotencyKey, "")
}

func (service *zoneDeletionService) RemoveZoneWithImpact(
	ctx context.Context,
	zoneID string,
	idempotencyKey string,
	impactToken string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Zone deletion context is required")
	}
	if ids.Validate(ids.KindNetwork, zoneID) != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Zone id is invalid")
	}
	for attempt := 0; attempt < maximumZoneDeletionAttempts; attempt++ {
		response, err := service.removeZoneOnce(ctx, zoneID, idempotencyKey, impactToken)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumZoneDeletionAttempts-1 {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Zone deletion retry bound was not enforced")
}

func (service *zoneDeletionService) removeZoneOnce(
	ctx context.Context,
	zoneID string,
	idempotencyKey string,
	impactToken string,
) (idempotencyrecord.IdempotencyResponse, error) {
	target := idempotencyrecord.IdempotencyReplayTarget{Kind: idempotencyrecord.IdempotencyReplayTargetZone, ID: zoneID}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx, target, http.MethodDelete, zoneDeletionRoute, idempotencyKey,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if indexed {
		return service.replayZoneDeletion(ctx, locator, target, impactToken)
	}
	zone, err := service.repository.GetZone(ctx, zoneID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	backing := zone.Record.Desired.OwnerKind == core.ZoneOwnerBackingProject
	if backing {
		if service.impacts == nil {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Zone removal impact service is not configured",
			)
		}
		impact, impactErr := service.impacts.GetZoneRemovalImpact(ctx, zoneID)
		if impactErr != nil {
			return idempotencyrecord.IdempotencyResponse{}, impactErr
		}
		if impactToken == "" || impact.ImpactToken != impactToken {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindStateConflict, "backing Zone removal impact changed")
		}
	}
	environment, err := service.repository.GetEnvironment(ctx, zone.Record.EnvironmentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindResourceInUse, "Zone Environment is not ready")
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	taskOwner, err := taskjournal.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	ordinaryOwnership := project.Record.Kind == hierarchyrecord.ProjectKindTenant &&
		zone.Record.Desired.OwnerKind == core.ZoneOwnerEnvironment && zone.Record.Desired.OwnerID == environment.Record.ID
	backingOwnership := project.Record.Kind == hierarchyrecord.ProjectKindBacking &&
		zone.Record.Desired.OwnerKind == core.ZoneOwnerBackingProject && zone.Record.Desired.OwnerID == project.Record.ID
	if (!backing && !ordinaryOwnership) || (backing && !backingOwnership) {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindStateConflict, "Zone ownership is inconsistent")
	}
	authorities, found, err := service.repository.GetEnvironmentZoneRemovalAuthorities(ctx, environment.Record.ID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if !found {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindStateConflict, "Zone desired revision is missing")
	}
	projection := authorities.Desired
	locator = idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment, ScopeID: environment.Record.ID,
		Method: http.MethodDelete, Route: zoneDeletionRoute, Key: idempotencyKey,
	}
	evidence, err := service.idempotency.Prepare(ctx, locator, zoneID, impactToken)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != requestidempotency.ResolutionReplay {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Zone deletion replay resolution is invalid")
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
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	projectionEvidence, err := controllerrevision.PreflightProjection(candidate)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	claim, err := service.repository.ClaimEnvironmentBlueprintStage(ctx, etcd.EnvironmentBlueprintStageClaimRequest{
		EnvironmentID: environment.Record.ID, CandidateRevisionID: candidateRevisionID,
		CandidateTaskID: candidateRevisionID, Locator: locator, Intent: evidence.durable,
		BaselineHeadRevision: projection.Revision, SourceKind: etcd.EnvironmentBlueprintSourceMutation,
		RenderGeneration: candidate.RenderGeneration, ProjectionSchema: etcd.EnvironmentDesiredProjectionSchema,
		CreatedAt: now,
	})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if claim.Existing {
		matched, matchErr := service.idempotency.MatchesStaged(ctx, evidence, claim.Intent)
		if matchErr != nil {
			return idempotencyrecord.IdempotencyResponse{}, matchErr
		}
		if !matched {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(
				errs.KindIdempotencyMismatch,
				"idempotency key was used for another Zone removal",
			)
		}
		candidate, affected, err = buildZoneRemovalProjection(
			projection.Record, zone.Record.Desired.ID, zone.Record.Desired.Name,
			claim.RevisionID, claim.RenderGeneration,
		)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		projectionEvidence, err = controllerrevision.PreflightProjection(candidate)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		now = claim.CreatedAt
	}
	if claim.EnvironmentID != environment.Record.ID || claim.RevisionID != claim.TaskID ||
		claim.Locator != locator || claim.BaselineHeadRevision != projection.Revision ||
		claim.SourceKind != etcd.EnvironmentBlueprintSourceMutation ||
		claim.RenderGeneration != candidate.RenderGeneration ||
		claim.ProjectionSchema != etcd.EnvironmentDesiredProjectionSchema {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindStateConflict, "Zone staged baseline changed")
	}
	task := etcd.TaskRecord{
		ID: claim.RevisionID, OperationID: ids.New(ids.KindOperation), IdempotencyKey: idempotencyKey,
		Owner: taskOwner, Actor: taskjournal.TaskActorOperator,
		Executor: taskjournal.TaskExecutorAgent, PlanID: zoneStableIDFromRevision(ids.KindPlan, claim.RevisionID),
		Type: taskjournal.TaskRemove, Target: zoneID,
		TimeoutSeconds: zoneDeletionTimeoutSeconds,
		Status:         taskjournal.TaskStatusPending, NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	intent, err := etcd.NewZoneRemovalIntent(
		task.OperationID, task.ID, zone, authorities, claim, candidate, affected, now,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if backing {
		task.Executor = taskjournal.TaskExecutorController
		task.Params = map[string]string{
			taskjournal.TaskResourceKindParam:          taskjournal.TaskResourceBackingZone,
			taskjournal.TaskZoneEnvironmentParam:       environment.Record.ID,
			taskjournal.TaskZoneImpactTokenParam:       impactToken,
			taskjournal.TaskZoneRemovalOperationParam:  task.OperationID,
			blueprints.EnvironmentDesiredRevisionParam: claim.RevisionID,
		}
		task.RenderGeneration = int32(candidate.RenderGeneration)
		task.Steps = []taskjournal.TaskStepRecord{{Kind: taskjournal.TaskStepOperation, ID: ids.New(ids.KindStep)}}
		task.TimeoutSeconds = backingZoneCascadeTimeoutSeconds
		task.PlanHash, err = backingZoneCascadePlanHash(intent, impactToken)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	} else {
		serviceSteps := make([]string, len(affected))
		for index := range serviceSteps {
			serviceSteps[index] = ids.New(ids.KindStep)
		}
		task, err = service.plans.PrepareZoneRemovalTask(ctx, task, intent, taskplanning.ZoneRemovalTaskProcedureIDs{
			ArtifactID:     zoneStableIDFromRevision(ids.KindConfig, claim.RevisionID),
			ServiceStepIDs: serviceSteps, NetworkStepID: ids.New(ids.KindStep),
		})
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	if candidate.RenderGeneration > math.MaxInt32 {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
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
		return idempotencyrecord.IdempotencyResponse{}, err
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
		Locator: locator, ReplayTarget: &target, Intent: claim.Intent, Response: response,
		TaskID: task.ID, CreatedAt: now, UpdatedAt: now,
	}
	tombstone := deletionrecord.DeletionTombstoneRecord{
		TargetKind: deletionrecord.DeletionTargetZone, TargetID: zoneID, TargetRevision: zone.Revision,
		TaskID: task.ID, Phase: deletionrecord.DeletionPhaseHostEffects, CreatedAt: now, UpdatedAt: now,
	}
	result, mutationErr := service.repository.BeginZoneDeletionWithTask(
		ctx, environment, project, zone, authorities, tombstone, intent, task, marker,
	)
	if mutationErr != nil {
		if !isUnknownZoneDeletionOutcome(mutationErr) {
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
		return cloneIdempotencyResponse(response), nil
	case requestidempotency.ResolutionReplay:
		return cloneIdempotencyResponse(resolution.Response), nil
	default:
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Zone deletion resolution is invalid")
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
	locator idempotencyrecord.IdempotencyLocator,
	target idempotencyrecord.IdempotencyReplayTarget,
	impactToken string,
) (idempotencyrecord.IdempotencyResponse, error) {
	evidence, err := service.idempotency.Prepare(ctx, locator, target.ID, impactToken)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if !existing || resolution.Kind != requestidempotency.ResolutionReplay {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Zone deletion replay target is inconsistent")
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
