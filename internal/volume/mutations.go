package volume

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"math"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/slug"
	"github.com/AlanD20/groundplane/internal/common/volumeidentity"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	volumeCreationRoute           = "/volumes"
	volumeIdentityRoute           = "/volumes/{id}"
	volumeMutationTimeoutSeconds  = int64(120)
	maximumVolumeMutationAttempts = 3
	volumeActionParam             = "volume_action"
	volumeComposeKeyParam         = "volume_compose_key"
	volumeBaselineRevisionParam   = "volume_baseline_revision_id"
	volumeMutationActionAdd       = "add"
	volumeMutationActionEdit      = "edit"
	volumeMutationActionRemove    = "remove"
)

type MutationService struct {
	volumeRoot  string
	repository  MutationRepository
	idempotency *mutationIdempotency
	reads       *ReadService
	now         func() time.Time
}

func NewMutationService(
	volumeRoot string,
	repository MutationRepository,
	coordinator *idempotentintent.Coordinator,
	idempotencyRepository *etcd.IdempotencyRepository,
	reads *ReadService,
) (*MutationService, error) {
	if volumeRoot == "" || repository == nil || reads == nil {
		return nil, errs.New(errs.KindInternal, "Volume mutation service is not configured")
	}
	idempotency, err := newMutationIdempotency(coordinator, idempotencyRepository)
	if err != nil {
		return nil, err
	}
	return &MutationService{
		volumeRoot: volumeRoot, repository: repository, idempotency: idempotency, reads: reads, now: time.Now,
	}, nil
}

func (service *MutationService) CreateVolume(
	ctx context.Context,
	input apiTypes.VolumeCreate,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil || ids.Validate(ids.KindEnvironment, input.EnvironmentID) != nil ||
		slug.Validate("volume slug", input.Slug) != nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Volume creation input is invalid")
	}
	keySupplied := input.Key != ""
	if input.Key == "" {
		input.Key = input.Slug
	}
	if err := volumeidentity.ValidateKey(input.Key); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	intent := mutationIntent{
		method: http.MethodPost, route: volumeCreationRoute, environmentID: input.EnvironmentID,
		query: idempotentintent.Object(),
		body: idempotentintent.JSONBody(idempotentintent.Object(
			idempotentintent.Field{Name: "environment_id", Value: idempotentintent.String(input.EnvironmentID)},
			idempotentintent.Field{Name: "key", Value: idempotentintent.String(input.Key)},
			idempotentintent.Field{Name: "slug", Value: idempotentintent.String(input.Slug)},
		)),
	}
	return service.mutateWithRetry(ctx, volumeMutationRequest{
		action: volumeMutationActionAdd, environmentID: input.EnvironmentID,
		slug: input.Slug, key: input.Key, keySupplied: keySupplied,
		idempotencyKey: idempotencyKey, intent: intent,
	})
}

func (service *MutationService) EditVolume(
	ctx context.Context,
	volumeID string,
	input apiTypes.VolumeEdit,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil || ids.Validate(ids.KindVolume, volumeID) != nil || slug.Validate("volume slug", input.Slug) != nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Volume edit input is invalid")
	}
	projection, identity, err := service.repository.FindEnvironmentVolume(ctx, volumeID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	intent := mutationIntent{
		method: http.MethodPatch, route: volumeIdentityRoute,
		environmentID: projection.Record.EnvironmentID, volumeID: volumeID,
		query: idempotentintent.Object(),
		body: idempotentintent.JSONBody(idempotentintent.Object(
			idempotentintent.Field{Name: "slug", Value: idempotentintent.String(input.Slug)},
		)),
	}
	return service.mutateWithRetry(ctx, volumeMutationRequest{
		action: volumeMutationActionEdit, environmentID: projection.Record.EnvironmentID,
		volumeID: volumeID, slug: input.Slug, key: identity.Key,
		idempotencyKey: idempotencyKey, intent: intent,
	})
}

func (service *MutationService) RemoveVolume(
	ctx context.Context,
	volumeID string,
	impactToken string,
	confirmKey string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil || ids.Validate(ids.KindVolume, volumeID) != nil || len(impactToken) != sha256.Size*2 {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Volume removal input is invalid")
	}
	target := etcd.IdempotencyReplayTarget{Kind: etcd.IdempotencyReplayTargetVolume, ID: volumeID}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx, target, http.MethodDelete, volumeIdentityRoute, idempotencyKey,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if indexed {
		return service.replayVolumeRemoval(ctx, locator, volumeID, impactToken, confirmKey)
	}
	projection, identity, err := service.repository.FindEnvironmentVolume(ctx, volumeID)
	if err != nil {
		if kind, ok := errs.KindOf(err); ok && kind == errs.KindVolumeNotFound {
			locator, indexed, replayErr := service.idempotency.ResolveReplayLocator(
				ctx, target, http.MethodDelete, volumeIdentityRoute, idempotencyKey,
			)
			if replayErr != nil {
				return etcd.IdempotencyResponse{}, replayErr
			}
			if indexed {
				return service.replayVolumeRemoval(ctx, locator, volumeID, impactToken, confirmKey)
			}
		}
		return etcd.IdempotencyResponse{}, err
	}
	backupImpact, err := service.repository.ResolveVolumeRemovalImpactAtRevision(
		ctx, projection.Record.EnvironmentID, volumeID, projection.ReadRevision, service.now().UTC(),
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	items := volumeRemovalImpactItems(projection.Record, volumeID, backupImpact)
	expectedImpact, err := volumeImpactDigest(projection.Record, identity, backupImpact, items)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if confirmKey != identity.Key || impactToken != expectedImpact {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict, "Volume removal confirmation does not match the current fixed-revision impact",
		)
	}
	intent := volumeRemovalIntent(projection.Record.EnvironmentID, volumeID, impactToken, confirmKey)
	return service.mutateWithRetry(ctx, volumeMutationRequest{
		action: volumeMutationActionRemove, environmentID: projection.Record.EnvironmentID,
		volumeID: volumeID, slug: identity.Slug, key: identity.Key, keySupplied: true,
		impactToken: impactToken, idempotencyKey: idempotencyKey, intent: intent,
	})
}

func (service *MutationService) replayVolumeRemoval(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	volumeID string,
	impactToken string,
	confirmKey string,
) (etcd.IdempotencyResponse, error) {
	intent := volumeRemovalIntent(locator.ScopeID, volumeID, impactToken, confirmKey)
	evidence, err := service.idempotency.Prepare(ctx, intent)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveOperationRootExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if !existing {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Volume removal replay index is inconsistent")
	}
	return replayResponse(resolution)
}

func volumeRemovalIntent(
	environmentID string,
	volumeID string,
	impactToken string,
	confirmKey string,
) mutationIntent {
	return mutationIntent{
		method: http.MethodDelete, route: volumeIdentityRoute,
		environmentID: environmentID, volumeID: volumeID,
		query: idempotentintent.Object(
			idempotentintent.Field{Name: "confirm_key", Value: idempotentintent.String(confirmKey)},
			idempotentintent.Field{Name: "impact_token", Value: idempotentintent.String(impactToken)},
		),
		body: idempotentintent.NoBody(),
	}
}

type volumeMutationRequest struct {
	action         string
	environmentID  string
	volumeID       string
	slug           string
	key            string
	keySupplied    bool
	impactToken    string
	idempotencyKey string
	intent         mutationIntent
}

func (service *MutationService) mutateWithRetry(
	ctx context.Context,
	request volumeMutationRequest,
) (etcd.IdempotencyResponse, error) {
	for attempt := 0; attempt < maximumVolumeMutationAttempts; attempt++ {
		response, err := service.mutateOnce(ctx, request)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumVolumeMutationAttempts-1 {
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Volume mutation retry bound was not enforced")
}

func (service *MutationService) mutateOnce(
	ctx context.Context,
	request volumeMutationRequest,
) (etcd.IdempotencyResponse, error) {
	evidence, err := service.idempotency.Prepare(ctx, request.intent)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := mutationLocator(request.intent, request.idempotencyKey)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		return replayResponse(resolution)
	}
	environment, err := service.repository.GetEnvironment(ctx, request.environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	tenant, err := service.repository.GetTenant(ctx, project.Record.TenantID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if project.Record.Kind != etcd.ProjectKindTenant ||
		environment.Record.ProvisioningState != etcd.EnvironmentProvisioningReady {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindResourceInUse, "Environment is not ready for Volume mutation")
	}
	head, hasHead, err := service.repository.GetEnvironmentBlueprintHead(ctx, request.environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	current, hasCurrent, err := service.repository.GetEnvironmentComposeProjection(ctx, request.environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	expectedHeadRevision, generation, err := desiredState(
		request.environmentID, head, hasHead, current, hasCurrent,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if generation > math.MaxInt32 {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindResourceInUse, "Environment render generation is exhausted")
	}
	if err := validateVolumeMutationAgainstProjection(request, current.Record, hasCurrent); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if request.action == volumeMutationActionRemove {
		identity := etcd.EnvironmentVolumeIdentity{}
		for _, candidate := range current.Record.Volumes {
			if candidate.ID == request.volumeID {
				identity = candidate
				break
			}
		}
		backupImpact, impactErr := service.repository.ResolveVolumeRemovalImpactAtRevision(
			ctx, request.environmentID, request.volumeID, current.ReadRevision, service.now().UTC(),
		)
		if impactErr != nil {
			return etcd.IdempotencyResponse{}, impactErr
		}
		items := volumeRemovalImpactItems(current.Record, request.volumeID, backupImpact)
		expectedImpact, impactErr := volumeImpactDigest(current.Record, identity, backupImpact, items)
		if impactErr != nil {
			return etcd.IdempotencyResponse{}, impactErr
		}
		if request.impactToken != expectedImpact {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindStateConflict, "Volume removal impact changed before publication",
			)
		}
	}

	candidateTaskID := ids.New(ids.KindTask)
	candidateRequest, candidateProjection, _, _, err := buildVolumeMutationCandidate(
		tenant.Record.ID, project.Record.ID, environment.Record,
		current.Record, hasCurrent, request, candidateTaskID, generation,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	createdAt := service.now().UTC()
	claim, _, err := desiredrevision.PreflightAndClaim(
		ctx, service.repository, candidateProjection, desiredrevision.ClaimInput{
			EnvironmentID: request.environmentID, CandidateTaskID: candidateTaskID,
			Locator: locator, Intent: evidence.durable, BaselineHeadRevision: expectedHeadRevision,
			SourceKind: etcd.EnvironmentBlueprintSourceMutation,
			MatchExistingIntent: func(ctx context.Context, existing etcd.ProtectedIntentRecord) (bool, error) {
				return service.idempotency.MatchesStaged(ctx, evidence, existing)
			},
			RenderGeneration: generation, CreatedAt: createdAt,
		},
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	taskID := claim.TaskID
	request, candidate, oldArtifact, newArtifact, err := buildVolumeMutationCandidate(
		tenant.Record.ID, project.Record.ID, environment.Record,
		current.Record, hasCurrent, candidateRequest, claim.RevisionID, generation,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	intentDigest, err := hex.DecodeString(claim.Intent.CiphertextDigest)
	if err != nil || len(intentDigest) != sha256.Size {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Volume protected intent digest is invalid")
	}
	planID := stableIDFromTask(ids.KindPlan, taskID)
	artifactID := stableIDFromTask(ids.KindConfig, taskID)
	plan, steps, err := buildVolumeMutationPlan(
		service.volumeRoot, planID, generation, request, oldArtifact, newArtifact, intentDigest,
		volumeMutationConsumerIDs(current.Record, request.volumeID),
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	projectionEvidence, err := desiredrevision.PreflightProjection(candidate)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	dependencyDigest := projectionEvidence.DependencyDigest
	var precondition [sha256.Size]byte
	if request.action == volumeMutationActionRemove && hasCurrent {
		precondition, err = etcd.EnvironmentBlueprintDependencyDigest(current.Record)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
	}
	action := etcd.EnvironmentVolumeMutationAdd
	if request.action == volumeMutationActionEdit {
		action = etcd.EnvironmentVolumeMutationEdit
	} else if request.action == volumeMutationActionRemove {
		action = etcd.EnvironmentVolumeMutationRemove
	}
	if _, err := service.repository.StageEnvironmentBlueprintRevision(ctx, etcd.EnvironmentBlueprintStageRequest{
		Claim: claim,
		Mutation: &etcd.EnvironmentDesiredMutationAudit{Volume: &etcd.EnvironmentVolumeMutationAudit{
			Action: action, VolumeID: request.volumeID, Slug: request.slug, Key: request.key,
			KeySupplied: request.keySupplied, PreconditionDigest: precondition,
		}},
		Projection: candidate, DependencyDigest: dependencyDigest,
	}); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	owner, err := etcd.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	params := map[string]string{
		etcd.TaskResourceKindParam:               etcd.TaskResourceVolume,
		etcd.TaskMaterializationEnvironmentParam: request.environmentID,
		etcd.EnvironmentDesiredRevisionParam:     claim.RevisionID,
		etcd.TaskComposeArtifactParam:            artifactID,
		volumeActionParam:                        request.action, volumeComposeKeyParam: request.key,
		controller.VolumeTaskIntentSHA256Param: hex.EncodeToString(intentDigest),
	}
	if request.action == volumeMutationActionRemove && hasCurrent {
		params[volumeBaselineRevisionParam] = current.Record.RevisionID
	}
	task := etcd.TaskRecord{
		ID: taskID, OperationID: stableIDFromTask(ids.KindOperation, taskID),
		IdempotencyKey: request.idempotencyKey, Owner: owner, Actor: etcd.TaskActorOperator,
		Executor: etcd.TaskExecutorAgent, PlanID: planID, PlanHash: hex.EncodeToString(plan.PlanHash),
		RenderGeneration: int32(generation), Type: volumeMutationTaskType(request.action), Target: request.volumeID,
		Params: params, Steps: steps, TimeoutSeconds: volumeMutationTimeoutSeconds,
		Status: etcd.TaskStatusPending, NextEventSequence: 1, CreatedAt: claim.CreatedAt, UpdatedAt: claim.CreatedAt,
	}
	response, err := volumeMutationResponse(request, environment.Record, taskID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	marker := etcd.IdempotencyMarker{
		Kind: etcd.IdempotencyMarkerTask, State: etcd.IdempotencyMarkerPending,
		Locator: locator, Intent: claim.Intent, Response: response, TaskID: taskID,
		CreatedAt: claim.CreatedAt, UpdatedAt: claim.CreatedAt,
	}
	if request.action == volumeMutationActionRemove {
		target := etcd.IdempotencyReplayTarget{Kind: etcd.IdempotencyReplayTargetVolume, ID: request.volumeID}
		marker.ReplayTarget = &target
	}
	result, mutationErr := service.repository.PublishEnvironmentDesiredRevisionWithTask(
		ctx, project, environment, expectedHeadRevision, claim,
		etcd.EnvironmentDesiredRevisionIdentity{EnvironmentID: request.environmentID, RevisionID: claim.RevisionID},
		candidate, nil, nil, nil, etcd.ReleaseGroupBlueprintPreparedMutation{}, etcd.ComponentTaskPreparation{}, etcd.BlueprintAttachTaskPreparation{}, task, marker,
	)
	if mutationErr != nil {
		if !isUnknownPublicationOutcome(mutationErr) {
			return etcd.IdempotencyResponse{}, mutationErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, mutationErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if resolution.Kind == idempotentintent.ResolutionReplay {
		return cloneResponse(resolution.Response), nil
	}
	if resolution.Kind != idempotentintent.ResolutionApplied {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Volume mutation resolution is invalid")
	}
	return cloneResponse(response), nil
}
