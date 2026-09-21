package volume

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"math"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/slug"
	"github.com/AlanD20/groundplane/internal/common/volumeidentity"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/volumeremoval"
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
	volumeRoot      string
	repository      MutationRepository
	idempotency     *mutationIdempotency
	reads           *ReadService
	policies        *etcd.BackupPolicyRepository
	removalEvidence *volumeremoval.EvidenceRepository
	now             func() time.Time
}

func NewMutationService(
	volumeRoot string,
	repository MutationRepository,
	coordinator *requestidempotency.Coordinator,
	idempotencyRepository *etcd.IdempotencyRepository,
	reads *ReadService,
	policies *etcd.BackupPolicyRepository,
	removalEvidence *volumeremoval.EvidenceRepository,
) (*MutationService, error) {
	if volumeRoot == "" || repository == nil || reads == nil || policies == nil || removalEvidence == nil {
		return nil, errs.New(errs.KindInternal, "Volume mutation service is not configured")
	}
	idempotency, err := newMutationIdempotency(coordinator, idempotencyRepository)
	if err != nil {
		return nil, err
	}
	return &MutationService{
		volumeRoot: volumeRoot, repository: repository, idempotency: idempotency, reads: reads, now: time.Now,
		policies: policies, removalEvidence: removalEvidence,
	}, nil
}

func (service *MutationService) CreateVolume(
	ctx context.Context,
	input apiTypes.VolumeCreate,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil || ids.Validate(ids.KindEnvironment, input.EnvironmentID) != nil ||
		slug.Validate("volume slug", input.Slug) != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Volume creation input is invalid")
	}
	keySupplied := input.Key != ""
	if input.Key == "" {
		input.Key = input.Slug
	}
	if err := volumeidentity.ValidateKey(input.Key); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	intent := mutationIntent{
		method: http.MethodPost, route: volumeCreationRoute, environmentID: input.EnvironmentID,
		query: requestidempotency.Object(),
		body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "environment_id", Value: requestidempotency.String(input.EnvironmentID)},
			requestidempotency.Field{Name: "key", Value: requestidempotency.String(input.Key)},
			requestidempotency.Field{Name: "slug", Value: requestidempotency.String(input.Slug)},
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
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil || ids.Validate(ids.KindVolume, volumeID) != nil || slug.Validate("volume slug", input.Slug) != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Volume edit input is invalid")
	}
	projection, identity, err := service.repository.FindEnvironmentVolume(ctx, volumeID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	intent := mutationIntent{
		method: http.MethodPatch, route: volumeIdentityRoute,
		environmentID: projection.Record.EnvironmentID, volumeID: volumeID,
		query: requestidempotency.Object(),
		body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "slug", Value: requestidempotency.String(input.Slug)},
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
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil || ids.Validate(ids.KindVolume, volumeID) != nil || len(impactToken) != sha256.Size*2 {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Volume removal input is invalid")
	}
	target := idempotencyrecord.IdempotencyReplayTarget{Kind: idempotencyrecord.IdempotencyReplayTargetVolume, ID: volumeID}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx, target, http.MethodDelete, volumeIdentityRoute, idempotencyKey,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
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
				return idempotencyrecord.IdempotencyResponse{}, replayErr
			}
			if indexed {
				return service.replayVolumeRemoval(ctx, locator, volumeID, impactToken, confirmKey)
			}
		}
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	backupImpact, err := service.repository.ResolveVolumeRemovalImpactAtRevision(
		ctx, projection.Record.EnvironmentID, volumeID, projection.ReadRevision, service.now().UTC(),
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	items := volumeRemovalImpactItems(projection.Record, volumeID, backupImpact)
	expectedImpact, err := volumeImpactDigest(projection.Record, identity, backupImpact, items)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if confirmKey != identity.Key || impactToken != expectedImpact {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
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
	locator idempotencyrecord.IdempotencyLocator,
	volumeID string,
	impactToken string,
	confirmKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	intent := volumeRemovalIntent(locator.ScopeID, volumeID, impactToken, confirmKey)
	evidence, err := service.idempotency.Prepare(ctx, intent)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveOperationRootExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if !existing {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Volume removal replay index is inconsistent")
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
		query: requestidempotency.Object(
			requestidempotency.Field{Name: "confirm_key", Value: requestidempotency.String(confirmKey)},
			requestidempotency.Field{Name: "impact_token", Value: requestidempotency.String(impactToken)},
		),
		body: requestidempotency.NoBody(),
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
) (idempotencyrecord.IdempotencyResponse, error) {
	for attempt := 0; attempt < maximumVolumeMutationAttempts; attempt++ {
		response, err := service.mutateOnce(ctx, request)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumVolumeMutationAttempts-1 {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Volume mutation retry bound was not enforced")
}

func (service *MutationService) mutateOnce(
	ctx context.Context,
	request volumeMutationRequest,
) (idempotencyrecord.IdempotencyResponse, error) {
	evidence, err := service.idempotency.Prepare(ctx, request.intent)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := mutationLocator(request.intent, request.idempotencyKey)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if existing {
		return replayResponse(resolution)
	}
	environment, err := service.repository.GetEnvironment(ctx, request.environmentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	tenant, err := service.repository.GetTenant(ctx, project.Record.TenantID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if project.Record.Kind != hierarchyrecord.ProjectKindTenant ||
		environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindResourceInUse,
			"Environment is not ready for Volume mutation",
		)
	}
	head, hasHead, err := service.repository.GetEnvironmentBlueprintHead(ctx, request.environmentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	current, hasCurrent, err := service.repository.GetEnvironmentComposeProjection(ctx, request.environmentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	expectedHeadRevision, generation, err := desiredState(
		request.environmentID, head, hasHead, current, hasCurrent,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if generation > math.MaxInt32 {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindResourceInUse,
			"Environment render generation is exhausted",
		)
	}
	if err := validateVolumeMutationAgainstProjection(request, current.Record, hasCurrent); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if request.action == volumeMutationActionRemove {
		identity := projectionrecord.EnvironmentVolumeIdentity{}
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
			return idempotencyrecord.IdempotencyResponse{}, impactErr
		}
		items := volumeRemovalImpactItems(current.Record, request.volumeID, backupImpact)
		expectedImpact, impactErr := volumeImpactDigest(current.Record, identity, backupImpact, items)
		if impactErr != nil {
			return idempotencyrecord.IdempotencyResponse{}, impactErr
		}
		if request.impactToken != expectedImpact {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(
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
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	createdAt := service.now().UTC()
	var removalPolicy etcd.VolumeRemovalBackupPolicyPreparation
	if request.action == volumeMutationActionRemove {
		removalPolicy, err = service.policies.PrepareVolumeRemovalBackupPolicy(
			ctx,
			request.environmentID,
			request.volumeID,
			current.ReadRevision,
		)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		candidateProjection.Backup = removalPolicy.Projection()
	}
	claim, _, err := desiredrevision.PreflightAndClaim(
		ctx, service.repository, candidateProjection, desiredrevision.ClaimInput{
			EnvironmentID: request.environmentID, CandidateTaskID: candidateTaskID,
			Locator: locator, Intent: evidence.durable, BaselineHeadRevision: expectedHeadRevision,
			SourceKind: etcd.EnvironmentBlueprintSourceMutation,
			MatchExistingIntent: func(ctx context.Context, existing idempotencyrecord.ProtectedIntentRecord) (bool, error) {
				return service.idempotency.MatchesStaged(ctx, evidence, existing)
			},
			RenderGeneration: generation, CreatedAt: createdAt,
		},
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	taskID := claim.TaskID
	request, candidate, oldArtifact, newArtifact, err := buildVolumeMutationCandidate(
		tenant.Record.ID, project.Record.ID, environment.Record,
		current.Record, hasCurrent, candidateRequest, claim.RevisionID, generation,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	intentDigest, err := hex.DecodeString(claim.Intent.CiphertextDigest)
	if err != nil || len(intentDigest) != sha256.Size {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Volume protected intent digest is invalid")
	}
	if request.action == volumeMutationActionRemove {
		candidate.Backup = removalPolicy.Projection()
	}
	planID := stableIDFromTask(ids.KindPlan, taskID)
	artifactID := stableIDFromTask(ids.KindConfig, taskID)
	plan, steps, err := buildVolumeMutationPlan(
		service.volumeRoot, planID, generation, request, oldArtifact, newArtifact, intentDigest,
		volumeMutationConsumerIDs(current.Record, request.volumeID),
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	projectionEvidence, err := desiredrevision.PreflightProjection(candidate)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	dependencyDigest := projectionEvidence.DependencyDigest
	var precondition [sha256.Size]byte
	if request.action == volumeMutationActionRemove && hasCurrent {
		precondition, err = etcd.EnvironmentBlueprintDependencyDigest(current.Record)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
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
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	owner, err := taskjournal.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	params := map[string]string{
		taskjournal.TaskResourceKindParam:               taskjournal.TaskResourceVolume,
		taskjournal.TaskMaterializationEnvironmentParam: request.environmentID,
		etcd.EnvironmentDesiredRevisionParam:            claim.RevisionID,
		taskjournal.TaskComposeArtifactParam:            artifactID,
		volumeActionParam:                               request.action, volumeComposeKeyParam: request.key,
		taskplanning.VolumeTaskIntentSHA256Param: hex.EncodeToString(intentDigest),
	}
	if request.action == volumeMutationActionRemove && hasCurrent {
		params[volumeBaselineRevisionParam] = current.Record.RevisionID
	}
	task := etcd.TaskRecord{
		ID: taskID, OperationID: stableIDFromTask(ids.KindOperation, taskID),
		IdempotencyKey: request.idempotencyKey, Owner: owner, Actor: taskjournal.TaskActorOperator,
		Executor: taskjournal.TaskExecutorAgent, PlanID: planID, PlanHash: hex.EncodeToString(plan.PlanHash),
		RenderGeneration: int32(generation), Type: volumeMutationTaskType(request.action), Target: request.volumeID,
		Params: params, Steps: steps, TimeoutSeconds: volumeMutationTimeoutSeconds,
		Status: taskjournal.TaskStatusPending, NextEventSequence: 1, CreatedAt: claim.CreatedAt, UpdatedAt: claim.CreatedAt,
	}
	response, err := volumeMutationResponse(request, environment.Record, taskID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	marker := idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
		Locator: locator, Intent: claim.Intent, Response: response, TaskID: taskID,
		CreatedAt: claim.CreatedAt, UpdatedAt: claim.CreatedAt,
	}
	if request.action == volumeMutationActionRemove {
		target := idempotencyrecord.IdempotencyReplayTarget{Kind: idempotencyrecord.IdempotencyReplayTargetVolume, ID: request.volumeID}
		marker.ReplayTarget = &target
	}
	var result etcd.IdempotencyTransactionResult
	var mutationErr error
	if request.action == volumeMutationActionRemove {
		initial, preparationErr := service.prepareRemoval(ctx, current, request, &task, marker)
		if preparationErr != nil {
			return idempotencyrecord.IdempotencyResponse{}, preparationErr
		}
		result, mutationErr = service.repository.PublishEnvironmentVolumeRemovalWithTask(
			ctx, project, environment, expectedHeadRevision, claim, candidate, removalPolicy, initial, task, marker,
		)
	} else {
		result, mutationErr = service.repository.PublishEnvironmentDesiredRevisionWithTask(
			ctx,
			project,
			environment,
			expectedHeadRevision,
			claim,
			etcd.EnvironmentDesiredRevisionIdentity{EnvironmentID: request.environmentID, RevisionID: claim.RevisionID},
			candidate,
			nil,
			nil,
			nil,
			etcd.ReleaseGroupBlueprintPreparedMutation{},
			etcd.ComponentTaskPreparation{},
			etcd.BlueprintAttachTaskPreparation{},
			task,
			marker,
		)
	}
	if mutationErr != nil {
		if !isUnknownPublicationOutcome(mutationErr) {
			return idempotencyrecord.IdempotencyResponse{}, mutationErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, mutationErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if resolution.Kind == requestidempotency.ResolutionReplay {
		return cloneResponse(resolution.Response), nil
	}
	if resolution.Kind != requestidempotency.ResolutionApplied {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Volume mutation resolution is invalid")
	}
	return cloneResponse(response), nil
}
