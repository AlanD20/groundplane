package operations

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	controllerrevision "github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	blueprintplanning "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintplanning"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	componentplanning "github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	groupstore "github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"math"
	"net/http"
)

const entryBulkUpsertRoute = "/entries/bulk"

type entryBulkUpsertService struct {
	desired     *entryDesiredMutationService
	idempotency entryBulkUpsertIdempotency
}

func NewBulkUpsertService(
	desired *entryDesiredMutationService,
	idempotency entryBulkUpsertIdempotency,
) (*entryBulkUpsertService, error) {
	if desired == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Entry bulk upsert service is not configured")
	}
	return &entryBulkUpsertService{desired: desired, idempotency: idempotency}, nil
}

func (service *entryBulkUpsertService) BulkUpsertEntries(
	ctx context.Context,
	request apiTypes.EntryBulkUpsertRequest,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Entry bulk upsert context is required",
		)
	}
	input, err := prepareEntryBulkUpsert(request)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	for attempt := 0; attempt < maximumEntryEditAttempts; attempt++ {
		evidence, err := service.idempotency.Prepare(ctx, input)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		response, mutationErr := service.bulkUpsertOnce(ctx, input, idempotencyKey, evidence)
		clear(evidence.durable.Ciphertext)
		if mutationErr == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(mutationErr)
		if !ok || kind != errs.KindStateConflict || attempt == maximumEntryEditAttempts-1 {
			return idempotencyrecord.IdempotencyResponse{}, mutationErr
		}
	}
	return idempotencyrecord.IdempotencyResponse{}, errs.New(
		errs.KindInternal,
		"Entry bulk upsert retry bound was not enforced",
	)
}

func (service *entryBulkUpsertService) bulkUpsertOnce(
	ctx context.Context,
	input entryBulkUpsertInput,
	idempotencyKey string,
	evidence entryBulkUpsertEvidence,
) (idempotencyrecord.IdempotencyResponse, error) {
	locator := idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment,
		ScopeID:   input.environmentID,
		Method:    http.MethodPost,
		Route:     entryBulkUpsertRoute,
		Key:       idempotencyKey,
	}
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != requestidempotency.ResolutionReplay {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Entry bulk upsert replay resolution is invalid",
			)
		}
		return requestidempotency.CloneResponse(resolution.Response), nil
	}
	environment, err := service.desired.repository.GetEnvironment(ctx, input.environmentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	project, err := service.desired.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if _, err := service.desired.repository.GetTenant(ctx, project.Record.TenantID); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if project.Record.Kind != hierarchyrecord.ProjectKindTenant ||
		environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindResourceInUse,
			"Environment is not ready for Entry mutation",
		)
	}
	if err := service.desired.validateExposure(ctx, input.environmentID, input.exposure); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	head, hasHead, err := service.desired.repository.GetEnvironmentBlueprintHead(ctx, input.environmentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	current, hasCurrent, err := service.desired.repository.GetEnvironmentComposeProjection(ctx, input.environmentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	headRevision, generation, err := controllerrevision.NextGeneration(
		input.environmentID,
		head,
		hasHead,
		current,
		hasCurrent,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if !hasCurrent || generation > math.MaxInt32 {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict,
			"Entry mutation requires initialized Environment desired state",
		)
	}
	runtime, err := service.desired.plans.CaptureEntryMutationRuntime(ctx, current)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	now := service.desired.now().UTC()
	candidateTaskID := ids.New(ids.KindTask)
	candidateRecords, err := buildEntryBulkCandidate(current.Record, input, candidateTaskID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	candidate, _, err := composerender.ProjectEnvironmentEntryMutation(
		runtime.Projection,
		composerender.EnvironmentEntryArtifactMutation{
			RevisionID:       candidateTaskID,
			ArtifactID:       entryStableIDFromRevision(ids.KindConfig, candidateTaskID),
			PlanID:           entryStableIDFromRevision(ids.KindPlan, candidateTaskID),
			RenderGeneration: generation,
			Entries:          candidateRecords.entries,
		},
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	candidate = controllerrevision.CloneProjection(candidate)
	claim, _, err := controllerrevision.PreflightAndClaim(
		ctx,
		service.desired.repository,
		candidate,
		controllerrevision.ClaimInput{
			EnvironmentID:   input.environmentID,
			CandidateTaskID: candidateTaskID,
			Locator:         locator,
			Intent:          evidence.durable,
			MatchExistingIntent: func(ctx context.Context, existing idempotencyrecord.ProtectedIntentRecord) (bool, error) {
				return service.idempotency.MatchesStaged(ctx, evidence, existing)
			},
			BaselineHeadRevision: headRevision,
			SourceKind:           blueprints.EnvironmentBlueprintSourceMutation,
			RenderGeneration:     generation,
			CreatedAt:            now,
		},
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	candidateRecords, err = buildEntryBulkCandidate(current.Record, input, claim.TaskID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	for _, change := range candidateRecords.changes {
		if err := service.desired.prepareEntryGeneration(
			ctx, project.Record.ID, input.environmentID, change.desired, change.record, claim.CreatedAt,
		); err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	candidate, materializations, err := composerender.ProjectEnvironmentEntryMutation(
		runtime.Projection,
		composerender.EnvironmentEntryArtifactMutation{
			RevisionID:       claim.RevisionID,
			ArtifactID:       entryStableIDFromRevision(ids.KindConfig, claim.RevisionID),
			PlanID:           entryStableIDFromRevision(ids.KindPlan, claim.RevisionID),
			RenderGeneration: generation,
			Entries:          candidateRecords.entries,
		},
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	candidate = controllerrevision.CloneProjection(candidate)
	serviceIdentities, err := entryDesiredServiceIdentities(candidate)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	removals, err := PlanEntryRemovals(
		input.environmentID, candidateRecords.previous, candidateRecords.entries, serviceIdentities,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	materializations = append(materializations, removals...)
	allocator, err := controllerrevision.NewBlueprintIdentityAllocator(claim)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	materializationRecords, err := service.desired.entryMaterializations(
		ctx, input.environmentID, allocator, materializations,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	planID := entryStableIDFromRevision(ids.KindPlan, claim.RevisionID)
	owner, err := taskjournal.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	task := etcd.TaskRecord{
		ID:                claim.TaskID,
		OperationID:       allocator.Named(ids.KindOperation, "entry-bulk-operation"),
		IdempotencyKey:    idempotencyKey,
		Owner:             owner,
		Actor:             taskjournal.TaskActorOperator,
		Executor:          taskjournal.TaskExecutorAgent,
		PlanID:            planID,
		RenderGeneration:  int32(generation),
		Type:              taskjournal.TaskUpdate,
		Target:            input.environmentID,
		Materializations:  materializationRecords,
		TimeoutSeconds:    controllerrevision.TaskTimeoutSeconds,
		Status:            taskjournal.TaskStatusPending,
		NextEventSequence: 1,
		CreatedAt:         claim.CreatedAt,
		UpdatedAt:         claim.CreatedAt,
	}
	task, err = runtime.PrepareTask(service.desired.volumeRoot, task, candidate,
		allocator.Named(ids.KindStep, "entry-compose-apply"))
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	response, err := entryBulkUpsertResponse(candidateRecords.changes, claim.TaskID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(response.Body)
	marker := idempotencyrecord.IdempotencyMarker{
		Kind:      idempotencyrecord.IdempotencyMarkerTask,
		State:     idempotencyrecord.IdempotencyMarkerPending,
		Locator:   locator,
		Intent:    claim.Intent,
		Response:  response,
		TaskID:    task.ID,
		CreatedAt: claim.CreatedAt,
		UpdatedAt: claim.CreatedAt,
	}
	projectionEvidence, err := controllerrevision.PreflightProjection(candidate)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	audits := make([]blueprints.EnvironmentEntryMutationAudit, len(candidateRecords.changes))
	for index, change := range candidateRecords.changes {
		action := blueprints.EnvironmentEntryMutationCreate
		if change.previous != nil {
			action = blueprints.EnvironmentEntryMutationEdit
		}
		record := change.record
		if record.Entry.Source.Kind == core.SourceLiteral {
			record.Entry.Source.Literal = ""
		}
		audits[index] = blueprints.EnvironmentEntryMutationAudit{
			Action:         action,
			BaseRevisionID: current.Record.RevisionID,
			EntryID:        record.Entry.ID,
			Record:         &record,
		}
	}
	if _, err := service.desired.repository.StageEnvironmentBlueprintRevision(ctx, blueprints.EnvironmentBlueprintStageRequest{
		Claim:            claim,
		Mutation:         &blueprints.EnvironmentDesiredMutationAudit{Entries: audits},
		Projection:       candidate,
		DependencyDigest: projectionEvidence.DependencyDigest,
	}); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	result, publicationErr := service.desired.repository.PublishEnvironmentDesiredRevisionWithTask(
		ctx, project, environment, headRevision, claim,
		blueprints.EnvironmentDesiredRevisionIdentity{EnvironmentID: input.environmentID, RevisionID: claim.RevisionID},
		candidate, nil, nil, nil, groupstore.ReleaseGroupBlueprintPreparedMutation{},
		componentplanning.ComponentTaskPreparation{}, blueprintplanning.BlueprintAttachTaskPreparation{}, task, marker,
	)
	if publicationErr != nil {
		if !isUnknownEntryCreationOutcome(publicationErr) {
			return idempotencyrecord.IdempotencyResponse{}, publicationErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, publicationErr)
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
			"Entry bulk upsert resolution is invalid",
		)
	}
	return requestidempotency.CloneResponse(response), nil
}
