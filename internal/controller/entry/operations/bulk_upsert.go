package operations

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	controllerrevision "github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
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
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Entry bulk upsert context is required")
	}
	input, err := prepareEntryBulkUpsert(request)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	for attempt := 0; attempt < maximumEntryEditAttempts; attempt++ {
		evidence, err := service.idempotency.Prepare(ctx, input)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
		response, mutationErr := service.bulkUpsertOnce(ctx, input, idempotencyKey, evidence)
		clear(evidence.durable.Ciphertext)
		if mutationErr == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(mutationErr)
		if !ok || kind != errs.KindStateConflict || attempt == maximumEntryEditAttempts-1 {
			return etcd.IdempotencyResponse{}, mutationErr
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Entry bulk upsert retry bound was not enforced")
}

func (service *entryBulkUpsertService) bulkUpsertOnce(
	ctx context.Context,
	input entryBulkUpsertInput,
	idempotencyKey string,
	evidence entryBulkUpsertEvidence,
) (etcd.IdempotencyResponse, error) {
	locator := etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeEnvironment,
		ScopeID:   input.environmentID,
		Method:    http.MethodPost,
		Route:     entryBulkUpsertRoute,
		Key:       idempotencyKey,
	}
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != requestidempotency.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Entry bulk upsert replay resolution is invalid",
			)
		}
		return requestidempotency.CloneResponse(resolution.Response), nil
	}
	environment, err := service.desired.repository.GetEnvironment(ctx, input.environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	project, err := service.desired.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if _, err := service.desired.repository.GetTenant(ctx, project.Record.TenantID); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if project.Record.Kind != hierarchyrecord.ProjectKindTenant ||
		environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindResourceInUse,
			"Environment is not ready for Entry mutation",
		)
	}
	if err := service.desired.validateExposure(ctx, input.environmentID, input.exposure); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	head, hasHead, err := service.desired.repository.GetEnvironmentBlueprintHead(ctx, input.environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	current, hasCurrent, err := service.desired.repository.GetEnvironmentComposeProjection(ctx, input.environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	headRevision, generation, err := controllerrevision.NextGeneration(input.environmentID, head, hasHead, current, hasCurrent)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if !hasCurrent || generation > math.MaxInt32 {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict,
			"Entry mutation requires initialized Environment desired state",
		)
	}
	runtime, err := service.desired.plans.CaptureEntryMutationRuntime(ctx, current)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	now := service.desired.now().UTC()
	candidateTaskID := ids.New(ids.KindTask)
	candidateRecords, err := buildEntryBulkCandidate(current.Record, input, candidateTaskID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	candidate, _, err := taskplanning.ProjectEnvironmentEntryMutation(
		runtime.Projection,
		taskplanning.EnvironmentEntryArtifactMutation{
			RevisionID:       candidateTaskID,
			ArtifactID:       entryStableIDFromRevision(ids.KindConfig, candidateTaskID),
			PlanID:           entryStableIDFromRevision(ids.KindPlan, candidateTaskID),
			RenderGeneration: generation,
			Entries:          candidateRecords.entries,
		},
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
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
			MatchExistingIntent: func(ctx context.Context, existing etcd.ProtectedIntentRecord) (bool, error) {
				return service.idempotency.MatchesStaged(ctx, evidence, existing)
			},
			BaselineHeadRevision: headRevision,
			SourceKind:           etcd.EnvironmentBlueprintSourceMutation,
			RenderGeneration:     generation,
			CreatedAt:            now,
		},
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	candidateRecords, err = buildEntryBulkCandidate(current.Record, input, claim.TaskID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	for _, change := range candidateRecords.changes {
		if err := service.desired.prepareEntryGeneration(
			ctx, project.Record.ID, input.environmentID, change.desired, change.record, claim.CreatedAt,
		); err != nil {
			return etcd.IdempotencyResponse{}, err
		}
	}
	candidate, materializations, err := taskplanning.ProjectEnvironmentEntryMutation(
		runtime.Projection,
		taskplanning.EnvironmentEntryArtifactMutation{
			RevisionID:       claim.RevisionID,
			ArtifactID:       entryStableIDFromRevision(ids.KindConfig, claim.RevisionID),
			PlanID:           entryStableIDFromRevision(ids.KindPlan, claim.RevisionID),
			RenderGeneration: generation,
			Entries:          candidateRecords.entries,
		},
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	candidate = controllerrevision.CloneProjection(candidate)
	serviceIdentities, err := entryDesiredServiceIdentities(candidate)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	removals, err := PlanEntryRemovals(
		input.environmentID, candidateRecords.previous, candidateRecords.entries, serviceIdentities,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	materializations = append(materializations, removals...)
	allocator, err := controllerrevision.NewBlueprintIdentityAllocator(claim)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	materializationRecords, err := service.desired.entryMaterializations(
		ctx, input.environmentID, allocator, materializations,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	planID := entryStableIDFromRevision(ids.KindPlan, claim.RevisionID)
	owner, err := etcd.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	task := etcd.TaskRecord{
		ID:                claim.TaskID,
		OperationID:       allocator.Named(ids.KindOperation, "entry-bulk-operation"),
		IdempotencyKey:    idempotencyKey,
		Owner:             owner,
		Actor:             etcd.TaskActorOperator,
		Executor:          etcd.TaskExecutorAgent,
		PlanID:            planID,
		RenderGeneration:  int32(generation),
		Type:              etcd.TaskUpdate,
		Target:            input.environmentID,
		Materializations:  materializationRecords,
		TimeoutSeconds:    controllerrevision.TaskTimeoutSeconds,
		Status:            etcd.TaskStatusPending,
		NextEventSequence: 1,
		CreatedAt:         claim.CreatedAt,
		UpdatedAt:         claim.CreatedAt,
	}
	task, err = runtime.PrepareTask(service.desired.volumeRoot, task, candidate,
		allocator.Named(ids.KindStep, "entry-compose-apply"))
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	response, err := entryBulkUpsertResponse(candidateRecords.changes, claim.TaskID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(response.Body)
	marker := etcd.IdempotencyMarker{
		Kind:      etcd.IdempotencyMarkerTask,
		State:     etcd.IdempotencyMarkerPending,
		Locator:   locator,
		Intent:    claim.Intent,
		Response:  response,
		TaskID:    task.ID,
		CreatedAt: claim.CreatedAt,
		UpdatedAt: claim.CreatedAt,
	}
	projectionEvidence, err := controllerrevision.PreflightProjection(candidate)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	audits := make([]etcd.EnvironmentEntryMutationAudit, len(candidateRecords.changes))
	for index, change := range candidateRecords.changes {
		action := etcd.EnvironmentEntryMutationCreate
		if change.previous != nil {
			action = etcd.EnvironmentEntryMutationEdit
		}
		record := change.record
		if record.Entry.Source.Kind == core.SourceLiteral {
			record.Entry.Source.Literal = ""
		}
		audits[index] = etcd.EnvironmentEntryMutationAudit{
			Action:         action,
			BaseRevisionID: current.Record.RevisionID,
			EntryID:        record.Entry.ID,
			Record:         &record,
		}
	}
	if _, err := service.desired.repository.StageEnvironmentBlueprintRevision(ctx, etcd.EnvironmentBlueprintStageRequest{
		Claim:            claim,
		Mutation:         &etcd.EnvironmentDesiredMutationAudit{Entries: audits},
		Projection:       candidate,
		DependencyDigest: projectionEvidence.DependencyDigest,
	}); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	result, publicationErr := service.desired.repository.PublishEnvironmentDesiredRevisionWithTask(
		ctx, project, environment, headRevision, claim,
		etcd.EnvironmentDesiredRevisionIdentity{EnvironmentID: input.environmentID, RevisionID: claim.RevisionID},
		candidate, nil, nil, nil, etcd.ReleaseGroupBlueprintPreparedMutation{},
		etcd.ComponentTaskPreparation{}, etcd.BlueprintAttachTaskPreparation{}, task, marker,
	)
	if publicationErr != nil {
		if !isUnknownEntryCreationOutcome(publicationErr) {
			return etcd.IdempotencyResponse{}, publicationErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, publicationErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if resolution.Kind == requestidempotency.ResolutionReplay {
		return requestidempotency.CloneResponse(resolution.Response), nil
	}
	if resolution.Kind != requestidempotency.ResolutionApplied {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Entry bulk upsert resolution is invalid")
	}
	return requestidempotency.CloneResponse(response), nil
}
