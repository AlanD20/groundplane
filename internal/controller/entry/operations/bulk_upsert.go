package operations

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	controllerrevision "github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	entrycontroller "github.com/AlanD20/groundplane/internal/controller/entry"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"time"

	"github.com/AlanD20/groundplane/internal/core"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/pkg/errs"
	"math"
)

const entryBulkUpsertRoute = "/entries/bulk"

type entryBulkUpsertService struct {
	desired  *entryDesiredMutationService
	accepted *acceptedaction.Submitter
}

func NewBulkUpsertService(
	desired *entryDesiredMutationService,
	accepted *acceptedaction.Submitter,
) (*entryBulkUpsertService, error) {
	if desired == nil || accepted == nil {
		return nil, errs.New(errs.KindInternal, "Entry bulk upsert service is not configured")
	}
	return &entryBulkUpsertService{desired: desired, accepted: accepted}, nil
}

func (service *entryBulkUpsertService) prepareAcceptedBulk(
	ctx context.Context,
	accepted workqueue.TaskSnapshot,
	_ int64,
	at time.Time,
	request acceptedEntryBulkUpsert,
) (taskdependencies.Prepared, error) {
	defer clear(request.Intent.Ciphertext)
	defer func() {
		for _, entry := range request.Entries {
			clear(entry.Value)
		}
	}()
	input, err := request.input()
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if accepted.Task.TargetID != input.environmentID || accepted.Task.Scope.EnvironmentID != input.environmentID {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindStateConflict,
			"Entry Environment changed after acceptance",
		)
	}
	exposure, err := service.desired.work.ResolveEntryMutationExposure(ctx, input.environmentID, request.Bindings)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	input.exposure, err = normalizeEntryExposure(exposure)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	locator := request.Locator
	environment, err := service.desired.repository.GetEnvironment(ctx, input.environmentID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	project, err := service.desired.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if project.Record.ID != accepted.Task.Scope.ProjectID || project.Record.TenantID != accepted.Task.Scope.TenantID {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindStateConflict,
			"Entry Environment ownership changed after acceptance",
		)
	}
	if _, err := service.desired.repository.GetTenant(ctx, project.Record.TenantID); err != nil {
		return taskdependencies.Prepared{}, err
	}
	if project.Record.Kind != hierarchyrecord.ProjectKindTenant ||
		environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindResourceInUse,
			"Environment is not ready for Entry mutation",
		)
	}
	if err := service.desired.validateExposure(ctx, input.environmentID, input.exposure); err != nil {
		return taskdependencies.Prepared{}, err
	}
	if err := service.desired.repository.InitializeEnvironmentDesiredState(ctx, input.environmentID, locator, request.Intent, at); err != nil {
		return taskdependencies.Prepared{}, err
	}
	head, hasHead, err := service.desired.repository.GetEnvironmentBlueprintHead(ctx, input.environmentID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	source, err := service.desired.repository.ReadEntryMutationSource(ctx, input.environmentID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if source.Desired.Revision != head.Revision || source.Desired.Record.RevisionID != head.Record.RevisionID {
		return taskdependencies.Prepared{}, errs.New(errs.KindStateConflict, "Entry desired source changed")
	}
	if source.Runtime == nil {
		return service.bulkUnrenderedEntries(
			ctx,
			input,
			locator,
			request.Intent,
			source,
			project,
			environment,
			accepted,
			at,
		)
	}
	current, hasCurrent := *source.Runtime, true
	headRevision, generation, err := controllerrevision.NextGeneration(
		input.environmentID,
		head,
		hasHead,
		current,
		hasCurrent,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if !hasCurrent || generation > math.MaxInt32 {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindStateConflict,
			"Entry mutation requires initialized Environment desired state",
		)
	}
	runtime, err := service.desired.runtime.CaptureEntryMutationRuntime(ctx, current)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	candidateTaskID := accepted.Task.ID
	candidateRecords, err := buildEntryBulkCandidate(
		current.Record.EnvironmentID,
		current.Record.Entries,
		input,
		candidateTaskID,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
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
		return taskdependencies.Prepared{}, err
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
			Intent:          request.Intent,
			MatchExistingIntent: func(ctx context.Context, existing idempotencyrecord.ProtectedIntentRecord) (bool, error) {
				return blueprints.SameBlueprintProtectedIntent(existing, request.Intent), nil
			},
			BaselineHeadRevision: headRevision,
			SourceKind:           blueprints.EnvironmentBlueprintSourceMutation,
			RenderGeneration:     generation,
			CreatedAt:            accepted.Task.CreatedAt,
			AcceptedTaskRevision: accepted.Revision,
			TaskLocator:          &request.Locator,
		},
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	keepStage := false
	defer func() {
		if !keepStage {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			_ = service.desired.repository.AbandonEnvironmentBlueprintStage(cleanupCtx, claim)
		}
	}()
	candidateRecords, err = buildEntryBulkCandidate(
		current.Record.EnvironmentID,
		current.Record.Entries,
		input,
		claim.TaskID,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	for _, change := range candidateRecords.changes {
		if err := service.desired.prepareEntryGeneration(
			ctx, project.Record.ID, input.environmentID, change.desired, change.record, at,
		); err != nil {
			return taskdependencies.Prepared{}, err
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
		return taskdependencies.Prepared{}, err
	}
	candidate = controllerrevision.CloneProjection(candidate)
	serviceIdentities, err := entryDesiredServiceIdentities(candidate)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	removals, err := PlanEntryRemovals(
		input.environmentID, candidateRecords.previous, candidateRecords.entries, serviceIdentities,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	materializations = append(materializations, removals...)
	allocator, err := controllerrevision.NewBlueprintIdentityAllocator(claim)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	materializationRecords, err := service.desired.entryMaterializations(
		ctx, input.environmentID, allocator, materializations,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	response, err := entryTaskResponse(claim.TaskID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	defer clear(response.Body)
	marker := idempotencyrecord.IdempotencyMarker{
		Kind:      idempotencyrecord.IdempotencyMarkerTask,
		State:     idempotencyrecord.IdempotencyMarkerPending,
		Locator:   locator,
		Intent:    claim.Intent,
		Response:  response,
		TaskID:    claim.TaskID,
		CreatedAt: claim.CreatedAt,
		UpdatedAt: claim.CreatedAt,
	}
	projectionEvidence, err := controllerrevision.PreflightProjection(candidate)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	desiredInput, err := controllerrevision.DeriveCurrentMutationDesiredInput(
		ctx, service.desired.repository, input.environmentID, current.Record.RevisionID,
		candidate, environment.Record.NetworkPool,
		func(operatorInput *core.BlueprintDesiredInput) error {
			entries, err := entrycontroller.BlueprintAuthoring(candidate.Entries)
			if err != nil {
				return err
			}
			operatorInput.Entries = entries
			return nil
		},
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
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
		DesiredInput:     desiredInput,
		DependencyDigest: projectionEvidence.DependencyDigest,
	}); err != nil {
		return taskdependencies.Prepared{}, err
	}
	prepared, err := service.desired.prepareConfiguration(ctx, entryConfigurationPublication{
		Project: project, Environment: environment, Claim: claim, Runtime: runtime, Accepted: accepted, At: at,
		Candidate: candidate, ReadRevision: current.ReadRevision, Files: materializationRecords,
		Marker: marker, Action: workexecution.ActionUpdate, TargetID: input.environmentID, TargetName: "Entries",
	})
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	priorFinalize := prepared.Finalize
	prepared.Finalize = func(finalCtx context.Context, committed bool) error {
		var finishErr error
		if priorFinalize != nil {
			finishErr = priorFinalize(finalCtx, committed)
		}
		if !committed {
			cleanupErr := service.desired.repository.AbandonEnvironmentBlueprintStage(finalCtx, claim)
			if kind, _ := errs.KindOf(cleanupErr); cleanupErr != nil && kind != errs.KindStateConflict {
				return errs.WrapJoined(errs.KindInternal, finishErr, cleanupErr)
			}
		}
		return finishErr
	}
	keepStage = true
	return prepared, nil
}
