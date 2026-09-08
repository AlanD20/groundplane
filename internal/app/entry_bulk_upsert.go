package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"net/http"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	controllerrevision "github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const entryBulkUpsertRoute = "/entries/bulk"

type entryBulkPair struct {
	key   string
	value string
}

type entryBulkUpsertInput struct {
	environmentID string
	entries       []entryBulkPair
	exposure      []string
	secret        bool
}

type entryBulkUpsertEvidence struct {
	candidate idempotentintent.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type entryBulkUpsertIdempotency interface {
	Prepare(context.Context, entryBulkUpsertInput) (entryBulkUpsertEvidence, error)
	MatchesStaged(context.Context, entryBulkUpsertEvidence, etcd.ProtectedIntentRecord) (bool, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		entryBulkUpsertEvidence,
	) (idempotentintent.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		entryBulkUpsertEvidence,
		etcd.IdempotencyTransactionResult,
	) (idempotentintent.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		entryBulkUpsertEvidence,
		error,
	) (idempotentintent.Resolution, error)
}

type durableEntryBulkUpsertIdempotency struct {
	coordinator *idempotentintent.Coordinator
	repository  *etcd.IdempotencyRepository
}

func newDurableEntryBulkUpsertIdempotency(
	coordinator *idempotentintent.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableEntryBulkUpsertIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Entry bulk upsert idempotency is not configured")
	}
	return &durableEntryBulkUpsertIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableEntryBulkUpsertIdempotency) Prepare(
	ctx context.Context,
	input entryBulkUpsertInput,
) (entryBulkUpsertEvidence, error) {
	items := make([]idempotentintent.Value, len(input.entries))
	for index, entry := range input.entries {
		digest := sha256.Sum256([]byte(entry.value))
		items[index] = idempotentintent.Object(
			idempotentintent.Field{Name: "key", Value: idempotentintent.String(entry.key)},
			idempotentintent.Field{Name: "value_sha256", Value: idempotentintent.String(hex.EncodeToString(digest[:]))},
		)
	}
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: http.MethodPost,
		Route:  entryBulkUpsertRoute,
		Scope:  idempotentintent.Scope{Kind: idempotentintent.ScopeEnvironment, ID: input.environmentID},
		Query:  idempotentintent.Object(),
		Body: idempotentintent.JSONBody(idempotentintent.Object(
			idempotentintent.Field{Name: "environment_id", Value: idempotentintent.String(input.environmentID)},
			idempotentintent.Field{Name: "entries", Value: idempotentintent.List(items...)},
			idempotentintent.Field{Name: "exposure", Value: canonicalEntryExposure(input.exposure)},
			idempotentintent.Field{Name: "secret", Value: idempotentintent.Bool(input.secret)},
		)),
	})
	if err != nil {
		return entryBulkUpsertEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return entryBulkUpsertEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return entryBulkUpsertEvidence{}, err
	}
	return entryBulkUpsertEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableEntryBulkUpsertIdempotency) MatchesStaged(
	ctx context.Context,
	evidence entryBulkUpsertEvidence,
	existing etcd.ProtectedIntentRecord,
) (bool, error) {
	return service.coordinator.MatchesDurable(ctx, evidence.candidate, existing)
}

func (service *durableEntryBulkUpsertIdempotency) ResolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence entryBulkUpsertEvidence,
) (idempotentintent.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableEntryBulkUpsertIdempotency) ResolveKnown(
	ctx context.Context,
	evidence entryBulkUpsertEvidence,
	result etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableEntryBulkUpsertIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence entryBulkUpsertEvidence,
	original error,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type entryBulkUpsertService struct {
	desired     *entryDesiredMutationService
	idempotency entryBulkUpsertIdempotency
}

func newEntryBulkUpsertService(
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

type entryBulkChange struct {
	desired  core.EnvEntry
	record   etcd.EntryRecord
	previous *etcd.EntryRecord
}

type entryBulkCandidate struct {
	entries  []etcd.EntryRecord
	changes  []entryBulkChange
	previous []etcd.EntryRecord
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
		if resolution.Kind != idempotentintent.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Entry bulk upsert replay resolution is invalid",
			)
		}
		return cloneIdempotencyResponse(resolution.Response), nil
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
	if project.Record.Kind != etcd.ProjectKindTenant ||
		environment.Record.ProvisioningState != etcd.EnvironmentProvisioningReady {
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
	expectedHeadRevision, generation, err := serviceDesiredState(
		input.environmentID,
		head,
		hasHead,
		current,
		hasCurrent,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if !hasCurrent || generation > math.MaxInt32 {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict,
			"Entry mutation requires initialized Environment desired state",
		)
	}
	now := service.desired.now().UTC()
	candidateTaskID := ids.New(ids.KindTask)
	candidateRecords, err := buildEntryBulkCandidate(current.Record, input, candidateTaskID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	candidate, _, err := controller.ProjectEnvironmentEntryMutation(
		current.Record,
		controller.EnvironmentEntryArtifactMutation{
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
	candidate = cloneEnvironmentDesiredProjection(candidate)
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
			BaselineHeadRevision: expectedHeadRevision,
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
	candidate, materializations, err := controller.ProjectEnvironmentEntryMutation(
		current.Record,
		controller.EnvironmentEntryArtifactMutation{
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
	candidate = cloneEnvironmentDesiredProjection(candidate)
	serviceIdentities, err := entryDesiredServiceIdentities(candidate)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	removals, err := environmentBlueprintEntryRemovals(
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
	artifactID := entryStableIDFromRevision(ids.KindConfig, claim.RevisionID)
	materializationRecords, steps, err := service.desired.entryMaterializations(
		ctx, input.environmentID, artifactID, allocator, materializations,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	steps = append(steps, &agentpb.ExecutionStep{
		StepId:         allocator.Named(ids.KindStep, "entry-compose-apply"),
		TimeoutSeconds: uint32(environmentBlueprintTimeoutSeconds),
		Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
			ArtifactId:    artifactID,
			FullReconcile: true,
		}},
	})
	stepRecords := make([]etcd.TaskStepRecord, len(steps))
	for index, step := range steps {
		stepRecords[index] = etcd.TaskStepRecord{Kind: etcd.TaskStepOperation, ID: step.StepId}
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := protoUnmarshalEntryArtifact(candidate.ComposeArtifact, artifact); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	planID := entryStableIDFromRevision(ids.KindPlan, claim.RevisionID)
	plan, err := controller.BuildPlan(controller.PlanBuildInput{
		VolumeRoot:       service.desired.volumeRoot,
		PlanID:           planID,
		RenderGeneration: generation,
		Operation:        agentpb.PlanOperation_PLAN_OPERATION_RECONCILE,
		TargetID:         input.environmentID,
		Artifacts:        []*agentpb.ComposeArtifact{artifact},
		Steps:            steps,
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	owner, err := etcd.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	task := etcd.TaskRecord{
		ID:               claim.TaskID,
		OperationID:      allocator.Named(ids.KindOperation, "entry-bulk-operation"),
		IdempotencyKey:   idempotencyKey,
		Owner:            owner,
		Actor:            etcd.TaskActorOperator,
		Executor:         etcd.TaskExecutorAgent,
		PlanID:           planID,
		PlanHash:         hex.EncodeToString(plan.PlanHash),
		RenderGeneration: int32(generation),
		Type:             etcd.TaskUpdate,
		Target:           input.environmentID,
		Params: map[string]string{
			etcd.EnvironmentDesiredRevisionParam:         claim.RevisionID,
			etcd.TaskMaterializationEnvironmentParam:     input.environmentID,
			controller.EnvironmentBlueprintArtifactParam: artifactID,
			taskcontract.EnvironmentBlueprintProcedureParam: string(
				taskcontract.BlueprintComposeProcedureFullReconcile,
			),
		},
		Steps:             stepRecords,
		Materializations:  materializationRecords,
		TimeoutSeconds:    environmentBlueprintTimeoutSeconds,
		Status:            etcd.TaskStatusPending,
		NextEventSequence: 1,
		CreatedAt:         claim.CreatedAt,
		UpdatedAt:         claim.CreatedAt,
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
		ctx, project, environment, expectedHeadRevision, claim,
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
	if resolution.Kind == idempotentintent.ResolutionReplay {
		return cloneIdempotencyResponse(resolution.Response), nil
	}
	if resolution.Kind != idempotentintent.ResolutionApplied {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Entry bulk upsert resolution is invalid")
	}
	return cloneIdempotencyResponse(response), nil
}

func prepareEntryBulkUpsert(request apiTypes.EntryBulkUpsertRequest) (entryBulkUpsertInput, error) {
	if ids.Validate(ids.KindEnvironment, request.EnvironmentID) != nil {
		return entryBulkUpsertInput{}, errs.New(
			errs.KindValidationFailed,
			"Entry bulk upsert requires a stable Environment id",
		)
	}
	if len(request.Entries) == 0 || len(request.Entries) > apiTypes.MaximumBulkEntryCount {
		return entryBulkUpsertInput{}, errs.Newf(
			errs.KindValidationFailed,
			"Entry bulk upsert requires 1 through %d entries",
			apiTypes.MaximumBulkEntryCount,
		)
	}
	exposure, err := normalizeEntryExposure(request.Exposure)
	if err != nil {
		return entryBulkUpsertInput{}, err
	}
	seen := make(map[string]struct{}, len(request.Entries))
	entries := make([]entryBulkPair, len(request.Entries))
	totalBytes := 0
	for index, item := range request.Entries {
		if _, duplicate := seen[item.Key]; duplicate {
			return entryBulkUpsertInput{}, errs.Newf(errs.KindValidationFailed, "Entry key %q is duplicated", item.Key)
		}
		seen[item.Key] = struct{}{}
		if len(item.Value) > apiTypes.MaximumEntryValueBytes {
			return entryBulkUpsertInput{}, errs.Newf(
				errs.KindValidationFailed,
				"Entry %q literal exceeds the 256 KiB limit",
				item.Key,
			)
		}
		totalBytes += len(item.Key) + len(item.Value)
		if totalBytes > apiTypes.MaximumBulkEntryPayloadBytes {
			return entryBulkUpsertInput{}, errs.New(
				errs.KindValidationFailed,
				"Entry bulk upsert exceeds the 1 MiB value limit",
			)
		}
		validation := core.EnvEntry{
			ID: ids.New(ids.KindEnvEntry), Kind: core.EntryKindEnv, Key: item.Key,
			Source:   core.EntrySource{Kind: core.SourceLiteral, Literal: item.Value},
			Exposure: append([]string(nil), exposure...), Secret: request.Secret,
		}
		if validation.Secret {
			validation.Source.Literal = ""
		}
		if err := validation.Validate(); err != nil {
			return entryBulkUpsertInput{}, errs.Wrap(errs.KindValidationFailed, err)
		}
		entries[index] = entryBulkPair{key: item.Key, value: item.Value}
	}
	sort.Slice(entries, func(left, right int) bool { return entries[left].key < entries[right].key })
	return entryBulkUpsertInput{
		environmentID: request.EnvironmentID,
		entries:       entries,
		exposure:      exposure,
		secret:        request.Secret,
	}, nil
}

func buildEntryBulkCandidate(
	current etcd.EnvironmentComposeProjection,
	input entryBulkUpsertInput,
	revisionID string,
) (entryBulkCandidate, error) {
	byKey := make(map[string]etcd.EntryRecord)
	for _, record := range current.Entries {
		if record.Entry.Kind != core.EntryKindEnv {
			continue
		}
		if _, duplicate := byKey[record.Entry.Key]; duplicate {
			return entryBulkCandidate{}, errs.New(
				errs.KindInternal,
				"Environment Entry projection has a duplicated env key",
			)
		}
		byKey[record.Entry.Key] = record
	}
	changes := make([]entryBulkChange, 0, len(input.entries))
	previous := make([]etcd.EntryRecord, 0, len(input.entries))
	replaced := make(map[string]struct{}, len(input.entries))
	for _, item := range input.entries {
		desired := core.EnvEntry{
			Kind: core.EntryKindEnv, Key: item.key,
			Source:   core.EntrySource{Kind: core.SourceLiteral, Literal: item.value},
			Exposure: append([]string(nil), input.exposure...), Secret: input.secret,
		}
		var prior *etcd.EntryRecord
		if existing, found := byKey[item.key]; found {
			if existing.Entry.Secret != input.secret {
				return entryBulkCandidate{}, errs.Newf(
					errs.KindValidationFailed, "Entry %q already uses the %s storage class", item.key,
					entryStorageClass(existing.Entry.Secret),
				)
			}
			value := existing
			prior = &value
			previous = append(previous, value)
			desired.ID = existing.Entry.ID
			replaced[existing.Entry.ID] = struct{}{}
		} else {
			entryID, err := deriveEntryBulkID(ids.KindEnvEntry, revisionID, "entry/"+item.key)
			if err != nil {
				return entryBulkCandidate{}, err
			}
			desired.ID = entryID
		}
		generationID, err := deriveEntryBulkID(ids.KindConfig, revisionID, "value/"+item.key)
		if err != nil {
			return entryBulkCandidate{}, err
		}
		persisted := desired
		if persisted.Secret {
			persisted.Source.Literal = ""
		}
		record, err := etcd.NewEntryRecord(current.EnvironmentID, persisted, generationID)
		if err != nil {
			return entryBulkCandidate{}, err
		}
		if prior != nil {
			record.BlueprintKey = prior.BlueprintKey
		}
		changes = append(changes, entryBulkChange{desired: desired, record: record, previous: prior})
	}
	entries := make([]etcd.EntryRecord, 0, len(current.Entries)+len(changes))
	for _, record := range current.Entries {
		if _, drop := replaced[record.Entry.ID]; !drop {
			entries = append(entries, record)
		}
	}
	for _, change := range changes {
		entries = append(entries, change.record)
	}
	sort.Slice(entries, func(left, right int) bool { return entries[left].Entry.ID < entries[right].Entry.ID })
	sort.Slice(previous, func(left, right int) bool { return previous[left].Entry.ID < previous[right].Entry.ID })
	return entryBulkCandidate{entries: entries, changes: changes, previous: previous}, nil
}

func deriveEntryBulkID(kind ids.Kind, revisionID string, purpose string) (string, error) {
	createdAt, err := ids.Timestamp(ids.KindTask, revisionID)
	if err != nil {
		return "", errs.New(errs.KindInternal, "Entry bulk upsert revision identity is invalid")
	}
	return ids.DeriveAt(kind, createdAt, revisionID, "entry-bulk/"+purpose), nil
}

func entryStorageClass(secret bool) string {
	if secret {
		return "secret"
	}
	return "plain"
}

func entryBulkUpsertResponse(changes []entryBulkChange, taskID string) (etcd.IdempotencyResponse, error) {
	entries := make([]apiTypes.Entry, len(changes))
	for index, change := range changes {
		entries[index] = entryCreationResponse(change.record.Entry)
	}
	body, err := json.Marshal(apiTypes.EntryBulkUpsertResult{TaskID: taskID, Entries: entries})
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	return etcd.IdempotencyResponse{Status: http.StatusAccepted, ContentKind: "application/json", Body: body}, nil
}
