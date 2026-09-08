package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"net/http"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	controllerrevision "github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	entrycontroller "github.com/AlanD20/groundplane/internal/controller/entry"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type entryDesiredMutationRepository interface {
	controllerrevision.Repository
	GetTenant(context.Context, string) (etcd.Versioned[etcd.TenantRecord], error)
	GetProject(context.Context, string) (etcd.Versioned[etcd.ProjectRecord], error)
	GetEnvironment(context.Context, string) (etcd.Versioned[etcd.EnvironmentRecord], error)
	GetEnvironmentBlueprintHead(context.Context, string) (etcd.Versioned[etcd.EnvironmentBlueprintHead], bool, error)
	GetEnvironmentComposeProjection(
		context.Context,
		string,
	) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error)
	ListServices(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.ServiceRecord], error)
	ListEnvironmentComponents(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.ComponentRecord], error)
	ResolveBlueprintEntryEnvironment(context.Context, string) (string, bool, error)
	BlueprintEntryValueGenerationExists(context.Context, etcd.EntryRecord) (bool, error)
	CreateBlueprintEntryValueGeneration(context.Context, etcd.EntryValueGeneration) error
	BindBlueprintEntryEnvironment(context.Context, string, string) error
}

type entryDesiredMutationService struct {
	volumeRoot string
	repository entryDesiredMutationRepository
	generator  entryCreationGenerator
	materials  environmentBlueprintMaterializationResolver
	creation   entryCreationIdempotency
	edit       entryEditIdempotency
	removal    entryDesiredRemovalIdempotency
	now        func() time.Time
}

func newEntryDesiredMutationService(
	volumeRoot string,
	repository entryDesiredMutationRepository,
	generator entryCreationGenerator,
	materials environmentBlueprintMaterializationResolver,
	creation entryCreationIdempotency,
	edit entryEditIdempotency,
	removal entryDesiredRemovalIdempotency,
) (*entryDesiredMutationService, error) {
	if environmentpath.ValidateRoot(volumeRoot) != nil || repository == nil || generator == nil || materials == nil ||
		creation == nil || edit == nil || removal == nil {
		return nil, errs.New(errs.KindInternal, "Entry desired mutation service is not configured")
	}
	return &entryDesiredMutationService{
		volumeRoot: volumeRoot, repository: repository, generator: generator, materials: materials,
		creation: creation, edit: edit, removal: removal, now: time.Now,
	}, nil
}

type entryDesiredMutationAction uint8

const (
	entryDesiredMutationCreate entryDesiredMutationAction = iota + 1
	entryDesiredMutationEdit
	entryDesiredMutationRemove
)

type entryDesiredEvidence struct {
	durable         etcd.ProtectedIntentRecord
	resolveExisting func(context.Context, etcd.IdempotencyLocator) (idempotentintent.Resolution, bool, error)
	matchesStaged   func(context.Context, etcd.ProtectedIntentRecord) (bool, error)
	resolveKnown    func(context.Context, etcd.IdempotencyTransactionResult) (idempotentintent.Resolution, error)
	resolveUnknown  func(context.Context, etcd.IdempotencyLocator, error) (idempotentintent.Resolution, error)
}

type entryDesiredRemovalEvidence struct {
	candidate idempotentintent.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type entryDesiredRemovalIdempotency interface {
	Prepare(context.Context, string, string) (entryDesiredRemovalEvidence, error)
	ResolveReplayLocator(
		context.Context,
		etcd.IdempotencyReplayTarget,
		string,
		string,
		string,
	) (etcd.IdempotencyLocator, bool, error)
	MatchesStaged(context.Context, entryDesiredRemovalEvidence, etcd.ProtectedIntentRecord) (bool, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		entryDesiredRemovalEvidence,
	) (idempotentintent.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		entryDesiredRemovalEvidence,
		etcd.IdempotencyTransactionResult,
	) (idempotentintent.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		entryDesiredRemovalEvidence,
		error,
	) (idempotentintent.Resolution, error)
}

type durableEntryDesiredRemovalIdempotency struct {
	coordinator *idempotentintent.Coordinator
	repository  *etcd.IdempotencyRepository
}

func newDurableEntryDesiredRemovalIdempotency(
	coordinator *idempotentintent.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableEntryDesiredRemovalIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Entry removal idempotency is not configured")
	}
	return &durableEntryDesiredRemovalIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableEntryDesiredRemovalIdempotency) Prepare(
	ctx context.Context,
	environmentID string,
	entryID string,
) (entryDesiredRemovalEvidence, error) {
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: http.MethodDelete, Route: entryEditRoute,
		Scope: idempotentintent.Scope{Kind: idempotentintent.ScopeEnvironment, ID: environmentID},
		Path:  []idempotentintent.PathBinding{{Name: "id", Value: entryID}},
		Query: idempotentintent.Object(), Body: idempotentintent.NoBody(),
	})
	if err != nil {
		return entryDesiredRemovalEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return entryDesiredRemovalEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return entryDesiredRemovalEvidence{}, err
	}
	return entryDesiredRemovalEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableEntryDesiredRemovalIdempotency) ResolveReplayLocator(
	ctx context.Context,
	target etcd.IdempotencyReplayTarget,
	method string,
	route string,
	key string,
) (etcd.IdempotencyLocator, bool, error) {
	return service.repository.ResolveReplayLocator(ctx, target, method, route, key)
}

func (service *durableEntryDesiredRemovalIdempotency) MatchesStaged(
	ctx context.Context,
	evidence entryDesiredRemovalEvidence,
	existing etcd.ProtectedIntentRecord,
) (bool, error) {
	return service.coordinator.MatchesDurable(ctx, evidence.candidate, existing)
}

func (service *durableEntryDesiredRemovalIdempotency) ResolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence entryDesiredRemovalEvidence,
) (idempotentintent.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableEntryDesiredRemovalIdempotency) ResolveKnown(
	ctx context.Context,
	evidence entryDesiredRemovalEvidence,
	result etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableEntryDesiredRemovalIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence entryDesiredRemovalEvidence,
	original error,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type entryDesiredMutationRequest struct {
	action         entryDesiredMutationAction
	environmentID  string
	entryID        string
	desired        core.EnvEntry
	idempotencyKey string
	locator        etcd.IdempotencyLocator
	evidence       entryDesiredEvidence
	status         int
}

func (service *entryDesiredMutationService) CreateEntry(
	ctx context.Context,
	input apiTypes.EntryCreateRequest,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Entry creation context is required")
	}
	desired, err := prepareEntryCreation(input)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	for attempt := 0; attempt < maximumEntryCreationAttempts; attempt++ {
		evidence, err := service.creation.Prepare(ctx, input.EnvironmentID, desired)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
		request := entryDesiredMutationRequest{
			action: entryDesiredMutationCreate, environmentID: input.EnvironmentID,
			desired: desired, idempotencyKey: idempotencyKey, status: http.StatusCreated,
			locator: etcd.IdempotencyLocator{
				ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: input.EnvironmentID,
				Method: http.MethodPost, Route: entryCreationRoute, Key: idempotencyKey,
			},
			evidence: entryDesiredEvidence{
				durable: evidence.durable,
				resolveExisting: func(ctx context.Context, locator etcd.IdempotencyLocator) (idempotentintent.Resolution, bool, error) {
					return service.creation.ResolveExisting(ctx, locator, evidence)
				},
				matchesStaged: func(ctx context.Context, existing etcd.ProtectedIntentRecord) (bool, error) {
					return service.creation.MatchesStaged(ctx, evidence, existing)
				},
				resolveKnown: func(ctx context.Context, result etcd.IdempotencyTransactionResult) (idempotentintent.Resolution, error) {
					return service.creation.ResolveKnown(ctx, evidence, result)
				},
				resolveUnknown: func(ctx context.Context, locator etcd.IdempotencyLocator, original error) (idempotentintent.Resolution, error) {
					return service.creation.ResolveUnknown(ctx, locator, evidence, original)
				},
			},
		}
		response, err := service.mutateEntryOnce(ctx, request)
		clear(evidence.durable.Ciphertext)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumEntryCreationAttempts-1 {
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Entry creation retry bound was not enforced")
}

func (service *entryDesiredMutationService) EditEntry(
	ctx context.Context,
	entryID string,
	input apiTypes.EntryEditRequest,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil || ids.Validate(ids.KindEnvEntry, entryID) != nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Entry edit requires a stable Entry id")
	}
	prepared, err := prepareEntryEditInput(input)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	target := etcd.IdempotencyReplayTarget{Kind: etcd.IdempotencyReplayTargetEntry, ID: entryID}
	locator, indexed, err := service.edit.ResolveReplayLocator(
		ctx,
		target,
		http.MethodPatch,
		entryEditRoute,
		idempotencyKey,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if indexed {
		return service.replayEntryDesiredEdit(ctx, locator, entryID, prepared)
	}
	environmentID, current, err := service.resolveCurrentEntry(ctx, entryID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	desired := current.Entry
	desired.Source = prepared.Source
	desired.Exposure = append([]string(nil), prepared.Exposure...)
	if err := desired.Validate(); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	for attempt := 0; attempt < maximumEntryEditAttempts; attempt++ {
		evidence, err := service.edit.Prepare(ctx, environmentID, entryID, prepared)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
		request := entryDesiredMutationRequest{
			action: entryDesiredMutationEdit, environmentID: environmentID, entryID: entryID,
			desired: desired, idempotencyKey: idempotencyKey, status: http.StatusOK,
			locator: etcd.IdempotencyLocator{
				ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: environmentID,
				Method: http.MethodPatch, Route: entryEditRoute, Key: idempotencyKey,
			},
			evidence: entryDesiredEvidence{
				durable: evidence.durable,
				resolveExisting: func(ctx context.Context, locator etcd.IdempotencyLocator) (idempotentintent.Resolution, bool, error) {
					return service.edit.ResolveExisting(ctx, locator, evidence)
				},
				matchesStaged: func(ctx context.Context, existing etcd.ProtectedIntentRecord) (bool, error) {
					return service.edit.MatchesStaged(ctx, evidence, existing)
				},
				resolveKnown: func(ctx context.Context, result etcd.IdempotencyTransactionResult) (idempotentintent.Resolution, error) {
					return service.edit.ResolveKnown(ctx, evidence, result)
				},
				resolveUnknown: func(ctx context.Context, locator etcd.IdempotencyLocator, original error) (idempotentintent.Resolution, error) {
					return service.edit.ResolveUnknown(ctx, locator, evidence, original)
				},
			},
		}
		response, err := service.mutateEntryOnce(ctx, request)
		clear(evidence.durable.Ciphertext)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumEntryEditAttempts-1 {
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Entry edit retry bound was not enforced")
}

func (service *entryDesiredMutationService) RemoveEntry(
	ctx context.Context,
	request entrycontroller.RemoveRequest,
) (entrycontroller.RemovalOutcome, error) {
	if ctx == nil || ids.Validate(ids.KindEnvEntry, request.EntryID) != nil {
		return entrycontroller.RemovalOutcome{}, errs.New(
			errs.KindValidationFailed, "Entry removal requires a stable Entry id",
		)
	}
	target := etcd.IdempotencyReplayTarget{Kind: etcd.IdempotencyReplayTargetEntry, ID: request.EntryID}
	locator, indexed, err := service.removal.ResolveReplayLocator(
		ctx, target, http.MethodDelete, entryEditRoute, request.IdempotencyKey,
	)
	if err != nil {
		return entrycontroller.RemovalOutcome{}, err
	}
	if indexed {
		evidence, err := service.removal.Prepare(ctx, locator.ScopeID, request.EntryID)
		if err != nil {
			return entrycontroller.RemovalOutcome{}, err
		}
		defer clear(evidence.durable.Ciphertext)
		resolution, existing, err := service.removal.ResolveExisting(ctx, locator, evidence)
		if err != nil {
			return entrycontroller.RemovalOutcome{}, err
		}
		if !existing || resolution.Kind != idempotentintent.ResolutionReplay {
			return entrycontroller.RemovalOutcome{}, errs.New(
				errs.KindInternal,
				"Entry removal replay resolution is invalid",
			)
		}
		return entryDesiredRemovalOutcome(resolution.Response)
	}
	environmentID, current, err := service.resolveCurrentEntry(ctx, request.EntryID)
	if err != nil {
		return entrycontroller.RemovalOutcome{}, err
	}
	for attempt := 0; attempt < maximumEntryEditAttempts; attempt++ {
		evidence, err := service.removal.Prepare(ctx, environmentID, request.EntryID)
		if err != nil {
			return entrycontroller.RemovalOutcome{}, err
		}
		mutation := entryDesiredMutationRequest{
			action: entryDesiredMutationRemove, environmentID: environmentID, entryID: request.EntryID,
			desired: current.Entry, idempotencyKey: request.IdempotencyKey, status: http.StatusAccepted,
			locator: etcd.IdempotencyLocator{
				ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: environmentID,
				Method: http.MethodDelete, Route: entryEditRoute, Key: request.IdempotencyKey,
			},
			evidence: entryDesiredEvidence{
				durable: evidence.durable,
				resolveExisting: func(ctx context.Context, locator etcd.IdempotencyLocator) (idempotentintent.Resolution, bool, error) {
					return service.removal.ResolveExisting(ctx, locator, evidence)
				},
				matchesStaged: func(ctx context.Context, existing etcd.ProtectedIntentRecord) (bool, error) {
					return service.removal.MatchesStaged(ctx, evidence, existing)
				},
				resolveKnown: func(ctx context.Context, result etcd.IdempotencyTransactionResult) (idempotentintent.Resolution, error) {
					return service.removal.ResolveKnown(ctx, evidence, result)
				},
				resolveUnknown: func(ctx context.Context, locator etcd.IdempotencyLocator, original error) (idempotentintent.Resolution, error) {
					return service.removal.ResolveUnknown(ctx, locator, evidence, original)
				},
			},
		}
		response, err := service.mutateEntryOnce(ctx, mutation)
		clear(evidence.durable.Ciphertext)
		if err == nil {
			return entryDesiredRemovalOutcome(response)
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumEntryEditAttempts-1 {
			return entrycontroller.RemovalOutcome{}, err
		}
	}
	return entrycontroller.RemovalOutcome{}, errs.New(errs.KindInternal, "Entry removal retry bound was not enforced")
}

func (service *entryDesiredMutationService) replayEntryDesiredEdit(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	entryID string,
	input entryEditInput,
) (etcd.IdempotencyResponse, error) {
	evidence, err := service.edit.Prepare(ctx, locator.ScopeID, entryID, input)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.edit.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if !existing || resolution.Kind != idempotentintent.ResolutionReplay {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Entry edit replay resolution is invalid")
	}
	return cloneIdempotencyResponse(resolution.Response), nil
}

func (service *entryDesiredMutationService) mutateEntryOnce(
	ctx context.Context,
	request entryDesiredMutationRequest,
) (etcd.IdempotencyResponse, error) {
	resolution, existing, err := request.evidence.resolveExisting(ctx, request.locator)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != idempotentintent.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Entry mutation replay resolution is invalid",
			)
		}
		return cloneIdempotencyResponse(resolution.Response), nil
	}
	environment, err := service.repository.GetEnvironment(ctx, request.environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if _, err := service.repository.GetTenant(ctx, project.Record.TenantID); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if project.Record.Kind != etcd.ProjectKindTenant ||
		environment.Record.ProvisioningState != etcd.EnvironmentProvisioningReady {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindResourceInUse,
			"Environment is not ready for Entry mutation",
		)
	}
	if request.action != entryDesiredMutationRemove {
		if err := service.validateExposure(ctx, request.environmentID, request.desired.Exposure); err != nil {
			return etcd.IdempotencyResponse{}, err
		}
	}
	head, hasHead, err := service.repository.GetEnvironmentBlueprintHead(ctx, request.environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	current, hasCurrent, err := service.repository.GetEnvironmentComposeProjection(ctx, request.environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	expectedHeadRevision, generation, err := serviceDesiredState(
		request.environmentID, head, hasHead, current, hasCurrent,
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
	now := service.now().UTC()
	candidateTaskID := ids.New(ids.KindTask)
	candidateRecord, previous, err := entryDesiredCandidateRecord(current.Record, request, candidateTaskID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	candidate, _, err := controller.ProjectEnvironmentEntryMutation(
		current.Record,
		controller.EnvironmentEntryArtifactMutation{
			RevisionID: candidateTaskID, ArtifactID: entryStableIDFromRevision(ids.KindConfig, candidateTaskID),
			PlanID: entryStableIDFromRevision(ids.KindPlan, candidateTaskID), RenderGeneration: generation,
			Entries: replaceProjectedEntry(current.Record.Entries, previous, candidateRecord),
		},
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	candidate = cloneEnvironmentDesiredProjection(candidate)
	claim, _, err := controllerrevision.PreflightAndClaim(
		ctx,
		service.repository,
		candidate,
		controllerrevision.ClaimInput{
			EnvironmentID: request.environmentID, CandidateTaskID: candidateTaskID,
			Locator: request.locator, Intent: request.evidence.durable,
			MatchExistingIntent:  request.evidence.matchesStaged,
			BaselineHeadRevision: expectedHeadRevision, SourceKind: etcd.EnvironmentBlueprintSourceMutation,
			RenderGeneration: generation, CreatedAt: now,
		},
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	candidateRecord, previous, err = entryDesiredCandidateRecord(current.Record, request, claim.TaskID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if request.action != entryDesiredMutationRemove {
		if err := service.prepareEntryGeneration(
			ctx, project.Record.ID, request.environmentID, request.desired, *candidateRecord, claim.CreatedAt,
		); err != nil {
			return etcd.IdempotencyResponse{}, err
		}
	}
	entries := replaceProjectedEntry(current.Record.Entries, previous, candidateRecord)
	candidate, materializations, err := controller.ProjectEnvironmentEntryMutation(
		current.Record,
		controller.EnvironmentEntryArtifactMutation{
			RevisionID: claim.RevisionID, ArtifactID: entryStableIDFromRevision(ids.KindConfig, claim.RevisionID),
			PlanID: entryStableIDFromRevision(ids.KindPlan, claim.RevisionID), RenderGeneration: generation,
			Entries: entries,
		},
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	candidate = cloneEnvironmentDesiredProjection(candidate)
	if previous != nil {
		serviceIdentities, snapshotErr := entryDesiredServiceIdentities(candidate)
		if snapshotErr != nil {
			return etcd.IdempotencyResponse{}, snapshotErr
		}
		removals, err := environmentBlueprintEntryRemovals(
			request.environmentID, []etcd.EntryRecord{*previous}, entries,
			serviceIdentities,
		)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
		materializations = append(materializations, removals...)
	}
	allocator, err := controllerrevision.NewBlueprintIdentityAllocator(claim)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	artifactID := entryStableIDFromRevision(ids.KindConfig, claim.RevisionID)
	materializationRecords, steps, err := service.entryMaterializations(
		ctx, request.environmentID, artifactID, allocator, materializations,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	applyStepID := allocator.Named(ids.KindStep, "entry-compose-apply")
	steps = append(steps, &agentpb.ExecutionStep{
		StepId: applyStepID, TimeoutSeconds: uint32(environmentBlueprintTimeoutSeconds),
		Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
			ArtifactId: artifactID, FullReconcile: true,
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
		VolumeRoot: service.volumeRoot, PlanID: planID, RenderGeneration: generation,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_RECONCILE, TargetID: request.environmentID,
		Artifacts: []*agentpb.ComposeArtifact{artifact}, Steps: steps,
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	owner, err := etcd.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	task := etcd.TaskRecord{
		ID: claim.TaskID, OperationID: allocator.Named(ids.KindOperation, "entry-operation"),
		IdempotencyKey: request.idempotencyKey, Owner: owner, Actor: etcd.TaskActorOperator,
		Executor: etcd.TaskExecutorAgent, PlanID: planID, PlanHash: hex.EncodeToString(plan.PlanHash),
		RenderGeneration: int32(generation), Type: etcd.TaskUpdate, Target: request.environmentID,
		Params: map[string]string{
			etcd.EnvironmentDesiredRevisionParam:         claim.RevisionID,
			etcd.TaskMaterializationEnvironmentParam:     request.environmentID,
			controller.EnvironmentBlueprintArtifactParam: artifactID,
			taskcontract.EnvironmentBlueprintProcedureParam: string(
				taskcontract.BlueprintComposeProcedureFullReconcile,
			),
		},
		Steps: stepRecords, Materializations: materializationRecords,
		TimeoutSeconds: environmentBlueprintTimeoutSeconds, Status: etcd.TaskStatusPending,
		NextEventSequence: 1, CreatedAt: claim.CreatedAt, UpdatedAt: claim.CreatedAt,
	}
	response, err := entryDesiredResponse(candidateRecord, claim.TaskID, request.status)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(response.Body)
	targetEntryID := request.entryID
	if candidateRecord != nil {
		targetEntryID = candidateRecord.Entry.ID
	}
	target := etcd.IdempotencyReplayTarget{Kind: etcd.IdempotencyReplayTargetEntry, ID: targetEntryID}
	marker := etcd.IdempotencyMarker{
		Kind: etcd.IdempotencyMarkerTask, State: etcd.IdempotencyMarkerPending,
		Locator: request.locator, ReplayTarget: &target, Intent: claim.Intent, Response: response,
		TaskID: task.ID, CreatedAt: claim.CreatedAt, UpdatedAt: claim.CreatedAt,
	}
	projectionEvidence, err := controllerrevision.PreflightProjection(candidate)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	var auditRecord *etcd.EntryRecord
	action := etcd.EnvironmentEntryMutationCreate
	if request.action == entryDesiredMutationEdit {
		action = etcd.EnvironmentEntryMutationEdit
	} else if request.action == entryDesiredMutationRemove {
		action = etcd.EnvironmentEntryMutationRemove
	}
	if candidateRecord != nil {
		value := *candidateRecord
		if value.Entry.Source.Kind == core.SourceLiteral {
			value.Entry.Source.Literal = ""
		}
		auditRecord = &value
	}
	if _, err := service.repository.StageEnvironmentBlueprintRevision(ctx, etcd.EnvironmentBlueprintStageRequest{
		Claim: claim,
		Mutation: &etcd.EnvironmentDesiredMutationAudit{Entry: &etcd.EnvironmentEntryMutationAudit{
			Action: action, BaseRevisionID: current.Record.RevisionID,
			EntryID: targetEntryID, Record: auditRecord,
		}},
		Projection: candidate, DependencyDigest: projectionEvidence.DependencyDigest,
	}); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	result, publicationErr := service.repository.PublishEnvironmentDesiredRevisionWithTask(
		ctx, project, environment, expectedHeadRevision, claim,
		etcd.EnvironmentDesiredRevisionIdentity{EnvironmentID: request.environmentID, RevisionID: claim.RevisionID},
		candidate, nil, nil, nil, etcd.ReleaseGroupBlueprintPreparedMutation{},
		etcd.ComponentTaskPreparation{}, etcd.BlueprintAttachTaskPreparation{}, task, marker,
	)
	if publicationErr != nil {
		if !isUnknownEntryCreationOutcome(publicationErr) {
			return etcd.IdempotencyResponse{}, publicationErr
		}
		resolution, err = request.evidence.resolveUnknown(ctx, request.locator, publicationErr)
	} else {
		resolution, err = request.evidence.resolveKnown(ctx, result)
	}
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if resolution.Kind == idempotentintent.ResolutionReplay {
		return cloneIdempotencyResponse(resolution.Response), nil
	}
	if resolution.Kind != idempotentintent.ResolutionApplied {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Entry desired mutation resolution is invalid")
	}
	return cloneIdempotencyResponse(response), nil
}

func entryDesiredServiceIdentities(
	projection etcd.EnvironmentComposeProjection,
) ([]controller.ComposeResourceIdentity, error) {
	result := make([]controller.ComposeResourceIdentity, len(projection.DesiredServices))
	seenIDs := make(map[string]string, len(projection.DesiredServices))
	seenNames := make(map[string]string, len(projection.DesiredServices))
	for index, service := range projection.DesiredServices {
		if service.EnvironmentID != projection.EnvironmentID ||
			ids.Validate(ids.KindService, service.Desired.ID) != nil ||
			service.Desired.Name == "" {
			return nil, errs.New(errs.KindInternal, "Environment desired projection has an invalid Service identity")
		}
		if name, duplicate := seenIDs[service.Desired.ID]; duplicate && name != service.Desired.Name {
			return nil, errs.New(errs.KindInternal, "Environment desired projection repeats a Service id")
		}
		if serviceID, duplicate := seenNames[service.Desired.Name]; duplicate && serviceID != service.Desired.ID {
			return nil, errs.New(errs.KindInternal, "Environment desired projection repeats a Service name")
		}
		seenIDs[service.Desired.ID] = service.Desired.Name
		seenNames[service.Desired.Name] = service.Desired.ID
		result[index] = controller.ComposeResourceIdentity{ID: service.Desired.ID, Name: service.Desired.Name}
	}
	return result, nil
}

func entryDesiredCandidateRecord(
	current etcd.EnvironmentComposeProjection,
	request entryDesiredMutationRequest,
	revisionID string,
) (*etcd.EntryRecord, *etcd.EntryRecord, error) {
	desired := request.desired
	var previous *etcd.EntryRecord
	if request.action == entryDesiredMutationCreate {
		desired.ID = entryStableIDFromRevision(ids.KindEnvEntry, revisionID)
	} else {
		for index := range current.Entries {
			if current.Entries[index].Entry.ID == request.entryID {
				value := current.Entries[index]
				previous = &value
				break
			}
		}
		if previous == nil {
			return nil, nil, errs.New(errs.KindEntryNotFound, "Entry was not found")
		}
		if request.action == entryDesiredMutationRemove {
			return nil, previous, nil
		}
		desired.ID = previous.Entry.ID
	}
	persisted := desired
	if persisted.Secret && persisted.Source.Kind == core.SourceLiteral {
		persisted.Source.Literal = ""
	}
	record, err := etcd.NewEntryRecord(
		current.EnvironmentID, persisted, entryStableIDFromRevision(ids.KindConfig, revisionID),
	)
	if err != nil {
		return nil, nil, err
	}
	if previous != nil {
		record.BlueprintKey = previous.BlueprintKey
	}
	return &record, previous, nil
}

func replaceProjectedEntry(
	current []etcd.EntryRecord,
	previous *etcd.EntryRecord,
	next *etcd.EntryRecord,
) []etcd.EntryRecord {
	result := make([]etcd.EntryRecord, 0, len(current)+1)
	for _, record := range current {
		if previous == nil || record.Entry.ID != previous.Entry.ID {
			result = append(result, record)
		}
	}
	if next != nil {
		result = append(result, *next)
	}
	sort.Slice(result, func(left, right int) bool { return result[left].Entry.ID < result[right].Entry.ID })
	return result
}

func (service *entryDesiredMutationService) prepareEntryGeneration(
	ctx context.Context,
	projectID string,
	environmentID string,
	desired core.EnvEntry,
	record etcd.EntryRecord,
	createdAt time.Time,
) error {
	found, err := service.repository.BlueprintEntryValueGenerationExists(ctx, record)
	if err != nil {
		return err
	}
	if !found {
		desired.ID = record.Entry.ID
		generation, err := service.generator.Generate(
			ctx, projectID, environmentID, desired, record.CurrentValueGenerationID, createdAt,
		)
		if err != nil {
			return err
		}
		createErr := service.repository.CreateBlueprintEntryValueGeneration(ctx, generation)
		ClearEntryValueGeneration(&generation)
		if createErr != nil {
			return createErr
		}
	}
	return service.repository.BindBlueprintEntryEnvironment(ctx, environmentID, record.Entry.ID)
}

func (service *entryDesiredMutationService) entryMaterializations(
	ctx context.Context,
	environmentID string,
	artifactID string,
	allocator *controllerrevision.BlueprintIdentityAllocator,
	inputs []controller.EnvironmentEntryMaterialization,
) ([]etcd.TaskMaterializationRecord, []*agentpb.ExecutionStep, error) {
	sort.Slice(inputs, func(left, right int) bool { return inputs[left].Destination < inputs[right].Destination })
	records := make([]etcd.TaskMaterializationRecord, 0, len(inputs))
	steps := make([]*agentpb.ExecutionStep, 0, len(inputs))
	previous := ""
	for _, input := range inputs {
		if input.Destination == previous {
			return nil, nil, errs.New(errs.KindNameConflict, "Environment materialization destination is duplicated")
		}
		content, err := service.materials.ResolveTaskMaterializationSource(ctx, environmentID, input.Source)
		if err != nil {
			clear(content)
			return nil, nil, err
		}
		digest := sha256.Sum256(content)
		record := etcd.TaskMaterializationRecord{
			StepID:            allocator.Named(ids.KindStep, "entry-materialization-step/"+input.Destination),
			MaterializationID: allocator.Named(ids.KindConfig, "entry-materialization/"+input.Destination),
			EnvironmentID:     environmentID, Destination: input.Destination,
			ServiceID: input.ServiceID, ServiceName: input.ServiceName,
			OutputKind: input.OutputKind, UID: input.UID, GID: input.GID, Mode: uint32(input.Mode),
			Length: uint64(len(content)), SHA256: hex.EncodeToString(digest[:]), Source: input.Source,
		}
		clear(content)
		step, err := controller.BuildTaskMaterializationStep(
			record,
			artifactID,
			uint32(environmentBlueprintTimeoutSeconds),
		)
		if err != nil {
			return nil, nil, err
		}
		records = append(records, record)
		steps = append(steps, step)
		previous = input.Destination
	}
	sort.Slice(records, func(left, right int) bool { return records[left].StepID < records[right].StepID })
	return records, steps, nil
}

func (service *entryDesiredMutationService) resolveCurrentEntry(
	ctx context.Context,
	entryID string,
) (string, etcd.EntryRecord, error) {
	environmentID, found, err := service.repository.ResolveBlueprintEntryEnvironment(ctx, entryID)
	if err != nil {
		return "", etcd.EntryRecord{}, err
	}
	if !found {
		return "", etcd.EntryRecord{}, errs.New(errs.KindEntryNotFound, "Entry was not found")
	}
	projection, found, err := service.repository.GetEnvironmentComposeProjection(ctx, environmentID)
	if err != nil {
		return "", etcd.EntryRecord{}, err
	}
	if found {
		for _, record := range projection.Record.Entries {
			if record.Entry.ID == entryID {
				return environmentID, record, nil
			}
		}
	}
	return "", etcd.EntryRecord{}, errs.New(errs.KindEntryNotFound, "Entry was not found")
}

func (service *entryDesiredMutationService) validateExposure(
	ctx context.Context,
	environmentID string,
	exposure []string,
) error {
	if len(exposure) == 1 && exposure[0] == "all" {
		return nil
	}
	missing := make(map[string]struct{}, len(exposure))
	for _, name := range exposure {
		missing[name] = struct{}{}
	}
	cursor := ""
	for {
		page, err := service.repository.ListServices(ctx, environmentID, etcd.PageRequest{Limit: 200, Cursor: cursor})
		if err != nil {
			return err
		}
		for _, item := range page.Items {
			delete(missing, item.Record.Desired.Name)
		}
		if len(missing) == 0 {
			return nil
		}
		if page.NextCursor == "" {
			return errs.New(errs.KindServiceNotFound, "Entry exposure Service was not found")
		}
		cursor = page.NextCursor
	}
}

func entryDesiredResponse(
	record *etcd.EntryRecord,
	taskID string,
	status int,
) (etcd.IdempotencyResponse, error) {
	var value any = apiTypes.TaskAccepted{TaskID: taskID}
	if record != nil {
		value = entryCreationResponse(record.Entry)
	}
	body, err := json.Marshal(value)
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	return etcd.IdempotencyResponse{Status: status, ContentKind: "application/json", Body: body}, nil
}

func entryDesiredRemovalOutcome(response etcd.IdempotencyResponse) (entrycontroller.RemovalOutcome, error) {
	if response.Status != http.StatusAccepted {
		return entrycontroller.RemovalOutcome{}, errs.New(errs.KindInternal, "Entry removal response status is invalid")
	}
	accepted := apiTypes.TaskAccepted{}
	if json.Unmarshal(response.Body, &accepted) != nil || ids.Validate(ids.KindTask, accepted.TaskID) != nil {
		return entrycontroller.RemovalOutcome{}, errs.New(errs.KindInternal, "Entry removal response is invalid")
	}
	return entrycontroller.RemovalOutcome{TaskID: accepted.TaskID}, nil
}

func entryStableIDFromRevision(kind ids.Kind, revisionID string) string {
	if ids.Validate(ids.KindTask, revisionID) != nil {
		return ""
	}
	return string(kind) + "_" + revisionID[len("task_"):]
}

func protoUnmarshalEntryArtifact(value []byte, artifact *agentpb.ComposeArtifact) error {
	if err := proto.Unmarshal(value, artifact); err != nil {
		return errs.New(errs.KindInternal, "Environment Entry candidate artifact is corrupt")
	}
	return nil
}
