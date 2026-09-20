package operations

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	controllerrevision "github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/pkg/errs"
	"math"
	"time"
)

type entryDesiredMutationRepository interface {
	controllerrevision.Repository
	GetTenant(context.Context, string) (etcd.Versioned[hierarchyrecord.TenantRecord], error)
	GetProject(context.Context, string) (etcd.Versioned[hierarchyrecord.ProjectRecord], error)
	GetEnvironment(context.Context, string) (etcd.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetEnvironmentBlueprintHead(context.Context, string) (etcd.Versioned[etcd.EnvironmentBlueprintHead], bool, error)
	GetEnvironmentComposeProjection(
		context.Context,
		string,
	) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error)
	ListServices(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.ServiceRecord], error)
	ListEnvironmentComponents(context.Context, string, etcd.PageRequest) (etcd.Page[componentrecord.Record], error)
	ResolveBlueprintEntryEnvironment(context.Context, string) (string, bool, error)
	BlueprintEntryValueGenerationExists(context.Context, entryrecord.Record) (bool, error)
	CreateBlueprintEntryValueGeneration(context.Context, etcd.EntryValueGeneration) error
	BindBlueprintEntryEnvironment(context.Context, string, string) error
}

type entryDesiredMutationService struct {
	volumeRoot  string
	repository  entryDesiredMutationRepository
	generator   entryCreationGenerator
	materials   materializationResolver
	creation    entryCreationIdempotency
	edit        entryEditIdempotency
	removal     entryDesiredRemovalIdempotency
	removePlans *controller.EntryRemovalPlanner
	plans       *controller.TaskPlanResolver
	now         func() time.Time
}

func NewDesiredMutationService(
	volumeRoot string,
	repository entryDesiredMutationRepository,
	generator entryCreationGenerator,
	materials materializationResolver,
	creation entryCreationIdempotency,
	edit entryEditIdempotency,
	removal entryDesiredRemovalIdempotency,
	plans *controller.TaskPlanResolver,
	hierarchy *etcd.HierarchyRepository,
) (*entryDesiredMutationService, error) {
	if environmentpath.ValidateRoot(volumeRoot) != nil || repository == nil || generator == nil || materials == nil ||
		creation == nil || edit == nil || removal == nil {
		return nil, errs.New(errs.KindInternal, "Entry desired mutation service is not configured")
	}
	removePlans, err := controller.NewEntryRemovalPlanner(plans, materials, hierarchy)
	if err != nil {
		return nil, err
	}
	return &entryDesiredMutationService{
		volumeRoot: volumeRoot, repository: repository, generator: generator, materials: materials,
		creation: creation, edit: edit, removal: removal, removePlans: removePlans, plans: plans, now: time.Now,
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
	resolveExisting func(context.Context, etcd.IdempotencyLocator) (requestidempotency.Resolution, bool, error)
	matchesStaged   func(context.Context, etcd.ProtectedIntentRecord) (bool, error)
	resolveKnown    func(context.Context, etcd.IdempotencyTransactionResult) (requestidempotency.Resolution, error)
	resolveUnknown  func(context.Context, etcd.IdempotencyLocator, error) (requestidempotency.Resolution, error)
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

func (service *entryDesiredMutationService) mutateEntryOnce(
	ctx context.Context,
	request entryDesiredMutationRequest,
) (etcd.IdempotencyResponse, error) {
	resolution, existing, err := request.evidence.resolveExisting(ctx, request.locator)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != requestidempotency.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Entry mutation replay resolution is invalid",
			)
		}
		return requestidempotency.CloneResponse(resolution.Response), nil
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
	if project.Record.Kind != hierarchyrecord.ProjectKindTenant ||
		environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady {
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
	expectedHeadRevision, generation, err := controllerrevision.NextGeneration(
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
	runtime := controller.EntryMutationRuntime{Projection: current.Record}
	if request.action != entryDesiredMutationRemove {
		runtime, err = service.plans.CaptureEntryMutationRuntime(ctx, current)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
	}
	now := service.now().UTC()
	candidateTaskID := ids.New(ids.KindTask)
	candidateRecord, previous, err := entryDesiredCandidateRecord(current.Record, request, candidateTaskID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	candidate, _, err := controller.ProjectEnvironmentEntryMutation(
		runtime.Projection,
		controller.EnvironmentEntryArtifactMutation{
			RevisionID: candidateTaskID, ArtifactID: entryStableIDFromRevision(ids.KindConfig, candidateTaskID),
			PlanID: entryStableIDFromRevision(ids.KindPlan, candidateTaskID), RenderGeneration: generation,
			Entries: replaceProjectedEntry(current.Record.Entries, previous, candidateRecord),
		},
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	candidate = controllerrevision.CloneProjection(candidate)
	claim, _, err := controllerrevision.PreflightAndClaim(ctx, service.repository, candidate,
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
		runtime.Projection,
		controller.EnvironmentEntryArtifactMutation{
			RevisionID: claim.RevisionID, ArtifactID: entryStableIDFromRevision(ids.KindConfig, claim.RevisionID),
			PlanID: entryStableIDFromRevision(ids.KindPlan, claim.RevisionID), RenderGeneration: generation,
			Entries: entries,
		},
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	candidate = controllerrevision.CloneProjection(candidate)
	if previous != nil {
		serviceIdentities, snapshotErr := entryDesiredServiceIdentities(candidate)
		if snapshotErr != nil {
			return etcd.IdempotencyResponse{}, snapshotErr
		}
		removals, err := PlanEntryRemovals(
			request.environmentID, []entryrecord.Record{*previous}, entries,
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
	var references []etcd.TaskMaterializationRecord
	if request.action != entryDesiredMutationRemove {
		references, err = service.entryMaterializations(ctx, request.environmentID, allocator, materializations)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
	}
	planID := entryStableIDFromRevision(ids.KindPlan, claim.RevisionID)
	owner, err := etcd.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	task := etcd.TaskRecord{
		ID: claim.TaskID, OperationID: allocator.Named(ids.KindOperation, "entry-operation"),
		IdempotencyKey: request.idempotencyKey, Owner: owner, Actor: etcd.TaskActorOperator,
		Executor: etcd.TaskExecutorAgent, PlanID: planID,
		RenderGeneration: int32(generation), Type: etcd.TaskUpdate, Target: request.environmentID,
		Materializations: references,
		TimeoutSeconds:   controllerrevision.TaskTimeoutSeconds, Status: etcd.TaskStatusPending,
		NextEventSequence: 1, CreatedAt: claim.CreatedAt, UpdatedAt: claim.CreatedAt,
	}
	if request.action == entryDesiredMutationRemove {
		task.Type, task.Target = etcd.TaskRemove, request.entryID
		task, err = service.removePlans.PrepareDesiredEntryRemoval(ctx, task, claim)
	} else {
		task, err = runtime.PrepareTask(service.volumeRoot, task, candidate,
			allocator.Named(ids.KindStep, "entry-compose-apply"))
	}
	if err != nil {
		return etcd.IdempotencyResponse{}, err
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
	var auditRecord *entryrecord.Record
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
	if resolution.Kind == requestidempotency.ResolutionReplay {
		return requestidempotency.CloneResponse(resolution.Response), nil
	}
	if resolution.Kind != requestidempotency.ResolutionApplied {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Entry desired mutation resolution is invalid")
	}
	return requestidempotency.CloneResponse(response), nil
}
