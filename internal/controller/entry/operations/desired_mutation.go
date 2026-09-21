package operations

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"
	materializationrecord "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	controllerrevision "github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"math"
	"time"
)

type entryDesiredMutationRepository interface {
	controllerrevision.Repository
	GetTenant(context.Context, string) (etcdstore.Versioned[hierarchyrecord.TenantRecord], error)
	GetProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetEnvironmentBlueprintHead(context.Context, string) (etcdstore.Versioned[blueprints.EnvironmentBlueprintHead], bool, error)
	GetEnvironmentComposeProjection(
		context.Context,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error)
	ListServices(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[servicerecord.ServiceRecord], error)
	ListEnvironmentComponents(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[componentrecord.Record], error)
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
	removePlans *taskplanning.EntryRemovalPlanner
	plans       *taskplanning.TaskPlanResolver
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
	plans *taskplanning.TaskPlanResolver,
	hierarchy *etcd.HierarchyRepository,
) (*entryDesiredMutationService, error) {
	if environmentpath.ValidateRoot(volumeRoot) != nil || repository == nil || generator == nil || materials == nil ||
		creation == nil || edit == nil || removal == nil {
		return nil, errs.New(errs.KindInternal, "Entry desired mutation service is not configured")
	}
	removePlans, err := taskplanning.NewEntryRemovalPlanner(plans, materials, hierarchy)
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
	durable         idempotencyrecord.ProtectedIntentRecord
	resolveExisting func(context.Context, idempotencyrecord.IdempotencyLocator) (requestidempotency.Resolution, bool, error)
	matchesStaged   func(context.Context, idempotencyrecord.ProtectedIntentRecord) (bool, error)
	resolveKnown    func(context.Context, etcd.IdempotencyTransactionResult) (requestidempotency.Resolution, error)
	resolveUnknown  func(context.Context, idempotencyrecord.IdempotencyLocator, error) (requestidempotency.Resolution, error)
}

type entryDesiredMutationRequest struct {
	action         entryDesiredMutationAction
	environmentID  string
	entryID        string
	desired        core.EnvEntry
	idempotencyKey string
	locator        idempotencyrecord.IdempotencyLocator
	evidence       entryDesiredEvidence
	status         int
}

func (service *entryDesiredMutationService) mutateEntryOnce(
	ctx context.Context,
	request entryDesiredMutationRequest,
) (idempotencyrecord.IdempotencyResponse, error) {
	resolution, existing, err := request.evidence.resolveExisting(ctx, request.locator)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != requestidempotency.ResolutionReplay {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Entry mutation replay resolution is invalid",
			)
		}
		return requestidempotency.CloneResponse(resolution.Response), nil
	}
	environment, err := service.repository.GetEnvironment(ctx, request.environmentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if _, err := service.repository.GetTenant(ctx, project.Record.TenantID); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if project.Record.Kind != hierarchyrecord.ProjectKindTenant ||
		environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindResourceInUse,
			"Environment is not ready for Entry mutation",
		)
	}
	if request.action != entryDesiredMutationRemove {
		if err := service.validateExposure(ctx, request.environmentID, request.desired.Exposure); err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	head, hasHead, err := service.repository.GetEnvironmentBlueprintHead(ctx, request.environmentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	current, hasCurrent, err := service.repository.GetEnvironmentComposeProjection(ctx, request.environmentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	expectedHeadRevision, generation, err := controllerrevision.NextGeneration(
		request.environmentID, head, hasHead, current, hasCurrent,
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
	runtime := taskplanning.EntryMutationRuntime{Projection: current.Record}
	if request.action != entryDesiredMutationRemove {
		runtime, err = service.plans.CaptureEntryMutationRuntime(ctx, current)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	now := service.now().UTC()
	candidateTaskID := ids.New(ids.KindTask)
	candidateRecord, previous, err := entryDesiredCandidateRecord(current.Record, request, candidateTaskID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	candidate, _, err := composerender.ProjectEnvironmentEntryMutation(
		runtime.Projection,
		composerender.EnvironmentEntryArtifactMutation{
			RevisionID: candidateTaskID, ArtifactID: entryStableIDFromRevision(ids.KindConfig, candidateTaskID),
			PlanID: entryStableIDFromRevision(ids.KindPlan, candidateTaskID), RenderGeneration: generation,
			Entries: replaceProjectedEntry(current.Record.Entries, previous, candidateRecord),
		},
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	candidate = controllerrevision.CloneProjection(candidate)
	claim, _, err := controllerrevision.PreflightAndClaim(ctx, service.repository, candidate,
		controllerrevision.ClaimInput{
			EnvironmentID: request.environmentID, CandidateTaskID: candidateTaskID,
			Locator: request.locator, Intent: request.evidence.durable,
			MatchExistingIntent:  request.evidence.matchesStaged,
			BaselineHeadRevision: expectedHeadRevision, SourceKind: blueprints.EnvironmentBlueprintSourceMutation,
			RenderGeneration: generation, CreatedAt: now,
		},
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	candidateRecord, previous, err = entryDesiredCandidateRecord(current.Record, request, claim.TaskID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if request.action != entryDesiredMutationRemove {
		if err := service.prepareEntryGeneration(
			ctx, project.Record.ID, request.environmentID, request.desired, *candidateRecord, claim.CreatedAt,
		); err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	entries := replaceProjectedEntry(current.Record.Entries, previous, candidateRecord)
	candidate, materializations, err := composerender.ProjectEnvironmentEntryMutation(
		runtime.Projection,
		composerender.EnvironmentEntryArtifactMutation{
			RevisionID: claim.RevisionID, ArtifactID: entryStableIDFromRevision(ids.KindConfig, claim.RevisionID),
			PlanID: entryStableIDFromRevision(ids.KindPlan, claim.RevisionID), RenderGeneration: generation,
			Entries: entries,
		},
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	candidate = controllerrevision.CloneProjection(candidate)
	if previous != nil {
		serviceIdentities, snapshotErr := entryDesiredServiceIdentities(candidate)
		if snapshotErr != nil {
			return idempotencyrecord.IdempotencyResponse{}, snapshotErr
		}
		removals, err := PlanEntryRemovals(
			request.environmentID, []entryrecord.Record{*previous}, entries,
			serviceIdentities,
		)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		materializations = append(materializations, removals...)
	}
	allocator, err := controllerrevision.NewBlueprintIdentityAllocator(claim)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	var references []materializationrecord.Record
	if request.action != entryDesiredMutationRemove {
		references, err = service.entryMaterializations(ctx, request.environmentID, allocator, materializations)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	planID := entryStableIDFromRevision(ids.KindPlan, claim.RevisionID)
	owner, err := taskjournal.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	task := etcd.TaskRecord{
		ID: claim.TaskID, OperationID: allocator.Named(ids.KindOperation, "entry-operation"),
		IdempotencyKey: request.idempotencyKey, Owner: owner, Actor: taskjournal.TaskActorOperator,
		Executor: taskjournal.TaskExecutorAgent, PlanID: planID,
		RenderGeneration: int32(generation), Type: taskjournal.TaskUpdate, Target: request.environmentID,
		Materializations: references,
		TimeoutSeconds:   controllerrevision.TaskTimeoutSeconds, Status: taskjournal.TaskStatusPending,
		NextEventSequence: 1, CreatedAt: claim.CreatedAt, UpdatedAt: claim.CreatedAt,
	}
	if request.action == entryDesiredMutationRemove {
		task.Type, task.Target = taskjournal.TaskRemove, request.entryID
		task, err = service.removePlans.PrepareDesiredEntryRemoval(ctx, task, claim)
	} else {
		task, err = runtime.PrepareTask(service.volumeRoot, task, candidate,
			allocator.Named(ids.KindStep, "entry-compose-apply"))
	}
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	response, err := entryDesiredResponse(candidateRecord, claim.TaskID, request.status)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(response.Body)
	targetEntryID := request.entryID
	if candidateRecord != nil {
		targetEntryID = candidateRecord.Entry.ID
	}
	target := idempotencyrecord.IdempotencyReplayTarget{Kind: idempotencyrecord.IdempotencyReplayTargetEntry, ID: targetEntryID}
	marker := idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
		Locator: request.locator, ReplayTarget: &target, Intent: claim.Intent, Response: response,
		TaskID: task.ID, CreatedAt: claim.CreatedAt, UpdatedAt: claim.CreatedAt,
	}
	projectionEvidence, err := controllerrevision.PreflightProjection(candidate)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	var auditRecord *entryrecord.Record
	action := blueprints.EnvironmentEntryMutationCreate
	if request.action == entryDesiredMutationEdit {
		action = blueprints.EnvironmentEntryMutationEdit
	} else if request.action == entryDesiredMutationRemove {
		action = blueprints.EnvironmentEntryMutationRemove
	}
	if candidateRecord != nil {
		value := *candidateRecord
		if value.Entry.Source.Kind == core.SourceLiteral {
			value.Entry.Source.Literal = ""
		}
		auditRecord = &value
	}
	if _, err := service.repository.StageEnvironmentBlueprintRevision(ctx, blueprints.EnvironmentBlueprintStageRequest{
		Claim: claim,
		Mutation: &blueprints.EnvironmentDesiredMutationAudit{Entry: &blueprints.EnvironmentEntryMutationAudit{
			Action: action, BaseRevisionID: current.Record.RevisionID,
			EntryID: targetEntryID, Record: auditRecord,
		}},
		Projection: candidate, DependencyDigest: projectionEvidence.DependencyDigest,
	}); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	result, publicationErr := service.repository.PublishEnvironmentDesiredRevisionWithTask(
		ctx, project, environment, expectedHeadRevision, claim,
		blueprints.EnvironmentDesiredRevisionIdentity{EnvironmentID: request.environmentID, RevisionID: claim.RevisionID},
		candidate, nil, nil, nil, etcd.ReleaseGroupBlueprintPreparedMutation{},
		etcd.ComponentTaskPreparation{}, etcd.BlueprintAttachTaskPreparation{}, task, marker,
	)
	if publicationErr != nil {
		if !isUnknownEntryCreationOutcome(publicationErr) {
			return idempotencyrecord.IdempotencyResponse{}, publicationErr
		}
		resolution, err = request.evidence.resolveUnknown(ctx, request.locator, publicationErr)
	} else {
		resolution, err = request.evidence.resolveKnown(ctx, result)
	}
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if resolution.Kind == requestidempotency.ResolutionReplay {
		return requestidempotency.CloneResponse(resolution.Response), nil
	}
	if resolution.Kind != requestidempotency.ResolutionApplied {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Entry desired mutation resolution is invalid")
	}
	return requestidempotency.CloneResponse(response), nil
}
