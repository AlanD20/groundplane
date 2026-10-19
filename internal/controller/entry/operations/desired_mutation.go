package operations

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	materializationrecord "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	controllerrevision "github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	entrycontroller "github.com/AlanD20/groundplane/internal/controller/entry"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entrywork"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
	"math"
	"time"
)

type entryDesiredMutationRepository interface {
	InitializeEnvironmentDesiredState(
		context.Context,
		string,
		idempotencyrecord.IdempotencyLocator,
		idempotencyrecord.ProtectedIntentRecord,
		time.Time,
	) error
	controllerrevision.ClaimRepository
	StageEnvironmentBlueprintRevision(
		context.Context,
		blueprints.EnvironmentBlueprintStageRequest,
	) (blueprints.EnvironmentBlueprintSeal, error)
	GetEnvironmentDesiredInput(
		context.Context,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput], bool, error)
	GetTenant(context.Context, string) (etcdstore.Versioned[hierarchyrecord.TenantRecord], error)
	GetProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetEnvironmentBlueprintHead(
		context.Context,
		string,
	) (etcdstore.Versioned[blueprints.EnvironmentBlueprintHead], bool, error)
	GetEnvironmentComposeProjection(
		context.Context,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error)
	ListServices(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[servicerecord.ServiceRecord], error)
	ListEnvironmentComponents(
		context.Context,
		string,
		etcdstore.PageRequest,
	) (etcdstore.Page[componentrecord.Record], error)
	ResolveBlueprintEntryEnvironment(context.Context, string) (string, bool, error)
	BlueprintEntryValueGenerationExists(context.Context, entryrecord.Record) (bool, error)
	CreateBlueprintEntryValueGeneration(context.Context, entryrecord.EntryValueGeneration) error
	BindBlueprintEntryEnvironment(context.Context, string, string) error
}

type entryRuntimeCapture interface {
	CaptureEntryMutationRuntime(
		context.Context,
		etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	) (taskplanning.EntryMutationRuntime, error)
}

type entryDesiredMutationService struct {
	repository entryDesiredMutationRepository
	generator  entryCreationGenerator
	materials  materializationResolver
	creation   entryCreationIdempotency
	edit       entryEditIdempotency
	removal    entryDesiredRemovalIdempotency
	work       *etcd.HierarchyRepository
	runtime    entryRuntimeCapture
	now        func() time.Time
}

func NewDesiredMutationService(
	repository entryDesiredMutationRepository,
	generator entryCreationGenerator,
	materials materializationResolver,
	creation entryCreationIdempotency,
	edit entryEditIdempotency,
	removal entryDesiredRemovalIdempotency,
	runtime entryRuntimeCapture,
	hierarchy *etcd.HierarchyRepository,
) (*entryDesiredMutationService, error) {
	if repository == nil || generator == nil || materials == nil ||
		creation == nil || edit == nil || removal == nil || hierarchy == nil || runtime == nil {
		return nil, errs.New(errs.KindInternal, "Entry desired mutation service is not configured")
	}
	return &entryDesiredMutationService{
		repository: repository, generator: generator, materials: materials,
		creation: creation, edit: edit, removal: removal, runtime: runtime, work: hierarchy, now: time.Now,
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
	if err := service.repository.InitializeEnvironmentDesiredState(ctx, request.environmentID, request.locator, request.evidence.durable, service.now().UTC()); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
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
	var removalConfiguration etcd.EntryRemovalConfiguration
	if request.action != entryDesiredMutationRemove {
		runtime, err = service.runtime.CaptureEntryMutationRuntime(ctx, current)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	} else {
		removalConfiguration, err = service.work.ReadEntryRemovalConfiguration(ctx, request.environmentID, current.ReadRevision)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		runtime.EpochRevision = removalConfiguration.EpochRevision
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
	if request.action == entryDesiredMutationRemove {
		references, err = service.removalSources(ctx, request.entryID, removalConfiguration.Files, allocator)
	} else {
		references, err = service.entryMaterializations(ctx, request.environmentID, allocator, materializations)
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
	target := idempotencyrecord.IdempotencyReplayTarget{
		Kind: idempotencyrecord.IdempotencyReplayTargetEntry,
		ID:   targetEntryID,
	}
	marker := idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
		Locator: request.locator, ReplayTarget: &target, Intent: claim.Intent, Response: response,
		TaskID: claim.TaskID, CreatedAt: claim.CreatedAt, UpdatedAt: claim.CreatedAt,
	}
	projectionEvidence, err := controllerrevision.PreflightProjection(candidate)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	desiredInput, err := controllerrevision.DeriveCurrentMutationDesiredInput(
		ctx, service.repository, request.environmentID, current.Record.RevisionID,
		candidate, environment.Record.NetworkPool,
		func(input *core.BlueprintDesiredInput) error {
			entries, err := entrycontroller.BlueprintAuthoring(candidate.Entries)
			if err != nil {
				return err
			}
			input.Entries = entries
			return nil
		},
	)
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
		DesiredInput: desiredInput, DependencyDigest: projectionEvidence.DependencyDigest,
	}); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	workAction := workexecution.ActionUpdate
	targetName := ""
	var removal *entrywork.Removal
	switch request.action {
	case entryDesiredMutationCreate:
		workAction = workexecution.ActionCreate
	case entryDesiredMutationRemove:
		workAction = workexecution.ActionRemove
		removal = &entrywork.Removal{EntryID: request.entryID, BaseRevisionID: current.Record.RevisionID,
			HeadRevision: expectedHeadRevision, DescriptorID: claim.DescriptorID, RevisionID: claim.RevisionID}
	}
	if candidateRecord != nil {
		targetName = candidateRecord.Entry.Key
		if targetName == "" {
			targetName = candidateRecord.Entry.Path
		}
	} else if previous != nil {
		targetName = previous.Entry.Key
		if targetName == "" {
			targetName = previous.Entry.Path
		}
	}
	result, publicationErr := service.publishConfiguration(ctx, entryConfigurationPublication{
		Project: project, Environment: environment, Claim: claim, Runtime: runtime,
		Candidate: candidate, ReadRevision: current.ReadRevision, Files: references,
		Marker: marker, Action: workAction, TargetID: targetEntryID, TargetName: targetName,
		Removal: removal, ConfigurationRevision: removalConfiguration.HeadRevision,
	})
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
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Entry desired mutation resolution is invalid",
		)
	}
	return requestidempotency.CloneResponse(response), nil
}
