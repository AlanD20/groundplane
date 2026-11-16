package operations

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	materializationrecord "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	controllerrevision "github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	entrycontroller "github.com/AlanD20/groundplane/internal/controller/entry"
	"github.com/AlanD20/groundplane/internal/controller/entryfiles"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entrywork"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"math"
	"time"
)

type entryDesiredMutationRepository interface {
	ReadEntryMutationSource(context.Context, string) (etcd.EntryMutationSource, error)
	PrepareUnrenderedEntryWork(
		context.Context,
		etcd.EntryMetadataPublication,
	) (etcd.EntryWorkCapture, error)
	InitializeEnvironmentDesiredState(
		context.Context,
		string,
		idempotencyrecord.IdempotencyLocator,
		idempotencyrecord.ProtectedIntentRecord,
		time.Time,
	) error
	controllerrevision.ClaimRepository
	controllerrevision.AbandonmentRepository
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
	accepted   *acceptedaction.Submitter
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
	accepted *acceptedaction.Submitter,
) (*entryDesiredMutationService, error) {
	if repository == nil || generator == nil || materials == nil ||
		creation == nil || edit == nil || removal == nil || hierarchy == nil || runtime == nil || accepted == nil {
		return nil, errs.New(errs.KindInternal, "Entry desired mutation service is not configured")
	}
	return &entryDesiredMutationService{
		repository: repository, generator: generator, materials: materials,
		creation: creation, edit: edit, removal: removal, runtime: runtime, work: hierarchy, accepted: accepted, now: time.Now,
	}, nil
}

type entryDesiredMutationAction uint8

const (
	entryDesiredMutationCreate entryDesiredMutationAction = iota + 1
	entryDesiredMutationEdit
	entryDesiredMutationRemove
)

type entryDesiredEvidence struct {
	durable       idempotencyrecord.ProtectedIntentRecord
	matchesStaged func(context.Context, idempotencyrecord.ProtectedIntentRecord) (bool, error)
}

type entryDesiredMutationRequest struct {
	accepted      workqueue.TaskSnapshot
	at            time.Time
	action        entryDesiredMutationAction
	environmentID string
	entryID       string
	desired       core.EnvEntry
	locator       idempotencyrecord.IdempotencyLocator
	evidence      entryDesiredEvidence
}

func (service *entryDesiredMutationService) prepareEntryMutation(
	ctx context.Context,
	request entryDesiredMutationRequest,
) (taskdependencies.Prepared, error) {
	environment, err := service.repository.GetEnvironment(ctx, request.environmentID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if _, err := service.repository.GetTenant(ctx, project.Record.TenantID); err != nil {
		return taskdependencies.Prepared{}, err
	}
	if project.Record.Kind != hierarchyrecord.ProjectKindTenant ||
		environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindResourceInUse,
			"Environment is not ready for Entry mutation",
		)
	}
	if request.action != entryDesiredMutationRemove {
		if err := service.validateExposure(ctx, request.environmentID, request.desired.Exposure); err != nil {
			return taskdependencies.Prepared{}, err
		}
	}
	if err := service.repository.InitializeEnvironmentDesiredState(ctx, request.environmentID, request.locator, request.evidence.durable, request.at); err != nil {
		return taskdependencies.Prepared{}, err
	}
	head, hasHead, err := service.repository.GetEnvironmentBlueprintHead(ctx, request.environmentID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	source, err := service.repository.ReadEntryMutationSource(ctx, request.environmentID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if source.Desired.Revision != head.Revision || source.Desired.Record.RevisionID != head.Record.RevisionID {
		return taskdependencies.Prepared{}, errs.New(errs.KindStateConflict, "Entry desired source changed")
	}
	if source.Runtime == nil {
		return service.prepareUnrenderedEntry(ctx, request, source, project, environment)
	}
	current, hasCurrent := *source.Runtime, true
	expectedHeadRevision, generation, err := controllerrevision.NextGeneration(
		request.environmentID, head, hasHead, current, hasCurrent,
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
	runtime := taskplanning.EntryMutationRuntime{Projection: current.Record}
	var removalConfiguration etcd.EntryRemovalConfiguration
	if request.action != entryDesiredMutationRemove {
		runtime, err = service.runtime.CaptureEntryMutationRuntime(ctx, current)
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
	} else {
		removalConfiguration, err = service.work.ReadEntryRemovalConfiguration(ctx, request.environmentID, current.ReadRevision)
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
		runtime.EpochRevision = removalConfiguration.EpochRevision
	}
	candidateTaskID := request.accepted.Task.ID
	candidateRecord, previous, err := entryDesiredCandidateRecord(
		current.Record.EnvironmentID,
		current.Record.Entries,
		request,
		candidateTaskID,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
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
		return taskdependencies.Prepared{}, err
	}
	candidate = controllerrevision.CloneProjection(candidate)
	claim, _, err := controllerrevision.PreflightAndClaim(ctx, service.repository, candidate,
		controllerrevision.ClaimInput{
			EnvironmentID: request.environmentID, CandidateTaskID: candidateTaskID,
			Locator: request.locator, Intent: request.evidence.durable,
			MatchExistingIntent:  request.evidence.matchesStaged,
			BaselineHeadRevision: expectedHeadRevision, SourceKind: blueprints.EnvironmentBlueprintSourceMutation,
			RenderGeneration: generation, CreatedAt: request.accepted.Task.CreatedAt, AcceptedTaskRevision: request.accepted.Revision, TaskLocator: &request.locator,
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
			_ = service.repository.AbandonEnvironmentBlueprintStage(cleanupCtx, claim)
		}
	}()
	candidateRecord, previous, err = entryDesiredCandidateRecord(
		current.Record.EnvironmentID,
		current.Record.Entries,
		request,
		claim.TaskID,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if request.action != entryDesiredMutationRemove {
		if err := service.prepareEntryGeneration(
			ctx, project.Record.ID, request.environmentID, request.desired, *candidateRecord, request.at,
		); err != nil {
			return taskdependencies.Prepared{}, err
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
		return taskdependencies.Prepared{}, err
	}
	candidate = controllerrevision.CloneProjection(candidate)
	if previous != nil {
		serviceIdentities, snapshotErr := entryDesiredServiceIdentities(candidate)
		if snapshotErr != nil {
			return taskdependencies.Prepared{}, snapshotErr
		}
		removals, err := PlanEntryRemovals(
			request.environmentID, []entryrecord.Record{*previous}, entries,
			serviceIdentities,
		)
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
		materializations = append(materializations, removals...)
	}
	allocator, err := controllerrevision.NewBlueprintIdentityAllocator(claim)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	var references []materializationrecord.Record
	if request.action == entryDesiredMutationRemove {
		references, err = entryfiles.RemoveEntry(
			ctx,
			request.entryID,
			removalConfiguration.Files,
			allocator.Named,
			service.materials,
		)
	} else {
		references, err = service.entryMaterializations(ctx, request.environmentID, allocator, materializations)
	}
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	response, err := entryTaskResponse(claim.TaskID)
	if err != nil {
		return taskdependencies.Prepared{}, err
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
		return taskdependencies.Prepared{}, err
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
		return taskdependencies.Prepared{}, err
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
		return taskdependencies.Prepared{}, err
	}
	workAction := workexecution.ActionUpdate
	targetName := request.accepted.Task.TargetName
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
	prepared, publicationErr := service.prepareConfiguration(ctx, entryConfigurationPublication{
		Project: project, Environment: environment, Claim: claim, Runtime: runtime, Accepted: request.accepted, At: request.at,
		Candidate: candidate, ReadRevision: current.ReadRevision, Files: references,
		Marker: marker, Action: workAction, TargetID: targetEntryID, TargetName: targetName,
		Removal: removal, ConfigurationRevision: removalConfiguration.HeadRevision,
	})
	if publicationErr != nil {
		return taskdependencies.Prepared{}, publicationErr
	}
	priorFinalize := prepared.Finalize
	prepared.Finalize = func(finalCtx context.Context, committed bool) error {
		var finishErr error
		if priorFinalize != nil {
			finishErr = priorFinalize(finalCtx, committed)
		}
		if !committed {
			cleanupErr := service.repository.AbandonEnvironmentBlueprintStage(finalCtx, claim)
			if kind, _ := errs.KindOf(cleanupErr); cleanupErr != nil && kind != errs.KindStateConflict {
				return errs.WrapJoined(errs.KindInternal, finishErr, cleanupErr)
			}
		}
		return finishErr
	}
	keepStage = true
	return prepared, nil
}
