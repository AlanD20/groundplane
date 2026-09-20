package blueprint

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/attachments"
	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/controller/entry"
	entryoperations "github.com/AlanD20/groundplane/internal/controller/entry/operations"
	"github.com/AlanD20/groundplane/internal/controller/entrygeneration"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"io"
	"net/http"
	"net/netip"
	"strings"
	"time"
)

const (
	environmentBlueprintRoute           = "/environments/{id}/blueprint"
	maximumEnvironmentBlueprintAttempts = 3
)

type Service struct {
	volumeRoot        string
	environmentPool   netip.Prefix
	repository        environmentBlueprintRepository
	idempotency       *desiredrevision.Idempotency
	materials         materializationResolver
	releaseGroups     *controller.ReleaseGroupBlueprintPlanner
	blueprintReleases *blueprintrelease.Service
	entryGeneration   *entrygeneration.EntryGenerationService
	attachFacts       *attachments.FactService
	componentCatalog  []controller.EnvironmentComponentRegistration
	backups           environmentBlueprintBackupRepository
	backupKeys        environmentBlueprintBackupKeyFactory
	random            io.Reader
	now               func() time.Time
}

func NewService(
	volumeRoot string,
	environmentPool string,
	repository environmentBlueprintRepository,
	idempotency *desiredrevision.Idempotency,
	materials materializationResolver,
	releaseGroups *controller.ReleaseGroupBlueprintPlanner,
	blueprintReleases *blueprintrelease.Service,
	entryGeneration *entrygeneration.EntryGenerationService,
	attachFacts *attachments.FactService,
	componentCatalog []controller.EnvironmentComponentRegistration,
	backups environmentBlueprintBackupRepository,
	backupKeys environmentBlueprintBackupKeyFactory,
) (*Service, error) {
	parsedEnvironmentPool, poolErr := netip.ParsePrefix(environmentPool)
	if poolErr != nil || !parsedEnvironmentPool.Addr().Is4() ||
		parsedEnvironmentPool != parsedEnvironmentPool.Masked() ||
		repository == nil ||
		idempotency == nil ||
		materials == nil ||
		releaseGroups == nil ||
		entryGeneration == nil ||
		attachFacts == nil ||
		blueprintReleases == nil {
		return nil, errs.New(errs.KindInternal, "Environment Blueprint service is not configured")
	}
	if _, err := controller.NewTaskPlanResolver(volumeRoot, componentCatalog); err != nil {
		return nil, err
	}
	return &Service{
		volumeRoot: volumeRoot, environmentPool: parsedEnvironmentPool, repository: repository, idempotency: idempotency,
		materials: materials, releaseGroups: releaseGroups, blueprintReleases: blueprintReleases,
		entryGeneration:  entryGeneration,
		attachFacts:      attachFacts,
		componentCatalog: controller.CloneEnvironmentComponentCatalog(componentCatalog),
		backups:          backups,
		backupKeys:       backupKeys,
		random:           rand.Reader, now: time.Now,
	}, nil
}

func (service *Service) ApplyBlueprint(
	ctx context.Context,
	environmentID string,
	bundle core.BlueprintBundle,
	expectedRevision string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	return service.applyBlueprint(ctx, environmentID, environmentID, bundle, expectedRevision, idempotencyKey, false)
}

func (service *Service) ApplyComponentBlueprint(
	ctx context.Context,
	environmentID string,
	componentID string,
	bundle core.BlueprintBundle,
	expectedRevision, idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if ids.Validate(ids.KindComponent, componentID) != nil || expectedRevision == "" {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Component id or Blueprint revision is invalid",
		)
	}
	return service.applyBlueprint(ctx, environmentID, environmentID, bundle, expectedRevision, idempotencyKey, true)
}

func (service *Service) applyBlueprint(
	ctx context.Context,
	environmentID string,
	taskTarget string,
	bundle core.BlueprintBundle,
	expectedRevision string,
	idempotencyKey string,
	preserveRoutes bool,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Environment Blueprint context is required")
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Environment id is invalid")
	}
	if err := bundle.Validate(); err != nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Blueprint bundle is invalid")
	}
	for attempt := 0; attempt < maximumEnvironmentBlueprintAttempts; attempt++ {
		response, err := service.applyBlueprintOnce(
			ctx,
			environmentID,
			taskTarget,
			bundle,
			expectedRevision,
			idempotencyKey,
			preserveRoutes,
		)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumEnvironmentBlueprintAttempts-1 {
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Environment Blueprint retry bound was not enforced")
}

func (service *Service) applyBlueprintOnce(
	ctx context.Context,
	environmentID string,
	taskTarget string,
	bundle core.BlueprintBundle,
	expectedRevision string,
	idempotencyKey string,
	preserveRoutes bool,
) (etcd.IdempotencyResponse, error) {
	evidence, err := service.idempotency.Prepare(ctx, desiredrevision.IntentAddress{
		Method: http.MethodPut, Route: environmentBlueprintRoute,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: environmentID},
		Path:  []requestidempotency.PathBinding{{Name: "id", Value: environmentID}},
	}, bundle)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.Durable.Ciphertext)
	locator := etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: environmentID,
		Method: http.MethodPut, Route: environmentBlueprintRoute, Key: idempotencyKey,
	}
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != requestidempotency.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Environment Blueprint replay resolution is invalid",
			)
		}
		return requestidempotency.CloneResponse(resolution.Response), nil
	}

	baseline, err := service.loadApplyBaseline(ctx, environmentID, expectedRevision)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	environment, project, tenant := baseline.environment, baseline.project, baseline.tenant
	taskOwner := baseline.taskOwner
	previousProjection, hasProjection := baseline.previousProjection, baseline.hasProjection
	expectedHeadRevision, previous, generation := baseline.expectedHeadRevision, baseline.previous, baseline.generation
	candidateTaskID := ids.New(ids.KindTask)
	candidateCreatedAt := service.now().UTC()

	preflight, err := service.prepareApplyPreflight(ctx, environmentID, bundle, baseline, preserveRoutes)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	bundle, parsed := preflight.bundle, preflight.parsed
	currentAttaches, attachReadRevision := preflight.currentAttaches, preflight.attachReadRevision
	requirements, submittedServiceNames := preflight.requirements, preflight.submittedServiceNames
	priorProject, desiredEnvironment, workloads := preflight.priorProject, preflight.desiredEnvironment, preflight.workloads
	claim, err := desiredrevision.Claim(
		ctx, service.repository, desiredrevision.ClaimInput{
			EnvironmentID: environmentID, CandidateTaskID: candidateTaskID,
			Locator: locator, Intent: evidence.Durable, BaselineHeadRevision: expectedHeadRevision,
			SourceKind: etcd.EnvironmentBlueprintSourceApply,
			MatchExistingIntent: func(ctx context.Context, existing etcd.ProtectedIntentRecord) (bool, error) {
				return service.idempotency.MatchesStaged(ctx, evidence, existing)
			},
			RenderGeneration: generation, CreatedAt: candidateCreatedAt,
		})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	allocator, err := desiredrevision.NewBlueprintIdentityAllocator(claim)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	taskID := claim.TaskID
	now := claim.CreatedAt
	releaseMemberships, err := blueprintrelease.BuildNormalizedServiceMemberships(priorProject, parsed.Project)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	normalizedCompose, err := controller.MarshalNormalizedEnvironmentProject(parsed.Project)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	runtimeFiles, err := blueprintparser.SelectRuntimeFiles(
		parsed.Project,
		bundle.Files,
		previousProjection.Record.RuntimeFiles,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	volumeSlugs, err := environmentBlueprintVolumeSlugs(
		parsed.Project,
		previousProjection.Record,
		hasProjection,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	changes, err := composeidentity.ReconcileOwned(parsed.Project, previous, allocator.New)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if len(changes.RemovedServiceIDs) != 0 || len(changes.RemovedNetworkIDs) != 0 ||
		len(changes.RemovedVolumeIDs) != 0 {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindResourceInUse,
			"Blueprint omits an existing owned resource; remove it explicitly before apply",
		)
	}
	zoneOwnerKind := core.ZoneOwnerEnvironment
	zoneOwnerID := environmentID
	if project.Record.Kind == hierarchyrecord.ProjectKindBacking {
		zoneOwnerKind = core.ZoneOwnerBackingProject
		zoneOwnerID = project.Record.ID
	}
	desiredZones, err := controller.ProjectZoneProjection(
		parsed.Project,
		changes.Current,
		zoneOwnerKind,
		zoneOwnerID,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	currentZones, err := service.listBlueprintZones(ctx, environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	zoneChanges, err := prepareEnvironmentBlueprintZoneChanges(environmentID, desiredZones, currentZones)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	currentServices, err := service.listBlueprintServices(ctx, environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	serviceExtensions, err := preserveEnvironmentBlueprintServiceExtensions(
		parsed.ServiceExtensions,
		submittedServiceNames,
		previous.Services,
		previousProjection.Record.ServiceExtensions,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	desiredServices, err := controller.ProjectServiceProjection(
		parsed.Project,
		changes.Current,
		serviceExtensions,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	serviceChanges, err := blueprintrelease.PrepareServiceChanges(
		environmentID, desiredServices, currentServices,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	currentEntries, err := service.listBlueprintEntries(ctx, environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	pinnedEntries, err := entry.BlueprintProjection(currentEntries)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	reconciledEntries, err := controller.ReconcileBlueprintEntries(
		environmentID, parsed.Extensions.Entries, pinnedEntries, allocator.Named,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	reconciledScripts, scriptPublication, err := service.prepareApplyScripts(
		ctx, environmentID, claim.RevisionID, parsed.Extensions.Scripts, desiredServices,
		desiredrevision.BlueprintScriptResources{Volumes: changes.Current.Volumes, Entries: reconciledEntries.Current},
		allocator.Named,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer scriptPublication.Clear()
	serviceNames := make(map[string]string, len(desiredServices))
	for _, desired := range desiredServices {
		serviceNames[desired.ID] = desired.Name
	}
	effectiveReleaseGroups, err := service.releaseGroups.AuthoringSpecs(ctx, environmentID, serviceNames)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	for name, spec := range parsed.Extensions.ReleaseGroups {
		effectiveReleaseGroups[name] = spec
	}
	releaseGroupPreparation, err := service.releaseGroups.Prepare(
		ctx,
		environmentID,
		effectiveReleaseGroups,
		desiredServices,
		func() string { return allocator.New(ids.KindReleaseGroup) },
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	reconciledRoutes, routeChanges, err := service.prepareApplyRoutes(ctx, environmentID, &parsed, desiredServices, preserveRoutes, allocator.New)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	currentComponents, err := service.listBlueprintComponents(ctx, environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	componentPreparation, pinnedComponents, effectiveComponents, err := service.prepareBlueprintComponents(
		ctx,
		environmentID,
		taskID,
		now,
		allocator.New,
		parsed.Extensions.Components,
		currentComponents,
		zoneChanges,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	preparedAttaches, err := service.prepareBlueprintAttaches(
		ctx, environmentID, taskID, parsed.Extensions.Attachments, serviceChanges, currentAttaches,
		allocator.Named, componentTaskPreparationIsZeroForBlueprint(componentPreparation), now,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer preparedAttaches.clear()
	componentEnvironment := blueprintComponentEnvironment(
		desiredEnvironment.Record,
		desiredZones,
		desiredServices,
		reconciledRoutes.Current,
		effectiveComponents,
		reconciledEntries.Current,
	)
	componentProjection, err := controller.ProjectEnvironmentComponents(
		parsed.Project,
		componentEnvironment,
		service.componentCatalog,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	renderIdentities := environmentComponentComposeIdentities(changes.Current, componentProjection.Services)
	entryProjection, err := controller.ProjectEnvironmentEntries(
		componentProjection.Project,
		environmentID,
		environment.Record.VolumeDir,
		changes.Current.Services,
		reconciledEntries.Current,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	entryRemovals, err := entryoperations.PlanEntryRemovals(
		environmentID, reconciledEntries.Removed, reconciledEntries.Current, renderIdentities.Services,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	entryProjection.Materializations = append(entryProjection.Materializations, entryRemovals...)
	componentProjection.Project = entryProjection.Project
	attachZones, attachServices, _ := environmentBlueprintTopologyProjection(zoneChanges, serviceChanges, nil)
	externalNetworks, err := controller.ProjectEnvironmentAttachNetworks(
		componentProjection.Project, environmentID, attachZones, attachServices, preparedAttaches.effective,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	volumeMounts, err := environmentBlueprintVolumeMounts(componentProjection.Project, renderIdentities)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	planID := allocator.Named(ids.KindPlan, "execution-plan")
	artifactID := allocator.Named(ids.KindConfig, "compose-artifact")
	artifact, err := controller.RenderCompose(controller.ComposeRenderInput{
		Project: componentProjection.Project, ArtifactID: artifactID,
		ProjectOwnerKind: controller.ComposeProjectOwnerTenant,
		TenantID:         tenant.Record.ID, ProjectID: project.Record.ID, EnvironmentID: environmentID,
		PlanID: planID, RenderGeneration: generation, AuthorizedVolumeDir: environment.Record.VolumeDir,
		Identities: renderIdentities, ExternalNetworks: externalNetworks,
		RetainedComponentRuntime: componentPreparation.AppliedComponentRuntime(),
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	artifact, err = service.blueprintReleases.PrepareRuntimeArtifact(workloads, artifact, serviceChanges)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	artifactValue, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	entryGeneration := service.entryGeneration.WithFactResolver(preparedAttaches.facts)
	if err := service.prepareBlueprintEntryValues(
		ctx, entryGeneration, project.Record.ID, environmentID, reconciledEntries, now,
	); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	materializations, materializationSteps, err := service.environmentComponentMaterializations(
		ctx,
		environmentID,
		project.Record.ID,
		taskID,
		artifactID,
		generation,
		allocator.Named,
		runtimeFiles,
		componentProjection,
		entryProjection.Materializations,
		reconciledEntries.Current,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	componentSteps, _, err := controller.BuildEnvironmentComponentTaskContribution(
		controller.EnvironmentComponentTaskContributionInput{
			Apply: controller.EnvironmentManagedConfigApplyInput{
				RevisionID: taskID, RenderGeneration: generation, Components: pinnedComponents,
				ComponentCatalog: service.componentCatalog,
				Materializations: materializations, Artifact: artifact,
			},
			AllocateStep: func() string {
				return allocator.Named(ids.KindStep, "http-router-config-activate")
			},
			TimeoutSeconds: uint32(desiredrevision.TaskTimeoutSeconds),
		},
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	steps := append([]*agentpb.ExecutionStep(nil), materializationSteps...)
	stepRecords := make([]etcd.TaskStepRecord, 0, len(materializationSteps)+4)
	for _, step := range materializationSteps {
		stepRecords = append(stepRecords, etcd.TaskStepRecord{Kind: etcd.TaskStepOperation, ID: step.StepId})
	}
	managedVolumeIDs := managedEnvironmentVolumeIDs(changes.Current.Volumes)
	var volumeIntentDigest []byte
	if len(managedVolumeIDs) != 0 {
		intentDigest, decodeErr := hex.DecodeString(evidence.Durable.CiphertextDigest)
		if decodeErr != nil || len(intentDigest) != sha256.Size {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Blueprint protected intent digest is invalid",
			)
		}
		volumeIntentDigest = intentDigest
		stepID := allocator.Named(ids.KindStep, "managed-volume-directories")
		steps = append(steps, &agentpb.ExecutionStep{
			StepId: stepID, TimeoutSeconds: uint32(desiredrevision.TaskTimeoutSeconds),
			Payload: &agentpb.ExecutionStep_ManagedVolumeDirectoriesEnsure{
				ManagedVolumeDirectoriesEnsure: &agentpb.ManagedVolumeDirectoriesEnsure{
					ArtifactId: artifactID, VolumeIds: managedVolumeIDs,
					IntentSha256: append([]byte(nil), intentDigest...),
				},
			},
		})
		stepRecords = append(stepRecords, etcd.TaskStepRecord{Kind: etcd.TaskStepOperation, ID: stepID})
	}
	attachSteps, attachStepRecords, err := preparedAttaches.procedureSteps(
		taskID, desiredrevision.TaskTimeoutSeconds, allocator.Named,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clearBlueprintAttachProcedureSteps(attachSteps)
	steps = append(steps, attachSteps...)
	stepRecords = append(stepRecords, attachStepRecords...)
	dependencyPlans, err := buildEnvironmentDependencyPlans(renderIdentities.Services, serviceExtensions)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	revision := desiredrevision.BlueprintRevision(environmentID, taskID, now, bundle)
	projection := desiredrevision.ComposeProjection(
		environmentID,
		taskID,
		generation,
		renderIdentities,
		volumeSlugs,
		volumeMounts,
		artifactValue,
		normalizedCompose,
		runtimeFiles,
		serviceExtensions,
		reconciledRoutes.Current,
		pinnedComponents,
		reconciledEntries.Current,
	)
	projection.ManagedComponentRuntimeSources = append(
		[]etcd.ManagedComponentRuntimeSource(nil),
		previousProjection.Record.ManagedComponentRuntimeSources...,
	)
	projection.ManagedComponentRuntimeSources, err = etcd.ProjectManagedComponentRuntimeSources(
		componentPreparation,
		projection,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	topologyZones, topologyServices, topologyRoutes := environmentBlueprintTopologyProjection(
		zoneChanges, serviceChanges, routeChanges,
	)
	projection = desiredrevision.WithDesiredTopology(projection, topologyZones, topologyServices, topologyRoutes)
	projection.ServiceDependencyPlans = dependencyPlans.Clone()
	projection.BlueprintRequirements = requirements.Clone()
	backup, backupPreparation, err := service.prepareEnvironmentBlueprintBackup(
		ctx, environmentID, taskID, attachReadRevision, parsed.Extensions.Backup,
		projection, preparedAttaches, allocator.Named, now,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer backupPreparation.Clear()
	projection.Backup = backup
	stagedPublication, err := desiredrevision.Stage(ctx, service.repository, desiredrevision.StageInput{
		Claim: claim, Blueprint: revision, Projection: projection,
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	params := map[string]string{
		etcd.EnvironmentDesiredRevisionParam:         taskID,
		etcd.TaskMaterializationEnvironmentParam:     environmentID,
		controller.EnvironmentBlueprintArtifactParam: artifactID,
		taskcontract.EnvironmentBlueprintProcedureParam: string(
			taskcontract.BlueprintComposeProcedureNone,
		),
	}
	if len(managedVolumeIDs) != 0 {
		params[controller.EnvironmentBlueprintManagedVolumesParam] = strings.Join(managedVolumeIDs, ",")
		params[controller.VolumeTaskIntentSHA256Param] = hex.EncodeToString(volumeIntentDigest)
	}
	task := etcd.TaskRecord{
		ID: taskID, OperationID: allocator.Named(ids.KindOperation, "operation"), IdempotencyKey: idempotencyKey,
		Owner: taskOwner, Actor: etcd.TaskActorOperator,
		Executor: etcd.TaskExecutorAgent, PlanID: planID,
		RenderGeneration: int32(generation), Type: etcd.TaskUpdate, Target: taskTarget,
		Params: params, Steps: stepRecords, TimeoutSeconds: desiredrevision.TaskTimeoutSeconds,
		Materializations: materializations,
		Status:           etcd.TaskStatusPending, NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	task.ManagedComponentTeardownSources = componentPreparation.ManagedComponentTeardownSources()
	preparedRelease, err := service.blueprintReleases.Prepare(ctx, blueprintrelease.PrepareInput{
		IntendedAttaches: preparedAttaches.effective,
		Workloads:        workloads,
		VolumeRoot:       service.volumeRoot,
		Tenant:           tenant, Project: project, Environment: environment,
		Projection: projection, ServiceChanges: serviceChanges, Memberships: releaseMemberships,
		Scripts:       reconciledScripts.Current,
		ReleaseGroups: effectiveReleaseGroups, Task: task,
		PrefixSteps: steps, ComponentSteps: componentSteps, Artifact: artifact,
		AllocateNamed: allocator.Named, CreatedAt: now,
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, desiredrevision.Abandon(ctx, service.repository, stagedPublication, err)
	}
	task = preparedRelease.Task
	abandonPrepared := func(cause error) error {
		return desiredrevision.AbandonBlueprint(
			ctx, service.repository, stagedPublication, preparedRelease.Publication, cause,
		)
	}
	requirementGate, err := service.prepareRequirementGate(ctx, task, requirements)
	if err != nil {
		return etcd.IdempotencyResponse{}, abandonPrepared(err)
	}
	componentPreparation, routeChanges, err = service.prepareRoutePublication(
		componentEnvironment, pinnedComponents, componentPreparation, generation, routeChanges,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, abandonPrepared(err)
	}
	return desiredrevision.Publish(ctx, service.repository, service.idempotency, desiredrevision.PublishInput{
		Project: project, Environment: environment, EnvironmentPool: service.environmentPool,
		NetworkPool: desiredEnvironment.Record.NetworkPool, ExpectedHeadRevision: expectedHeadRevision,
		Staged: stagedPublication, Evidence: evidence, Locator: locator,
		ZoneChanges: zoneChanges, ServiceChanges: serviceChanges, RouteChanges: routeChanges,
		ReleaseGroupPreparation: releaseGroupPreparation,
		ComponentPreparation:    componentPreparation,
		AttachPreparation:       preparedAttaches.publication,
		BackupPreparation:       backupPreparation,
		ScriptPublication:       scriptPublication,
		ReleasePublication:      preparedRelease.Publication,
		RequirementGate:         requirementGate,
		Task:                    task,
	})
}
