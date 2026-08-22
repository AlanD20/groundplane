package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	componentregistry "github.com/AlanD20/groundplane/internal/components"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	environmentBlueprintRoute           = "/environments/{id}/blueprint"
	environmentBlueprintTimeoutSeconds  = int64(120)
	maximumEnvironmentBlueprintAttempts = 3
)

type environmentBlueprintRepository interface {
	GetTenant(context.Context, string) (etcd.Versioned[etcd.TenantRecord], error)
	GetProject(context.Context, string) (etcd.Versioned[etcd.ProjectRecord], error)
	GetEnvironment(context.Context, string) (etcd.Versioned[etcd.EnvironmentRecord], error)
	GetEnvironmentBlueprintHead(context.Context, string) (etcd.Versioned[etcd.EnvironmentBlueprintHead], bool, error)
	GetEnvironmentComposeProjection(
		context.Context,
		string,
	) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error)
	ListZones(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.ZoneRecord], error)
	ListServices(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.ServiceRecord], error)
	ListRoutes(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.RouteRecord], error)
	ListEntries(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.EntryRecord], error)
	ListEnvironmentComponents(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.ComponentRecord], error)
	PrepareEnvironmentComponentTask(
		context.Context,
		string,
		string,
		[]etcd.EnvironmentBlueprintZoneChange,
		[]etcd.EnvironmentComponentCandidateInput,
		time.Time,
	) (etcd.ComponentTaskPreparation, error)
	ApplyEnvironmentBlueprintWithTask(
		context.Context,
		etcd.Versioned[etcd.ProjectRecord],
		etcd.Versioned[etcd.EnvironmentRecord],
		int64,
		etcd.EnvironmentBlueprintRevision,
		etcd.EnvironmentComposeProjection,
		[]etcd.EnvironmentBlueprintZoneChange,
		[]etcd.EnvironmentBlueprintServiceChange,
		[]etcd.EnvironmentBlueprintRouteChange,
		etcd.ComponentTaskPreparation,
		etcd.TaskRecord,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type environmentBlueprintEvidence struct {
	candidate idempotentintent.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type environmentBlueprintIdempotency interface {
	Prepare(context.Context, string, core.BlueprintBundle) (environmentBlueprintEvidence, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		environmentBlueprintEvidence,
	) (idempotentintent.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		environmentBlueprintEvidence,
		etcd.IdempotencyTransactionResult,
	) (idempotentintent.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		environmentBlueprintEvidence,
		error,
	) (idempotentintent.Resolution, error)
}

type durableEnvironmentBlueprintIdempotency struct {
	coordinator *idempotentintent.Coordinator
	repository  *etcd.IdempotencyRepository
}

func newDurableEnvironmentBlueprintIdempotency(
	coordinator *idempotentintent.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableEnvironmentBlueprintIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Environment Blueprint idempotency is not configured")
	}
	return &durableEnvironmentBlueprintIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableEnvironmentBlueprintIdempotency) Prepare(
	ctx context.Context,
	environmentID string,
	bundle core.BlueprintBundle,
) (environmentBlueprintEvidence, error) {
	manifest, err := environmentBlueprintIntentManifest(bundle)
	if err != nil {
		return environmentBlueprintEvidence{}, err
	}
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: http.MethodPut,
		Route:  environmentBlueprintRoute,
		Scope:  idempotentintent.Scope{Kind: idempotentintent.ScopeEnvironment, ID: environmentID},
		Path:   []idempotentintent.PathBinding{{Name: "id", Value: environmentID}},
		Query:  idempotentintent.Object(),
		Body:   idempotentintent.BlueprintBody(manifest),
	})
	if err != nil {
		return environmentBlueprintEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return environmentBlueprintEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return environmentBlueprintEvidence{}, err
	}
	return environmentBlueprintEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableEnvironmentBlueprintIdempotency) ResolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence environmentBlueprintEvidence,
) (idempotentintent.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableEnvironmentBlueprintIdempotency) ResolveKnown(
	ctx context.Context,
	evidence environmentBlueprintEvidence,
	result etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableEnvironmentBlueprintIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence environmentBlueprintEvidence,
	original error,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type environmentBlueprintService struct {
	volumeRoot  string
	repository  environmentBlueprintRepository
	idempotency environmentBlueprintIdempotency
	materials   environmentBlueprintMaterializationResolver
	now         func() time.Time
}

type environmentBlueprintMaterializationResolver interface {
	ResolveTaskMaterializationSource(
		context.Context,
		string,
		etcd.TaskMaterializationSource,
	) ([]byte, error)
}

type durableEnvironmentBlueprintRepository struct {
	*etcd.HierarchyRepository
	zones      *etcd.ZoneRepository
	services   *etcd.ServiceRepository
	routes     *etcd.RouteRepository
	entries    *etcd.EntryRepository
	components *etcd.ComponentRepository
}

func newDurableEnvironmentBlueprintRepository(
	hierarchy *etcd.HierarchyRepository,
	zones *etcd.ZoneRepository,
	services *etcd.ServiceRepository,
	routes *etcd.RouteRepository,
	entries *etcd.EntryRepository,
	components *etcd.ComponentRepository,
) (*durableEnvironmentBlueprintRepository, error) {
	if hierarchy == nil || zones == nil || services == nil || routes == nil || entries == nil || components == nil {
		return nil, errs.New(errs.KindInternal, "Environment Blueprint repositories are not configured")
	}
	return &durableEnvironmentBlueprintRepository{
		HierarchyRepository: hierarchy,
		zones:               zones,
		services:            services,
		routes:              routes,
		entries:             entries,
		components:          components,
	}, nil
}

func (repository *durableEnvironmentBlueprintRepository) ListEntries(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.EntryRecord], error) {
	return repository.entries.ListEntries(ctx, environmentID, request)
}

func (repository *durableEnvironmentBlueprintRepository) ListEnvironmentComponents(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.ComponentRecord], error) {
	return repository.components.ListEnvironmentComponents(ctx, environmentID, request)
}

func (repository *durableEnvironmentBlueprintRepository) ListZones(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.ZoneRecord], error) {
	return repository.zones.ListZones(ctx, environmentID, request)
}

func (repository *durableEnvironmentBlueprintRepository) ListServices(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.ServiceRecord], error) {
	return repository.services.ListServices(ctx, environmentID, request)
}

func (repository *durableEnvironmentBlueprintRepository) ListRoutes(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.RouteRecord], error) {
	return repository.routes.ListRoutes(ctx, environmentID, request)
}

func newEnvironmentBlueprintService(
	volumeRoot string,
	repository environmentBlueprintRepository,
	idempotency environmentBlueprintIdempotency,
	materials environmentBlueprintMaterializationResolver,
) (*environmentBlueprintService, error) {
	if repository == nil || idempotency == nil || materials == nil {
		return nil, errs.New(errs.KindInternal, "Environment Blueprint service is not configured")
	}
	if _, err := controller.NewTaskPlanResolver(volumeRoot); err != nil {
		return nil, err
	}
	return &environmentBlueprintService{
		volumeRoot: volumeRoot, repository: repository, idempotency: idempotency,
		materials: materials, now: time.Now,
	}, nil
}

func (service *environmentBlueprintService) ApplyBlueprint(
	ctx context.Context,
	environmentID string,
	bundle core.BlueprintBundle,
	idempotencyKey string,
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
		response, err := service.applyBlueprintOnce(ctx, environmentID, bundle, idempotencyKey)
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

func (service *environmentBlueprintService) applyBlueprintOnce(
	ctx context.Context,
	environmentID string,
	bundle core.BlueprintBundle,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	evidence, err := service.idempotency.Prepare(ctx, environmentID, bundle)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: environmentID,
		Method: http.MethodPut, Route: environmentBlueprintRoute, Key: idempotencyKey,
	}
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != idempotentintent.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Environment Blueprint replay resolution is invalid",
			)
		}
		return cloneIdempotencyResponse(resolution.Response), nil
	}

	environment, err := service.repository.GetEnvironment(ctx, environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	tenant, err := service.repository.GetTenant(ctx, project.Record.TenantID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if environment.Record.ProjectID != project.Record.ID || project.Record.TenantID != tenant.Record.ID ||
		project.Record.Kind != etcd.ProjectKindTenant {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Environment hierarchy is inconsistent")
	}

	head, hasHead, err := service.repository.GetEnvironmentBlueprintHead(ctx, environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	previousProjection, hasProjection, err := service.repository.GetEnvironmentComposeProjection(ctx, environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	expectedHeadRevision, previous, generation, err := environmentBlueprintState(
		environmentID, head, hasHead, previousProjection, hasProjection,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if generation > math.MaxInt32 {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Environment render generation is exhausted")
	}

	parsed, err := blueprintparser.Parse(ctx, blueprintparser.EnvironmentScope{
		EnvironmentID: environmentID,
		Tenant:        tenant.Record.Slug, Project: project.Record.Slug, Environment: environment.Record.Name,
	}, bundle)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if err := validateBasicEnvironmentBlueprint(parsed); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	changes, err := controller.ReconcileOwnedComposeIdentities(parsed.Project, previous, ids.New)
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
	desiredZones, err := controller.ProjectZoneProjection(parsed.Project, changes.Current, environmentID)
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
	desiredServices, err := controller.ProjectServiceProjection(parsed.Project, changes.Current)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	currentServices, err := service.listBlueprintServices(ctx, environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	serviceChanges, err := prepareEnvironmentBlueprintServiceChanges(
		environmentID,
		desiredServices,
		currentServices,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	currentRoutes, err := service.listBlueprintRoutes(ctx, environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	previousRoutes := make([]controller.RouteIdentity, len(currentRoutes))
	for index, route := range currentRoutes {
		previousRoutes[index] = controller.RouteIdentity{
			ID: route.Record.Desired.ID, Host: route.Record.Desired.Host, Path: route.Record.Desired.Path,
		}
	}
	reconciledRoutes, err := controller.ReconcileBlueprintRoutes(
		parsed.Extensions.Routes,
		desiredServices,
		previousRoutes,
		ids.New,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if len(reconciledRoutes.RemovedRouteIDs) != 0 {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindResourceInUse,
			"Blueprint omits an existing Route; remove it explicitly before apply",
		)
	}
	routeChanges, err := prepareEnvironmentBlueprintRouteChanges(
		environmentID,
		reconciledRoutes.Current,
		currentRoutes,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	currentComponents, err := service.listBlueprintComponents(ctx, environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	currentEntries, err := service.listBlueprintEntries(ctx, environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	now := service.now().UTC()
	taskID := ids.New(ids.KindTask)
	componentPreparation, pinnedComponents, effectiveComponents, err := service.prepareBlueprintComponents(
		ctx,
		environmentID,
		taskID,
		now,
		parsed.Extensions.Components,
		currentComponents,
		zoneChanges,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	pinnedEntries, err := environmentEntryProjection(currentEntries)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	componentProjection, err := controller.ProjectEnvironmentComponents(
		parsed.Project,
		blueprintComponentEnvironment(
			environment.Record,
			desiredZones,
			desiredServices,
			reconciledRoutes.Current,
			effectiveComponents,
			pinnedEntries,
		),
		componentregistry.All(),
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	renderIdentities := environmentComponentComposeIdentities(changes.Current, componentProjection.Services)
	planID := ids.New(ids.KindPlan)
	artifactID := ids.New(ids.KindConfig)
	artifact, err := controller.RenderCompose(controller.ComposeRenderInput{
		Project: componentProjection.Project, ArtifactID: artifactID,
		TenantID: tenant.Record.ID, ProjectID: project.Record.ID, EnvironmentID: environmentID,
		PlanID: planID, RenderGeneration: generation, AuthorizedVolumeDir: environment.Record.VolumeDir,
		Identities: renderIdentities,
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	materializations, materializationSteps, err := service.environmentComponentMaterializations(
		ctx,
		environmentID,
		taskID,
		artifactID,
		componentProjection,
		pinnedEntries,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	steps := append([]*agentpb.ExecutionStep(nil), materializationSteps...)
	stepRecords := make([]etcd.TaskStepRecord, 0, len(materializationSteps)+2)
	for _, step := range materializationSteps {
		stepRecords = append(stepRecords, etcd.TaskStepRecord{ID: step.StepId})
	}
	if len(artifact.Volumes) != 0 {
		stepID := ids.New(ids.KindStep)
		steps = append(steps, &agentpb.ExecutionStep{
			StepId: stepID, TimeoutSeconds: uint32(environmentBlueprintTimeoutSeconds),
			Payload: &agentpb.ExecutionStep_ManagedVolumeDirectoriesEnsure{
				ManagedVolumeDirectoriesEnsure: &agentpb.ManagedVolumeDirectoriesEnsure{ArtifactId: artifactID},
			},
		})
		stepRecords = append(stepRecords, etcd.TaskStepRecord{ID: stepID})
	}
	applyStepID := ids.New(ids.KindStep)
	steps = append(steps, &agentpb.ExecutionStep{
		StepId: applyStepID, TimeoutSeconds: uint32(environmentBlueprintTimeoutSeconds),
		Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
			ArtifactId: artifactID, FullReconcile: true,
		}},
	})
	stepRecords = append(stepRecords, etcd.TaskStepRecord{ID: applyStepID})
	plan, err := controller.BuildPlan(controller.PlanBuildInput{
		VolumeRoot: service.volumeRoot, PlanID: planID, RenderGeneration: generation,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_RECONCILE, TargetID: environmentID,
		Artifacts: []*agentpb.ComposeArtifact{artifact}, Steps: steps,
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	task := etcd.TaskRecord{
		ID: taskID, OperationID: ids.New(ids.KindOperation), IdempotencyKey: idempotencyKey,
		Executor: etcd.TaskExecutorAgent, PlanID: planID, PlanHash: hex.EncodeToString(plan.PlanHash),
		RenderGeneration: int32(generation), Type: etcd.TaskUpdate, Target: environmentID,
		Params: map[string]string{
			etcd.EnvironmentBlueprintRevisionParam:       taskID,
			etcd.TaskMaterializationEnvironmentParam:     environmentID,
			controller.EnvironmentBlueprintArtifactParam: artifactID,
		},
		Steps: stepRecords, TimeoutSeconds: environmentBlueprintTimeoutSeconds,
		Materializations: materializations,
		Status:           etcd.TaskStatusPending, NextEventSequence: 1, CreatedAt: now,
	}
	revision := environmentBlueprintRevision(environmentID, taskID, now, bundle)
	projection := environmentComposeProjection(
		environmentID,
		taskID,
		generation,
		renderIdentities,
		reconciledRoutes.Current,
		pinnedComponents,
		pinnedEntries,
	)
	responseBody, err := json.Marshal(apiTypes.TaskAccepted{TaskID: taskID})
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: append([]byte(nil), responseBody...),
	}
	marker := etcd.IdempotencyMarker{
		Kind: etcd.IdempotencyMarkerTask, State: etcd.IdempotencyMarkerPending,
		Locator: locator, Intent: evidence.durable, Response: response,
		TaskID: taskID, CreatedAt: now, UpdatedAt: now,
	}
	result, applyErr := service.repository.ApplyEnvironmentBlueprintWithTask(
		ctx,
		project,
		environment,
		expectedHeadRevision,
		revision,
		projection,
		zoneChanges,
		serviceChanges,
		routeChanges,
		componentPreparation,
		task,
		marker,
	)
	if applyErr != nil {
		if !isUnknownEnvironmentBlueprintOutcome(applyErr) {
			return etcd.IdempotencyResponse{}, applyErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, applyErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case idempotentintent.ResolutionApplied:
		return cloneIdempotencyResponse(response), nil
	case idempotentintent.ResolutionReplay:
		return cloneIdempotencyResponse(resolution.Response), nil
	default:
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Environment Blueprint resolution is invalid")
	}
}

func (service *environmentBlueprintService) listBlueprintZones(
	ctx context.Context,
	environmentID string,
) ([]etcd.Versioned[etcd.ZoneRecord], error) {
	zones := []etcd.Versioned[etcd.ZoneRecord](nil)
	cursor := ""
	for {
		page, err := service.repository.ListZones(
			ctx,
			environmentID,
			etcd.PageRequest{Limit: 200, Cursor: cursor},
		)
		if err != nil {
			return nil, err
		}
		zones = append(zones, page.Items...)
		if page.NextCursor == "" {
			return zones, nil
		}
		cursor = page.NextCursor
	}
}

func prepareEnvironmentBlueprintZoneChanges(
	environmentID string,
	desired []core.Zone,
	current []etcd.Versioned[etcd.ZoneRecord],
) ([]etcd.EnvironmentBlueprintZoneChange, error) {
	currentByID := make(map[string]etcd.Versioned[etcd.ZoneRecord], len(current))
	for _, zone := range current {
		if zone.Record.EnvironmentID != environmentID || zone.Record.Desired.ID == "" {
			return nil, errs.New(errs.KindInternal, "durable Blueprint Zone state is inconsistent")
		}
		if _, duplicate := currentByID[zone.Record.Desired.ID]; duplicate {
			return nil, errs.New(errs.KindInternal, "durable Blueprint Zone state repeats an id")
		}
		currentByID[zone.Record.Desired.ID] = zone
	}
	changes := make([]etcd.EnvironmentBlueprintZoneChange, 0, len(desired))
	for _, next := range desired {
		if existing, found := currentByID[next.ID]; found {
			if existing.Record.Desired != next {
				return nil, errs.New(
					errs.KindValidationFailed,
					"Blueprint changed an immutable Zone; add a new Zone and move Services explicitly",
				)
			}
			currentCopy := existing
			changes = append(changes, etcd.EnvironmentBlueprintZoneChange{
				Current: &currentCopy,
				Record:  existing.Record,
			})
			delete(currentByID, next.ID)
			continue
		}
		record, err := etcd.NewZoneRecord(environmentID, next)
		if err != nil {
			return nil, err
		}
		changes = append(changes, etcd.EnvironmentBlueprintZoneChange{Record: record})
	}
	if len(currentByID) != 0 {
		return nil, errs.New(
			errs.KindResourceInUse,
			"Blueprint omits an existing Zone; remove it explicitly after moving Services",
		)
	}
	return changes, nil
}

func (service *environmentBlueprintService) listBlueprintServices(
	ctx context.Context,
	environmentID string,
) ([]etcd.Versioned[etcd.ServiceRecord], error) {
	services := []etcd.Versioned[etcd.ServiceRecord](nil)
	cursor := ""
	for {
		page, err := service.repository.ListServices(
			ctx,
			environmentID,
			etcd.PageRequest{Limit: 200, Cursor: cursor},
		)
		if err != nil {
			return nil, err
		}
		services = append(services, page.Items...)
		if page.NextCursor == "" {
			return services, nil
		}
		cursor = page.NextCursor
	}
}

func (service *environmentBlueprintService) listBlueprintRoutes(
	ctx context.Context,
	environmentID string,
) ([]etcd.Versioned[etcd.RouteRecord], error) {
	routes := []etcd.Versioned[etcd.RouteRecord](nil)
	cursor := ""
	for {
		page, err := service.repository.ListRoutes(
			ctx,
			environmentID,
			etcd.PageRequest{Limit: 200, Cursor: cursor},
		)
		if err != nil {
			return nil, err
		}
		routes = append(routes, page.Items...)
		if page.NextCursor == "" {
			return routes, nil
		}
		cursor = page.NextCursor
	}
}

func (service *environmentBlueprintService) listBlueprintComponents(
	ctx context.Context,
	environmentID string,
) ([]etcd.Versioned[etcd.ComponentRecord], error) {
	componentRecords := []etcd.Versioned[etcd.ComponentRecord](nil)
	cursor := ""
	for {
		page, err := service.repository.ListEnvironmentComponents(
			ctx,
			environmentID,
			etcd.PageRequest{Limit: 200, Cursor: cursor},
		)
		if err != nil {
			return nil, err
		}
		componentRecords = append(componentRecords, page.Items...)
		if page.NextCursor == "" {
			return componentRecords, nil
		}
		cursor = page.NextCursor
	}
}

func (service *environmentBlueprintService) listBlueprintEntries(
	ctx context.Context,
	environmentID string,
) ([]etcd.Versioned[etcd.EntryRecord], error) {
	entries := []etcd.Versioned[etcd.EntryRecord](nil)
	cursor := ""
	for {
		page, err := service.repository.ListEntries(
			ctx,
			environmentID,
			etcd.PageRequest{Limit: 200, Cursor: cursor},
		)
		if err != nil {
			return nil, err
		}
		entries = append(entries, page.Items...)
		if page.NextCursor == "" {
			return entries, nil
		}
		cursor = page.NextCursor
	}
}

func (service *environmentBlueprintService) prepareBlueprintComponents(
	ctx context.Context,
	environmentID string,
	taskID string,
	createdAt time.Time,
	specs map[string]core.ComponentSpec,
	current []etcd.Versioned[etcd.ComponentRecord],
	zoneChanges []etcd.EnvironmentBlueprintZoneChange,
) (etcd.ComponentTaskPreparation, []etcd.ComponentRecord, []core.Component, error) {
	currentComponents := make([]core.Component, len(current))
	currentByID := make(map[string]etcd.Versioned[etcd.ComponentRecord], len(current))
	for index, versioned := range current {
		component, err := etcd.ProjectComponentRecord(versioned.Record)
		if err != nil {
			return etcd.ComponentTaskPreparation{}, nil, nil, err
		}
		if _, duplicate := currentByID[component.ID]; duplicate {
			return etcd.ComponentTaskPreparation{}, nil, nil, errs.New(
				errs.KindInternal,
				"Environment Component singleton set repeats an id",
			)
		}
		currentComponents[index] = component
		currentByID[component.ID] = versioned
	}
	changes, err := controller.ReconcileBlueprintComponents(specs, currentComponents, ids.New)
	if err != nil {
		return etcd.ComponentTaskPreparation{}, nil, nil, err
	}
	inputs := make([]etcd.EnvironmentComponentCandidateInput, len(changes.Candidates))
	for index, candidate := range changes.Candidates {
		versioned, exists := currentByID[candidate.Current.ID]
		if !exists {
			return etcd.ComponentTaskPreparation{}, nil, nil, errs.New(
				errs.KindInternal,
				"Blueprint Component candidate lost its active record",
			)
		}
		inputs[index] = etcd.EnvironmentComponentCandidateInput{
			Current: versioned, Candidate: candidate.Candidate,
		}
	}
	preparation := etcd.ComponentTaskPreparation{}
	if len(inputs) != 0 {
		preparation, err = service.repository.PrepareEnvironmentComponentTask(
			ctx,
			taskID,
			environmentID,
			zoneChanges,
			inputs,
			createdAt,
		)
		if err != nil {
			return etcd.ComponentTaskPreparation{}, nil, nil, err
		}
	}
	recordsByID := make(map[string]etcd.ComponentRecord, len(changes.Effective))
	for _, component := range changes.Effective {
		record, recordErr := etcd.NewComponentRecord(component)
		if recordErr != nil {
			return etcd.ComponentTaskPreparation{}, nil, nil, recordErr
		}
		recordsByID[component.ID] = record
	}
	if len(preparation.Intent.Candidates) != len(changes.Candidates) {
		return etcd.ComponentTaskPreparation{}, nil, nil, errs.New(
			errs.KindInternal,
			"prepared Blueprint Component candidate count changed",
		)
	}
	for _, candidate := range preparation.Intent.Candidates {
		if _, exists := recordsByID[candidate.Candidate.Desired.ID]; !exists {
			return etcd.ComponentTaskPreparation{}, nil, nil, errs.New(
				errs.KindInternal,
				"prepared Blueprint Component candidate is unknown",
			)
		}
		recordsByID[candidate.Candidate.Desired.ID] = candidate.Candidate
	}
	records := make([]etcd.ComponentRecord, 0, len(recordsByID))
	for _, record := range recordsByID {
		records = append(records, record)
	}
	sort.Slice(records, func(left int, right int) bool {
		return records[left].Desired.Kind < records[right].Desired.Kind
	})
	effective := make([]core.Component, len(records))
	for index, record := range records {
		effective[index], err = etcd.ProjectComponentRecord(record)
		if err != nil {
			return etcd.ComponentTaskPreparation{}, nil, nil, err
		}
	}
	return preparation, records, effective, nil
}

func environmentEntryProjection(
	current []etcd.Versioned[etcd.EntryRecord],
) ([]etcd.EntryRecord, error) {
	records := make([]etcd.EntryRecord, len(current))
	for index, versioned := range current {
		var err error
		records[index], err = etcd.NewEntryRecord(
			versioned.Record.EnvironmentID,
			versioned.Record.Entry,
			versioned.Record.CurrentValueGenerationID,
		)
		if err != nil {
			return nil, err
		}
	}
	sort.Slice(records, func(left int, right int) bool {
		return records[left].Entry.ID < records[right].Entry.ID
	})
	return records, nil
}

func blueprintComponentEnvironment(
	environment etcd.EnvironmentRecord,
	zones []core.Zone,
	services []core.Service,
	routes []core.Route,
	components []core.Component,
	entries []etcd.EntryRecord,
) core.Environment {
	projected := core.Environment{
		ID: environment.ID, ProjectID: environment.ProjectID, Name: environment.Name,
		NetworkPool: environment.NetworkPool, VolumeDir: environment.VolumeDir,
		Zones: make(map[string]core.Zone, len(zones)), Services: make(map[string]core.Service, len(services)),
		Routes: append([]core.Route(nil), routes...), Components: append([]core.Component(nil), components...),
		Entries: projectedEnvironmentEntries(entries),
	}
	for _, zone := range zones {
		projected.Zones[zone.Name] = zone
	}
	for _, service := range services {
		projected.Services[service.Name] = service
	}
	return projected
}

func projectedEnvironmentEntries(records []etcd.EntryRecord) []core.EnvEntry {
	entries := make([]core.EnvEntry, len(records))
	for index, record := range records {
		entries[index] = record.Entry
	}
	return entries
}

func environmentComponentComposeIdentities(
	authored controller.ComposeIdentitySnapshot,
	generated []controller.ComposeResourceIdentity,
) controller.ComposeIdentitySnapshot {
	result := controller.ComposeIdentitySnapshot{
		Services: append([]controller.ComposeResourceIdentity(nil), authored.Services...),
		Networks: append([]controller.ComposeResourceIdentity(nil), authored.Networks...),
		Volumes:  append([]controller.ComposeResourceIdentity(nil), authored.Volumes...),
	}
	result.Services = append(result.Services, generated...)
	sort.Slice(result.Services, func(left int, right int) bool {
		return result.Services[left].Name < result.Services[right].Name
	})
	return result
}

type environmentComponentMaterializationInput struct {
	destination string
	serviceID   string
	serviceName string
	outputKind  etcd.TaskMaterializationOutputKind
	mode        entrymaterialization.Mode
	source      etcd.TaskMaterializationSource
	content     []byte
	resolve     bool
}

func (service *environmentBlueprintService) environmentComponentMaterializations(
	ctx context.Context,
	environmentID string,
	revisionID string,
	artifactID string,
	projection controller.EnvironmentComponentComposeProjection,
	entries []etcd.EntryRecord,
) ([]etcd.TaskMaterializationRecord, []*agentpb.ExecutionStep, error) {
	inputs := make([]environmentComponentMaterializationInput, 0,
		len(projection.PlainFiles)+len(projection.EnvironmentFiles))
	for _, file := range projection.PlainFiles {
		inputs = append(inputs, environmentComponentMaterializationInput{
			destination: file.Path, outputKind: etcd.TaskMaterializationOutputPlainFile,
			mode: entrymaterialization.ModeReadOnly,
			source: etcd.TaskMaterializationSource{
				Kind: etcd.TaskMaterializationSourceComponentFile,
				ComponentFile: &etcd.TaskComponentFileValueReference{
					RevisionID: revisionID, ComponentID: file.ComponentID, Path: file.Path,
				},
			},
			content: append([]byte(nil), file.Content...),
		})
	}
	entriesByID := make(map[string]etcd.EntryRecord, len(entries))
	for _, record := range entries {
		entriesByID[record.Entry.ID] = record
	}
	for _, file := range projection.EnvironmentFiles {
		values := make([]etcd.TaskGeneratedEnvironmentEntryReference, len(file.Values))
		for index, binding := range file.Values {
			record, exists := entriesByID[binding.EntryID]
			if !exists || record.EnvironmentID != environmentID || !record.Entry.Secret {
				return nil, nil, errs.New(
					errs.KindValidationFailed,
					"Component generated Environment references an unavailable secret Entry",
				)
			}
			values[index] = etcd.TaskGeneratedEnvironmentEntryReference{
				Name: binding.Name,
				Value: etcd.TaskEntryValueReference{
					EntryID: record.Entry.ID, ValueGenerationID: record.CurrentValueGenerationID,
					Storage: etcd.TaskEntryValueStorageSecret,
				},
			}
		}
		sort.Slice(values, func(left int, right int) bool { return values[left].Name < values[right].Name })
		inputs = append(inputs, environmentComponentMaterializationInput{
			destination: file.Destination, serviceID: file.ServiceID, serviceName: file.ServiceName,
			outputKind: etcd.TaskMaterializationOutputGeneratedEnvironment,
			mode:       entrymaterialization.ModePrivate, resolve: true,
			source: etcd.TaskMaterializationSource{
				Kind: etcd.TaskMaterializationSourceGeneratedEnvironment,
				GeneratedEnvironment: &etcd.TaskGeneratedEnvironmentValueReference{
					FormatVersion: 1, Values: values,
				},
			},
		})
	}
	sort.Slice(inputs, func(left int, right int) bool { return inputs[left].destination < inputs[right].destination })
	references := make([]etcd.TaskMaterializationRecord, 0, len(inputs))
	steps := make([]*agentpb.ExecutionStep, 0, len(inputs))
	previousDestination := ""
	for _, input := range inputs {
		if input.destination == previousDestination {
			return nil, nil, errs.New(errs.KindNameConflict, "Component materialization destination is duplicated")
		}
		content := input.content
		var err error
		if input.resolve {
			content, err = service.materials.ResolveTaskMaterializationSource(ctx, environmentID, input.source)
			if err != nil {
				clear(content)
				return nil, nil, err
			}
		}
		digest := sha256.Sum256(content)
		reference := etcd.TaskMaterializationRecord{
			StepID: ids.New(ids.KindStep), MaterializationID: ids.New(ids.KindConfig),
			EnvironmentID: environmentID, Destination: input.destination,
			ServiceID: input.serviceID, ServiceName: input.serviceName,
			OutputKind: input.outputKind, Mode: uint32(input.mode),
			Length: uint64(len(content)), SHA256: hex.EncodeToString(digest[:]), Source: input.source,
		}
		clear(content)
		step, err := controller.BuildTaskMaterializationStep(
			reference,
			artifactID,
			uint32(environmentBlueprintTimeoutSeconds),
		)
		if err != nil {
			return nil, nil, err
		}
		references = append(references, reference)
		steps = append(steps, step)
		previousDestination = input.destination
	}
	sort.Slice(references, func(left int, right int) bool {
		return references[left].StepID < references[right].StepID
	})
	return references, steps, nil
}

func prepareEnvironmentBlueprintServiceChanges(
	environmentID string,
	desired []core.Service,
	current []etcd.Versioned[etcd.ServiceRecord],
) ([]etcd.EnvironmentBlueprintServiceChange, error) {
	currentByID := make(map[string]etcd.Versioned[etcd.ServiceRecord], len(current))
	for _, service := range current {
		if service.Record.EnvironmentID != environmentID || service.Record.Desired.ID == "" {
			return nil, errs.New(errs.KindInternal, "durable Blueprint Service state is inconsistent")
		}
		if _, duplicate := currentByID[service.Record.Desired.ID]; duplicate {
			return nil, errs.New(errs.KindInternal, "durable Blueprint Service state repeats an id")
		}
		currentByID[service.Record.Desired.ID] = service
	}
	changes := make([]etcd.EnvironmentBlueprintServiceChange, 0, len(desired))
	for _, next := range desired {
		if existing, found := currentByID[next.ID]; found {
			replacement, err := etcd.ReplaceServiceDesired(existing.Record, next)
			if err != nil {
				return nil, err
			}
			currentCopy := existing
			changes = append(changes, etcd.EnvironmentBlueprintServiceChange{
				Current: &currentCopy,
				Record:  replacement,
			})
			delete(currentByID, next.ID)
			continue
		}
		record, err := etcd.NewServiceRecord(environmentID, next, "")
		if err != nil {
			return nil, err
		}
		changes = append(changes, etcd.EnvironmentBlueprintServiceChange{Record: record})
	}
	if len(currentByID) != 0 {
		return nil, errs.New(
			errs.KindResourceInUse,
			"Blueprint omits an existing Service; remove it explicitly before apply",
		)
	}
	return changes, nil
}

func prepareEnvironmentBlueprintRouteChanges(
	environmentID string,
	desired []core.Route,
	current []etcd.Versioned[etcd.RouteRecord],
) ([]etcd.EnvironmentBlueprintRouteChange, error) {
	currentByID := make(map[string]etcd.Versioned[etcd.RouteRecord], len(current))
	for _, route := range current {
		if route.Record.EnvironmentID != environmentID || route.Record.Desired.ID == "" {
			return nil, errs.New(errs.KindInternal, "durable Blueprint Route state is inconsistent")
		}
		if _, duplicate := currentByID[route.Record.Desired.ID]; duplicate {
			return nil, errs.New(errs.KindInternal, "durable Blueprint Route state repeats an id")
		}
		currentByID[route.Record.Desired.ID] = route
	}
	changes := make([]etcd.EnvironmentBlueprintRouteChange, 0, len(desired))
	for _, next := range desired {
		if existing, found := currentByID[next.ID]; found {
			replacement, err := etcd.ReplaceRouteDesired(existing.Record, next)
			if err != nil {
				return nil, err
			}
			currentCopy := existing
			changes = append(changes, etcd.EnvironmentBlueprintRouteChange{
				Current: &currentCopy,
				Record:  replacement,
			})
			delete(currentByID, next.ID)
			continue
		}
		record, err := etcd.NewRouteRecord(environmentID, next)
		if err != nil {
			return nil, err
		}
		changes = append(changes, etcd.EnvironmentBlueprintRouteChange{Record: record})
	}
	if len(currentByID) != 0 {
		return nil, errs.New(
			errs.KindResourceInUse,
			"Blueprint omits an existing Route; remove it explicitly before apply",
		)
	}
	return changes, nil
}

func environmentBlueprintIntentManifest(bundle core.BlueprintBundle) (idempotentintent.BlueprintManifestV1, error) {
	if err := bundle.Validate(); err != nil {
		return idempotentintent.BlueprintManifestV1{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint bundle is invalid",
		)
	}
	keys := make([]string, 0, len(bundle.Interpolation))
	for key := range bundle.Interpolation {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	manifest := idempotentintent.BlueprintManifestV1{
		FormatVersion: 1, RootPath: bundle.RootPath,
		ComposeSources: append([]string(nil), bundle.ComposeSources...),
		Interpolation:  make([]idempotentintent.Interpolation, len(keys)),
		Files:          make([]idempotentintent.BlueprintFile, len(bundle.Files)),
	}
	for index, key := range keys {
		manifest.Interpolation[index] = idempotentintent.Interpolation{Name: key, Value: bundle.Interpolation[key]}
	}
	for index, file := range bundle.Files {
		manifest.Files[index] = idempotentintent.BlueprintFile{
			Path: file.Path, Part: "file-" + leftPadBlueprintPart(index+1),
			Size: uint64(len(file.Content)), SHA256: sha256.Sum256(file.Content),
		}
	}
	return manifest, nil
}

func leftPadBlueprintPart(value int) string {
	digits := []byte{'0', '0', '0', '0', '0', '0'}
	for index := len(digits) - 1; index >= 0 && value > 0; index-- {
		digits[index] = byte('0' + value%10)
		value /= 10
	}
	return string(digits)
}

func environmentBlueprintState(
	environmentID string,
	head etcd.Versioned[etcd.EnvironmentBlueprintHead],
	hasHead bool,
	projection etcd.Versioned[etcd.EnvironmentComposeProjection],
	hasProjection bool,
) (int64, controller.ComposeIdentitySnapshot, uint64, error) {
	if hasHead != hasProjection {
		return 0, controller.ComposeIdentitySnapshot{}, 0, errs.New(
			errs.KindInternal,
			"Environment desired-state pointers are inconsistent",
		)
	}
	if !hasHead {
		return 0, controller.ComposeIdentitySnapshot{}, 1, nil
	}
	if head.Record.EnvironmentID != environmentID || projection.Record.EnvironmentID != environmentID ||
		head.Record.RevisionID != projection.Record.BlueprintRevisionID || head.Revision <= 0 ||
		projection.Revision != head.Revision || projection.Record.RenderGeneration == math.MaxUint64 {
		return 0, controller.ComposeIdentitySnapshot{}, 0, errs.New(
			errs.KindInternal,
			"Environment desired-state pointers are corrupt",
		)
	}
	return head.Revision, authoredComposeIdentitySnapshot(projection.Record),
		projection.Record.RenderGeneration + 1, nil
}

func authoredComposeIdentitySnapshot(
	projection etcd.EnvironmentComposeProjection,
) controller.ComposeIdentitySnapshot {
	snapshot := composeIdentitySnapshot(projection)
	generatedServiceIDs := make(map[string]struct{})
	for _, component := range projection.Components {
		for _, serviceID := range component.Runtime.GeneratedServices {
			generatedServiceIDs[serviceID] = struct{}{}
		}
	}
	services := make([]controller.ComposeResourceIdentity, 0, len(snapshot.Services))
	for _, service := range snapshot.Services {
		if _, generated := generatedServiceIDs[service.ID]; !generated {
			services = append(services, service)
		}
	}
	snapshot.Services = services
	return snapshot
}

func validateBasicEnvironmentBlueprint(parsed blueprintparser.Result) error {
	extensions := parsed.Extensions
	if parsed.Project == nil || len(extensions.Requires) != 0 || len(extensions.Attachments) != 0 ||
		len(extensions.Entries) != 0 || extensions.Backup != nil ||
		len(extensions.ReleaseGroups) != 0 || len(parsed.Project.Configs) != 0 ||
		len(parsed.Project.Secrets) != 0 {
		return errs.New(errs.KindValidationFailed, "Blueprint uses a desired-state contract that is not available yet")
	}
	for _, network := range parsed.Project.Networks {
		if network.External || len(network.Extensions) != 0 {
			return errs.New(errs.KindValidationFailed, "Blueprint external network ownership is not available yet")
		}
	}
	for _, volume := range parsed.Project.Volumes {
		if volume.External || (volume.Driver != "" && volume.Driver != "local") || len(volume.DriverOpts) != 0 ||
			len(volume.Extensions) != 0 {
			return errs.New(errs.KindValidationFailed, "Blueprint volume runtime is not managed by Groundplane")
		}
	}
	return nil
}

func composeIdentitySnapshot(projection etcd.EnvironmentComposeProjection) controller.ComposeIdentitySnapshot {
	convert := func(values []etcd.EnvironmentComposeIdentity) []controller.ComposeResourceIdentity {
		result := make([]controller.ComposeResourceIdentity, len(values))
		for index, value := range values {
			result[index] = controller.ComposeResourceIdentity{ID: value.ID, Name: value.Name}
		}
		return result
	}
	return controller.ComposeIdentitySnapshot{
		Services: convert(
			projection.Services,
		),
		Networks: convert(projection.Networks),
		Volumes:  convert(projection.Volumes),
	}
}

func environmentComposeProjection(
	environmentID string,
	revisionID string,
	generation uint64,
	snapshot controller.ComposeIdentitySnapshot,
	routes []core.Route,
	components []etcd.ComponentRecord,
	entries []etcd.EntryRecord,
) etcd.EnvironmentComposeProjection {
	convert := func(values []controller.ComposeResourceIdentity) []etcd.EnvironmentComposeIdentity {
		result := make([]etcd.EnvironmentComposeIdentity, len(values))
		for index, value := range values {
			result[index] = etcd.EnvironmentComposeIdentity{ID: value.ID, Name: value.Name}
		}
		return result
	}
	routeIdentities := make([]etcd.EnvironmentRouteIdentity, len(routes))
	for index, route := range routes {
		routeIdentities[index] = etcd.EnvironmentRouteIdentity{ID: route.ID, Host: route.Host, Path: route.Path}
	}
	sort.Slice(routeIdentities, func(left int, right int) bool {
		leftMatch := routeIdentities[left].Host + "\x00" + routeIdentities[left].Path
		rightMatch := routeIdentities[right].Host + "\x00" + routeIdentities[right].Path
		return leftMatch < rightMatch
	})
	return etcd.EnvironmentComposeProjection{
		EnvironmentID: environmentID, BlueprintRevisionID: revisionID, RenderGeneration: generation,
		Services: convert(snapshot.Services), Networks: convert(snapshot.Networks), Volumes: convert(snapshot.Volumes),
		Routes: routeIdentities, Components: components, Entries: entries,
	}
}

func environmentBlueprintRevision(
	environmentID string,
	revisionID string,
	createdAt time.Time,
	bundle core.BlueprintBundle,
) etcd.EnvironmentBlueprintRevision {
	files := make([]etcd.EnvironmentBlueprintFile, len(bundle.Files))
	for index, file := range bundle.Files {
		files[index] = etcd.EnvironmentBlueprintFile{Path: file.Path, Content: file.Content}
	}
	return etcd.EnvironmentBlueprintRevision{
		EnvironmentID: environmentID, RevisionID: revisionID, RootPath: bundle.RootPath,
		ComposeSources: append([]string(nil), bundle.ComposeSources...),
		Interpolation:  cloneBlueprintInterpolation(bundle.Interpolation),
		Files:          files, CreatedAt: createdAt,
	}
}

func cloneBlueprintInterpolation(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func isUnknownEnvironmentBlueprintOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
