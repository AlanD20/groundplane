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

	"github.com/AlanD20/groundplane/internal/common/ids"
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
	ListServices(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.ServiceRecord], error)
	ListRoutes(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.RouteRecord], error)
	ApplyEnvironmentBlueprintWithTask(
		context.Context,
		etcd.Versioned[etcd.ProjectRecord],
		etcd.Versioned[etcd.EnvironmentRecord],
		int64,
		etcd.EnvironmentBlueprintRevision,
		etcd.EnvironmentComposeProjection,
		[]etcd.EnvironmentBlueprintServiceChange,
		[]etcd.EnvironmentBlueprintRouteChange,
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
	now         func() time.Time
}

type durableEnvironmentBlueprintRepository struct {
	*etcd.HierarchyRepository
	services *etcd.ServiceRepository
	routes   *etcd.RouteRepository
}

func newDurableEnvironmentBlueprintRepository(
	hierarchy *etcd.HierarchyRepository,
	services *etcd.ServiceRepository,
	routes *etcd.RouteRepository,
) (*durableEnvironmentBlueprintRepository, error) {
	if hierarchy == nil || services == nil || routes == nil {
		return nil, errs.New(errs.KindInternal, "Environment Blueprint repositories are not configured")
	}
	return &durableEnvironmentBlueprintRepository{
		HierarchyRepository: hierarchy,
		services:            services,
		routes:              routes,
	}, nil
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
) (*environmentBlueprintService, error) {
	if repository == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Environment Blueprint service is not configured")
	}
	if _, err := controller.NewTaskPlanResolver(volumeRoot); err != nil {
		return nil, err
	}
	return &environmentBlueprintService{
		volumeRoot: volumeRoot, repository: repository, idempotency: idempotency, now: time.Now,
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

	now := service.now().UTC()
	taskID := ids.New(ids.KindTask)
	planID := ids.New(ids.KindPlan)
	artifactID := ids.New(ids.KindConfig)
	artifact, err := controller.RenderCompose(controller.ComposeRenderInput{
		Project: parsed.Project, ArtifactID: artifactID,
		TenantID: tenant.Record.ID, ProjectID: project.Record.ID, EnvironmentID: environmentID,
		PlanID: planID, RenderGeneration: generation, AuthorizedVolumeDir: environment.Record.VolumeDir,
		Identities: changes.Current,
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	steps := make([]*agentpb.ExecutionStep, 0, 2)
	stepRecords := make([]etcd.TaskStepRecord, 0, 2)
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
		Status: etcd.TaskStatusPending, NextEventSequence: 1, CreatedAt: now,
	}
	revision := environmentBlueprintRevision(environmentID, taskID, now, bundle)
	projection := environmentComposeProjection(environmentID, taskID, generation, changes.Current)
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
		serviceChanges,
		routeChanges,
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
	return head.Revision, composeIdentitySnapshot(projection.Record), projection.Record.RenderGeneration + 1, nil
}

func validateBasicEnvironmentBlueprint(parsed blueprintparser.Result) error {
	extensions := parsed.Extensions
	if parsed.Project == nil || len(extensions.Requires) != 0 || len(extensions.Attachments) != 0 ||
		len(extensions.Entries) != 0 || len(extensions.Components) != 0 ||
		extensions.Backup != nil || len(extensions.ReleaseGroups) != 0 || len(parsed.Project.Configs) != 0 ||
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
) etcd.EnvironmentComposeProjection {
	convert := func(values []controller.ComposeResourceIdentity) []etcd.EnvironmentComposeIdentity {
		result := make([]etcd.EnvironmentComposeIdentity, len(values))
		for index, value := range values {
			result[index] = etcd.EnvironmentComposeIdentity{ID: value.ID, Name: value.Name}
		}
		return result
	}
	return etcd.EnvironmentComposeProjection{
		EnvironmentID: environmentID, BlueprintRevisionID: revisionID, RenderGeneration: generation,
		Services: convert(snapshot.Services), Networks: convert(snapshot.Networks), Volumes: convert(snapshot.Volumes),
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
