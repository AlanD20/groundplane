package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	entryDeletionRoute                   = "/entries/{id}"
	entryRemovalAgentTimeoutSeconds      = int64(120)
	entryRemovalControllerTimeoutSeconds = int64(30)
	maximumEntryDeletionAttempts         = 3
)

type entryDeletionRepository interface {
	GetEntry(context.Context, string) (etcd.Versioned[etcd.EntryRecord], error)
	GetEnvironment(context.Context, string) (etcd.Versioned[etcd.EnvironmentRecord], error)
	GetProject(context.Context, string) (etcd.Versioned[etcd.ProjectRecord], error)
	GetEnvironmentComposeProjection(
		context.Context,
		string,
	) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error)
	ListEnvironmentComponents(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.ComponentRecord], error)
	BeginEntryDeletionWithTask(
		context.Context,
		etcd.Versioned[etcd.EnvironmentRecord],
		etcd.Versioned[etcd.ProjectRecord],
		etcd.Versioned[etcd.EntryRecord],
		*etcd.Versioned[etcd.EnvironmentComposeProjection],
		*etcd.Versioned[etcd.ComponentRecord],
		etcd.DeletionTombstoneRecord,
		etcd.EntryRemovalIntent,
		etcd.TaskRecord,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type durableEntryDeletionRepository struct {
	entries    *durableEntryCreationRepository
	components *etcd.ComponentRepository
}

func newDurableEntryDeletionRepository(
	entries *durableEntryCreationRepository,
	components *etcd.ComponentRepository,
) (*durableEntryDeletionRepository, error) {
	if entries == nil || components == nil {
		return nil, errs.New(errs.KindInternal, "Entry deletion repositories are not configured")
	}
	return &durableEntryDeletionRepository{entries: entries, components: components}, nil
}

func (repository *durableEntryDeletionRepository) GetEntry(
	ctx context.Context,
	entryID string,
) (etcd.Versioned[etcd.EntryRecord], error) {
	return repository.entries.entries.GetEntry(ctx, entryID)
}

func (repository *durableEntryDeletionRepository) GetEnvironment(
	ctx context.Context,
	environmentID string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return repository.entries.GetEnvironment(ctx, environmentID)
}

func (repository *durableEntryDeletionRepository) GetProject(
	ctx context.Context,
	projectID string,
) (etcd.Versioned[etcd.ProjectRecord], error) {
	return repository.entries.GetProject(ctx, projectID)
}

func (repository *durableEntryDeletionRepository) GetEnvironmentComposeProjection(
	ctx context.Context,
	environmentID string,
) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
	return repository.entries.hierarchy.GetEnvironmentComposeProjection(ctx, environmentID)
}

func (repository *durableEntryDeletionRepository) ListEnvironmentComponents(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.ComponentRecord], error) {
	return repository.components.ListEnvironmentComponents(ctx, environmentID, request)
}

func (repository *durableEntryDeletionRepository) BeginEntryDeletionWithTask(
	ctx context.Context,
	environment etcd.Versioned[etcd.EnvironmentRecord],
	project etcd.Versioned[etcd.ProjectRecord],
	entry etcd.Versioned[etcd.EntryRecord],
	projection *etcd.Versioned[etcd.EnvironmentComposeProjection],
	cloudflare *etcd.Versioned[etcd.ComponentRecord],
	tombstone etcd.DeletionTombstoneRecord,
	intent etcd.EntryRemovalIntent,
	task etcd.TaskRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.entries.entries.BeginEntryDeletionWithTask(
		ctx, environment, project, entry, projection, cloudflare, tombstone, intent, task, marker,
	)
}

type entryDeletionEvidence struct {
	candidate idempotentintent.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type entryDeletionIdempotency interface {
	ResolveReplayLocator(
		context.Context,
		etcd.IdempotencyReplayTarget,
		string,
		string,
		string,
	) (etcd.IdempotencyLocator, bool, error)
	Prepare(context.Context, etcd.IdempotencyLocator, string) (entryDeletionEvidence, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		entryDeletionEvidence,
	) (idempotentintent.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		entryDeletionEvidence,
		etcd.IdempotencyTransactionResult,
	) (idempotentintent.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		entryDeletionEvidence,
		error,
	) (idempotentintent.Resolution, error)
}

type durableEntryDeletionIdempotency struct {
	coordinator *idempotentintent.Coordinator
	repository  *etcd.IdempotencyRepository
}

func newDurableEntryDeletionIdempotency(
	coordinator *idempotentintent.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableEntryDeletionIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Entry deletion idempotency is not configured")
	}
	return &durableEntryDeletionIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableEntryDeletionIdempotency) ResolveReplayLocator(
	ctx context.Context,
	target etcd.IdempotencyReplayTarget,
	method string,
	route string,
	key string,
) (etcd.IdempotencyLocator, bool, error) {
	return service.repository.ResolveReplayLocator(ctx, target, method, route, key)
}

func (service *durableEntryDeletionIdempotency) Prepare(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	entryID string,
) (entryDeletionEvidence, error) {
	if locator.ScopeKind != etcd.IdempotencyScopeEnvironment ||
		ids.Validate(ids.KindEnvironment, locator.ScopeID) != nil {
		return entryDeletionEvidence{}, errs.New(errs.KindInternal, "Entry deletion replay scope is invalid")
	}
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: http.MethodDelete,
		Route:  entryDeletionRoute,
		Scope: idempotentintent.Scope{
			Kind: idempotentintent.ScopeEnvironment,
			ID:   locator.ScopeID,
		},
		Path:  []idempotentintent.PathBinding{{Name: "id", Value: entryID}},
		Query: idempotentintent.Object(),
		Body:  idempotentintent.NoBody(),
	})
	if err != nil {
		return entryDeletionEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return entryDeletionEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return entryDeletionEvidence{}, err
	}
	return entryDeletionEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableEntryDeletionIdempotency) ResolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence entryDeletionEvidence,
) (idempotentintent.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableEntryDeletionIdempotency) ResolveKnown(
	ctx context.Context,
	evidence entryDeletionEvidence,
	result etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableEntryDeletionIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence entryDeletionEvidence,
	original error,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type entryDeletionPlanResolver interface {
	PrepareEntryRemovalTask(
		context.Context,
		etcd.TaskRecord,
		etcd.EntryRemovalIntent,
		controller.EntryRemovalTaskProcedureIDs,
		controller.EntryRemovalMaterializationResolver,
	) (etcd.TaskRecord, error)
}

type entryDeletionService struct {
	repository  entryDeletionRepository
	plans       entryDeletionPlanResolver
	materials   controller.EntryRemovalMaterializationResolver
	idempotency entryDeletionIdempotency
	now         func() time.Time
}

func newEntryDeletionService(
	repository entryDeletionRepository,
	plans entryDeletionPlanResolver,
	materials controller.EntryRemovalMaterializationResolver,
	idempotency entryDeletionIdempotency,
) (*entryDeletionService, error) {
	if repository == nil || plans == nil || materials == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Entry deletion service is not configured")
	}
	return &entryDeletionService{
		repository: repository, plans: plans, materials: materials, idempotency: idempotency, now: time.Now,
	}, nil
}

func (service *entryDeletionService) RemoveEntry(
	ctx context.Context,
	entryID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Entry deletion context is required")
	}
	if ids.Validate(ids.KindEnvEntry, entryID) != nil {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Entry deletion requires a stable Entry id",
		)
	}
	for attempt := 0; attempt < maximumEntryDeletionAttempts; attempt++ {
		response, err := service.removeEntryOnce(ctx, entryID, idempotencyKey)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumEntryDeletionAttempts-1 {
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Entry deletion retry bound was not enforced")
}

func (service *entryDeletionService) removeEntryOnce(
	ctx context.Context,
	entryID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	target := etcd.IdempotencyReplayTarget{Kind: etcd.IdempotencyReplayTargetEntry, ID: entryID}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx, target, http.MethodDelete, entryDeletionRoute, idempotencyKey,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if indexed {
		evidence, prepareErr := service.idempotency.Prepare(ctx, locator, entryID)
		if prepareErr != nil {
			return etcd.IdempotencyResponse{}, prepareErr
		}
		defer clear(evidence.durable.Ciphertext)
		resolution, existing, resolveErr := service.idempotency.ResolveExisting(ctx, locator, evidence)
		if resolveErr != nil {
			return etcd.IdempotencyResponse{}, resolveErr
		}
		if !existing || resolution.Kind != idempotentintent.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Entry deletion replay target is inconsistent",
			)
		}
		return cloneIdempotencyResponse(resolution.Response), nil
	}

	current, err := service.repository.GetEntry(ctx, entryID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	locator = etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeEnvironment,
		ScopeID:   current.Record.EnvironmentID,
		Method:    http.MethodDelete,
		Route:     entryDeletionRoute,
		Key:       idempotencyKey,
	}
	evidence, err := service.idempotency.Prepare(ctx, locator, entryID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != idempotentintent.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Entry deletion replay resolution is invalid",
			)
		}
		return cloneIdempotencyResponse(resolution.Response), nil
	}

	environment, err := service.repository.GetEnvironment(ctx, current.Record.EnvironmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	taskOwner, err := etcd.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	cloudflare, err := service.cloudflareComponent(ctx, environment.Record.ID, entryID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	projection, hasProjection, err := service.repository.GetEnvironmentComposeProjection(ctx, environment.Record.ID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	var projectionInput *etcd.Versioned[etcd.EnvironmentComposeProjection]
	if hasProjection {
		projectionInput = &projection
	}

	now := service.now().UTC()
	task := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation), IdempotencyKey: idempotencyKey,
		Owner: taskOwner, Actor: etcd.TaskActorOperator,
		PlanID: ids.New(ids.KindPlan), Type: etcd.TaskRemove, Target: entryID,
		Status: etcd.TaskStatusPending, NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	intent, err := etcd.NewEntryRemovalIntent(
		task.ID, environment.Record.ID, entryID, current.Revision, projectionInput, now,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if intent.CurrentProjection != nil {
		task.Executor = etcd.TaskExecutorAgent
		task.TimeoutSeconds = entryRemovalAgentTimeoutSeconds
		task, err = service.plans.PrepareEntryRemovalTask(
			ctx,
			task,
			intent,
			controller.EntryRemovalTaskProcedureIDs{ArtifactID: ids.New(ids.KindConfig)},
			service.materials,
		)
	} else {
		task, err = prepareControllerEntryRemovalTask(task, intent)
		projectionInput = nil
	}
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}

	responseBody, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: append([]byte(nil), responseBody...),
	}
	marker := etcd.IdempotencyMarker{
		Kind: etcd.IdempotencyMarkerTask, State: etcd.IdempotencyMarkerPending,
		Locator: locator, ReplayTarget: &target, Intent: evidence.durable, Response: response,
		TaskID: task.ID, CreatedAt: now, UpdatedAt: now,
	}
	phase := etcd.DeletionPhaseFinalizing
	if intent.CurrentProjection != nil {
		phase = etcd.DeletionPhaseHostEffects
	}
	tombstone := etcd.DeletionTombstoneRecord{
		TargetKind: etcd.DeletionTargetEntry, TargetID: entryID, TargetRevision: current.Revision,
		TaskID: task.ID, Phase: phase, CreatedAt: now, UpdatedAt: now,
	}
	result, mutationErr := service.repository.BeginEntryDeletionWithTask(
		ctx, environment, project, current, projectionInput, cloudflare, tombstone, intent, task, marker,
	)
	if mutationErr != nil {
		if !isUnknownEntryCreationOutcome(mutationErr) {
			return etcd.IdempotencyResponse{}, mutationErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, mutationErr)
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
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Entry deletion resolution is invalid")
	}
}

func (service *entryDeletionService) cloudflareComponent(
	ctx context.Context,
	environmentID string,
	entryID string,
) (*etcd.Versioned[etcd.ComponentRecord], error) {
	cursor := ""
	var selected *etcd.Versioned[etcd.ComponentRecord]
	for {
		page, err := service.repository.ListEnvironmentComponents(
			ctx, environmentID, etcd.PageRequest{Limit: 200, Cursor: cursor},
		)
		if err != nil {
			return nil, err
		}
		for _, item := range page.Items {
			if item.Record.Desired.Kind != core.ComponentKindEdgeCloudflare {
				continue
			}
			if selected != nil {
				return nil, errs.New(errs.KindInternal, "Environment has duplicate Cloudflare Components")
			}
			copy := item
			selected = &copy
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if selected == nil || !selected.Record.Desired.Enabled {
		return selected, nil
	}
	component, err := etcd.ProjectComponentRecord(selected.Record)
	if err != nil {
		return nil, err
	}
	tokenEntryID, ok := component.Config["token_entry_id"].(string)
	if !ok || ids.Validate(ids.KindEnvEntry, tokenEntryID) != nil {
		return nil, errs.New(errs.KindInternal, "enabled Cloudflare Component token reference is invalid")
	}
	if tokenEntryID == entryID {
		return nil, errs.New(errs.KindResourceInUse, "Entry is the enabled Cloudflare Tunnel token")
	}
	return selected, nil
}

func prepareControllerEntryRemovalTask(
	task etcd.TaskRecord,
	intent etcd.EntryRemovalIntent,
) (etcd.TaskRecord, error) {
	renderGeneration := uint64(1)
	if intent.CandidateProjection != nil {
		renderGeneration = intent.CandidateProjection.RenderGeneration
	}
	if renderGeneration == 0 || renderGeneration > math.MaxInt32 {
		return etcd.TaskRecord{}, errs.New(
			errs.KindStateConflict,
			"Entry render generation exceeds Controller Task limits",
		)
	}
	task.Executor = etcd.TaskExecutorController
	task.RenderGeneration = int32(renderGeneration)
	task.Params = map[string]string{
		etcd.TaskResourceKindParam:     etcd.TaskResourceEntry,
		etcd.TaskEntryEnvironmentParam: intent.EnvironmentID,
	}
	task.Steps = []etcd.TaskStepRecord{{ID: ids.New(ids.KindStep)}}
	task.TimeoutSeconds = entryRemovalControllerTimeoutSeconds
	planHash, err := controllerEntryRemovalPlanHash(intent)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	task.PlanHash = planHash
	return task, nil
}

func controllerEntryRemovalPlanHash(intent etcd.EntryRemovalIntent) (string, error) {
	value, err := json.Marshal(struct {
		Version                   int    `json:"version"`
		Type                      string `json:"type"`
		EntryID                   string `json:"entry_id"`
		EnvironmentID             string `json:"environment_id"`
		EntryRevision             int64  `json:"entry_revision"`
		CurrentProjectionRevision int64  `json:"current_projection_revision"`
	}{
		Version: 1, Type: string(etcd.TaskRemove), EntryID: intent.EntryID,
		EnvironmentID: intent.EnvironmentID, EntryRevision: intent.EntryRevision,
		CurrentProjectionRevision: intent.CurrentProjectionRevision,
	})
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(value)
	clear(value)
	return hex.EncodeToString(digest[:]), nil
}
