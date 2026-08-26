package entry

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const entryDeletionRoute = "/entries/{id}"

// EtcdRepository is the private persistence adapter for Entry removal. Its
// exported constructor exists for app composition; the use-case boundary is
// the DTO-free Repository interface.
type EtcdRepository struct {
	hierarchy   *etcd.HierarchyRepository
	entries     *etcd.EntryRepository
	components  *etcd.ComponentRepository
	coordinator *idempotentintent.Coordinator
	idempotency *etcd.IdempotencyRepository
}

func NewEtcdRepository(
	hierarchy *etcd.HierarchyRepository,
	entries *etcd.EntryRepository,
	components *etcd.ComponentRepository,
	coordinator *idempotentintent.Coordinator,
	idempotency *etcd.IdempotencyRepository,
) (*EtcdRepository, error) {
	if hierarchy == nil || entries == nil || components == nil || coordinator == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "entry deletion persistence is not configured")
	}
	return &EtcdRepository{
		hierarchy: hierarchy, entries: entries, components: components,
		coordinator: coordinator, idempotency: idempotency,
	}, nil
}

type etcdRemovalState struct {
	entry       etcd.Versioned[etcd.EntryRecord]
	environment etcd.Versioned[etcd.EnvironmentRecord]
	project     etcd.Versioned[etcd.ProjectRecord]
	tenant      *etcd.Versioned[etcd.TenantRecord]
	projection  *etcd.Versioned[etcd.EnvironmentComposeProjection]
	cloudflare  *etcd.Versioned[etcd.ComponentRecord]
}

type etcdRemovalEvidence struct {
	candidate idempotentintent.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

func (repository *EtcdRepository) InspectRemoval(
	ctx context.Context,
	request RemoveRequest,
) (RemovalInspection, error) {
	if repository == nil {
		return RemovalInspection{}, errs.New(errs.KindInternal, "entry deletion persistence is not configured")
	}
	target := etcd.IdempotencyReplayTarget{Kind: etcd.IdempotencyReplayTargetEntry, ID: request.EntryID}
	locator, indexed, err := repository.idempotency.ResolveReplayLocator(
		ctx, target, http.MethodDelete, entryDeletionRoute, request.IdempotencyKey,
	)
	if err != nil {
		return RemovalInspection{}, err
	}
	if indexed {
		outcome, found, replayErr := repository.resolveExisting(ctx, locator, request.EntryID)
		if replayErr != nil {
			return RemovalInspection{}, replayErr
		}
		if !found {
			return RemovalInspection{}, errs.New(errs.KindInternal, "entry deletion replay target is inconsistent")
		}
		return RemovalInspection{Replay: &outcome}, nil
	}
	state, err := repository.loadRemovalState(ctx, request.EntryID)
	if err != nil {
		return RemovalInspection{}, err
	}
	locator = removalLocator(state.environment.Record.ID, request.IdempotencyKey)
	outcome, found, err := repository.resolveExisting(ctx, locator, request.EntryID)
	if err != nil {
		return RemovalInspection{}, err
	}
	if found {
		return RemovalInspection{Replay: &outcome}, nil
	}
	return RemovalInspection{Candidate: removalCandidate(state)}, nil
}

func (repository *EtcdRepository) PublishRemoval(
	ctx context.Context,
	publication RemovalPublication,
) (RemovalOutcome, error) {
	if repository == nil {
		return RemovalOutcome{}, errs.New(errs.KindInternal, "entry deletion persistence is not configured")
	}
	request := publication.Request
	target := etcd.IdempotencyReplayTarget{Kind: etcd.IdempotencyReplayTargetEntry, ID: request.EntryID}
	locator, indexed, err := repository.idempotency.ResolveReplayLocator(
		ctx, target, http.MethodDelete, entryDeletionRoute, request.IdempotencyKey,
	)
	if err != nil {
		return RemovalOutcome{}, err
	}
	if indexed {
		outcome, found, replayErr := repository.resolveExisting(ctx, locator, request.EntryID)
		if replayErr != nil {
			return RemovalOutcome{}, replayErr
		}
		if !found {
			return RemovalOutcome{}, errs.New(errs.KindInternal, "entry deletion replay target is inconsistent")
		}
		return outcome, nil
	}
	state, err := repository.loadRemovalState(ctx, request.EntryID)
	if err != nil {
		return RemovalOutcome{}, err
	}
	if !sameRemovalCandidate(removalCandidate(state), publication.Candidate) {
		return RemovalOutcome{}, errs.New(errs.KindStateConflict, "entry deletion candidate changed")
	}
	locator = removalLocator(state.environment.Record.ID, request.IdempotencyKey)
	evidence, err := repository.prepareEvidence(ctx, locator, request.EntryID)
	if err != nil {
		return RemovalOutcome{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := repository.coordinator.ResolveExisting(
		ctx, repository.idempotency, locator, evidence.candidate,
	)
	if err != nil {
		return RemovalOutcome{}, err
	}
	if existing {
		if resolution.Kind != idempotentintent.ResolutionReplay {
			return RemovalOutcome{}, errs.New(errs.KindInternal, "entry deletion replay resolution is invalid")
		}
		return removalOutcome(resolution.Response)
	}

	owner, err := etcd.EnvironmentTaskOwner(state.project.Record, state.environment.Record)
	if err != nil {
		return RemovalOutcome{}, err
	}
	taskInput := publication.Task
	task := etcd.TaskRecord{
		ID: taskInput.ID, OperationID: taskInput.OperationID, IdempotencyKey: request.IdempotencyKey,
		Owner: owner, Actor: etcd.TaskActorOperator, PlanID: taskInput.PlanID,
		Type: etcd.TaskRemove, Target: request.EntryID, Status: etcd.TaskStatusPending,
		NextEventSequence: 1, CreatedAt: taskInput.CreatedAt, UpdatedAt: taskInput.CreatedAt,
	}
	intent, err := etcd.NewEntryRemovalIntent(
		task.ID, state.environment.Record.ID, request.EntryID, state.entry.Revision,
		state.projection, task.CreatedAt,
	)
	if err != nil {
		return RemovalOutcome{}, err
	}
	task, err = applyRemovalTaskPlan(task, publication.Plan)
	if err != nil {
		return RemovalOutcome{}, err
	}
	task.Params[etcd.TaskEntryCloudflareComponentParam] = ""
	task.Params[etcd.TaskEntryCloudflareRevisionParam] = "0"
	if publication.Candidate.Cloudflare.Present {
		task.Params[etcd.TaskEntryCloudflareComponentParam] = publication.Candidate.Cloudflare.ID
		task.Params[etcd.TaskEntryCloudflareRevisionParam] = strconv.FormatInt(
			publication.Candidate.Cloudflare.Revision, 10)
	}

	responseBody, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	if err != nil {
		return RemovalOutcome{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json",
		Body: append([]byte(nil), responseBody...),
	}
	marker := etcd.IdempotencyMarker{
		Kind: etcd.IdempotencyMarkerTask, State: etcd.IdempotencyMarkerPending,
		Locator: locator, ReplayTarget: &target, Intent: evidence.durable, Response: response,
		TaskID: task.ID, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt,
	}
	phase := etcd.DeletionPhaseFinalizing
	if state.projection != nil {
		phase = etcd.DeletionPhaseHostEffects
	}
	tombstone := etcd.DeletionTombstoneRecord{
		TargetKind: etcd.DeletionTargetEntry, TargetID: request.EntryID,
		TargetRevision: state.entry.Revision, TaskID: task.ID, Phase: phase,
		CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt,
	}
	result, mutationErr := repository.entries.BeginEntryDeletionWithTask(
		ctx, state.environment, state.project, state.entry, state.projection,
		state.cloudflare, tombstone, intent, task, marker,
	)
	if mutationErr != nil {
		if !isUnknownRemovalOutcome(mutationErr) {
			return RemovalOutcome{}, mutationErr
		}
		resolution, err = repository.coordinator.ResolveUnknown(
			ctx, repository.idempotency, locator, evidence.candidate, mutationErr,
		)
	} else {
		resolution, err = repository.coordinator.ResolveKnown(ctx, evidence.candidate, result)
	}
	if err != nil {
		return RemovalOutcome{}, err
	}
	switch resolution.Kind {
	case idempotentintent.ResolutionApplied:
		return removalOutcome(response)
	case idempotentintent.ResolutionReplay:
		return removalOutcome(resolution.Response)
	default:
		return RemovalOutcome{}, errs.New(errs.KindInternal, "entry deletion resolution is invalid")
	}
}

func (repository *EtcdRepository) loadRemovalState(
	ctx context.Context,
	entryID string,
) (etcdRemovalState, error) {
	entry, err := repository.entries.GetEntry(ctx, entryID)
	if err != nil {
		return etcdRemovalState{}, err
	}
	environment, err := repository.hierarchy.GetEnvironment(ctx, entry.Record.EnvironmentID)
	if err != nil {
		return etcdRemovalState{}, err
	}
	project, err := repository.hierarchy.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return etcdRemovalState{}, err
	}
	var tenant *etcd.Versioned[etcd.TenantRecord]
	switch project.Record.Kind {
	case etcd.ProjectKindTenant:
		stored, tenantErr := repository.hierarchy.GetTenant(ctx, project.Record.TenantID)
		if tenantErr != nil {
			return etcdRemovalState{}, tenantErr
		}
		tenant = &stored
	case etcd.ProjectKindBacking:
		if project.Record.TenantID != "" {
			return etcdRemovalState{}, errs.New(errs.KindInternal, "backing Entry Project has a Tenant")
		}
	default:
		return etcdRemovalState{}, errs.New(errs.KindInternal, "entry Project kind is invalid")
	}
	cloudflare, err := repository.cloudflareComponent(ctx, environment.Record.ID, entryID)
	if err != nil {
		return etcdRemovalState{}, err
	}
	projection, found, err := repository.hierarchy.GetEnvironmentComposeProjection(ctx, environment.Record.ID)
	if err != nil {
		return etcdRemovalState{}, err
	}
	var projectionPointer *etcd.Versioned[etcd.EnvironmentComposeProjection]
	if found {
		projectionPointer = &projection
	}
	return etcdRemovalState{
		entry: entry, environment: environment, project: project, tenant: tenant,
		projection: projectionPointer, cloudflare: cloudflare,
	}, nil
}

func (repository *EtcdRepository) cloudflareComponent(
	ctx context.Context,
	environmentID string,
	entryID string,
) (*etcd.Versioned[etcd.ComponentRecord], error) {
	cursor := ""
	var selected *etcd.Versioned[etcd.ComponentRecord]
	for {
		page, err := repository.components.ListEnvironmentComponents(
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
				return nil, errs.New(errs.KindInternal, "environment has duplicate Cloudflare Components")
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
		return nil, errs.New(errs.KindResourceInUse, "entry is the enabled Cloudflare Tunnel token")
	}
	return selected, nil
}

func removalCandidate(state etcdRemovalState) RemovalCandidate {
	identity := RemovalEnvironmentIdentity{
		ProjectID: state.project.Record.ID, ProjectSlug: state.project.Record.Slug,
		EnvironmentID: state.environment.Record.ID, EnvironmentName: state.environment.Record.Name,
		AuthorizedVolumeDir: state.environment.Record.VolumeDir,
	}
	tenantRevision := int64(0)
	if state.tenant != nil {
		identity.TenantID = state.tenant.Record.ID
		identity.TenantSlug = state.tenant.Record.Slug
		tenantRevision = state.tenant.Revision
	}
	projectionRevision := int64(0)
	if state.projection != nil {
		projectionRevision = state.projection.Revision
	}
	cloudflare := RemovalDependencyFence{}
	if state.cloudflare != nil {
		cloudflare = RemovalDependencyFence{
			Present: true, ID: state.cloudflare.Record.Desired.ID, Revision: state.cloudflare.Revision,
		}
	}
	return RemovalCandidate{
		EntryID: state.entry.Record.Entry.ID, EntryRevision: state.entry.Revision,
		EnvironmentRevision: state.environment.Revision, ProjectRevision: state.project.Revision,
		TenantRevision: tenantRevision, ProjectionRevision: projectionRevision,
		Identity: identity, Cloudflare: cloudflare,
	}
}

func sameRemovalCandidate(left RemovalCandidate, right RemovalCandidate) bool {
	return left.EntryID == right.EntryID && left.EntryRevision == right.EntryRevision &&
		left.EnvironmentRevision == right.EnvironmentRevision && left.ProjectRevision == right.ProjectRevision &&
		left.TenantRevision == right.TenantRevision && left.ProjectionRevision == right.ProjectionRevision &&
		left.Identity == right.Identity && left.Cloudflare == right.Cloudflare
}

func removalLocator(environmentID string, key string) etcd.IdempotencyLocator {
	return etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: environmentID,
		Method: http.MethodDelete, Route: entryDeletionRoute, Key: key,
	}
}

func (repository *EtcdRepository) prepareEvidence(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	entryID string,
) (etcdRemovalEvidence, error) {
	if locator.ScopeKind != etcd.IdempotencyScopeEnvironment ||
		ids.Validate(ids.KindEnvironment, locator.ScopeID) != nil {
		return etcdRemovalEvidence{}, errs.New(errs.KindInternal, "entry deletion replay scope is invalid")
	}
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: http.MethodDelete, Route: entryDeletionRoute,
		Scope: idempotentintent.Scope{Kind: idempotentintent.ScopeEnvironment, ID: locator.ScopeID},
		Path:  []idempotentintent.PathBinding{{Name: "id", Value: entryID}},
		Query: idempotentintent.Object(), Body: idempotentintent.NoBody(),
	})
	if err != nil {
		return etcdRemovalEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := repository.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return etcdRemovalEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return etcdRemovalEvidence{}, err
	}
	return etcdRemovalEvidence{candidate: candidate, durable: durable}, nil
}

func (repository *EtcdRepository) resolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	entryID string,
) (RemovalOutcome, bool, error) {
	evidence, err := repository.prepareEvidence(ctx, locator, entryID)
	if err != nil {
		return RemovalOutcome{}, false, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, found, err := repository.coordinator.ResolveExisting(
		ctx, repository.idempotency, locator, evidence.candidate,
	)
	if err != nil || !found {
		return RemovalOutcome{}, found, err
	}
	if resolution.Kind != idempotentintent.ResolutionReplay {
		return RemovalOutcome{}, false, errs.New(errs.KindInternal, "entry deletion replay resolution is invalid")
	}
	outcome, err := removalOutcome(resolution.Response)
	return outcome, true, err
}

func removalOutcome(response etcd.IdempotencyResponse) (RemovalOutcome, error) {
	if response.Status != http.StatusAccepted || response.ContentKind != "application/json" {
		return RemovalOutcome{}, errs.New(errs.KindInternal, "entry deletion replay response is invalid")
	}
	var accepted apiTypes.TaskAccepted
	if err := json.Unmarshal(
		response.Body,
		&accepted,
	); err != nil ||
		ids.Validate(ids.KindTask, accepted.TaskID) != nil {
		return RemovalOutcome{}, errs.New(errs.KindInternal, "entry deletion replay body is invalid")
	}
	return RemovalOutcome{TaskID: accepted.TaskID}, nil
}

func isUnknownRemovalOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}

func applyRemovalTaskPlan(task etcd.TaskRecord, plan RemovalTaskPlan) (etcd.TaskRecord, error) {
	switch plan.Executor {
	case RemovalExecutorAgent:
		identity := plan.Identity
		task.Executor = etcd.TaskExecutorAgent
		task.Params = map[string]string{
			etcd.TaskEntryEnvironmentParam:           plan.EnvironmentID,
			etcd.TaskMaterializationEnvironmentParam: plan.EnvironmentID,
			etcd.EnvironmentBlueprintRevisionParam:   plan.BlueprintRevisionID,
			etcd.TaskComposeArtifactParam:            plan.ArtifactID,
			etcd.TaskEntryTenantSlugParam:            identity.TenantSlug,
			etcd.TaskEntryProjectSlugParam:           identity.ProjectSlug,
			etcd.TaskEntryEnvironmentNameParam:       identity.EnvironmentName,
			etcd.TaskEntryAuthorizedVolumeDirParam:   identity.AuthorizedVolumeDir,
		}
	case RemovalExecutorController:
		task.Executor = etcd.TaskExecutorController
		task.Params = map[string]string{
			etcd.TaskResourceKindParam:     etcd.TaskResourceEntry,
			etcd.TaskEntryEnvironmentParam: plan.EnvironmentID,
		}
	default:
		return etcd.TaskRecord{}, errs.New(errs.KindInternal, "entry removal executor is invalid")
	}
	task.PlanHash = plan.PlanHash
	task.RenderGeneration = plan.RenderGeneration
	task.TimeoutSeconds = plan.TimeoutSeconds
	task.Steps = make([]etcd.TaskStepRecord, len(plan.Steps))
	for index, step := range plan.Steps {
		task.Steps[index] = etcd.TaskStepRecord{ID: step.ID}
	}
	task.Materializations = make([]etcd.TaskMaterializationRecord, len(plan.Materializations))
	for index, materialization := range plan.Materializations {
		converted, err := persistedRemovalMaterialization(materialization)
		if err != nil {
			return etcd.TaskRecord{}, err
		}
		task.Materializations[index] = converted
	}
	return task, nil
}

func persistedRemovalMaterialization(input RemovalMaterialization) (etcd.TaskMaterializationRecord, error) {
	source := etcd.TaskMaterializationSource{}
	switch input.Source.Kind {
	case RemovalSourceRemoval:
		source.Kind = etcd.TaskMaterializationSourceRemoval
	case RemovalSourceGeneratedEnvironment:
		if input.Source.GeneratedEnvironment == nil {
			return etcd.TaskMaterializationRecord{}, errs.New(errs.KindInternal, "entry removal source is incomplete")
		}
		values := make([]etcd.TaskGeneratedEnvironmentEntryReference, len(input.Source.GeneratedEnvironment.Values))
		for index, value := range input.Source.GeneratedEnvironment.Values {
			storage, err := persistedRemovalValueStorage(value.Storage)
			if err != nil {
				return etcd.TaskMaterializationRecord{}, err
			}
			values[index] = etcd.TaskGeneratedEnvironmentEntryReference{
				Name: value.Name,
				Value: etcd.TaskEntryValueReference{
					EntryID: value.EntryID, ValueGenerationID: value.ValueGenerationID, Storage: storage,
				},
			}
		}
		source.Kind = etcd.TaskMaterializationSourceGeneratedEnvironment
		source.GeneratedEnvironment = &etcd.TaskGeneratedEnvironmentValueReference{
			FormatVersion: input.Source.GeneratedEnvironment.FormatVersion, Values: values,
		}
	default:
		return etcd.TaskMaterializationRecord{}, errs.New(errs.KindInternal, "entry removal source kind is invalid")
	}
	outputKind, err := persistedRemovalOutputKind(input.OutputKind)
	if err != nil {
		return etcd.TaskMaterializationRecord{}, err
	}
	return etcd.TaskMaterializationRecord{
		StepID: input.StepID, MaterializationID: input.MaterializationID,
		EnvironmentID: input.EnvironmentID, Destination: input.Destination,
		ServiceID: input.ServiceID, ServiceName: input.ServiceName,
		OutputKind: outputKind, UID: input.UID, GID: input.GID,
		Mode: input.Mode, Length: input.Length, SHA256: input.SHA256, Source: source,
	}, nil
}

func persistedRemovalValueStorage(input RemovalValueStorage) (etcd.TaskEntryValueStorage, error) {
	switch input {
	case RemovalValueStoragePlain:
		return etcd.TaskEntryValueStoragePlain, nil
	case RemovalValueStorageSecret:
		return etcd.TaskEntryValueStorageSecret, nil
	default:
		return "", errs.New(errs.KindInternal, "entry removal value storage is invalid")
	}
}

func persistedRemovalOutputKind(input RemovalOutputKind) (etcd.TaskMaterializationOutputKind, error) {
	switch input {
	case RemovalOutputGeneratedEnvironment:
		return etcd.TaskMaterializationOutputGeneratedEnvironment, nil
	case RemovalOutputPlainFile:
		return etcd.TaskMaterializationOutputPlainFile, nil
	case RemovalOutputSecretFile:
		return etcd.TaskMaterializationOutputSecretFile, nil
	case RemovalOutputRemoveGeneratedEnv:
		return etcd.TaskMaterializationOutputRemoveGeneratedEnv, nil
	case RemovalOutputRemovePlainFile:
		return etcd.TaskMaterializationOutputRemovePlainFile, nil
	case RemovalOutputRemoveSecretFile:
		return etcd.TaskMaterializationOutputRemoveSecretFile, nil
	default:
		return "", errs.New(errs.KindInternal, "entry removal output kind is invalid")
	}
}
