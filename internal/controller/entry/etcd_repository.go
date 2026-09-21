package entry

import (
	"context"
	"encoding/json"
	"errors"
	materializationrecord "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
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
	coordinator *requestidempotency.Coordinator
	idempotency *etcd.IdempotencyRepository
}

func NewEtcdRepository(
	hierarchy *etcd.HierarchyRepository,
	entries *etcd.EntryRepository,
	coordinator *requestidempotency.Coordinator,
	idempotency *etcd.IdempotencyRepository,
) (*EtcdRepository, error) {
	if hierarchy == nil || entries == nil || coordinator == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "entry deletion persistence is not configured")
	}
	return &EtcdRepository{
		hierarchy: hierarchy, entries: entries,
		coordinator: coordinator, idempotency: idempotency,
	}, nil
}

type etcdRemovalState struct {
	entry       etcdstore.Versioned[entryrecord.Record]
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	project     etcdstore.Versioned[hierarchyrecord.ProjectRecord]
	tenant      *etcdstore.Versioned[hierarchyrecord.TenantRecord]
	projection  *etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]
}

type etcdRemovalEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   idempotencyrecord.ProtectedIntentRecord
}

func (repository *EtcdRepository) InspectRemoval(
	ctx context.Context,
	request RemoveRequest,
) (RemovalInspection, error) {
	if repository == nil {
		return RemovalInspection{}, errs.New(errs.KindInternal, "entry deletion persistence is not configured")
	}
	target := idempotencyrecord.IdempotencyReplayTarget{Kind: idempotencyrecord.IdempotencyReplayTargetEntry, ID: request.EntryID}
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
	target := idempotencyrecord.IdempotencyReplayTarget{Kind: idempotencyrecord.IdempotencyReplayTargetEntry, ID: request.EntryID}
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
		if resolution.Kind != requestidempotency.ResolutionReplay {
			return RemovalOutcome{}, errs.New(errs.KindInternal, "entry deletion replay resolution is invalid")
		}
		return removalOutcome(resolution.Response)
	}

	owner, err := taskjournal.EnvironmentTaskOwner(state.project.Record, state.environment.Record)
	if err != nil {
		return RemovalOutcome{}, err
	}
	taskInput := publication.Task
	task := etcd.TaskRecord{
		ID: taskInput.ID, OperationID: taskInput.OperationID, IdempotencyKey: request.IdempotencyKey,
		Owner: owner, Actor: taskjournal.TaskActorOperator, PlanID: taskInput.PlanID,
		Type: taskjournal.TaskRemove, Target: request.EntryID, Status: taskjournal.TaskStatusPending,
		NextEventSequence: 1, CreatedAt: taskInput.CreatedAt, UpdatedAt: taskInput.CreatedAt,
	}
	intent, err := environmentchanges.NewEntryRemovalIntent(
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
	responseBody, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	if err != nil {
		return RemovalOutcome{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := idempotencyrecord.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json",
		Body: append([]byte(nil), responseBody...),
	}
	marker := idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
		Locator: locator, ReplayTarget: &target, Intent: evidence.durable, Response: response,
		TaskID: task.ID, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt,
	}
	phase := deletionrecord.DeletionPhaseFinalizing
	if state.projection != nil {
		phase = deletionrecord.DeletionPhaseHostEffects
	}
	tombstone := deletionrecord.DeletionTombstoneRecord{
		TargetKind: deletionrecord.DeletionTargetEntry, TargetID: request.EntryID,
		TargetRevision: state.entry.Revision, TaskID: task.ID, Phase: phase,
		CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt,
	}
	result, mutationErr := repository.entries.BeginEntryDeletionWithTask(
		ctx, state.environment, state.project, state.entry, state.projection,
		tombstone, intent, task, marker,
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
	case requestidempotency.ResolutionApplied:
		return removalOutcome(response)
	case requestidempotency.ResolutionReplay:
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
	var tenant *etcdstore.Versioned[hierarchyrecord.TenantRecord]
	switch project.Record.Kind {
	case hierarchyrecord.ProjectKindTenant:
		stored, tenantErr := repository.hierarchy.GetTenant(ctx, project.Record.TenantID)
		if tenantErr != nil {
			return etcdRemovalState{}, tenantErr
		}
		tenant = &stored
	case hierarchyrecord.ProjectKindBacking:
		if project.Record.TenantID != "" {
			return etcdRemovalState{}, errs.New(errs.KindInternal, "backing Entry Project has a Tenant")
		}
	default:
		return etcdRemovalState{}, errs.New(errs.KindInternal, "entry Project kind is invalid")
	}
	projection, found, err := repository.hierarchy.GetEnvironmentAppliedComposeProjection(ctx, environment.Record.ID)
	if err != nil {
		return etcdRemovalState{}, err
	}
	projectionPointer := appliedEntryProjection(projection, found, entry.Record.Entry.ID)
	return etcdRemovalState{
		entry: entry, environment: environment, project: project, tenant: tenant,
		projection: projectionPointer,
	}, nil
}

func appliedEntryProjection(
	projection etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	found bool,
	entryID string,
) *etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection] {
	if !found {
		return nil
	}

	for _, record := range projection.Record.Entries {
		if record.Entry.ID == entryID {
			return &projection
		}
	}

	return nil
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
	return RemovalCandidate{
		EntryID: state.entry.Record.Entry.ID, EntryRevision: state.entry.Revision,
		EnvironmentRevision: state.environment.Revision, ProjectRevision: state.project.Revision,
		TenantRevision: tenantRevision, ProjectionRevision: projectionRevision,
		Identity: identity,
	}
}

func sameRemovalCandidate(left RemovalCandidate, right RemovalCandidate) bool {
	return left.EntryID == right.EntryID && left.EntryRevision == right.EntryRevision &&
		left.EnvironmentRevision == right.EnvironmentRevision && left.ProjectRevision == right.ProjectRevision &&
		left.TenantRevision == right.TenantRevision && left.ProjectionRevision == right.ProjectionRevision &&
		left.Identity == right.Identity
}

func removalLocator(environmentID string, key string) idempotencyrecord.IdempotencyLocator {
	return idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment, ScopeID: environmentID,
		Method: http.MethodDelete, Route: entryDeletionRoute, Key: key,
	}
}

func (repository *EtcdRepository) prepareEvidence(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	entryID string,
) (etcdRemovalEvidence, error) {
	if locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		ids.Validate(ids.KindEnvironment, locator.ScopeID) != nil {
		return etcdRemovalEvidence{}, errs.New(errs.KindInternal, "entry deletion replay scope is invalid")
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodDelete, Route: entryDeletionRoute,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: locator.ScopeID},
		Path:  []requestidempotency.PathBinding{{Name: "id", Value: entryID}},
		Query: requestidempotency.Object(), Body: requestidempotency.NoBody(),
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
	locator idempotencyrecord.IdempotencyLocator,
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
	if resolution.Kind != requestidempotency.ResolutionReplay {
		return RemovalOutcome{}, false, errs.New(errs.KindInternal, "entry deletion replay resolution is invalid")
	}
	outcome, err := removalOutcome(resolution.Response)
	return outcome, true, err
}

func removalOutcome(response idempotencyrecord.IdempotencyResponse) (RemovalOutcome, error) {
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
		task.Executor = taskjournal.TaskExecutorAgent
		task.Params = map[string]string{
			taskjournal.TaskEntryEnvironmentParam:           plan.EnvironmentID,
			taskjournal.TaskMaterializationEnvironmentParam: plan.EnvironmentID,
			blueprints.EnvironmentDesiredRevisionParam:      plan.BlueprintRevisionID,
			taskjournal.TaskComposeArtifactParam:            plan.ArtifactID,
			taskjournal.TaskEntryTenantSlugParam:            identity.TenantSlug,
			taskjournal.TaskEntryProjectSlugParam:           identity.ProjectSlug,
			taskjournal.TaskEntryEnvironmentNameParam:       identity.EnvironmentName,
			taskjournal.TaskEntryAuthorizedVolumeDirParam:   identity.AuthorizedVolumeDir,
		}
	case RemovalExecutorController:
		task.Executor = taskjournal.TaskExecutorController
		task.Params = map[string]string{
			taskjournal.TaskResourceKindParam:     taskjournal.TaskResourceEntry,
			taskjournal.TaskEntryEnvironmentParam: plan.EnvironmentID,
		}
	default:
		return etcd.TaskRecord{}, errs.New(errs.KindInternal, "entry removal executor is invalid")
	}
	task.PlanHash = plan.PlanHash
	task.RenderGeneration = plan.RenderGeneration
	task.TimeoutSeconds = plan.TimeoutSeconds
	task.Steps = make([]taskjournal.TaskStepRecord, len(plan.Steps))
	for index, step := range plan.Steps {
		task.Steps[index] = taskjournal.TaskStepRecord{Kind: taskjournal.TaskStepOperation, ID: step.ID}
	}
	task.Materializations = make([]materializationrecord.Record, len(plan.Materializations))
	for index, materialization := range plan.Materializations {
		converted, err := persistedRemovalMaterialization(materialization)
		if err != nil {
			return etcd.TaskRecord{}, err
		}
		task.Materializations[index] = converted
	}
	return task, nil
}

func persistedRemovalMaterialization(input RemovalMaterialization) (materializationrecord.Record, error) {
	source := materializationrecord.Source{}
	switch input.Source.Kind {
	case RemovalSourceRemoval:
		source.Kind = materializationrecord.SourceRemoval
	case RemovalSourceGeneratedEnvironment:
		if input.Source.GeneratedEnvironment == nil {
			return materializationrecord.Record{}, errs.New(errs.KindInternal, "entry removal source is incomplete")
		}
		values := make([]materializationrecord.GeneratedEnvironmentEntryReference, len(input.Source.GeneratedEnvironment.Values))
		for index, value := range input.Source.GeneratedEnvironment.Values {
			storage, err := persistedRemovalValueStorage(value.Storage)
			if err != nil {
				return materializationrecord.Record{}, err
			}
			values[index] = materializationrecord.GeneratedEnvironmentEntryReference{
				Name: value.Name,
				Value: materializationrecord.EntryValueReference{
					EntryID: value.EntryID, ValueGenerationID: value.ValueGenerationID, Storage: storage,
				},
			}
		}
		source.Kind = materializationrecord.SourceGeneratedEnvironment
		source.GeneratedEnvironment = &materializationrecord.GeneratedEnvironmentValueReference{
			FormatVersion: input.Source.GeneratedEnvironment.FormatVersion, Values: values,
		}
	default:
		return materializationrecord.Record{}, errs.New(errs.KindInternal, "entry removal source kind is invalid")
	}
	outputKind, err := persistedRemovalOutputKind(input.OutputKind)
	if err != nil {
		return materializationrecord.Record{}, err
	}
	return materializationrecord.Record{
		StepID: input.StepID, MaterializationID: input.MaterializationID,
		EnvironmentID: input.EnvironmentID, Destination: input.Destination,
		ServiceID: input.ServiceID, ServiceName: input.ServiceName,
		OutputKind: outputKind, UID: input.UID, GID: input.GID,
		Mode: input.Mode, Length: input.Length, SHA256: input.SHA256, Source: source,
	}, nil
}

func persistedRemovalValueStorage(input RemovalValueStorage) (materializationrecord.EntryValueStorage, error) {
	switch input {
	case RemovalValueStoragePlain:
		return materializationrecord.EntryValueStoragePlain, nil
	case RemovalValueStorageSecret:
		return materializationrecord.EntryValueStorageSecret, nil
	default:
		return "", errs.New(errs.KindInternal, "entry removal value storage is invalid")
	}
}

func persistedRemovalOutputKind(input RemovalOutputKind) (materializationrecord.OutputKind, error) {
	switch input {
	case RemovalOutputGeneratedEnvironment:
		return materializationrecord.OutputGeneratedEnvironment, nil
	case RemovalOutputPlainFile:
		return materializationrecord.OutputPlainFile, nil
	case RemovalOutputSecretFile:
		return materializationrecord.OutputSecretFile, nil
	case RemovalOutputRemoveGeneratedEnv:
		return materializationrecord.OutputRemoveGeneratedEnv, nil
	case RemovalOutputRemovePlainFile:
		return materializationrecord.OutputRemovePlainFile, nil
	case RemovalOutputRemoveSecretFile:
		return materializationrecord.OutputRemoveSecretFile, nil
	default:
		return "", errs.New(errs.KindInternal, "entry removal output kind is invalid")
	}
}
