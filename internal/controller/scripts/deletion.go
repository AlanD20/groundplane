package scripts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	scriptDeletionRoute           = "/scripts/{id}"
	scriptDeletionTimeoutSeconds  = int64(30)
	maximumScriptDeletionAttempts = 3
)

type scriptDeletionRepository interface {
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
	GetService(context.Context, string) (etcdstore.Versioned[servicerecord.ServiceRecord], error)
	GetScript(context.Context, string) (etcdstore.Versioned[scriptrecord.Record], error)
	BeginScriptDeletionWithTask(
		context.Context,
		etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
		etcdstore.Versioned[hierarchyrecord.ProjectRecord],
		etcdstore.Versioned[servicerecord.ServiceRecord],
		etcdstore.Versioned[scriptrecord.Record],
		deletionrecord.DeletionTombstoneRecord,
		etcd.TaskRecord,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

func (repository *MutationRepository) BeginScriptDeletionWithTask(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[servicerecord.ServiceRecord],
	current etcdstore.Versioned[scriptrecord.Record],
	tombstone deletionrecord.DeletionTombstoneRecord,
	task etcd.TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.scripts.BeginScriptDeletionWithTask(
		ctx, environment, project, target, current, tombstone, task, marker,
	)
}

type scriptDeletionEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   idempotencyrecord.ProtectedIntentRecord
}

type scriptDeletionIdempotency interface {
	ResolveReplayLocator(
		context.Context,
		idempotencyrecord.IdempotencyReplayTarget,
		string,
		string,
		string,
	) (idempotencyrecord.IdempotencyLocator, bool, error)
	Prepare(context.Context, idempotencyrecord.IdempotencyLocator, string) (scriptDeletionEvidence, error)
	ResolveExisting(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		scriptDeletionEvidence,
	) (requestidempotency.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		scriptDeletionEvidence,
		etcd.IdempotencyTransactionResult,
	) (requestidempotency.Resolution, error)
	ResolveUnknown(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		scriptDeletionEvidence,
		error,
	) (requestidempotency.Resolution, error)
}

type durableScriptDeletionIdempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  *etcd.IdempotencyRepository
}

func NewDeletionIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableScriptDeletionIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Script deletion idempotency is not configured")
	}
	return &durableScriptDeletionIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableScriptDeletionIdempotency) ResolveReplayLocator(
	ctx context.Context,
	target idempotencyrecord.IdempotencyReplayTarget,
	method string,
	route string,
	key string,
) (idempotencyrecord.IdempotencyLocator, bool, error) {
	return service.repository.ResolveReplayLocator(ctx, target, method, route, key)
}

func (service *durableScriptDeletionIdempotency) Prepare(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	scriptID string,
) (scriptDeletionEvidence, error) {
	if locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		ids.Validate(ids.KindEnvironment, locator.ScopeID) != nil {
		return scriptDeletionEvidence{}, errs.New(errs.KindInternal, "Script deletion replay scope is invalid")
	}
	scope := requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: locator.ScopeID}
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodDelete, Route: scriptDeletionRoute, Scope: scope,
		Path:  []requestidempotency.PathBinding{{Name: "id", Value: scriptID}},
		Query: requestidempotency.Object(), Body: requestidempotency.NoBody(),
	})
	if err != nil {
		return scriptDeletionEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return scriptDeletionEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return scriptDeletionEvidence{}, err
	}
	return scriptDeletionEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableScriptDeletionIdempotency) ResolveExisting(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence scriptDeletionEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableScriptDeletionIdempotency) ResolveKnown(
	ctx context.Context,
	evidence scriptDeletionEvidence,
	result etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableScriptDeletionIdempotency) ResolveUnknown(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence scriptDeletionEvidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type scriptDeletionService struct {
	repository  scriptDeletionRepository
	idempotency scriptDeletionIdempotency
	now         func() time.Time
}

func NewDeletionService(
	repository scriptDeletionRepository,
	idempotency scriptDeletionIdempotency,
) (*scriptDeletionService, error) {
	if repository == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Script deletion service is not configured")
	}
	return &scriptDeletionService{repository: repository, idempotency: idempotency, now: time.Now}, nil
}

func (service *scriptDeletionService) RemoveScript(
	ctx context.Context,
	scriptID string,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Script deletion context is required")
	}
	if ids.Validate(ids.KindScript, scriptID) != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Script id is invalid")
	}
	for attempt := 0; attempt < maximumScriptDeletionAttempts; attempt++ {
		response, err := service.deleteScriptOnce(ctx, scriptID, idempotencyKey)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumScriptDeletionAttempts-1 {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Script deletion retry bound was not enforced")
}

func (service *scriptDeletionService) deleteScriptOnce(
	ctx context.Context,
	scriptID string,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	target := idempotencyrecord.IdempotencyReplayTarget{Kind: idempotencyrecord.IdempotencyReplayTargetScript, ID: scriptID}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx, target, http.MethodDelete, scriptDeletionRoute, idempotencyKey,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if indexed {
		evidence, err := service.idempotency.Prepare(ctx, locator, scriptID)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		defer clear(evidence.durable.Ciphertext)
		resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		if !existing || resolution.Kind != requestidempotency.ResolutionReplay {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Script deletion replay target is inconsistent",
			)
		}
		return requestidempotency.CloneResponse(resolution.Response), nil
	}

	current, err := service.repository.GetScript(ctx, scriptID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	environment, err := service.repository.GetEnvironment(ctx, current.Record.EnvironmentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	taskOwner, err := etcd.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	targetService, err := service.repository.GetService(ctx, current.Record.ServiceID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	locator = idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment, ScopeID: environment.Record.ID,
		Method: http.MethodDelete, Route: scriptDeletionRoute, Key: idempotencyKey,
	}
	evidence, err := service.idempotency.Prepare(ctx, locator, scriptID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != requestidempotency.ResolutionReplay {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Script deletion replay resolution is invalid",
			)
		}
		return requestidempotency.CloneResponse(resolution.Response), nil
	}

	now := service.now().UTC()
	task := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation), IdempotencyKey: idempotencyKey,
		Owner: taskOwner, Actor: etcd.TaskActorOperator,
		Executor: etcd.TaskExecutorController, PlanID: ids.New(ids.KindPlan), RenderGeneration: 1,
		Type: etcd.TaskRemove, Target: scriptID,
		Params:         map[string]string{etcd.TaskResourceKindParam: etcd.TaskResourceScript},
		Steps:          []etcd.TaskStepRecord{{Kind: etcd.TaskStepOperation, ID: ids.New(ids.KindStep)}},
		TimeoutSeconds: scriptDeletionTimeoutSeconds,
		Status:         etcd.TaskStatusPending, NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	task.PlanHash, err = scriptDeletionPlanHash(scriptID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	responseBody, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := idempotencyrecord.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json",
		Body: append([]byte(nil), responseBody...),
	}
	marker := idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
		Locator: locator, ReplayTarget: &target, Intent: evidence.durable, Response: response,
		TaskID: task.ID, CreatedAt: now, UpdatedAt: now,
	}
	tombstone := deletionrecord.DeletionTombstoneRecord{
		TargetKind: deletionrecord.DeletionTargetScript, TargetID: scriptID,
		TargetRevision: current.Revision, TaskID: task.ID, Phase: deletionrecord.DeletionPhaseFinalizing,
		CreatedAt: now, UpdatedAt: now,
	}
	result, deleteErr := service.repository.BeginScriptDeletionWithTask(
		ctx, environment, project, targetService, current, tombstone, task, marker,
	)
	if deleteErr != nil {
		if !isUnknownScriptDeletionOutcome(deleteErr) {
			return idempotencyrecord.IdempotencyResponse{}, deleteErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, deleteErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case requestidempotency.ResolutionApplied:
		return requestidempotency.CloneResponse(response), nil
	case requestidempotency.ResolutionReplay:
		return requestidempotency.CloneResponse(resolution.Response), nil
	default:
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Script deletion resolution is invalid")
	}
}

func scriptDeletionPlanHash(scriptID string) (string, error) {
	value, err := json.Marshal(struct {
		Version  int    `json:"version"`
		Type     string `json:"type"`
		ScriptID string `json:"script_id"`
	}{Version: 1, Type: string(etcd.TaskRemove), ScriptID: scriptID})
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(value)
	clear(value)
	return hex.EncodeToString(digest[:]), nil
}

func isUnknownScriptDeletionOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
