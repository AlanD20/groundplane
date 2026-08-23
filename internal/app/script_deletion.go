package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
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
	GetEnvironment(context.Context, string) (etcd.Versioned[etcd.EnvironmentRecord], error)
	GetProject(context.Context, string) (etcd.Versioned[etcd.ProjectRecord], error)
	GetService(context.Context, string) (etcd.Versioned[etcd.ServiceRecord], error)
	GetScript(context.Context, string) (etcd.Versioned[etcd.ScriptRecord], error)
	BeginScriptDeletionWithTask(
		context.Context,
		etcd.Versioned[etcd.EnvironmentRecord],
		etcd.Versioned[etcd.ProjectRecord],
		etcd.Versioned[etcd.ServiceRecord],
		etcd.Versioned[etcd.ScriptRecord],
		etcd.DeletionTombstoneRecord,
		etcd.TaskRecord,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

func (repository *durableScriptMutationRepository) BeginScriptDeletionWithTask(
	ctx context.Context,
	environment etcd.Versioned[etcd.EnvironmentRecord],
	project etcd.Versioned[etcd.ProjectRecord],
	target etcd.Versioned[etcd.ServiceRecord],
	current etcd.Versioned[etcd.ScriptRecord],
	tombstone etcd.DeletionTombstoneRecord,
	task etcd.TaskRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.scripts.BeginScriptDeletionWithTask(
		ctx, environment, project, target, current, tombstone, task, marker,
	)
}

type scriptDeletionEvidence struct {
	candidate idempotentintent.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type scriptDeletionIdempotency interface {
	ResolveReplayLocator(
		context.Context,
		etcd.IdempotencyReplayTarget,
		string,
		string,
		string,
	) (etcd.IdempotencyLocator, bool, error)
	Prepare(context.Context, etcd.IdempotencyLocator, string) (scriptDeletionEvidence, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		scriptDeletionEvidence,
	) (idempotentintent.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		scriptDeletionEvidence,
		etcd.IdempotencyTransactionResult,
	) (idempotentintent.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		scriptDeletionEvidence,
		error,
	) (idempotentintent.Resolution, error)
}

type durableScriptDeletionIdempotency struct {
	coordinator *idempotentintent.Coordinator
	repository  *etcd.IdempotencyRepository
}

func newDurableScriptDeletionIdempotency(
	coordinator *idempotentintent.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableScriptDeletionIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Script deletion idempotency is not configured")
	}
	return &durableScriptDeletionIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableScriptDeletionIdempotency) ResolveReplayLocator(
	ctx context.Context,
	target etcd.IdempotencyReplayTarget,
	method string,
	route string,
	key string,
) (etcd.IdempotencyLocator, bool, error) {
	return service.repository.ResolveReplayLocator(ctx, target, method, route, key)
}

func (service *durableScriptDeletionIdempotency) Prepare(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	scriptID string,
) (scriptDeletionEvidence, error) {
	if locator.ScopeKind != etcd.IdempotencyScopeEnvironment ||
		ids.Validate(ids.KindEnvironment, locator.ScopeID) != nil {
		return scriptDeletionEvidence{}, errs.New(errs.KindInternal, "Script deletion replay scope is invalid")
	}
	scope := idempotentintent.Scope{Kind: idempotentintent.ScopeEnvironment, ID: locator.ScopeID}
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: http.MethodDelete, Route: scriptDeletionRoute, Scope: scope,
		Path:  []idempotentintent.PathBinding{{Name: "id", Value: scriptID}},
		Query: idempotentintent.Object(), Body: idempotentintent.NoBody(),
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
	locator etcd.IdempotencyLocator,
	evidence scriptDeletionEvidence,
) (idempotentintent.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableScriptDeletionIdempotency) ResolveKnown(
	ctx context.Context,
	evidence scriptDeletionEvidence,
	result etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableScriptDeletionIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence scriptDeletionEvidence,
	original error,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type scriptDeletionService struct {
	repository  scriptDeletionRepository
	idempotency scriptDeletionIdempotency
	now         func() time.Time
}

func newScriptDeletionService(
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
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Script deletion context is required")
	}
	if ids.Validate(ids.KindScript, scriptID) != nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Script id is invalid")
	}
	for attempt := 0; attempt < maximumScriptDeletionAttempts; attempt++ {
		response, err := service.deleteScriptOnce(ctx, scriptID, idempotencyKey)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumScriptDeletionAttempts-1 {
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Script deletion retry bound was not enforced")
}

func (service *scriptDeletionService) deleteScriptOnce(
	ctx context.Context,
	scriptID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	target := etcd.IdempotencyReplayTarget{Kind: etcd.IdempotencyReplayTargetScript, ID: scriptID}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx, target, http.MethodDelete, scriptDeletionRoute, idempotencyKey,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if indexed {
		evidence, err := service.idempotency.Prepare(ctx, locator, scriptID)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
		defer clear(evidence.durable.Ciphertext)
		resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
		if !existing || resolution.Kind != idempotentintent.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Script deletion replay target is inconsistent",
			)
		}
		return cloneIdempotencyResponse(resolution.Response), nil
	}

	current, err := service.repository.GetScript(ctx, scriptID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	environment, err := service.repository.GetEnvironment(ctx, current.Record.EnvironmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	targetService, err := service.repository.GetService(ctx, current.Record.ServiceID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	locator = etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: environment.Record.ID,
		Method: http.MethodDelete, Route: scriptDeletionRoute, Key: idempotencyKey,
	}
	evidence, err := service.idempotency.Prepare(ctx, locator, scriptID)
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
				"Script deletion replay resolution is invalid",
			)
		}
		return cloneIdempotencyResponse(resolution.Response), nil
	}

	now := service.now().UTC()
	task := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation), IdempotencyKey: idempotencyKey,
		Executor: etcd.TaskExecutorController, PlanID: ids.New(ids.KindPlan), RenderGeneration: 1,
		Type: etcd.TaskRemove, Target: scriptID,
		Params:         map[string]string{etcd.TaskResourceKindParam: etcd.TaskResourceScript},
		Steps:          []etcd.TaskStepRecord{{ID: ids.New(ids.KindStep)}},
		TimeoutSeconds: scriptDeletionTimeoutSeconds,
		Status:         etcd.TaskStatusPending, NextEventSequence: 1, CreatedAt: now,
	}
	task.PlanHash, err = scriptDeletionPlanHash(scriptID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	responseBody, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json",
		Body: append([]byte(nil), responseBody...),
	}
	marker := etcd.IdempotencyMarker{
		Kind: etcd.IdempotencyMarkerTask, State: etcd.IdempotencyMarkerPending,
		Locator: locator, ReplayTarget: &target, Intent: evidence.durable, Response: response,
		TaskID: task.ID, CreatedAt: now, UpdatedAt: now,
	}
	tombstone := etcd.DeletionTombstoneRecord{
		TargetKind: etcd.DeletionTargetScript, TargetID: scriptID,
		TargetRevision: current.Revision, TaskID: task.ID, Phase: etcd.DeletionPhaseFinalizing,
		CreatedAt: now, UpdatedAt: now,
	}
	result, deleteErr := service.repository.BeginScriptDeletionWithTask(
		ctx, environment, project, targetService, current, tombstone, task, marker,
	)
	if deleteErr != nil {
		if !isUnknownScriptDeletionOutcome(deleteErr) {
			return etcd.IdempotencyResponse{}, deleteErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, deleteErr)
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
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Script deletion resolution is invalid")
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
