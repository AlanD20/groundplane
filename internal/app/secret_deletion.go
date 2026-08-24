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
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	secretDeletionRoute           = "/secrets/{id}"
	secretDeletionTimeoutSeconds  = int64(30)
	maximumSecretDeletionAttempts = 3
)

type secretDeletionRepository interface {
	GetProject(context.Context, string) (etcd.Versioned[etcd.ProjectRecord], error)
	GetSecret(context.Context, string) (etcd.Versioned[etcd.SecretRecord], error)
	BeginSecretDeletionWithTask(
		context.Context,
		etcd.SecretOwner,
		etcd.Versioned[etcd.SecretRecord],
		etcd.DeletionTombstoneRecord,
		etcd.TaskRecord,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type secretDeletionEvidence struct {
	candidate idempotentintent.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type secretDeletionIdempotency interface {
	ResolveReplayLocator(
		context.Context,
		etcd.IdempotencyReplayTarget,
		string,
		string,
		string,
	) (etcd.IdempotencyLocator, bool, error)
	Prepare(context.Context, etcd.IdempotencyLocator, string) (secretDeletionEvidence, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		secretDeletionEvidence,
	) (idempotentintent.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		secretDeletionEvidence,
		etcd.IdempotencyTransactionResult,
	) (idempotentintent.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		secretDeletionEvidence,
		error,
	) (idempotentintent.Resolution, error)
}

type durableSecretDeletionIdempotency struct {
	coordinator *idempotentintent.Coordinator
	repository  *etcd.IdempotencyRepository
}

func newDurableSecretDeletionIdempotency(
	coordinator *idempotentintent.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableSecretDeletionIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Secret deletion idempotency is not configured")
	}
	return &durableSecretDeletionIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableSecretDeletionIdempotency) ResolveReplayLocator(
	ctx context.Context,
	target etcd.IdempotencyReplayTarget,
	method string,
	route string,
	key string,
) (etcd.IdempotencyLocator, bool, error) {
	return service.repository.ResolveReplayLocator(ctx, target, method, route, key)
}

func (service *durableSecretDeletionIdempotency) Prepare(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	secretID string,
) (secretDeletionEvidence, error) {
	scope := idempotentintent.Scope{Kind: idempotentintent.ScopePlatform}
	if locator.ScopeKind == etcd.IdempotencyScopeProject {
		scope = idempotentintent.Scope{Kind: idempotentintent.ScopeProject, ID: locator.ScopeID}
	} else if locator.ScopeKind != etcd.IdempotencyScopePlatform || locator.ScopeID != "-" {
		return secretDeletionEvidence{}, errs.New(errs.KindInternal, "Secret deletion replay scope is invalid")
	}
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: http.MethodDelete, Route: secretDeletionRoute, Scope: scope,
		Path:  []idempotentintent.PathBinding{{Name: "id", Value: secretID}},
		Query: idempotentintent.Object(), Body: idempotentintent.NoBody(),
	})
	if err != nil {
		return secretDeletionEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return secretDeletionEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return secretDeletionEvidence{}, err
	}
	return secretDeletionEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableSecretDeletionIdempotency) ResolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence secretDeletionEvidence,
) (idempotentintent.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableSecretDeletionIdempotency) ResolveKnown(
	ctx context.Context,
	evidence secretDeletionEvidence,
	result etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableSecretDeletionIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence secretDeletionEvidence,
	original error,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type secretDeletionService struct {
	repository  secretDeletionRepository
	idempotency secretDeletionIdempotency
	now         func() time.Time
}

func newSecretDeletionService(
	repository secretDeletionRepository,
	idempotency secretDeletionIdempotency,
) (*secretDeletionService, error) {
	if repository == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Secret deletion service is not configured")
	}
	return &secretDeletionService{repository: repository, idempotency: idempotency, now: time.Now}, nil
}

func (service *secretDeletionService) DeleteSecret(
	ctx context.Context,
	secretID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Secret deletion context is required")
	}
	if ids.Validate(ids.KindSecret, secretID) != nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Secret id is invalid")
	}
	for attempt := 0; attempt < maximumSecretDeletionAttempts; attempt++ {
		response, err := service.deleteSecretOnce(ctx, secretID, idempotencyKey)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumSecretDeletionAttempts-1 {
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Secret deletion retry bound was not enforced")
}

func (service *secretDeletionService) deleteSecretOnce(
	ctx context.Context,
	secretID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	target := etcd.IdempotencyReplayTarget{Kind: etcd.IdempotencyReplayTargetSecret, ID: secretID}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx, target, http.MethodDelete, secretDeletionRoute, idempotencyKey,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if indexed {
		evidence, err := service.idempotency.Prepare(ctx, locator, secretID)
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
				"Secret deletion replay target is inconsistent",
			)
		}
		return cloneIdempotencyResponse(resolution.Response), nil
	}

	current, err := service.repository.GetSecret(ctx, secretID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	owner := etcd.PlatformSecretOwner()
	locator = etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopePlatform, ScopeID: "-",
		Method: http.MethodDelete, Route: secretDeletionRoute, Key: idempotencyKey,
	}
	if current.Record.Secret.Scope == core.SecretScopeProject {
		project, err := service.repository.GetProject(ctx, current.Record.Secret.ProjectID)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
		owner = etcd.ProjectSecretOwner(project)
		locator.ScopeKind = etcd.IdempotencyScopeProject
		locator.ScopeID = project.Record.ID
	}
	evidence, err := service.idempotency.Prepare(ctx, locator, secretID)
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
				"Secret deletion replay resolution is invalid",
			)
		}
		return cloneIdempotencyResponse(resolution.Response), nil
	}

	taskOwner := etcd.PlatformTaskOwner()
	if owner.Project != nil {
		taskOwner, err = etcd.ProjectTaskOwner(owner.Project.Record)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
	}
	now := service.now().UTC()
	task := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation), IdempotencyKey: idempotencyKey,
		Owner: taskOwner, Actor: etcd.TaskActorOperator,
		Executor: etcd.TaskExecutorController, PlanID: ids.New(ids.KindPlan), RenderGeneration: 1,
		Type: etcd.TaskRemove, Target: secretID,
		Params:         map[string]string{etcd.TaskResourceKindParam: etcd.TaskResourceSecret},
		Steps:          []etcd.TaskStepRecord{{ID: ids.New(ids.KindStep)}},
		TimeoutSeconds: secretDeletionTimeoutSeconds,
		Status:         etcd.TaskStatusPending, NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	task.PlanHash, err = secretDeletionPlanHash(secretID)
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
		TargetKind: etcd.DeletionTargetSecret, TargetID: secretID,
		TargetRevision: current.Revision, TaskID: task.ID, Phase: etcd.DeletionPhaseFinalizing,
		CreatedAt: now, UpdatedAt: now,
	}
	result, deleteErr := service.repository.BeginSecretDeletionWithTask(
		ctx, owner, current, tombstone, task, marker,
	)
	if deleteErr != nil {
		if !isUnknownSecretDeletionOutcome(deleteErr) {
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
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Secret deletion resolution is invalid")
	}
}

func secretDeletionPlanHash(secretID string) (string, error) {
	value, err := json.Marshal(struct {
		Version  int    `json:"version"`
		Type     string `json:"type"`
		SecretID string `json:"secret_id"`
	}{Version: 1, Type: string(etcd.TaskRemove), SecretID: secretID})
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(value)
	clear(value)
	return hex.EncodeToString(digest[:]), nil
}

func isUnknownSecretDeletionOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
